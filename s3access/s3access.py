import base64
import fnmatch
import glob
import hashlib
import logging
import multiprocessing
import os
import threading
from concurrent import futures
from dataclasses import dataclass, field
from typing import Union, List, Iterator, Dict, Type, TypeVar, Optional, Tuple

import boto3
from botocore.client import BaseClient
from readstr import readstr

from .conditions import Condition, make_conditions, Conditionable
from .reader import Reader, Pandas
from .s3path import S3Path

logger = logging.getLogger(__name__)


@dataclass
class LsResult:
    prefixes: List[S3Path] = field(default_factory=list)
    contents: List[S3Path] = field(default_factory=list)

    @property
    def directories(self):
        return self.prefixes

    @property
    def files(self):
        return self.contents


# noinspection PyShadowingBuiltins
def read_value(value, type):
    if value is None:
        return None
    try:
        return readstr(value, type)
    except ValueError:
        return None


T = TypeVar('T')


class _NoValue:
    pass


class S3Access:
    def __init__(self, parallelism: int = multiprocessing.cpu_count() * 4, cachedir: str = _NoValue):
        self._num_workers: int = parallelism
        if cachedir is _NoValue:
            cachedir = os.getenv('S3ACCESSCACHE')
        self._cachedir: Optional[str] = cachedir

    @staticmethod
    def s3client() -> BaseClient:
        """
        Returns an S3 client for this thread.
        """
        try:
            client_ = threading.local().s3client
        except AttributeError:
            endpoint_url = os.getenv('S3_ENDPOINT_URL') or None  # prevent empty string
            client_ = boto3.client('s3', endpoint_url=endpoint_url)
            threading.local().s3client = client_
        return client_

    def ls(self, s3path: Union[str, S3Path]) -> LsResult:
        """
        Lists all files and folders at the given location.

        The s3path given is assumed to refer to a directory in S3.
        Yes, there are no directories in S3 but common prefixes, but
        that does not stop anyone from thinking about it like directories.

        The result is separated into the prefixes (directories) and
        contents (files).

        This method is faster than just listing everything with the same
        prefix, due to the use of `Delimiter` in the api call. If there are
        more than 1000 entries this method automatically follows up using
        the continuation token (this is hidden away from the user).
        """
        if isinstance(s3path, str):
            s3path = S3Path(s3path)
        key = s3path.key
        if key and key[-1] != '/':
            key += '/'
        response = self.s3client().list_objects_v2(
            Bucket=s3path.bucket,
            Prefix=key,
            Delimiter='/',
        )
        result = LsResult()
        result.prefixes.extend(s3path.with_key(common_prefix['Prefix'])
                               for common_prefix in response.get('CommonPrefixes', []))
        result.contents.extend(s3path.with_key(content['Key'])
                               for content in response.get('Contents', []))
        while 'NextContinuationToken' in response:
            response = self.s3client().list_objects_v2(
                Bucket=s3path.bucket,
                Prefix=s3path.key + '/',
                Delimiter='/',
                ContinuationToken=response['NextContinuationToken']
            )
            result.prefixes.extend(s3path.with_key(common_prefix['Prefix'])
                                   for common_prefix in response.get('CommonPrefixes', []))
            result.contents.extend(s3path.with_key(content['Key'])
                                   for content in response.get('Contents', []))
        return result

    def ls_prefix(self, s3path: Union[str, S3Path]) -> Iterator[S3Path]:
        """
        List all entries with the given s3path as prefix.

        Do so exhaustively, i.e. if the number of entries exceeds the maximum page size
        this method will automatically follow up using a contiuation token. This happens
        on-demand as it returns an iterator, i.e. the seconds api call will only happen
        once you consumed the first 1000 entries from the iterator returned.
        """
        if isinstance(s3path, str):
            s3path = S3Path(s3path)
        response = self.s3client().list_objects_v2(
            Bucket=s3path.bucket,
            Prefix=s3path.key,
        )
        yield from (s3path.with_key(content['Key']) for content in response.get('Contents', []))
        while 'NextContinuationToken' in response:
            logger.debug("Looking for more items using %s", response['NextContinuationToken'])
            response = self.s3client().list_objects_v2(
                Bucket=s3path.bucket,
                Prefix=s3path.key,
                ContinuationToken=response['NextContinuationToken']
            )
            yield from (s3path.with_key(content['Key']) for content in response.get('Contents', []))

    @staticmethod
    def is_glob(value):
        return glob.escape(value) != value

    def glob(self, s3path: Union[str, S3Path]) -> Iterator[S3Path]:
        """
        Lists all entries that match a given glob-pattern.

        glob-patterns are actually more powerful than just the asterisk-any-match,
        but this method is concerned with those only.

        TODO: if needed pimp this in the future. Currently this method splits the
        path on the first occurence of an asterisk, fetches everything with the
        determined prefix, and checks the results for whether they match. No optimization
        is applied if other glob patterns (?, []) are used.

        Behind the scenes this method uses ls_prefix which lazily fetches more items
        using a continuation token if needed, and this laziness propagates into this
        method, which also does return an Iterator.
        """
        if isinstance(s3path, str):
            s3path = S3Path(s3path)

        if '*' in s3path.key:
            prefix, _r = s3path.key.split('*', maxsplit=1)
        else:
            prefix = s3path.key
        for p in self.ls_prefix(s3path.with_key(prefix)):
            if fnmatch.fnmatch(p.key, s3path.key):
                yield p

    @staticmethod
    def _check_path(path: S3Path, conditions: Dict[str, Condition]) -> bool:
        for k, c in conditions.items():
            if k in path.params:
                if not c.check(path[k]):
                    return False
        return True

    def _select_glob(self,
                     s3path: Union[str, S3Path],
                     columns: Dict[str, Type],
                     filters: Dict[str, Condition],
                     reader: Reader[T]) -> T:
        paths = self.glob(s3path)

        pool = futures.ThreadPoolExecutor(max_workers=self._num_workers)

        def worker(p: S3Path) -> T:
            logger.debug("Spawned selecting from %s", p)
            result = self.select(p, columns, filters, reader)
            return result

        it = iter(paths)
        pending_futures = []
        results: List[T] = []
        try:
            spawned = 0
            while True:  # the loop will stop when next() raises StopIteration
                # poor man's barrier
                while spawned < self._num_workers:
                    path = next(it)
                    if filters and not self._check_path(path, filters):
                        continue
                    spawned += 1
                    pending_futures.append(pool.submit(worker, path))
                done, pending = futures.wait(pending_futures, return_when=futures.FIRST_COMPLETED)
                spawned -= len(done)
                results.extend(future.result() for future in done)
                pending_futures = [*pending]
        except StopIteration:
            done, _ = futures.wait(pending_futures, return_when=futures.ALL_COMPLETED)
            results.extend(future.result() for future in done)
        finally:
            pool.shutdown()
        return reader.combine(results)

    @staticmethod
    def _build_expression(s3path: S3Path, columns: Dict[str, Type], filters: Dict[str, Condition]) -> str:
        query = f"SELECT {', '.join(f's.{key}' for key in columns.keys())} FROM S3Object s"
        object_filters = {k: f for k, f in filters.items() if k not in s3path.params}
        if object_filters:
            query += ' WHERE '
            query += ' AND '.join(c.get_sql_fragment(f"s.{k}") for k, c in object_filters.items())
        return query

    def _select(self,
                s3path: Union[str, S3Path],
                columns: Dict[str, Type],
                filters: Dict[str, Condition],
                reader: Reader[T]) -> T:

        # noinspection SqlResolve,SqlNoDataSourceInspection
        query = self._build_expression(s3path, columns, filters)
        logger.debug("Issuing S3 Select Query: ``%s'' on %s", query, s3path)
        response = self.s3client().select_object_content(
            Bucket=s3path.bucket,
            Key=s3path.key,
            InputSerialization={'Parquet': {}},
            OutputSerialization=reader.serialization,
            ExpressionType='SQL',
            Expression=query,
        )
        bs = self._read_s3_select_response(response)
        return reader.read(bs, columns=columns)

    @staticmethod
    def _read_s3_select_response(response) -> bytearray:
        res = bytearray()
        for stream in response['Payload']:
            try:
                res.extend(stream['Records']['Payload'])
            except KeyError:
                continue
        return res

    @staticmethod
    def _cache_fingerprint(s3path: S3Path, columns: Dict[str, Type], filters: Dict[str, Condition]) -> str:
        md = hashlib.sha1()
        md.update(str(s3path).encode('utf8'))
        for column_name in columns.keys():
            md.update(column_name.encode('utf8'))
        for column_name, condition in filters.items():
            md.update(column_name.encode('utf8'))
            md.update(str(condition).encode('utf8'))
        fingerprint: str = base64.b32encode(md.digest()).decode('ascii')
        return fingerprint

    def _realm(self, s3path: S3Path) -> str:
        if not self.is_glob(s3path.key):
            return 'file'
        return 'glob_' + s3path.key.translate(str.maketrans({'/': ',', '*': '_', '[': '_', ']': '_', '?': '_'}))

    def _in_cache(
            self,
            s3path: Union[str, S3Path],
            columns: Dict[str, Type],
            filters: Dict[str, Condition]) -> Tuple[bool, Optional[str]]:
        fingerprint = self._cache_fingerprint(s3path, columns, filters)
        realm = self._realm(s3path)
        cache_file = None
        if self._cachedir and os.path.isdir(self._cachedir):
            cache_file = f"{self._cachedir}/{s3path.bucket}/{realm}/{fingerprint}.bin"
            if os.path.exists(cache_file):
                return True, cache_file
        return False, cache_file

    def select(self,
               s3path: Union[str, S3Path],
               columns: Dict[str, Type],
               filters: Dict[str, Conditionable] = None,
               reader: Reader[T] = Pandas()
               ) -> T:
        """
        Selects the given columns of the given type from the given s3path.

        Args:
            s3path: A url to s3 like s3://bucket/path/file.parquet or s3://bucket/date=2020-*/*.parquet
            columns: A dict of name/type pairs for which columns to select. For example:
                {
                    'campaign_name': str,
                    'campaign_id': str,
                    'cost': float,
                    'installs': int,
                }
            filters: An optional dict of filters which are combined using AND. Each key refers to a column,
                each value is a Condition of which there are EQ, LT, LTE, GT, GTE, NET, and IN. For example:
                {
                    'cost': GT(2.5),
                    'country': IN('USA', 'NZL', 'ITA'),
                }
            reader: A Reader that deserializes the response. Defaults to the Pandas dataframe reader.
        """
        if isinstance(s3path, str):
            s3path = S3Path(s3path)
        filters = make_conditions(filters or {})

        in_cache, cache_file = False, None
        if reader.supports_caching:
            in_cache, cache_file = self._in_cache(s3path, columns, filters)
            if in_cache:
                return reader.read_cache(cache_file)

        if self.is_glob(s3path.key):
            result = self._select_glob(s3path, columns, filters, reader)
        else:
            result = self._select(s3path, columns, filters, reader)

        if reader.supports_caching and cache_file:
            cachedir = os.path.join(cache_file.rpartition('/')[0])
            try:
                os.makedirs(cachedir, exist_ok=True)
            except Exception:
                logger.warning("could not create cahce directory %s, not caching", cachedir)
            else:
                logger.debug("Writing result to cache %s", cache_file)
                reader.write_cache(cache_file, result)

        return result

    async def select_async(
            self,
            s3path: Union[str, S3Path],
            columns: Dict[str, Type],
            filters: Dict[str, Conditionable] = None,
            reader: Reader[T] = Pandas()) -> T:
        # async interface is optional
        import aiobotocore
        from s3access.s3async.s3select import multiple_as_completed

        if isinstance(s3path, str):
            s3path = S3Path(s3path)
        filters = filters or {}
        filters = make_conditions(filters)

        is_glob = self.is_glob(s3path.key)
        in_cache, global_cache_file = False, None

        if is_glob and reader.supports_caching:
            in_cache, global_cache_file = self._in_cache(s3path, columns, filters)
            if in_cache:
                return reader.read_cache(global_cache_file)

        # TODO: the glob part should eventually be asynchronous as well
        paths = [s3path] if not is_glob else self.glob(s3path)
        if not paths:
            return reader.combine([])

        results = []
        missing_paths = []
        cache_files: Dict[Tuple[str, str], str] = {}
        for p in paths:
            in_cache, cache_file = self._in_cache(p, columns, filters)
            if in_cache:
                results.append(reader.read_cache(cache_file))
            else:
                missing_paths.append(p)
                if cache_file:
                    cache_files[(p.bucket, p.key)] = cache_file
        if len(missing_paths) == 0:
            combined = reader.combine(results)
            if is_glob and global_cache_file:
                reader.write_cache(global_cache_file, combined)
            return combined

        bucket = missing_paths[0].bucket
        sources = {bucket: [p.key for p in missing_paths]}
        query = self._build_expression(s3path, columns, filters)
        session = aiobotocore.get_session()
        results = []
        async with session.create_client('s3') as client:
            async for content, cache_key in multiple_as_completed(client, sources, query):
                logger.debug("fetch completed for %s - %s", cache_key[0], cache_key[1])
                parsed = reader.read(content, columns)
                results.append(parsed)
                cache_file = cache_files.get(cache_key)
                if cache_file:
                    reader.write_cache(cache_file, content)

        combined = reader.combine(results)
        if is_glob and global_cache_file:
            reader.write_cache(global_cache_file, combined)

        return combined

import base64
import csv
import fnmatch
import glob
import hashlib
import logging
import multiprocessing
import os
import threading
from concurrent import futures
from dataclasses import dataclass, field
from io import StringIO
from typing import Union, List, Iterator, Dict, Type, Sequence

import boto3
import pandas as pd
from botocore.client import BaseClient
from readstr import readstr

from .conditions import Condition, EQ, IN
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


class S3Access:
    def __init__(self, parallelism: int = multiprocessing.cpu_count() * 4):
        self._num_workers = parallelism
        self._cachedir = os.getenv('S3ACCESSCACHE')

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
                     filters: Dict[str, Condition]) -> pd.DataFrame:
        paths = self.glob(s3path)

        spawned = 0
        pool = futures.ThreadPoolExecutor(max_workers=self._num_workers)

        def worker(p: S3Path) -> pd.DataFrame:
            nonlocal spawned
            logger.debug("Spawned selecting from %s", p)
            result = self.select(p, columns, filters)
            spawned -= 1
            logger.debug("Got %s items", len(result))
            return result

        it = iter(paths)
        pending_futures = []
        result_dfs = []
        try:
            while True:  # the loop will stop when next() raises StopIteration
                # poor man's barrier
                while spawned < self._num_workers:
                    path = next(it)
                    if filters and not self._check_path(path, filters):
                        continue
                    spawned += 1
                    pending_futures.append(pool.submit(worker, path))
                done, pending = futures.wait(pending_futures, return_when=futures.FIRST_COMPLETED)
                result_dfs.extend(future.result() for future in done)
                pending_futures = [*pending]
        except StopIteration:
            done, _ = futures.wait(pending_futures, return_when=futures.ALL_COMPLETED)
            result_dfs.extend(future.result() for future in done)
        finally:
            pool.shutdown()

        if not result_dfs:  # in case no path matched or no files were actually there
            return pd.DataFrame([], columns=[*s3path.params.keys(), *columns.keys()])
        return pd.concat(result_dfs)

    def _select(self,
                s3path: Union[str, S3Path],
                columns: Dict[str, Type],
                filters: Dict[str, Condition]) -> pd.DataFrame:
        # noinspection SqlResolve,SqlNoDataSourceInspection
        query = f"SELECT {', '.join(f's.{key}' for key in columns.keys())} FROM S3Object s"
        object_filters = {}
        for k, f in filters.items():
            if k in s3path.params:
                continue
            object_filters[k] = f
        if object_filters:
            query += ' WHERE '
            query += ' AND '.join(c.get_sql_fragment(f"s.{k}") for k, c in object_filters.items())
        logger.debug("Issuing S3 Select Query: ``%s'' on %s", query, s3path)
        response = self.s3client().select_object_content(
            Bucket=s3path.bucket,
            Key=s3path.key,
            InputSerialization={'Parquet': {}},
            OutputSerialization={'CSV': {
                'QuoteFields': 'ALWAYS',
                'QuoteEscapeCharacter': '"',
                'FieldDelimiter': ',',
                'QuoteCharacter': '"',
            }},
            ExpressionType='SQL',
            Expression=query,
        )

        rows = []
        for row in csv.reader(self._read_s3_select_response(response), dialect='unix'):
            rows.append([
                *s3path.params.values(),
                *(read_value(field_value, field_type) for field_value, field_type in zip(row, columns.values())),
            ])
        return pd.DataFrame(rows, columns=[*s3path.params.keys(), *columns.keys()])

    def select(self,
               s3path: Union[str, S3Path],
               columns: Dict[str, Type],
               filters: Dict[str, Union[Condition, str, int, float, Sequence[str]]] = None
               ) -> pd.DataFrame:
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
        """
        if isinstance(s3path, str):
            s3path = S3Path(s3path)
        if filters is None:
            filters = {}

        def make_condition(v):
            if isinstance(v, (str, int, float)):
                return EQ(v)
            if isinstance(v, Sequence):
                return IN(*v)
            assert isinstance(v, Condition), "must be a condition"
            return v

        filters: Dict[str, Condition] = {k: make_condition(v) for k, v in filters.items()}

        md = hashlib.sha1()
        md.update(str(s3path).encode('utf8'))
        for column_name in columns.keys():
            md.update(column_name.encode('utf8'))
        for column_name, condition in filters.items():
            md.update(column_name.encode('utf8'))
            md.update(str(condition).encode('utf8'))
        fingerprint: str = base64.b32encode(md.digest()).decode('ascii')

        if self.is_glob(s3path.key):
            realm = 'glob_' + s3path.key.translate(str.maketrans({'/': ',', '*': '_', '[': '_', ']': '_', '?': '_'}))
        else:
            realm = 'file'

        if self._cachedir and os.path.isdir(self._cachedir):
            cache_file = f"{self._cachedir}/{s3path.bucket}/{realm}/{fingerprint}.parquet"
            if os.path.exists(cache_file):
                logger.debug("Using cached dataframe from %s", cache_file)
                return pd.read_parquet(cache_file)

        if self.is_glob(s3path.key):
            result_df = self._select_glob(s3path, columns, filters)
        else:
            result_df = self._select(s3path, columns, filters)

        if self._cachedir:
            if not os.path.exists(self._cachedir):
                os.mkdir(self._cachedir)
            if not os.path.isdir(self._cachedir):
                logger.warning("Caching enabled, but %s is not a directory (not writing cache)", self._cachedir)
            else:
                if not os.path.exists(cachedir := f"{self._cachedir}/{s3path.bucket}"):
                    os.mkdir(cachedir)
                if not os.path.exists(cachedir := f"{cachedir}/{realm}"):
                    os.mkdir(cachedir)
                cache_file = f"{cachedir}/{fingerprint}.parquet"
                logger.debug("Writing result to cache %s", cache_file)
                result_df.to_parquet(cache_file)

        return result_df

    @staticmethod
    def _read_s3_select_response(response) -> StringIO:
        res = bytearray()
        for stream in response['Payload']:
            try:
                res.extend(stream['Records']['Payload'])
            except KeyError:
                break
        return StringIO(res.decode('utf8'))

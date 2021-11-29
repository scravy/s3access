import os
import fnmatch
from contextlib import asynccontextmanager
from typing import Union, AsyncIterator

from ..s3path import S3Path


@asynccontextmanager
async def s3client():
    params = {}
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    if endpoint_url:
        params['endpoint_url'] = endpoint_url
    from aiobotocore.session import get_session
    session = get_session()
    async with session.create_client('s3', **params) as client:
        yield client


def _glob_path(s3path: S3Path) -> S3Path:
    if '*' in s3path.key:
        prefix, _ = s3path.key.split('*', maxsplit=1)
    else:
        prefix = s3path.key
    return s3path.with_key(prefix)


async def glob(s3path: Union[str, S3Path]) -> AsyncIterator[S3Path]:
    if isinstance(s3path, str):
        s3path = S3Path(s3path)
    glob_path = _glob_path(s3path)
    async with s3client() as client:
        paginator = client.get_paginator('list_objects')
        async for result in paginator.paginate(Bucket=glob_path.bucket, Prefix=glob_path.key):
            for obj in result['Contents']:
                key = obj['Key']
                if fnmatch.fnmatch(key, s3path.key):
                    yield glob_path.with_key(key)

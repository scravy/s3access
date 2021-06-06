import asyncio
from typing import Sequence, Dict, Any, Optional, Tuple, Callable, AsyncIterator, Mapping, Coroutine
from aiobotocore.client import AioBaseClient

from ..select_spec import \
    Input, Output, DEFAULT_INPUT, DEFAULT_OUTPUT, DEFAULT_SCAN_RANGE, ScanRange, ExpressionType, \
    WITH_PROGRESS, NO_PROGRESS, simple_parquet_selection


ProgressCallback = Callable[[str, str, Any], None]
BucketKeyPair = Tuple[str, str]

"""
Sources are mappings where the mapping-key is the bucket and the value is a sequence of keys inside the bucket
Example: {'some-bucket': ['path/to/file.parquet', 'path/to/another_file.parquet', 'some/other/key.parquet']
"""
Sources = Mapping[str, Sequence[str]]


async def select(
        client: AioBaseClient,
        bucket: str,
        key: str,
        expression: str,
        expression_type: ExpressionType = ExpressionType.SQL,
        input_serialization: Input = DEFAULT_INPUT,
        output_serialization: Output = DEFAULT_OUTPUT,
        scan_range: ScanRange = DEFAULT_SCAN_RANGE,
        progress_callback: Optional[ProgressCallback] = None) -> bytes:
    obj = await client.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=expression,
        ExpressionType=expression_type.value,
        InputSerialization=input_serialization.params(),
        OutputSerialization=output_serialization.params(),
        ScanRange=scan_range.params(),
        RequestProgress=(WITH_PROGRESS if progress_callback else NO_PROGRESS).params()
    )
    res = bytearray()
    end = False
    async for event in obj['Payload']:
        if 'Records' in event:
            res.extend(event['Records']['Payload'])
        elif 'Progress' in event and progress_callback:
            progress_callback(bucket, key, event['Progress']['Details'])
        elif 'End' in event:
            end = True
    if not end:
        raise IOError("stream was not read in full")
    return res


def _select_tasks(
        client: AioBaseClient,
        sources: Sources,
        expression: str,
        expression_type: ExpressionType = ExpressionType.SQL,
        input_serialization: Input = DEFAULT_INPUT,
        output_serialization: Output = DEFAULT_OUTPUT,
        scan_range: ScanRange = DEFAULT_SCAN_RANGE,
        progress_callback: Optional[ProgressCallback] = None) -> Tuple[Sequence[Coroutine], Sequence[BucketKeyPair]]:
    tasks = []
    key_list = []
    for bucket, keys in sources.items():
        for key in keys:
            tasks.append(
                select(client, bucket, key, expression, expression_type, input_serialization,
                       output_serialization, scan_range, progress_callback))
            key_list.append((bucket, key))
    return tasks, key_list


async def multiple(
        client: AioBaseClient,
        sources: Sources,
        expression: str,
        expression_type: ExpressionType = ExpressionType.SQL,
        input_serialization: Input = DEFAULT_INPUT,
        output_serialization: Output = DEFAULT_OUTPUT,
        scan_range: ScanRange = DEFAULT_SCAN_RANGE,
        progress_callback: Optional[ProgressCallback] = None) -> Dict[BucketKeyPair, bytes]:
    tasks, key_list = _select_tasks(
        client, sources, expression, expression_type, input_serialization,
        output_serialization, scan_range, progress_callback)
    results = await asyncio.gather(*tasks)
    return dict(zip(key_list, results))


async def multiple_as_completed(
        client: AioBaseClient,
        sources: Sources,
        expression: str,
        expression_type: ExpressionType = ExpressionType.SQL,
        input_serialization: Input = DEFAULT_INPUT,
        output_serialization: Output = DEFAULT_OUTPUT,
        scan_range: ScanRange = DEFAULT_SCAN_RANGE,
        progress_callback: Optional[ProgressCallback] = None) -> AsyncIterator[Tuple[bytes, BucketKeyPair]]:
    tasks, key_list = _select_tasks(
        client, sources, expression, expression_type, input_serialization,
        output_serialization, scan_range, progress_callback)
    for i, file_f in enumerate(asyncio.as_completed(tasks)):
        file = await file_f
        yield file, key_list[i]


async def parquets_to_csv_bytes(
        client: AioBaseClient,
        sources: Sources,
        columns: Optional[Sequence[str]] = None,
        filters: Sequence[Tuple[Any, str, Any]] = None,
        progress_callback: Optional[ProgressCallback] = None) -> bytes:
    """
    example usage:
    -----------------------------------------
    def print_progress(bucket, key, details):
        if details == 'completed':
            print(bucket, key, details)
    bucket = 'some-bucket'
    keys = ['key/key1.parquet', 'key/key2.parquet', ...]
    columns = ['col1', 'col2', 'col5', 'col10']  # some columns all files share
    filters = [('col1', '=', 'some string value'), ('col5', '>=', 10.2), ('col5', '<', 20.3)]
    session = aiobotocore.get_session()
    async with session.create_client('s3') as client:
        blob = await parquets_to_csv_bytes(client, {bucket: keys}, columns, filters, print_progress)
    content = io.StringIO(blob.decode('utf-8'))
    for row in csv.reader(content):
        do_something_with_row(row)
    :param client: initialized AioBaseClient to use
    :param sources: a dictionary mapping a bucket to a list of files to scan from it. ALL files must be in the same
                    schema
    :param columns: which column to load
    :param filters: how to filter the parquet file
    :param progress_callback: a callback to report progress to, each file when completed will also report completion
    :return: bytes
    """
    q = simple_parquet_selection(columns, filters)
    total = bytearray()
    async for content, (bucket, key) in multiple_as_completed(
            client, sources, q, progress_callback=progress_callback):
        total.extend(content)
        if progress_callback:
            progress_callback(bucket, key, "completed")
    return total

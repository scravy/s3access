import io
import asyncio
import pandas as pd
from datetime import date, datetime
from typing import Sequence, Dict, Any, Optional, Tuple, Callable, AsyncIterator, Mapping, Coroutine
from aiobotocore.client import AioBaseClient

from s3access.select_spec import \
    Input, Output, DEFAULT_INPUT, DEFAULT_OUTPUT, DEFAULT_SCAN_RANGE, ScanRange, ExpressionType, \
    WITH_PROGRESS, NO_PROGRESS


ProgressCallback = Callable[[str, str, Any], None]


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
        sources: Mapping[str, Sequence[str]],
        expression: str,
        expression_type: ExpressionType = ExpressionType.SQL,
        input_serialization: Input = DEFAULT_INPUT,
        output_serialization: Output = DEFAULT_OUTPUT,
        scan_range: ScanRange = DEFAULT_SCAN_RANGE,
        progress_callback: Optional[ProgressCallback] = None) -> Tuple[Sequence[Coroutine], Sequence[Tuple[str, str]]]:
    tasks = []
    key_list = []
    for bucket, keys in sources.items():
        for key in keys:
            tasks.append(
                select(client, bucket, key, expression, expression_type, input_serialization,
                       output_serialization, scan_range, progress_callback))
            key_list.append((bucket, key))
    return tasks, key_list


async def select_multi(
        client: AioBaseClient,
        sources: Mapping[str, Sequence[str]],
        expression: str,
        expression_type: ExpressionType = ExpressionType.SQL,
        input_serialization: Input = DEFAULT_INPUT,
        output_serialization: Output = DEFAULT_OUTPUT,
        scan_range: ScanRange = DEFAULT_SCAN_RANGE,
        progress_callback: Optional[ProgressCallback] = None) -> Dict[Tuple[str, str], bytes]:
    tasks, key_list = _select_tasks(
        client, sources, expression, expression_type, input_serialization,
        output_serialization, scan_range, progress_callback)
    results = await asyncio.gather(*tasks)
    return dict(zip(key_list, results))


async def select_multi_as_completed(
        client: AioBaseClient,
        sources: Mapping[str, Sequence[str]],
        expression: str,
        expression_type: ExpressionType = ExpressionType.SQL,
        input_serialization: Input = DEFAULT_INPUT,
        output_serialization: Output = DEFAULT_OUTPUT,
        scan_range: ScanRange = DEFAULT_SCAN_RANGE,
        progress_callback: Optional[ProgressCallback] = None) -> AsyncIterator[Tuple[bytes, str]]:
    tasks, key_list = _select_tasks(
        client, sources, expression, expression_type, input_serialization,
        output_serialization, scan_range, progress_callback)
    for i, file_f in enumerate(asyncio.as_completed(tasks)):
        file = await file_f
        yield file, key_list[i]


def _quote(v) -> str:
    if isinstance(v, str):
        return f"'{v}'"
    if isinstance(v, (date, datetime)):
        return f"'{v.isoformat()}'"
    if isinstance(v, Sequence):
        return f"({','.join(_quote(m) for m in v)})"
    return str(v)


def _csv_bytes_to_df(
        data: bytes, columns: Sequence[str], types: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
    if len(data) == 0:
        return None
    file_handle = io.BytesIO(data)
    return pd.read_csv(file_handle, header=None, encoding='utf-8', names=columns, dtype=types)


async def select_dataframe_from_parquets(
        client: AioBaseClient,
        sources: Mapping[str, Sequence[str]],
        columns: Sequence[str],
        types: Optional[Dict[str, Any]] = None,
        filters: Sequence[Tuple[Any, str, Any]] = None,
        progress_callback: Optional[ProgressCallback] = None) -> Optional[pd.DataFrame]:
    """
    example usage:
    -----------------------------------------
    def print_progress(bucket, key, details):
        if details == 'completed':
            print(bucket, key, details)

    bucket = '<some bucket you have access to>'
    keys = [<parquet file key path>, ...]
    columns = ['col1', 'col2', 'col5', 'col10']  # some columns all files share
    dtypes = {'col1': str, 'col2': 'category', 'col5': float, 'col10': float}
    filters = [('col1', '=', 'some string value'), ('col5', '>=', 10.2), ('col5', '<', 20.3)]
    session = aiobotocore.get_session()
    async with session.create_client('s3') as client:
        new_df = await select_dataframe_from_parquets(client, {bucket: keys}, columns, dtypes, filters, print_progress)
    :param client: initialized AioBaseClient to use
    :param sources: a dictionary mapping a bucket to a list of files to scan from it. ALL files must be in the same
                    schema
    :param columns: which column to load
    :param types: type conversion to the resulting columns
    :param filters: how to filter the parquet file
    :param progress_callback: a callback to report progress to, each file when completed will also report completion
    :return: pandas.DataFrame
    """
    projection = '*' if not columns else ','.join(f"s.{col}" for col in columns)
    q = f'SELECT {projection} FROM S3Object s'
    if filters:
        f = ' AND '.join(f"s.{col} {op} {_quote(right)}" for col, op, right in filters)
        q = f'{q} WHERE {f}'
    total = bytearray()
    async for content, (bucket, key) in select_multi_as_completed(
            client, sources, q, progress_callback=progress_callback):
        if len(total):
            total.extend(b'\n')
        total.extend(content)
        if progress_callback:
            progress_callback(bucket, key, "completed")
    return _csv_bytes_to_df(total, columns, types)

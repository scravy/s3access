from typing import Mapping, Sequence, Optional, Dict, Any, Tuple

import pandas as pd
from aiobotocore.client import AioBaseClient

from .s3select import ProgressCallback, parquets_to_csv_bytes
from ..s3pandas.dataframe import from_csv_bytes


async def dataframe_from_parquets(
        client: AioBaseClient,
        sources: Mapping[str, Sequence[str]],
        columns: Optional[Sequence[str]] = None,
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
    dtypes = {'col1': str, 'col2': 'category', 'col5': float, 'col10': date}
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
    blob = await parquets_to_csv_bytes(client, sources, columns, filters, progress_callback)
    return from_csv_bytes(blob, columns, types)

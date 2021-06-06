import io
from datetime import date, datetime
from typing import Sequence, Optional, Dict, Any

import pandas as pd
from pandas.api.types import union_categoricals


def from_csv_bytes(
        data: bytes,
        columns: Sequence[str],
        types: Optional[Dict[str, Any]] = None,
        encoding: str = 'utf-8') -> Optional[pd.DataFrame]:
    if len(data) == 0:
        return None
    parse_dates = None
    if types:
        parse_dates = [k for k, v in types.items() if v is date or v is datetime]
        types = {k: v for k, v in types.items() if v is not date and v is not datetime}
    file_handle = io.BytesIO(data)
    res = pd.read_csv(
        file_handle,
        header=None,
        encoding=encoding,
        names=columns,
        dtype=types,
        parse_dates=parse_dates,
        infer_datetime_format=True)
    return res


def merge_categories(results: Sequence[pd.DataFrame]):
    to_cat = []
    df = results[0]
    for col in df:
        if isinstance(df[col].dtype, pd.CategoricalDtype):
            to_cat.append(col)
    if to_cat:
        unify = {}
        for col in to_cat:
            u = unify[col] = []
            for result in results:
                u.append(result[col])
        unified = {col: union_categoricals(v).categories for col, v in unify.items()}

        for res in results:
            for col in to_cat:
                res[col] = pd.Categorical(res[col].values, categories=unified[col])

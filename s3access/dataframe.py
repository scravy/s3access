import io
import pandas as pd
from datetime import date, datetime
from typing import Sequence, Optional, Dict, Any


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

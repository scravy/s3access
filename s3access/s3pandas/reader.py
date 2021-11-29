from io import BytesIO
from numbers import Number
from typing import Union, Dict, Type, Sequence

import pandas as pd

from .dataframe import from_csv_bytes, merge_categories
from ..reader import Reader, Options


def empty(columns: Dict[str, Union[Type, str]]) -> pd.DataFrame:
    df = pd.DataFrame(columns=list(columns.keys()))
    df = df.astype(columns)
    return df


class Pandas(Reader[pd.DataFrame]):
    def __init__(self, strict: bool = False):
        self._strict = strict

    def read(self, bs: Union[bytes, bytearray], columns: Dict[str, Union[Type, str]], options: Options) -> pd.DataFrame:
        if len(bs) == 0:
            return empty(columns)
        if self._strict:
            return from_csv_bytes(bs, list(columns.keys()), columns)
        df: pd.DataFrame = pd.read_csv(BytesIO(bs), header=None, names=columns.keys())
        for c, t in columns.items():
            if issubclass(t, Number):
                df[c] = pd.to_numeric(df[c], errors='coerce')
        if options.distinct:
            df.drop_duplicates(inplace=True)
        return df

    def combine(self, results: Sequence[pd.DataFrame], options: Options) -> pd.DataFrame:
        if not results:
            return pd.DataFrame([])
        if len(results) == 1:  # no need to concat
            return results[0]
        merge_categories(results)
        df: pd.DataFrame = pd.concat(results, ignore_index=True)
        if options.distinct:
            df.drop_duplicates(inplace=True)
        return df

    @property
    def supports_caching(self):
        return True

    @property
    def serialization(self):
        return {'CSV': {
            'QuoteFields': 'ALWAYS',
            'QuoteEscapeCharacter': '"',
            'FieldDelimiter': ',',
            'QuoteCharacter': '"',
        }}

    def read_cache(self, cache_file: str) -> pd.DataFrame:
        return pd.read_parquet(cache_file)

    def write_cache(self, cache_file: str, contents: pd.DataFrame):
        contents.to_parquet(cache_file)

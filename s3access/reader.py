import csv
from abc import abstractmethod, ABC
from io import StringIO
from json import JSONDecoder, JSONDecodeError
from typing import Generic, TypeVar, Union, Dict, Type, Iterator, Sequence, List

R = TypeVar('R')


class Reader(ABC, Generic[R]):
    @abstractmethod
    def read(self, bs: Union[bytes, bytearray], columns: Dict[str, Type]) -> R:
        raise NotImplementedError

    @abstractmethod
    def combine(self, results: Sequence[R]) -> R:
        raise NotImplementedError

    @property
    @abstractmethod
    def serialization(self):
        raise NotImplementedError

    @property
    def supports_caching(self):
        return False

    def read_cache(self, cache_file: str) -> R:
        return NotImplemented

    def write_cache(self, cache_file: str, contents: R):
        return NotImplemented


RowDict = Sequence[Union[int, float, str]]


class Python(Reader[Iterator[RowDict]]):
    def read(self, bs: Union[bytes, bytearray], columns: Dict[str, Type]) -> Iterator[RowDict]:
        yield from csv.reader(StringIO(bs.decode('utf8')), dialect='unix')

    def combine(self, results: Sequence[Iterator[RowDict]]) -> Iterator[RowDict]:
        for result in results:
            yield from result

    @property
    def serialization(self):
        return {'CSV': {
            'QuoteFields': 'ALWAYS',
            'QuoteEscapeCharacter': '"',
            'FieldDelimiter': ',',
            'QuoteCharacter': '"',
        }}


JsonValue = Union[None, bool, str, int, float, List['JSON'], Dict[str, 'JSON']]


class Json(Reader[Iterator[JsonValue]]):
    def read(self, bs: Union[bytes, bytearray], columns: Dict[str, Type]) -> Iterator[JsonValue]:
        s = bs.decode('utf8')
        decoder = JSONDecoder()
        offset = 0
        try:
            while True:
                obj, offset = decoder.raw_decode(s, offset)
                offset += 1  # advances past the newline delimiter
                yield obj
        except JSONDecodeError:
            # finally arrived at the end of the string (or actually got malformed json from s3, supposed not to)
            pass

    @property
    def serialization(self):
        return {'JSON': {'RecordDelimiter': '\n'}}

    def combine(self, results: Sequence[Iterator[JsonValue]]) -> Iterator[JsonValue]:
        for r in results:
            yield from r

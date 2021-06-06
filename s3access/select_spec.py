import dataclasses
from datetime import date, datetime
from enum import Enum
from typing import Dict, Optional, Union, Sequence, Tuple, Any


# https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html


class ExpressionType(Enum):
    SQL = 'SQL'


class InputSerialization(Enum):
    PARQUET = 'Parquet'
    CSV = 'CSV'
    JSON = 'JSON'


class OutputSerialization(Enum):
    CSV = 'CSV'
    JSON = 'JSON'


class QuoteField(Enum):
    ASNEEDED = 'ASNEEDED'
    ALWAYS = 'ALWAYS'


@dataclasses.dataclass(frozen=True)
class RequestProgress:
    enabled: bool = False

    def params(self):
        return {'Enabled': self.enabled}


NO_PROGRESS = RequestProgress()
WITH_PROGRESS = RequestProgress(True)


@dataclasses.dataclass(frozen=True)
class ParquetInput:

    @staticmethod
    def params():
        return {InputSerialization.PARQUET.value: {}}


DEFAULT_PARQUET_INPUT = ParquetInput()


class FileHeaderInfo(Enum):
    NONE = 'NONE'
    IGNORE = 'IGNORE'
    USE = 'USE'


@dataclasses.dataclass(frozen=True)
class CsvInput:
    """
    setting 'allow_quoted_record_delimiter' to True may lower performance.
    """
    allow_quoted_record_delimiter: bool = False
    comments: Optional[str] = None
    field_delimiter: str = ','
    file_header_info: FileHeaderInfo = FileHeaderInfo.NONE
    quote_character: str = '"'
    quote_escape_character: str = '"'
    record_delimiter: Optional[str] = None

    def params(self):
        kwargs = {
            'AllowQuotedRecordDelimiter': self.allow_quoted_record_delimiter,
            'FieldDelimiter': self.field_delimiter,
            'FileHeaderInfo': self.file_header_info.value,
            'QuoteCharacter': self.quote_character,
            'QuoteEscapeCharacter': self.quote_escape_character
        }
        if self.comments is not None:
            kwargs['Comments'] = self.comments
        if self.record_delimiter is not None:
            kwargs['RecordDelimiter'] = self.record_delimiter
        return {InputSerialization.CSV.value: kwargs}


DEFAULT_CSV_INPUT = CsvInput()


class JsonType(Enum):
    DOCUMENT = 'DOCUMENT'
    LINES = 'LINES'


@dataclasses.dataclass(frozen=True)
class JsonInput:
    type: Optional[JsonType] = None

    @staticmethod
    def serialization_kind() -> InputSerialization:
        return InputSerialization.JSON

    def params(self):
        kwargs = {}
        if self.type is not None:
            kwargs['Type'] = self.type.value
        return {InputSerialization.JSON.value: kwargs}


DEFAULT_JSON_INPUT = JsonInput()

AnyInput = Union[ParquetInput, CsvInput, JsonInput, Dict[str, Any]]


class CompressionType(Enum):
    NONE = 'NONE'
    GZIP = 'GZIP'
    BZIP = 'BZIP'


@dataclasses.dataclass(frozen=True)
class Input:
    compression_type: CompressionType = CompressionType.NONE
    input: AnyInput = DEFAULT_PARQUET_INPUT

    def params(self):
        res = self.input if isinstance(self.input, dict) else self.input.params()
        res['CompressionType'] = self.compression_type.value
        return res


DEFAULT_INPUT = Input()


@dataclasses.dataclass(frozen=True)
class CsvOutput:
    quote_fields: QuoteField = QuoteField.ALWAYS
    record_delimited: str = '\n'
    field_delimiter: str = ','
    quote_character: str = '"'
    quote_escape_character = '"'

    def params(self):
        return {
            OutputSerialization.CSV.value: {
                'QuoteFields': self.quote_fields.value,
                'RecordDelimiter': self.record_delimited,
                'FieldDelimiter': self.field_delimiter,
                'QuoteCharacter': self.quote_character,
                'QuoteEscapeCharacter': self.quote_escape_character
            }
        }


CSV_DEFAULT_OUTPUT = CsvOutput()


@dataclasses.dataclass(frozen=True)
class JsonOutput:
    record_delimiter: str = '\n'

    def params(self):
        return {OutputSerialization.JSON.value: {'RecordDelimiter': self.record_delimiter}}


JSON_DEFAULT_OUTPUT = JsonOutput()

AnyOutput = Union[CsvOutput, JsonOutput, Dict[str, Any]]


@dataclasses.dataclass(frozen=True)
class Output:
    output: AnyOutput = CSV_DEFAULT_OUTPUT

    def params(self):
        return self.output if isinstance(self.output, dict) else self.output.params()


DEFAULT_OUTPUT = Output()


@dataclasses.dataclass(frozen=True)
class ScanRange:
    start: int = 0
    end: Optional[int] = None

    def params(self) -> Dict[str, int]:
        res = {'Start': self.start}
        if self.end is not None:
            res['End'] = self.end
        return res


DEFAULT_SCAN_RANGE = ScanRange()


def _quote(v) -> str:
    if v is None:
        return 'NULL'
    if isinstance(v, str):
        v = v.replace("'", "''")
        return f"'{v}'"
    if isinstance(v, (date, datetime)):
        return f"'{v.isoformat()}'"
    if isinstance(v, Sequence):
        return f"({','.join(map(_quote, v))})"
    return str(v)


def simple_parquet_selection(
        columns: Optional[Sequence[str]] = None,
        filters: Sequence[Tuple[Any, str, Any]] = None) -> str:
    """
    select the given columns and filter the data by using "AND" between all given filters

    example usage:
    ---------------------------------------------
    columns = ['col1', 'col2', 'col5', 'col10']  # some columns all files share
    filters = [('col1', '=', 'some string value'), ('col5', '>=', 10.2), ('col5', '<', 20.3)]

    output:
    SELECT s.col1, s.col2, s.col5, s.col10 FROM S3Object s WHERE s.col1 = 'some string value' AND s.col5 >= 10.2 AND s.col5 < 20.3

    :param columns: the columns to select from the parquet
    :param filters: how to filter the data
    :return: s3-select query string
    """
    projection = '*' if not columns else ','.join(f"s.{col}" for col in columns)
    q = f'SELECT {projection} FROM S3Object s'
    if filters:
        f = ' AND '.join(f"s.{col} {op} {_quote(right)}" for col, op, right in filters)
        q = f'{q} WHERE {f}'
    return q

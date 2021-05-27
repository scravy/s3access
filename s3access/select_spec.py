import dataclasses
from enum import Enum
from typing import Dict, Optional, Union


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


AnyInput = Union[ParquetInput, CsvInput, JsonInput]


class CompressionType(Enum):
    NONE = 'NONE'
    GZIP = 'GZIP'
    BZIP = 'BZIP'


@dataclasses.dataclass(frozen=True)
class Input:
    compression_type: CompressionType = CompressionType.NONE
    input: AnyInput = DEFAULT_PARQUET_INPUT

    def params(self):
        res = self.input.params()
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

AnyOutput = Union[CsvOutput, JsonOutput]


@dataclasses.dataclass(frozen=True)
class Output:
    output: AnyOutput = CSV_DEFAULT_OUTPUT

    def params(self):
        return self.output.params()


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

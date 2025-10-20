# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from collections.abc import Iterable, Iterator, Sequence
from typing import IO, Any, Literal, TypeAlias, TypedDict

from _typeshed import StrPath

from ._stubs_typing import Order
from .lib import (
    Buffer,
    ChunkedArray,
    KeyValueMetadata,
    MemoryPool,
    NativeFile,
    RecordBatch,
    Schema,
    Table,
    _Weakrefable,
)

_PhysicalType: TypeAlias = Literal[
    "BOOLEAN",
    "INT32",
    "INT64",
    "INT96",
    "FLOAT",
    "DOUBLE",
    "BYTE_ARRAY",
    "FIXED_LEN_BYTE_ARRAY",
    "UNKNOWN",
]
_LogicTypeName: TypeAlias = Literal[
    "UNDEFINED",
    "STRING",
    "MAP",
    "LIST",
    "ENUM",
    "DECIMAL",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "INT",
    "FLOAT16",
    "JSON",
    "BSON",
    "UUID",
    "NONE",
    "UNKNOWN",
]
_ConvertedType: TypeAlias = Literal[
    "NONE",
    "UTF8",
    "MAP",
    "MAP_KEY_VALUE",
    "LIST",
    "ENUM",
    "DECIMAL",
    "DATE",
    "TIME_MILLIS",
    "TIME_MICROS",
    "TIMESTAMP_MILLIS",
    "TIMESTAMP_MICROS",
    "UINT_8",
    "UINT_16",
    "UINT_32",
    "UINT_64",
    "INT_8",
    "INT_16",
    "INT_32",
    "INT_64",
    "JSON",
    "BSON",
    "INTERVAL",
    "UNKNOWN",
]
_Encoding: TypeAlias = Literal[
    "PLAIN",
    "PLAIN_DICTIONARY",
    "RLE",
    "BIT_PACKED",
    "DELTA_BINARY_PACKED",
    "DELTA_LENGTH_BYTE_ARRAY",
    "DELTA_BYTE_ARRAY",
    "RLE_DICTIONARY",
    "BYTE_STREAM_SPLIT",
    "UNKNOWN",
]
_Compression: TypeAlias = Literal[
    "UNCOMPRESSED",
    "SNAPPY",
    "GZIP",
    "LZO",
    "BROTLI",
    "LZ4",
    "ZSTD",
    "UNKNOWN",
]


class _Statistics(TypedDict):
    has_min_max: bool
    min: Any | None
    max: Any | None
    null_count: int | None
    distinct_count: int | None
    num_values: int
    physical_type: _PhysicalType


class Statistics(_Weakrefable):
    def to_dict(self) -> _Statistics: ...
    def equals(self, other: Statistics) -> bool: ...
    @property
    def has_min_max(self) -> bool: ...
    @property
    def has_null_count(self) -> bool: ...
    @property
    def has_distinct_count(self) -> bool: ...
    @property
    def min_raw(self) -> Any | None: ...
    @property
    def max_raw(self) -> Any | None: ...
    @property
    def min(self) -> Any | None: ...
    @property
    def max(self) -> Any | None: ...
    @property
    def null_count(self) -> int | None: ...
    @property
    def distinct_count(self) -> int | None: ...
    @property
    def num_values(self) -> int: ...
    @property
    def physical_type(self) -> _PhysicalType: ...
    @property
    def logical_type(self) -> ParquetLogicalType: ...
    @property
    def converted_type(self) -> _ConvertedType | None: ...
    @property
    def is_min_exact(self) -> bool: ...
    @property
    def is_max_exact(self) -> bool: ...


class ParquetLogicalType(_Weakrefable):
    def to_json(self) -> str: ...
    @property
    def type(self) -> _LogicTypeName: ...


class _ColumnChunkMetaData(TypedDict):
    file_offset: int
    file_path: str | None
    physical_type: _PhysicalType
    num_values: int
    path_in_schema: str
    is_stats_set: bool
    statistics: Statistics | None
    compression: _Compression
    encodings: tuple[_Encoding, ...]
    has_dictionary_page: bool
    dictionary_page_offset: int | None
    data_page_offset: int
    total_compressed_size: int
    total_uncompressed_size: int


class ColumnChunkMetaData(_Weakrefable):
    def to_dict(self) -> _ColumnChunkMetaData: ...
    def equals(self, other: ColumnChunkMetaData) -> bool: ...
    @property
    def file_offset(self) -> int: ...
    @property
    def file_path(self) -> str | None: ...
    @property
    def physical_type(self) -> _PhysicalType: ...
    @property
    def num_values(self) -> int: ...
    @property
    def path_in_schema(self) -> str: ...
    @property
    def is_stats_set(self) -> bool: ...
    @property
    def statistics(self) -> Statistics | None: ...
    @property
    def compression(self) -> _Compression: ...
    @property
    def encodings(self) -> tuple[_Encoding, ...]: ...
    @property
    def has_dictionary_page(self) -> bool: ...
    @property
    def dictionary_page_offset(self) -> int | None: ...
    @property
    def data_page_offset(self) -> int: ...
    @property
    def has_index_page(self) -> bool: ...
    @property
    def index_page_offset(self) -> int: ...
    @property
    def total_compressed_size(self) -> int: ...
    @property
    def total_uncompressed_size(self) -> int: ...
    @property
    def has_offset_index(self) -> bool: ...
    @property
    def has_column_index(self) -> bool: ...
    @property
    def metadata(self) -> dict[bytes, bytes] | None: ...
    @property
    def name(self) -> str: ...
    @property
    def max_definition_level(self) -> int: ...
    @property
    def max_repetition_level(self) -> int: ...
    @property
    def converted_type(self) -> _ConvertedType: ...
    @property
    def logical_type(self) -> ParquetLogicalType: ...


class _SortingColumn(TypedDict):
    column_index: int
    descending: bool
    nulls_first: bool


class SortingColumn:
    def __init__(
        self, column_index: int, descending: bool = False, nulls_first: bool = False
    ) -> None: ...

    @classmethod
    def from_ordering(
        cls,
        schema: Schema,
        sort_keys: Sequence[str] | Sequence[tuple[str, Order]] | Sequence[str | tuple[str, Order]],
        null_placement: Literal["at_start", "at_end"] = "at_end",
    ) -> tuple[SortingColumn, ...]: ...

    @staticmethod
    def to_ordering(
        schema: Schema, sorting_columns: tuple[SortingColumn, ...] | list[SortingColumn]
    ) -> tuple[Sequence[tuple[str, Order]], Literal["at_start", "at_end"]]: ...
    def __hash__(self) -> int: ...
    @property
    def column_index(self) -> int: ...
    @property
    def descending(self) -> bool: ...
    @property
    def nulls_first(self) -> bool: ...
    def to_dict(self) -> _SortingColumn: ...


class _RowGroupMetaData(TypedDict):
    num_columns: int
    num_rows: int
    total_byte_size: int
    columns: list[ColumnChunkMetaData]
    sorting_columns: list[SortingColumn]


class RowGroupMetaData(_Weakrefable):
    def __init__(self, parent: FileMetaData, index: int) -> None: ...
    def equals(self, other: RowGroupMetaData) -> bool: ...
    def column(self, i: int) -> ColumnChunkMetaData: ...
    def to_dict(self) -> _RowGroupMetaData: ...
    @property
    def num_columns(self) -> int: ...
    @property
    def num_rows(self) -> int: ...
    @property
    def total_byte_size(self) -> int: ...
    @property
    def sorting_columns(self) -> list[SortingColumn]: ...


class _FileMetaData(TypedDict):
    created_by: str
    num_columns: int
    num_rows: int
    num_row_groups: int
    format_version: str
    serialized_size: int
    row_groups: list[Any]  # List of row group metadata dictionaries


class FileMetaData(_Weakrefable):
    def __hash__(self) -> int: ...
    def to_dict(self) -> _FileMetaData: ...
    def equals(self, other: FileMetaData) -> bool: ...
    @property
    def schema(self) -> ParquetSchema: ...
    @property
    def serialized_size(self) -> int: ...
    @property
    def num_columns(self) -> int: ...
    @property
    def num_rows(self) -> int: ...
    @property
    def num_row_groups(self) -> int: ...
    @property
    def format_version(self) -> str: ...
    @property
    def created_by(self) -> str: ...
    @property
    def metadata(self) -> dict[bytes, bytes] | None: ...
    def row_group(self, i: int) -> RowGroupMetaData: ...
    def set_file_path(self, path: str) -> None: ...
    def append_row_groups(self, other: FileMetaData) -> None: ...
    def write_metadata_file(self, where: StrPath | Buffer |
                            NativeFile | IO) -> None: ...


class ParquetSchema(_Weakrefable):
    def __init__(self, container: FileMetaData) -> None: ...
    def __getitem__(self, i: int) -> "ColumnSchema": ...
    def __hash__(self) -> int: ...
    def __len__(self) -> int: ...
    @property
    def names(self) -> list[str]: ...
    def to_arrow_schema(self) -> Schema: ...
    def equals(self, other: ParquetSchema) -> bool: ...
    def column(self, i: int) -> "ColumnSchema": ...


class ColumnSchema(_Weakrefable):
    def __init__(self, schema: ParquetSchema, index: int) -> None: ...
    def equals(self, other: ColumnSchema) -> bool: ...
    @property
    def name(self) -> str: ...
    @property
    def path(self) -> str: ...
    @property
    def max_definition_level(self) -> int: ...
    @property
    def max_repetition_level(self) -> int: ...
    @property
    def physical_type(self) -> _PhysicalType: ...
    @property
    def logical_type(self) -> ParquetLogicalType: ...
    @property
    def converted_type(self) -> _ConvertedType | None: ...
    @property
    def length(self) -> int | None: ...
    @property
    def precision(self) -> int | None: ...
    @property
    def scale(self) -> int | None: ...


class ParquetReader(_Weakrefable):
    def __init__(self, memory_pool: MemoryPool | None = None) -> None: ...

    def open(
        self,
        source: StrPath | Buffer | NativeFile | IO,
        *,
        use_memory_map: bool = False,
        read_dictionary: Iterable[int] | Iterable[str] | None = None,
        metadata: FileMetaData | None = None,
        buffer_size: int = 0,
        pre_buffer: bool = False,
        coerce_int96_timestamp_unit: str | None = None,
        decryption_properties: FileDecryptionProperties | None = None,
        thrift_string_size_limit: int | None = None,
        thrift_container_size_limit: int | None = None,
        page_checksum_verification: bool = False,
    ): ...
    @property
    def column_paths(self) -> list[str]: ...
    @property
    def metadata(self) -> FileMetaData: ...
    @property
    def schema_arrow(self) -> Schema: ...
    @property
    def num_row_groups(self) -> int: ...
    def set_use_threads(self, use_threads: bool) -> None: ...
    def set_batch_size(self, batch_size: int) -> None: ...

    def iter_batches(
        self,
        batch_size: int = 65536,
        row_groups: list[int] | range | None = None,
        column_indices: list[str] | list[int] | None = None,
        use_threads: bool = True,
        use_pandas_metadata: bool = False,
    ) -> Iterator[RecordBatch]: ...

    def read_row_group(
        self, i: int, column_indices: list[int] | None = None, use_threads: bool = True
    ) -> Table: ...

    def read_row_groups(
        self,
        row_groups: Sequence[int] | range,
        column_indices: list[str] | list[int] | None = None,
        use_threads: bool = True,
        use_pandas_metadata: bool = False,
    ) -> Table: ...

    def read_all(
        self, column_indices: list[int] | None = None, use_threads: bool = True
    ) -> Table: ...
    def scan_contents(
        self, columns: Sequence[str] | Sequence[int] | None = None, batch_size: int = 65536
    ) -> int: ...

    def column_name_idx(self, column_name: str) -> int: ...
    def read_column(self, column_index: int) -> ChunkedArray: ...
    def close(self) -> None: ...
    @property
    def closed(self) -> bool: ...


class ParquetWriter(_Weakrefable):
    def __init__(
        self,
        where: StrPath | NativeFile | IO,
        schema: Schema,
        use_dictionary: bool | list[str] | None = None,
        compression: _Compression | dict[str, _Compression] | None = None,
        version: str | None = None,
        write_statistics: bool | list[str] | None = None,
        memory_pool: MemoryPool | None = None,
        use_deprecated_int96_timestamps: bool = False,
        coerce_timestamps: Literal["ms", "us"] | None = None,
        data_page_size: int | None = None,
        allow_truncated_timestamps: bool = False,
        compression_level: int | dict[str, int] | None = None,
        use_byte_stream_split: bool | list[str] = False,
        column_encoding: _Encoding | dict[str, _Encoding] | None = None,
        writer_engine_version: str | None = None,
        data_page_version: str | None = None,
        use_compliant_nested_type: bool = True,
        encryption_properties: FileDecryptionProperties | None = None,
        write_batch_size: int | None = None,
        dictionary_pagesize_limit: int | None = None,
        store_schema: bool = True,
        write_page_index: bool = False,
        write_page_checksum: bool = False,
        sorting_columns: tuple[SortingColumn, ...] | None = None,
        store_decimal_as_integer: bool = False,
    ): ...
    def close(self) -> None: ...
    def write_table(self, table: Table, row_group_size: int | None = None) -> None: ...
    def add_key_value_metadata(self, key_value_metadata: KeyValueMetadata) -> None: ...
    @property
    def metadata(self) -> FileMetaData: ...
    @property
    def use_dictionary(self) -> bool | list[str] | None: ...
    @property
    def use_deprecated_int96_timestamps(self) -> bool: ...
    @property
    def use_byte_stream_split(self) -> bool | list[str]: ...
    @property
    def column_encoding(self) -> _Encoding | dict[str, _Encoding] | None: ...
    @property
    def coerce_timestamps(self) -> Literal["ms", "us"] | None: ...
    @property
    def allow_truncated_timestamps(self) -> bool: ...
    @property
    def compression(self) -> _Compression | dict[str, _Compression] | None: ...
    @property
    def compression_level(self) -> int | dict[str, int] | None: ...
    @property
    def data_page_version(self) -> str | None: ...
    @property
    def use_compliant_nested_type(self) -> bool: ...
    @property
    def version(self) -> str | None: ...
    @property
    def write_statistics(self) -> bool | list[str] | None: ...
    @property
    def writer_engine_version(self) -> str: ...
    @property
    def row_group_size(self) -> int: ...
    @property
    def data_page_size(self) -> int: ...
    @property
    def encryption_properties(self) -> FileDecryptionProperties: ...
    @property
    def write_batch_size(self) -> int: ...
    @property
    def dictionary_pagesize_limit(self) -> int: ...
    @property
    def store_schema(self) -> bool: ...
    @property
    def store_decimal_as_integer(self) -> bool: ...


class FileEncryptionProperties:
    ...


class FileDecryptionProperties:
    ...

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

import sys

from pathlib import Path

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
from collections.abc import Callable, Iterator, Sequence
from typing import IO, Literal

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from pyarrow import _parquet
from pyarrow._compute import Expression
from pyarrow._fs import FileSystem, SupportedFileSystem
from pyarrow._parquet import (
    ColumnChunkMetaData,
    ColumnSchema,
    FileDecryptionProperties,
    FileEncryptionProperties,
    FileMetaData,
    ParquetLogicalType,
    ParquetReader,
    ParquetSchema,
    RowGroupMetaData,
    SortingColumn,
    Statistics,
)
from pyarrow._stubs_typing import FilterTuple, SingleOrList
from pyarrow.dataset import ParquetFileFragment, Partitioning
from pyarrow.lib import Buffer, NativeFile, RecordBatch, Schema, Table
from typing_extensions import deprecated

__all__ = (
    "ColumnChunkMetaData",
    "ColumnSchema",
    "FileDecryptionProperties",
    "FileEncryptionProperties",
    "FileMetaData",
    "ParquetDataset",
    "ParquetFile",
    "ParquetLogicalType",
    "ParquetReader",
    "ParquetSchema",
    "ParquetWriter",
    "RowGroupMetaData",
    "SortingColumn",
    "Statistics",
    "read_metadata",
    "read_pandas",
    "read_schema",
    "read_table",
    "write_metadata",
    "write_table",
    "write_to_dataset",
    "_filters_to_expression",
    "filters_to_expression",
)


def filters_to_expression(
    filters: list[FilterTuple | list[FilterTuple]]) -> Expression: ...


@deprecated("use filters_to_expression")
def _filters_to_expression(
    filters: list[FilterTuple | list[FilterTuple]]) -> Expression: ...


_Compression: TypeAlias = Literal["gzip", "bz2",
                                  "brotli", "lz4", "zstd", "snappy", "none"]


class ParquetFile:
    reader: ParquetReader
    common_metadata: FileMetaData

    def __init__(
        self,
        source: str | Path | NativeFile | IO,
        *,
        metadata: FileMetaData | None = None,
        common_metadata: FileMetaData | None = None,
        read_dictionary: list[str] | None = None,
        memory_map: bool = False,
        buffer_size: int = 0,
        pre_buffer: bool = False,
        coerce_int96_timestamp_unit: str | None = None,
        decryption_properties: FileDecryptionProperties | None = None,
        thrift_string_size_limit: int | None = None,
        thrift_container_size_limit: int | None = None,
        filesystem: SupportedFileSystem | None = None,
        page_checksum_verification: bool = False,
    ): ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *args, **kwargs) -> None: ...
    @property
    def metadata(self) -> FileMetaData: ...
    @property
    def schema(self) -> ParquetSchema: ...
    @property
    def schema_arrow(self) -> Schema: ...
    @property
    def num_row_groups(self) -> int: ...
    def close(self, force: bool = False) -> None: ...
    @property
    def closed(self) -> bool: ...

    def read_row_group(
        self,
        i: int,
        columns: list | None = None,
        use_threads: bool = True,
        use_pandas_metadata: bool = False,
    ) -> Table: ...

    def read_row_groups(
        self,
        row_groups: list,
        columns: list | None = None,
        use_threads: bool = True,
        use_pandas_metadata: bool = False,
    ) -> Table: ...

    def iter_batches(
        self,
        batch_size: int = 65536,
        row_groups: list | None = None,
        columns: list | None = None,
        use_threads: bool = True,
        use_pandas_metadata: bool = False,
    ) -> Iterator[RecordBatch]: ...

    def read(
        self,
        columns: list | None = None,
        use_threads: bool = True,
        use_pandas_metadata: bool = False,
    ) -> Table: ...
    def scan_contents(self, columns: list | None = None,
                      batch_size: int = 65536) -> int: ...


class ParquetWriter:
    flavor: str
    schema_changed: bool
    schema: ParquetSchema
    where: str | Path | IO
    file_handler: NativeFile | None
    writer: _parquet.ParquetWriter
    is_open: bool

    def __init__(
        self,
        where: str | Path | IO | NativeFile,
        schema: Schema,
        filesystem: SupportedFileSystem | None = None,
        flavor: str | None = None,
        version: Literal["1.0", "2.4", "2.6"] = ...,
        use_dictionary: bool = True,
        compression: _Compression | dict[str, _Compression] = "snappy",
        write_statistics: bool | list = True,
        use_deprecated_int96_timestamps: bool | None = None,
        compression_level: int | dict | None = None,
        use_byte_stream_split: bool | list = False,
        column_encoding: str | dict | None = None,
        writer_engine_version=None,
        data_page_version: Literal["1.0", "2.0"] = ...,
        use_compliant_nested_type: bool = True,
        encryption_properties: FileEncryptionProperties | None = None,
        write_batch_size: int | None = None,
        dictionary_pagesize_limit: int | None = None,
        store_schema: bool = True,
        write_page_index: bool = False,
        write_page_checksum: bool = False,
        sorting_columns: Sequence[SortingColumn] | None = None,
        store_decimal_as_integer: bool = False,
        **options,
    ) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *args, **kwargs) -> Literal[False]: ...

    def write(
        self, table_or_batch: RecordBatch | Table, row_group_size: int | None = None
    ) -> None: ...
    def write_batch(self, batch: RecordBatch,
                    row_group_size: int | None = None) -> None: ...

    def write_table(self, table: Table, row_group_size: int | None = None) -> None: ...
    def close(self) -> None: ...
    def add_key_value_metadata(self, key_value_metadata: dict[str, str]) -> None: ...


class ParquetDataset:
    def __init__(
        self,
        path_or_paths: SingleOrList[str]
        | SingleOrList[Path]
        | SingleOrList[NativeFile]
        | SingleOrList[IO],
        filesystem: SupportedFileSystem | None = None,
        schema: Schema | None = None,
        *,
        filters: Expression | FilterTuple | list[FilterTuple] | None = None,
        read_dictionary: list[str] | None = None,
        memory_map: bool = False,
        buffer_size: int = 0,
        partitioning: str | list[str] | Partitioning | None = "hive",
        ignore_prefixes: list[str] | None = None,
        pre_buffer: bool = True,
        coerce_int96_timestamp_unit: str | None = None,
        decryption_properties: FileDecryptionProperties | None = None,
        thrift_string_size_limit: int | None = None,
        thrift_container_size_limit: int | None = None,
        page_checksum_verification: bool = False,
    ): ...
    def equals(self, other: ParquetDataset) -> bool: ...
    @property
    def schema(self) -> Schema: ...

    def read(
        self,
        columns: list[str] | None = None,
        use_threads: bool = True,
        use_pandas_metadata: bool = False,
    ) -> Table: ...
    def read_pandas(self, **kwargs) -> Table: ...
    @property
    def fragments(self) -> list[ParquetFileFragment]: ...
    @property
    def files(self) -> list[str]: ...
    @property
    def filesystem(self) -> FileSystem: ...
    @property
    def partitioning(self) -> Partitioning: ...


def read_table(
    source: SingleOrList[str]
    | SingleOrList[Path] | SingleOrList[NativeFile] | SingleOrList[IO] | Buffer,
    *,
    columns: list | None = None,
    use_threads: bool = True,
    schema: Schema | None = None,
    use_pandas_metadata: bool = False,
    read_dictionary: list[str] | None = None,
    memory_map: bool = False,
    buffer_size: int = 0,
    partitioning: str | list[str] | Partitioning | None = "hive",
    filesystem: SupportedFileSystem | None = None,
    filters: Expression | FilterTuple | list[FilterTuple] | None = None,
    ignore_prefixes: list[str] | None = None,
    pre_buffer: bool = True,
    coerce_int96_timestamp_unit: str | None = None,
    decryption_properties: FileDecryptionProperties | None = None,
    thrift_string_size_limit: int | None = None,
    thrift_container_size_limit: int | None = None,
    page_checksum_verification: bool = False,
) -> Table: ...


def read_pandas(
    source: str | Path | NativeFile | IO | Buffer, columns: list | None = None, **kwargs
) -> Table: ...


def write_table(
    table: Table,
    where: str | Path | NativeFile | IO,
    row_group_size: int | None = None,
    version: Literal["1.0", "2.4", "2.6"] = "2.6",
    use_dictionary: bool = True,
    compression: _Compression | dict[str, _Compression] = "snappy",
    write_statistics: bool | list = True,
    use_deprecated_int96_timestamps: bool | None = None,
    coerce_timestamps: str | None = None,
    allow_truncated_timestamps: bool = False,
    data_page_size: int | None = None,
    flavor: str | None = None,
    filesystem: SupportedFileSystem | None = None,
    compression_level: int | dict | None = None,
    use_byte_stream_split: bool = False,
    column_encoding: str | dict | None = None,
    data_page_version: Literal["1.0", "2.0"] = ...,
    use_compliant_nested_type: bool = True,
    encryption_properties: FileEncryptionProperties | None = None,
    write_batch_size: int | None = None,
    dictionary_pagesize_limit: int | None = None,
    store_schema: bool = True,
    write_page_index: bool = False,
    write_page_checksum: bool = False,
    sorting_columns: Sequence[SortingColumn] | None = None,
    store_decimal_as_integer: bool = False,
    **kwargs,
) -> None: ...


def write_to_dataset(
    table: Table,
    root_path: str | Path,
    partition_cols: list[str] | None = None,
    filesystem: SupportedFileSystem | None = None,
    schema: Schema | None = None,
    partitioning: Partitioning | list[str] | None = None,
    basename_template: str | None = None,
    use_threads: bool | None = None,
    file_visitor: Callable[[str], None] | None = None,
    existing_data_behavior: Literal["overwrite_or_ignore", "error", "delete_matching"]
    | None = None,
    **kwargs,
) -> None: ...


def write_metadata(
    schema: Schema,
    where: str | NativeFile,
    metadata_collector: list[FileMetaData] | None = None,
    filesystem: SupportedFileSystem | None = None,
    **kwargs,
) -> None: ...


def read_metadata(
    where: str | Path | IO | NativeFile,
    memory_map: bool = False,
    decryption_properties: FileDecryptionProperties | None = None,
    filesystem: SupportedFileSystem | None = None,
) -> FileMetaData: ...


def read_schema(
    where: str | Path | IO | NativeFile,
    memory_map: bool = False,
    decryption_properties: FileDecryptionProperties | None = None,
    filesystem: SupportedFileSystem | None = None,
) -> Schema: ...

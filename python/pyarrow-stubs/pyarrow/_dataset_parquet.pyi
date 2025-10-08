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

from collections.abc import Iterable
from dataclasses import dataclass
from typing import IO, Any, TypedDict

from _typeshed import StrPath

from ._compute import Expression
from ._dataset import (
    DatasetFactory,
    FileFormat,
    FileFragment,
    FileWriteOptions,
    Fragment,
    FragmentScanOptions,
    Partitioning,
    PartitioningFactory,
)
from ._dataset_parquet_encryption import ParquetDecryptionConfig
from ._fs import SupportedFileSystem
from ._parquet import FileDecryptionProperties, FileMetaData
from .lib import CacheOptions, Schema, _Weakrefable

parquet_encryption_enabled: bool


class ParquetFileFormat(FileFormat):

    def __init__(
        self,
        read_options: ParquetReadOptions | None = None,
        default_fragment_scan_options: ParquetFragmentScanOptions | None = None,
        **kwargs,
    ) -> None: ...
    @property
    def read_options(self) -> ParquetReadOptions: ...
    def make_write_options(
        self) -> ParquetFileWriteOptions: ...  # type: ignore[override]

    def equals(self, other: ParquetFileFormat) -> bool: ...
    @property
    def default_extname(self) -> str: ...

    def make_fragment(
        self,
        file: StrPath | IO,
        filesystem: SupportedFileSystem | None = None,
        partition_expression: Expression | None = None,
        row_groups: Iterable[int] | None = None,
        *,
        file_size: int | None = None,
    ) -> Fragment: ...


class _NameStats(TypedDict):
    min: Any
    max: Any


class RowGroupInfo:

    id: int
    metadata: FileMetaData
    schema: Schema

    def __init__(self, id: int, metadata: FileMetaData, schema: Schema) -> None: ...
    @property
    def num_rows(self) -> int: ...
    @property
    def total_byte_size(self) -> int: ...
    @property
    def statistics(self) -> dict[str, _NameStats]: ...


class ParquetFileFragment(FileFragment):

    def ensure_complete_metadata(self) -> None: ...
    @property
    def row_groups(self) -> list[RowGroupInfo]: ...
    @property
    def metadata(self) -> FileMetaData: ...
    @property
    def num_row_groups(self) -> int: ...

    def split_by_row_group(
        self, filter: Expression | None = None, schema: Schema | None = None
    ) -> list[Fragment]: ...

    def subset(
        self,
        filter: Expression | None = None,
        schema: Schema | None = None,
        row_group_ids: list[int] | None = None,
    ) -> ParquetFileFormat: ...


class ParquetReadOptions(_Weakrefable):

    def __init__(
        self,
        dictionary_columns: list[str] | None,
        coerce_int96_timestamp_unit: str | None = None) -> None: ...

    @property
    def coerce_int96_timestamp_unit(self) -> str: ...
    @coerce_int96_timestamp_unit.setter
    def coerce_int96_timestamp_unit(self, unit: str) -> None: ...
    def equals(self, other: ParquetReadOptions) -> bool: ...


class ParquetFileWriteOptions(FileWriteOptions):
    def update(self, **kwargs) -> None: ...
    def _set_properties(self) -> None: ...
    def _set_arrow_properties(self) -> None: ...
    def _set_encryption_config(self) -> None: ...


@dataclass(kw_only=True)
class ParquetFragmentScanOptions(FragmentScanOptions):

    use_buffered_stream: bool = False
    buffer_size: int = 8192
    pre_buffer: bool = True
    cache_options: CacheOptions | None = None
    thrift_string_size_limit: int | None = None
    thrift_container_size_limit: int | None = None
    decryption_config: ParquetDecryptionConfig | None = None
    decryption_properties: FileDecryptionProperties | None = None
    page_checksum_verification: bool = False

    def equals(self, other: ParquetFragmentScanOptions) -> bool: ...


@dataclass
class ParquetFactoryOptions(_Weakrefable):

    partition_base_dir: str | None = None
    partitioning: Partitioning | PartitioningFactory | None = None
    validate_column_chunk_paths: bool = False


class ParquetDatasetFactory(DatasetFactory):

    def __init__(
        self,
        metadata_path: str,
        filesystem: SupportedFileSystem,
        format: FileFormat,
        options: ParquetFactoryOptions | None = None,
    ) -> None: ...

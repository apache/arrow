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

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
from collections.abc import Callable, Iterator
from typing import (
    IO,
    Any,
    Generic,
    Literal,
    NamedTuple,
    TypeVar,
)

from _typeshed import StrPath

from . import csv, _json, _parquet, lib
from ._fs import FileSelector, FileSystem, SupportedFileSystem
from ._stubs_typing import Indices, JoinType, Order
from .acero import ExecNodeOptions
from .compute import Expression
from .ipc import IpcWriteOptions, RecordBatchReader


class Dataset(lib._Weakrefable):

    @property
    def partition_expression(self) -> Expression: ...

    def replace_schema(self, schema: lib.Schema) -> Self: ...

    def get_fragments(self, filter: Expression | None = None): ...

    def scanner(
        self,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> Scanner: ...

    def to_batches(
        self,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> Iterator[lib.RecordBatch]: ...

    def to_table(
        self,
        columns: list[str] | dict[str, Expression] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> lib.Table: ...

    def take(
        self,
        indices: Indices,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> lib.Table: ...

    def head(
        self,
        num_rows: int,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> lib.Table: ...

    def count_rows(
        self,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> int: ...

    @property
    def schema(self) -> lib.Schema: ...

    def filter(self, expression: Expression) -> Self: ...

    def sort_by(self, sorting: str |
                list[tuple[str, Order]], **kwargs) -> InMemoryDataset: ...

    def join(
        self,
        right_dataset: Dataset,
        keys: str | list[str],
        right_keys: str | list[str] | None = None,
        join_type: JoinType = "left outer",
        left_suffix: str | None = None,
        right_suffix: str | None = None,
        coalesce_keys: bool = True,
        use_threads: bool = True,
    ) -> InMemoryDataset: ...

    def join_asof(
        self,
        right_dataset: Dataset,
        on: str,
        by: str | list[str],
        tolerance: int,
        right_on: str | list[str] | None = None,
        right_by: str | list[str] | None = None,
    ) -> InMemoryDataset: ...


class InMemoryDataset(Dataset):
    ...


class UnionDataset(Dataset):

    @property
    def children(self) -> list[Dataset]: ...


class FileSystemDataset(Dataset):

    def __init__(
        self,
        fragments: list[Fragment],
        schema: lib.Schema,
        format: FileFormat,
        filesystem: SupportedFileSystem | None = None,
        root_partition: Expression | None = None,
    ) -> None: ...

    @classmethod
    def from_paths(
        cls,
        paths: list[str],
        schema: lib.Schema | None = None,
        format: FileFormat | None = None,
        filesystem: SupportedFileSystem | None = None,
        partitions: list[Expression] | None = None,
        root_partition: Expression | None = None,
    ) -> FileSystemDataset: ...

    @property
    def filesystem(self) -> FileSystem: ...
    @property
    def partitioning(self) -> Partitioning | None: ...

    @property
    def files(self) -> list[str]: ...

    @property
    def format(self) -> FileFormat: ...


class FileWriteOptions(lib._Weakrefable):
    @property
    def format(self) -> FileFormat: ...


class FileFormat(lib._Weakrefable):
    def inspect(
        self, file: StrPath | IO, filesystem: SupportedFileSystem | None = None
    ) -> lib.Schema: ...

    def make_fragment(
        self,
        file: StrPath | IO,
        filesystem: SupportedFileSystem | None = None,
        partition_expression: Expression | None = None,
        *,
        file_size: int | None = None,
    ) -> Fragment: ...

    def make_write_options(self) -> FileWriteOptions: ...
    @property
    def default_extname(self) -> str: ...
    @property
    def default_fragment_scan_options(self) -> FragmentScanOptions: ...
    @default_fragment_scan_options.setter
    def default_fragment_scan_options(self, options: FragmentScanOptions) -> None: ...


class Fragment(lib._Weakrefable):

    @property
    def filesystem(self) -> SupportedFileSystem: ...

    @property
    def physical_schema(self) -> lib.Schema: ...

    @property
    def partition_expression(self) -> Expression: ...

    def scanner(
        self,
        schema: lib.Schema | None = None,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> Scanner: ...

    def to_batches(
        self,
        schema: lib.Schema | None = None,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> Iterator[lib.RecordBatch]: ...

    def to_table(
        self,
        schema: lib.Schema | None = None,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> lib.Table: ...

    def take(
        self,
        indices: Indices,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> lib.Table: ...

    def head(
        self,
        num_rows: int,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> lib.Table: ...

    def count_rows(
        self,
        columns: list[str] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> int: ...


class FileFragment(Fragment):

    def open(self) -> lib.NativeFile: ...

    @property
    def path(self) -> str: ...

    @property
    def filesystem(self) -> FileSystem: ...

    @property
    def buffer(self) -> lib.Buffer: ...

    @property
    def format(self) -> FileFormat: ...


class FragmentScanOptions(lib._Weakrefable):

    @property
    def type_name(self) -> str: ...


class IpcFileWriteOptions(FileWriteOptions):
    @property
    def write_options(self) -> IpcWriteOptions: ...
    @write_options.setter
    def write_options(self, write_options: IpcWriteOptions) -> None: ...


class IpcFileFormat(FileFormat):
    def equals(self, other: IpcFileFormat) -> bool: ...
    def make_write_options(self, **kwargs) -> IpcFileWriteOptions: ...
    @property
    def default_extname(self) -> str: ...


class FeatherFileFormat(IpcFileFormat):
    ...


class CsvFileFormat(FileFormat):

    def __init__(
        self,
        parse_options: csv.ParseOptions | None = None,
        default_fragment_scan_options: CsvFragmentScanOptions | None = None,
        convert_options: csv.ConvertOptions | None = None,
        read_options: csv.ReadOptions | None = None,
    ) -> None: ...
    def make_write_options(self) -> csv.WriteOptions: ...  # type: ignore[override]
    @property
    def parse_options(self) -> csv.ParseOptions: ...
    @parse_options.setter
    def parse_options(self, parse_options: csv.ParseOptions) -> None: ...
    def equals(self, other: CsvFileFormat) -> bool: ...


class CsvFragmentScanOptions(FragmentScanOptions):

    convert_options: csv.ConvertOptions
    read_options: csv.ReadOptions

    def __init__(
        self, convert_options: csv.ConvertOptions, read_options: csv.ReadOptions
    ) -> None: ...
    def equals(self, other: CsvFragmentScanOptions) -> bool: ...


class CsvFileWriteOptions(FileWriteOptions):
    write_options: csv.WriteOptions


class JsonFileFormat(FileFormat):

    def __init__(
        self,
        default_fragment_scan_options: JsonFragmentScanOptions | None = None,
        parse_options: _json.ParseOptions | None = None,
        read_options: _json.ReadOptions | None = None,
    ) -> None: ...
    def equals(self, other: JsonFileFormat) -> bool: ...


class JsonFragmentScanOptions(FragmentScanOptions):

    parse_options: _json.ParseOptions
    read_options: _json.ReadOptions

    def __init__(
        self, parse_options: _json.ParseOptions, read_options: _json.ReadOptions
    ) -> None: ...
    def equals(self, other: JsonFragmentScanOptions) -> bool: ...


class Partitioning(lib._Weakrefable):
    def parse(self, path: str) -> Expression: ...

    def format(self, expr: Expression) -> tuple[str, str]: ...

    @property
    def schema(self) -> lib.Schema: ...


class PartitioningFactory(lib._Weakrefable):
    @property
    def type_name(self) -> str: ...


class KeyValuePartitioning(Partitioning):
    @property
    def dictionaries(self) -> list[lib.Array | None]: ...


class DirectoryPartitioning(KeyValuePartitioning):

    @staticmethod
    def discover(
        field_names: list[str] | None = None,
        infer_dictionary: bool = False,
        max_partition_dictionary_size: int = 0,
        schema: lib.Schema | None = None,
        segment_encoding: Literal["uri", "none"] = "uri",
    ) -> PartitioningFactory: ...

    def __init__(
        self,
        schema: lib.Schema,
        dictionaries: dict[str, lib.Array] | None = None,
        segment_encoding: Literal["uri", "none"] = "uri",
    ) -> None: ...


class HivePartitioning(KeyValuePartitioning):

    def __init__(
        self,
        schema: lib.Schema,
        dictionaries: dict[str, lib.Array] | None = None,
        null_fallback: str = "__HIVE_DEFAULT_PARTITION__",
        segment_encoding: Literal["uri", "none"] = "uri",
    ) -> None: ...

    @staticmethod
    def discover(
        infer_dictionary: bool = False,
        max_partition_dictionary_size: int = 0,
        null_fallback="__HIVE_DEFAULT_PARTITION__",
        schema: lib.Schema | None = None,
        segment_encoding: Literal["uri", "none"] = "uri",
    ) -> PartitioningFactory: ...


class FilenamePartitioning(KeyValuePartitioning):

    def __init__(
        self,
        schema: lib.Schema,
        dictionaries: dict[str, lib.Array] | None = None,
        segment_encoding: Literal["uri", "none"] = "uri",
    ) -> None: ...

    @staticmethod
    def discover(
        field_names: list[str] | None = None,
        infer_dictionary: bool = False,
        schema: lib.Schema | None = None,
        segment_encoding: Literal["uri", "none"] = "uri",
    ) -> PartitioningFactory: ...


class DatasetFactory(lib._Weakrefable):

    root_partition: Expression
    def finish(self, schema: lib.Schema | None = None) -> Dataset: ...

    def inspect(self) -> lib.Schema: ...

    def inspect_schemas(self) -> list[lib.Schema]: ...


class FileSystemFactoryOptions(lib._Weakrefable):

    partitioning: Partitioning
    partitioning_factory: PartitioningFactory
    partition_base_dir: str
    exclude_invalid_files: bool
    selector_ignore_prefixes: list[str]

    def __init__(
        self,
        artition_base_dir: str | None = None,
        partitioning: Partitioning | PartitioningFactory | None = None,
        exclude_invalid_files: bool = True,
        selector_ignore_prefixes: list[str] | None = None,
    ) -> None: ...


class FileSystemDatasetFactory(DatasetFactory):

    def __init__(
        self,
        filesystem: SupportedFileSystem,
        paths_or_selector: FileSelector,
        format: FileFormat,
        options: FileSystemFactoryOptions | None = None,
    ) -> None: ...


class UnionDatasetFactory(DatasetFactory):

    def __init__(self, factories: list[DatasetFactory]) -> None: ...


_RecordBatchT = TypeVar("_RecordBatchT", bound=lib.RecordBatch)


class RecordBatchIterator(lib._Weakrefable, Generic[_RecordBatchT]):

    def __iter__(self) -> Self: ...
    def __next__(self) -> _RecordBatchT: ...


class TaggedRecordBatch(NamedTuple):

    record_batch: lib.RecordBatch
    fragment: Fragment


class TaggedRecordBatchIterator(lib._Weakrefable):

    def __iter__(self) -> Self: ...
    def __next__(self) -> TaggedRecordBatch: ...


class Scanner(lib._Weakrefable):

    @staticmethod
    def from_dataset(
        dataset: Dataset,
        *,
        columns: list[str] | dict[str, Expression] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> Scanner: ...

    @staticmethod
    def from_fragment(
        fragment: Fragment,
        *,
        schema: lib.Schema | None = None,
        columns: list[str] | dict[str, Expression] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> Scanner: ...

    @staticmethod
    def from_batches(
        source: Iterator[lib.RecordBatch] | RecordBatchReader,
        *,
        schema: lib.Schema | None = None,
        columns: list[str] | dict[str, Expression] | None = None,
        filter: Expression | None = None,
        batch_size: int = ...,
        batch_readahead: int = 16,
        fragment_readahead: int = 4,
        fragment_scan_options: FragmentScanOptions | None = None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        memory_pool: lib.MemoryPool | None = None,
    ) -> Scanner: ...

    @property
    def dataset_schema(self) -> lib.Schema: ...

    @property
    def projected_schema(self) -> lib.Schema: ...

    def to_batches(self) -> Iterator[lib.RecordBatch]: ...

    def scan_batches(self) -> TaggedRecordBatchIterator: ...

    def to_table(self) -> lib.Table: ...

    def take(self, indices: Indices) -> lib.Table: ...

    def head(self, num_rows: int) -> lib.Table: ...

    def count_rows(self) -> int: ...

    def to_reader(self) -> RecordBatchReader: ...


def get_partition_keys(partition_expression: Expression) -> dict[str, Any]: ...


class WrittenFile(lib._Weakrefable):

    def __init__(self, path: str, metadata: _parquet.FileMetaData |
                 None, size: int) -> None: ...


def _filesystemdataset_write(
    data: Scanner,
    base_dir: StrPath,
    basename_template: str,
    filesystem: SupportedFileSystem,
    partitioning: Partitioning,
    file_options: FileWriteOptions,
    max_partitions: int,
    file_visitor: Callable[[str], None],
    existing_data_behavior: Literal["error", "overwrite_or_ignore", "delete_matching"],
    max_open_files: int,
    max_rows_per_file: int,
    min_rows_per_group: int,
    max_rows_per_group: int,
    create_dir: bool,
): ...


class _ScanNodeOptions(ExecNodeOptions):
    def _set_options(self, dataset: Dataset, scan_options: dict) -> None: ...


class ScanNodeOptions(_ScanNodeOptions):

    def __init__(
        self, dataset: Dataset, require_sequenced_output: bool = False, **kwargs
    ) -> None: ...

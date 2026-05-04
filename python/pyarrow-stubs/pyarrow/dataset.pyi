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

from collections.abc import Callable, Iterable, Sequence
from typing import Literal, TypeAlias, Any

from _typeshed import StrPath
from pyarrow._dataset import (
    CsvFileFormat,
    CsvFragmentScanOptions,
    Dataset,
    DatasetFactory,
    DirectoryPartitioning,
    FeatherFileFormat,
    FileFormat,
    FileFragment,
    FilenamePartitioning,
    FileSystemDataset,
    FileSystemDatasetFactory,
    FileSystemFactoryOptions,
    FileWriteOptions,
    Fragment,
    FragmentScanOptions,
    HivePartitioning,
    InMemoryDataset,
    IpcFileFormat,
    IpcFileWriteOptions,
    JsonFileFormat,
    JsonFragmentScanOptions,
    Partitioning,
    PartitioningFactory,
    Scanner,
    TaggedRecordBatch,
    UnionDataset,
    UnionDatasetFactory,
    WrittenFile,
    get_partition_keys,
)
from pyarrow._dataset_orc import OrcFileFormat
from pyarrow._dataset_parquet import (
    ParquetDatasetFactory,
    ParquetFactoryOptions,
    ParquetFileFormat,
    ParquetFileFragment,
    ParquetFileWriteOptions,
    ParquetFragmentScanOptions,
    ParquetReadOptions,
    RowGroupInfo,
)
from pyarrow._dataset_parquet_encryption import (
    ParquetDecryptionConfig,
    ParquetEncryptionConfig,
)
from pyarrow.compute import Expression, field, scalar
from pyarrow.lib import Array, RecordBatch, RecordBatchReader, Schema, Table

from ._fs import SupportedFileSystem

_orc_available: bool
_parquet_available: bool

__all__ = [
    "CsvFileFormat",
    "CsvFragmentScanOptions",
    "Dataset",
    "DatasetFactory",
    "DirectoryPartitioning",
    "FeatherFileFormat",
    "FileFormat",
    "FileFragment",
    "FilenamePartitioning",
    "FileSystemDataset",
    "FileSystemDatasetFactory",
    "FileSystemFactoryOptions",
    "FileWriteOptions",
    "Fragment",
    "FragmentScanOptions",
    "HivePartitioning",
    "InMemoryDataset",
    "IpcFileFormat",
    "IpcFileWriteOptions",
    "JsonFileFormat",
    "JsonFragmentScanOptions",
    "Partitioning",
    "PartitioningFactory",
    "Scanner",
    "TaggedRecordBatch",
    "UnionDataset",
    "UnionDatasetFactory",
    "WrittenFile",
    "get_partition_keys",
    # Orc
    "OrcFileFormat",
    # Parquet
    "ParquetDatasetFactory",
    "ParquetFactoryOptions",
    "ParquetFileFormat",
    "ParquetFileFragment",
    "ParquetFileWriteOptions",
    "ParquetFragmentScanOptions",
    "ParquetReadOptions",
    "RowGroupInfo",
    # Parquet Encryption
    "ParquetDecryptionConfig",
    "ParquetEncryptionConfig",
    # Compute
    "Expression",
    "field",
    "scalar",
    # Dataset
    "partitioning",
    "parquet_dataset",
    "write_dataset",
]

_DatasetFormat: TypeAlias = (
    Literal["parquet", "ipc", "arrow", "feather", "csv", "json", "orc", str]
)


def partitioning(
    schema: Schema = None,
    *,
    field_names: list[str] = None,
    flavor: Literal["hive"] = None,
    dictionaries: dict[str, Array] | Literal["infer"] | None = None,
) -> Partitioning | PartitioningFactory: ...


def parquet_dataset(
    metadata_path: StrPath,
    schema: Schema | None = None,
    filesystem: SupportedFileSystem | None = None,
    format: ParquetFileFormat | None = None,
    partitioning: Partitioning | PartitioningFactory | str | None = None,
    partition_base_dir: str | None = None,
) -> FileSystemDataset: ...


def dataset(
    source: StrPath
    | Sequence[Dataset]
    | Sequence[StrPath]
    | Iterable[RecordBatch]
    | Iterable[Table]
    | RecordBatchReader
    | RecordBatch
    | Table,
    schema: Schema | None = None,
    format: FileFormat | _DatasetFormat | None = None,
    filesystem: SupportedFileSystem | str | None = None,
    partitioning: Partitioning | PartitioningFactory | str | list[str] | None = None,
    partition_base_dir: str | None = None,
    exclude_invalid_files: bool | None = None,
    ignore_prefixes: list[str] | None = None,
) -> FileSystemDataset | UnionDataset | InMemoryDataset | Dataset: ...


def write_dataset(
    data: Any | Dataset | Table | RecordBatch | RecordBatchReader | list[Table]
    | Iterable[RecordBatch] | Scanner,
    base_dir: StrPath,
    *,
    basename_template: str | None = None,
    format: FileFormat | _DatasetFormat | None = None,
    partitioning: Partitioning | PartitioningFactory | list[str] | None = None,
    partitioning_flavor: str | None = None,
    schema: Schema | None = None,
    filesystem: SupportedFileSystem | str | None = None,
    file_options: FileWriteOptions | None = None,
    use_threads: bool | None = True,
    max_partitions: int = 1024,
    max_open_files: int = 1024,
    max_rows_per_file: int = 0,
    min_rows_per_group: int = 0,
    max_rows_per_group: int = 1024 * 1024,  # noqa: Y011
    file_visitor: Callable[[str], None] | None = None,
    existing_data_behavior:
    Literal["error", "overwrite_or_ignore", "delete_matching"] = "error",
    create_dir: bool = True,
    preserve_order: bool | None = None,
): ...


def _get_partition_keys(partition_expression: Expression) -> dict[str, Any]: ...

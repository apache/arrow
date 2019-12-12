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

"""Dataset is currently unstable. APIs subject to change without notice."""

from __future__ import absolute_import

import sys

if sys.version_info < (3,):
    raise ImportError("Python Dataset bindings require Python 3")

from pyarrow._dataset import (  # noqa
    AndExpression,
    CastExpression,
    CompareOperator,
    ComparisonExpression,
    Dataset,
    DefaultPartitioning,
    DirectoryPartitioning,
    Expression,
    FieldExpression,
    FileFormat,
    FileSystemSource,
    FileSystemSourceFactory,
    FileSystemFactoryOptions,
    HivePartitioning,
    InExpression,
    IsValidExpression,
    NotExpression,
    OrExpression,
    ParquetFileFormat,
    Partitioning,
    PartitioningFactory,
    ScalarExpression,
    Scanner,
    ScannerBuilder,
    ScanTask,
    Source,
    TreeSource,
)


def open_dataset(path_or_paths, filesystem=None, partition_scheme=None,
                 format="parquet"):
    """
    Open a multi-file dataset.

    Parameters
    ----------
    path_or_paths : str or list of str
        String path to a directory containing the data files.
    filesystem : FileSystem, default None
        By default will be inferred from the path.
    partition_scheme : PartitionScheme or Schema
        If a Schema is passed, it is interpreted as a HivePartitionScheme.
    format : str
        Currently only "parquet" is supported.

    Returns
    -------
    Dataset

    """
    import pyarrow
    from pyarrow.fs import LocalFileSystem, FileType, FileSelector

    if filesystem is None:
        # TODO handle other file systems
        filesystem = LocalFileSystem()

    if isinstance(path_or_paths, str):
        path = filesystem.get_target_stats([path_or_paths])[0]
        if path.type == FileType.Directory:
            # for directory, pass a selector
            path_or_paths = FileSelector(path_or_paths, recursive=True)
        else:
            # path_or_paths is a single file path, pass it as a list
            path_or_paths = [path_or_paths]

    if format == "parquet":
        format = ParquetFileFormat()
    elif not isinstance(format, FileFormat):
        raise ValueError("format '{0}' is not supported".format(format))

    # TODO pass through options
    options = FileSystemDiscoveryOptions()
    discovery = FileSystemDataSourceDiscovery(
        filesystem, path_or_paths, format, options)

    if partition_scheme is not None:
        if isinstance(partition_scheme, pyarrow.Schema):
            partition_scheme = HivePartitionScheme(partition_scheme)
        discovery.partition_scheme = partition_scheme

    inspected_schema = discovery.inspect()
    return Dataset([discovery.finish()], inspected_schema)

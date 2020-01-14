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

from pyarrow.util import _stringify_path


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


def open_dataset(sources, filesystem=None, partition_scheme=None,
                 format="parquet"):
    """
    Open a multi-file dataset.

    Parameters
    ----------
    sources : str, pathlib.Path, or list of those
        Path to a directory containing the data files, or a list of paths.
    filesystem : FileSystem, default None
        By default will be inferred from the path. Currently, only the local
        file system is supported.
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

    if isinstance(sources, list):
        sources = [_stringify_path(path) for path in sources]
    else:
        sources = _stringify_path(sources)

    if isinstance(sources, str):
        path = filesystem.get_target_stats([sources])[0]
        if path.type == FileType.Directory:
            # for directory, pass a selector
            sources = FileSelector(sources, recursive=True)
        else:
            # sources is a single file path, pass it as a list
            sources = [sources]

    if format == "parquet":
        format = ParquetFileFormat()
    elif not isinstance(format, FileFormat):
        raise ValueError("format '{0}' is not supported".format(format))

    # TODO pass through options
    options = FileSystemDiscoveryOptions()

    if partition_scheme is not None:
        if isinstance(partition_scheme, pyarrow.Schema):
            partition_scheme = HivePartitionScheme(partition_scheme)
        options.partition_scheme = partition_scheme

    discovery = FileSystemDataSourceDiscovery(
        filesystem, sources, format, options)

    inspected_schema = discovery.inspect()
    return Dataset([discovery.finish()], inspected_schema)

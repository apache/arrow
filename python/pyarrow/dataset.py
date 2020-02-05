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

import pyarrow as pa
from pyarrow.util import _stringify_path, _is_path_like

if sys.version_info < (3,):
    raise ImportError("Python Dataset bindings require Python 3")

from pyarrow._dataset import (  # noqa
    AndExpression,
    CastExpression,
    CompareOperator,
    ComparisonExpression,
    Dataset,
    DatasetFactory,
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
    ScanTask,
    Source,
    TreeSource,
    SourceFactory
)


def partitioning(schema=None, field_names=None, flavor=None):
    """
    Specify a partitioning scheme.

    The supported schemes include:

    - "DirectoryPartitioning": this scheme expects one segment in the file path
      for each field in the specified schema (all fields are required to be
      present). For example given schema<year:int16, month:int8> the path
      "/2009/11" would be parsed to ("year"_ == 2009 and "month"_ == 11).
    - "HivePartitioning": a scheme for "/$key=$value/" nested directories as
      found in Apache Hive. This is a multi-level, directory based partitioning
      scheme. Data is partitioned by static values of a particular column in
      the schema. Partition keys are represented in the form $key=$value in
      directory names. Field order is ignored, as are missing or unrecognized
      field names.
      For example, given schema<year:int16, month:int8, day:int8>, a possible
      path would be "/year=2009/month=11/day=15" (but the field order does not
      need to match).

    Parameters
    ----------
    schema : pyarrow.Schema, default None
        The schema that describes the partitions present in the file path.
        If not specified, and `field_names` and/or `flavor` are specified,
        the schema will be inferred from the file path (and a
        PartitioningFactory is returned).
    field_names :  list of str, default None
        A list of strings (field names). If specified, the schema's types are
        inferred from the file paths (only valid for DirectoryPartitioning).
    flavor : str, default None
        The default is DirectoryPartitioning. Specify ``flavor="hive"`` for
        a HivePartitioning.

    Returns
    -------
    Partitioning or PartitioningFactory

    Examples
    --------

    Specify the Schema for paths like "/2009/June":

    >>> partitioning(pa.schema([("year", pa.int16()), ("month", pa.string())]))

    or let the types be inferred by only specifying the field names:

    >>> partitioning(field_names=["year", "month"])

    For paths like "/2009/June", the year will be inferred as int32 while month
    will be inferred as string.

    Create a Hive scheme for a path like "/year=2009/month=11":

    >>> partitioning(
    ...     pa.schema([("year", pa.int16()), ("month", pa.int8())]),
    ...     flavor="hive")

    A Hive scheme can also be discovered from the directory structure (and
    types will be inferred):

    >>> partitioning(flavor="hive")

    """
    if flavor is None:
        # default flavor
        if schema is not None:
            if field_names is not None:
                raise ValueError(
                    "Cannot specify both 'schema' and 'field_names'")
            return DirectoryPartitioning(schema)
        elif field_names is not None:
            if isinstance(field_names, list):
                return DirectoryPartitioning.discover(field_names)
            else:
                raise ValueError(
                    "Expected list of field names, got {0}".format(
                        type(field_names)))
        else:
            raise ValueError(
                "For the default directory flavor, need to specify "
                "a Schema or a list of field names")
    elif flavor == 'hive':
        if field_names is not None:
            raise ValueError("Cannot specify 'field_names' for flavor 'hive'")
        elif schema is not None:
            if isinstance(schema, pa.Schema):
                return HivePartitioning(schema)
            else:
                raise ValueError(
                    "Expected Schema for 'schema', got {0}".format(
                        type(schema)))
        else:
            return HivePartitioning.discover()
    else:
        raise ValueError("Unsupported flavor")


def _ensure_fs(filesystem, path):
    # Validate or infer the filesystem from the path
    from pyarrow.fs import FileSystem, LocalFileSystem

    if filesystem is None:
        try:
            filesystem, _ = FileSystem.from_uri(path)
        except Exception:
            # when path is not found, we fall back to local file system
            filesystem = LocalFileSystem()
    return filesystem


def _ensure_fs_and_paths(path_or_paths, filesystem=None):
    # Validate and convert the path-likes and filesystem.
    # Returns filesystem and list of string paths or FileSelector
    from pyarrow.fs import FileType, FileSelector

    if isinstance(path_or_paths, list):
        paths_or_selector = [_stringify_path(path) for path in path_or_paths]
        # infer from first path
        filesystem = _ensure_fs(filesystem, paths_or_selector[0])
    else:
        path = _stringify_path(path_or_paths)
        filesystem = _ensure_fs(filesystem, path)
        stats = filesystem.get_target_stats([path])[0]
        if stats.type == FileType.Directory:
            # for directory, pass a selector
            paths_or_selector = FileSelector(path, recursive=True)
        elif stats.type == FileType.File:
            # for a single file path, pass it as a list
            paths_or_selector = [path]
        else:
            raise FileNotFoundError(path)

    return filesystem, paths_or_selector


def _ensure_partitioning(scheme):
    # Validate input and return a Partitioning(Factory) or passthrough None
    # for no partitioning
    if scheme is None:
        pass
    elif isinstance(scheme, str):
        scheme = partitioning(flavor=scheme)
    elif isinstance(scheme, list):
        scheme = partitioning(field_names=scheme)
    elif isinstance(scheme, (Partitioning, PartitioningFactory)):
        pass
    else:
        ValueError(
            "Expected Partitioning or PartitioningFactory, got {0}".format(
                type(scheme)))
    return scheme


def _ensure_format(obj):
    if isinstance(obj, FileFormat):
        return obj
    elif obj == "parquet":
        return ParquetFileFormat()
    else:
        raise ValueError("format '{0}' is not supported".format(obj))


def source(path_or_paths, filesystem=None, partitioning=None,
           format=None):
    """
    Open a (multi-file) data source.

    Parameters
    ----------
    path_or_paths : str, pathlib.Path, or list of those
        Path to a file or to a directory containing the data files, or
        a list of paths.
    filesystem : FileSystem, default None
        By default will be inferred from the path.
    partitioning : Partitioning(Factory), str or list of str
        The partitioning scheme specified with the ``partitioning()``
        function. A flavor string can be used as shortcut, and with a list of
        field names a DirectionaryPartitioning will be inferred.
    format : str, default None
        Currently only "parquet" is supported.

    Returns
    -------
    DataSource of DataSourceDiscovery

    """
    fs, paths_or_selector = _ensure_fs_and_paths(path_or_paths, filesystem)
    partitioning = _ensure_partitioning(partitioning)
    format = _ensure_format(format or "parquet")

    # TODO pass through options
    options = FileSystemFactoryOptions()
    if isinstance(partitioning, PartitioningFactory):
        options.partitioning_factory = partitioning
    elif isinstance(partitioning, Partitioning):
        options.partitioning = partitioning

    return FileSystemSourceFactory(fs, paths_or_selector, format, options)


def _ensure_source(src, **kwargs):
    # Need to return SourceFactory since `dataset` might need to finish the
    # factory with a unified schema.
    # TODO: return Source if a specific schema was passed?
    if _is_path_like(src):
        return source(src, **kwargs)
    elif isinstance(src, SourceFactory):
        if any(v is not None for v in kwargs.values()):
            # when passing a SourceFactory, the arguments cannot be specified
            raise ValueError(
                "When passing a Source(Factory), you cannot pass any "
                "additional arguments"
            )
        return src
    elif isinstance(src, Source):
        raise TypeError(
            "Source objects are currently not supported, only SourceFactory "
            "instances. Use the source() function to create such objects."
        )
    else:
        raise TypeError(
            "Expected a path-like or Source, got {0}".format(type(src))
        )


def dataset(sources, filesystem=None, partitioning=None, format=None):
    """
    Open a (multi-source) dataset.

    Parameters
    ----------
    sources : path or list of paths or source or list of sources
        Path to a file or to a directory containing the data files, or a list
        of paths for a multi-source dataset. To have more control, a list of
        sources can be passed, created with the ``source()`` function (in this
        case, the additional keywords will be ignored).
    filesystem : FileSystem, default None
        By default will be inferred from the path.
    partitioning : Partitioning(Factory), str, list of str
        The partitioning scheme specified with the ``partitioning()``
        function. A flavor string can be used as shortcut, and with a list of
        field names a DirectionaryPartitioning will be inferred.
    format : str
        Currently only "parquet" is supported.

    Returns
    -------
    Dataset

    Examples
    --------
    Opening a dataset for a single directory:

    >>> dataset("path/to/nyc-taxi/", format="parquet")

    Combining different sources:

    >>> dataset([
    ...     source("s3://old-taxi-data", format="parquet"),
    ...     source("local/path/to/new/data", format="csv")
    ... ])

    """
    if not isinstance(sources, list):
        sources = [sources]

    sources = [
        _ensure_source(src, filesystem=filesystem, partitioning=partitioning,
                       format=format)
        for src in sources
    ]
    return DatasetFactory(sources).finish()


def field(name):
    """References a named column of the dataset.

    Stores only the field's name. Type and other information is known only when
    the expression is applied on a dataset having an explicit scheme.

    Parameters
    ----------
    name : string
        The name of the field the expression references to.

    Returns
    -------
    field_expr : FieldExpression
    """
    return FieldExpression(name)


def scalar(value):
    """Expression representing a scalar value.

    Parameters
    ----------
    value : bool, int, float or string
        Python value of the scalar. Note that only a subset of types are
        currently supported.

    Returns
    -------
    scalar_expr : ScalarExpression
    """
    return ScalarExpression(value)

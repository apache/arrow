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

import pyarrow as pa
from pyarrow.util import _stringify_path, _is_path_like

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
    FileFragment,
    FileSystemDataset,
    FileSystemDatasetFactory,
    FileSystemFactoryOptions,
    Fragment,
    HivePartitioning,
    InExpression,
    IpcFileFormat,
    IsValidExpression,
    NotExpression,
    OrExpression,
    ParquetFileFormat,
    ParquetFileFragment,
    ParquetReadOptions,
    Partitioning,
    PartitioningFactory,
    ScalarExpression,
    Scanner,
    ScanTask,
    UnionDataset,
    UnionDatasetFactory
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
                    "Expected list of field names, got {}".format(
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
                    "Expected Schema for 'schema', got {}".format(
                        type(schema)))
        else:
            return HivePartitioning.discover()
    else:
        raise ValueError("Unsupported flavor")


def _ensure_fs(filesystem, path):
    # Validate or infer the filesystem from the path
    from pyarrow.fs import (
        FileSystem, LocalFileSystem, FileType, _normalize_path)

    if filesystem is None:
        # First check if the file exists as a local (relative) file path
        filesystem = LocalFileSystem()
        try:
            infos = filesystem.get_file_info([path])[0]
        except OSError:
            local_path_exists = False
        else:
            local_path_exists = (infos.type != FileType.NotFound)

        if not local_path_exists:
            # Perhaps it's a URI?
            try:
                return FileSystem.from_uri(path)
            except ValueError as e:
                if "empty scheme" not in str(e):
                    raise
                # ARROW-8213: not a URI, assume local path
                # to get a nice error message.

    # ensure we have a proper path (eg no backslashes on Windows)
    path = _normalize_path(filesystem, path)

    return filesystem, path


def _ensure_fs_and_paths(path, filesystem=None):
    # Return filesystem and list of string paths or FileSelector
    from pyarrow.fs import FileType, FileSelector

    path = _stringify_path(path)
    filesystem, path = _ensure_fs(filesystem, path)
    infos = filesystem.get_file_info([path])[0]
    if infos.type == FileType.Directory:
        # for directory, pass a selector
        paths_or_selector = FileSelector(path, recursive=True)
    elif infos.type == FileType.File:
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
            "Expected Partitioning or PartitioningFactory, got {}".format(
                type(scheme)))
    return scheme


def _ensure_format(obj):
    if isinstance(obj, FileFormat):
        return obj
    elif obj == "parquet":
        return ParquetFileFormat()
    elif obj == "ipc":
        return IpcFileFormat()
    else:
        raise ValueError("format '{}' is not supported".format(obj))


def factory(path_or_paths, filesystem=None, partitioning=None,
            format=None):
    """
    Create a factory which can be used to build a Dataset.

    Parameters
    ----------
    path_or_paths : str, pathlib.Path, or list of those
        Path to a file or to a directory containing the data files, or
        a list of paths.
    filesystem : FileSystem, default None
        By default will be inferred from the path.
    partitioning : Partitioning or PartitioningFactory or str or list of str
        The partitioning scheme specified with the ``partitioning()``
        function. A flavor string can be used as shortcut, and with a list of
        field names a DirectionaryPartitioning will be inferred.
    format : str, default None
        Currently only "parquet" is supported.

    Returns
    -------
    FileSystemDatasetFactory
    """
    if not isinstance(path_or_paths, (list, tuple)):
        path_or_paths = [path_or_paths]

    partitioning = _ensure_partitioning(partitioning)
    format = _ensure_format(format or "parquet")

    # TODO pass through options
    options = FileSystemFactoryOptions()
    if isinstance(partitioning, PartitioningFactory):
        options.partitioning_factory = partitioning
    elif isinstance(partitioning, Partitioning):
        options.partitioning = partitioning

    factories = []
    for path in path_or_paths:
        fs, paths_or_selector = _ensure_fs_and_paths(path, filesystem)
        factories.append(FileSystemDatasetFactory(fs, paths_or_selector,
                                                  format, options))

    if len(factories) == 0:
        raise ValueError("Need at least one path")
    elif len(factories) == 1:
        return factories[0]
    else:
        return UnionDatasetFactory(factories)


def _ensure_factory(src, **kwargs):
    # Need to return DatasetFactory since `dataset` might need to finish the
    # factory with a unified schema.
    # TODO: return Dataset if a specific schema was passed?
    if _is_path_like(src):
        return factory(src, **kwargs)
    elif isinstance(src, DatasetFactory):
        if any(v is not None for v in kwargs.values()):
            # when passing a SourceFactory, the arguments cannot be specified
            raise ValueError(
                "When passing a DatasetFactory, you cannot pass any "
                "additional arguments"
            )
        return src
    elif isinstance(src, Dataset):
        raise TypeError(
            "Dataset objects are currently not supported, only DatasetFactory "
            "instances. Use the factory() function to create such objects."
        )
    else:
        raise TypeError(
            "Expected a path-like or DatasetFactory, got {}".format(type(src))
        )


def dataset(paths_or_factories, filesystem=None, partitioning=None,
            format=None, schema=None):
    """
    Open a dataset.

    Parameters
    ----------
    paths_or_factories : path or list of paths or factory or list of factories
        Path to a file or to a directory containing the data files, or a list
        of paths for a multi-directory dataset. To have more control, a list of
        factories can be passed, created with the ``factory()`` function (in
        this case, the additional keywords will be ignored).
    filesystem : FileSystem, default None
        By default will be inferred from the path.
    partitioning : Partitioning, PartitioningFactory, str, list of str
        The partitioning scheme specified with the ``partitioning()``
        function. A flavor string can be used as shortcut, and with a list of
        field names a DirectionaryPartitioning will be inferred.
    format : str
        Currently only "parquet" is supported.
    schema : Schema, optional
        Optionally provide the Schema for the Dataset, in which case it will
        not be inferred from the source.

    Returns
    -------
    Dataset

    Examples
    --------
    Opening a dataset for a single directory:

    >>> dataset("path/to/nyc-taxi/", format="parquet")

    Construction from multiple factories:

    >>> dataset([
    ...     factory("s3://old-taxi-data", format="parquet"),
    ...     factory("local/path/to/new/data", format="csv")
    ... ])

    """
    # bundle the keyword arguments
    kwargs = dict(filesystem=filesystem, partitioning=partitioning,
                  format=format)

    single_dataset = False
    if not isinstance(paths_or_factories, list):
        paths_or_factories = [paths_or_factories]
        single_dataset = True

    factories = [_ensure_factory(f, **kwargs) for f in paths_or_factories]
    if single_dataset:
        return factories[0].finish(schema=schema)
    return UnionDatasetFactory(factories).finish(schema=schema)


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

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

import six

from pyarrow import from_pylist
from pyarrow._parquet import (ParquetReader, FileMetaData,  # noqa
                              RowGroupMetaData, Schema, ParquetWriter)
import pyarrow._parquet as _parquet  # noqa
from pyarrow.filesystem import LocalFilesystem
from pyarrow.table import concat_tables, Table

from itertools import chain
from os import path as osp

EXCLUDED_PARQUET_PATHS = {'_metadata', '_common_metadata', '_SUCCESS'}


class ParquetFile(object):
    """
    Open a Parquet binary file for reading

    Parameters
    ----------
    source : str or pyarrow.io.NativeFile
        Readable source. For passing Python file objects or byte buffers,
        see pyarrow.io.PythonFileInterface or pyarrow.io.BufferReader.
    metadata : ParquetFileMetadata, default None
        Use existing metadata object, rather than reading from file.
    """
    def __init__(self, source, metadata=None):
        self.reader = ParquetReader()
        self.reader.open(source, metadata=metadata)

    @property
    def metadata(self):
        return self.reader.metadata

    @property
    def schema(self):
        return self.metadata.schema

    @property
    def num_row_groups(self):
        return self.reader.num_row_groups

    def read_row_group(self, i, columns=None, nthreads=1):
        """
        Read a single row group from a Parquet file

        Parameters
        ----------
        columns: list
            If not None, only these columns will be read from the row group.
        nthreads : int, default 1
            Number of columns to read in parallel. If > 1, requires that the
            underlying file source is threadsafe

        Returns
        -------
        pyarrow.table.Table
            Content of the row group as a table (of columns)
        """
        column_indices = self._get_column_indices(columns)
        self.reader.set_num_threads(nthreads)
        return self.reader.read_row_group(i, column_indices=column_indices)

    def read(self, columns=None, nthreads=1):
        """
        Read a Table from Parquet format

        Parameters
        ----------
        columns: list
            If not None, only these columns will be read from the file.
        nthreads : int, default 1
            Number of columns to read in parallel. If > 1, requires that the
            underlying file source is threadsafe

        Returns
        -------
        pyarrow.table.Table
            Content of the file as a table (of columns)
        """
        column_indices = self._get_column_indices(columns)
        self.reader.set_num_threads(nthreads)
        return self.reader.read_all(column_indices=column_indices)

    def _get_column_indices(self, column_names):
        if column_names is None:
            return None
        else:
            return [self.reader.column_name_idx(column)
                    for column in column_names]


def read_table(source, columns=None, nthreads=1, metadata=None):
    """
    Read a Table from Parquet format

    Parameters
    ----------
    source: str or pyarrow.io.NativeFile
        Location of Parquet dataset. If a string passed, can be a single file
        name or directory name. For passing Python file objects or byte
        buffers, see pyarrow.io.PythonFileInterface or pyarrow.io.BufferReader.
    columns: list
        If not None, only these columns will be read from the file.
    nthreads : int, default 1
        Number of columns to read in parallel. Requires that the underlying
        file source is threadsafe
    metadata : FileMetaData
        If separately computed

    Returns
    -------
    pyarrow.Table
        Content of the file as a table (of columns)
    """

    if isinstance(source, six.string_types):
        fs = LocalFilesystem.get_instance()
        if fs.isdir(source):
            return fs.read_parquet(source, columns=columns,
                                   metadata=metadata)

    pf = ParquetFile(source, metadata=metadata)
    return pf.read(columns=columns, nthreads=nthreads)


def read_multiple_files(paths, columns=None, filesystem=None, nthreads=1,
                        metadata=None, schema=None):
    """
    Read multiple Parquet files as a single pyarrow.Table

    Parameters
    ----------
    paths : List[str]
        List of file paths
    columns : List[str]
        Names of columns to read from the file
    filesystem : Filesystem, default None
        If nothing passed, paths assumed to be found in the local on-disk
        filesystem
    nthreads : int, default 1
        Number of columns to read in parallel. Requires that the underlying
        file source is threadsafe
    metadata : pyarrow.parquet.FileMetaData
        Use metadata obtained elsewhere to validate file schemas
    schema : pyarrow.parquet.Schema
        Use schema obtained elsewhere to validate file schemas. Alternative to
        metadata parameter

    Returns
    -------
    pyarrow.Table
        Content of the file as a table (of columns)
    """
    if filesystem is None:
        def open_file(path, meta=None):
            return ParquetFile(path, metadata=meta)
    else:
        def open_file(path, meta=None):
            return ParquetFile(filesystem.open(path, mode='rb'), metadata=meta)

    if len(paths) == 0:
        raise ValueError('Must pass at least one file path')

    if metadata is None and schema is None:
        schema = open_file(paths[0]).schema
    elif schema is None:
        schema = metadata.schema

    # Verify schemas are all equal
    all_file_metadata = []
    for path in paths:
        file_metadata = open_file(path).metadata
        if not schema.equals(file_metadata.schema):
            raise ValueError('Schema in {0} was different. {1!s} vs {2!s}'
                             .format(path, file_metadata.schema, schema))
        all_file_metadata.append(file_metadata)

    # Read the tables
    tables = []
    for path, path_metadata in zip(paths, all_file_metadata):
        reader = open_file(path, meta=path_metadata)
        table = reader.read(columns=columns, nthreads=nthreads)
        tables.append(table)

    all_data = concat_tables(tables)
    return all_data


def write_table(table, sink, row_group_size=None, version='1.0',
                use_dictionary=True, compression='snappy', **kwargs):
    """
    Write a Table to Parquet format

    Parameters
    ----------
    table : pyarrow.Table
    sink: string or pyarrow.io.NativeFile
    row_group_size : int, default None
        The maximum number of rows in each Parquet RowGroup. As a default,
        we will write a single RowGroup per file.
    version : {"1.0", "2.0"}, default "1.0"
        The Parquet format version, defaults to 1.0
    use_dictionary : bool or list
        Specify if we should use dictionary encoding in general or only for
        some columns.
    compression : str or dict
        Specify the compression codec, either on a general basis or per-column.
    """
    row_group_size = kwargs.get('chunk_size', row_group_size)
    writer = ParquetWriter(sink, use_dictionary=use_dictionary,
                           compression=compression,
                           version=version)
<<<<<<< HEAD
    writer.write_table(table, row_group_size=row_group_size)
||||||| parent of d9f764c... [ARROW-539] [Python] Support reading Parquet datasets with standard partition directory schemes
    writer.write_table(table, row_group_size=chunk_size)
=======
    writer.write_table(table, row_group_size=chunk_size)


def _iter_parquet(root_dir, fs, ext='.parq'):
    """Yield all parquet files under root_dir"""
    if not fs.isdir(root_dir):
        raise ValueError('"{0}" is not a directory'.format(root_dir))

    for path in fs.ls(root_dir):
        if fs.isfile(path) and path.endswith(ext):
            yield path
        if fs.isdir(path):
            for p in _iter_parquet(path, fs, ext):
                yield p


def _partitions(path, root):
    """
    >>> partitions('/root/x=1/y=2/x.parq')
    {'x': '1', 'y': '2'}
    """
    parts = {}
    # '/root/x=1/y=2/x.parq' -> '/x=1/y=2'
    inner = osp.dirname(path[len(root):])
    while True:
        head, tail = osp.split(inner)
        if '=' not in tail:
            raise ValueError('sub directory without = in {0!r}'.format(path))
        key, value = tail.split('=', maxsplit=1)
        if key in parts:
            raise ValueError(
                'partition {0!r} appears twice in {1!r}'.format(key, path))
        parts[key] = value
        # /, \ or ''
        if len(head) < 2:
            return parts
        inner = head


def _add_parts(table, parts, part_fields):
    """Add data from directory partitions to table"""
    size = len(table)
    names = []
    arrays = []
    for i, field in enumerate(table.schema):
        names.append(field.name)
        arrays.append(table.column(i))

    for name in part_fields:
        arrays.append(from_pylist([parts[name]] * size))
        names.append(name)

    return Table.from_arrays(arrays, names, table.name)


def read_dir(root_dir, columns=None, filesystem=None, nthreads=1,
             metadata=None, schema=None, ext='.parq'):
    """
    Read a partitioned directory as a single pyarrow.Table

    Parameters
    ----------
    root_dir : str
        Root directory
    columns : List[str]
        Names of columns to read from the file
    filesystem : Filesystem, default None
        If nothing passed, paths assumed to be found in the local on-disk
        filesystem
    nthreads : int, default 1
        Number of columns to read in parallel. Requires that the underlying
        file source is threadsafe
    metadata : pyarrow.parquet.FileMetaData
        Use metadata obtained elsewhere to validate file schemas
    schema : pyarrow.parquet.Schema
        Use schema obtained elsewhere to validate file schemas. Alternative to
        metadata parameter
    ext: str
        Parqet file extension

    Returns
    -------
    pyarrow.Table
        Content of the directory as a table (of columns)
    """
    fs = LocalFilesystem() if filesystem is None else filesystem
    files = _iter_parquet(root_dir, fs, ext)

    try:
        path = next(files)
        part_fields = sorted(_partitions(path, root_dir))
    except StopIteration:
        raise ValueError('no parquet files in {0!r}'.format(root_dir))

    if metadata is None and schema is None:
        schema = ParquetFile(fs.open(path), metadata=metadata).schema
    elif schema is None:
        schema = metadata.schema

    tps = []
    for path in chain([path], files):
        parts = _partitions(path, root_dir)
        if sorted(parts) != part_fields:
            raise ValueError(
                '{0!r} has different partitioning than {1}'.format(
                    path, part_fields))
        pfile = ParquetFile(fs.open(path), metadata=metadata)
        if not schema.equals(pfile.metadata.schema):
            raise ValueError(
                'Schema in {0} was different. {1!s} vs {2!s}'.format(
                    path, pfile.metadata.schema, schema))

        table = pfile.read(columns=columns, nthreads=nthreads)
        tps.append((table, parts))

    tables = [_add_parts(table, parts, part_fields) for table, parts in tps]
    return concat_tables(tables)

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

from os import path as osp

EXCLUDED_PARQUET_PATHS = {'_metadata', '_common_metadata', '_SUCCESS'}


class ParquetFile(object):
    """
    Reader interface for a single Parquet file

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


class ParquetPartitionSet(object):

    def __init__(self, keys=None):
        self.keys = keys or []
        self.key_indices = {k: i for i, k in enumerate(self.keys)}

    def get_index(self, key):
        if key in self.key_indices:
            return self.key_indices[key]
        else:
            index = len(self.key_indices)
            self.key_indices[key] = index
            return index

    @property
    def is_sorted(self):
        return list(self.keys) == sorted(self.keys)


class ParquetDatasetPiece(object):
    """
    A single chunk of a potentially larger Parquet dataset to read. The
    arguments will indicate to read either a single row group or all row
    groups, and whether to add partition keys to the resulting pyarrow.Table

    Parameters
    ----------
    partition_keys : list of tuples
      [(column name, ordinal index)]
    """

    def __init__(self, path, row_group=None, partition_keys=None):
        self.path = path
        self.row_group = row_group
        self.partition_keys = partition_keys

    def __eq__(self, other):
        if not isinstance(other, ParquetDatasetPiece):
            return False
        return (self.path == other.path and
                self.row_group == other.row_group and
                self.partition_keys == other.partition_keys)

    def __ne__(self, other):
        return not (self == other)

    def __str__(self):
        result = ''

        if self.partition_keys is not None:
            partition_str = ', '.join('{0}={1}'.format(name, index)
                                      for name, index in self.partition_keys)
            result += 'partition[{0}] '.format(partition_str)

        result += self.path

        if self.row_group is not None:
            result += ' | row_group={0}'.format(self.row_group)

        return result

    def open(self, open_file_func):
        return open_file_func(self.path)

    def read(self, open_file_func, columns=None, nthreads=None):
        reader = self.open(open_file_func)

        if self.row_group is not None:
            table = reader.read_row_group(self.row_group, columns=columns,
                                          nthreads=nthreads)
        else:
            table = reader.read(columns=columns, nthreads=nthreads)

        # TODO(wesm): add partition keys

        return table


def _is_parquet_file(path):
    return path.endswith('parq') or path.endswith('parquet')


class ParquetDataset(object):
    """
    Encapsulates details of reading a complete Parquet dataset possibly
    consisting of multiple files and partitions in subdirectories

    Parameters
    ----------
    path_or_paths : str or List[str]
        One or more file paths
    filesystem : Filesystem, default None
        If nothing passed, paths assumed to be found in the local on-disk
        filesystem
    metadata : pyarrow.parquet.FileMetaData
        Use metadata obtained elsewhere to validate file schemas
    schema : pyarrow.parquet.Schema
        Use schema obtained elsewhere to validate file schemas. Alternative to
        metadata parameter
    split_row_groups
    validate_schema
    """
    def __init__(self, path_or_paths, filesystem=None, schema=None,
                 metadata=None, split_row_groups=False, validate_schema=True):
        from pyarrow.filesystem import LocalFilesystem
        if filesystem is None:
            self.fs = LocalFilesystem.get_instance()
        else:
            self.fs = filesystem

        if isinstance(path_or_paths, six.string_types):
            path_or_paths = [path_or_paths]

        if len(path_or_paths) == 0:
            raise ValueError('Must pass at least one file path')

        self.paths = path_or_paths
        self.metadata = metadata
        self.schema = schema

        self.split_row_groups = split_row_groups
        self.pieces = self.get_dataset_pieces()

        if validate_schema:
            self.validate_schemas()

    def validate_schemas(self):
        open_file = self._get_open_file_func()

        if self.metadata is None and self.schema is None:
            self.schema = self.pieces[0].open(open_file).schema
        elif self.schema is None:
            self.schema = self.metadata.schema

        # Verify schemas are all equal
        for piece in self.pieces:
            file_metadata = piece.open(open_file).metadata
            if not self.schema.equals(file_metadata.schema):
                raise ValueError('Schema in {0!s} was different. '
                                 '{1!s} vs {2!s}'
                                 .format(piece, file_metadata.schema,
                                         self.schema))

    def get_dataset_pieces(self):
        pieces = []

        for path in self.paths:
            self._visit_path(path)
            pieces.extend(path_pieces)

        return pieces

    def _visit_path(self, path, pieces):
        part_keys = None

        def _push_piece(path):
            if _is_parquet_file(path):
                piece = ParquetDatasetPiece(path, partition_keys=part_keys)
                pieces.append(piece)

        if self.fs.isdir(path):
            for path in self.fs.ls(path):
                _push_piece(path)
        else:
            _push_piece(path)

        return paths_to_read

    def read(self, columns=None, nthreads=None):
        """
        Read multiple Parquet files as a single pyarrow.Table

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file
        nthreads : int, default 1
            Number of columns to read in parallel. Requires that the underlying
            file source is threadsafe

        Returns
        -------
        pyarrow.Table
            Content of the file as a table (of columns)
        """
        open_file = self._get_open_file_func()

        tables = []
        for piece in self.pieces:
            table = piece.read(open_file, columns=columns, nthreads=nthreads)
            tables.append(table)

        all_data = concat_tables(tables)
        return all_data

    def _get_open_file_func(self):
        if self.fs is None:
            def open_file(path, meta=None):
                return ParquetFile(path, metadata=meta)
        else:
            def open_file(path, meta=None):
                return ParquetFile(self.fs.open(path, mode='rb'),
                                   metadata=meta)


class _ParquetDirectoryListing(object):
    """

    """
    def __init__(self, filesystem, dirpath):
        self.filesystem = filesystem
        self.dirpath = dirpath
        self.partitions = []

    def _visit_level(self, level):
        paths =


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


def _partitions(path, root, path_separator):
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


def write_table(table, where, row_group_size=None, version='1.0',
                use_dictionary=True, compression='snappy', **kwargs):
    """
    Write a Table to Parquet format

    Parameters
    ----------
    table : pyarrow.Table
    where: string or pyarrow.io.NativeFile
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
    writer = ParquetWriter(where, use_dictionary=use_dictionary,
                           compression=compression,
                           version=version)
    writer.write_table(table, row_group_size=row_group_size)

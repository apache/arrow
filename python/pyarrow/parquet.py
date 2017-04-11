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

from pyarrow._parquet import (ParquetReader, FileMetaData,  # noqa
                              RowGroupMetaData, Schema, ParquetWriter)
import pyarrow._parquet as _parquet  # noqa
from pyarrow.filesystem import LocalFilesystem
from pyarrow.table import concat_tables


# ----------------------------------------------------------------------
# Reading a single Parquet file


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
        if nthreads is not None:
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
        if nthreads is not None:
            self.reader.set_num_threads(nthreads)
        return self.reader.read_all(column_indices=column_indices)

    def _get_column_indices(self, column_names):
        if column_names is None:
            return None
        else:
            return [self.reader.column_name_idx(column)
                    for column in column_names]


# ----------------------------------------------------------------------
# Metadata container providing instructions about reading a single Parquet
# file, possibly part of a partitioned dataset


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

    def __repr__(self):
        return ('{0}({1!r}, row_group={2!r}, partition_keys={3!r})'
                .format(type(self).__name__, self.path,
                        self.row_group,
                        self.partition_keys))

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

    def read(self, open_file_func, columns=None, nthreads=1):
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


class PartitionSet(object):

    def __init__(self, name, keys=None):
        self.name = name
        self.keys = keys or []
        self.key_indices = {k: i for i, k in enumerate(self.keys)}

    def get_index(self, key):
        if key in self.key_indices:
            return self.key_indices[key]
        else:
            index = len(self.key_indices)
            self.keys.append(key)
            self.key_indices[key] = index
            return index

    @property
    def is_sorted(self):
        return list(self.keys) == sorted(self.keys)


class ParquetPartitions(object):

    def __init__(self):
        self.levels = []
        self.partition_names = set()

    def get_index(self, level, name, key):
        if level == len(self.levels):
            if name in self.partition_names:
                raise ValueError('{0} was the name of the partition in '
                                 'another level'.format(name))

            part_set = PartitionSet(name)
            self.levels.append(part_set)
            self.partition_names.add(name)

        return self.levels[level].get_index(key)


def is_string(x):
    return isinstance(x, six.string_types)


class ParquetManifest(object):
    """

    """
    def __init__(self, dirpath, filesystem=None, pathsep='/'):
        self.filesystem = filesystem or LocalFilesystem.get_instance()
        self.pathsep = pathsep
        self.dirpath = dirpath
        self.partitions = ParquetPartitions()
        self.pieces = []

        self.common_metadata_path = None
        self.metadata_path = None

        self._visit_level(0, self.dirpath, [])

    def _visit_level(self, level, base_path, part_keys):
        directories = []
        files = []
        fs = self.filesystem

        if not fs.isdir(base_path):
            raise ValueError('"{0}" is not a directory'.format(base_path))

        for path in sorted(fs.ls(base_path)):
            if fs.isfile(path):
                if _is_parquet_file(path):
                    files.append(path)
                elif path == '_common_metadata':
                    self.common_metadata_path = path
                elif path == '_metadata':
                    self.metadata_path = path
                else:
                    print('Ignoring path: {0}'.format(path))
            elif fs.isdir(path):
                directories.append(path)

        if len(files) > 0 and len(directories) > 0:
            raise ValueError('Found files in an intermediate '
                             'directory: {0}'.format(base_path))
        elif len(directories) > 0:
            self._visit_directories(level, directories, part_keys)
        else:
            self._push_pieces(files, part_keys)

    def _visit_directories(self, level, directories, part_keys):
        for path in directories:
            head, tail = _path_split(path, self.pathsep)
            name, key = _parse_hive_partition(tail)

            index = self.partitions.get_index(level, name, key)
            dir_part_keys = part_keys + [(name, index)]
            self._visit_level(level + 1, path, dir_part_keys)

    def _push_pieces(self, files, part_keys):
        for path in files:
            piece = ParquetDatasetPiece(path, partition_keys=part_keys)
            self.pieces.append(piece)


def _parse_hive_partition(value):
    if '=' not in value:
        raise ValueError('Directory name did not appear to be a '
                         'partition: {0}'.format(value))
    return value.split('=', 1)


def _path_split(path, sep):
    i = path.rfind(sep) + 1
    head, tail = path[:i], path[i:]
    head = head.rstrip(sep)
    return head, tail


EXCLUDED_PARQUET_PATHS = {'_SUCCESS'}


class ParquetDataset(object):
    """
    Encapsulates details of reading a complete Parquet dataset possibly
    consisting of multiple files and partitions in subdirectories

    Parameters
    ----------
    path_or_paths : str or List[str]
        A directory name, single file name, or list of file names
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
        if filesystem is None:
            self.fs = LocalFilesystem.get_instance()
        else:
            self.fs = filesystem

        self.pieces, self.partitions = _make_manifest(path_or_paths, self.fs)

        self.metadata = metadata
        self.schema = schema

        self.split_row_groups = split_row_groups

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

    def read(self, columns=None, nthreads=1):
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
        return open_file


def _make_manifest(path_or_paths, fs, pathsep='/'):
    partitions = None

    if is_string(path_or_paths) and fs.isdir(path_or_paths):
        manifest = ParquetManifest(path_or_paths, filesystem=fs,
                                   pathsep=pathsep)
        pieces = manifest.pieces
        partitions = manifest.partitions
    else:
        if not isinstance(path_or_paths, list):
            path_or_paths = [path_or_paths]

        # List of paths
        if len(path_or_paths) == 0:
            raise ValueError('Must pass at least one file path')

        pieces = []
        for path in path_or_paths:
            if not fs.isfile(path):
                raise IOError('Passed non-file path: {0}'
                              .format(path))
            piece = ParquetDatasetPiece(path)
            pieces.append(piece)

    return pieces, partitions


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
    if is_string(source):
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

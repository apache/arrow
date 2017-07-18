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

import os
import json

import six

import numpy as np

from pyarrow.filesystem import LocalFilesystem
from pyarrow._parquet import (ParquetReader, FileMetaData,  # noqa
                              RowGroupMetaData, ParquetSchema,
                              ParquetWriter)
import pyarrow._parquet as _parquet  # noqa
import pyarrow.lib as lib


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

    def read_pandas(self, columns=None, nthreads=1):
        column_indices = self._get_column_indices(columns)
        custom_metadata = self.metadata.metadata

        if custom_metadata and b'pandas' in custom_metadata:
            index_columns = json.loads(
                custom_metadata[b'pandas'].decode('utf8')
            )['index_columns']
        else:
            index_columns = []

        if column_indices is not None and index_columns:
            column_indices += map(self.reader.column_name_idx, index_columns)

        if nthreads is not None:
            self.reader.set_num_threads(nthreads)
        return self.reader.read_all(column_indices=column_indices)

    def _get_column_indices(self, column_names):
        if column_names is None:
            return None
        return list(map(self.reader.column_name_idx, column_names))


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
    path : str
        Path to file in the file system where this piece is located
    partition_keys : list of tuples
      [(column name, ordinal index)]
    row_group : int, default None
        Row group to load. By default, reads all row groups
    """

    def __init__(self, path, row_group=None, partition_keys=None):
        self.path = path
        self.row_group = row_group
        self.partition_keys = partition_keys or []

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

        if len(self.partition_keys) > 0:
            partition_str = ', '.join('{0}={1}'.format(name, index)
                                      for name, index in self.partition_keys)
            result += 'partition[{0}] '.format(partition_str)

        result += self.path

        if self.row_group is not None:
            result += ' | row_group={0}'.format(self.row_group)

        return result

    def get_metadata(self, open_file_func=None):
        """
        Given a function that can create an open ParquetFile object, return the
        file's metadata
        """
        return self._open(open_file_func).metadata

    def _open(self, open_file_func=None):
        """
        Returns instance of ParquetFile
        """
        reader = open_file_func(self.path)
        if not isinstance(reader, ParquetFile):
            reader = ParquetFile(reader)
        return reader

    def read(self, columns=None, nthreads=1, partitions=None,
             open_file_func=None, file=None):
        """
        Read this piece as a pyarrow.Table

        Parameters
        ----------
        columns : list of column names, default None
        nthreads : int, default 1
            For multithreaded file reads
        partitions : ParquetPartitions, default None
        open_file_func : function, default None
            A function that knows how to construct a ParquetFile object given
            the file path in this piece

        Returns
        -------
        table : pyarrow.Table
        """
        if open_file_func is not None:
            reader = self._open(open_file_func)
        elif file is not None:
            reader = ParquetFile(file)
        else:
            # try to read the local path
            reader = ParquetFile(self.path)

        if self.row_group is not None:
            table = reader.read_row_group(self.row_group, columns=columns,
                                          nthreads=nthreads)
        else:
            table = reader.read(columns=columns, nthreads=nthreads)

        if len(self.partition_keys) > 0:
            if partitions is None:
                raise ValueError('Must pass partition sets')

            # Here, the index is the categorical code of the partition where
            # this piece is located. Suppose we had
            #
            # /foo=a/0.parq
            # /foo=b/0.parq
            # /foo=c/0.parq
            #
            # Then we assign a=0, b=1, c=2. And the resulting Table pieces will
            # have a DictionaryArray column named foo having the constant index
            # value as indicated. The distinct categories of the partition have
            # been computed in the ParquetManifest
            for i, (name, index) in enumerate(self.partition_keys):
                # The partition code is the same for all values in this piece
                indices = np.array([index], dtype='i4').repeat(len(table))

                # This is set of all partition values, computed as part of the
                # manifest, so ['a', 'b', 'c'] as in our example above.
                dictionary = partitions.levels[i].dictionary

                arr = lib.DictionaryArray.from_arrays(indices, dictionary)
                col = lib.Column.from_array(name, arr)
                table = table.append_column(col)

        return table


def _is_parquet_file(path):
    return path.endswith('parq') or path.endswith('parquet')


class PartitionSet(object):
    """A data structure for cataloguing the observed Parquet partitions at a
    particular level. So if we have

    /foo=a/bar=0
    /foo=a/bar=1
    /foo=a/bar=2
    /foo=b/bar=0
    /foo=b/bar=1
    /foo=b/bar=2

    Then we have two partition sets, one for foo, another for bar. As we visit
    levels of the partition hierarchy, a PartitionSet tracks the distinct
    values and assigns categorical codes to use when reading the pieces
    """

    def __init__(self, name, keys=None):
        self.name = name
        self.keys = keys or []
        self.key_indices = {k: i for i, k in enumerate(self.keys)}
        self._dictionary = None

    def get_index(self, key):
        """
        Get the index of the partition value if it is known, otherwise assign
        one
        """
        if key in self.key_indices:
            return self.key_indices[key]
        else:
            index = len(self.key_indices)
            self.keys.append(key)
            self.key_indices[key] = index
            return index

    @property
    def dictionary(self):
        if self._dictionary is not None:
            return self._dictionary

        if len(self.keys) == 0:
            raise ValueError('No known partition keys')

        # Only integer and string partition types are supported right now
        try:
            integer_keys = [int(x) for x in self.keys]
            dictionary = lib.array(integer_keys)
        except ValueError:
            dictionary = lib.array(self.keys)

        self._dictionary = dictionary
        return dictionary

    @property
    def is_sorted(self):
        return list(self.keys) == sorted(self.keys)


class ParquetPartitions(object):

    def __init__(self):
        self.levels = []
        self.partition_names = set()

    def __len__(self):
        return len(self.levels)

    def __getitem__(self, i):
        return self.levels[i]

    def get_index(self, level, name, key):
        """
        Record a partition value at a particular level, returning the distinct
        code for that value at that level. Example:

        partitions.get_index(1, 'foo', 'a') returns 0
        partitions.get_index(1, 'foo', 'b') returns 1
        partitions.get_index(1, 'foo', 'c') returns 2
        partitions.get_index(1, 'foo', 'a') returns 0

        Parameters
        ----------
        level : int
            The nesting level of the partition we are observing
        name : string
            The partition name
        key : string or int
            The partition value
        """
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
    def __init__(self, dirpath, filesystem=None, pathsep='/',
                 partition_scheme='hive'):
        self.filesystem = filesystem or LocalFilesystem.get_instance()
        self.pathsep = pathsep
        self.dirpath = dirpath
        self.partition_scheme = partition_scheme
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
                elif path.endswith('_common_metadata'):
                    self.common_metadata_path = path
                elif path.endswith('_metadata'):
                    self.metadata_path = path
                elif not self._should_silently_exclude(path):
                    print('Ignoring path: {0}'.format(path))
            elif fs.isdir(path):
                directories.append(path)

        # ARROW-1079: Filter out "private" directories starting with underscore
        directories = [x for x in directories if not _is_private_directory(x)]

        if len(files) > 0 and len(directories) > 0:
            raise ValueError('Found files in an intermediate '
                             'directory: {0}'.format(base_path))
        elif len(directories) > 0:
            self._visit_directories(level, directories, part_keys)
        else:
            self._push_pieces(files, part_keys)

    def _should_silently_exclude(self, path):
        _, tail = path.rsplit(self.pathsep, 1)
        return tail.endswith('.crc') or tail in EXCLUDED_PARQUET_PATHS

    def _visit_directories(self, level, directories, part_keys):
        for path in directories:
            head, tail = _path_split(path, self.pathsep)
            name, key = _parse_hive_partition(tail)

            index = self.partitions.get_index(level, name, key)
            dir_part_keys = part_keys + [(name, index)]
            self._visit_level(level + 1, path, dir_part_keys)

    def _parse_partition(self, dirname):
        if self.partition_scheme == 'hive':
            return _parse_hive_partition(dirname)
        else:
            raise NotImplementedError('partition schema: {0}'
                                      .format(self.partition_scheme))

    def _push_pieces(self, files, part_keys):
        self.pieces.extend([
            ParquetDatasetPiece(path, partition_keys=part_keys)
            for path in files
        ])


def _parse_hive_partition(value):
    if '=' not in value:
        raise ValueError('Directory name did not appear to be a '
                         'partition: {0}'.format(value))
    return value.split('=', 1)


def _is_private_directory(x):
    _, tail = os.path.split(x)
    return tail.startswith('_') and '=' not in tail


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
    split_row_groups : boolean, default False
        Divide files into pieces for each row group in the file
    validate_schema : boolean, default True
        Check that individual file schemas are all the same / compatible
    """
    def __init__(self, path_or_paths, filesystem=None, schema=None,
                 metadata=None, split_row_groups=False, validate_schema=True):
        if filesystem is None:
            self.fs = LocalFilesystem.get_instance()
        else:
            self.fs = filesystem

        self.paths = path_or_paths

        (self.pieces, self.partitions,
         self.metadata_path) = _make_manifest(path_or_paths, self.fs)

        self.metadata = metadata
        self.schema = schema

        self.split_row_groups = split_row_groups

        if split_row_groups:
            raise NotImplementedError("split_row_groups not yet implemented")

        if validate_schema:
            self.validate_schemas()

    def validate_schemas(self):
        open_file = self._get_open_file_func()

        if self.metadata is None and self.schema is None:
            if self.metadata_path is not None:
                self.schema = open_file(self.metadata_path).schema
            else:
                self.schema = self.pieces[0].get_metadata(open_file).schema
        elif self.schema is None:
            self.schema = self.metadata.schema

        # Verify schemas are all equal
        for piece in self.pieces:
            file_metadata = piece.get_metadata(open_file)
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
            table = piece.read(columns=columns, nthreads=nthreads,
                               partitions=self.partitions,
                               open_file_func=open_file)
            tables.append(table)

        all_data = lib.concat_tables(tables)
        return all_data

    def _get_open_file_func(self):
        if self.fs is None or isinstance(self.fs, LocalFilesystem):
            def open_file(path, meta=None):
                return ParquetFile(path, metadata=meta)
        else:
            def open_file(path, meta=None):
                return ParquetFile(self.fs.open(path, mode='rb'),
                                   metadata=meta)
        return open_file


def _make_manifest(path_or_paths, fs, pathsep='/'):
    partitions = None
    metadata_path = None

    if len(path_or_paths) == 1:
        # Dask passes a directory as a list of length 1
        path_or_paths = path_or_paths[0]

    if is_string(path_or_paths) and fs.isdir(path_or_paths):
        manifest = ParquetManifest(path_or_paths, filesystem=fs,
                                   pathsep=fs.pathsep)
        metadata_path = manifest.metadata_path
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

    return pieces, partitions, metadata_path


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


def read_pandas(source, columns=None, nthreads=1, metadata=None):
    """
    Read a Table from Parquet format, reconstructing the index values if
    available.

    Parameters
    ----------
    source: str or pyarrow.io.NativeFile
        Location of Parquet dataset. If a string passed, can be a single file
        name. For passing Python file objects or byte buffers,
        see pyarrow.io.PythonFileInterface or pyarrow.io.BufferReader.
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
        Content of the file as a Table of Columns, including DataFrame indexes
        as Columns.
    """
    if is_string(source):
        fs = LocalFilesystem.get_instance()
        if fs.isdir(source):
            raise NotImplementedError(
                'Reading a directory of Parquet files with DataFrame index '
                'metadata is not yet supported'
            )

    pf = ParquetFile(source, metadata=metadata)
    return pf.read_pandas(columns=columns, nthreads=nthreads)


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
    writer = ParquetWriter(where, table.schema,
                           use_dictionary=use_dictionary,
                           compression=compression,
                           version=version)
    writer.write_table(table, row_group_size=row_group_size)
    writer.close()


def write_metadata(schema, where, version='1.0'):
    """
    Write metadata-only Parquet file from schema

    Parameters
    ----------
    schema : pyarrow.Schema
    where: string or pyarrow.io.NativeFile
    version : {"1.0", "2.0"}, default "1.0"
        The Parquet format version, defaults to 1.0
    """
    writer = ParquetWriter(where, schema, version=version)
    writer.close()

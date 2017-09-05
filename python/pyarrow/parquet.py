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
import inspect
import json

import six

import numpy as np

from pyarrow.filesystem import FileSystem, LocalFileSystem, S3FSWrapper
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
    common_metadata : ParquetFileMetadata, default None
        Will be used in reads for pandas schema metadata if not found in the
        main file's metadata, no other uses at the moment
    """
    def __init__(self, source, metadata=None, common_metadata=None):
        self.reader = ParquetReader()
        self.reader.open(source, metadata=metadata)
        self.common_metadata = common_metadata

    @property
    def metadata(self):
        return self.reader.metadata

    @property
    def schema(self):
        return self.metadata.schema

    @property
    def num_row_groups(self):
        return self.reader.num_row_groups

    def read_row_group(self, i, columns=None, nthreads=1,
                       use_pandas_metadata=False):
        """
        Read a single row group from a Parquet file

        Parameters
        ----------
        columns: list
            If not None, only these columns will be read from the row group.
        nthreads : int, default 1
            Number of columns to read in parallel. If > 1, requires that the
            underlying file source is threadsafe
        use_pandas_metadata : boolean, default False
            If True and file has custom pandas schema metadata, ensure that
            index columns are also loaded

        Returns
        -------
        pyarrow.table.Table
            Content of the row group as a table (of columns)
        """
        column_indices = self._get_column_indices(
            columns, use_pandas_metadata=use_pandas_metadata)
        return self.reader.read_row_group(i, column_indices=column_indices,
                                          nthreads=nthreads)

    def read(self, columns=None, nthreads=1, use_pandas_metadata=False):
        """
        Read a Table from Parquet format

        Parameters
        ----------
        columns: list
            If not None, only these columns will be read from the file.
        nthreads : int, default 1
            Number of columns to read in parallel. If > 1, requires that the
            underlying file source is threadsafe
        use_pandas_metadata : boolean, default False
            If True and file has custom pandas schema metadata, ensure that
            index columns are also loaded

        Returns
        -------
        pyarrow.table.Table
            Content of the file as a table (of columns)
        """
        column_indices = self._get_column_indices(
            columns, use_pandas_metadata=use_pandas_metadata)
        return self.reader.read_all(column_indices=column_indices,
                                    nthreads=nthreads)

    def scan_contents(self, columns=None, batch_size=65536):
        """
        Read contents of file with a single thread for indicated columns and
        batch size. Number of rows in file is returned. This function is used
        for benchmarking

        Parameters
        ----------
        columns : list of integers, default None
            If None, scan all columns
        batch_size : int, default 64K
            Number of rows to read at a time internally

        Returns
        -------
        num_rows : number of rows in file
        """
        column_indices = self._get_column_indices(columns)
        return self.reader.scan_contents(column_indices,
                                         batch_size=batch_size)

    def _get_column_indices(self, column_names, use_pandas_metadata=False):
        if column_names is None:
            return None

        indices = list(map(self.reader.column_name_idx, column_names))

        if use_pandas_metadata:
            file_keyvalues = self.metadata.metadata
            common_keyvalues = (self.common_metadata.metadata
                                if self.common_metadata is not None
                                else None)

            if file_keyvalues and b'pandas' in file_keyvalues:
                index_columns = _get_pandas_index_columns(file_keyvalues)
            elif common_keyvalues and b'pandas' in common_keyvalues:
                index_columns = _get_pandas_index_columns(common_keyvalues)
            else:
                index_columns = []

            if indices is not None and index_columns:
                indices += map(self.reader.column_name_idx, index_columns)

        return indices


def _get_pandas_index_columns(keyvalues):
    return (json.loads(keyvalues[b'pandas'].decode('utf8'))
            ['index_columns'])


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
             open_file_func=None, file=None, use_pandas_metadata=False):
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
        file : file-like object
            passed to ParquetFile

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

        options = dict(columns=columns,
                       nthreads=nthreads,
                       use_pandas_metadata=use_pandas_metadata)

        if self.row_group is not None:
            table = reader.read_row_group(self.row_group, **options)
        else:
            table = reader.read(**options)

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
        self.filesystem = filesystem or LocalFileSystem.get_instance()
        self.pathsep = pathsep
        self.dirpath = dirpath
        self.partition_scheme = partition_scheme
        self.partitions = ParquetPartitions()
        self.pieces = []

        self.common_metadata_path = None
        self.metadata_path = None

        self._visit_level(0, self.dirpath, [])

    def _visit_level(self, level, base_path, part_keys):
        fs = self.filesystem

        _, directories, files = next(fs.walk(base_path))

        filtered_files = []
        for path in files:
            full_path = self.pathsep.join((base_path, path))
            if _is_parquet_file(path):
                filtered_files.append(full_path)
            elif path.endswith('_common_metadata'):
                self.common_metadata_path = full_path
            elif path.endswith('_metadata'):
                self.metadata_path = full_path
            elif not self._should_silently_exclude(path):
                print('Ignoring path: {0}'.format(full_path))

        # ARROW-1079: Filter out "private" directories starting with underscore
        filtered_directories = [self.pathsep.join((base_path, x))
                                for x in directories
                                if not _is_private_directory(x)]

        filtered_files.sort()
        filtered_directories.sort()

        if len(files) > 0 and len(filtered_directories) > 0:
            raise ValueError('Found files in an intermediate '
                             'directory: {0}'.format(base_path))
        elif len(filtered_directories) > 0:
            self._visit_directories(level, filtered_directories, part_keys)
        else:
            self._push_pieces(filtered_files, part_keys)

    def _should_silently_exclude(self, file_name):
        return (file_name.endswith('.crc') or
                file_name in EXCLUDED_PARQUET_PATHS)

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
    filesystem : FileSystem, default None
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
            self.fs = LocalFileSystem.get_instance()
        else:
            self.fs = _ensure_filesystem(filesystem)

        self.paths = path_or_paths

        (self.pieces, self.partitions,
         self.metadata_path) = _make_manifest(path_or_paths, self.fs)

        if self.metadata_path is not None:
            with self.fs.open(self.metadata_path) as f:
                self.common_metadata = ParquetFile(f).metadata
        else:
            self.common_metadata = None

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

    def read(self, columns=None, nthreads=1, use_pandas_metadata=False):
        """
        Read multiple Parquet files as a single pyarrow.Table

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file
        nthreads : int, default 1
            Number of columns to read in parallel. Requires that the underlying
            file source is threadsafe
        use_pandas_metadata : bool, default False
            Passed through to each dataset piece

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
                               open_file_func=open_file,
                               use_pandas_metadata=use_pandas_metadata)
            tables.append(table)

        all_data = lib.concat_tables(tables)

        if use_pandas_metadata:
            # We need to ensure that this metadata is set in the Table's schema
            # so that Table.to_pandas will construct pandas.DataFrame with the
            # right index
            common_metadata = self._get_common_pandas_metadata()
            current_metadata = all_data.schema.metadata or {}

            if common_metadata and b'pandas' not in current_metadata:
                all_data = all_data.replace_schema_metadata({
                    b'pandas': common_metadata})

        return all_data

    def read_pandas(self, **kwargs):
        """
        Read dataset including pandas metadata, if any. Other arguments passed
        through to ParquetDataset.read, see docstring for further details

        Returns
        -------
        pyarrow.Table
            Content of the file as a table (of columns)
        """
        return self.read(use_pandas_metadata=True, **kwargs)

    def _get_common_pandas_metadata(self):
        if self.common_metadata is None:
            return None

        keyvalues = self.common_metadata.metadata
        return keyvalues.get(b'pandas', None)

    def _get_open_file_func(self):
        if self.fs is None or isinstance(self.fs, LocalFileSystem):
            def open_file(path, meta=None):
                return ParquetFile(path, metadata=meta,
                                   common_metadata=self.common_metadata)
        else:
            def open_file(path, meta=None):
                return ParquetFile(self.fs.open(path, mode='rb'),
                                   metadata=meta,
                                   common_metadata=self.common_metadata)
        return open_file


def _ensure_filesystem(fs):
    fs_type = type(fs)

    # If the arrow filesystem was subclassed, assume it supports the full
    # interface and return it
    if not issubclass(fs_type, FileSystem):
        for mro in inspect.getmro(fs_type):
            if mro.__name__ is 'S3FileSystem':
                return S3FSWrapper(fs)
            # In case its a simple LocalFileSystem (e.g. dask) use native arrow
            # FS
            elif mro.__name__ is 'LocalFileSystem':
                return LocalFileSystem.get_instance()

        raise IOError('Unrecognized filesystem: {0}'.format(fs_type))
    else:
        return fs


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


def read_table(source, columns=None, nthreads=1, metadata=None,
               use_pandas_metadata=False):
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
    use_pandas_metadata : boolean, default False
        If True and file has custom pandas schema metadata, ensure that
        index columns are also loaded

    Returns
    -------
    pyarrow.Table
        Content of the file as a table (of columns)
    """
    if is_string(source):
        fs = LocalFileSystem.get_instance()
        if fs.isdir(source):
            return fs.read_parquet(source, columns=columns,
                                   metadata=metadata)

    pf = ParquetFile(source, metadata=metadata)
    return pf.read(columns=columns, nthreads=nthreads,
                   use_pandas_metadata=use_pandas_metadata)


def read_pandas(source, columns=None, nthreads=1, metadata=None):
    """
    Read a Table from Parquet format, also reading DataFrame index values if
    known in the file metadata

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
    return read_table(source, columns=columns, nthreads=nthreads,
                      metadata=metadata, use_pandas_metadata=True)


def write_table(table, where, row_group_size=None, version='1.0',
                use_dictionary=True, compression='snappy',
                use_deprecated_int96_timestamps=False,
                coerce_timestamps=None, **kwargs):
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
    use_deprecated_int96_timestamps : boolean, default False
        Write nanosecond resolution timestamps to INT96 Parquet format
    coerce_timestamps : string, default None
        Cast timestamps a particular resolution.
        Valid values: {None, 'ms', 'us'}
    compression : str or dict
        Specify the compression codec, either on a general basis or per-column.
    """
    row_group_size = kwargs.get('chunk_size', row_group_size)
    options = dict(
        use_dictionary=use_dictionary,
        compression=compression,
        version=version,
        use_deprecated_int96_timestamps=use_deprecated_int96_timestamps,
        coerce_timestamps=coerce_timestamps)

    writer = None
    try:
        writer = ParquetWriter(where, table.schema, **options)
        writer.write_table(table, row_group_size=row_group_size)
    except:
        if writer is not None:
            writer.close()
        if isinstance(where, six.string_types):
            try:
                os.remove(where)
            except os.error:
                pass
        raise
    else:
        writer.close()


def write_to_dataset(table, root_path, partition_cols=None,
                     filesystem=None, preserve_index=True, **kwargs):
    """
    Wrapper around parquet.write_table for writing a Table to
    Parquet format by partitions.
    For each combination of partition columns and values,
    a subdirectories are created in the following
    manner:

    root_dir/
      group1=value1
        group2=value1
          <uuid>.parquet
        group2=value2
          <uuid>.parquet
      group1=valueN
        group2=value1
          <uuid>.parquet
        group2=valueN
          <uuid>.parquet

    Parameters
    ----------
    table : pyarrow.Table
    root_path : string,
        The root directory of the dataset
    filesystem : FileSystem, default None
        If nothing passed, paths assumed to be found in the local on-disk
        filesystem
    partition_cols : list,
        Column names by which to partition the dataset
        Columns are partitioned in the order they are given
    preserve_index : bool,
        Parameter for instantiating Table; preserve pandas index or not.
    **kwargs : dict, kwargs for write_table function.
    """
    from pyarrow import (
        Table,
        compat
    )

    if filesystem is None:
        fs = LocalFileSystem.get_instance()
    else:
        fs = _ensure_filesystem(filesystem)

    if not fs.exists(root_path):
        fs.mkdir(root_path)

    if partition_cols is not None and len(partition_cols) > 0:
        df = table.to_pandas()
        partition_keys = [df[col] for col in partition_cols]
        data_df = df.drop(partition_cols, axis='columns')
        data_cols = df.columns.drop(partition_cols)
        if len(data_cols) == 0:
            raise ValueError("No data left to save outside partition columns")
        for keys, subgroup in data_df.groupby(partition_keys):
            if not isinstance(keys, tuple):
                keys = (keys,)
            subdir = "/".join(
                ["{colname}={value}".format(colname=name, value=val)
                 for name, val in zip(partition_cols, keys)])
            subtable = Table.from_pandas(subgroup,
                                         preserve_index=preserve_index)
            prefix = "/".join([root_path, subdir])
            if not fs.exists(prefix):
                fs.mkdir(prefix)
            outfile = compat.guid() + ".parquet"
            full_path = "/".join([prefix, outfile])
            with fs.open(full_path, 'wb') as f:
                write_table(subtable, f, **kwargs)
    else:
        outfile = compat.guid() + ".parquet"
        full_path = "/".join([root_path, outfile])
        with fs.open(full_path, 'wb') as f:
            write_table(table, f, **kwargs)


def write_metadata(schema, where, version='1.0',
                   use_deprecated_int96_timestamps=False,
                   coerce_timestamps=None):
    """
    Write metadata-only Parquet file from schema

    Parameters
    ----------
    schema : pyarrow.Schema
    where: string or pyarrow.io.NativeFile
    version : {"1.0", "2.0"}, default "1.0"
        The Parquet format version, defaults to 1.0
    use_deprecated_int96_timestamps : boolean, default False
        Write nanosecond resolution timestamps to INT96 Parquet format
    coerce_timestamps : string, default None
        Cast timestamps a particular resolution.
        Valid values: {None, 'ms', 'us'}
    """
    options = dict(
        version=version,
        use_deprecated_int96_timestamps=use_deprecated_int96_timestamps,
        coerce_timestamps=coerce_timestamps
    )
    writer = ParquetWriter(where, schema, **options)
    writer.close()


def read_metadata(where):
    """
    Read FileMetadata from footer of a single Parquet file

    Parameters
    ----------
    where : string (filepath) or file-like object

    Returns
    -------
    metadata : FileMetadata
    """
    return ParquetFile(where).metadata


def read_schema(where):
    """
    Read effective Arrow schema from Parquet file metadata

    Parameters
    ----------
    where : string (filepath) or file-like object

    Returns
    -------
    schema : pyarrow.Schema
    """
    return ParquetFile(where).schema.to_arrow_schema()

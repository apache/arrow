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

from __future__ import absolute_import

from collections import defaultdict
from concurrent import futures
from functools import partial

from six.moves.urllib.parse import urlparse
import json
import numpy as np
import os
import re
import six
import warnings

import pyarrow as pa
import pyarrow.lib as lib
import pyarrow._parquet as _parquet

from pyarrow._parquet import (ParquetReader, Statistics,  # noqa
                              FileMetaData, RowGroupMetaData,
                              ColumnChunkMetaData,
                              ParquetSchema, ColumnSchema)
from pyarrow.compat import guid
from pyarrow.filesystem import (LocalFileSystem, _ensure_filesystem,
                                resolve_filesystem_and_path)
from pyarrow.util import _is_path_like, _stringify_path

_URI_STRIP_SCHEMES = ('hdfs',)


def _parse_uri(path):
    path = _stringify_path(path)
    parsed_uri = urlparse(path)
    if parsed_uri.scheme in _URI_STRIP_SCHEMES:
        return parsed_uri.path
    else:
        # ARROW-4073: On Windows returning the path with the scheme
        # stripped removes the drive letter, if any
        return path


def _get_filesystem_and_path(passed_filesystem, path):
    if passed_filesystem is None:
        return resolve_filesystem_and_path(path, passed_filesystem)
    else:
        passed_filesystem = _ensure_filesystem(passed_filesystem)
        parsed_path = _parse_uri(path)
        return passed_filesystem, parsed_path


def _check_contains_null(val):
    if isinstance(val, six.binary_type):
        for byte in val:
            if isinstance(byte, six.binary_type):
                compare_to = chr(0)
            else:
                compare_to = 0
            if byte == compare_to:
                return True
    elif isinstance(val, six.text_type):
        return u'\x00' in val
    return False


def _check_filters(filters):
    """
    Check if filters are well-formed.
    """
    if filters is not None:
        if len(filters) == 0 or any(len(f) == 0 for f in filters):
            raise ValueError("Malformed filters")
        if isinstance(filters[0][0], six.string_types):
            # We have encountered the situation where we have one nesting level
            # too few:
            #   We have [(,,), ..] instead of [[(,,), ..]]
            filters = [filters]
        for conjunction in filters:
            for col, op, val in conjunction:
                if (
                    isinstance(val, list)
                    and all(_check_contains_null(v) for v in val)
                    or _check_contains_null(val)
                ):
                    raise NotImplementedError(
                        "Null-terminated binary strings are not supported as"
                        " filter values."
                    )
    return filters

# ----------------------------------------------------------------------
# Reading a single Parquet file


class ParquetFile(object):
    """
    Reader interface for a single Parquet file

    Parameters
    ----------
    source : str, pathlib.Path, pyarrow.NativeFile, or file-like object
        Readable source. For passing bytes or buffer-like file containing a
        Parquet file, use pyarorw.BufferReader
    metadata : FileMetaData, default None
        Use existing metadata object, rather than reading from file.
    common_metadata : FileMetaData, default None
        Will be used in reads for pandas schema metadata if not found in the
        main file's metadata, no other uses at the moment
    memory_map : boolean, default True
        If the source is a file path, use a memory map to read file, which can
        improve performance in some environments
    """
    def __init__(self, source, metadata=None, common_metadata=None,
                 read_dictionary=None, memory_map=True):
        self.reader = ParquetReader()
        self.reader.open(source, use_memory_map=memory_map,
                         read_dictionary=read_dictionary, metadata=metadata)
        self.common_metadata = common_metadata
        self._nested_paths_by_prefix = self._build_nested_paths()

    def _build_nested_paths(self):
        paths = self.reader.column_paths

        result = defaultdict(list)

        def _visit_piece(i, key, rest):
            result[key].append(i)

            if len(rest) > 0:
                nested_key = '.'.join((key, rest[0]))
                _visit_piece(i, nested_key, rest[1:])

        for i, path in enumerate(paths):
            _visit_piece(i, path[0], path[1:])

        return result

    @property
    def metadata(self):
        return self.reader.metadata

    @property
    def schema(self):
        return self.metadata.schema

    @property
    def num_row_groups(self):
        return self.reader.num_row_groups

    def read_row_group(self, i, columns=None, use_threads=True,
                       use_pandas_metadata=False):
        """
        Read a single row group from a Parquet file

        Parameters
        ----------
        columns: list
            If not None, only these columns will be read from the row group. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'
        use_threads : boolean, default True
            Perform multi-threaded column reads
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
                                          use_threads=use_threads)

    def read(self, columns=None, use_threads=True, use_pandas_metadata=False):
        """
        Read a Table from Parquet format

        Parameters
        ----------
        columns: list
            If not None, only these columns will be read from the file. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'
        use_threads : boolean, default True
            Perform multi-threaded column reads
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
                                    use_threads=use_threads)

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

        indices = []

        for name in column_names:
            if name in self._nested_paths_by_prefix:
                indices.extend(self._nested_paths_by_prefix[name])

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
                indices += [self.reader.column_name_idx(descr)
                            for descr in index_columns
                            if not isinstance(descr, dict)]

        return indices


_SPARK_DISALLOWED_CHARS = re.compile('[ ,;{}()\n\t=]')


def _sanitized_spark_field_name(name):
    return _SPARK_DISALLOWED_CHARS.sub('_', name)


def _sanitize_schema(schema, flavor):
    if 'spark' in flavor:
        sanitized_fields = []

        schema_changed = False

        for field in schema:
            name = field.name
            sanitized_name = _sanitized_spark_field_name(name)

            if sanitized_name != name:
                schema_changed = True
                sanitized_field = pa.field(sanitized_name, field.type,
                                           field.nullable, field.metadata)
                sanitized_fields.append(sanitized_field)
            else:
                sanitized_fields.append(field)

        new_schema = pa.schema(sanitized_fields, metadata=schema.metadata)
        return new_schema, schema_changed
    else:
        return schema, False


def _sanitize_table(table, new_schema, flavor):
    # TODO: This will not handle prohibited characters in nested field names
    if 'spark' in flavor:
        column_data = [table[i].data for i in range(table.num_columns)]
        return pa.Table.from_arrays(column_data, schema=new_schema)
    else:
        return table


_parquet_writer_arg_docs = """version : {"1.0", "2.0"}, default "1.0"
    The Parquet format version, defaults to 1.0
use_dictionary : bool or list
    Specify if we should use dictionary encoding in general or only for
    some columns.
use_deprecated_int96_timestamps : boolean, default None
    Write timestamps to INT96 Parquet format. Defaults to False unless enabled
    by flavor argument. This take priority over the coerce_timestamps option.
coerce_timestamps : string, default None
    Cast timestamps a particular resolution.
    Valid values: {None, 'ms', 'us'}
data_page_size : int, default None
    Set a target threshhold for the approximate encoded size of data
    pages within a column chunk. If None, use the default data page
    size of 1MByte.
allow_truncated_timestamps : boolean, default False
    Allow loss of data when coercing timestamps to a particular
    resolution. E.g. if microsecond or nanosecond data is lost when coercing to
    'ms', do not raise an exception
compression : str or dict
    Specify the compression codec, either on a general basis or per-column.
    Valid values: {'NONE', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI', 'LZ4', 'ZSTD'}
write_statistics : bool or list
    Specify if we should write statistics in general (default is True) or only
    for some columns.
flavor : {'spark'}, default None
    Sanitize schema or set other compatibility options to work with
    various target systems
filesystem : FileSystem, default None
    If nothing passed, will be inferred from `where` if path-like, else
    `where` is already a file-like object so no filesystem is needed."""


class ParquetWriter(object):

    __doc__ = """
Class for incrementally building a Parquet file for Arrow tables

Parameters
----------
where : path or file-like object
schema : arrow Schema
{0}
**options : dict
    If options contains a key `metadata_collector` then the
    corresponding value is assumed to be a list (or any object with
    `.append` method) that will be filled with file metadata instances
    of dataset pieces.
""".format(_parquet_writer_arg_docs)

    def __init__(self, where, schema, filesystem=None,
                 flavor=None,
                 version='1.0',
                 use_dictionary=True,
                 compression='snappy',
                 write_statistics=True,
                 use_deprecated_int96_timestamps=None, **options):
        if use_deprecated_int96_timestamps is None:
            # Use int96 timestamps for Spark
            if flavor is not None and 'spark' in flavor:
                use_deprecated_int96_timestamps = True
            else:
                use_deprecated_int96_timestamps = False

        self.flavor = flavor
        if flavor is not None:
            schema, self.schema_changed = _sanitize_schema(schema, flavor)
        else:
            self.schema_changed = False

        self.schema = schema
        self.where = where

        # If we open a file using a filesystem, store file handle so we can be
        # sure to close it when `self.close` is called.
        self.file_handle = None

        filesystem, path = resolve_filesystem_and_path(where, filesystem)
        if filesystem is not None:
            sink = self.file_handle = filesystem.open(path, 'wb')
        else:
            sink = where
        self._metadata_collector = options.pop('metadata_collector', None)
        self.writer = _parquet.ParquetWriter(
            sink, schema,
            version=version,
            compression=compression,
            use_dictionary=use_dictionary,
            write_statistics=write_statistics,
            use_deprecated_int96_timestamps=use_deprecated_int96_timestamps,
            **options)
        self.is_open = True

    def __del__(self):
        if getattr(self, 'is_open', False):
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()
        # return false since we want to propagate exceptions
        return False

    def write_table(self, table, row_group_size=None):
        if self.schema_changed:
            table = _sanitize_table(table, self.schema, self.flavor)
        assert self.is_open

        if not table.schema.equals(self.schema, check_metadata=False):
            msg = ('Table schema does not match schema used to create file: '
                   '\ntable:\n{0!s} vs. \nfile:\n{1!s}'.format(table.schema,
                                                               self.schema))
            raise ValueError(msg)

        self.writer.write_table(table, row_group_size=row_group_size)

    def close(self):
        if self.is_open:
            self.writer.close()
            self.is_open = False
            if self._metadata_collector is not None:
                self._metadata_collector.append(self.writer.metadata)
        if self.file_handle is not None:
            self.file_handle.close()


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
    path : str or pathlib.Path
        Path to file in the file system where this piece is located
    open_file_func : callable
        Function to use for obtaining file handle to dataset piece
    partition_keys : list of tuples
      [(column name, ordinal index)]
    row_group : int, default None
        Row group to load. By default, reads all row groups
    """
    def __init__(self, path, open_file_func=partial(open, mode='rb'),
                 file_options=None, row_group=None, partition_keys=None):
        self.path = _stringify_path(path)
        self.open_file_func = open_file_func
        self.row_group = row_group
        self.partition_keys = partition_keys or []
        self.file_options = file_options or {}

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

    def get_metadata(self):
        """
        Returns the file's metadata

        Returns
        -------
        metadata : FileMetaData
        """
        f = self.open()
        return f.metadata

    def open(self):
        """
        Returns instance of ParquetFile
        """
        reader = self.open_file_func(self.path)
        if not isinstance(reader, ParquetFile):
            reader = ParquetFile(reader, **self.file_options)
        return reader

    def read(self, columns=None, use_threads=True, partitions=None,
             file=None, use_pandas_metadata=False):
        """
        Read this piece as a pyarrow.Table

        Parameters
        ----------
        columns : list of column names, default None
        use_threads : boolean, default True
            Perform multi-threaded column reads
        partitions : ParquetPartitions, default None
        file : file-like object
            passed to ParquetFile

        Returns
        -------
        table : pyarrow.Table
        """
        if self.open_file_func is not None:
            reader = self.open()
        elif file is not None:
            reader = ParquetFile(file, **self.file_options)
        else:
            # try to read the local path
            reader = ParquetFile(self.path, **self.file_options)

        options = dict(columns=columns,
                       use_threads=use_threads,
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

                arr = pa.DictionaryArray.from_arrays(indices, dictionary)
                table = table.append_column(name, arr)

        return table


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

    def equals(self, other):
        if not isinstance(other, ParquetPartitions):
            raise TypeError('`other` must be an instance of ParquetPartitions')

        return (self.levels == other.levels and
                self.partition_names == other.partition_names)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def __ne__(self, other):
        # required for python 2, cython implements it by default
        return not (self == other)

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

    def filter_accepts_partition(self, part_key, filter, level):
        p_column, p_value_index = part_key
        f_column, op, f_value = filter
        if p_column != f_column:
            return True

        f_type = type(f_value)

        if isinstance(f_value, set):
            if not f_value:
                raise ValueError("Cannot use empty set as filter value")
            if op not in {'in', 'not in'}:
                raise ValueError("Op '%s' not supported with set value",
                                 op)
            if len(set([type(item) for item in f_value])) != 1:
                raise ValueError("All elements of set '%s' must be of"
                                 " same type", f_value)
            f_type = type(next(iter(f_value)))

        p_value = f_type((self.levels[level]
                          .dictionary[p_value_index]
                          .as_py()))

        if op == "=" or op == "==":
            return p_value == f_value
        elif op == "!=":
            return p_value != f_value
        elif op == '<':
            return p_value < f_value
        elif op == '>':
            return p_value > f_value
        elif op == '<=':
            return p_value <= f_value
        elif op == '>=':
            return p_value >= f_value
        elif op == 'in':
            return p_value in f_value
        elif op == 'not in':
            return p_value not in f_value
        else:
            raise ValueError("'%s' is not a valid operator in predicates.",
                             filter[1])


class ParquetManifest(object):
    """

    """
    def __init__(self, dirpath, open_file_func=None, filesystem=None,
                 pathsep='/', partition_scheme='hive', metadata_nthreads=1):
        filesystem, dirpath = _get_filesystem_and_path(filesystem, dirpath)
        self.filesystem = filesystem
        self.open_file_func = open_file_func
        self.pathsep = pathsep
        self.dirpath = _stringify_path(dirpath)
        self.partition_scheme = partition_scheme
        self.partitions = ParquetPartitions()
        self.pieces = []
        self._metadata_nthreads = metadata_nthreads
        self._thread_pool = futures.ThreadPoolExecutor(
            max_workers=metadata_nthreads)

        self.common_metadata_path = None
        self.metadata_path = None

        self._visit_level(0, self.dirpath, [])

        # Due to concurrency, pieces will potentially by out of order if the
        # dataset is partitioned so we sort them to yield stable results
        self.pieces.sort(key=lambda piece: piece.path)

        if self.common_metadata_path is None:
            # _common_metadata is a subset of _metadata
            self.common_metadata_path = self.metadata_path

        self._thread_pool.shutdown()

    def _visit_level(self, level, base_path, part_keys):
        fs = self.filesystem

        _, directories, files = next(fs.walk(base_path))

        filtered_files = []
        for path in files:
            full_path = self.pathsep.join((base_path, path))
            if path.endswith('_common_metadata'):
                self.common_metadata_path = full_path
            elif path.endswith('_metadata'):
                self.metadata_path = full_path
            elif self._should_silently_exclude(path):
                continue
            else:
                filtered_files.append(full_path)

        # ARROW-1079: Filter out "private" directories starting with underscore
        filtered_directories = [self.pathsep.join((base_path, x))
                                for x in directories
                                if not _is_private_directory(x)]

        filtered_files.sort()
        filtered_directories.sort()

        if len(filtered_files) > 0 and len(filtered_directories) > 0:
            raise ValueError('Found files in an intermediate '
                             'directory: {0}'.format(base_path))
        elif len(filtered_directories) > 0:
            self._visit_directories(level, filtered_directories, part_keys)
        else:
            self._push_pieces(filtered_files, part_keys)

    def _should_silently_exclude(self, file_name):
        return (file_name.endswith('.crc') or  # Checksums
                file_name.endswith('_$folder$') or  # HDFS directories in S3
                file_name.startswith('.') or  # Hidden files starting with .
                file_name.startswith('_') or  # Hidden files starting with _
                file_name in EXCLUDED_PARQUET_PATHS)

    def _visit_directories(self, level, directories, part_keys):
        futures_list = []
        for path in directories:
            head, tail = _path_split(path, self.pathsep)
            name, key = _parse_hive_partition(tail)

            index = self.partitions.get_index(level, name, key)
            dir_part_keys = part_keys + [(name, index)]
            # If you have less threads than levels, the wait call will block
            # indefinitely due to multiple waits within a thread.
            if level < self._metadata_nthreads:
                future = self._thread_pool.submit(self._visit_level,
                                                  level + 1,
                                                  path,
                                                  dir_part_keys)
                futures_list.append(future)
            else:
                self._visit_level(level + 1, path, dir_part_keys)
        if futures_list:
            futures.wait(futures_list)

    def _parse_partition(self, dirname):
        if self.partition_scheme == 'hive':
            return _parse_hive_partition(dirname)
        else:
            raise NotImplementedError('partition schema: {0}'
                                      .format(self.partition_scheme))

    def _push_pieces(self, files, part_keys):
        self.pieces.extend([
            ParquetDatasetPiece(path, partition_keys=part_keys,
                                open_file_func=self.open_file_func)
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


def _open_dataset_file(dataset, path, meta=None):
    if dataset.fs is None or isinstance(dataset.fs, LocalFileSystem):
        return ParquetFile(path, metadata=meta, memory_map=dataset.memory_map,
                           read_dictionary=dataset.read_dictionary,
                           common_metadata=dataset.common_metadata)
    else:
        return ParquetFile(dataset.fs.open(path, mode='rb'), metadata=meta,
                           memory_map=dataset.memory_map,
                           read_dictionary=dataset.read_dictionary,
                           common_metadata=dataset.common_metadata)


_read_docstring_common = """\
read_dictionary : list, default None
    List of names or column paths (for nested types) to read directly
    as DictionaryArray. Only supported for BYTE_ARRAY storage. To read
    a flat column as dictionary-encoded pass the column name. For
    nested types, you must pass the full column "path", which could be
    something like level1.level2.list.item. Refer to the Parquet
    file's schema to obtain the paths.
memory_map : boolean, default True
    If the source is a file path, use a memory map to read file, which can
    improve performance in some environments"""


class ParquetDataset(object):

    __doc__ = """
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
filters : List[Tuple] or List[List[Tuple]] or None (default)
    List of filters to apply, like ``[[('x', '=', 0), ...], ...]``. This
    implements partition-level (hive) filtering only, i.e., to prevent the
    loading of some files of the dataset.

    Predicates are expressed in disjunctive normal form (DNF). This means
    that the innermost tuple describe a single column predicate. These
    inner predicate make are all combined with a conjunction (AND) into a
    larger predicate. The most outer list then combines all filters
    with a disjunction (OR). By this, we should be able to express all
    kinds of filters that are possible using boolean logic.

    This function also supports passing in as List[Tuple]. These predicates
    are evaluated as a conjunction. To express OR in predictates, one must
    use the (preferred) List[List[Tuple]] notation.
metadata_nthreads: int, default 1
    How many threads to allow the thread pool which is used to read the
    dataset metadata. Increasing this is helpful to read partitioned
    datasets.
{0}
""".format(_read_docstring_common)

    def __init__(self, path_or_paths, filesystem=None, schema=None,
                 metadata=None, split_row_groups=False, validate_schema=True,
                 filters=None, metadata_nthreads=1,
                 read_dictionary=None, memory_map=True):
        a_path = path_or_paths
        if isinstance(a_path, list):
            a_path = a_path[0]

        self.fs, _ = _get_filesystem_and_path(filesystem, a_path)
        if isinstance(path_or_paths, list):
            self.paths = [_parse_uri(path) for path in path_or_paths]
        else:
            self.paths = _parse_uri(path_or_paths)

        self.read_dictionary = read_dictionary
        self.memory_map = memory_map

        (self.pieces,
         self.partitions,
         self.common_metadata_path,
         self.metadata_path) = _make_manifest(
             path_or_paths, self.fs, metadata_nthreads=metadata_nthreads,
             open_file_func=partial(_open_dataset_file, self))

        if self.common_metadata_path is not None:
            with self.fs.open(self.common_metadata_path) as f:
                self.common_metadata = read_metadata(f, memory_map=memory_map)
        else:
            self.common_metadata = None

        if metadata is None and self.metadata_path is not None:
            with self.fs.open(self.metadata_path) as f:
                self.metadata = read_metadata(f, memory_map=memory_map)
        else:
            self.metadata = metadata

        self.schema = schema

        self.split_row_groups = split_row_groups

        if split_row_groups:
            raise NotImplementedError("split_row_groups not yet implemented")

        if filters is not None:
            filters = _check_filters(filters)
            self._filter(filters)

        if validate_schema:
            self.validate_schemas()

    def equals(self, other):
        if not isinstance(other, ParquetDataset):
            raise TypeError('`other` must be an instance of ParquetDataset')

        if self.fs.__class__ != other.fs.__class__:
            return False
        for prop in ('paths', 'memory_map', 'pieces', 'partitions',
                     'common_metadata_path', 'metadata_path',
                     'common_metadata', 'metadata', 'schema',
                     'split_row_groups'):
            if getattr(self, prop) != getattr(other, prop):
                return False

        return True

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def __ne__(self, other):
        # required for python 2, cython implements it by default
        return not (self == other)

    def validate_schemas(self):
        if self.metadata is None and self.schema is None:
            if self.common_metadata is not None:
                self.schema = self.common_metadata.schema
            else:
                self.schema = self.pieces[0].get_metadata().schema
        elif self.schema is None:
            self.schema = self.metadata.schema

        # Verify schemas are all compatible
        dataset_schema = self.schema.to_arrow_schema()
        # Exclude the partition columns from the schema, they are provided
        # by the path, not the DatasetPiece
        if self.partitions is not None:
            for partition_name in self.partitions.partition_names:
                if dataset_schema.get_field_index(partition_name) != -1:
                    field_idx = dataset_schema.get_field_index(partition_name)
                    dataset_schema = dataset_schema.remove(field_idx)

        for piece in self.pieces:
            file_metadata = piece.get_metadata()
            file_schema = file_metadata.schema.to_arrow_schema()
            if not dataset_schema.equals(file_schema, check_metadata=False):
                raise ValueError('Schema in {0!s} was different. \n'
                                 '{1!s}\n\nvs\n\n{2!s}'
                                 .format(piece, file_schema,
                                         dataset_schema))

    def read(self, columns=None, use_threads=True, use_pandas_metadata=False):
        """
        Read multiple Parquet files as a single pyarrow.Table

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file
        use_threads : boolean, default True
            Perform multi-threaded column reads
        use_pandas_metadata : bool, default False
            Passed through to each dataset piece

        Returns
        -------
        pyarrow.Table
            Content of the file as a table (of columns)
        """
        tables = []
        for piece in self.pieces:
            table = piece.read(columns=columns, use_threads=use_threads,
                               partitions=self.partitions,
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

    def _filter(self, filters):
        accepts_filter = self.partitions.filter_accepts_partition

        def one_filter_accepts(piece, filter):
            return all(accepts_filter(part_key, filter, level)
                       for level, part_key in enumerate(piece.partition_keys))

        def all_filters_accept(piece):
            return any(all(one_filter_accepts(piece, f) for f in conjunction)
                       for conjunction in filters)

        self.pieces = [p for p in self.pieces if all_filters_accept(p)]


def _make_manifest(path_or_paths, fs, pathsep='/', metadata_nthreads=1,
                   open_file_func=None):
    partitions = None
    common_metadata_path = None
    metadata_path = None

    if isinstance(path_or_paths, list) and len(path_or_paths) == 1:
        # Dask passes a directory as a list of length 1
        path_or_paths = path_or_paths[0]

    if _is_path_like(path_or_paths) and fs.isdir(path_or_paths):
        manifest = ParquetManifest(path_or_paths, filesystem=fs,
                                   open_file_func=open_file_func,
                                   pathsep=fs.pathsep,
                                   metadata_nthreads=metadata_nthreads)
        common_metadata_path = manifest.common_metadata_path
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
            piece = ParquetDatasetPiece(path, open_file_func=open_file_func)
            pieces.append(piece)

    return pieces, partitions, common_metadata_path, metadata_path


_read_table_docstring = """
{0}

Parameters
----------
source: str, pyarrow.NativeFile, or file-like object
    If a string passed, can be a single file name or directory name. For
    file-like objects, only read a single file. Use pyarrow.BufferReader to
    read a file contained in a bytes or buffer-like object
columns: list
    If not None, only these columns will be read from the file. A column
    name may be a prefix of a nested field, e.g. 'a' will select 'a.b',
    'a.c', and 'a.d.e'
use_threads : boolean, default True
    Perform multi-threaded column reads
metadata : FileMetaData
    If separately computed
{1}
filters : List[Tuple] or List[List[Tuple]] or None (default)
    List of filters to apply, like ``[[('x', '=', 0), ...], ...]``. This
    implements partition-level (hive) filtering only, i.e., to prevent the
    loading of some files of the dataset if `source` is a directory.
    See the docstring of ParquetDataset for more details.

Returns
-------
{2}
"""


def read_table(source, columns=None, use_threads=True, metadata=None,
               use_pandas_metadata=False, memory_map=True,
               read_dictionary=None, filesystem=None, filters=None):
    if _is_path_like(source):
        pf = ParquetDataset(source, metadata=metadata, memory_map=memory_map,
                            read_dictionary=read_dictionary,
                            filesystem=filesystem, filters=filters)
    else:
        pf = ParquetFile(source, metadata=metadata,
                         read_dictionary=read_dictionary,
                         memory_map=memory_map)
    return pf.read(columns=columns, use_threads=use_threads,
                   use_pandas_metadata=use_pandas_metadata)


read_table.__doc__ = _read_table_docstring.format(
    'Read a Table from Parquet format',
    "\n".join((_read_docstring_common,
               """use_pandas_metadata : boolean, default False
    If True and file has custom pandas schema metadata, ensure that
    index columns are also loaded""")),
    """pyarrow.Table
    Content of the file as a table (of columns)""")


def read_pandas(source, columns=None, use_threads=True, memory_map=True,
                metadata=None, filters=None):
    return read_table(source, columns=columns,
                      use_threads=use_threads,
                      metadata=metadata, memory_map=True,
                      filters=filters,
                      use_pandas_metadata=True)


read_pandas.__doc__ = _read_table_docstring.format(
    'Read a Table from Parquet format, also reading DataFrame\n'
    'index values if known in the file metadata',
    _read_docstring_common,
    """pyarrow.Table
    Content of the file as a Table of Columns, including DataFrame
    indexes as columns""")


def write_table(table, where, row_group_size=None, version='1.0',
                use_dictionary=True, compression='snappy',
                write_statistics=True,
                use_deprecated_int96_timestamps=None,
                coerce_timestamps=None,
                allow_truncated_timestamps=False,
                data_page_size=None, flavor=None,
                filesystem=None, **kwargs):
    row_group_size = kwargs.pop('chunk_size', row_group_size)
    use_int96 = use_deprecated_int96_timestamps
    try:
        with ParquetWriter(
                where, table.schema,
                filesystem=filesystem,
                version=version,
                flavor=flavor,
                use_dictionary=use_dictionary,
                write_statistics=write_statistics,
                coerce_timestamps=coerce_timestamps,
                data_page_size=data_page_size,
                allow_truncated_timestamps=allow_truncated_timestamps,
                compression=compression,
                use_deprecated_int96_timestamps=use_int96,
                **kwargs) as writer:
            writer.write_table(table, row_group_size=row_group_size)
    except Exception:
        if _is_path_like(where):
            try:
                os.remove(_stringify_path(where))
            except os.error:
                pass
        raise


write_table.__doc__ = """
Write a Table to Parquet format

Parameters
----------
table : pyarrow.Table
where: string or pyarrow.NativeFile
{0}
""".format(_parquet_writer_arg_docs)


def _mkdir_if_not_exists(fs, path):
    if fs._isfilestore() and not fs.exists(path):
        try:
            fs.mkdir(path)
        except OSError:
            assert fs.exists(path)


def write_to_dataset(table, root_path, partition_cols=None, filesystem=None,
                     preserve_index=None, **kwargs):
    """Wrapper around parquet.write_table for writing a Table to
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
    **kwargs : dict,
        kwargs for write_table function. Using `metadata_collector` in
        kwargs allows one to collect the file metadata instances of
        dataset pieces. See docstring for `write_table` or
        `ParquetWriter` for more information.
    """
    if preserve_index is not None:
        warnings.warn('preserve_index argument is deprecated as of 0.13.0 and '
                      'has no effect', DeprecationWarning)

    fs, root_path = _get_filesystem_and_path(filesystem, root_path)

    _mkdir_if_not_exists(fs, root_path)

    if partition_cols is not None and len(partition_cols) > 0:
        df = table.to_pandas(ignore_metadata=True)
        partition_keys = [df[col] for col in partition_cols]
        data_df = df.drop(partition_cols, axis='columns')
        data_cols = df.columns.drop(partition_cols)
        if len(data_cols) == 0:
            raise ValueError('No data left to save outside partition columns')

        subschema = table.schema

        # ARROW-2891: Ensure the output_schema is preserved when writing a
        # partitioned dataset
        for col in table.schema.names:
            if col in partition_cols:
                subschema = subschema.remove(subschema.get_field_index(col))

        for keys, subgroup in data_df.groupby(partition_keys):
            if not isinstance(keys, tuple):
                keys = (keys,)
            subdir = '/'.join(
                ['{colname}={value}'.format(colname=name, value=val)
                 for name, val in zip(partition_cols, keys)])
            subtable = pa.Table.from_pandas(subgroup, preserve_index=False,
                                            schema=subschema, safe=False)
            prefix = '/'.join([root_path, subdir])
            _mkdir_if_not_exists(fs, prefix)
            outfile = guid() + '.parquet'
            full_path = '/'.join([prefix, outfile])
            with fs.open(full_path, 'wb') as f:
                write_table(subtable, f, **kwargs)
    else:
        outfile = guid() + '.parquet'
        full_path = '/'.join([root_path, outfile])
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
    where: string or pyarrow.NativeFile
    version : {"1.0", "2.0"}, default "1.0"
        The Parquet format version, defaults to 1.0
    use_deprecated_int96_timestamps : boolean, default False
        Write nanosecond resolution timestamps to INT96 Parquet format
    coerce_timestamps : string, default None
        Cast timestamps a particular resolution.
        Valid values: {None, 'ms', 'us'}
    filesystem : FileSystem, default None
        If nothing passed, paths assumed to be found in the local on-disk
        filesystem
    """
    writer = ParquetWriter(
        where, schema, version=version,
        use_deprecated_int96_timestamps=use_deprecated_int96_timestamps,
        coerce_timestamps=coerce_timestamps)
    writer.close()


def read_metadata(where, memory_map=False):
    """
    Read FileMetadata from footer of a single Parquet file

    Parameters
    ----------
    where : string (filepath) or file-like object
    memory_map : boolean, default False
        Create memory map when the source is a file path

    Returns
    -------
    metadata : FileMetadata
    """
    return ParquetFile(where, memory_map=memory_map).metadata


def read_schema(where, memory_map=False):
    """
    Read effective Arrow schema from Parquet file metadata

    Parameters
    ----------
    where : string (filepath) or file-like object
    memory_map : boolean, default False
        Create memory map when the source is a file path

    Returns
    -------
    schema : pyarrow.Schema
    """
    return ParquetFile(where, memory_map=memory_map).schema.to_arrow_schema()

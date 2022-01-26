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


from collections import defaultdict
from concurrent import futures
from functools import partial, reduce

import json
from collections.abc import Collection
import numpy as np
import os
import re
import operator
import urllib.parse
import warnings

import pyarrow as pa
import pyarrow.lib as lib
import pyarrow._parquet as _parquet

from pyarrow._parquet import (ParquetReader, Statistics,  # noqa
                              FileMetaData, RowGroupMetaData,
                              ColumnChunkMetaData,
                              ParquetSchema, ColumnSchema)
from pyarrow.fs import (LocalFileSystem, FileSystem,
                        _resolve_filesystem_and_path, _ensure_filesystem)
from pyarrow import filesystem as legacyfs
from pyarrow.util import guid, _is_path_like, _stringify_path

_URI_STRIP_SCHEMES = ('hdfs',)


def _parse_uri(path):
    path = _stringify_path(path)
    parsed_uri = urllib.parse.urlparse(path)
    if parsed_uri.scheme in _URI_STRIP_SCHEMES:
        return parsed_uri.path
    else:
        # ARROW-4073: On Windows returning the path with the scheme
        # stripped removes the drive letter, if any
        return path


def _get_filesystem_and_path(passed_filesystem, path):
    if passed_filesystem is None:
        return legacyfs.resolve_filesystem_and_path(path, passed_filesystem)
    else:
        passed_filesystem = legacyfs._ensure_filesystem(passed_filesystem)
        parsed_path = _parse_uri(path)
        return passed_filesystem, parsed_path


def _check_contains_null(val):
    if isinstance(val, bytes):
        for byte in val:
            if isinstance(byte, bytes):
                compare_to = chr(0)
            else:
                compare_to = 0
            if byte == compare_to:
                return True
    elif isinstance(val, str):
        return '\x00' in val
    return False


def _check_filters(filters, check_null_strings=True):
    """
    Check if filters are well-formed.
    """
    if filters is not None:
        if len(filters) == 0 or any(len(f) == 0 for f in filters):
            raise ValueError("Malformed filters")
        if isinstance(filters[0][0], str):
            # We have encountered the situation where we have one nesting level
            # too few:
            #   We have [(,,), ..] instead of [[(,,), ..]]
            filters = [filters]
        if check_null_strings:
            for conjunction in filters:
                for col, op, val in conjunction:
                    if (
                        isinstance(val, list) and
                        all(_check_contains_null(v) for v in val) or
                        _check_contains_null(val)
                    ):
                        raise NotImplementedError(
                            "Null-terminated binary strings are not supported "
                            "as filter values."
                        )
    return filters


_DNF_filter_doc = """Predicates are expressed in disjunctive normal form (DNF), like
    ``[[('x', '=', 0), ...], ...]``. DNF allows arbitrary boolean logical
    combinations of single column predicates. The innermost tuples each
    describe a single column predicate. The list of inner predicates is
    interpreted as a conjunction (AND), forming a more selective and
    multiple column predicate. Finally, the most outer list combines these
    filters as a disjunction (OR).

    Predicates may also be passed as List[Tuple]. This form is interpreted
    as a single conjunction. To express OR in predicates, one must
    use the (preferred) List[List[Tuple]] notation.

    Each tuple has format: (``key``, ``op``, ``value``) and compares the
    ``key`` with the ``value``.
    The supported ``op`` are:  ``=`` or ``==``, ``!=``, ``<``, ``>``, ``<=``,
    ``>=``, ``in`` and ``not in``. If the ``op`` is ``in`` or ``not in``, the
    ``value`` must be a collection such as a ``list``, a ``set`` or a
    ``tuple``.

    Examples:

    .. code-block:: python

        ('x', '=', 0)
        ('y', 'in', ['a', 'b', 'c'])
        ('z', 'not in', {'a','b'})

    """


def _filters_to_expression(filters):
    """
    Check if filters are well-formed.

    See _DNF_filter_doc above for more details.
    """
    import pyarrow.dataset as ds

    if isinstance(filters, ds.Expression):
        return filters

    filters = _check_filters(filters, check_null_strings=False)

    def convert_single_predicate(col, op, val):
        field = ds.field(col)

        if op == "=" or op == "==":
            return field == val
        elif op == "!=":
            return field != val
        elif op == '<':
            return field < val
        elif op == '>':
            return field > val
        elif op == '<=':
            return field <= val
        elif op == '>=':
            return field >= val
        elif op == 'in':
            return field.isin(val)
        elif op == 'not in':
            return ~field.isin(val)
        else:
            raise ValueError(
                '"{0}" is not a valid operator in predicates.'.format(
                    (col, op, val)))

    disjunction_members = []

    for conjunction in filters:
        conjunction_members = [
            convert_single_predicate(col, op, val)
            for col, op, val in conjunction
        ]

        disjunction_members.append(reduce(operator.and_, conjunction_members))

    return reduce(operator.or_, disjunction_members)


# ----------------------------------------------------------------------
# Reading a single Parquet file


class ParquetFile:
    """
    Reader interface for a single Parquet file.

    Parameters
    ----------
    source : str, pathlib.Path, pyarrow.NativeFile, or file-like object
        Readable source. For passing bytes or buffer-like file containing a
        Parquet file, use pyarrow.BufferReader.
    metadata : FileMetaData, default None
        Use existing metadata object, rather than reading from file.
    common_metadata : FileMetaData, default None
        Will be used in reads for pandas schema metadata if not found in the
        main file's metadata, no other uses at the moment.
    memory_map : bool, default False
        If the source is a file path, use a memory map to read file, which can
        improve performance in some environments.
    buffer_size : int, default 0
        If positive, perform read buffering when deserializing individual
        column chunks. Otherwise IO calls are unbuffered.
    pre_buffer : bool, default False
        Coalesce and issue file reads in parallel to improve performance on
        high-latency filesystems (e.g. S3). If True, Arrow will use a
        background I/O thread pool.
    read_dictionary : list
        List of column names to read directly as DictionaryArray.
    coerce_int96_timestamp_unit : str, default None.
        Cast timestamps that are stored in INT96 format to a particular
        resolution (e.g. 'ms'). Setting to None is equivalent to 'ns'
        and therefore INT96 timestamps will be infered as timestamps
        in nanoseconds.
    """

    def __init__(self, source, metadata=None, common_metadata=None,
                 read_dictionary=None, memory_map=False, buffer_size=0,
                 pre_buffer=False, coerce_int96_timestamp_unit=None):
        self.reader = ParquetReader()
        self.reader.open(
            source, use_memory_map=memory_map,
            buffer_size=buffer_size, pre_buffer=pre_buffer,
            read_dictionary=read_dictionary, metadata=metadata,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
        )
        self.common_metadata = common_metadata
        self._nested_paths_by_prefix = self._build_nested_paths()

    def _build_nested_paths(self):
        paths = self.reader.column_paths

        result = defaultdict(list)

        for i, path in enumerate(paths):
            key = path[0]
            rest = path[1:]
            while True:
                result[key].append(i)

                if not rest:
                    break

                key = '.'.join((key, rest[0]))
                rest = rest[1:]

        return result

    @property
    def metadata(self):
        return self.reader.metadata

    @property
    def schema(self):
        """
        Return the Parquet schema, unconverted to Arrow types
        """
        return self.metadata.schema

    @property
    def schema_arrow(self):
        """
        Return the inferred Arrow schema, converted from the whole Parquet
        file's schema
        """
        return self.reader.schema_arrow

    @property
    def num_row_groups(self):
        return self.reader.num_row_groups

    def read_row_group(self, i, columns=None, use_threads=True,
                       use_pandas_metadata=False):
        """
        Read a single row group from a Parquet file.

        Parameters
        ----------
        i : int
            Index of the individual row group that we want to read.
        columns : list
            If not None, only these columns will be read from the row group. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'.
        use_threads : bool, default True
            Perform multi-threaded column reads.
        use_pandas_metadata : bool, default False
            If True and file has custom pandas schema metadata, ensure that
            index columns are also loaded.

        Returns
        -------
        pyarrow.table.Table
            Content of the row group as a table (of columns)
        """
        column_indices = self._get_column_indices(
            columns, use_pandas_metadata=use_pandas_metadata)
        return self.reader.read_row_group(i, column_indices=column_indices,
                                          use_threads=use_threads)

    def read_row_groups(self, row_groups, columns=None, use_threads=True,
                        use_pandas_metadata=False):
        """
        Read a multiple row groups from a Parquet file.

        Parameters
        ----------
        row_groups : list
            Only these row groups will be read from the file.
        columns : list
            If not None, only these columns will be read from the row group. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'.
        use_threads : bool, default True
            Perform multi-threaded column reads.
        use_pandas_metadata : bool, default False
            If True and file has custom pandas schema metadata, ensure that
            index columns are also loaded.

        Returns
        -------
        pyarrow.table.Table
            Content of the row groups as a table (of columns).
        """
        column_indices = self._get_column_indices(
            columns, use_pandas_metadata=use_pandas_metadata)
        return self.reader.read_row_groups(row_groups,
                                           column_indices=column_indices,
                                           use_threads=use_threads)

    def iter_batches(self, batch_size=65536, row_groups=None, columns=None,
                     use_threads=True, use_pandas_metadata=False):
        """
        Read streaming batches from a Parquet file

        Parameters
        ----------
        batch_size : int, default 64K
            Maximum number of records to yield per batch. Batches may be
            smaller if there aren't enough rows in the file.
        row_groups : list
            Only these row groups will be read from the file.
        columns : list
            If not None, only these columns will be read from the file. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'.
        use_threads : boolean, default True
            Perform multi-threaded column reads.
        use_pandas_metadata : boolean, default False
            If True and file has custom pandas schema metadata, ensure that
            index columns are also loaded.

        Returns
        -------
        iterator of pyarrow.RecordBatch
            Contents of each batch as a record batch
        """
        if row_groups is None:
            row_groups = range(0, self.metadata.num_row_groups)
        column_indices = self._get_column_indices(
            columns, use_pandas_metadata=use_pandas_metadata)

        batches = self.reader.iter_batches(batch_size,
                                           row_groups=row_groups,
                                           column_indices=column_indices,
                                           use_threads=use_threads)
        return batches

    def read(self, columns=None, use_threads=True, use_pandas_metadata=False):
        """
        Read a Table from Parquet format,

        Parameters
        ----------
        columns : list
            If not None, only these columns will be read from the file. A
            column name may be a prefix of a nested field, e.g. 'a' will select
            'a.b', 'a.c', and 'a.d.e'.
        use_threads : bool, default True
            Perform multi-threaded column reads.
        use_pandas_metadata : bool, default False
            If True and file has custom pandas schema metadata, ensure that
            index columns are also loaded.

        Returns
        -------
        pyarrow.table.Table
            Content of the file as a table (of columns).
        """
        column_indices = self._get_column_indices(
            columns, use_pandas_metadata=use_pandas_metadata)
        return self.reader.read_all(column_indices=column_indices,
                                    use_threads=use_threads)

    def scan_contents(self, columns=None, batch_size=65536):
        """
        Read contents of file for the given columns and batch size.

        Notes
        -----
        This function's primary purpose is benchmarking.
        The scan is executed on a single thread.

        Parameters
        ----------
        columns : list of integers, default None
            Select columns to read, if None scan all columns.
        batch_size : int, default 64K
            Number of rows to read at a time internally.

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
        column_data = [table[i] for i in range(table.num_columns)]
        return pa.Table.from_arrays(column_data, schema=new_schema)
    else:
        return table


_parquet_writer_arg_docs = """version : {"1.0", "2.4", "2.6"}, default "1.0"
    Determine which Parquet logical types are available for use, whether the
    reduced set from the Parquet 1.x.x format or the expanded logical types
    added in later format versions.
    Files written with version='2.4' or '2.6' may not be readable in all
    Parquet implementations, so version='1.0' is likely the choice that
    maximizes file compatibility.
    UINT32 and some logical types are only available with version '2.4'.
    Nanosecond timestamps are only available with version '2.6'.
    Other features such as compression algorithms or the new serialized
    data page format must be enabled separately (see 'compression' and
    'data_page_version').
use_dictionary : bool or list
    Specify if we should use dictionary encoding in general or only for
    some columns.
use_deprecated_int96_timestamps : bool, default None
    Write timestamps to INT96 Parquet format. Defaults to False unless enabled
    by flavor argument. This take priority over the coerce_timestamps option.
coerce_timestamps : str, default None
    Cast timestamps to a particular resolution. If omitted, defaults are chosen
    depending on `version`. By default, for ``version='1.0'`` (the default)
    and ``version='2.4'``, nanoseconds are cast to microseconds ('us'), while
    for other `version` values, they are written natively without loss
    of resolution.  Seconds are always cast to milliseconds ('ms') by default,
    as Parquet does not have any temporal type with seconds resolution.
    If the casting results in loss of data, it will raise an exception
    unless ``allow_truncated_timestamps=True`` is given.
    Valid values: {None, 'ms', 'us'}
data_page_size : int, default None
    Set a target threshold for the approximate encoded size of data
    pages within a column chunk (in bytes). If None, use the default data page
    size of 1MByte.
allow_truncated_timestamps : bool, default False
    Allow loss of data when coercing timestamps to a particular
    resolution. E.g. if microsecond or nanosecond data is lost when coercing to
    'ms', do not raise an exception. Passing ``allow_truncated_timestamp=True``
    will NOT result in the truncation exception being ignored unless
    ``coerce_timestamps`` is not None.
compression : str or dict
    Specify the compression codec, either on a general basis or per-column.
    Valid values: {'NONE', 'SNAPPY', 'GZIP', 'BROTLI', 'LZ4', 'ZSTD'}.
write_statistics : bool or list
    Specify if we should write statistics in general (default is True) or only
    for some columns.
flavor : {'spark'}, default None
    Sanitize schema or set other compatibility options to work with
    various target systems.
filesystem : FileSystem, default None
    If nothing passed, will be inferred from `where` if path-like, else
    `where` is already a file-like object so no filesystem is needed.
compression_level : int or dict, default None
    Specify the compression level for a codec, either on a general basis or
    per-column. If None is passed, arrow selects the compression level for
    the compression codec in use. The compression level has a different
    meaning for each codec, so you have to read the documentation of the
    codec you are using.
    An exception is thrown if the compression codec does not allow specifying
    a compression level.
use_byte_stream_split : bool or list, default False
    Specify if the byte_stream_split encoding should be used in general or
    only for some columns. If both dictionary and byte_stream_stream are
    enabled, then dictionary is preferred.
    The byte_stream_split encoding is valid only for floating-point data types
    and should be combined with a compression codec.
column_encoding : string or dict, default None
    Specify the encoding scheme on a per column basis.
    Currently supported values: {'PLAIN', 'BYTE_STREAM_SPLIT'}.
    Certain encodings are only compatible with certain data types.
    Please refer to the encodings section of `Reading and writing Parquet
    files <https://arrow.apache.org/docs/cpp/parquet.html#encodings>`_.
data_page_version : {"1.0", "2.0"}, default "1.0"
    The serialized Parquet data page format version to write, defaults to
    1.0. This does not impact the file schema logical types and Arrow to
    Parquet type casting behavior; for that use the "version" option.
use_compliant_nested_type : bool, default False
    Whether to write compliant Parquet nested type (lists) as defined
    `here <https://github.com/apache/parquet-format/blob/master/
    LogicalTypes.md#nested-types>`_, defaults to ``False``.
    For ``use_compliant_nested_type=True``, this will write into a list
    with 3-level structure where the middle level, named ``list``,
    is a repeated group with a single field named ``element``::

        <list-repetition> group <name> (LIST) {
            repeated group list {
                  <element-repetition> <element-type> element;
            }
        }

    For ``use_compliant_nested_type=False``, this will also write into a list
    with 3-level structure, where the name of the single field of the middle
    level ``list`` is taken from the element name for nested columns in Arrow,
    which defaults to ``item``::

        <list-repetition> group <name> (LIST) {
            repeated group list {
                <element-repetition> <element-type> item;
            }
        }
"""


class ParquetWriter:

    __doc__ = """
Class for incrementally building a Parquet file for Arrow tables.

Parameters
----------
where : path or file-like object
schema : pyarrow.Schema
{}
writer_engine_version : unused
**options : dict
    If options contains a key `metadata_collector` then the
    corresponding value is assumed to be a list (or any object with
    `.append` method) that will be filled with the file metadata instance
    of the written file.
""".format(_parquet_writer_arg_docs)

    def __init__(self, where, schema, filesystem=None,
                 flavor=None,
                 version='1.0',
                 use_dictionary=True,
                 compression='snappy',
                 write_statistics=True,
                 use_deprecated_int96_timestamps=None,
                 compression_level=None,
                 use_byte_stream_split=False,
                 column_encoding=None,
                 writer_engine_version=None,
                 data_page_version='1.0',
                 use_compliant_nested_type=False,
                 **options):
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

        filesystem, path = _resolve_filesystem_and_path(
            where, filesystem, allow_legacy_filesystem=True
        )
        if filesystem is not None:
            if isinstance(filesystem, legacyfs.FileSystem):
                # legacy filesystem (eg custom subclass)
                # TODO deprecate
                sink = self.file_handle = filesystem.open(path, 'wb')
            else:
                # ARROW-10480: do not auto-detect compression.  While
                # a filename like foo.parquet.gz is nonconforming, it
                # shouldn't implicitly apply compression.
                sink = self.file_handle = filesystem.open_output_stream(
                    path, compression=None)
        else:
            sink = where
        self._metadata_collector = options.pop('metadata_collector', None)
        engine_version = 'V2'
        self.writer = _parquet.ParquetWriter(
            sink, schema,
            version=version,
            compression=compression,
            use_dictionary=use_dictionary,
            write_statistics=write_statistics,
            use_deprecated_int96_timestamps=use_deprecated_int96_timestamps,
            compression_level=compression_level,
            use_byte_stream_split=use_byte_stream_split,
            column_encoding=column_encoding,
            writer_engine_version=engine_version,
            data_page_version=data_page_version,
            use_compliant_nested_type=use_compliant_nested_type,
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

    def write(self, table_or_batch, row_group_size=None):
        """
        Write RecordBatch or Table to the Parquet file.

        Parameters
        ----------
        table_or_batch : {RecordBatch, Table}
        row_group_size : int, default None
            Maximum size of each written row group. If None, the
            row group size will be the minimum of the input
            table or batch length and 64 * 1024 * 1024.
        """
        if isinstance(table_or_batch, pa.RecordBatch):
            self.write_batch(table_or_batch, row_group_size)
        elif isinstance(table_or_batch, pa.Table):
            self.write_table(table_or_batch, row_group_size)
        else:
            raise TypeError(type(table_or_batch))

    def write_batch(self, batch, row_group_size=None):
        """
        Write RecordBatch to the Parquet file.

        Parameters
        ----------
        batch : RecordBatch
        row_group_size : int, default None
            Maximum size of each written row group. If None, the
            row group size will be the minimum of the RecordBatch
            size and 64 * 1024 * 1024.
        """
        table = pa.Table.from_batches([batch], batch.schema)
        self.write_table(table, row_group_size)

    def write_table(self, table, row_group_size=None):
        """
        Write Table to the Parquet file.

        Parameters
        ----------
        table : Table
        row_group_size : int, default None
            Maximum size of each written row group. If None, the
            row group size will be the minimum of the Table size
            and 64 * 1024 * 1024.
        """
        if self.schema_changed:
            table = _sanitize_table(table, self.schema, self.flavor)
        assert self.is_open

        if not table.schema.equals(self.schema, check_metadata=False):
            msg = ('Table schema does not match schema used to create file: '
                   '\ntable:\n{!s} vs. \nfile:\n{!s}'
                   .format(table.schema, self.schema))
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


class ParquetDatasetPiece:
    """
    DEPRECATED: A single chunk of a potentially larger Parquet dataset to read.

    The arguments will indicate to read either a single row group or all row
    groups, and whether to add partition keys to the resulting pyarrow.Table.

    .. deprecated:: 5.0
        Directly constructing a ``ParquetDatasetPiece`` is deprecated, as well
        as accessing the pieces of a ``ParquetDataset`` object. Specify
        ``use_legacy_dataset=False`` when constructing the ``ParquetDataset``
        and use the ``ParquetDataset.fragments`` attribute instead.

    Parameters
    ----------
    path : str or pathlib.Path
        Path to file in the file system where this piece is located.
    open_file_func : callable
        Function to use for obtaining file handle to dataset piece.
    partition_keys : list of tuples
        Two-element tuples of ``(column name, ordinal index)``.
    row_group : int, default None
        Row group to load. By default, reads all row groups.
    file_options : dict
        Options
    """

    def __init__(self, path, open_file_func=partial(open, mode='rb'),
                 file_options=None, row_group=None, partition_keys=None):
        warnings.warn(
            "ParquetDatasetPiece is deprecated as of pyarrow 5.0.0 and will "
            "be removed in a future version.",
            DeprecationWarning, stacklevel=2)
        self._init(
            path, open_file_func, file_options, row_group, partition_keys)

    @staticmethod
    def _create(path, open_file_func=partial(open, mode='rb'),
                file_options=None, row_group=None, partition_keys=None):
        self = ParquetDatasetPiece.__new__(ParquetDatasetPiece)
        self._init(
            path, open_file_func, file_options, row_group, partition_keys)
        return self

    def _init(self, path, open_file_func, file_options, row_group,
              partition_keys):
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

    def __repr__(self):
        return ('{}({!r}, row_group={!r}, partition_keys={!r})'
                .format(type(self).__name__, self.path,
                        self.row_group,
                        self.partition_keys))

    def __str__(self):
        result = ''

        if len(self.partition_keys) > 0:
            partition_str = ', '.join('{}={}'.format(name, index)
                                      for name, index in self.partition_keys)
            result += 'partition[{}] '.format(partition_str)

        result += self.path

        if self.row_group is not None:
            result += ' | row_group={}'.format(self.row_group)

        return result

    def get_metadata(self):
        """
        Return the file's metadata.

        Returns
        -------
        metadata : FileMetaData
        """
        f = self.open()
        return f.metadata

    def open(self):
        """
        Return instance of ParquetFile.
        """
        reader = self.open_file_func(self.path)
        if not isinstance(reader, ParquetFile):
            reader = ParquetFile(reader, **self.file_options)
        return reader

    def read(self, columns=None, use_threads=True, partitions=None,
             file=None, use_pandas_metadata=False):
        """
        Read this piece as a pyarrow.Table.

        Parameters
        ----------
        columns : list of column names, default None
        use_threads : bool, default True
            Perform multi-threaded column reads.
        partitions : ParquetPartitions, default None
        file : file-like object
            Passed to ParquetFile.
        use_pandas_metadata : bool
            If pandas metadata should be used or not.

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
                indices = np.full(len(table), index, dtype='i4')

                # This is set of all partition values, computed as part of the
                # manifest, so ['a', 'b', 'c'] as in our example above.
                dictionary = partitions.levels[i].dictionary

                arr = pa.DictionaryArray.from_arrays(indices, dictionary)
                table = table.append_column(name, arr)

        return table


class PartitionSet:
    """
    A data structure for cataloguing the observed Parquet partitions at a
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

    Parameters
    ----------
    name : str
        Name of the partition set. Under which key to collect all values.
    keys : list
        All possible values that have been collected for that partition set.
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

        Parameters
        ----------
        key : The value for which we want to known the index.
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


class ParquetPartitions:

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

    def get_index(self, level, name, key):
        """
        Record a partition value at a particular level, returning the distinct
        code for that value at that level.

        Examples
        --------

        partitions.get_index(1, 'foo', 'a') returns 0
        partitions.get_index(1, 'foo', 'b') returns 1
        partitions.get_index(1, 'foo', 'c') returns 2
        partitions.get_index(1, 'foo', 'a') returns 0

        Parameters
        ----------
        level : int
            The nesting level of the partition we are observing
        name : str
            The partition name
        key : str or int
            The partition value
        """
        if level == len(self.levels):
            if name in self.partition_names:
                raise ValueError('{} was the name of the partition in '
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

        if op in {'in', 'not in'}:
            if not isinstance(f_value, Collection):
                raise TypeError(
                    "'%s' object is not a collection", f_type.__name__)
            if not f_value:
                raise ValueError("Cannot use empty collection as filter value")
            if len({type(item) for item in f_value}) != 1:
                raise ValueError("All elements of the collection '%s' must be"
                                 " of same type", f_value)
            f_type = type(next(iter(f_value)))

        elif not isinstance(f_value, str) and isinstance(f_value, Collection):
            raise ValueError(
                "Op '%s' not supported with a collection value", op)

        p_value = f_type(self.levels[level]
                         .dictionary[p_value_index].as_py())

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


class ParquetManifest:

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
                             'directory: {}'.format(base_path))
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
            raise NotImplementedError('partition schema: {}'
                                      .format(self.partition_scheme))

    def _push_pieces(self, files, part_keys):
        self.pieces.extend([
            ParquetDatasetPiece._create(path, partition_keys=part_keys,
                                        open_file_func=self.open_file_func)
            for path in files
        ])


def _parse_hive_partition(value):
    if '=' not in value:
        raise ValueError('Directory name did not appear to be a '
                         'partition: {}'.format(value))
    return value.split('=', 1)


def _is_private_directory(x):
    _, tail = os.path.split(x)
    return (tail.startswith('_') or tail.startswith('.')) and '=' not in tail


def _path_split(path, sep):
    i = path.rfind(sep) + 1
    head, tail = path[:i], path[i:]
    head = head.rstrip(sep)
    return head, tail


EXCLUDED_PARQUET_PATHS = {'_SUCCESS'}


class _ParquetDatasetMetadata:
    __slots__ = ('fs', 'memory_map', 'read_dictionary', 'common_metadata',
                 'buffer_size')


def _open_dataset_file(dataset, path, meta=None):
    if (dataset.fs is not None and
            not isinstance(dataset.fs, legacyfs.LocalFileSystem)):
        path = dataset.fs.open(path, mode='rb')
    return ParquetFile(
        path,
        metadata=meta,
        memory_map=dataset.memory_map,
        read_dictionary=dataset.read_dictionary,
        common_metadata=dataset.common_metadata,
        buffer_size=dataset.buffer_size
    )


_DEPR_MSG = (
    "'{}' attribute is deprecated as of pyarrow 5.0.0 and will be removed "
    "in a future version.{}"
)


_read_docstring_common = """\
read_dictionary : list, default None
    List of names or column paths (for nested types) to read directly
    as DictionaryArray. Only supported for BYTE_ARRAY storage. To read
    a flat column as dictionary-encoded pass the column name. For
    nested types, you must pass the full column "path", which could be
    something like level1.level2.list.item. Refer to the Parquet
    file's schema to obtain the paths.
memory_map : bool, default False
    If the source is a file path, use a memory map to read file, which can
    improve performance in some environments.
buffer_size : int, default 0
    If positive, perform read buffering when deserializing individual
    column chunks. Otherwise IO calls are unbuffered.
partitioning : pyarrow.dataset.Partitioning or str or list of str, \
default "hive"
    The partitioning scheme for a partitioned dataset. The default of "hive"
    assumes directory names with key=value pairs like "/year=2009/month=11".
    In addition, a scheme like "/2009/11" is also supported, in which case
    you need to specify the field names or a full schema. See the
    ``pyarrow.dataset.partitioning()`` function for more details."""


class ParquetDataset:

    __doc__ = """
Encapsulates details of reading a complete Parquet dataset possibly
consisting of multiple files and partitions in subdirectories.

Parameters
----------
path_or_paths : str or List[str]
    A directory name, single file name, or list of file names.
filesystem : FileSystem, default None
    If nothing passed, paths assumed to be found in the local on-disk
    filesystem.
metadata : pyarrow.parquet.FileMetaData
    Use metadata obtained elsewhere to validate file schemas.
schema : pyarrow.parquet.Schema
    Use schema obtained elsewhere to validate file schemas. Alternative to
    metadata parameter.
split_row_groups : bool, default False
    Divide files into pieces for each row group in the file.
validate_schema : bool, default True
    Check that individual file schemas are all the same / compatible.
filters : List[Tuple] or List[List[Tuple]] or None (default)
    Rows which do not match the filter predicate will be removed from scanned
    data. Partition keys embedded in a nested directory structure will be
    exploited to avoid loading files at all if they contain no matching rows.
    If `use_legacy_dataset` is True, filters can only reference partition
    keys and only a hive-style directory structure is supported. When
    setting `use_legacy_dataset` to False, also within-file level filtering
    and different partitioning schemes are supported.

    {1}
metadata_nthreads : int, default 1
    How many threads to allow the thread pool which is used to read the
    dataset metadata. Increasing this is helpful to read partitioned
    datasets.
{0}
use_legacy_dataset : bool, default True
    Set to False to enable the new code path (experimental, using the
    new Arrow Dataset API). Among other things, this allows to pass
    `filters` for all columns and not only the partition keys, enables
    different partitioning schemes, etc.
pre_buffer : bool, default True
    Coalesce and issue file reads in parallel to improve performance on
    high-latency filesystems (e.g. S3). If True, Arrow will use a
    background I/O thread pool. This option is only supported for
    use_legacy_dataset=False. If using a filesystem layer that itself
    performs readahead (e.g. fsspec's S3FS), disable readahead for best
    results.
coerce_int96_timestamp_unit : str, default None.
    Cast timestamps that are stored in INT96 format to a particular resolution
    (e.g. 'ms'). Setting to None is equivalent to 'ns' and therefore INT96
    timestamps will be infered as timestamps in nanoseconds.
""".format(_read_docstring_common, _DNF_filter_doc)

    def __new__(cls, path_or_paths=None, filesystem=None, schema=None,
                metadata=None, split_row_groups=False, validate_schema=True,
                filters=None, metadata_nthreads=1, read_dictionary=None,
                memory_map=False, buffer_size=0, partitioning="hive",
                use_legacy_dataset=None, pre_buffer=True,
                coerce_int96_timestamp_unit=None):
        if use_legacy_dataset is None:
            # if a new filesystem is passed -> default to new implementation
            if isinstance(filesystem, FileSystem):
                use_legacy_dataset = False
            # otherwise the default is still True
            else:
                use_legacy_dataset = True

        if not use_legacy_dataset:
            return _ParquetDatasetV2(
                path_or_paths, filesystem=filesystem,
                filters=filters,
                partitioning=partitioning,
                read_dictionary=read_dictionary,
                memory_map=memory_map,
                buffer_size=buffer_size,
                pre_buffer=pre_buffer,
                coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
                # unsupported keywords
                schema=schema, metadata=metadata,
                split_row_groups=split_row_groups,
                validate_schema=validate_schema,
                metadata_nthreads=metadata_nthreads
            )
        self = object.__new__(cls)
        return self

    def __init__(self, path_or_paths, filesystem=None, schema=None,
                 metadata=None, split_row_groups=False, validate_schema=True,
                 filters=None, metadata_nthreads=1, read_dictionary=None,
                 memory_map=False, buffer_size=0, partitioning="hive",
                 use_legacy_dataset=True, pre_buffer=True,
                 coerce_int96_timestamp_unit=None):
        if partitioning != "hive":
            raise ValueError(
                'Only "hive" for hive-like partitioning is supported when '
                'using use_legacy_dataset=True')
        self._metadata = _ParquetDatasetMetadata()
        a_path = path_or_paths
        if isinstance(a_path, list):
            a_path = a_path[0]

        self._metadata.fs, _ = _get_filesystem_and_path(filesystem, a_path)
        if isinstance(path_or_paths, list):
            self.paths = [_parse_uri(path) for path in path_or_paths]
        else:
            self.paths = _parse_uri(path_or_paths)

        self._metadata.read_dictionary = read_dictionary
        self._metadata.memory_map = memory_map
        self._metadata.buffer_size = buffer_size

        (self._pieces,
         self._partitions,
         self.common_metadata_path,
         self.metadata_path) = _make_manifest(
             path_or_paths, self._fs, metadata_nthreads=metadata_nthreads,
             open_file_func=partial(_open_dataset_file, self._metadata)
        )

        if self.common_metadata_path is not None:
            with self._fs.open(self.common_metadata_path) as f:
                self._metadata.common_metadata = read_metadata(
                    f,
                    memory_map=memory_map
                )
        else:
            self._metadata.common_metadata = None

        if metadata is None and self.metadata_path is not None:
            with self._fs.open(self.metadata_path) as f:
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

        if self._fs.__class__ != other._fs.__class__:
            return False
        for prop in ('paths', '_pieces', '_partitions',
                     'common_metadata_path', 'metadata_path',
                     'common_metadata', 'metadata', 'schema',
                     'split_row_groups'):
            if getattr(self, prop) != getattr(other, prop):
                return False
        for prop in ('memory_map', 'buffer_size'):
            if getattr(self._metadata, prop) != getattr(other._metadata, prop):
                return False

        return True

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def validate_schemas(self):
        if self.metadata is None and self.schema is None:
            if self.common_metadata is not None:
                self.schema = self.common_metadata.schema
            else:
                self.schema = self._pieces[0].get_metadata().schema
        elif self.schema is None:
            self.schema = self.metadata.schema

        # Verify schemas are all compatible
        dataset_schema = self.schema.to_arrow_schema()
        # Exclude the partition columns from the schema, they are provided
        # by the path, not the DatasetPiece
        if self._partitions is not None:
            for partition_name in self._partitions.partition_names:
                if dataset_schema.get_field_index(partition_name) != -1:
                    field_idx = dataset_schema.get_field_index(partition_name)
                    dataset_schema = dataset_schema.remove(field_idx)

        for piece in self._pieces:
            file_metadata = piece.get_metadata()
            file_schema = file_metadata.schema.to_arrow_schema()
            if not dataset_schema.equals(file_schema, check_metadata=False):
                raise ValueError('Schema in {!s} was different. \n'
                                 '{!s}\n\nvs\n\n{!s}'
                                 .format(piece, file_schema,
                                         dataset_schema))

    def read(self, columns=None, use_threads=True, use_pandas_metadata=False):
        """
        Read multiple Parquet files as a single pyarrow.Table.

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file.
        use_threads : bool, default True
            Perform multi-threaded column reads
        use_pandas_metadata : bool, default False
            Passed through to each dataset piece.

        Returns
        -------
        pyarrow.Table
            Content of the file as a table (of columns).
        """
        tables = []
        for piece in self._pieces:
            table = piece.read(columns=columns, use_threads=use_threads,
                               partitions=self._partitions,
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
        through to ParquetDataset.read, see docstring for further details.

        Parameters
        ----------
        **kwargs : optional
            All additional options to pass to the reader.

        Returns
        -------
        pyarrow.Table
            Content of the file as a table (of columns).
        """
        return self.read(use_pandas_metadata=True, **kwargs)

    def _get_common_pandas_metadata(self):
        if self.common_metadata is None:
            return None

        keyvalues = self.common_metadata.metadata
        return keyvalues.get(b'pandas', None)

    def _filter(self, filters):
        accepts_filter = self._partitions.filter_accepts_partition

        def one_filter_accepts(piece, filter):
            return all(accepts_filter(part_key, filter, level)
                       for level, part_key in enumerate(piece.partition_keys))

        def all_filters_accept(piece):
            return any(all(one_filter_accepts(piece, f) for f in conjunction)
                       for conjunction in filters)

        self._pieces = [p for p in self._pieces if all_filters_accept(p)]

    @property
    def pieces(self):
        warnings.warn(
            _DEPR_MSG.format(
                "ParquetDataset.pieces",
                " Specify 'use_legacy_dataset=False' while constructing the "
                "ParquetDataset, and then use the '.fragments' attribute "
                "instead."),
            DeprecationWarning, stacklevel=2)
        return self._pieces

    @property
    def partitions(self):
        warnings.warn(
            _DEPR_MSG.format(
                "ParquetDataset.partitions",
                " Specify 'use_legacy_dataset=False' while constructing the "
                "ParquetDataset, and then use the '.partitioning' attribute "
                "instead."),
            DeprecationWarning, stacklevel=2)
        return self._partitions

    @property
    def memory_map(self):
        warnings.warn(
            _DEPR_MSG.format("ParquetDataset.memory_map", ""),
            DeprecationWarning, stacklevel=2)
        return self._metadata.memory_map

    @property
    def read_dictionary(self):
        warnings.warn(
            _DEPR_MSG.format("ParquetDataset.read_dictionary", ""),
            DeprecationWarning, stacklevel=2)
        return self._metadata.read_dictionary

    @property
    def buffer_size(self):
        warnings.warn(
            _DEPR_MSG.format("ParquetDataset.buffer_size", ""),
            DeprecationWarning, stacklevel=2)
        return self._metadata.buffer_size

    _fs = property(
        operator.attrgetter('_metadata.fs')
    )

    @property
    def fs(self):
        warnings.warn(
            _DEPR_MSG.format(
                "ParquetDataset.fs",
                " Specify 'use_legacy_dataset=False' while constructing the "
                "ParquetDataset, and then use the '.filesystem' attribute "
                "instead."),
            DeprecationWarning, stacklevel=2)
        return self._metadata.fs

    common_metadata = property(
        operator.attrgetter('_metadata.common_metadata')
    )


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
                                   pathsep=getattr(fs, "pathsep", "/"),
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
                raise OSError('Passed non-file path: {}'
                              .format(path))
            piece = ParquetDatasetPiece._create(
                path, open_file_func=open_file_func)
            pieces.append(piece)

    return pieces, partitions, common_metadata_path, metadata_path


def _is_local_file_system(fs):
    return isinstance(fs, LocalFileSystem) or isinstance(
        fs, legacyfs.LocalFileSystem
    )


class _ParquetDatasetV2:
    """
    ParquetDataset shim using the Dataset API under the hood.
    """

    def __init__(self, path_or_paths, filesystem=None, filters=None,
                 partitioning="hive", read_dictionary=None, buffer_size=None,
                 memory_map=False, ignore_prefixes=None, pre_buffer=True,
                 coerce_int96_timestamp_unit=None, **kwargs):
        import pyarrow.dataset as ds

        # Raise error for not supported keywords
        for keyword, default in [
                ("schema", None), ("metadata", None),
                ("split_row_groups", False), ("validate_schema", True),
                ("metadata_nthreads", 1)]:
            if keyword in kwargs and kwargs[keyword] is not default:
                raise ValueError(
                    "Keyword '{0}' is not yet supported with the new "
                    "Dataset API".format(keyword))

        # map format arguments
        read_options = {
            "pre_buffer": pre_buffer,
            "coerce_int96_timestamp_unit": coerce_int96_timestamp_unit
        }
        if buffer_size:
            read_options.update(use_buffered_stream=True,
                                buffer_size=buffer_size)
        if read_dictionary is not None:
            read_options.update(dictionary_columns=read_dictionary)

        # map filters to Expressions
        self._filters = filters
        self._filter_expression = filters and _filters_to_expression(filters)

        # map old filesystems to new one
        if filesystem is not None:
            filesystem = _ensure_filesystem(
                filesystem, use_mmap=memory_map)
        elif filesystem is None and memory_map:
            # if memory_map is specified, assume local file system (string
            # path can in principle be URI for any filesystem)
            filesystem = LocalFileSystem(use_mmap=memory_map)

        # This needs to be checked after _ensure_filesystem, because that
        # handles the case of an fsspec LocalFileSystem
        if (
            hasattr(path_or_paths, "__fspath__") and
            filesystem is not None and
            not _is_local_file_system(filesystem)
        ):
            raise TypeError(
                "Path-like objects with __fspath__ must only be used with "
                f"local file systems, not {type(filesystem)}"
            )

        # check for single fragment dataset
        single_file = None
        if isinstance(path_or_paths, list):
            if len(path_or_paths) == 1:
                single_file = path_or_paths[0]
        else:
            if _is_path_like(path_or_paths):
                path_or_paths = _stringify_path(path_or_paths)
                if filesystem is None:
                    # path might be a URI describing the FileSystem as well
                    try:
                        filesystem, path_or_paths = FileSystem.from_uri(
                            path_or_paths)
                    except ValueError:
                        filesystem = LocalFileSystem(use_mmap=memory_map)
                if filesystem.get_file_info(path_or_paths).is_file:
                    single_file = path_or_paths
            else:
                single_file = path_or_paths

        if single_file is not None:
            self._enable_parallel_column_conversion = True
            read_options.update(enable_parallel_column_conversion=True)

            parquet_format = ds.ParquetFileFormat(**read_options)
            fragment = parquet_format.make_fragment(single_file, filesystem)

            self._dataset = ds.FileSystemDataset(
                [fragment], schema=fragment.physical_schema,
                format=parquet_format,
                filesystem=fragment.filesystem
            )
            return
        else:
            self._enable_parallel_column_conversion = False

        parquet_format = ds.ParquetFileFormat(**read_options)

        # check partitioning to enable dictionary encoding
        if partitioning == "hive":
            partitioning = ds.HivePartitioning.discover(
                infer_dictionary=True)

        self._dataset = ds.dataset(path_or_paths, filesystem=filesystem,
                                   format=parquet_format,
                                   partitioning=partitioning,
                                   ignore_prefixes=ignore_prefixes)

    @property
    def schema(self):
        return self._dataset.schema

    def read(self, columns=None, use_threads=True, use_pandas_metadata=False):
        """
        Read (multiple) Parquet files as a single pyarrow.Table.

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the dataset. The partition fields
            are not automatically included (in contrast to when setting
            ``use_legacy_dataset=True``).
        use_threads : bool, default True
            Perform multi-threaded column reads.
        use_pandas_metadata : bool, default False
            If True and file has custom pandas schema metadata, ensure that
            index columns are also loaded.

        Returns
        -------
        pyarrow.Table
            Content of the file as a table (of columns).
        """
        # if use_pandas_metadata, we need to include index columns in the
        # column selection, to be able to restore those in the pandas DataFrame
        metadata = self.schema.metadata
        if columns is not None and use_pandas_metadata:
            if metadata and b'pandas' in metadata:
                # RangeIndex can be represented as dict instead of column name
                index_columns = [
                    col for col in _get_pandas_index_columns(metadata)
                    if not isinstance(col, dict)
                ]
                columns = (
                    list(columns) + list(set(index_columns) - set(columns))
                )

        if self._enable_parallel_column_conversion:
            if use_threads:
                # Allow per-column parallelism; would otherwise cause
                # contention in the presence of per-file parallelism.
                use_threads = False

        table = self._dataset.to_table(
            columns=columns, filter=self._filter_expression,
            use_threads=use_threads
        )

        # if use_pandas_metadata, restore the pandas metadata (which gets
        # lost if doing a specific `columns` selection in to_table)
        if use_pandas_metadata:
            if metadata and b"pandas" in metadata:
                new_metadata = table.schema.metadata or {}
                new_metadata.update({b"pandas": metadata[b"pandas"]})
                table = table.replace_schema_metadata(new_metadata)

        return table

    def read_pandas(self, **kwargs):
        """
        Read dataset including pandas metadata, if any. Other arguments passed
        through to ParquetDataset.read, see docstring for further details.
        """
        return self.read(use_pandas_metadata=True, **kwargs)

    @property
    def pieces(self):
        warnings.warn(
            _DEPR_MSG.format("ParquetDataset.pieces",
                             " Use the '.fragments' attribute instead"),
            DeprecationWarning, stacklevel=2)
        return list(self._dataset.get_fragments())

    @property
    def fragments(self):
        return list(self._dataset.get_fragments())

    @property
    def files(self):
        return self._dataset.files

    @property
    def filesystem(self):
        return self._dataset.filesystem

    @property
    def partitioning(self):
        """
        The partitioning of the Dataset source, if discovered.
        """
        return self._dataset.partitioning


_read_table_docstring = """
{0}

Parameters
----------
source : str, pyarrow.NativeFile, or file-like object
    If a string passed, can be a single file name or directory name. For
    file-like objects, only read a single file. Use pyarrow.BufferReader to
    read a file contained in a bytes or buffer-like object.
columns : list
    If not None, only these columns will be read from the file. A column
    name may be a prefix of a nested field, e.g. 'a' will select 'a.b',
    'a.c', and 'a.d.e'. If empty, no columns will be read. Note
    that the table will still have the correct num_rows set despite having
    no columns.
use_threads : bool, default True
    Perform multi-threaded column reads.
metadata : FileMetaData
    If separately computed
{1}
use_legacy_dataset : bool, default False
    By default, `read_table` uses the new Arrow Datasets API since
    pyarrow 1.0.0. Among other things, this allows to pass `filters`
    for all columns and not only the partition keys, enables
    different partitioning schemes, etc.
    Set to True to use the legacy behaviour.
ignore_prefixes : list, optional
    Files matching any of these prefixes will be ignored by the
    discovery process if use_legacy_dataset=False.
    This is matched to the basename of a path.
    By default this is ['.', '_'].
    Note that discovery happens only if a directory is passed as source.
filesystem : FileSystem, default None
    If nothing passed, paths assumed to be found in the local on-disk
    filesystem.
filters : List[Tuple] or List[List[Tuple]] or None (default)
    Rows which do not match the filter predicate will be removed from scanned
    data. Partition keys embedded in a nested directory structure will be
    exploited to avoid loading files at all if they contain no matching rows.
    If `use_legacy_dataset` is True, filters can only reference partition
    keys and only a hive-style directory structure is supported. When
    setting `use_legacy_dataset` to False, also within-file level filtering
    and different partitioning schemes are supported.

    {3}
pre_buffer : bool, default True
    Coalesce and issue file reads in parallel to improve performance on
    high-latency filesystems (e.g. S3). If True, Arrow will use a
    background I/O thread pool. This option is only supported for
    use_legacy_dataset=False. If using a filesystem layer that itself
    performs readahead (e.g. fsspec's S3FS), disable readahead for best
    results.
coerce_int96_timestamp_unit : str, default None.
    Cast timestamps that are stored in INT96 format to a particular
    resolution (e.g. 'ms'). Setting to None is equivalent to 'ns'
    and therefore INT96 timestamps will be infered as timestamps
    in nanoseconds.

Returns
-------
{2}
"""


def read_table(source, columns=None, use_threads=True, metadata=None,
               use_pandas_metadata=False, memory_map=False,
               read_dictionary=None, filesystem=None, filters=None,
               buffer_size=0, partitioning="hive", use_legacy_dataset=False,
               ignore_prefixes=None, pre_buffer=True,
               coerce_int96_timestamp_unit=None):
    if not use_legacy_dataset:
        if metadata is not None:
            raise ValueError(
                "The 'metadata' keyword is no longer supported with the new "
                "datasets-based implementation. Specify "
                "'use_legacy_dataset=True' to temporarily recover the old "
                "behaviour."
            )
        try:
            dataset = _ParquetDatasetV2(
                source,
                filesystem=filesystem,
                partitioning=partitioning,
                memory_map=memory_map,
                read_dictionary=read_dictionary,
                buffer_size=buffer_size,
                filters=filters,
                ignore_prefixes=ignore_prefixes,
                pre_buffer=pre_buffer,
                coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
            )
        except ImportError:
            # fall back on ParquetFile for simple cases when pyarrow.dataset
            # module is not available
            if filters is not None:
                raise ValueError(
                    "the 'filters' keyword is not supported when the "
                    "pyarrow.dataset module is not available"
                )
            if partitioning != "hive":
                raise ValueError(
                    "the 'partitioning' keyword is not supported when the "
                    "pyarrow.dataset module is not available"
                )
            filesystem, path = _resolve_filesystem_and_path(source, filesystem)
            if filesystem is not None:
                source = filesystem.open_input_file(path)
            # TODO test that source is not a directory or a list
            dataset = ParquetFile(
                source, metadata=metadata, read_dictionary=read_dictionary,
                memory_map=memory_map, buffer_size=buffer_size,
                pre_buffer=pre_buffer,
                coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
            )

        return dataset.read(columns=columns, use_threads=use_threads,
                            use_pandas_metadata=use_pandas_metadata)

    if ignore_prefixes is not None:
        raise ValueError(
            "The 'ignore_prefixes' keyword is only supported when "
            "use_legacy_dataset=False")

    if _is_path_like(source):
        pf = ParquetDataset(
            source, metadata=metadata, memory_map=memory_map,
            read_dictionary=read_dictionary,
            buffer_size=buffer_size,
            filesystem=filesystem, filters=filters,
            partitioning=partitioning,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
        )
    else:
        pf = ParquetFile(
            source, metadata=metadata,
            read_dictionary=read_dictionary,
            memory_map=memory_map,
            buffer_size=buffer_size,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
        )
    return pf.read(columns=columns, use_threads=use_threads,
                   use_pandas_metadata=use_pandas_metadata)


read_table.__doc__ = _read_table_docstring.format(
    """Read a Table from Parquet format

Note: starting with pyarrow 1.0, the default for `use_legacy_dataset` is
switched to False.""",
    "\n".join((_read_docstring_common,
               """use_pandas_metadata : bool, default False
    If True and file has custom pandas schema metadata, ensure that
    index columns are also loaded.""")),
    """pyarrow.Table
    Content of the file as a table (of columns)""",
    _DNF_filter_doc)


def read_pandas(source, columns=None, **kwargs):
    return read_table(
        source, columns=columns, use_pandas_metadata=True, **kwargs
    )


read_pandas.__doc__ = _read_table_docstring.format(
    'Read a Table from Parquet format, also reading DataFrame\n'
    'index values if known in the file metadata',
    "\n".join((_read_docstring_common,
               """**kwargs
    additional options for :func:`read_table`""")),
    """pyarrow.Table
    Content of the file as a Table of Columns, including DataFrame
    indexes as columns""",
    _DNF_filter_doc)


def write_table(table, where, row_group_size=None, version='1.0',
                use_dictionary=True, compression='snappy',
                write_statistics=True,
                use_deprecated_int96_timestamps=None,
                coerce_timestamps=None,
                allow_truncated_timestamps=False,
                data_page_size=None, flavor=None,
                filesystem=None,
                compression_level=None,
                use_byte_stream_split=False,
                column_encoding=None,
                data_page_version='1.0',
                use_compliant_nested_type=False,
                **kwargs):
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
                compression_level=compression_level,
                use_byte_stream_split=use_byte_stream_split,
                column_encoding=column_encoding,
                data_page_version=data_page_version,
                use_compliant_nested_type=use_compliant_nested_type,
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
Write a Table to Parquet format.

Parameters
----------
table : pyarrow.Table
where : string or pyarrow.NativeFile
row_group_size : int
    Maximum size of each written row group. If None, the
    row group size will be the minimum of the Table size
    and 64 * 1024 * 1024.
{}
**kwargs : optional
    Additional options for ParquetWriter
""".format(_parquet_writer_arg_docs)


def _mkdir_if_not_exists(fs, path):
    if fs._isfilestore() and not fs.exists(path):
        try:
            fs.mkdir(path)
        except OSError:
            assert fs.exists(path)


def write_to_dataset(table, root_path, partition_cols=None,
                     partition_filename_cb=None, filesystem=None,
                     use_legacy_dataset=None, **kwargs):
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
    root_path : str, pathlib.Path
        The root directory of the dataset
    filesystem : FileSystem, default None
        If nothing passed, paths assumed to be found in the local on-disk
        filesystem
    partition_cols : list,
        Column names by which to partition the dataset
        Columns are partitioned in the order they are given
    partition_filename_cb : callable,
        A callback function that takes the partition key(s) as an argument
        and allow you to override the partition filename. If nothing is
        passed, the filename will consist of a uuid.
    use_legacy_dataset : bool
        Default is True unless a ``pyarrow.fs`` filesystem is passed.
        Set to False to enable the new code path (experimental, using the
        new Arrow Dataset API). This is more efficient when using partition
        columns, but does not (yet) support `partition_filename_cb` and
        `metadata_collector` keywords.
    **kwargs : dict,
        Additional kwargs for write_table function. See docstring for
        `write_table` or `ParquetWriter` for more information.
        Using `metadata_collector` in kwargs allows one to collect the
        file metadata instances of dataset pieces. The file paths in the
        ColumnChunkMetaData will be set relative to `root_path`.
    """
    if use_legacy_dataset is None:
        # if a new filesystem is passed -> default to new implementation
        if isinstance(filesystem, FileSystem):
            use_legacy_dataset = False
        # otherwise the default is still True
        else:
            use_legacy_dataset = True

    if not use_legacy_dataset:
        import pyarrow.dataset as ds

        # extract non-file format options
        schema = kwargs.pop("schema", None)
        use_threads = kwargs.pop("use_threads", True)

        # raise for unsupported keywords
        msg = (
            "The '{}' argument is not supported with the new dataset "
            "implementation."
        )
        metadata_collector = kwargs.pop('metadata_collector', None)
        file_visitor = None
        if metadata_collector is not None:
            def file_visitor(written_file):
                metadata_collector.append(written_file.metadata)
        if partition_filename_cb is not None:
            raise ValueError(msg.format("partition_filename_cb"))

        # map format arguments
        parquet_format = ds.ParquetFileFormat()
        write_options = parquet_format.make_write_options(**kwargs)

        # map old filesystems to new one
        if filesystem is not None:
            filesystem = _ensure_filesystem(filesystem)

        partitioning = None
        if partition_cols:
            part_schema = table.select(partition_cols).schema
            partitioning = ds.partitioning(part_schema, flavor="hive")

        ds.write_dataset(
            table, root_path, filesystem=filesystem,
            format=parquet_format, file_options=write_options, schema=schema,
            partitioning=partitioning, use_threads=use_threads,
            file_visitor=file_visitor)
        return

    fs, root_path = legacyfs.resolve_filesystem_and_path(root_path, filesystem)

    _mkdir_if_not_exists(fs, root_path)

    metadata_collector = kwargs.pop('metadata_collector', None)

    if partition_cols is not None and len(partition_cols) > 0:
        df = table.to_pandas()
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
            subtable = pa.Table.from_pandas(subgroup, schema=subschema,
                                            safe=False)
            _mkdir_if_not_exists(fs, '/'.join([root_path, subdir]))
            if partition_filename_cb:
                outfile = partition_filename_cb(keys)
            else:
                outfile = guid() + '.parquet'
            relative_path = '/'.join([subdir, outfile])
            full_path = '/'.join([root_path, relative_path])
            with fs.open(full_path, 'wb') as f:
                write_table(subtable, f, metadata_collector=metadata_collector,
                            **kwargs)
            if metadata_collector is not None:
                metadata_collector[-1].set_file_path(relative_path)
    else:
        if partition_filename_cb:
            outfile = partition_filename_cb(None)
        else:
            outfile = guid() + '.parquet'
        full_path = '/'.join([root_path, outfile])
        with fs.open(full_path, 'wb') as f:
            write_table(table, f, metadata_collector=metadata_collector,
                        **kwargs)
        if metadata_collector is not None:
            metadata_collector[-1].set_file_path(outfile)


def write_metadata(schema, where, metadata_collector=None, **kwargs):
    """
    Write metadata-only Parquet file from schema. This can be used with
    `write_to_dataset` to generate `_common_metadata` and `_metadata` sidecar
    files.

    Parameters
    ----------
    schema : pyarrow.Schema
    where : string or pyarrow.NativeFile
    metadata_collector : list
        where to collect metadata information.
    **kwargs : dict,
        Additional kwargs for ParquetWriter class. See docstring for
        `ParquetWriter` for more information.

    Examples
    --------

    Write a dataset and collect metadata information.

    >>> metadata_collector = []
    >>> write_to_dataset(
    ...     table, root_path,
    ...     metadata_collector=metadata_collector, **writer_kwargs)

    Write the `_common_metadata` parquet file without row groups statistics.

    >>> write_metadata(
    ...     table.schema, root_path / '_common_metadata', **writer_kwargs)

    Write the `_metadata` parquet file with row groups statistics.

    >>> write_metadata(
    ...     table.schema, root_path / '_metadata',
    ...     metadata_collector=metadata_collector, **writer_kwargs)
    """
    writer = ParquetWriter(where, schema, **kwargs)
    writer.close()

    if metadata_collector is not None:
        # ParquetWriter doesn't expose the metadata until it's written. Write
        # it and read it again.
        metadata = read_metadata(where)
        for m in metadata_collector:
            metadata.append_row_groups(m)
        metadata.write_metadata_file(where)


def read_metadata(where, memory_map=False):
    """
    Read FileMetadata from footer of a single Parquet file.

    Parameters
    ----------
    where : str (file path) or file-like object
    memory_map : bool, default False
        Create memory map when the source is a file path.

    Returns
    -------
    metadata : FileMetadata
    """
    return ParquetFile(where, memory_map=memory_map).metadata


def read_schema(where, memory_map=False):
    """
    Read effective Arrow schema from Parquet file metadata.

    Parameters
    ----------
    where : str (file path) or file-like object
    memory_map : bool, default False
        Create memory map when the source is a file path.

    Returns
    -------
    schema : pyarrow.Schema
    """
    return ParquetFile(where, memory_map=memory_map).schema.to_arrow_schema()

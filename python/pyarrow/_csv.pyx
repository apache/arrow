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

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from cython.operator cimport dereference as deref

import codecs
from collections.abc import Mapping

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (check_status, Field, MemoryPool, Schema,
                          _CRecordBatchReader, ensure_type,
                          maybe_unbox_memory_pool, get_input_stream,
                          native_transcoding_input_stream,
                          pyarrow_wrap_schema, pyarrow_wrap_table,
                          pyarrow_wrap_data_type, pyarrow_unwrap_data_type)
from pyarrow.lib import frombytes, tobytes


cdef unsigned char _single_char(s) except 0:
    val = ord(s)
    if val == 0 or val > 127:
        raise ValueError("Expecting an ASCII character")
    return <unsigned char> val


cdef class ReadOptions:
    """
    Options for reading CSV files.

    Parameters
    ----------
    use_threads : bool, optional (default True)
        Whether to use multiple threads to accelerate reading
    block_size : int, optional
        How much bytes to process at a time from the input stream.
        This will determine multi-threading granularity as well as
        the size of individual chunks in the Table.
    skip_rows: int, optional (default 0)
        The number of rows to skip before the column names (if any)
        and the CSV data.
    column_names: list, optional
        The column names of the target table.  If empty, fall back on
        `autogenerate_column_names`.
    autogenerate_column_names: bool, optional (default False)
        Whether to autogenerate column names if `column_names` is empty.
        If true, column names will be of the form "f0", "f1"...
        If false, column names will be read from the first CSV row
        after `skip_rows`.
    encoding: str, optional (default 'utf8')
        The character encoding of the CSV data.  Columns that cannot
        decode using this encoding can still be read as Binary.
    """
    cdef:
        CCSVReadOptions options
        public object encoding

    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, *, use_threads=None, block_size=None, skip_rows=None,
                 column_names=None, autogenerate_column_names=None,
                 encoding='utf8'):
        self.options = CCSVReadOptions.Defaults()
        if use_threads is not None:
            self.use_threads = use_threads
        if block_size is not None:
            self.block_size = block_size
        if skip_rows is not None:
            self.skip_rows = skip_rows
        if column_names is not None:
            self.column_names = column_names
        if autogenerate_column_names is not None:
            self.autogenerate_column_names= autogenerate_column_names
        # Python-specific option
        self.encoding = encoding

    @property
    def use_threads(self):
        """
        Whether to use multiple threads to accelerate reading.
        """
        return self.options.use_threads

    @use_threads.setter
    def use_threads(self, value):
        self.options.use_threads = value

    @property
    def block_size(self):
        """
        How much bytes to process at a time from the input stream.
        This will determine multi-threading granularity as well as
        the size of individual chunks in the Table.
        """
        return self.options.block_size

    @block_size.setter
    def block_size(self, value):
        self.options.block_size = value

    @property
    def skip_rows(self):
        """
        The number of rows to skip before the column names (if any)
        and the CSV data.
        """
        return self.options.skip_rows

    @skip_rows.setter
    def skip_rows(self, value):
        self.options.skip_rows = value

    @property
    def column_names(self):
        """
        The column names of the target table.  If empty, fall back on
        `autogenerate_column_names`.
        """
        return [frombytes(s) for s in self.options.column_names]

    @column_names.setter
    def column_names(self, value):
        self.options.column_names.clear()
        for item in value:
            self.options.column_names.push_back(tobytes(item))

    @property
    def autogenerate_column_names(self):
        """
        Whether to autogenerate column names if `column_names` is empty.
        If true, column names will be of the form "f0", "f1"...
        If false, column names will be read from the first CSV row
        after `skip_rows`.
        """
        return self.options.autogenerate_column_names

    @autogenerate_column_names.setter
    def autogenerate_column_names(self, value):
        self.options.autogenerate_column_names = value


cdef class ParseOptions:
    """
    Options for parsing CSV files.

    Parameters
    ----------
    delimiter: 1-character string, optional (default ',')
        The character delimiting individual cells in the CSV data.
    quote_char: 1-character string or False, optional (default '"')
        The character used optionally for quoting CSV values
        (False if quoting is not allowed).
    double_quote: bool, optional (default True)
        Whether two quotes in a quoted CSV value denote a single quote
        in the data.
    escape_char: 1-character string or False, optional (default False)
        The character used optionally for escaping special characters
        (False if escaping is not allowed).
    newlines_in_values: bool, optional (default False)
        Whether newline characters are allowed in CSV values.
        Setting this to True reduces the performance of multi-threaded
        CSV reading.
    ignore_empty_lines: bool, optional (default True)
        Whether empty lines are ignored in CSV input.
        If False, an empty line is interpreted as containing a single empty
        value (assuming a one-column CSV file).
    """
    __slots__ = ()

    def __init__(self, *, delimiter=None, quote_char=None, double_quote=None,
                 escape_char=None, newlines_in_values=None,
                 ignore_empty_lines=None):
        self.options = CCSVParseOptions.Defaults()
        if delimiter is not None:
            self.delimiter = delimiter
        if quote_char is not None:
            self.quote_char = quote_char
        if double_quote is not None:
            self.double_quote = double_quote
        if escape_char is not None:
            self.escape_char = escape_char
        if newlines_in_values is not None:
            self.newlines_in_values = newlines_in_values
        if ignore_empty_lines is not None:
            self.ignore_empty_lines = ignore_empty_lines

    @property
    def delimiter(self):
        """
        The character delimiting individual cells in the CSV data.
        """
        return chr(self.options.delimiter)

    @delimiter.setter
    def delimiter(self, value):
        self.options.delimiter = _single_char(value)

    @property
    def quote_char(self):
        """
        The character used optionally for quoting CSV values
        (False if quoting is not allowed).
        """
        if self.options.quoting:
            return chr(self.options.quote_char)
        else:
            return False

    @quote_char.setter
    def quote_char(self, value):
        if value is False:
            self.options.quoting = False
        else:
            self.options.quote_char = _single_char(value)
            self.options.quoting = True

    @property
    def double_quote(self):
        """
        Whether two quotes in a quoted CSV value denote a single quote
        in the data.
        """
        return self.options.double_quote

    @double_quote.setter
    def double_quote(self, value):
        self.options.double_quote = value

    @property
    def escape_char(self):
        """
        The character used optionally for escaping special characters
        (False if escaping is not allowed).
        """
        if self.options.escaping:
            return chr(self.options.escape_char)
        else:
            return False

    @escape_char.setter
    def escape_char(self, value):
        if value is False:
            self.options.escaping = False
        else:
            self.options.escape_char = _single_char(value)
            self.options.escaping = True

    @property
    def newlines_in_values(self):
        """
        Whether newline characters are allowed in CSV values.
        Setting this to True reduces the performance of multi-threaded
        CSV reading.
        """
        return self.options.newlines_in_values

    @newlines_in_values.setter
    def newlines_in_values(self, value):
        self.options.newlines_in_values = value

    @property
    def ignore_empty_lines(self):
        """
        Whether empty lines are ignored in CSV input.
        If False, an empty line is interpreted as containing a single empty
        value (assuming a one-column CSV file).
        """
        return self.options.ignore_empty_lines

    @ignore_empty_lines.setter
    def ignore_empty_lines(self, value):
        self.options.ignore_empty_lines = value

    def equals(self, ParseOptions other):
        return (
            self.delimiter == other.delimiter and
            self.quote_char == other.quote_char and
            self.double_quote == other.double_quote and
            self.escape_char == other.escape_char and
            self.newlines_in_values == other.newlines_in_values and
            self.ignore_empty_lines == other.ignore_empty_lines
        )

    @staticmethod
    cdef ParseOptions wrap(CCSVParseOptions options):
        out = ParseOptions()
        out.options = options
        return out

    def __getstate__(self):
        return (self.delimiter, self.quote_char, self.double_quote,
                self.escape_char, self.newlines_in_values,
                self.ignore_empty_lines)

    def __setstate__(self, state):
        (self.delimiter, self.quote_char, self.double_quote,
         self.escape_char, self.newlines_in_values,
         self.ignore_empty_lines) = state


cdef class _ISO8601:
    """
    A special object indicating ISO-8601 parsing.
    """
    __slots__ = ()

    def __str__(self):
        return 'ISO8601'

    def __eq__(self, other):
        return isinstance(other, _ISO8601)


ISO8601 = _ISO8601()


cdef class ConvertOptions:
    """
    Options for converting CSV data.

    Parameters
    ----------
    check_utf8 : bool, optional (default True)
        Whether to check UTF8 validity of string columns.
    column_types: pa.Schema or dict, optional
        Explicitly map column names to column types. Passing this argument
        disables type inference on the defined columns.
    null_values: list, optional
        A sequence of strings that denote nulls in the data
        (defaults are appropriate in most cases). Note that by default,
        string columns are not checked for null values. To enable
        null checking for those, specify ``strings_can_be_null=True``.
    true_values: list, optional
        A sequence of strings that denote true booleans in the data
        (defaults are appropriate in most cases).
    false_values: list, optional
        A sequence of strings that denote false booleans in the data
        (defaults are appropriate in most cases).
    timestamp_parsers: list, optional
        A sequence of strptime()-compatible format strings, tried in order
        when attempting to infer or convert timestamp values (the special
        value ISO8601() can also be given).  By default, a fast built-in
        ISO-8601 parser is used.
    strings_can_be_null: bool, optional (default False)
        Whether string / binary columns can have null values.
        If true, then strings in null_values are considered null for
        string columns.
        If false, then all strings are valid string values.
    auto_dict_encode: bool, optional (default False)
        Whether to try to automatically dict-encode string / binary data.
        If true, then when type inference detects a string or binary column,
        it it dict-encoded up to `auto_dict_max_cardinality` distinct values
        (per chunk), after which it switches to regular encoding.
        This setting is ignored for non-inferred columns (those in
        `column_types`).
    auto_dict_max_cardinality: int, optional
        The maximum dictionary cardinality for `auto_dict_encode`.
        This value is per chunk.
    include_columns: list, optional
        The names of columns to include in the Table.
        If empty, the Table will include all columns from the CSV file.
        If not empty, only these columns will be included, in this order.
    include_missing_columns: bool, optional (default False)
        If false, columns in `include_columns` but not in the CSV file will
        error out.
        If true, columns in `include_columns` but not in the CSV file will
        produce a column of nulls (whose type is selected using
        `column_types`, or null by default).
        This option is ignored if `include_columns` is empty.
    """
    cdef:
        CCSVConvertOptions options

    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, *, check_utf8=None, column_types=None, null_values=None,
                 true_values=None, false_values=None,
                 strings_can_be_null=None, include_columns=None,
                 include_missing_columns=None, auto_dict_encode=None,
                 auto_dict_max_cardinality=None, timestamp_parsers=None):
        self.options = CCSVConvertOptions.Defaults()
        if check_utf8 is not None:
            self.check_utf8 = check_utf8
        if column_types is not None:
            self.column_types = column_types
        if null_values is not None:
            self.null_values = null_values
        if true_values is not None:
            self.true_values = true_values
        if false_values is not None:
            self.false_values = false_values
        if strings_can_be_null is not None:
            self.strings_can_be_null = strings_can_be_null
        if include_columns is not None:
            self.include_columns = include_columns
        if include_missing_columns is not None:
            self.include_missing_columns = include_missing_columns
        if auto_dict_encode is not None:
            self.auto_dict_encode = auto_dict_encode
        if auto_dict_max_cardinality is not None:
            self.auto_dict_max_cardinality = auto_dict_max_cardinality
        if timestamp_parsers is not None:
            self.timestamp_parsers = timestamp_parsers

    @property
    def check_utf8(self):
        """
        Whether to check UTF8 validity of string columns.
        """
        return self.options.check_utf8

    @check_utf8.setter
    def check_utf8(self, value):
        self.options.check_utf8 = value

    @property
    def strings_can_be_null(self):
        """
        Whether string / binary columns can have null values.
        """
        return self.options.strings_can_be_null

    @strings_can_be_null.setter
    def strings_can_be_null(self, value):
        self.options.strings_can_be_null = value

    @property
    def column_types(self):
        """
        Explicitly map column names to column types.
        """
        d = {frombytes(item.first): pyarrow_wrap_data_type(item.second)
             for item in self.options.column_types}
        return d

    @column_types.setter
    def column_types(self, value):
        cdef:
            shared_ptr[CDataType] typ

        if isinstance(value, Mapping):
            value = value.items()

        self.options.column_types.clear()
        for item in value:
            if isinstance(item, Field):
                k = item.name
                v = item.type
            else:
                k, v = item
            typ = pyarrow_unwrap_data_type(ensure_type(v))
            assert typ != NULL
            self.options.column_types[tobytes(k)] = typ

    @property
    def null_values(self):
        """
        A sequence of strings that denote nulls in the data.
        """
        return [frombytes(x) for x in self.options.null_values]

    @null_values.setter
    def null_values(self, value):
        self.options.null_values = [tobytes(x) for x in value]

    @property
    def true_values(self):
        """
        A sequence of strings that denote true booleans in the data.
        """
        return [frombytes(x) for x in self.options.true_values]

    @true_values.setter
    def true_values(self, value):
        self.options.true_values = [tobytes(x) for x in value]

    @property
    def false_values(self):
        """
        A sequence of strings that denote false booleans in the data.
        """
        return [frombytes(x) for x in self.options.false_values]

    @false_values.setter
    def false_values(self, value):
        self.options.false_values = [tobytes(x) for x in value]

    @property
    def auto_dict_encode(self):
        """
        Whether to try to automatically dict-encode string / binary data.
        """
        return self.options.auto_dict_encode

    @auto_dict_encode.setter
    def auto_dict_encode(self, value):
        self.options.auto_dict_encode = value

    @property
    def auto_dict_max_cardinality(self):
        """
        The maximum dictionary cardinality for `auto_dict_encode`.

        This value is per chunk.
        """
        return self.options.auto_dict_max_cardinality

    @auto_dict_max_cardinality.setter
    def auto_dict_max_cardinality(self, value):
        self.options.auto_dict_max_cardinality = value

    @property
    def include_columns(self):
        """
        The names of columns to include in the Table.

        If empty, the Table will include all columns from the CSV file.
        If not empty, only these columns will be included, in this order.
        """
        return [frombytes(s) for s in self.options.include_columns]

    @include_columns.setter
    def include_columns(self, value):
        self.options.include_columns.clear()
        for item in value:
            self.options.include_columns.push_back(tobytes(item))

    @property
    def include_missing_columns(self):
        """
        If false, columns in `include_columns` but not in the CSV file will
        error out.
        If true, columns in `include_columns` but not in the CSV file will
        produce a null column (whose type is selected using `column_types`,
        or null by default).
        This option is ignored if `include_columns` is empty.
        """
        return self.options.include_missing_columns

    @include_missing_columns.setter
    def include_missing_columns(self, value):
        self.options.include_missing_columns = value

    @property
    def timestamp_parsers(self):
        """
        A sequence of strptime()-compatible format strings, tried in order
        when attempting to infer or convert timestamp values (the special
        value ISO8601() can also be given).  By default, a fast built-in
        ISO-8601 parser is used.
        """
        cdef:
            shared_ptr[CTimestampParser] c_parser
            c_string kind

        parsers = []
        for c_parser in self.options.timestamp_parsers:
            kind = deref(c_parser).kind()
            if kind == b'strptime':
                parsers.append(frombytes(deref(c_parser).format()))
            else:
                assert kind == b'iso8601'
                parsers.append(ISO8601)

        return parsers

    @timestamp_parsers.setter
    def timestamp_parsers(self, value):
        cdef:
            vector[shared_ptr[CTimestampParser]] c_parsers

        for v in value:
            if isinstance(v, str):
                c_parsers.push_back(CTimestampParser.MakeStrptime(tobytes(v)))
            elif v == ISO8601:
                c_parsers.push_back(CTimestampParser.MakeISO8601())
            else:
                raise TypeError("Expected list of str or ISO8601 objects")

        self.options.timestamp_parsers = move(c_parsers)


cdef _get_reader(input_file, ReadOptions read_options,
                 shared_ptr[CInputStream]* out):
    use_memory_map = False
    get_input_stream(input_file, use_memory_map, out)
    if read_options is not None:
        out[0] = native_transcoding_input_stream(out[0],
                                                 read_options.encoding,
                                                 'utf8')


cdef _get_read_options(ReadOptions read_options, CCSVReadOptions* out):
    if read_options is None:
        out[0] = CCSVReadOptions.Defaults()
    else:
        out[0] = read_options.options


cdef _get_parse_options(ParseOptions parse_options, CCSVParseOptions* out):
    if parse_options is None:
        out[0] = CCSVParseOptions.Defaults()
    else:
        out[0] = parse_options.options


cdef _get_convert_options(ConvertOptions convert_options,
                          CCSVConvertOptions* out):
    if convert_options is None:
        out[0] = CCSVConvertOptions.Defaults()
    else:
        out[0] = convert_options.options


cdef class CSVStreamingReader(_CRecordBatchReader):
    """An object that reads record batches incrementally from a CSV file.

    Should not be instantiated directly by user code.
    """
    cdef readonly:
        Schema schema

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly, "
                        "use pyarrow.csv.open_csv() instead."
                        .format(self.__class__.__name__))

    cdef _open(self, shared_ptr[CInputStream] stream,
               CCSVReadOptions c_read_options,
               CCSVParseOptions c_parse_options,
               CCSVConvertOptions c_convert_options,
               CMemoryPool* c_memory_pool):
        cdef:
            shared_ptr[CSchema] c_schema

        with nogil:
            self.reader = <shared_ptr[CRecordBatchReader]> GetResultValue(
                CCSVStreamingReader.Make(
                    c_memory_pool, stream,
                    move(c_read_options), move(c_parse_options),
                    move(c_convert_options)))
            c_schema = self.reader.get().schema()

        self.schema = pyarrow_wrap_schema(c_schema)


def read_csv(input_file, read_options=None, parse_options=None,
             convert_options=None, MemoryPool memory_pool=None):
    """
    Read a Table from a stream of CSV data.

    Parameters
    ----------
    input_file: string, path or file-like object
        The location of CSV data.  If a string or path, and if it ends
        with a recognized compressed file extension (e.g. ".gz" or ".bz2"),
        the data is automatically decompressed when reading.
    read_options: pyarrow.csv.ReadOptions, optional
        Options for the CSV reader (see pyarrow.csv.ReadOptions constructor
        for defaults)
    parse_options: pyarrow.csv.ParseOptions, optional
        Options for the CSV parser
        (see pyarrow.csv.ParseOptions constructor for defaults)
    convert_options: pyarrow.csv.ConvertOptions, optional
        Options for converting CSV data
        (see pyarrow.csv.ConvertOptions constructor for defaults)
    memory_pool: MemoryPool, optional
        Pool to allocate Table memory from

    Returns
    -------
    :class:`pyarrow.Table`
        Contents of the CSV file as a in-memory table.
    """
    cdef:
        shared_ptr[CInputStream] stream
        CCSVReadOptions c_read_options
        CCSVParseOptions c_parse_options
        CCSVConvertOptions c_convert_options
        shared_ptr[CCSVReader] reader
        shared_ptr[CTable] table

    _get_reader(input_file, read_options, &stream)
    _get_read_options(read_options, &c_read_options)
    _get_parse_options(parse_options, &c_parse_options)
    _get_convert_options(convert_options, &c_convert_options)

    reader = GetResultValue(CCSVReader.Make(
        maybe_unbox_memory_pool(memory_pool), stream,
        c_read_options, c_parse_options, c_convert_options))

    with nogil:
        table = GetResultValue(reader.get().Read())

    return pyarrow_wrap_table(table)


def open_csv(input_file, read_options=None, parse_options=None,
             convert_options=None, MemoryPool memory_pool=None):
    """
    Open a streaming reader of CSV data.

    Reading using this function is always single-threaded.

    Parameters
    ----------
    input_file: string, path or file-like object
        The location of CSV data.  If a string or path, and if it ends
        with a recognized compressed file extension (e.g. ".gz" or ".bz2"),
        the data is automatically decompressed when reading.
    read_options: pyarrow.csv.ReadOptions, optional
        Options for the CSV reader (see pyarrow.csv.ReadOptions constructor
        for defaults)
    parse_options: pyarrow.csv.ParseOptions, optional
        Options for the CSV parser
        (see pyarrow.csv.ParseOptions constructor for defaults)
    convert_options: pyarrow.csv.ConvertOptions, optional
        Options for converting CSV data
        (see pyarrow.csv.ConvertOptions constructor for defaults)
    memory_pool: MemoryPool, optional
        Pool to allocate Table memory from

    Returns
    -------
    :class:`pyarrow.csv.CSVStreamingReader`
    """
    cdef:
        shared_ptr[CInputStream] stream
        CCSVReadOptions c_read_options
        CCSVParseOptions c_parse_options
        CCSVConvertOptions c_convert_options
        CSVStreamingReader reader

    _get_reader(input_file, read_options, &stream)
    _get_read_options(read_options, &c_read_options)
    _get_parse_options(parse_options, &c_parse_options)
    _get_convert_options(convert_options, &c_convert_options)

    reader = CSVStreamingReader.__new__(CSVStreamingReader)
    reader._open(stream, move(c_read_options), move(c_parse_options),
                 move(c_convert_options),
                 maybe_unbox_memory_pool(memory_pool))
    return reader

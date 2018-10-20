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

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (check_status, MemoryPool, maybe_unbox_memory_pool,
                          pyarrow_wrap_table, get_input_stream)


cdef unsigned char _single_char(s) except 0:
    val = ord(s)
    if val == 0 or val > 127:
        raise ValueError("Expecting an ASCII character")
    return <unsigned char> val


cdef class ReadOptions:
    cdef:
        CCSVReadOptions options

    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, use_threads=None, block_size=None):
        self.options = CCSVReadOptions.Defaults()
        if use_threads is not None:
            self.use_threads = use_threads
        if block_size is not None:
            self.block_size = block_size

    @property
    def use_threads(self):
        return self.options.use_threads

    @use_threads.setter
    def use_threads(self, value):
        self.options.use_threads = value

    @property
    def block_size(self):
        return self.options.block_size

    @block_size.setter
    def block_size(self, value):
        self.options.block_size = value


cdef class ParseOptions:
    cdef:
        CCSVParseOptions options

    __slots__ = ()

    def __init__(self, delimiter=None, quote_char=None, double_quote=None,
                 escape_char=None, header_rows=None, newlines_in_values=None):
        self.options = CCSVParseOptions.Defaults()
        if delimiter is not None:
            self.delimiter = delimiter
        if quote_char is not None:
            self.quote_char = quote_char
        if double_quote is not None:
            self.double_quote = double_quote
        if escape_char is not None:
            self.escape_char = escape_char
        if header_rows is not None:
            self.header_rows = header_rows
        if newlines_in_values is not None:
            self.newlines_in_values = newlines_in_values

    @property
    def delimiter(self):
        return chr(self.options.delimiter)

    @delimiter.setter
    def delimiter(self, value):
        self.options.delimiter = _single_char(value)

    @property
    def quote_char(self):
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
        return self.options.double_quote

    @double_quote.setter
    def double_quote(self, value):
        self.options.double_quote = value

    @property
    def escape_char(self):
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
    def header_rows(self):
        return self.options.header_rows

    @header_rows.setter
    def header_rows(self, value):
        self.options.header_rows = value

    @property
    def newlines_in_values(self):
        return self.options.newlines_in_values

    @newlines_in_values.setter
    def newlines_in_values(self, value):
        self.options.newlines_in_values = value


cdef _get_reader(input_file, shared_ptr[InputStream]* out):
    use_memory_map = False
    get_input_stream(input_file, use_memory_map, out)


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


cdef _get_convert_options(convert_options, CCSVConvertOptions* out):
    if convert_options is None:
        out[0] = CCSVConvertOptions.Defaults()
    else:
        raise NotImplementedError("non-default convert options not supported")


def read_csv(input_file, read_options=None, parse_options=None,
             convert_options=None, MemoryPool memory_pool=None):
    cdef:
        shared_ptr[InputStream] stream
        CCSVReadOptions c_read_options
        CCSVParseOptions c_parse_options
        CCSVConvertOptions c_convert_options
        shared_ptr[CCSVReader] reader
        shared_ptr[CTable] table

    _get_reader(input_file, &stream)
    _get_read_options(read_options, &c_read_options)
    _get_parse_options(parse_options, &c_parse_options)
    _get_convert_options(convert_options, &c_convert_options)

    check_status(CCSVReader.Make(maybe_unbox_memory_pool(memory_pool),
                                 stream, c_read_options, c_parse_options,
                                 c_convert_options, &reader))
    with nogil:
        check_status(reader.get().Read(&table))

    return pyarrow_wrap_table(table)

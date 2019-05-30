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

from __future__ import absolute_import

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (check_status, Field, MemoryPool, ensure_type,
                          maybe_unbox_memory_pool, get_input_stream,
                          pyarrow_wrap_table, pyarrow_wrap_data_type,
                          pyarrow_unwrap_data_type, pyarrow_wrap_schema,
                          pyarrow_unwrap_schema)


cdef class ReadOptions:
    """
    Options for reading JSON files.

    Parameters
    ----------
    use_threads : bool, optional (default True)
        Whether to use multiple threads to accelerate reading
    block_size : int, optional
        How much bytes to process at a time from the input stream.
        This will determine multi-threading granularity as well as
        the size of individual chunks in the Table.
    """
    cdef:
        CJSONReadOptions options

    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, use_threads=None, block_size=None):
        self.options = CJSONReadOptions.Defaults()
        if use_threads is not None:
            self.use_threads = use_threads
        if block_size is not None:
            self.block_size = block_size

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

cdef class ParseOptions:
    """
    Options for parsing JSON files.

    Parameters
    ----------
    explicit_schema: Schema, optional (default None)
        Optional explicit schema (no type inference, ignores other fields).
    newlines_in_values: bool, optional (default False)
        Whether objects may be printed across multiple lines (for example
        pretty printed). If false, input must end with an empty line.
    """

    cdef:
        CJSONParseOptions options

    __slots__ = ()

    def __init__(self, explicit_schema=None, newlines_in_values=None):
        self.options = CJSONParseOptions.Defaults()
        if explicit_schema is not None:
            self.explicit_schema = explicit_schema
        if newlines_in_values is not None:
            self.newlines_in_values = newlines_in_values

    @property
    def explicit_schema(self):
        """
        Optional explicit schema (no type inference, ignores other fields)
        """
        if self.options.explicit_schema.get() == NULL:
            return None
        else:
            return pyarrow_wrap_schema(self.options.explicit_schema)

    @explicit_schema.setter
    def explicit_schema(self, value):
        self.options.explicit_schema = pyarrow_unwrap_schema(value)

    @property
    def newlines_in_values(self):
        """
        Whether newline characters are allowed in JSON values.
        Setting this to True reduces the performance of multi-threaded
        JSON reading.
        """
        return self.options.newlines_in_values

    @newlines_in_values.setter
    def newlines_in_values(self, value):
        self.options.newlines_in_values = value


cdef _get_reader(input_file, shared_ptr[InputStream]* out):
    use_memory_map = False
    get_input_stream(input_file, use_memory_map, out)

cdef _get_read_options(ReadOptions read_options, CJSONReadOptions* out):
    if read_options is None:
        out[0] = CJSONReadOptions.Defaults()
    else:
        out[0] = read_options.options

cdef _get_parse_options(ParseOptions parse_options, CJSONParseOptions* out):
    if parse_options is None:
        out[0] = CJSONParseOptions.Defaults()
    else:
        out[0] = parse_options.options


def read_json(input_file, read_options=None, parse_options=None,
              MemoryPool memory_pool=None):
    """
    Read a Table from a stream of JSON data.

    Parameters
    ----------
    input_file: string, path or file-like object
        The location of JSON data.
    read_options: ReadOptions, optional
        Options for the JSON reader (see ReadOptions constructor for defaults)
    parse_options: ParseOptions, optional
        Options for the JSON parser
        (see ParseOptions constructor for defaults)
    memory_pool: MemoryPool, optional
        Pool to allocate Table memory from

    Returns
    -------
    :class:`pyarrow.Table`
        Contents of the JSON file as a in-memory table.
    """
    cdef:
        shared_ptr[InputStream] stream
        CJSONReadOptions c_read_options
        CJSONParseOptions c_parse_options
        shared_ptr[CJSONReader] reader
        shared_ptr[CTable] table

    _get_reader(input_file, &stream)
    _get_read_options(read_options, &c_read_options)
    _get_parse_options(parse_options, &c_parse_options)

    check_status(CJSONReader.Make(maybe_unbox_memory_pool(memory_pool),
                                  stream, c_read_options, c_parse_options,
                                  &reader))

    with nogil:
        check_status(reader.get().Read(&table))

    return pyarrow_wrap_table(table)

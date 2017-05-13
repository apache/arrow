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

# Arrow file and stream reader/writer classes, and other messaging tools

import pyarrow.lib as lib


class RecordBatchStreamReader(lib._RecordBatchReader):
    """
    Reader for the Arrow streaming binary format

    Parameters
    ----------
    source : str, pyarrow.NativeFile, or file-like Python object
        Either a file path, or a readable file object
    """
    def __init__(self, source):
        self._open(source)

    def __iter__(self):
        while True:
            yield self.get_next_batch()


class RecordBatchStreamWriter(lib._RecordBatchWriter):
    """
    Writer for the Arrow streaming binary format

    Parameters
    ----------
    sink : str, pyarrow.NativeFile, or file-like Python object
        Either a file path, or a writeable file object
    schema : pyarrow.Schema
        The Arrow schema for data to be written to the file
    """
    def __init__(self, sink, schema):
        self._open(sink, schema)


class RecordBatchFileReader(lib._RecordBatchFileReader):
    """
    Class for reading Arrow record batch data from the Arrow binary file format

    Parameters
    ----------
    source : str, pyarrow.NativeFile, or file-like Python object
        Either a file path, or a readable file object
    footer_offset : int, default None
        If the file is embedded in some larger file, this is the byte offset to
        the very end of the file data
    """
    def __init__(self, source, footer_offset=None):
        self._open(source, footer_offset=footer_offset)


class RecordBatchFileWriter(lib._RecordBatchFileWriter):
    """
    Writer to create the Arrow binary file format

    Parameters
    ----------
    sink : str, pyarrow.NativeFile, or file-like Python object
        Either a file path, or a writeable file object
    schema : pyarrow.Schema
        The Arrow schema for data to be written to the file
    """
    def __init__(self, sink, schema):
        self._open(sink, schema)


def open_stream(source):
    """
    Create reader for Arrow streaming format

    Parameters
    ----------
    source : str, pyarrow.NativeFile, or file-like Python object
        Either a file path, or a readable file object
    footer_offset : int, default None
        If the file is embedded in some larger file, this is the byte offset to
        the very end of the file data

    Returns
    -------
    reader : RecordBatchStreamReader
    """
    return RecordBatchStreamReader(source)


def open_file(source, footer_offset=None):
    """
    Create reader for Arrow file format

    Parameters
    ----------
    source : str, pyarrow.NativeFile, or file-like Python object
        Either a file path, or a readable file object
    footer_offset : int, default None
        If the file is embedded in some larger file, this is the byte offset to
        the very end of the file data

    Returns
    -------
    reader : RecordBatchFileReader
    """
    return RecordBatchFileReader(source, footer_offset=footer_offset)

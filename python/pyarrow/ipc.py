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

import pyarrow as pa

from pyarrow.lib import (Message, MessageReader,  # noqa
                         read_message, read_record_batch, read_schema,
                         read_tensor, write_tensor,
                         get_record_batch_size, get_tensor_size)
import pyarrow.lib as lib


class _ReadPandasOption(object):

    def read_pandas(self, **options):
        """
        Read contents of stream and convert to pandas.DataFrame using
        Table.to_pandas

        Parameters
        ----------
        **options : arguments to forward to Table.to_pandas

        Returns
        -------
        df : pandas.DataFrame
        """
        table = self.read_all()
        return table.to_pandas(**options)


class RecordBatchStreamReader(lib._RecordBatchReader, _ReadPandasOption):
    """
    Reader for the Arrow streaming binary format

    Parameters
    ----------
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object
    """
    def __init__(self, source):
        self._open(source)


class RecordBatchStreamWriter(lib._RecordBatchWriter):
    """
    Writer for the Arrow streaming binary format

    Parameters
    ----------
    sink : str, pyarrow.NativeFile, or file-like Python object
        Either a file path, or a writable file object
    schema : pyarrow.Schema
        The Arrow schema for data to be written to the file
    """
    def __init__(self, sink, schema):
        self._open(sink, schema)


class RecordBatchFileReader(lib._RecordBatchFileReader, _ReadPandasOption):
    """
    Class for reading Arrow record batch data from the Arrow binary file format

    Parameters
    ----------
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object
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
        Either a file path, or a writable file object
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
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object
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
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object
    footer_offset : int, default None
        If the file is embedded in some larger file, this is the byte offset to
        the very end of the file data

    Returns
    -------
    reader : RecordBatchFileReader
    """
    return RecordBatchFileReader(source, footer_offset=footer_offset)


def serialize_pandas(df, nthreads=None, preserve_index=True):
    """Serialize a pandas DataFrame into a buffer protocol compatible object.

    Parameters
    ----------
    df : pandas.DataFrame
    nthreads : int, default None
        Number of threads to use for conversion to Arrow, default all CPUs
    preserve_index : boolean, default True
        If True, preserve the pandas index data, otherwise the result will have
        a default RangeIndex

    Returns
    -------
    buf : buffer
        An object compatible with the buffer protocol
    """
    batch = pa.RecordBatch.from_pandas(df, nthreads=nthreads,
                                       preserve_index=preserve_index)
    sink = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue()


def deserialize_pandas(buf, use_threads=True):
    """Deserialize a buffer protocol compatible object into a pandas DataFrame.

    Parameters
    ----------
    buf : buffer
        An object compatible with the buffer protocol
    use_threads: boolean, default True
        Whether to parallelize the conversion using multiple threads

    Returns
    -------
    df : pandas.DataFrame
    """
    buffer_reader = pa.BufferReader(buf)
    reader = pa.RecordBatchStreamReader(buffer_reader)
    table = reader.read_all()
    return table.to_pandas(use_threads=use_threads)

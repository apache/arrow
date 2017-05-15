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

import io
import pytest
import socket
import threading

import numpy as np

from pandas.util.testing import assert_frame_equal
import pandas as pd

from pyarrow.compat import unittest
import pyarrow as pa


class MessagingTest(object):

    def setUp(self):
        self.sink = self._get_sink()

    def _get_sink(self):
        return io.BytesIO()

    def _get_source(self):
        return pa.BufferReader(self.sink.getvalue())

    def write_batches(self):
        nrows = 5
        df = pd.DataFrame({
            'one': np.random.randn(nrows),
            'two': ['foo', np.nan, 'bar', 'bazbaz', 'qux']})

        batch = pa.RecordBatch.from_pandas(df)

        writer = self._get_writer(self.sink, batch.schema)

        num_batches = 5
        frames = []
        batches = []
        for i in range(num_batches):
            unique_df = df.copy()
            unique_df['one'] = np.random.randn(nrows)

            batch = pa.RecordBatch.from_pandas(unique_df)
            writer.write_batch(batch)
            frames.append(unique_df)
            batches.append(batch)

        writer.close()
        return batches


class TestFile(MessagingTest, unittest.TestCase):
    # Also tests writing zero-copy NumPy array with additional padding

    def _get_writer(self, sink, schema):
        return pa.RecordBatchFileWriter(sink, schema)

    def test_simple_roundtrip(self):
        batches = self.write_batches()
        file_contents = self._get_source()

        reader = pa.open_file(file_contents)

        assert reader.num_record_batches == len(batches)

        for i, batch in enumerate(batches):
            # it works. Must convert back to DataFrame
            batch = reader.get_batch(i)
            assert batches[i].equals(batch)

    def test_read_all(self):
        batches = self.write_batches()
        file_contents = self._get_source()

        reader = pa.open_file(file_contents)

        result = reader.read_all()
        expected = pa.Table.from_batches(batches)
        assert result.equals(expected)


class TestStream(MessagingTest, unittest.TestCase):

    def _get_writer(self, sink, schema):
        return pa.RecordBatchStreamWriter(sink, schema)

    def test_simple_roundtrip(self):
        batches = self.write_batches()
        file_contents = self._get_source()
        reader = pa.open_stream(file_contents)

        assert reader.schema.equals(batches[0].schema)

        total = 0
        for i, next_batch in enumerate(reader):
            assert next_batch.equals(batches[i])
            total += 1

        assert total == len(batches)

        with pytest.raises(StopIteration):
            reader.get_next_batch()

    def test_read_all(self):
        batches = self.write_batches()
        file_contents = self._get_source()
        reader = pa.open_stream(file_contents)

        result = reader.read_all()
        expected = pa.Table.from_batches(batches)
        assert result.equals(expected)


class TestSocket(MessagingTest, unittest.TestCase):

    class StreamReaderServer(threading.Thread):

        def init(self, do_read_all):
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.bind(('127.0.0.1', 0))
            self._sock.listen(1)
            host, port = self._sock.getsockname()
            self._do_read_all = do_read_all
            self._schema = None
            self._batches = []
            self._table = None
            return port

        def run(self):
            connection, client_address = self._sock.accept()
            try:
                source = connection.makefile(mode='rb')
                reader = pa.open_stream(source)
                self._schema = reader.schema
                if self._do_read_all:
                    self._table = reader.read_all()
                else:
                    for i, batch in enumerate(reader):
                        self._batches.append(batch)
            finally:
                connection.close()

        def get_result(self):
            return(self._schema, self._table if self._do_read_all
                   else self._batches)

    def setUp(self):
        # NOTE: must start and stop server in test
        pass

    def start_server(self, do_read_all):
        self._server = TestSocket.StreamReaderServer()
        port = self._server.init(do_read_all)
        self._server.start()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect(('127.0.0.1', port))
        self.sink = self._get_sink()

    def stop_and_get_result(self):
        import struct
        self.sink.write(struct.pack('i', 0))
        self.sink.flush()
        self._sock.close()
        self._server.join()
        return self._server.get_result()

    def _get_sink(self):
        return self._sock.makefile(mode='wb')

    def _get_writer(self, sink, schema):
        return pa.RecordBatchStreamWriter(sink, schema)

    def test_simple_roundtrip(self):
        self.start_server(do_read_all=False)
        writer_batches = self.write_batches()
        reader_schema, reader_batches = self.stop_and_get_result()

        assert reader_schema.equals(writer_batches[0].schema)
        assert len(reader_batches) == len(writer_batches)
        for i, batch in enumerate(writer_batches):
            assert reader_batches[i].equals(batch)

    def test_read_all(self):
        self.start_server(do_read_all=True)
        writer_batches = self.write_batches()
        _, result = self.stop_and_get_result()

        expected = pa.Table.from_batches(writer_batches)
        assert result.equals(expected)


class TestInMemoryFile(TestFile):

    def _get_sink(self):
        return pa.BufferOutputStream()

    def _get_source(self):
        return self.sink.get_result()


def test_ipc_zero_copy_numpy():
    df = pd.DataFrame({'foo': [1.5]})

    batch = pa.RecordBatch.from_pandas(df)
    sink = pa.BufferOutputStream()
    write_file(batch, sink)
    buffer = sink.get_result()
    reader = pa.BufferReader(buffer)

    batches = read_file(reader)

    data = batches[0].to_pandas()
    rdf = pd.DataFrame(data)
    assert_frame_equal(df, rdf)


def test_get_record_batch_size():
    N = 10
    itemsize = 8
    df = pd.DataFrame({'foo': np.random.randn(N)})

    batch = pa.RecordBatch.from_pandas(df)
    assert pa.get_record_batch_size(batch) > (N * itemsize)


def write_file(batch, sink):
    writer = pa.RecordBatchFileWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()


def read_file(source):
    reader = pa.open_file(source)
    return [reader.get_batch(i)
            for i in range(reader.num_record_batches)]

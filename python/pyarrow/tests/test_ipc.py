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

from pandas.util.testing import (assert_frame_equal,
                                 assert_series_equal)
import pandas as pd

from pyarrow.compat import unittest
import pyarrow as pa


class MessagingTest(object):

    def setUp(self):
        self.sink = self._get_sink()

    def _get_sink(self):
        return io.BytesIO()

    def _get_source(self):
        return self.sink.getvalue()

    def write_batches(self, num_batches=5, as_table=False):
        nrows = 5
        df = pd.DataFrame({
            'one': np.random.randn(nrows),
            'two': ['foo', np.nan, 'bar', 'bazbaz', 'qux']})
        batch = pa.RecordBatch.from_pandas(df)

        writer = self._get_writer(self.sink, batch.schema)

        frames = []
        batches = []
        for i in range(num_batches):
            unique_df = df.copy()
            unique_df['one'] = np.random.randn(len(df))
            batch = pa.RecordBatch.from_pandas(unique_df)
            frames.append(unique_df)
            batches.append(batch)

        if as_table:
            table = pa.Table.from_batches(batches)
            writer.write_table(table)
        else:
            for batch in batches:
                writer.write_batch(batch)

        writer.close()
        return frames, batches


class TestFile(MessagingTest, unittest.TestCase):
    # Also tests writing zero-copy NumPy array with additional padding

    def _get_writer(self, sink, schema):
        return pa.RecordBatchFileWriter(sink, schema)

    def test_empty_file(self):
        buf = io.BytesIO(b'')
        with pytest.raises(pa.ArrowInvalid):
            pa.open_file(buf)

    def _check_roundtrip(self, as_table=False):
        _, batches = self.write_batches(as_table=as_table)
        file_contents = pa.BufferReader(self._get_source())

        reader = pa.open_file(file_contents)

        assert reader.num_record_batches == len(batches)

        for i, batch in enumerate(batches):
            # it works. Must convert back to DataFrame
            batch = reader.get_batch(i)
            assert batches[i].equals(batch)
            assert reader.schema.equals(batches[0].schema)

    def test_simple_roundtrip(self):
        self._check_roundtrip(as_table=False)

    def test_write_table(self):
        self._check_roundtrip(as_table=True)

    def test_read_all(self):
        _, batches = self.write_batches()
        file_contents = pa.BufferReader(self._get_source())

        reader = pa.open_file(file_contents)

        result = reader.read_all()
        expected = pa.Table.from_batches(batches)
        assert result.equals(expected)

    def test_read_pandas(self):
        frames, _ = self.write_batches()

        file_contents = pa.BufferReader(self._get_source())
        reader = pa.open_file(file_contents)
        result = reader.read_pandas()

        expected = pd.concat(frames)
        assert_frame_equal(result, expected)


class TestStream(MessagingTest, unittest.TestCase):

    def _get_writer(self, sink, schema):
        return pa.RecordBatchStreamWriter(sink, schema)

    def test_empty_stream(self):
        buf = io.BytesIO(b'')
        with pytest.raises(pa.ArrowInvalid):
            pa.open_stream(buf)

    def test_categorical_roundtrip(self):
        df = pd.DataFrame({
            'one': np.random.randn(5),
            'two': pd.Categorical(['foo', np.nan, 'bar', 'foo', 'foo'],
                                  categories=['foo', 'bar'],
                                  ordered=True)
        })
        batch = pa.RecordBatch.from_pandas(df)
        writer = self._get_writer(self.sink, batch.schema)
        writer.write_batch(pa.RecordBatch.from_pandas(df))
        writer.close()

        table = (pa.open_stream(pa.BufferReader(self._get_source()))
                 .read_all())
        assert_frame_equal(table.to_pandas(), df)

    def test_stream_write_dispatch(self):
        # ARROW-1616
        df = pd.DataFrame({
            'one': np.random.randn(5),
            'two': pd.Categorical(['foo', np.nan, 'bar', 'foo', 'foo'],
                                  categories=['foo', 'bar'],
                                  ordered=True)
        })
        table = pa.Table.from_pandas(df, preserve_index=False)
        batch = pa.RecordBatch.from_pandas(df, preserve_index=False)
        writer = self._get_writer(self.sink, table.schema)
        writer.write(table)
        writer.write(batch)
        writer.close()

        table = (pa.open_stream(pa.BufferReader(self._get_source()))
                 .read_all())
        assert_frame_equal(table.to_pandas(),
                           pd.concat([df, df], ignore_index=True))

    def test_stream_write_table_batches(self):
        # ARROW-504
        df = pd.DataFrame({
            'one': np.random.randn(20),
        })

        b1 = pa.RecordBatch.from_pandas(df[:10], preserve_index=False)
        b2 = pa.RecordBatch.from_pandas(df, preserve_index=False)

        table = pa.Table.from_batches([b1, b2, b1])

        writer = self._get_writer(self.sink, table.schema)
        writer.write_table(table, chunksize=15)
        writer.close()

        batches = list(pa.open_stream(pa.BufferReader(self._get_source())))

        assert list(map(len, batches)) == [10, 15, 5, 10]
        result_table = pa.Table.from_batches(batches)
        assert_frame_equal(result_table.to_pandas(),
                           pd.concat([df[:10], df, df[:10]],
                                     ignore_index=True))

    def test_simple_roundtrip(self):
        _, batches = self.write_batches()
        file_contents = pa.BufferReader(self._get_source())
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
        _, batches = self.write_batches()
        file_contents = pa.BufferReader(self._get_source())
        reader = pa.open_stream(file_contents)

        result = reader.read_all()
        expected = pa.Table.from_batches(batches)
        assert result.equals(expected)


class TestMessageReader(MessagingTest, unittest.TestCase):

    def _get_example_messages(self):
        _, batches = self.write_batches()
        file_contents = self._get_source()
        buf_reader = pa.BufferReader(file_contents)
        reader = pa.MessageReader.open_stream(buf_reader)
        return batches, list(reader)

    def _get_writer(self, sink, schema):
        return pa.RecordBatchStreamWriter(sink, schema)

    def test_ctors_no_segfault(self):
        with pytest.raises(TypeError):
            repr(pa.Message())

        with pytest.raises(TypeError):
            repr(pa.MessageReader())

    def test_message_reader(self):
        _, messages = self._get_example_messages()

        assert len(messages) == 6
        assert messages[0].type == 'schema'
        for msg in messages[1:]:
            assert msg.type == 'record batch'

    def test_serialize_read_message(self):
        _, messages = self._get_example_messages()

        msg = messages[0]
        buf = msg.serialize()

        restored = pa.read_message(buf)
        restored2 = pa.read_message(pa.BufferReader(buf))
        restored3 = pa.read_message(buf.to_pybytes())

        assert msg.equals(restored)
        assert msg.equals(restored2)
        assert msg.equals(restored3)

    def test_read_record_batch(self):
        batches, messages = self._get_example_messages()

        for batch, message in zip(batches, messages[1:]):
            read_batch = pa.read_record_batch(message, batch.schema)
            assert read_batch.equals(batch)

    def test_read_pandas(self):
        frames, _ = self.write_batches()
        file_contents = pa.BufferReader(self._get_source())
        reader = pa.open_stream(file_contents)
        result = reader.read_pandas()

        expected = pd.concat(frames)
        assert_frame_equal(result, expected)


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
        _, writer_batches = self.write_batches()
        reader_schema, reader_batches = self.stop_and_get_result()

        assert reader_schema.equals(writer_batches[0].schema)
        assert len(reader_batches) == len(writer_batches)
        for i, batch in enumerate(writer_batches):
            assert reader_batches[i].equals(batch)

    def test_read_all(self):
        self.start_server(do_read_all=True)
        _, writer_batches = self.write_batches()
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


def test_ipc_stream_no_batches():
    # ARROW-2307
    table = pa.Table.from_arrays([pa.array([1, 2, 3, 4]),
                                  pa.array(['foo', 'bar', 'baz', 'qux'])],
                                 names=['a', 'b'])

    sink = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(sink, table.schema)
    writer.close()

    source = sink.get_result()
    reader = pa.open_stream(source)
    result = reader.read_all()

    assert result.schema.equals(table.schema)
    assert len(result) == 0


def test_get_record_batch_size():
    N = 10
    itemsize = 8
    df = pd.DataFrame({'foo': np.random.randn(N)})

    batch = pa.RecordBatch.from_pandas(df)
    assert pa.get_record_batch_size(batch) > (N * itemsize)


def _check_serialize_pandas_round_trip(df, use_threads=False):
    buf = pa.serialize_pandas(df, nthreads=2 if use_threads else 1)
    result = pa.deserialize_pandas(buf, use_threads=use_threads)
    assert_frame_equal(result, df)


def test_pandas_serialize_round_trip():
    index = pd.Index([1, 2, 3], name='my_index')
    columns = ['foo', 'bar']
    df = pd.DataFrame(
        {'foo': [1.5, 1.6, 1.7], 'bar': list('abc')},
        index=index, columns=columns
    )
    _check_serialize_pandas_round_trip(df)


def test_pandas_serialize_round_trip_nthreads():
    index = pd.Index([1, 2, 3], name='my_index')
    columns = ['foo', 'bar']
    df = pd.DataFrame(
        {'foo': [1.5, 1.6, 1.7], 'bar': list('abc')},
        index=index, columns=columns
    )
    _check_serialize_pandas_round_trip(df, use_threads=True)


def test_pandas_serialize_round_trip_multi_index():
    index1 = pd.Index([1, 2, 3], name='level_1')
    index2 = pd.Index(list('def'), name=None)
    index = pd.MultiIndex.from_arrays([index1, index2])

    columns = ['foo', 'bar']
    df = pd.DataFrame(
        {'foo': [1.5, 1.6, 1.7], 'bar': list('abc')},
        index=index,
        columns=columns,
    )
    _check_serialize_pandas_round_trip(df)


def test_serialize_pandas_empty_dataframe():
    df = pd.DataFrame()
    _check_serialize_pandas_round_trip(df)


def test_pandas_serialize_round_trip_not_string_columns():
    df = pd.DataFrame(list(zip([1.5, 1.6, 1.7], 'abc')))
    buf = pa.serialize_pandas(df)
    result = pa.deserialize_pandas(buf)
    assert_frame_equal(result, df)


def test_serialize_pandas_no_preserve_index():
    df = pd.DataFrame({'a': [1, 2, 3]}, index=[1, 2, 3])
    expected = pd.DataFrame({'a': [1, 2, 3]})

    buf = pa.serialize_pandas(df, preserve_index=False)
    result = pa.deserialize_pandas(buf)
    assert_frame_equal(result, expected)

    buf = pa.serialize_pandas(df, preserve_index=True)
    result = pa.deserialize_pandas(buf)
    assert_frame_equal(result, df)


def test_serialize_with_pandas_objects():
    df = pd.DataFrame({'a': [1, 2, 3]}, index=[1, 2, 3])
    s = pd.Series([1, 2, 3, 4])

    data = {
        'a_series': df['a'],
        'a_frame': df,
        's_series': s
    }

    serialized = pa.serialize(data).to_buffer()
    deserialized = pa.deserialize(serialized)
    assert_frame_equal(deserialized['a_frame'], df)

    assert_series_equal(deserialized['a_series'], df['a'])
    assert deserialized['a_series'].name == 'a'

    assert_series_equal(deserialized['s_series'], s)
    assert deserialized['s_series'].name is None


def test_schema_batch_serialize_methods():
    nrows = 5
    df = pd.DataFrame({
        'one': np.random.randn(nrows),
        'two': ['foo', np.nan, 'bar', 'bazbaz', 'qux']})
    batch = pa.RecordBatch.from_pandas(df)

    s_schema = batch.schema.serialize()
    s_batch = batch.serialize()

    recons_schema = pa.read_schema(s_schema)
    recons_batch = pa.read_record_batch(s_batch, recons_schema)
    assert recons_batch.equals(batch)


def test_schema_serialization_with_metadata():
    field_metadata = {b'foo': b'bar', b'kind': b'field'}
    schema_metadata = {b'foo': b'bar', b'kind': b'schema'}

    f0 = pa.field('a', pa.int8())
    f1 = pa.field('b', pa.string(), metadata=field_metadata)

    schema = pa.schema([f0, f1], metadata=schema_metadata)

    s_schema = schema.serialize()
    recons_schema = pa.read_schema(s_schema)

    assert recons_schema.equals(schema)
    assert recons_schema.metadata == schema_metadata
    assert recons_schema[0].metadata is None
    assert recons_schema[1].metadata == field_metadata


def write_file(batch, sink):
    writer = pa.RecordBatchFileWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()


def read_file(source):
    reader = pa.open_file(source)
    return [reader.get_batch(i)
            for i in range(reader.num_record_batches)]

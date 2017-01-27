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
        return pa.FileWriter(sink, schema)

    def test_simple_roundtrip(self):
        batches = self.write_batches()
        file_contents = self._get_source()

        reader = pa.FileReader(file_contents)

        assert reader.num_record_batches == len(batches)

        for i, batch in enumerate(batches):
            # it works. Must convert back to DataFrame
            batch = reader.get_batch(i)
            assert batches[i].equals(batch)

    def test_read_all(self):
        batches = self.write_batches()
        file_contents = self._get_source()

        reader = pa.FileReader(file_contents)

        result = reader.read_all()
        expected = pa.Table.from_batches(batches)
        assert result.equals(expected)


class TestStream(MessagingTest, unittest.TestCase):

    def _get_writer(self, sink, schema):
        return pa.StreamWriter(sink, schema)

    def test_simple_roundtrip(self):
        batches = self.write_batches()
        file_contents = self._get_source()
        reader = pa.StreamReader(file_contents)

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
        reader = pa.StreamReader(file_contents)

        result = reader.read_all()
        expected = pa.Table.from_batches(batches)
        assert result.equals(expected)


class TestInMemoryFile(TestFile):

    def _get_sink(self):
        return pa.InMemoryOutputStream()

    def _get_source(self):
        return self.sink.get_result()


def test_ipc_zero_copy_numpy():
    df = pd.DataFrame({'foo': [1.5]})

    batch = pa.RecordBatch.from_pandas(df)
    sink = pa.InMemoryOutputStream()
    write_file(batch, sink)
    buffer = sink.get_result()
    reader = pa.BufferReader(buffer)

    batches = read_file(reader)

    data = batches[0].to_pandas()
    rdf = pd.DataFrame(data)
    assert_frame_equal(df, rdf)


def write_file(batch, sink):
    writer = pa.FileWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()


def read_file(source):
    reader = pa.FileReader(source)
    return [reader.get_batch(i)
            for i in range(reader.num_record_batches)]

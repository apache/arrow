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

import numpy as np

from pandas.util.testing import assert_frame_equal
import pandas as pd

import pyarrow as A
import pyarrow.io as aio
import pyarrow.ipc as ipc


class RoundtripTest(object):
    # Also tests writing zero-copy NumPy array with additional padding

    def __init__(self):
        self.sink = self._get_sink()

    def _get_sink(self):
        return io.BytesIO()

    def _get_source(self):
        return self.sink.getvalue()

    def run(self):
        nrows = 5
        df = pd.DataFrame({
            'one': np.random.randn(nrows),
            'two': ['foo', np.nan, 'bar', 'bazbaz', 'qux']})

        batch = A.RecordBatch.from_pandas(df)
        writer = ipc.FileWriter(self.sink, batch.schema)

        num_batches = 5
        frames = []
        batches = []
        for i in range(num_batches):
            unique_df = df.copy()
            unique_df['one'] = np.random.randn(nrows)

            batch = A.RecordBatch.from_pandas(unique_df)
            writer.write_batch(batch)
            frames.append(unique_df)
            batches.append(batch)

        writer.close()

        file_contents = self._get_source()
        reader = ipc.FileReader(aio.BufferReader(file_contents))

        assert reader.num_record_batches == num_batches

        for i in range(num_batches):
            # it works. Must convert back to DataFrame
            batch = reader.get_batch(i)
            assert batches[i].equals(batch)


class InMemoryStreamTest(RoundtripTest):

    def _get_sink(self):
        return aio.InMemoryOutputStream()

    def _get_source(self):
        return self.sink.get_result()


def test_ipc_file_simple_roundtrip():
    helper = RoundtripTest()
    helper.run()


def test_ipc_zero_copy_numpy():
    df = pd.DataFrame({'foo': [1.5]})

    batch = A.RecordBatch.from_pandas(df)
    sink = aio.InMemoryOutputStream()
    write_file(batch, sink)
    buffer = sink.get_result()
    reader = aio.BufferReader(buffer)

    batches = read_file(reader)

    data = batches[0].to_pandas()
    rdf = pd.DataFrame(data)
    assert_frame_equal(df, rdf)


def write_file(batch, sink):
    writer = ipc.FileWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()


def read_file(source):
    reader = ipc.FileReader(source)
    return [reader.get_batch(i)
            for i in range(reader.num_record_batches)]

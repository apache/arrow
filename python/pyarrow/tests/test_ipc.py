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
import pandas as pd

import pyarrow as A
import pyarrow.io as arrow_io
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
        writer = ipc.ArrowFileWriter(self.sink, batch.schema)

        num_batches = 5
        frames = []
        batches = []
        for i in range(num_batches):
            unique_df = df.copy()
            unique_df['one'] = np.random.randn(nrows)

            batch = A.RecordBatch.from_pandas(unique_df)
            writer.write_record_batch(batch)
            frames.append(unique_df)
            batches.append(batch)

        writer.close()

        file_contents = self._get_source()
        reader = ipc.ArrowFileReader(file_contents)

        assert reader.num_record_batches == num_batches

        for i in range(num_batches):
            # it works. Must convert back to DataFrame
            batch = reader.get_record_batch(i)
            assert batches[i].equals(batch)


class InMemoryStreamTest(RoundtripTest):

    def _get_sink(self):
        return arrow_io.InMemoryOutputStream()

    def _get_source(self):
        return self.sink.get_result()


def test_ipc_file_simple_roundtrip():
    helper = RoundtripTest()
    helper.run()


# XXX: For benchmarking

def big_batch():
    df = pd.DataFrame(
        np.random.randn(2**4, 2**20).T,
        columns=[str(i) for i in range(2**4)]
    )

    df = pd.concat([df] * 2 ** 3, ignore_index=True)

    return A.RecordBatch.from_pandas(df)


def write_to_memory(batch):
    sink = io.BytesIO()
    write_file(batch, sink)
    return sink.getvalue()


def write_file(batch, sink):
    writer = ipc.ArrowFileWriter(sink, batch.schema)
    writer.write_record_batch(batch)
    writer.close()


def read_file(source):
    reader = ipc.ArrowFileReader(source)
    return [reader.get_record_batch(i)
            for i in range(reader.num_record_batches)]

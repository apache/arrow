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

from pandas.util.testing import assert_frame_equal
import numpy as np
import pandas as pd

import pyarrow as A
import pyarrow.ipc as ipc


def test_ipc_simple_roundtrip():
    nrows = 5
    df = pd.DataFrame({
        'one': np.random.randn(nrows),
        'two': ['foo', np.nan, 'bar', 'bazbaz', 'qux']})

    batch = A.RecordBatch.from_pandas(df)

    sink = io.BytesIO()
    writer = ipc.ArrowFileWriter(sink, batch.schema)

    num_batches = 5
    frames = []
    for i in range(num_batches):
        unique_df = df.copy()
        unique_df['one'] = np.random.randn(nrows)

        batch = A.RecordBatch.from_pandas(unique_df)
        writer.write_record_batch(batch)
        frames.append(unique_df)

    writer.close()

    file_contents = sink.getvalue()
    reader = ipc.ArrowFileReader(file_contents)

    assert reader.num_record_batches == num_batches

    for i in range(num_batches):
        # it works. Must convert back to DataFrame
        reader.get_record_batch(i)


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

batch = big_batch()
mem = write_to_memory(batch)

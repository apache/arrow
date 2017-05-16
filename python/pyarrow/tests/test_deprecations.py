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

# Check that various deprecation warnings are raised

import pyarrow as pa
import pytest


def test_inmemory_output_stream():
    with pytest.warns(FutureWarning):
        stream = pa.InMemoryOutputStream()
        assert isinstance(stream, pa.BufferOutputStream)


def test_file_reader_writer():
    data = [
        pa.array([1, 2, 3, 4]),
        pa.array(['foo', 'bar', 'baz', None]),
        pa.array([True, None, False, True])
    ]
    batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])

    sink = pa.BufferOutputStream()

    with pytest.warns(FutureWarning):
        stream_writer = pa.StreamWriter(sink, batch.schema)
        assert isinstance(stream_writer, pa.RecordBatchStreamWriter)

    sink2 = pa.BufferOutputStream()
    with pytest.warns(FutureWarning):
        file_writer = pa.FileWriter(sink2, batch.schema)
        assert isinstance(file_writer, pa.RecordBatchFileWriter)

    file_writer.write_batch(batch)
    stream_writer.write_batch(batch)

    file_writer.close()
    stream_writer.close()

    buf = sink.get_result()
    buf2 = sink2.get_result()

    with pytest.warns(FutureWarning):
        stream_reader = pa.StreamReader(buf)
        assert isinstance(stream_reader, pa.RecordBatchStreamReader)

    with pytest.warns(FutureWarning):
        file_reader = pa.FileReader(buf2)
        assert isinstance(file_reader, pa.RecordBatchFileReader)

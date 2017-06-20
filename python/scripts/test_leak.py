#!/usr/bin/env python

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

import pyarrow as pa
import numpy as np
import memory_profiler
import gc
import io


def leak():
    data = [pa.array(np.concatenate([np.random.randn(100000)] * 1000))]
    table = pa.Table.from_arrays(data, ['foo'])
    while True:
        print('calling to_pandas')
        print('memory_usage: {0}'.format(memory_profiler.memory_usage()))
        table.to_pandas()
        gc.collect()

# leak()


def leak2():
    data = [pa.array(np.concatenate([np.random.randn(100000)] * 10))]
    table = pa.Table.from_arrays(data, ['foo'])
    while True:
        print('calling to_pandas')
        print('memory_usage: {0}'.format(memory_profiler.memory_usage()))
        df = table.to_pandas()

        batch = pa.RecordBatch.from_pandas(df)

        sink = io.BytesIO()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()

        buf_reader = pa.BufferReader(sink.getvalue())
        reader = pa.open_file(buf_reader)
        reader.read_all()

        gc.collect()

leak2()

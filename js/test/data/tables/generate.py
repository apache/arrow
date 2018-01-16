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
import random
import numpy as np
import pandas as pd


cities = [u'Charlottesville', u'New York', u'San Francisco', u'Seattle', u'Terre Haute', u'Washington, DC']

def generate_batch(batch_len):
    return pa.RecordBatch.from_arrays([
        pa.Array.from_pandas(pd.Series(np.random.uniform(-90,90,batch_len), dtype="float32")),
        pa.Array.from_pandas(pd.Series(np.random.uniform(-180,180,batch_len), dtype="float32")),
        pa.Array.from_pandas(pd.Categorical((random.choice(cities) for i in range(batch_len)), cities)),
        pa.Array.from_pandas(pd.Categorical((random.choice(cities) for i in range(batch_len)), cities))
    ], ['lat', 'lng', 'origin', 'destination'])

def write_record_batches(fd, batch_len, num_batches):
    writer = pa.ipc.RecordBatchStreamWriter(fd, generate_batch(1).schema)
    for batch in range(num_batches):
        writer.write_batch(generate_batch(batch_len))

    writer.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='number of batches')
    parser.add_argument('-n', '--num-batches', help='number of batches', type=int, default=10)
    parser.add_argument('-b', '--batch-size', help='size of each batch', type=int, default=100000)

    args = parser.parse_args()

    print "Writing {} {}-element batches to '{}'".format(args.num_batches, args.batch_size, args.filename)
    with open(args.filename, 'w') as fd:
        write_record_batches(fd, args.batch_size, args.num_batches)

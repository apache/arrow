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

from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import random
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


class ParquetManifestCreation(object):
    """Benchmark creating a parquet manifest."""

    size = 10 ** 6
    tmpdir = None

    param_names = ('num_partitions', 'num_threads')
    params = [(10, 100, 1000), (1, 8, 'default')]

    def setup(self, num_partitions, num_threads):
        self.tmpdir = tempfile.mkdtemp('benchmark_parquet')
        num1 = [random.choice(range(0, num_partitions))
                for _ in range(self.size)]
        num2 = [random.choice(range(0, 1000)) for _ in range(self.size)]
        output_df = pd.DataFrame({'num1': num1, 'num2': num2})
        output_table = pa.Table.from_pandas(output_df)
        pq.write_to_dataset(output_table, self.tmpdir, ['num1'])

    def teardown(self, num_partitions, num_threads):
        shutil.rmtree(self.tmpdir)

    def time_manifest_creation(self, num_partitions, num_threads):
        if num_threads != 'default':
            thread_pool = ThreadPoolExecutor(num_threads)
        else:
            thread_pool = None
        pq.ParquetManifest(self.tmpdir, thread_pool=thread_pool)

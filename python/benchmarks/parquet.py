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

import shutil
import tempfile

from pandas.util.testing import rands
import numpy as np
import pandas as pd

import pyarrow as pa
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


class ParquetManifestCreation(object):
    """Benchmark creating a parquet manifest."""

    size = 10 ** 6
    tmpdir = None

    param_names = ('num_partitions', 'num_threads')
    params = [(10, 100, 1000), (1, 8)]

    def setup(self, num_partitions, num_threads):
        if pq is None:
            raise NotImplementedError("Parquet support not enabled")

        self.tmpdir = tempfile.mkdtemp('benchmark_parquet')
        rnd = np.random.RandomState(42)
        num1 = rnd.randint(0, num_partitions, size=self.size)
        num2 = rnd.randint(0, 1000, size=self.size)
        output_df = pd.DataFrame({'num1': num1, 'num2': num2})
        output_table = pa.Table.from_pandas(output_df)
        pq.write_to_dataset(output_table, self.tmpdir, ['num1'])

    def teardown(self, num_partitions, num_threads):
        if self.tmpdir is not None:
            shutil.rmtree(self.tmpdir)

    def time_manifest_creation(self, num_partitions, num_threads):
        pq.ParquetManifest(self.tmpdir, metadata_nthreads=num_threads)


class ParquetWriteBinary(object):

    def setup(self):
        nuniques = 100000
        value_size = 50
        length = 1000000
        num_cols = 10

        unique_values = np.array([rands(value_size) for
                                  i in range(nuniques)], dtype='O')
        values = unique_values[np.random.randint(0, nuniques, size=length)]
        self.table = pa.table([pa.array(values) for i in range(num_cols)],
                              names=['f{}'.format(i) for i in range(num_cols)])
        self.table_df = self.table.to_pandas()

    def time_write_binary_table(self):
        out = pa.BufferOutputStream()
        pq.write_table(self.table, out)

    def time_write_binary_table_uncompressed(self):
        out = pa.BufferOutputStream()
        pq.write_table(self.table, out, compression='none')

    def time_write_binary_table_no_dictionary(self):
        out = pa.BufferOutputStream()
        pq.write_table(self.table, out, use_dictionary=False)

    def time_convert_pandas_and_write_binary_table(self):
        out = pa.BufferOutputStream()
        pq.write_table(pa.table(self.table_df), out)


def generate_dict_strings(string_size, nunique, length, random_order=True):
    uniques = np.array([rands(string_size) for i in range(nunique)], dtype='O')
    if random_order:
        indices = np.random.randint(0, nunique, size=length).astype('i4')
    else:
        indices = np.arange(nunique).astype('i4').repeat(length // nunique)
    return pa.DictionaryArray.from_arrays(indices, uniques)


def generate_dict_table(num_cols, string_size, nunique, length,
                        random_order=True):
    data = generate_dict_strings(string_size, nunique, length,
                                 random_order=random_order)
    return pa.table([
        data for i in range(num_cols)
    ], names=['f{}'.format(i) for i in range(num_cols)])


class ParquetWriteDictionaries(object):

    param_names = ('nunique',)
    params = [(1000), (100000)]

    def setup(self, nunique):
        self.num_cols = 10
        self.value_size = 32
        self.nunique = nunique
        self.length = 10000000

        self.table = generate_dict_table(self.num_cols, self.value_size,
                                         self.nunique, self.length)
        self.table_sequential = generate_dict_table(self.num_cols,
                                                    self.value_size,
                                                    self.nunique, self.length,
                                                    random_order=False)

    def time_write_random_order(self, nunique):
        pq.write_table(self.table, pa.BufferOutputStream())

    def time_write_sequential(self, nunique):
        pq.write_table(self.table_sequential, pa.BufferOutputStream())


class ParquetManyColumns(object):

    total_cells = 10000000
    param_names = ('num_cols',)
    params = [100, 1000, 10000]

    def setup(self, num_cols):
        num_rows = self.total_cells // num_cols
        self.table = pa.table({'c' + str(i): np.random.randn(num_rows)
                               for i in range(num_cols)})

        out = pa.BufferOutputStream()
        pq.write_table(self.table, out)
        self.buf = out.getvalue()

    def time_write(self, num_cols):
        out = pa.BufferOutputStream()
        pq.write_table(self.table, out)

    def time_read(self, num_cols):
        pq.read_table(self.buf)

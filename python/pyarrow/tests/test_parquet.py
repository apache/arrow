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

import pyarrow as A
import pyarrow.io as paio

import numpy as np
import pandas as pd

import pandas.util.testing as pdt

try:
    import pyarrow.parquet as pq
    HAVE_PARQUET = True
except ImportError:
    HAVE_PARQUET = False

# XXX: Make Parquet tests opt-in rather than skip-if-not-build
parquet = pytest.mark.skipif(not HAVE_PARQUET,
                             reason='Parquet support not built')


@parquet
def test_single_pylist_column_roundtrip(tmpdir):
    for dtype in [int, float]:
        filename = tmpdir.join('single_{}_column.parquet'
                               .format(dtype.__name__))
        data = [A.from_pylist(list(map(dtype, range(5))))]
        table = A.Table.from_arrays(('a', 'b'), data, 'table_name')
        A.parquet.write_table(table, filename.strpath)
        table_read = pq.read_table(filename.strpath)
        for col_written, col_read in zip(table.itercolumns(),
                                         table_read.itercolumns()):
            assert col_written.name == col_read.name
            assert col_read.data.num_chunks == 1
            data_written = col_written.data.chunk(0)
            data_read = col_read.data.chunk(0)
            assert data_written.equals(data_read)


@parquet
def test_pandas_parquet_2_0_rountrip(tmpdir):
    size = 10000
    np.random.seed(0)
    df = pd.DataFrame({
        'uint8': np.arange(size, dtype=np.uint8),
        'uint16': np.arange(size, dtype=np.uint16),
        'uint32': np.arange(size, dtype=np.uint32),
        'uint64': np.arange(size, dtype=np.uint64),
        'int8': np.arange(size, dtype=np.int16),
        'int16': np.arange(size, dtype=np.int16),
        'int32': np.arange(size, dtype=np.int32),
        'int64': np.arange(size, dtype=np.int64),
        'float32': np.arange(size, dtype=np.float32),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0,
        # Pandas only support ns resolution, Arrow at the moment only ms
        'datetime': np.arange("2016-01-01T00:00:00.001", size,
                              dtype='datetime64[ms]'),
        'str': [str(x) for x in range(size)],
        'str_with_nulls': [None] + [str(x) for x in range(size - 2)] + [None],
        'empty_str': [''] * size
    })
    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = A.Table.from_pandas(df, timestamps_to_ms=True)
    A.parquet.write_table(arrow_table, filename.strpath, version="2.0")
    table_read = pq.read_table(filename.strpath)
    df_read = table_read.to_pandas()
    pdt.assert_frame_equal(df, df_read)


@parquet
def test_pandas_parquet_1_0_rountrip(tmpdir):
    size = 10000
    np.random.seed(0)
    df = pd.DataFrame({
        'uint8': np.arange(size, dtype=np.uint8),
        'uint16': np.arange(size, dtype=np.uint16),
        'uint32': np.arange(size, dtype=np.uint32),
        'uint64': np.arange(size, dtype=np.uint64),
        'int8': np.arange(size, dtype=np.int16),
        'int16': np.arange(size, dtype=np.int16),
        'int32': np.arange(size, dtype=np.int32),
        'int64': np.arange(size, dtype=np.int64),
        'float32': np.arange(size, dtype=np.float32),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0,
        'str': [str(x) for x in range(size)],
        'str_with_nulls': [None] + [str(x) for x in range(size - 2)] + [None],
        'empty_str': [''] * size
    })
    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = A.Table.from_pandas(df)
    A.parquet.write_table(arrow_table, filename.strpath, version="1.0")
    table_read = pq.read_table(filename.strpath)
    df_read = table_read.to_pandas()

    # We pass uint32_t as int64_t if we write Parquet version 1.0
    df['uint32'] = df['uint32'].values.astype(np.int64)

    pdt.assert_frame_equal(df, df_read)

@parquet
def test_pandas_column_selection(tmpdir):
    size = 10000
    np.random.seed(0)
    df = pd.DataFrame({
        'uint8': np.arange(size, dtype=np.uint8),
        'uint16': np.arange(size, dtype=np.uint16)
    })
    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = A.Table.from_pandas(df)
    A.parquet.write_table(arrow_table, filename.strpath)
    table_read = pq.read_table(filename.strpath, columns=['uint8'])
    df_read = table_read.to_pandas()

    pdt.assert_frame_equal(df[['uint8']], df_read)


def _test_dataframe(size=10000):
    np.random.seed(0)
    df = pd.DataFrame({
        'uint8': np.arange(size, dtype=np.uint8),
        'uint16': np.arange(size, dtype=np.uint16),
        'uint32': np.arange(size, dtype=np.uint32),
        'uint64': np.arange(size, dtype=np.uint64),
        'int8': np.arange(size, dtype=np.int16),
        'int16': np.arange(size, dtype=np.int16),
        'int32': np.arange(size, dtype=np.int32),
        'int64': np.arange(size, dtype=np.int64),
        'float32': np.arange(size, dtype=np.float32),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0
    })
    return df


@parquet
def test_pandas_parquet_native_file_roundtrip(tmpdir):
    df = _test_dataframe(10000)
    arrow_table = A.Table.from_pandas(df)
    imos = paio.InMemoryOutputStream()
    pq.write_table(arrow_table, imos, version="2.0")
    buf = imos.get_result()
    reader = paio.BufferReader(buf)
    df_read = pq.read_table(reader).to_pandas()
    pdt.assert_frame_equal(df, df_read)


@parquet
def test_pandas_parquet_pyfile_roundtrip(tmpdir):
    filename = tmpdir.join('pandas_pyfile_roundtrip.parquet').strpath
    size = 5
    df = pd.DataFrame({
        'int64': np.arange(size, dtype=np.int64),
        'float32': np.arange(size, dtype=np.float32),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0,
        'strings': ['foo', 'bar', None, 'baz', 'qux']
    })

    arrow_table = A.Table.from_pandas(df)

    with open(filename, 'wb') as f:
        A.parquet.write_table(arrow_table, f, version="1.0")

    data = io.BytesIO(open(filename, 'rb').read())

    table_read = pq.read_table(data)
    df_read = table_read.to_pandas()
    pdt.assert_frame_equal(df, df_read)


@parquet
def test_pandas_parquet_configuration_options(tmpdir):
    size = 10000
    np.random.seed(0)
    df = pd.DataFrame({
        'uint8': np.arange(size, dtype=np.uint8),
        'uint16': np.arange(size, dtype=np.uint16),
        'uint32': np.arange(size, dtype=np.uint32),
        'uint64': np.arange(size, dtype=np.uint64),
        'int8': np.arange(size, dtype=np.int16),
        'int16': np.arange(size, dtype=np.int16),
        'int32': np.arange(size, dtype=np.int32),
        'int64': np.arange(size, dtype=np.int64),
        'float32': np.arange(size, dtype=np.float32),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0
    })
    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = A.Table.from_pandas(df)

    for use_dictionary in [True, False]:
        A.parquet.write_table(
                arrow_table,
                filename.strpath,
                version="2.0",
                use_dictionary=use_dictionary)
        table_read = pq.read_table(filename.strpath)
        df_read = table_read.to_pandas()
        pdt.assert_frame_equal(df, df_read)

    for compression in ['NONE', 'SNAPPY', 'GZIP']:
        A.parquet.write_table(
                arrow_table,
                filename.strpath,
                version="2.0",
                compression=compression)
        table_read = pq.read_table(filename.strpath)
        df_read = table_read.to_pandas()
        pdt.assert_frame_equal(df, df_read)

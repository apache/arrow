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

from os.path import join as pjoin

import datetime
import decimal
import io
import json
import os
import sys

import pytest

from pyarrow.compat import guid, u, BytesIO, unichar
from pyarrow.tests import util
from pyarrow.filesystem import LocalFileSystem
import pyarrow as pa
from .pandas_examples import dataframe_with_arrays, dataframe_with_lists

import numpy as np
import pandas as pd

import pandas.util.testing as tm

# Ignore these with pytest ... -m 'not parquet'
parquet = pytest.mark.parquet


def _write_table(table, path, **kwargs):
    import pyarrow.parquet as pq

    if isinstance(table, pd.DataFrame):
        table = pa.Table.from_pandas(table)

    pq.write_table(table, path, **kwargs)
    return table


def _read_table(*args, **kwargs):
    import pyarrow.parquet as pq
    return pq.read_table(*args, **kwargs)


def _roundtrip_table(table, **params):
    buf = io.BytesIO()
    _write_table(table, buf, **params)
    buf.seek(0)

    return _read_table(buf)


def _check_roundtrip(table, expected=None, **params):
    if expected is None:
        expected = table

    result = _roundtrip_table(table, **params)
    if not result.equals(expected):
        print(expected)
        print(result)
        assert result.equals(expected)

    result = _roundtrip_table(result, **params)
    assert result.equals(expected)


def _roundtrip_pandas_dataframe(df, write_kwargs):
    table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(table, buf, **write_kwargs)

    buf.seek(0)
    table1 = _read_table(buf)
    return table1.to_pandas()


@parquet
def test_single_pylist_column_roundtrip(tmpdir):
    for dtype in [int, float]:
        filename = tmpdir.join('single_{}_column.parquet'
                               .format(dtype.__name__))
        data = [pa.array(list(map(dtype, range(5))))]
        table = pa.Table.from_arrays(data, names=['a'])
        _write_table(table, filename.strpath)
        table_read = _read_table(filename.strpath)
        for col_written, col_read in zip(table.itercolumns(),
                                         table_read.itercolumns()):
            assert col_written.name == col_read.name
            assert col_read.data.num_chunks == 1
            data_written = col_written.data.chunk(0)
            data_read = col_read.data.chunk(0)
            assert data_written.equals(data_read)


def alltypes_sample(size=10000, seed=0, categorical=False):
    np.random.seed(seed)
    arrays = {
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
        # TODO(wesm): Test other timestamp resolutions now that arrow supports
        # them
        'datetime': np.arange("2016-01-01T00:00:00.001", size,
                              dtype='datetime64[ms]'),
        'str': pd.Series([str(x) for x in range(size)]),
        'empty_str': [''] * size,
        'str_with_nulls': [None] + [str(x) for x in range(size - 2)] + [None],
        'null': [None] * size,
        'null_list': [None] * 2 + [[None] * (x % 4) for x in range(size - 2)],
    }
    if categorical:
        arrays['str_category'] = arrays['str'].astype('category')
    return pd.DataFrame(arrays)


@parquet
@pytest.mark.parametrize('chunk_size', [None, 1000])
def test_pandas_parquet_2_0_rountrip(tmpdir, chunk_size):
    import pyarrow.parquet as pq
    df = alltypes_sample(size=10000, categorical=True)

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df)
    assert b'pandas' in arrow_table.schema.metadata

    _write_table(arrow_table, filename.strpath, version="2.0",
                 coerce_timestamps='ms', chunk_size=chunk_size)
    table_read = pq.read_pandas(filename.strpath)
    assert b'pandas' in table_read.schema.metadata

    assert arrow_table.schema.metadata == table_read.schema.metadata

    df_read = table_read.to_pandas(categories=['str_category'])
    tm.assert_frame_equal(df, df_read, check_categorical=False)


@parquet
def test_chunked_table_write(tmpdir):
    # ARROW-232
    df = alltypes_sample(size=10)

    # The nanosecond->ms conversion is a nuisance, so we just avoid it here
    del df['datetime']

    batch = pa.RecordBatch.from_pandas(df)
    table = pa.Table.from_batches([batch] * 3)
    _check_roundtrip(table, version='2.0')

    df, _ = dataframe_with_lists()
    batch = pa.RecordBatch.from_pandas(df)
    table = pa.Table.from_batches([batch] * 3)
    _check_roundtrip(table, version='2.0')


@parquet
def test_empty_table_roundtrip(tmpdir):
    df = alltypes_sample(size=10)
    # The nanosecond->us conversion is a nuisance, so we just avoid it here
    del df['datetime']

    # Create a non-empty table to infer the types correctly, then slice to 0
    table = pa.Table.from_pandas(df)
    table = pa.Table.from_arrays(
        [col.data.chunk(0)[:0] for col in table.itercolumns()],
        names=table.schema.names)

    assert table.schema.field_by_name('null').type == pa.null()
    assert table.schema.field_by_name('null_list').type == pa.list_(pa.null())
    _check_roundtrip(table, version='2.0')


@parquet
def test_pandas_parquet_datetime_tz():
    import pyarrow.parquet as pq

    s = pd.Series([datetime.datetime(2017, 9, 6)])
    s = s.dt.tz_localize('utc')

    s.index = s

    # Both a column and an index to hit both use cases
    df = pd.DataFrame({'tz_aware': s,
                       'tz_eastern': s.dt.tz_convert('US/Eastern')},
                      index=s)

    f = BytesIO()

    arrow_table = pa.Table.from_pandas(df)

    _write_table(arrow_table, f, coerce_timestamps='ms')
    f.seek(0)

    table_read = pq.read_pandas(f)

    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@parquet
def test_pandas_parquet_custom_metadata(tmpdir):
    import pyarrow.parquet as pq

    df = alltypes_sample(size=10000)

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df)
    assert b'pandas' in arrow_table.schema.metadata

    _write_table(arrow_table, filename.strpath, version="2.0",
                 coerce_timestamps='ms')

    md = pq.read_metadata(filename.strpath).metadata
    assert b'pandas' in md

    js = json.loads(md[b'pandas'].decode('utf8'))
    assert js['index_columns'] == ['__index_level_0__']


@parquet
def test_pandas_parquet_column_multiindex(tmpdir):
    import pyarrow.parquet as pq

    df = alltypes_sample(size=10)
    df.columns = pd.MultiIndex.from_tuples(
        list(zip(df.columns, df.columns[::-1])),
        names=['level_1', 'level_2']
    )

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df)
    assert b'pandas' in arrow_table.schema.metadata

    _write_table(arrow_table, filename.strpath, version="2.0",
                 coerce_timestamps='ms')

    table_read = pq.read_pandas(filename.strpath)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@parquet
def test_pandas_parquet_2_0_rountrip_read_pandas_no_index_written(tmpdir):
    import pyarrow.parquet as pq

    df = alltypes_sample(size=10000)

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    js = json.loads(arrow_table.schema.metadata[b'pandas'].decode('utf8'))
    assert not js['index_columns']
    # ARROW-2170
    # While index_columns should be empty, columns needs to be filled still.
    assert js['columns']

    _write_table(arrow_table, filename.strpath, version="2.0",
                 coerce_timestamps='ms')
    table_read = pq.read_pandas(filename.strpath)

    js = json.loads(table_read.schema.metadata[b'pandas'].decode('utf8'))
    assert not js['index_columns']

    assert arrow_table.schema.metadata == table_read.schema.metadata

    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


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
    arrow_table = pa.Table.from_pandas(df)
    _write_table(arrow_table, filename.strpath, version="1.0")
    table_read = _read_table(filename.strpath)
    df_read = table_read.to_pandas()

    # We pass uint32_t as int64_t if we write Parquet version 1.0
    df['uint32'] = df['uint32'].values.astype(np.int64)

    tm.assert_frame_equal(df, df_read)


@parquet
@pytest.mark.skipif(sys.version_info < (3, 6), reason="need Python 3.6")
def test_path_objects(tmpdir):
    # Test compatibility with PEP 519 path-like objects
    import pathlib
    p = pathlib.Path(tmpdir) / 'zzz.parquet'
    df = pd.DataFrame({'x': np.arange(10, dtype=np.int64)})
    _write_table(df, p)
    table_read = _read_table(p)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@parquet
def test_pandas_column_selection(tmpdir):
    size = 10000
    np.random.seed(0)
    df = pd.DataFrame({
        'uint8': np.arange(size, dtype=np.uint8),
        'uint16': np.arange(size, dtype=np.uint16)
    })
    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df)
    _write_table(arrow_table, filename.strpath)
    table_read = _read_table(filename.strpath, columns=['uint8'])
    df_read = table_read.to_pandas()

    tm.assert_frame_equal(df[['uint8']], df_read)


def _random_integers(size, dtype):
    # We do not generate integers outside the int64 range
    platform_int_info = np.iinfo('int_')
    iinfo = np.iinfo(dtype)
    return np.random.randint(max(iinfo.min, platform_int_info.min),
                             min(iinfo.max, platform_int_info.max),
                             size=size).astype(dtype)


def _test_dataframe(size=10000, seed=0):
    np.random.seed(seed)
    df = pd.DataFrame({
        'uint8': _random_integers(size, np.uint8),
        'uint16': _random_integers(size, np.uint16),
        'uint32': _random_integers(size, np.uint32),
        'uint64': _random_integers(size, np.uint64),
        'int8': _random_integers(size, np.int8),
        'int16': _random_integers(size, np.int16),
        'int32': _random_integers(size, np.int32),
        'int64': _random_integers(size, np.int64),
        'float32': np.random.randn(size).astype(np.float32),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0,
        'strings': [tm.rands(10) for i in range(size)],
        'all_none': [None] * size,
        'all_none_category': [None] * size
    })
    # TODO(PARQUET-1015)
    # df['all_none_category'] = df['all_none_category'].astype('category')
    return df


@parquet
def test_pandas_parquet_native_file_roundtrip(tmpdir):
    df = _test_dataframe(10000)
    arrow_table = pa.Table.from_pandas(df)
    imos = pa.BufferOutputStream()
    _write_table(arrow_table, imos, version="2.0")
    buf = imos.get_result()
    reader = pa.BufferReader(buf)
    df_read = _read_table(reader).to_pandas()
    tm.assert_frame_equal(df, df_read)


@parquet
def test_parquet_incremental_file_build(tmpdir):
    import pyarrow.parquet as pq

    df = _test_dataframe(100)
    df['unique_id'] = 0

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    out = pa.BufferOutputStream()

    writer = pq.ParquetWriter(out, arrow_table.schema, version='2.0')

    frames = []
    for i in range(10):
        df['unique_id'] = i
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        writer.write_table(arrow_table)

        frames.append(df.copy())

    writer.close()

    buf = out.get_result()
    result = _read_table(pa.BufferReader(buf))

    expected = pd.concat(frames, ignore_index=True)
    tm.assert_frame_equal(result.to_pandas(), expected)


@parquet
def test_read_pandas_column_subset(tmpdir):
    import pyarrow.parquet as pq

    df = _test_dataframe(10000)
    arrow_table = pa.Table.from_pandas(df)
    imos = pa.BufferOutputStream()
    _write_table(arrow_table, imos, version="2.0")
    buf = imos.get_result()
    reader = pa.BufferReader(buf)
    df_read = pq.read_pandas(reader, columns=['strings', 'uint8']).to_pandas()
    tm.assert_frame_equal(df[['strings', 'uint8']], df_read)


@parquet
def test_pandas_parquet_empty_roundtrip(tmpdir):
    df = _test_dataframe(0)
    arrow_table = pa.Table.from_pandas(df)
    imos = pa.BufferOutputStream()
    _write_table(arrow_table, imos, version="2.0")
    buf = imos.get_result()
    reader = pa.BufferReader(buf)
    df_read = _read_table(reader).to_pandas()
    tm.assert_frame_equal(df, df_read)


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

    arrow_table = pa.Table.from_pandas(df)

    with open(filename, 'wb') as f:
        _write_table(arrow_table, f, version="1.0")

    data = io.BytesIO(open(filename, 'rb').read())

    table_read = _read_table(data)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


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
    arrow_table = pa.Table.from_pandas(df)

    for use_dictionary in [True, False]:
        _write_table(arrow_table, filename.strpath,
                     version="2.0",
                     use_dictionary=use_dictionary)
        table_read = _read_table(filename.strpath)
        df_read = table_read.to_pandas()
        tm.assert_frame_equal(df, df_read)

    for compression in ['NONE', 'SNAPPY', 'GZIP', 'LZ4']:
        _write_table(arrow_table, filename.strpath,
                     version="2.0",
                     compression=compression)
        table_read = _read_table(filename.strpath)
        df_read = table_read.to_pandas()
        tm.assert_frame_equal(df, df_read)


def make_sample_file(df):
    import pyarrow.parquet as pq

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, compression='SNAPPY', version='2.0',
                 coerce_timestamps='ms')

    buf.seek(0)
    return pq.ParquetFile(buf)


@parquet
def test_parquet_metadata_api():
    df = alltypes_sample(size=10000)
    df = df.reindex(columns=sorted(df.columns))

    fileh = make_sample_file(df)
    ncols = len(df.columns)

    # Series of sniff tests
    meta = fileh.metadata
    repr(meta)
    assert meta.num_rows == len(df)
    assert meta.num_columns == ncols + 1  # +1 for index
    assert meta.num_row_groups == 1
    assert meta.format_version == '2.0'
    assert 'parquet-cpp' in meta.created_by

    # Schema
    schema = fileh.schema
    assert meta.schema is schema
    assert len(schema) == ncols + 1  # +1 for index
    repr(schema)

    col = schema[0]
    repr(col)
    assert col.name == df.columns[0]
    assert col.max_definition_level == 1
    assert col.max_repetition_level == 0
    assert col.max_repetition_level == 0

    assert col.physical_type == 'BOOLEAN'
    assert col.logical_type == 'NONE'

    with pytest.raises(IndexError):
        schema[ncols + 1]  # +1 for index

    with pytest.raises(IndexError):
        schema[-1]

    # Row group
    for rg in range(meta.num_row_groups):
        rg_meta = meta.row_group(rg)
        repr(rg_meta)

        for col in range(rg_meta.num_columns):
            col_meta = rg_meta.column(col)
            repr(col_meta)

    assert rg_meta.num_rows == len(df)
    assert rg_meta.num_columns == ncols + 1  # +1 for index


@parquet
@pytest.mark.parametrize(
    'data, dtype, min_value, max_value, null_count, num_values',
    [
        ([1, 2, 2, None, 4], np.uint8, 1, 4, 1, 4),
        ([1, 2, 2, None, 4], np.uint16, 1, 4, 1, 4),
        ([1, 2, 2, None, 4], np.uint32, 1, 4, 1, 4),
        ([1, 2, 2, None, 4], np.uint64, 1, 4, 1, 4),
        ([-1, 2, 2, None, 4], np.int16, -1, 4, 1, 4),
        ([-1, 2, 2, None, 4], np.int32, -1, 4, 1, 4),
        ([-1, 2, 2, None, 4], np.int64, -1, 4, 1, 4),
        ([-1.1, 2.2, 2.3, None, 4.4], np.float32, -1.1, 4.4, 1, 4),
        ([-1.1, 2.2, 2.3, None, 4.4], np.float64, -1.1, 4.4, 1, 4),
        (
            [u'', u'b', unichar(1000), None, u'aaa'],
            object, b'', unichar(1000).encode('utf-8'), 1, 4
        ),
        ([True, False, False, True, True], np.bool, False, True, 0, 5),
    ]
)
def test_parquet_column_statistics_api(
        data,
        dtype,
        min_value,
        max_value,
        null_count,
        num_values):
    df = pd.DataFrame({'data': data}, dtype=dtype)

    fileh = make_sample_file(df)

    meta = fileh.metadata

    rg_meta = meta.row_group(0)
    col_meta = rg_meta.column(0)

    stat = col_meta.statistics
    assert stat.min == min_value
    assert stat.max == max_value
    assert stat.null_count == null_count
    assert stat.num_values == num_values


@parquet
def test_compare_schemas():
    df = alltypes_sample(size=10000)

    fileh = make_sample_file(df)
    fileh2 = make_sample_file(df)
    fileh3 = make_sample_file(df[df.columns[::2]])

    assert fileh.schema.equals(fileh.schema)
    assert fileh.schema.equals(fileh2.schema)

    assert not fileh.schema.equals(fileh3.schema)

    assert fileh.schema[0].equals(fileh.schema[0])
    assert not fileh.schema[0].equals(fileh.schema[1])


@parquet
def test_column_of_arrays(tmpdir):
    df, schema = dataframe_with_arrays()

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    _write_table(arrow_table, filename.strpath, version="2.0",
                 coerce_timestamps='ms')
    table_read = _read_table(filename.strpath)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@parquet
def test_coerce_timestamps(tmpdir):
    from collections import OrderedDict
    # ARROW-622
    arrays = OrderedDict()
    fields = [pa.field('datetime64',
                       pa.list_(pa.timestamp('ms')))]
    arrays['datetime64'] = [
        np.array(['2007-07-13T01:23:34.123456789',
                  None,
                  '2010-08-13T05:46:57.437699912'],
                 dtype='datetime64[ms]'),
        None,
        None,
        np.array(['2007-07-13T02',
                  None,
                  '2010-08-13T05:46:57.437699912'],
                 dtype='datetime64[ms]'),
    ]

    df = pd.DataFrame(arrays)
    schema = pa.schema(fields)

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df, schema=schema)

    _write_table(arrow_table, filename.strpath, version="2.0",
                 coerce_timestamps='us')
    table_read = _read_table(filename.strpath)
    df_read = table_read.to_pandas()

    df_expected = df.copy()
    for i, x in enumerate(df_expected['datetime64']):
        if isinstance(x, np.ndarray):
            df_expected['datetime64'][i] = x.astype('M8[us]')

    tm.assert_frame_equal(df_expected, df_read)

    with pytest.raises(ValueError):
        _write_table(arrow_table, filename.strpath, version="2.0",
                     coerce_timestamps='unknown')


@parquet
def test_column_of_lists(tmpdir):
    df, schema = dataframe_with_lists()

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    _write_table(arrow_table, filename.strpath, version="2.0",
                 coerce_timestamps='ms')
    table_read = _read_table(filename.strpath)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@parquet
def test_date_time_types():
    t1 = pa.date32()
    data1 = np.array([17259, 17260, 17261], dtype='int32')
    a1 = pa.array(data1, type=t1)

    t2 = pa.date64()
    data2 = data1.astype('int64') * 86400000
    a2 = pa.array(data2, type=t2)

    t3 = pa.timestamp('us')
    start = pd.Timestamp('2000-01-01').value / 1000
    data3 = np.array([start, start + 1, start + 2], dtype='int64')
    a3 = pa.array(data3, type=t3)

    t4 = pa.time32('ms')
    data4 = np.arange(3, dtype='i4')
    a4 = pa.array(data4, type=t4)

    t5 = pa.time64('us')
    a5 = pa.array(data4.astype('int64'), type=t5)

    t6 = pa.time32('s')
    a6 = pa.array(data4, type=t6)

    ex_t6 = pa.time32('ms')
    ex_a6 = pa.array(data4 * 1000, type=ex_t6)

    t7 = pa.timestamp('ns')
    start = pd.Timestamp('2001-01-01').value
    data7 = np.array([start, start + 1000, start + 2000],
                     dtype='int64')
    a7 = pa.array(data7, type=t7)

    t7_us = pa.timestamp('us')
    start = pd.Timestamp('2001-01-01').value
    data7_us = np.array([start, start + 1000, start + 2000],
                        dtype='int64') // 1000
    a7_us = pa.array(data7_us, type=t7_us)

    table = pa.Table.from_arrays([a1, a2, a3, a4, a5, a6, a7],
                                 ['date32', 'date64', 'timestamp[us]',
                                  'time32[s]', 'time64[us]',
                                  'time32_from64[s]',
                                  'timestamp[ns]'])

    # date64 as date32
    # time32[s] to time32[ms]
    # 'timestamp[ns]' to 'timestamp[us]'
    expected = pa.Table.from_arrays([a1, a1, a3, a4, a5, ex_a6, a7_us],
                                    ['date32', 'date64', 'timestamp[us]',
                                     'time32[s]', 'time64[us]',
                                     'time32_from64[s]',
                                     'timestamp[ns]'])

    _check_roundtrip(table, expected=expected, version='2.0')

    # date64 as date32
    # time32[s] to time32[ms]
    # 'timestamp[ns]' is saved as INT96 timestamp
    expected = pa.Table.from_arrays([a1, a1, a3, a4, a5, ex_a6, a7],
                                    ['date32', 'date64', 'timestamp[us]',
                                     'time32[s]', 'time64[us]',
                                     'time32_from64[s]',
                                     'timestamp[ns]'])

    _check_roundtrip(table, expected=expected, version='2.0',
                     use_deprecated_int96_timestamps=True)

    # Check that setting flavor to 'spark' uses int96 timestamps
    _check_roundtrip(table, expected=expected, version='2.0',
                     flavor='spark')

    # Unsupported stuff
    def _assert_unsupported(array):
        table = pa.Table.from_arrays([array], ['unsupported'])
        buf = io.BytesIO()

        with pytest.raises(NotImplementedError):
            _write_table(table, buf, version="2.0")

    t7 = pa.time64('ns')
    a7 = pa.array(data4.astype('int64'), type=t7)

    _assert_unsupported(a7)


@parquet
def test_large_list_records():
    # This was fixed in PARQUET-1100

    list_lengths = np.random.randint(0, 500, size=50)
    list_lengths[::10] = 0

    list_values = [list(map(int, np.random.randint(0, 100, size=x)))
                   if i % 8 else None
                   for i, x in enumerate(list_lengths)]

    a1 = pa.array(list_values)

    table = pa.Table.from_arrays([a1], ['int_lists'])
    _check_roundtrip(table)


@parquet
def test_sanitized_spark_field_names():
    a0 = pa.array([0, 1, 2, 3, 4])
    name = 'prohib; ,\t{}'
    table = pa.Table.from_arrays([a0], [name])

    result = _roundtrip_table(table, flavor='spark')

    expected_name = 'prohib______'
    assert result.schema[0].name == expected_name


@parquet
def test_spark_flavor_preserves_pandas_metadata():
    df = _test_dataframe(size=100)
    df.index = np.arange(0, 10 * len(df), 10)
    df.index.name = 'foo'

    result = _roundtrip_pandas_dataframe(df, {'version': '2.0',
                                              'flavor': 'spark'})
    tm.assert_frame_equal(result, df)


@parquet
def test_fixed_size_binary():
    t0 = pa.binary(10)
    data = [b'fooooooooo', None, b'barooooooo', b'quxooooooo']
    a0 = pa.array(data, type=t0)

    table = pa.Table.from_arrays([a0],
                                 ['binary[10]'])
    _check_roundtrip(table)


@parquet
def test_multithreaded_read():
    df = alltypes_sample(size=10000)

    table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(table, buf, compression='SNAPPY', version='2.0')

    buf.seek(0)
    table1 = _read_table(buf, nthreads=4)

    buf.seek(0)
    table2 = _read_table(buf, nthreads=1)

    assert table1.equals(table2)


@parquet
def test_min_chunksize():
    data = pd.DataFrame([np.arange(4)], columns=['A', 'B', 'C', 'D'])
    table = pa.Table.from_pandas(data.reset_index())

    buf = io.BytesIO()
    _write_table(table, buf, chunk_size=-1)

    buf.seek(0)
    result = _read_table(buf)

    assert result.equals(table)

    with pytest.raises(ValueError):
        _write_table(table, buf, chunk_size=0)


@parquet
def test_pass_separate_metadata():
    import pyarrow.parquet as pq

    # ARROW-471
    df = alltypes_sample(size=10000)

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, compression='snappy', version='2.0')

    buf.seek(0)
    metadata = pq.read_metadata(buf)

    buf.seek(0)

    fileh = pq.ParquetFile(buf, metadata=metadata)

    tm.assert_frame_equal(df, fileh.read().to_pandas())


@parquet
def test_read_single_row_group():
    import pyarrow.parquet as pq

    # ARROW-471
    N, K = 10000, 4
    df = alltypes_sample(size=N)

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)

    pf = pq.ParquetFile(buf)

    assert pf.num_row_groups == K

    row_groups = [pf.read_row_group(i) for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df, result.to_pandas())


@parquet
def test_read_single_row_group_with_column_subset():
    import pyarrow.parquet as pq

    N, K = 10000, 4
    df = alltypes_sample(size=N)
    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)
    pf = pq.ParquetFile(buf)

    cols = df.columns[:2]
    row_groups = [pf.read_row_group(i, columns=cols) for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df[cols], result.to_pandas())


@parquet
def test_scan_contents():
    import pyarrow.parquet as pq

    N, K = 10000, 4
    df = alltypes_sample(size=N)
    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)
    pf = pq.ParquetFile(buf)

    assert pf.scan_contents() == 10000
    assert pf.scan_contents(df.columns[:4]) == 10000


@parquet
def test_parquet_piece_read(tmpdir):
    import pyarrow.parquet as pq

    df = _test_dataframe(1000)
    table = pa.Table.from_pandas(df)

    path = tmpdir.join('parquet_piece_read.parquet').strpath
    _write_table(table, path, version='2.0')

    piece1 = pq.ParquetDatasetPiece(path)

    result = piece1.read()
    assert result.equals(table)


@parquet
def test_parquet_piece_basics():
    import pyarrow.parquet as pq

    path = '/baz.parq'

    piece1 = pq.ParquetDatasetPiece(path)
    piece2 = pq.ParquetDatasetPiece(path, row_group=1)
    piece3 = pq.ParquetDatasetPiece(
        path, row_group=1, partition_keys=[('foo', 0), ('bar', 1)])

    assert str(piece1) == path
    assert str(piece2) == '/baz.parq | row_group=1'
    assert str(piece3) == 'partition[foo=0, bar=1] /baz.parq | row_group=1'

    assert piece1 == piece1
    assert piece2 == piece2
    assert piece3 == piece3
    assert piece1 != piece3


@parquet
def test_partition_set_dictionary_type():
    import pyarrow.parquet as pq

    set1 = pq.PartitionSet('key1', [u('foo'), u('bar'), u('baz')])
    set2 = pq.PartitionSet('key2', [2007, 2008, 2009])

    assert isinstance(set1.dictionary, pa.StringArray)
    assert isinstance(set2.dictionary, pa.IntegerArray)

    set3 = pq.PartitionSet('key2', [datetime.datetime(2007, 1, 1)])
    with pytest.raises(TypeError):
        set3.dictionary


@parquet
def test_read_partitioned_directory(tmpdir):
    fs = LocalFileSystem.get_instance()
    base_path = str(tmpdir)

    _partition_test_for_filesystem(fs, base_path)


@parquet
def test_equivalency(tmpdir):
    fs = LocalFileSystem.get_instance()
    base_path = str(tmpdir)

    import pyarrow.parquet as pq

    integer_keys = [0, 1]
    string_keys = ['a', 'b', 'c']
    boolean_keys = [True, False]
    partition_spec = [
        ['integer', integer_keys],
        ['string', string_keys],
        ['boolean', boolean_keys]
    ]

    df = pd.DataFrame({
        'integer': np.array(integer_keys, dtype='i4').repeat(15),
        'string': np.tile(np.tile(np.array(string_keys, dtype=object), 5), 2),
        'boolean': np.tile(np.tile(np.array(boolean_keys, dtype='bool'), 5),
                           3),
    }, columns=['integer', 'string', 'boolean'])

    _generate_partition_directories(fs, base_path, partition_spec, df)

    dataset = pq.ParquetDataset(
        base_path, filesystem=fs,
        filters=[('integer', '=', 1), ('string', '!=', 'b'),
                 ('boolean', '==', True)]
    )
    table = dataset.read()
    result_df = (table.to_pandas().reset_index(drop=True))

    assert 0 not in result_df['integer'].values
    assert 'b' not in result_df['string'].values
    assert False not in result_df['boolean'].values

    @parquet
    def test_cutoff_exclusive_integer(tmpdir):
        fs = LocalFileSystem.get_instance()
        base_path = str(tmpdir)

        import pyarrow.parquet as pq

        integer_keys = [0, 1, 2, 3, 4]
        partition_spec = [
            ['integers', integer_keys],
        ]
        N = 5

        df = pd.DataFrame({
            'index': np.arange(N),
            'integers': np.array(integer_keys, dtype='i4'),
        }, columns=['index', 'integers'])

        _generate_partition_directories(fs, base_path, partition_spec, df)

        dataset = pq.ParquetDataset(
            base_path, filesystem=fs,
            filters=[
                ('integers', '<', 4),
                ('integers', '>', 1),
            ]
        )
        table = dataset.read()
        result_df = (table.to_pandas()
                          .sort_values(by='index')
                          .reset_index(drop=True))

        result_list = [x for x in map(int, result_df['integers'].values)]
        assert result_list == [2, 3]


@parquet
@pytest.mark.xfail(
    raises=TypeError,
    reason='Loss of type information in creation of categoricals.'
)
def test_cutoff_exclusive_datetime(tmpdir):
    fs = LocalFileSystem.get_instance()
    base_path = str(tmpdir)

    import pyarrow.parquet as pq

    date_keys = [
        datetime.date(2018, 4, 9),
        datetime.date(2018, 4, 10),
        datetime.date(2018, 4, 11),
        datetime.date(2018, 4, 12),
        datetime.date(2018, 4, 13)
    ]
    partition_spec = [
        ['dates', date_keys]
    ]
    N = 5

    df = pd.DataFrame({
        'index': np.arange(N),
        'dates': np.array(date_keys, dtype='datetime64'),
    }, columns=['index', 'dates'])

    _generate_partition_directories(fs, base_path, partition_spec, df)

    dataset = pq.ParquetDataset(
        base_path, filesystem=fs,
        filters=[
            ('dates', '<', "2018-04-12"),
            ('dates', '>', "2018-04-10")
        ]
    )
    table = dataset.read()
    result_df = (table.to_pandas()
                      .sort_values(by='index')
                      .reset_index(drop=True))

    expected = pd.Categorical(
        np.array([datetime.date(2018, 4, 11)], dtype='datetime64'),
        categories=np.array(date_keys, dtype='datetime64'))

    assert result_df['dates'].values == expected


@parquet
def test_inclusive_integer(tmpdir):
    fs = LocalFileSystem.get_instance()
    base_path = str(tmpdir)

    import pyarrow.parquet as pq

    integer_keys = [0, 1, 2, 3, 4]
    partition_spec = [
        ['integers', integer_keys],
    ]
    N = 5

    df = pd.DataFrame({
        'index': np.arange(N),
        'integers': np.array(integer_keys, dtype='i4'),
    }, columns=['index', 'integers'])

    _generate_partition_directories(fs, base_path, partition_spec, df)

    dataset = pq.ParquetDataset(
        base_path, filesystem=fs,
        filters=[
            ('integers', '<=', 3),
            ('integers', '>=', 2),
        ]
    )
    table = dataset.read()
    result_df = (table.to_pandas()
                      .sort_values(by='index')
                      .reset_index(drop=True))

    result_list = [int(x) for x in map(int, result_df['integers'].values)]
    assert result_list == [2, 3]


@parquet
def test_invalid_pred_op(tmpdir):
    fs = LocalFileSystem.get_instance()
    base_path = str(tmpdir)

    import pyarrow.parquet as pq

    integer_keys = [0, 1, 2, 3, 4]
    partition_spec = [
        ['integers', integer_keys],
    ]
    N = 5

    df = pd.DataFrame({
        'index': np.arange(N),
        'integers': np.array(integer_keys, dtype='i4'),
    }, columns=['index', 'integers'])

    _generate_partition_directories(fs, base_path, partition_spec, df)

    with pytest.raises(ValueError):
        pq.ParquetDataset(base_path,
                          filesystem=fs,
                          filters=[
                            ('integers', '=<', 3),
                          ])


@pytest.yield_fixture
def s3_example():
    access_key = os.environ['PYARROW_TEST_S3_ACCESS_KEY']
    secret_key = os.environ['PYARROW_TEST_S3_SECRET_KEY']
    bucket_name = os.environ['PYARROW_TEST_S3_BUCKET']

    import s3fs
    fs = s3fs.S3FileSystem(key=access_key, secret=secret_key)

    test_dir = guid()

    bucket_uri = 's3://{0}/{1}'.format(bucket_name, test_dir)
    fs.mkdir(bucket_uri)
    yield fs, bucket_uri
    fs.rm(bucket_uri, recursive=True)


@pytest.mark.s3
@parquet
def test_read_partitioned_directory_s3fs(s3_example):
    from pyarrow.filesystem import S3FSWrapper
    import pyarrow.parquet as pq

    fs, bucket_uri = s3_example
    wrapper = S3FSWrapper(fs)
    _partition_test_for_filesystem(wrapper, bucket_uri)

    # Check that we can auto-wrap
    dataset = pq.ParquetDataset(bucket_uri, filesystem=fs)
    dataset.read()


def _partition_test_for_filesystem(fs, base_path):
    import pyarrow.parquet as pq

    foo_keys = [0, 1]
    bar_keys = ['a', 'b', 'c']
    partition_spec = [
        ['foo', foo_keys],
        ['bar', bar_keys]
    ]
    N = 30

    df = pd.DataFrame({
        'index': np.arange(N),
        'foo': np.array(foo_keys, dtype='i4').repeat(15),
        'bar': np.tile(np.tile(np.array(bar_keys, dtype=object), 5), 2),
        'values': np.random.randn(N)
    }, columns=['index', 'foo', 'bar', 'values'])

    _generate_partition_directories(fs, base_path, partition_spec, df)

    dataset = pq.ParquetDataset(base_path, filesystem=fs)
    table = dataset.read()
    result_df = (table.to_pandas()
                 .sort_values(by='index')
                 .reset_index(drop=True))

    expected_df = (df.sort_values(by='index')
                   .reset_index(drop=True)
                   .reindex(columns=result_df.columns))
    expected_df['foo'] = pd.Categorical(df['foo'], categories=foo_keys)
    expected_df['bar'] = pd.Categorical(df['bar'], categories=bar_keys)

    assert (result_df.columns == ['index', 'values', 'foo', 'bar']).all()

    tm.assert_frame_equal(result_df, expected_df)


def _generate_partition_directories(fs, base_dir, partition_spec, df):
    # partition_spec : list of lists, e.g. [['foo', [0, 1, 2],
    #                                       ['bar', ['a', 'b', 'c']]
    # part_table : a pyarrow.Table to write to each partition
    DEPTH = len(partition_spec)

    def _visit_level(base_dir, level, part_keys):
        name, values = partition_spec[level]
        for value in values:
            this_part_keys = part_keys + [(name, value)]

            level_dir = pjoin(base_dir, '{0}={1}'.format(name, value))
            fs.mkdir(level_dir)

            if level == DEPTH - 1:
                # Generate example data
                file_path = pjoin(level_dir, guid())

                filtered_df = _filter_partition(df, this_part_keys)
                part_table = pa.Table.from_pandas(filtered_df)
                with fs.open(file_path, 'wb') as f:
                    _write_table(part_table, f)
                assert fs.exists(file_path)

                _touch(pjoin(level_dir, '_SUCCESS'))
            else:
                _visit_level(level_dir, level + 1, this_part_keys)
                _touch(pjoin(level_dir, '_SUCCESS'))

    _visit_level(base_dir, 0, [])


def _test_read_common_metadata_files(fs, base_path):
    import pyarrow.parquet as pq

    N = 100
    df = pd.DataFrame({
        'index': np.arange(N),
        'values': np.random.randn(N)
    }, columns=['index', 'values'])

    data_path = pjoin(base_path, 'data.parquet')

    table = pa.Table.from_pandas(df)

    with fs.open(data_path, 'wb') as f:
        _write_table(table, f)

    metadata_path = pjoin(base_path, '_common_metadata')
    with fs.open(metadata_path, 'wb') as f:
        pq.write_metadata(table.schema, f)

    dataset = pq.ParquetDataset(base_path, filesystem=fs)
    assert dataset.common_metadata_path == metadata_path

    with fs.open(data_path) as f:
        common_schema = pq.read_metadata(f).schema
    assert dataset.schema.equals(common_schema)

    # handle list of one directory
    dataset2 = pq.ParquetDataset([base_path], filesystem=fs)
    assert dataset2.schema.equals(dataset.schema)


@parquet
def test_read_common_metadata_files(tmpdir):
    base_path = str(tmpdir)
    fs = LocalFileSystem.get_instance()
    _test_read_common_metadata_files(fs, base_path)


@parquet
def test_read_schema(tmpdir):
    import pyarrow.parquet as pq

    N = 100
    df = pd.DataFrame({
        'index': np.arange(N),
        'values': np.random.randn(N)
    }, columns=['index', 'values'])

    data_path = pjoin(str(tmpdir), 'test.parquet')

    table = pa.Table.from_pandas(df)
    _write_table(table, data_path)

    assert table.schema.equals(pq.read_schema(data_path))


def _filter_partition(df, part_keys):
    predicate = np.ones(len(df), dtype=bool)

    to_drop = []
    for name, value in part_keys:
        to_drop.append(name)
        predicate &= df[name] == value

    return df[predicate].drop(to_drop, axis=1)


def _touch(path):
    with open(path, 'wb'):
        pass


@parquet
def test_read_multiple_files(tmpdir):
    import pyarrow.parquet as pq

    nfiles = 10
    size = 5

    dirpath = tmpdir.join(guid()).strpath
    os.mkdir(dirpath)

    test_data = []
    paths = []
    for i in range(nfiles):
        df = _test_dataframe(size, seed=i)

        # Hack so that we don't have a dtype cast in v1 files
        df['uint32'] = df['uint32'].astype(np.int64)

        path = pjoin(dirpath, '{0}.parquet'.format(i))

        table = pa.Table.from_pandas(df)
        _write_table(table, path)

        test_data.append(table)
        paths.append(path)

    # Write a _SUCCESS.crc file
    _touch(pjoin(dirpath, '_SUCCESS.crc'))

    def read_multiple_files(paths, columns=None, nthreads=None, **kwargs):
        dataset = pq.ParquetDataset(paths, **kwargs)
        return dataset.read(columns=columns, nthreads=nthreads)

    result = read_multiple_files(paths)
    expected = pa.concat_tables(test_data)

    assert result.equals(expected)

    # Read with provided metadata
    metadata = pq.read_metadata(paths[0])

    result2 = read_multiple_files(paths, metadata=metadata)
    assert result2.equals(expected)

    result3 = pa.localfs.read_parquet(dirpath, schema=metadata.schema)
    assert result3.equals(expected)

    # Read column subset
    to_read = [result[0], result[2], result[6], result[result.num_columns - 1]]

    result = pa.localfs.read_parquet(
        dirpath, columns=[c.name for c in to_read])
    expected = pa.Table.from_arrays(to_read, metadata=result.schema.metadata)
    assert result.equals(expected)

    # Read with multiple threads
    pa.localfs.read_parquet(dirpath, nthreads=2)

    # Test failure modes with non-uniform metadata
    bad_apple = _test_dataframe(size, seed=i).iloc[:, :4]
    bad_apple_path = tmpdir.join('{0}.parquet'.format(guid())).strpath

    t = pa.Table.from_pandas(bad_apple)
    _write_table(t, bad_apple_path)

    bad_meta = pq.read_metadata(bad_apple_path)

    with pytest.raises(ValueError):
        read_multiple_files(paths + [bad_apple_path])

    with pytest.raises(ValueError):
        read_multiple_files(paths, metadata=bad_meta)

    mixed_paths = [bad_apple_path, paths[0]]

    with pytest.raises(ValueError):
        read_multiple_files(mixed_paths, schema=bad_meta.schema)

    with pytest.raises(ValueError):
        read_multiple_files(mixed_paths)


@parquet
def test_dataset_read_pandas(tmpdir):
    import pyarrow.parquet as pq

    nfiles = 5
    size = 5

    dirpath = tmpdir.join(guid()).strpath
    os.mkdir(dirpath)

    test_data = []
    frames = []
    paths = []
    for i in range(nfiles):
        df = _test_dataframe(size, seed=i)
        df.index = np.arange(i * size, (i + 1) * size)
        df.index.name = 'index'

        path = pjoin(dirpath, '{0}.parquet'.format(i))

        table = pa.Table.from_pandas(df)
        _write_table(table, path)
        test_data.append(table)
        frames.append(df)
        paths.append(path)

    dataset = pq.ParquetDataset(dirpath)
    columns = ['uint8', 'strings']
    result = dataset.read_pandas(columns=columns).to_pandas()
    expected = pd.concat([x[columns] for x in frames])

    tm.assert_frame_equal(result, expected)


@parquet
@pytest.mark.parametrize('preserve_index', [True, False])
def test_dataset_read_pandas_common_metadata(tmpdir, preserve_index):
    # ARROW-1103
    import pyarrow.parquet as pq

    nfiles = 5
    size = 5

    dirpath = tmpdir.join(guid()).strpath
    os.mkdir(dirpath)

    test_data = []
    frames = []
    paths = []
    for i in range(nfiles):
        df = _test_dataframe(size, seed=i)
        df.index = pd.Index(np.arange(i * size, (i + 1) * size), name='index')

        path = pjoin(dirpath, '{:d}.parquet'.format(i))

        table = pa.Table.from_pandas(df, preserve_index=preserve_index)

        # Obliterate metadata
        table = table.replace_schema_metadata(None)
        assert table.schema.metadata is None

        _write_table(table, path)
        test_data.append(table)
        frames.append(df)
        paths.append(path)

    # Write _metadata common file
    table_for_metadata = pa.Table.from_pandas(
        df, preserve_index=preserve_index
    )
    pq.write_metadata(table_for_metadata.schema,
                      pjoin(dirpath, '_metadata'))

    dataset = pq.ParquetDataset(dirpath)
    columns = ['uint8', 'strings']
    result = dataset.read_pandas(columns=columns).to_pandas()
    expected = pd.concat([x[columns] for x in frames])
    expected.index.name = df.index.name if preserve_index else None
    tm.assert_frame_equal(result, expected)


@parquet
def test_ignore_private_directories(tmpdir):
    import pyarrow.parquet as pq

    nfiles = 10
    size = 5

    dirpath = tmpdir.join(guid()).strpath
    os.mkdir(dirpath)

    test_data = []
    paths = []
    for i in range(nfiles):
        df = _test_dataframe(size, seed=i)
        path = pjoin(dirpath, '{0}.parquet'.format(i))

        test_data.append(_write_table(df, path))
        paths.append(path)

    # private directory
    os.mkdir(pjoin(dirpath, '_impala_staging'))

    dataset = pq.ParquetDataset(dirpath)
    assert set(paths) == set(x.path for x in dataset.pieces)


@parquet
def test_multiindex_duplicate_values(tmpdir):
    num_rows = 3
    numbers = list(range(num_rows))
    index = pd.MultiIndex.from_arrays(
        [['foo', 'foo', 'bar'], numbers],
        names=['foobar', 'some_numbers'],
    )

    df = pd.DataFrame({'numbers': numbers}, index=index)
    table = pa.Table.from_pandas(df)

    filename = tmpdir.join('dup_multi_index_levels.parquet').strpath

    _write_table(table, filename)
    result_table = _read_table(filename)
    assert table.equals(result_table)

    result_df = result_table.to_pandas()
    tm.assert_frame_equal(result_df, df)


@parquet
def test_write_error_deletes_incomplete_file(tmpdir):
    # ARROW-1285
    df = pd.DataFrame({'a': list('abc'),
                       'b': list(range(1, 4)),
                       'c': np.arange(3, 6).astype('u1'),
                       'd': np.arange(4.0, 7.0, dtype='float64'),
                       'e': [True, False, True],
                       'f': pd.Categorical(list('abc')),
                       'g': pd.date_range('20130101', periods=3),
                       'h': pd.date_range('20130101', periods=3,
                                          tz='US/Eastern'),
                       'i': pd.date_range('20130101', periods=3, freq='ns')})

    pdf = pa.Table.from_pandas(df)

    filename = tmpdir.join('tmp_file').strpath
    try:
        _write_table(pdf, filename)
    except pa.ArrowException:
        pass

    assert not os.path.exists(filename)


@parquet
def test_read_non_existent_file(tmpdir):
    import pyarrow.parquet as pq

    path = 'non-existent-file.parquet'
    try:
        pq.read_table(path)
    except Exception as e:
        assert path in e.args[0]


def _test_write_to_dataset_with_partitions(base_path, filesystem=None):
    # ARROW-1400
    import pyarrow.parquet as pq

    output_df = pd.DataFrame({'group1': list('aaabbbbccc'),
                              'group2': list('eefeffgeee'),
                              'num': list(range(10)),
                              'date': np.arange('2017-01-01', '2017-01-11',
                                                dtype='datetime64[D]')})
    cols = output_df.columns.tolist()
    partition_by = ['group1', 'group2']
    output_table = pa.Table.from_pandas(output_df)
    pq.write_to_dataset(output_table, base_path, partition_by,
                        filesystem=filesystem)

    metadata_path = os.path.join(base_path, '_common_metadata')

    if filesystem is not None:
        with filesystem.open(metadata_path, 'wb') as f:
            pq.write_metadata(output_table.schema, f)
    else:
        pq.write_metadata(output_table.schema, metadata_path)

    dataset = pq.ParquetDataset(base_path, filesystem=filesystem)
    # ARROW-2209: Ensure the dataset schema also includes the partition columns
    dataset_cols = set(dataset.schema.to_arrow_schema().names)
    assert dataset_cols == set(output_table.schema.names)

    input_table = dataset.read()
    input_df = input_table.to_pandas()

    # Read data back in and compare with original DataFrame
    # Partitioned columns added to the end of the DataFrame when read
    input_df_cols = input_df.columns.tolist()
    assert partition_by == input_df_cols[-1 * len(partition_by):]

    # Partitioned columns become 'categorical' dtypes
    input_df = input_df[cols]
    for col in partition_by:
        output_df[col] = output_df[col].astype('category')
    assert output_df.equals(input_df)


def _test_write_to_dataset_no_partitions(base_path, filesystem=None):
    # ARROW-1400
    import pyarrow.parquet as pq

    output_df = pd.DataFrame({'group1': list('aaabbbbccc'),
                              'group2': list('eefeffgeee'),
                              'num': list(range(10)),
                              'date': np.arange('2017-01-01', '2017-01-11',
                                                dtype='datetime64[D]')})
    cols = output_df.columns.tolist()
    output_table = pa.Table.from_pandas(output_df)

    if filesystem is None:
        filesystem = LocalFileSystem.get_instance()

    # Without partitions, append files to root_path
    n = 5
    for i in range(n):
        pq.write_to_dataset(output_table, base_path,
                            filesystem=filesystem)
    output_files = [file for file in filesystem.ls(base_path)
                    if file.endswith(".parquet")]
    assert len(output_files) == n

    # Deduplicated incoming DataFrame should match
    # original outgoing Dataframe
    input_table = pq.ParquetDataset(base_path,
                                    filesystem=filesystem).read()
    input_df = input_table.to_pandas()
    input_df = input_df.drop_duplicates()
    input_df = input_df[cols]
    assert output_df.equals(input_df)


@parquet
def test_write_to_dataset_with_partitions(tmpdir):
    _test_write_to_dataset_with_partitions(str(tmpdir))


@parquet
def test_write_to_dataset_no_partitions(tmpdir):
    _test_write_to_dataset_no_partitions(str(tmpdir))


@pytest.mark.large_memory
@parquet
def test_large_table_int32_overflow():
    size = np.iinfo('int32').max + 1

    arr = np.ones(size, dtype='uint8')

    parr = pa.array(arr, type=pa.uint8())

    table = pa.Table.from_arrays([parr], names=['one'])
    f = io.BytesIO()
    _write_table(table, f)


@parquet
def test_index_column_name_duplicate(tmpdir):
    data = {
        'close': {
            pd.Timestamp('2017-06-30 01:31:00'): 154.99958999999998,
            pd.Timestamp('2017-06-30 01:32:00'): 154.99958999999998,
        },
        'time': {
            pd.Timestamp('2017-06-30 01:31:00'): pd.Timestamp(
                '2017-06-30 01:31:00'
            ),
            pd.Timestamp('2017-06-30 01:32:00'): pd.Timestamp(
                '2017-06-30 01:32:00'
            ),
        }
    }
    path = str(tmpdir / 'data.parquet')
    dfx = pd.DataFrame(data).set_index('time', drop=False)
    tdfx = pa.Table.from_pandas(dfx)
    _write_table(tdfx, path)
    arrow_table = _read_table(path)
    result_df = arrow_table.to_pandas()
    tm.assert_frame_equal(result_df, dfx)


@parquet
def test_parquet_nested_convenience(tmpdir):
    # ARROW-1684
    import pyarrow.parquet as pq

    df = pd.DataFrame({
        'a': [[1, 2, 3], None, [4, 5], []],
        'b': [[1.], None, None, [6., 7.]],
    })

    path = str(tmpdir / 'nested_convenience.parquet')

    table = pa.Table.from_pandas(df, preserve_index=False)
    _write_table(table, path)

    read = pq.read_table(path, columns=['a'])
    tm.assert_frame_equal(read.to_pandas(), df[['a']])

    read = pq.read_table(path, columns=['a', 'b'])
    tm.assert_frame_equal(read.to_pandas(), df)


@parquet
def test_backwards_compatible_index_naming():
    expected_string = b"""\
carat        cut  color  clarity  depth  table  price     x     y     z
 0.23      Ideal      E      SI2   61.5   55.0    326  3.95  3.98  2.43
 0.21    Premium      E      SI1   59.8   61.0    326  3.89  3.84  2.31
 0.23       Good      E      VS1   56.9   65.0    327  4.05  4.07  2.31
 0.29    Premium      I      VS2   62.4   58.0    334  4.20  4.23  2.63
 0.31       Good      J      SI2   63.3   58.0    335  4.34  4.35  2.75
 0.24  Very Good      J     VVS2   62.8   57.0    336  3.94  3.96  2.48
 0.24  Very Good      I     VVS1   62.3   57.0    336  3.95  3.98  2.47
 0.26  Very Good      H      SI1   61.9   55.0    337  4.07  4.11  2.53
 0.22       Fair      E      VS2   65.1   61.0    337  3.87  3.78  2.49
 0.23  Very Good      H      VS1   59.4   61.0    338  4.00  4.05  2.39"""
    expected = pd.read_csv(
        io.BytesIO(expected_string), sep=r'\s{2,}', index_col=None, header=0
    )
    path = os.path.join(os.path.dirname(__file__), 'data', 'v0.7.1.parquet')
    t = _read_table(path)
    result = t.to_pandas()
    tm.assert_frame_equal(result, expected)


@parquet
def test_backwards_compatible_index_multi_level_named():
    expected_string = b"""\
carat        cut  color  clarity  depth  table  price     x     y     z
 0.23      Ideal      E      SI2   61.5   55.0    326  3.95  3.98  2.43
 0.21    Premium      E      SI1   59.8   61.0    326  3.89  3.84  2.31
 0.23       Good      E      VS1   56.9   65.0    327  4.05  4.07  2.31
 0.29    Premium      I      VS2   62.4   58.0    334  4.20  4.23  2.63
 0.31       Good      J      SI2   63.3   58.0    335  4.34  4.35  2.75
 0.24  Very Good      J     VVS2   62.8   57.0    336  3.94  3.96  2.48
 0.24  Very Good      I     VVS1   62.3   57.0    336  3.95  3.98  2.47
 0.26  Very Good      H      SI1   61.9   55.0    337  4.07  4.11  2.53
 0.22       Fair      E      VS2   65.1   61.0    337  3.87  3.78  2.49
 0.23  Very Good      H      VS1   59.4   61.0    338  4.00  4.05  2.39"""
    expected = pd.read_csv(
        io.BytesIO(expected_string),
        sep=r'\s{2,}', index_col=['cut', 'color', 'clarity'], header=0
    ).sort_index()
    path = os.path.join(
        os.path.dirname(__file__), 'data', 'v0.7.1.all-named-index.parquet'
    )
    t = _read_table(path)
    result = t.to_pandas()
    tm.assert_frame_equal(result, expected)


@parquet
def test_backwards_compatible_index_multi_level_some_named():
    expected_string = b"""\
carat        cut  color  clarity  depth  table  price     x     y     z
 0.23      Ideal      E      SI2   61.5   55.0    326  3.95  3.98  2.43
 0.21    Premium      E      SI1   59.8   61.0    326  3.89  3.84  2.31
 0.23       Good      E      VS1   56.9   65.0    327  4.05  4.07  2.31
 0.29    Premium      I      VS2   62.4   58.0    334  4.20  4.23  2.63
 0.31       Good      J      SI2   63.3   58.0    335  4.34  4.35  2.75
 0.24  Very Good      J     VVS2   62.8   57.0    336  3.94  3.96  2.48
 0.24  Very Good      I     VVS1   62.3   57.0    336  3.95  3.98  2.47
 0.26  Very Good      H      SI1   61.9   55.0    337  4.07  4.11  2.53
 0.22       Fair      E      VS2   65.1   61.0    337  3.87  3.78  2.49
 0.23  Very Good      H      VS1   59.4   61.0    338  4.00  4.05  2.39"""
    expected = pd.read_csv(
        io.BytesIO(expected_string),
        sep=r'\s{2,}', index_col=['cut', 'color', 'clarity'], header=0
    ).sort_index()
    expected.index = expected.index.set_names(['cut', None, 'clarity'])
    path = os.path.join(
        os.path.dirname(__file__), 'data', 'v0.7.1.some-named-index.parquet'
    )
    t = _read_table(path)
    result = t.to_pandas()
    tm.assert_frame_equal(result, expected)


@parquet
def test_backwards_compatible_column_metadata_handling():
    expected = pd.DataFrame(
        {'a': [1, 2, 3], 'b': [.1, .2, .3],
         'c': pd.date_range("2017-01-01", periods=3, tz='Europe/Brussels')})
    expected.index = pd.MultiIndex.from_arrays(
        [['a', 'b', 'c'],
         pd.date_range("2017-01-01", periods=3, tz='Europe/Brussels')],
        names=['index', None])

    path = os.path.join(
        os.path.dirname(__file__), 'data',
        'v0.7.1.column-metadata-handling.parquet'
    )
    t = _read_table(path)
    result = t.to_pandas()
    tm.assert_frame_equal(result, expected)

    t = _read_table(path, columns=['a'])
    result = t.to_pandas()
    tm.assert_frame_equal(result, expected[['a']].reset_index(drop=True))


@parquet
def test_decimal_roundtrip(tmpdir):
    num_values = 10

    columns = {}

    for precision in range(1, 39):
        for scale in range(0, precision + 1):
            with util.random_seed(0):
                random_decimal_values = [
                    util.randdecimal(precision, scale)
                    for _ in range(num_values)
                ]
            column_name = ('dec_precision_{:d}_scale_{:d}'
                           .format(precision, scale))
            columns[column_name] = random_decimal_values

    expected = pd.DataFrame(columns)
    filename = tmpdir.join('decimals.parquet')
    string_filename = str(filename)
    t = pa.Table.from_pandas(expected)
    _write_table(t, string_filename)
    result_table = _read_table(string_filename)
    result = result_table.to_pandas()
    tm.assert_frame_equal(result, expected)


@parquet
@pytest.mark.xfail(
    raises=pa.ArrowException, reason='Parquet does not support negative scale'
)
def test_decimal_roundtrip_negative_scale(tmpdir):
    expected = pd.DataFrame({'decimal_num': [decimal.Decimal('1.23E4')]})
    filename = tmpdir.join('decimals.parquet')
    string_filename = str(filename)
    t = pa.Table.from_pandas(expected)
    _write_table(t, string_filename)
    result_table = _read_table(string_filename)
    result = result_table.to_pandas()
    tm.assert_frame_equal(result, expected)


@parquet
def test_parquet_writer_context_obj(tmpdir):

    import pyarrow.parquet as pq

    df = _test_dataframe(100)
    df['unique_id'] = 0

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    out = pa.BufferOutputStream()

    with pq.ParquetWriter(out, arrow_table.schema, version='2.0') as writer:

        frames = []
        for i in range(10):
            df['unique_id'] = i
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            writer.write_table(arrow_table)

            frames.append(df.copy())

    buf = out.get_result()
    result = _read_table(pa.BufferReader(buf))

    expected = pd.concat(frames, ignore_index=True)
    tm.assert_frame_equal(result.to_pandas(), expected)


@parquet
def test_parquet_writer_context_obj_with_exception(tmpdir):

    import pyarrow.parquet as pq

    df = _test_dataframe(100)
    df['unique_id'] = 0

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    out = pa.BufferOutputStream()
    error_text = 'Artificial Error'

    try:
        with pq.ParquetWriter(out,
                              arrow_table.schema,
                              version='2.0') as writer:

            frames = []
            for i in range(10):
                df['unique_id'] = i
                arrow_table = pa.Table.from_pandas(df, preserve_index=False)
                writer.write_table(arrow_table)
                frames.append(df.copy())
                if i == 5:
                    raise ValueError(error_text)
    except Exception as e:
        assert str(e) == error_text

    buf = out.get_result()
    result = _read_table(pa.BufferReader(buf))

    expected = pd.concat(frames, ignore_index=True)
    tm.assert_frame_equal(result.to_pandas(), expected)

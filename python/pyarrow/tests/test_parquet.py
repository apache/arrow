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
import io
import os
import pytest

from pyarrow.compat import guid, u
from pyarrow.filesystem import LocalFilesystem
import pyarrow as pa
from .pandas_examples import dataframe_with_arrays, dataframe_with_lists

import numpy as np
import pandas as pd

import pandas.util.testing as tm

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
        data = [pa.from_pylist(list(map(dtype, range(5))))]
        table = pa.Table.from_arrays(data, names=('a', 'b'))
        pq.write_table(table, filename.strpath)
        table_read = pq.read_table(filename.strpath)
        for col_written, col_read in zip(table.itercolumns(),
                                         table_read.itercolumns()):
            assert col_written.name == col_read.name
            assert col_read.data.num_chunks == 1
            data_written = col_written.data.chunk(0)
            data_read = col_read.data.chunk(0)
            assert data_written.equals(data_read)


def alltypes_sample(size=10000, seed=0):
    np.random.seed(seed)
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
        # TODO(wesm): Test other timestamp resolutions now that arrow supports
        # them
        'datetime': np.arange("2016-01-01T00:00:00.001", size,
                              dtype='datetime64[ms]'),
        'str': [str(x) for x in range(size)],
        'str_with_nulls': [None] + [str(x) for x in range(size - 2)] + [None],
        'empty_str': [''] * size
    })
    return df


@parquet
def test_pandas_parquet_2_0_rountrip(tmpdir):
    df = alltypes_sample(size=10000)

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df, timestamps_to_ms=True)
    pq.write_table(arrow_table, filename.strpath, version="2.0")
    table_read = pq.read_table(filename.strpath)
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
    pq.write_table(arrow_table, filename.strpath, version="1.0")
    table_read = pq.read_table(filename.strpath)
    df_read = table_read.to_pandas()

    # We pass uint32_t as int64_t if we write Parquet version 1.0
    df['uint32'] = df['uint32'].values.astype(np.int64)

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
    pq.write_table(arrow_table, filename.strpath)
    table_read = pq.read_table(filename.strpath, columns=['uint8'])
    df_read = table_read.to_pandas()

    tm.assert_frame_equal(df[['uint8']], df_read)


def _random_integers(size, dtype):
    # We do not generate integers outside the int64 range
    i64_info = np.iinfo('int64')
    iinfo = np.iinfo(dtype)
    return np.random.randint(max(iinfo.min, i64_info.min),
                             min(iinfo.max, i64_info.max),
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
        'float64': np.random.randn(size),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0,
        'strings': [tm.rands(10) for i in range(size)]
    })
    return df


@parquet
def test_pandas_parquet_native_file_roundtrip(tmpdir):
    df = _test_dataframe(10000)
    arrow_table = pa.Table.from_pandas(df)
    imos = pa.InMemoryOutputStream()
    pq.write_table(arrow_table, imos, version="2.0")
    buf = imos.get_result()
    reader = pa.BufferReader(buf)
    df_read = pq.read_table(reader).to_pandas()
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
        pq.write_table(arrow_table, f, version="1.0")

    data = io.BytesIO(open(filename, 'rb').read())

    table_read = pq.read_table(data)
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
        pq.write_table(arrow_table, filename.strpath,
                       version="2.0",
                       use_dictionary=use_dictionary)
        table_read = pq.read_table(filename.strpath)
        df_read = table_read.to_pandas()
        tm.assert_frame_equal(df, df_read)

    for compression in ['NONE', 'SNAPPY', 'GZIP']:
        pq.write_table(arrow_table, filename.strpath,
                       version="2.0",
                       compression=compression)
        table_read = pq.read_table(filename.strpath)
        df_read = table_read.to_pandas()
        tm.assert_frame_equal(df, df_read)


def make_sample_file(df):
    a_table = pa.Table.from_pandas(df, timestamps_to_ms=True)

    buf = io.BytesIO()
    pq.write_table(a_table, buf, compression='SNAPPY', version='2.0')

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
    assert meta.num_columns == ncols
    assert meta.num_row_groups == 1
    assert meta.format_version == '2.0'
    assert 'parquet-cpp' in meta.created_by

    # Schema
    schema = fileh.schema
    assert meta.schema is schema
    assert len(schema) == ncols
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
        schema[ncols]

    with pytest.raises(IndexError):
        schema[-1]

    # Row group
    rg_meta = meta.row_group(0)
    repr(rg_meta)

    assert rg_meta.num_rows == len(df)
    assert rg_meta.num_columns == ncols


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
    arrow_table = pa.Table.from_pandas(df, timestamps_to_ms=True,
                                       schema=schema)
    pq.write_table(arrow_table, filename.strpath, version="2.0")
    table_read = pq.read_table(filename.strpath)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@parquet
def test_column_of_lists(tmpdir):
    df, schema = dataframe_with_lists()

    filename = tmpdir.join('pandas_rountrip.parquet')
    arrow_table = pa.Table.from_pandas(df, timestamps_to_ms=True,
                                       schema=schema)
    pq.write_table(arrow_table, filename.strpath, version="2.0")
    table_read = pq.read_table(filename.strpath)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@parquet
def test_multithreaded_read():
    df = alltypes_sample(size=10000)

    table = pa.Table.from_pandas(df, timestamps_to_ms=True)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression='SNAPPY', version='2.0')

    buf.seek(0)
    table1 = pq.read_table(buf, nthreads=4)

    buf.seek(0)
    table2 = pq.read_table(buf, nthreads=1)

    assert table1.equals(table2)


@parquet
def test_min_chunksize():
    data = pd.DataFrame([np.arange(4)], columns=['A', 'B', 'C', 'D'])
    table = pa.Table.from_pandas(data.reset_index())

    buf = io.BytesIO()
    pq.write_table(table, buf, chunk_size=-1)

    buf.seek(0)
    result = pq.read_table(buf)

    assert result.equals(table)

    with pytest.raises(ValueError):
        pq.write_table(table, buf, chunk_size=0)


@parquet
def test_pass_separate_metadata():
    # ARROW-471
    df = alltypes_sample(size=10000)

    a_table = pa.Table.from_pandas(df, timestamps_to_ms=True)

    buf = io.BytesIO()
    pq.write_table(a_table, buf, compression='snappy', version='2.0')

    buf.seek(0)
    metadata = pq.ParquetFile(buf).metadata

    buf.seek(0)

    fileh = pq.ParquetFile(buf, metadata=metadata)

    tm.assert_frame_equal(df, fileh.read().to_pandas())


@parquet
def test_read_single_row_group():
    # ARROW-471
    N, K = 10000, 4
    df = alltypes_sample(size=N)

    a_table = pa.Table.from_pandas(df, timestamps_to_ms=True)

    buf = io.BytesIO()
    pq.write_table(a_table, buf, row_group_size=N / K,
                   compression='snappy', version='2.0')

    buf.seek(0)

    pf = pq.ParquetFile(buf)

    assert pf.num_row_groups == K

    row_groups = [pf.read_row_group(i) for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df, result.to_pandas())

    cols = df.columns[:2]
    row_groups = [pf.read_row_group(i, columns=cols)
                  for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df[cols], result.to_pandas())


@parquet
def test_parquet_piece_basics():
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
    set1 = pq.PartitionSet('key1', [u('foo'), u('bar'), u('baz')])
    set2 = pq.PartitionSet('key2', [2007, 2008, 2009])

    assert isinstance(set1.dictionary, pa.StringArray)
    assert isinstance(set2.dictionary, pa.IntegerArray)

    set3 = pq.PartitionSet('key2', [datetime.datetime(2007, 1, 1)])
    with pytest.raises(TypeError):
        set3.dictionary


@parquet
def test_read_partitioned_directory(tmpdir):
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

    base_path = str(tmpdir)
    _generate_partition_directories(base_path, partition_spec, df)

    dataset = pq.ParquetDataset(base_path)
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


def _generate_partition_directories(base_dir, partition_spec, df):
    # partition_spec : list of lists, e.g. [['foo', [0, 1, 2],
    #                                       ['bar', ['a', 'b', 'c']]
    # part_table : a pyarrow.Table to write to each partition
    DEPTH = len(partition_spec)
    fs = LocalFilesystem.get_instance()

    def _visit_level(base_dir, level, part_keys):
        name, values = partition_spec[level]
        for value in values:
            this_part_keys = part_keys + [(name, value)]

            level_dir = pjoin(base_dir, '{0}={1}'.format(name, value))
            fs.mkdir(level_dir)

            if level == DEPTH - 1:
                # Generate example data
                file_path = pjoin(level_dir, 'data.parq')

                filtered_df = _filter_partition(df, this_part_keys)
                part_table = pa.Table.from_pandas(filtered_df)
                pq.write_table(part_table, file_path)
            else:
                _visit_level(level_dir, level + 1, this_part_keys)

    _visit_level(base_dir, 0, [])


def _filter_partition(df, part_keys):
    predicate = np.ones(len(df), dtype=bool)

    to_drop = []
    for name, value in part_keys:
        to_drop.append(name)
        predicate &= df[name] == value

    return df[predicate].drop(to_drop, axis=1)


@parquet
def test_read_multiple_files(tmpdir):
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
        pq.write_table(table, path)

        test_data.append(table)
        paths.append(path)

    # Write a _SUCCESS.crc file
    with open(pjoin(dirpath, '_SUCCESS.crc'), 'wb') as f:
        f.write(b'0')

    def read_multiple_files(paths, columns=None, nthreads=None, **kwargs):
        dataset = pq.ParquetDataset(paths, **kwargs)
        return dataset.read(columns=columns, nthreads=nthreads)

    result = read_multiple_files(paths)
    expected = pa.concat_tables(test_data)

    assert result.equals(expected)

    # Read with provided metadata
    metadata = pq.ParquetFile(paths[0]).metadata

    result2 = read_multiple_files(paths, metadata=metadata)
    assert result2.equals(expected)

    result3 = pa.localfs.read_parquet(dirpath, schema=metadata.schema)
    assert result3.equals(expected)

    # Read column subset
    to_read = [result[0], result[3], result[6]]
    result = pa.localfs.read_parquet(
        dirpath, columns=[c.name for c in to_read])
    expected = pa.Table.from_arrays(to_read)
    assert result.equals(expected)

    # Read with multiple threads
    pa.localfs.read_parquet(dirpath, nthreads=2)

    # Test failure modes with non-uniform metadata
    bad_apple = _test_dataframe(size, seed=i).iloc[:, :4]
    bad_apple_path = tmpdir.join('{0}.parquet'.format(guid())).strpath

    t = pa.Table.from_pandas(bad_apple)
    pq.write_table(t, bad_apple_path)

    bad_meta = pq.ParquetFile(bad_apple_path).metadata

    with pytest.raises(ValueError):
        read_multiple_files(paths + [bad_apple_path])

    with pytest.raises(ValueError):
        read_multiple_files(paths, metadata=bad_meta)

    mixed_paths = [bad_apple_path, paths[0]]

    with pytest.raises(ValueError):
        read_multiple_files(mixed_paths, schema=bad_meta.schema)

    with pytest.raises(ValueError):
        read_multiple_files(mixed_paths)

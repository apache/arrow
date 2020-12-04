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

import datetime
import decimal
import io
import json
import os
from collections import OrderedDict

import numpy as np
import pytest

import pyarrow as pa
from pyarrow import fs
from pyarrow.filesystem import LocalFileSystem
from pyarrow.tests import util
from pyarrow.tests.parquet.common import (
    _check_roundtrip, _roundtrip_table, make_sample_file,
    parametrize_legacy_dataset, parametrize_legacy_dataset_fixed,
    parametrize_legacy_dataset_not_supported, _simple_table_write_read)

try:
    import pyarrow.parquet as pq
    from pyarrow.tests.parquet.common import (_read_table, _test_dataframe,
                                              _write_table)
except ImportError:
    pq = None


try:
    import pandas as pd
    import pandas.testing as tm

    from pyarrow.tests.pandas_examples import (dataframe_with_arrays,
                                               dataframe_with_lists)
    from pyarrow.tests.parquet.common import (_roundtrip_pandas_dataframe,
                                              alltypes_sample)
except ImportError:
    pd = tm = None


def test_large_binary():
    data = [b'foo', b'bar'] * 50
    for type in [pa.large_binary(), pa.large_string()]:
        arr = pa.array(data, type=type)
        table = pa.Table.from_arrays([arr], names=['strs'])
        for use_dictionary in [False, True]:
            _check_roundtrip(table, use_dictionary=use_dictionary)


@pytest.mark.large_memory
def test_large_binary_huge():
    s = b'xy' * 997
    data = [s] * ((1 << 33) // len(s))
    for type in [pa.large_binary(), pa.large_string()]:
        arr = pa.array(data, type=type)
        table = pa.Table.from_arrays([arr], names=['strs'])
        for use_dictionary in [False, True]:
            _check_roundtrip(table, use_dictionary=use_dictionary)
        del arr, table


@pytest.mark.large_memory
def test_large_binary_overflow():
    s = b'x' * (1 << 31)
    arr = pa.array([s], type=pa.large_binary())
    table = pa.Table.from_arrays([arr], names=['strs'])
    for use_dictionary in [False, True]:
        writer = pa.BufferOutputStream()
        with pytest.raises(
                pa.ArrowInvalid,
                match="Parquet cannot store strings with size 2GB or more"):
            _write_table(table, writer, use_dictionary=use_dictionary)


@parametrize_legacy_dataset
@pytest.mark.parametrize('dtype', [int, float])
def test_single_pylist_column_roundtrip(tempdir, dtype, use_legacy_dataset):
    filename = tempdir / 'single_{}_column.parquet'.format(dtype.__name__)
    data = [pa.array(list(map(dtype, range(5))))]
    table = pa.Table.from_arrays(data, names=['a'])
    _write_table(table, filename)
    table_read = _read_table(filename, use_legacy_dataset=use_legacy_dataset)
    for i in range(table.num_columns):
        col_written = table[i]
        col_read = table_read[i]
        assert table.field(i).name == table_read.field(i).name
        assert col_read.num_chunks == 1
        data_written = col_written.chunk(0)
        data_read = col_read.chunk(0)
        assert data_written.equals(data_read)



def test_parquet_invalid_version(tempdir):
    table = pa.table({'a': [1, 2, 3]})
    with pytest.raises(ValueError, match="Unsupported Parquet format version"):
        _write_table(table, tempdir / 'test_version.parquet', version="2.2")
    with pytest.raises(ValueError, match="Unsupported Parquet data page " +
                       "version"):
        _write_table(table, tempdir / 'test_version.parquet',
                     data_page_version="2.2")


@parametrize_legacy_dataset
def test_set_data_page_size(use_legacy_dataset):
    arr = pa.array([1, 2, 3] * 100000)
    t = pa.Table.from_arrays([arr], names=['f0'])

    # 128K, 512K
    page_sizes = [2 << 16, 2 << 18]
    for target_page_size in page_sizes:
        _check_roundtrip(t, data_page_size=target_page_size,
                         use_legacy_dataset=use_legacy_dataset)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_chunked_table_write(use_legacy_dataset):
    # ARROW-232
    tables = []
    batch = pa.RecordBatch.from_pandas(alltypes_sample(size=10))
    tables.append(pa.Table.from_batches([batch] * 3))
    df, _ = dataframe_with_lists()
    batch = pa.RecordBatch.from_pandas(df)
    tables.append(pa.Table.from_batches([batch] * 3))

    for data_page_version in ['1.0', '2.0']:
        for use_dictionary in [True, False]:
            for table in tables:
                _check_roundtrip(
                    table, version='2.0',
                    use_legacy_dataset=use_legacy_dataset,
                    data_page_version=data_page_version,
                    use_dictionary=use_dictionary)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_memory_map(tempdir, use_legacy_dataset):
    df = alltypes_sample(size=10)

    table = pa.Table.from_pandas(df)
    _check_roundtrip(table, read_table_kwargs={'memory_map': True},
                     version='2.0', use_legacy_dataset=use_legacy_dataset)

    filename = str(tempdir / 'tmp_file')
    with open(filename, 'wb') as f:
        _write_table(table, f, version='2.0')
    table_read = pq.read_pandas(filename, memory_map=True,
                                use_legacy_dataset=use_legacy_dataset)
    assert table_read.equals(table)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_enable_buffered_stream(tempdir, use_legacy_dataset):
    df = alltypes_sample(size=10)

    table = pa.Table.from_pandas(df)
    _check_roundtrip(table, read_table_kwargs={'buffer_size': 1025},
                     version='2.0', use_legacy_dataset=use_legacy_dataset)

    filename = str(tempdir / 'tmp_file')
    with open(filename, 'wb') as f:
        _write_table(table, f, version='2.0')
    table_read = pq.read_pandas(filename, buffer_size=4096,
                                use_legacy_dataset=use_legacy_dataset)
    assert table_read.equals(table)


@parametrize_legacy_dataset
def test_special_chars_filename(tempdir, use_legacy_dataset):
    table = pa.Table.from_arrays([pa.array([42])], ["ints"])
    filename = "foo # bar"
    path = tempdir / filename
    assert not path.exists()
    _write_table(table, str(path))
    assert path.exists()
    table_read = _read_table(str(path), use_legacy_dataset=use_legacy_dataset)
    assert table_read.equals(table)


@pytest.mark.slow
def test_file_with_over_int16_max_row_groups():
    # PARQUET-1857: Parquet encryption support introduced a INT16_MAX upper
    # limit on the number of row groups, but this limit only impacts files with
    # encrypted row group metadata because of the int16 row group ordinal used
    # in the Parquet Thrift metadata. Unencrypted files are not impacted, so
    # this test checks that it works (even if it isn't a good idea)
    t = pa.table([list(range(40000))], names=['f0'])
    _check_roundtrip(t, row_group_size=1)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_empty_table_roundtrip(use_legacy_dataset):
    df = alltypes_sample(size=10)

    # Create a non-empty table to infer the types correctly, then slice to 0
    table = pa.Table.from_pandas(df)
    table = pa.Table.from_arrays(
        [col.chunk(0)[:0] for col in table.itercolumns()],
        names=table.schema.names)

    assert table.schema.field('null').type == pa.null()
    assert table.schema.field('null_list').type == pa.list_(pa.null())
    _check_roundtrip(
        table, version='2.0', use_legacy_dataset=use_legacy_dataset)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_empty_table_no_columns(use_legacy_dataset):
    df = pd.DataFrame()
    empty = pa.Table.from_pandas(df, preserve_index=False)
    _check_roundtrip(empty, use_legacy_dataset=use_legacy_dataset)


@parametrize_legacy_dataset
def test_empty_lists_table_roundtrip(use_legacy_dataset):
    # ARROW-2744: Shouldn't crash when writing an array of empty lists
    arr = pa.array([[], []], type=pa.list_(pa.int32()))
    table = pa.Table.from_arrays([arr], ["A"])
    _check_roundtrip(table, use_legacy_dataset=use_legacy_dataset)


@parametrize_legacy_dataset
def test_nested_list_nonnullable_roundtrip_bug(use_legacy_dataset):
    # Reproduce failure in ARROW-5630
    typ = pa.list_(pa.field("item", pa.float32(), False))
    num_rows = 10000
    t = pa.table([
        pa.array(([[0] * ((i + 5) % 10) for i in range(0, 10)] *
                  (num_rows // 10)), type=typ)
    ], ['a'])
    _check_roundtrip(
        t, data_page_size=4096, use_legacy_dataset=use_legacy_dataset)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_multiple_path_types(tempdir, use_legacy_dataset):
    # Test compatibility with PEP 519 path-like objects
    path = tempdir / 'zzz.parquet'
    df = pd.DataFrame({'x': np.arange(10, dtype=np.int64)})
    _write_table(df, path)
    table_read = _read_table(path, use_legacy_dataset=use_legacy_dataset)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)

    # Test compatibility with plain string paths
    path = str(tempdir) + 'zzz.parquet'
    df = pd.DataFrame({'x': np.arange(10, dtype=np.int64)})
    _write_table(df, path)
    table_read = _read_table(path, use_legacy_dataset=use_legacy_dataset)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@pytest.mark.dataset
@parametrize_legacy_dataset
@pytest.mark.parametrize("filesystem", [
    None, fs.LocalFileSystem(), LocalFileSystem._get_instance()
])
def test_relative_paths(tempdir, use_legacy_dataset, filesystem):
    # reading and writing from relative paths
    table = pa.table({"a": [1, 2, 3]})

    # reading
    pq.write_table(table, str(tempdir / "data.parquet"))
    with util.change_cwd(tempdir):
        result = pq.read_table("data.parquet", filesystem=filesystem,
                               use_legacy_dataset=use_legacy_dataset)
    assert result.equals(table)

    # writing
    with util.change_cwd(tempdir):
        pq.write_table(table, "data2.parquet", filesystem=filesystem)
    result = pq.read_table(tempdir / "data2.parquet")
    assert result.equals(table)


@parametrize_legacy_dataset
def test_read_non_existing_file(use_legacy_dataset):
    # ensure we have a proper error message
    with pytest.raises(FileNotFoundError):
        pq.read_table('i-am-not-existing.parquet')



@parametrize_legacy_dataset
def test_parquet_read_from_buffer(tempdir, use_legacy_dataset):
    # reading from a buffer from python's open()
    table = pa.table({"a": [1, 2, 3]})
    pq.write_table(table, str(tempdir / "data.parquet"))

    with open(str(tempdir / "data.parquet"), "rb") as f:
        result = pq.read_table(f, use_legacy_dataset=use_legacy_dataset)
    assert result.equals(table)

    with open(str(tempdir / "data.parquet"), "rb") as f:
        result = pq.read_table(pa.PythonFile(f),
                               use_legacy_dataset=use_legacy_dataset)
    assert result.equals(table)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_parquet_incremental_file_build(tempdir, use_legacy_dataset):
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

    buf = out.getvalue()
    result = _read_table(
        pa.BufferReader(buf), use_legacy_dataset=use_legacy_dataset)

    expected = pd.concat(frames, ignore_index=True)
    tm.assert_frame_equal(result.to_pandas(), expected)


@parametrize_legacy_dataset
def test_byte_stream_split(use_legacy_dataset):
    # This is only a smoke test.
    arr_float = pa.array(list(map(float, range(100))))
    arr_int = pa.array(list(map(int, range(100))))
    data_float = [arr_float, arr_float]
    table = pa.Table.from_arrays(data_float, names=['a', 'b'])

    # Check with byte_stream_split for both columns.
    _check_roundtrip(table, expected=table, compression="gzip",
                     use_dictionary=False, use_byte_stream_split=True)

    # Check with byte_stream_split for column 'b' and dictionary
    # for column 'a'.
    _check_roundtrip(table, expected=table, compression="gzip",
                     use_dictionary=['a'],
                     use_byte_stream_split=['b'])

    # Check with a collision for both columns.
    _check_roundtrip(table, expected=table, compression="gzip",
                     use_dictionary=['a', 'b'],
                     use_byte_stream_split=['a', 'b'])

    # Check with mixed column types.
    mixed_table = pa.Table.from_arrays([arr_float, arr_int],
                                       names=['a', 'b'])
    _check_roundtrip(mixed_table, expected=mixed_table,
                     use_dictionary=['b'],
                     use_byte_stream_split=['a'])

    # Try to use the wrong data type with the byte_stream_split encoding.
    # This should throw an exception.
    table = pa.Table.from_arrays([arr_int], names=['tmp'])
    with pytest.raises(IOError):
        _check_roundtrip(table, expected=table, use_byte_stream_split=True,
                         use_dictionary=False,
                         use_legacy_dataset=use_legacy_dataset)


@parametrize_legacy_dataset
def test_compression_level(use_legacy_dataset):
    arr = pa.array(list(map(int, range(1000))))
    data = [arr, arr]
    table = pa.Table.from_arrays(data, names=['a', 'b'])

    # Check one compression level.
    _check_roundtrip(table, expected=table, compression="gzip",
                     compression_level=1,
                     use_legacy_dataset=use_legacy_dataset)

    # Check another one to make sure that compression_level=1 does not
    # coincide with the default one in Arrow.
    _check_roundtrip(table, expected=table, compression="gzip",
                     compression_level=5,
                     use_legacy_dataset=use_legacy_dataset)

    # Check that the user can provide a compression per column
    _check_roundtrip(table, expected=table,
                     compression={'a': "gzip", 'b': "snappy"},
                     use_legacy_dataset=use_legacy_dataset)

    # Check that the user can provide a compression level per column
    _check_roundtrip(table, expected=table, compression="gzip",
                     compression_level={'a': 2, 'b': 3},
                     use_legacy_dataset=use_legacy_dataset)

    # Check that specifying a compression level for a codec which does allow
    # specifying one, results into an error.
    # Uncompressed, snappy, lz4 and lzo do not support specifying a compression
    # level.
    # GZIP (zlib) allows for specifying a compression level but as of up
    # to version 1.2.11 the valid range is [-1, 9].
    invalid_combinations = [("snappy", 4), ("lz4", 5), ("gzip", -1337),
                            ("None", 444), ("lzo", 14)]
    buf = io.BytesIO()
    for (codec, level) in invalid_combinations:
        with pytest.raises((ValueError, OSError)):
            _write_table(table, buf, compression=codec,
                         compression_level=level)


@pytest.mark.pandas
def test_compare_schemas():
    df = alltypes_sample(size=10000)

    fileh = make_sample_file(df)
    fileh2 = make_sample_file(df)
    fileh3 = make_sample_file(df[df.columns[::2]])

    # ParquetSchema
    assert isinstance(fileh.schema, pq.ParquetSchema)
    assert fileh.schema.equals(fileh.schema)
    assert fileh.schema == fileh.schema
    assert fileh.schema.equals(fileh2.schema)
    assert fileh.schema == fileh2.schema
    assert fileh.schema != 'arbitrary object'
    assert not fileh.schema.equals(fileh3.schema)
    assert fileh.schema != fileh3.schema

    # ColumnSchema
    assert isinstance(fileh.schema[0], pq.ColumnSchema)
    assert fileh.schema[0].equals(fileh.schema[0])
    assert fileh.schema[0] == fileh.schema[0]
    assert not fileh.schema[0].equals(fileh.schema[1])
    assert fileh.schema[0] != fileh.schema[1]
    assert fileh.schema[0] != 'arbitrary object'


def test_validate_schema_write_table(tempdir):
    # ARROW-2926
    simple_fields = [
        pa.field('POS', pa.uint32()),
        pa.field('desc', pa.string())
    ]

    simple_schema = pa.schema(simple_fields)

    # simple_table schema does not match simple_schema
    simple_from_array = [pa.array([1]), pa.array(['bla'])]
    simple_table = pa.Table.from_arrays(simple_from_array, ['POS', 'desc'])

    path = tempdir / 'simple_validate_schema.parquet'

    with pq.ParquetWriter(path, simple_schema,
                          version='2.0',
                          compression='snappy', flavor='spark') as w:
        with pytest.raises(ValueError):
            w.write_table(simple_table)


@pytest.mark.pandas
def test_column_of_arrays(tempdir):
    df, schema = dataframe_with_arrays()

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    _write_table(arrow_table, filename, version="2.0", coerce_timestamps='ms')
    table_read = _read_table(filename)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@pytest.mark.pandas
def test_column_of_lists(tempdir):
    df, schema = dataframe_with_lists(parquet_compatible=True)

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    _write_table(arrow_table, filename, version='2.0')
    table_read = _read_table(filename)
    df_read = table_read.to_pandas()

    tm.assert_frame_equal(df, df_read)



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


def test_sanitized_spark_field_names():
    a0 = pa.array([0, 1, 2, 3, 4])
    name = 'prohib; ,\t{}'
    table = pa.Table.from_arrays([a0], [name])

    result = _roundtrip_table(table, write_table_kwargs={'flavor': 'spark'})

    expected_name = 'prohib______'
    assert result.schema[0].name == expected_name


def test_fixed_size_binary():
    t0 = pa.binary(10)
    data = [b'fooooooooo', None, b'barooooooo', b'quxooooooo']
    a0 = pa.array(data, type=t0)

    table = pa.Table.from_arrays([a0],
                                 ['binary[10]'])
    _check_roundtrip(table)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_multithreaded_read(use_legacy_dataset):
    df = alltypes_sample(size=10000)

    table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(table, buf, compression='SNAPPY', version='2.0')

    buf.seek(0)
    table1 = _read_table(
        buf, use_threads=True, use_legacy_dataset=use_legacy_dataset)

    buf.seek(0)
    table2 = _read_table(
        buf, use_threads=False, use_legacy_dataset=use_legacy_dataset)

    assert table1.equals(table2)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_min_chunksize(use_legacy_dataset):
    data = pd.DataFrame([np.arange(4)], columns=['A', 'B', 'C', 'D'])
    table = pa.Table.from_pandas(data.reset_index())

    buf = io.BytesIO()
    _write_table(table, buf, chunk_size=-1)

    buf.seek(0)
    result = _read_table(buf, use_legacy_dataset=use_legacy_dataset)

    assert result.equals(table)

    with pytest.raises(ValueError):
        _write_table(table, buf, chunk_size=0)


@pytest.mark.pandas
def test_read_single_row_group():
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


@pytest.mark.pandas
def test_read_single_row_group_with_column_subset():
    N, K = 10000, 4
    df = alltypes_sample(size=N)
    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)
    pf = pq.ParquetFile(buf)

    cols = list(df.columns[:2])
    row_groups = [pf.read_row_group(i, columns=cols) for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df[cols], result.to_pandas())

    # ARROW-4267: Selection of duplicate columns still leads to these columns
    # being read uniquely.
    row_groups = [pf.read_row_group(i, columns=cols + cols) for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df[cols], result.to_pandas())


@pytest.mark.pandas
def test_read_multiple_row_groups():
    N, K = 10000, 4
    df = alltypes_sample(size=N)

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)

    pf = pq.ParquetFile(buf)

    assert pf.num_row_groups == K

    result = pf.read_row_groups(range(K))
    tm.assert_frame_equal(df, result.to_pandas())


@pytest.mark.pandas
def test_read_multiple_row_groups_with_column_subset():
    N, K = 10000, 4
    df = alltypes_sample(size=N)
    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)
    pf = pq.ParquetFile(buf)

    cols = list(df.columns[:2])
    result = pf.read_row_groups(range(K), columns=cols)
    tm.assert_frame_equal(df[cols], result.to_pandas())

    # ARROW-4267: Selection of duplicate columns still leads to these columns
    # being read uniquely.
    result = pf.read_row_groups(range(K), columns=cols + cols)
    tm.assert_frame_equal(df[cols], result.to_pandas())


@pytest.mark.pandas
def test_scan_contents():
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


@parametrize_legacy_dataset_fixed
def test_empty_directory(tempdir, use_legacy_dataset):
    # ARROW-5310 - reading empty directory
    # fails with legacy implementation
    empty_dir = tempdir / 'dataset'
    empty_dir.mkdir()

    dataset = pq.ParquetDataset(
        empty_dir, use_legacy_dataset=use_legacy_dataset)
    result = dataset.read()
    assert result.num_rows == 0
    assert result.num_columns == 0



@pytest.mark.pandas
def test_write_error_deletes_incomplete_file(tempdir):
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

    filename = tempdir / 'tmp_file'
    try:
        _write_table(pdf, filename)
    except pa.ArrowException:
        pass

    assert not filename.exists()


@parametrize_legacy_dataset
def test_read_non_existent_file(tempdir, use_legacy_dataset):
    path = 'non-existent-file.parquet'
    try:
        pq.read_table(path, use_legacy_dataset=use_legacy_dataset)
    except Exception as e:
        assert path in e.args[0]


@parametrize_legacy_dataset
def test_read_table_doesnt_warn(datadir, use_legacy_dataset):
    with pytest.warns(None) as record:
        pq.read_table(datadir / 'v0.7.1.parquet',
                      use_legacy_dataset=use_legacy_dataset)

    assert len(record) == 0


@pytest.mark.large_memory
def test_large_table_int32_overflow():
    size = np.iinfo('int32').max + 1

    arr = np.ones(size, dtype='uint8')

    parr = pa.array(arr, type=pa.uint8())

    table = pa.Table.from_arrays([parr], names=['one'])
    f = io.BytesIO()
    _write_table(table, f)


def _simple_table_roundtrip(table, use_legacy_dataset=False, **write_kwargs):
    stream = pa.BufferOutputStream()
    _write_table(table, stream, **write_kwargs)
    buf = stream.getvalue()
    return _read_table(buf, use_legacy_dataset=use_legacy_dataset)


@pytest.mark.large_memory
@parametrize_legacy_dataset
def test_byte_array_exactly_2gb(use_legacy_dataset):
    # Test edge case reported in ARROW-3762
    val = b'x' * (1 << 10)

    base = pa.array([val] * ((1 << 21) - 1))
    cases = [
        [b'x' * 1023],  # 2^31 - 1
        [b'x' * 1024],  # 2^31
        [b'x' * 1025]   # 2^31 + 1
    ]
    for case in cases:
        values = pa.chunked_array([base, pa.array(case)])
        t = pa.table([values], names=['f0'])
        result = _simple_table_roundtrip(
            t, use_legacy_dataset=use_legacy_dataset, use_dictionary=False)
        assert t.equals(result)


@pytest.mark.pandas
@pytest.mark.large_memory
@parametrize_legacy_dataset
def test_binary_array_overflow_to_chunked(use_legacy_dataset):
    # ARROW-3762

    # 2^31 + 1 bytes
    values = [b'x'] + [
        b'x' * (1 << 20)
    ] * 2 * (1 << 10)
    df = pd.DataFrame({'byte_col': values})

    tbl = pa.Table.from_pandas(df, preserve_index=False)
    read_tbl = _simple_table_roundtrip(
        tbl, use_legacy_dataset=use_legacy_dataset)

    col0_data = read_tbl[0]
    assert isinstance(col0_data, pa.ChunkedArray)

    # Split up into 2GB chunks
    assert col0_data.num_chunks == 2

    assert tbl.equals(read_tbl)


@pytest.mark.pandas
@pytest.mark.large_memory
@parametrize_legacy_dataset
def test_list_of_binary_large_cell(use_legacy_dataset):
    # ARROW-4688
    data = []

    # TODO(wesm): handle chunked children
    # 2^31 - 1 bytes in a single cell
    # data.append([b'x' * (1 << 20)] * 2047 + [b'x' * ((1 << 20) - 1)])

    # A little under 2GB in cell each containing approximately 10MB each
    data.extend([[b'x' * 1000000] * 10] * 214)

    arr = pa.array(data)
    table = pa.Table.from_arrays([arr], ['chunky_cells'])
    read_table = _simple_table_roundtrip(
        table, use_legacy_dataset=use_legacy_dataset)
    assert table.equals(read_table)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_parquet_nested_convenience(tempdir, use_legacy_dataset):
    # ARROW-1684
    df = pd.DataFrame({
        'a': [[1, 2, 3], None, [4, 5], []],
        'b': [[1.], None, None, [6., 7.]],
    })

    path = str(tempdir / 'nested_convenience.parquet')

    table = pa.Table.from_pandas(df, preserve_index=False)
    _write_table(table, path)

    read = pq.read_table(
        path, columns=['a'], use_legacy_dataset=use_legacy_dataset)
    tm.assert_frame_equal(read.to_pandas(), df[['a']])

    read = pq.read_table(
        path, columns=['a', 'b'], use_legacy_dataset=use_legacy_dataset)
    tm.assert_frame_equal(read.to_pandas(), df)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_decimal_roundtrip(tempdir, use_legacy_dataset):
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
    filename = tempdir / 'decimals.parquet'
    string_filename = str(filename)
    table = pa.Table.from_pandas(expected)
    _write_table(table, string_filename)
    result_table = _read_table(
        string_filename, use_legacy_dataset=use_legacy_dataset)
    result = result_table.to_pandas()
    tm.assert_frame_equal(result, expected)


@pytest.mark.pandas
@pytest.mark.xfail(
    raises=pa.ArrowException, reason='Parquet does not support negative scale'
)
def test_decimal_roundtrip_negative_scale(tempdir):
    expected = pd.DataFrame({'decimal_num': [decimal.Decimal('1.23E4')]})
    filename = tempdir / 'decimals.parquet'
    string_filename = str(filename)
    t = pa.Table.from_pandas(expected)
    _write_table(t, string_filename)
    result_table = _read_table(string_filename)
    result = result_table.to_pandas()
    tm.assert_frame_equal(result, expected)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_parquet_writer_context_obj(tempdir, use_legacy_dataset):
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

    buf = out.getvalue()
    result = _read_table(
        pa.BufferReader(buf), use_legacy_dataset=use_legacy_dataset)

    expected = pd.concat(frames, ignore_index=True)
    tm.assert_frame_equal(result.to_pandas(), expected)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_parquet_writer_context_obj_with_exception(
    tempdir, use_legacy_dataset
):
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

    buf = out.getvalue()
    result = _read_table(
        pa.BufferReader(buf), use_legacy_dataset=use_legacy_dataset)

    expected = pd.concat(frames, ignore_index=True)
    tm.assert_frame_equal(result.to_pandas(), expected)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_zlib_compression_bug(use_legacy_dataset):
    # ARROW-3514: "zlib deflate failed, output buffer too small"
    table = pa.Table.from_arrays([pa.array(['abc', 'def'])], ['some_col'])
    f = io.BytesIO()
    pq.write_table(table, f, compression='gzip')

    f.seek(0)
    roundtrip = pq.read_table(f, use_legacy_dataset=use_legacy_dataset)
    tm.assert_frame_equal(roundtrip.to_pandas(), table.to_pandas())



def test_empty_row_groups(tempdir):
    # ARROW-3020
    table = pa.Table.from_arrays([pa.array([], type='int32')], ['f0'])

    path = tempdir / 'empty_row_groups.parquet'

    num_groups = 3
    with pq.ParquetWriter(path, table.schema) as writer:
        for i in range(num_groups):
            writer.write_table(table)

    reader = pq.ParquetFile(path)
    assert reader.metadata.num_row_groups == num_groups

    for i in range(num_groups):
        assert reader.read_row_group(i).equals(table)


def test_parquet_file_pass_directory_instead_of_file(tempdir):
    # ARROW-7208
    path = tempdir / 'directory'
    os.mkdir(str(path))

    with pytest.raises(IOError, match="Expected file path"):
        pq.ParquetFile(path)


def test_writing_empty_lists():
    # ARROW-2591: [Python] Segmentation fault issue in pq.write_table
    arr1 = pa.array([[], []], pa.list_(pa.int32()))
    table = pa.Table.from_arrays([arr1], ['list(int32)'])
    _check_roundtrip(table)


@parametrize_legacy_dataset
def test_write_nested_zero_length_array_chunk_failure(use_legacy_dataset):
    # Bug report in ARROW-3792
    cols = OrderedDict(
        int32=pa.int32(),
        list_string=pa.list_(pa.string())
    )
    data = [[], [OrderedDict(int32=1, list_string=('G',)), ]]

    # This produces a table with a column like
    # <Column name='list_string' type=ListType(list<item: string>)>
    # [
    #   [],
    #   [
    #     [
    #       "G"
    #     ]
    #   ]
    # ]
    #
    # Each column is a ChunkedArray with 2 elements
    my_arrays = [pa.array(batch, type=pa.struct(cols)).flatten()
                 for batch in data]
    my_batches = [pa.RecordBatch.from_arrays(batch, schema=pa.schema(cols))
                  for batch in my_arrays]
    tbl = pa.Table.from_batches(my_batches, pa.schema(cols))
    _check_roundtrip(tbl, use_legacy_dataset=use_legacy_dataset)


def test_read_column_invalid_index():
    table = pa.table([pa.array([4, 5]), pa.array(["foo", "bar"])],
                     names=['ints', 'strs'])
    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)
    f = pq.ParquetFile(bio.getvalue())
    assert f.reader.read_column(0).to_pylist() == [4, 5]
    assert f.reader.read_column(1).to_pylist() == ["foo", "bar"]
    for index in (-1, 2):
        with pytest.raises((ValueError, IndexError)):
            f.reader.read_column(index)



@parametrize_legacy_dataset
def test_parquet_file_too_small(tempdir, use_legacy_dataset):
    path = str(tempdir / "test.parquet")
    # TODO(dataset) with datasets API it raises OSError instead
    with pytest.raises((pa.ArrowInvalid, OSError),
                       match='size is 0 bytes'):
        with open(path, 'wb') as f:
            pass
        pq.read_table(path, use_legacy_dataset=use_legacy_dataset)

    with pytest.raises((pa.ArrowInvalid, OSError),
                       match='size is 4 bytes'):
        with open(path, 'wb') as f:
            f.write(b'ffff')
        pq.read_table(path, use_legacy_dataset=use_legacy_dataset)




@parametrize_legacy_dataset
@pytest.mark.pandas
def test_filter_before_validate_schema(tempdir, use_legacy_dataset):
    # ARROW-4076 apply filter before schema validation
    # to avoid checking unneeded schemas

    # create partitioned dataset with mismatching schemas which would
    # otherwise raise if first validation all schemas
    dir1 = tempdir / 'A=0'
    dir1.mkdir()
    table1 = pa.Table.from_pandas(pd.DataFrame({'B': [1, 2, 3]}))
    pq.write_table(table1, dir1 / 'data.parquet')

    dir2 = tempdir / 'A=1'
    dir2.mkdir()
    table2 = pa.Table.from_pandas(pd.DataFrame({'B': ['a', 'b', 'c']}))
    pq.write_table(table2, dir2 / 'data.parquet')

    # read single file using filter
    table = pq.read_table(tempdir, filters=[[('A', '==', 0)]],
                          use_legacy_dataset=use_legacy_dataset)
    assert table.column('B').equals(pa.chunked_array([[1, 2, 3]]))


@pytest.mark.pandas
@pytest.mark.fastparquet
@pytest.mark.filterwarnings("ignore:RangeIndex:FutureWarning")
@pytest.mark.filterwarnings("ignore:tostring:DeprecationWarning:fastparquet")
def test_fastparquet_cross_compatibility(tempdir):
    fp = pytest.importorskip('fastparquet')

    df = pd.DataFrame(
        {
            "a": list("abc"),
            "b": list(range(1, 4)),
            "c": np.arange(4.0, 7.0, dtype="float64"),
            "d": [True, False, True],
            "e": pd.date_range("20130101", periods=3),
            "f": pd.Categorical(["a", "b", "a"]),
            # fastparquet writes list as BYTE_ARRAY JSON, so no roundtrip
            # "g": [[1, 2], None, [1, 2, 3]],
        }
    )
    table = pa.table(df)

    # Arrow -> fastparquet
    file_arrow = str(tempdir / "cross_compat_arrow.parquet")
    pq.write_table(table, file_arrow, compression=None)

    fp_file = fp.ParquetFile(file_arrow)
    df_fp = fp_file.to_pandas()
    tm.assert_frame_equal(df, df_fp)

    # Fastparquet -> arrow
    file_fastparquet = str(tempdir / "cross_compat_fastparquet.parquet")
    fp.write(file_fastparquet, df)

    table_fp = pq.read_pandas(file_fastparquet)
    # for fastparquet written file, categoricals comes back as strings
    # (no arrow schema in parquet metadata)
    df['f'] = df['f'].astype(object)
    tm.assert_frame_equal(table_fp.to_pandas(), df)


@parametrize_legacy_dataset
@pytest.mark.parametrize('array_factory', [
    lambda: pa.array([0, None] * 10),
    lambda: pa.array([0, None] * 10).dictionary_encode(),
    lambda: pa.array(["", None] * 10),
    lambda: pa.array(["", None] * 10).dictionary_encode(),
])
@pytest.mark.parametrize('use_dictionary', [False, True])
@pytest.mark.parametrize('read_dictionary', [False, True])
def test_buffer_contents(
        array_factory, use_dictionary, read_dictionary, use_legacy_dataset
):
    # Test that null values are deterministically initialized to zero
    # after a roundtrip through Parquet.
    # See ARROW-8006 and ARROW-8011.
    orig_table = pa.Table.from_pydict({"col": array_factory()})
    bio = io.BytesIO()
    pq.write_table(orig_table, bio, use_dictionary=True)
    bio.seek(0)
    read_dictionary = ['col'] if read_dictionary else None
    table = pq.read_table(bio, use_threads=False,
                          read_dictionary=read_dictionary,
                          use_legacy_dataset=use_legacy_dataset)

    for col in table.columns:
        [chunk] = col.chunks
        buf = chunk.buffers()[1]
        assert buf.to_pybytes() == buf.size * b"\0"


def test_parquet_compression_roundtrip(tempdir):
    # ARROW-10480: ensure even with nonstandard Parquet file naming
    # conventions, writing and then reading a file works. In
    # particular, ensure that we don't automatically double-compress
    # the stream due to auto-detecting the extension in the filename
    table = pa.table([pa.array(range(4))], names=["ints"])
    path = tempdir / "arrow-10480.pyarrow.gz"
    pq.write_table(table, path, compression="GZIP")
    result = pq.read_table(path)
    assert result.equals(table)

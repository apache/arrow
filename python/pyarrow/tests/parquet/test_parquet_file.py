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
import os

import pytest

import pyarrow as pa

try:
    import pyarrow.parquet as pq
    from pyarrow.tests.parquet.common import _write_table
except ImportError:
    pq = None

try:
    import pandas as pd
    import pandas.testing as tm

    from pyarrow.tests.parquet.common import alltypes_sample
except ImportError:
    pd = tm = None

pytestmark = pytest.mark.parquet


@pytest.mark.pandas
def test_pass_separate_metadata():
    # ARROW-471
    df = alltypes_sample(size=10000)

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, compression='snappy', version='2.6')

    buf.seek(0)
    metadata = pq.read_metadata(buf)

    buf.seek(0)

    fileh = pq.ParquetFile(buf, metadata=metadata)

    tm.assert_frame_equal(df, fileh.read().to_pandas())


@pytest.mark.pandas
def test_read_single_row_group():
    # ARROW-471
    N, K = 10000, 4
    df = alltypes_sample(size=N)

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.6')

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
                 compression='snappy', version='2.6')

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
                 compression='snappy', version='2.6')

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
                 compression='snappy', version='2.6')

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
                 compression='snappy', version='2.6')

    buf.seek(0)
    pf = pq.ParquetFile(buf)

    assert pf.scan_contents() == 10000
    assert pf.scan_contents(df.columns[:4]) == 10000


def test_parquet_file_pass_directory_instead_of_file(tempdir):
    # ARROW-7208
    path = tempdir / 'directory'
    os.mkdir(str(path))

    with pytest.raises(IOError, match="Expected file path"):
        pq.ParquetFile(path)


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


@pytest.mark.pandas
@pytest.mark.parametrize('batch_size', [300, 1000, 1300])
def test_iter_batches_columns_reader(tempdir, batch_size):
    total_size = 3000
    chunk_size = 1000
    # TODO: Add categorical support
    df = alltypes_sample(size=total_size)

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df)
    _write_table(arrow_table, filename, version='2.6',
                 coerce_timestamps='ms', chunk_size=chunk_size)

    file_ = pq.ParquetFile(filename)
    for columns in [df.columns[:10], df.columns[10:]]:
        batches = file_.iter_batches(batch_size=batch_size, columns=columns)
        batch_starts = range(0, total_size+batch_size, batch_size)
        for batch, start in zip(batches, batch_starts):
            end = min(total_size, start + batch_size)
            tm.assert_frame_equal(
                batch.to_pandas(),
                df.iloc[start:end, :].loc[:, columns].reset_index(drop=True)
            )


@pytest.mark.pandas
@pytest.mark.parametrize('chunk_size', [1000])
def test_iter_batches_reader(tempdir, chunk_size):
    df = alltypes_sample(size=10000, categorical=True)

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df)
    assert arrow_table.schema.pandas_metadata is not None

    _write_table(arrow_table, filename, version='2.6',
                 coerce_timestamps='ms', chunk_size=chunk_size)

    file_ = pq.ParquetFile(filename)

    def get_all_batches(f):
        for row_group in range(f.num_row_groups):
            batches = f.iter_batches(
                batch_size=900,
                row_groups=[row_group],
            )

            for batch in batches:
                yield batch

    batches = list(get_all_batches(file_))
    batch_no = 0

    for i in range(file_.num_row_groups):
        tm.assert_frame_equal(
            batches[batch_no].to_pandas(),
            file_.read_row_groups([i]).to_pandas().head(900)
        )

        batch_no += 1

        tm.assert_frame_equal(
            batches[batch_no].to_pandas().reset_index(drop=True),
            file_.read_row_groups([i]).to_pandas().iloc[900:].reset_index(
                drop=True
            )
        )

        batch_no += 1


@pytest.mark.pandas
@pytest.mark.parametrize('pre_buffer', [False, True])
def test_pre_buffer(pre_buffer):
    N, K = 10000, 4
    df = alltypes_sample(size=N)
    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.6')

    buf.seek(0)
    pf = pq.ParquetFile(buf, pre_buffer=pre_buffer)
    assert pf.read().num_rows == N


def test_column_selection(tempdir):
    # create a table with nested types
    inner = pa.field('inner', pa.int64())
    middle = pa.field('middle', pa.struct([inner]))
    fields = [
        pa.field('basic', pa.int32()),
        pa.field(
            'list', pa.list_(pa.field('item', pa.int32()))
        ),
        pa.field(
            'struct', pa.struct([middle, pa.field('inner2', pa.int64())])
        ),
        pa.field(
            'list-struct', pa.list_(pa.field(
                'item', pa.struct([
                    pa.field('inner1', pa.int64()),
                    pa.field('inner2', pa.int64())
                ])
            ))
        ),
        pa.field('basic2', pa.int64()),
    ]
    arrs = [
        [0], [[1, 2]], [{"middle": {"inner": 3}, "inner2": 4}],
        [[{"inner1": 5, "inner2": 6}, {"inner1": 7, "inner2": 8}]], [9]]
    table = pa.table(arrs, schema=pa.schema(fields))

    path = str(tempdir / 'test.parquet')
    pq.write_table(table, path)
    pf = pq.ParquetFile(path)

    # default selecting all columns
    result1 = pf.read()
    assert result1.equals(table)

    # selecting with columns names
    result2 = pf.read(columns=["basic", "basic2"])
    assert result2.equals(table.select(["basic", "basic2"]))

    result3 = pf.read(columns=["list", "struct", "basic2"])
    assert result3.equals(table.select(["list", "struct", "basic2"]))

    # using dotted paths
    result4 = pf.read(columns=["struct.middle.inner"])
    expected4 = pa.table({"struct": [{"middle": {"inner": 3}}]})
    assert result4.equals(expected4)

    result5 = pf.read(columns=["struct.inner2"])
    expected5 = pa.table({"struct": [{"inner2": 4}]})
    assert result5.equals(expected5)

    result6 = pf.read(
        columns=["list", "struct.middle.inner", "struct.inner2"]
    )
    assert result6.equals(table.select(["list", "struct"]))

    # dotted paths for lists with or without list.item
    result7 = pf.read(columns=["list-struct.list.item.inner1"])
    expected7 = pa.table({"list-struct": [[{"inner1": 5}, {"inner1": 7}]]})
    assert result7.equals(expected7)

    result7b = pf.read(columns=["list-struct.inner1"])
    assert result7b.equals(expected7)

    # empty table on non-existing name
    # TODO is this the behaviour we want?
    empty = pf.read(columns=["wrong"])
    assert empty.num_columns == 0
    assert empty.num_rows == 1

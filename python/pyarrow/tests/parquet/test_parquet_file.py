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


@pytest.mark.pandas
def test_pass_separate_metadata():
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

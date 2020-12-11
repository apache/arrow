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

import pytest

import pyarrow as pa
from pyarrow import fs
from pyarrow.filesystem import FileSystem, LocalFileSystem
from pyarrow.tests.parquet.common import parametrize_legacy_dataset

try:
    import pyarrow.parquet as pq
    from pyarrow.tests.parquet.common import _read_table, _test_dataframe
except ImportError:
    pq = None


try:
    import pandas as pd
    import pandas.testing as tm

except ImportError:
    pd = tm = None


@pytest.mark.pandas
@pytest.mark.parametrize("filesystem", [
    None,
    LocalFileSystem._get_instance(),
    fs.LocalFileSystem(),
])
def test_parquet_writer_filesystem_local(tempdir, filesystem):
    df = _test_dataframe(100)
    table = pa.Table.from_pandas(df, preserve_index=False)
    path = str(tempdir / 'data.parquet')

    with pq.ParquetWriter(
        path, table.schema, filesystem=filesystem, version='2.0'
    ) as writer:
        writer.write_table(table)

    result = _read_table(path).to_pandas()
    tm.assert_frame_equal(result, df)


@pytest.fixture
def s3_example_fs(s3_connection, s3_server):
    from pyarrow.fs import FileSystem

    host, port, access_key, secret_key = s3_connection
    uri = (
        "s3://{}:{}@mybucket/data.parquet?scheme=http&endpoint_override={}:{}"
        .format(access_key, secret_key, host, port)
    )
    fs, path = FileSystem.from_uri(uri)

    fs.create_dir("mybucket")

    yield fs, uri, path


@pytest.mark.pandas
@pytest.mark.s3
def test_parquet_writer_filesystem_s3(s3_example_fs):
    df = _test_dataframe(100)
    table = pa.Table.from_pandas(df, preserve_index=False)

    fs, uri, path = s3_example_fs

    with pq.ParquetWriter(
        path, table.schema, filesystem=fs, version='2.0'
    ) as writer:
        writer.write_table(table)

    result = _read_table(uri).to_pandas()
    tm.assert_frame_equal(result, df)


@pytest.mark.pandas
@pytest.mark.s3
def test_parquet_writer_filesystem_s3_uri(s3_example_fs):
    df = _test_dataframe(100)
    table = pa.Table.from_pandas(df, preserve_index=False)

    fs, uri, path = s3_example_fs

    with pq.ParquetWriter(uri, table.schema, version='2.0') as writer:
        writer.write_table(table)

    result = _read_table(path, filesystem=fs).to_pandas()
    tm.assert_frame_equal(result, df)


@pytest.mark.pandas
def test_parquet_writer_filesystem_s3fs(s3_example_s3fs):
    df = _test_dataframe(100)
    table = pa.Table.from_pandas(df, preserve_index=False)

    fs, directory = s3_example_s3fs
    path = directory + "/test.parquet"

    with pq.ParquetWriter(
        path, table.schema, filesystem=fs, version='2.0'
    ) as writer:
        writer.write_table(table)

    result = _read_table(path, filesystem=fs).to_pandas()
    tm.assert_frame_equal(result, df)


@pytest.mark.pandas
def test_parquet_writer_filesystem_buffer_raises():
    df = _test_dataframe(100)
    table = pa.Table.from_pandas(df, preserve_index=False)
    filesystem = fs.LocalFileSystem()

    # Should raise ValueError when filesystem is passed with file-like object
    with pytest.raises(ValueError, match="specified path is file-like"):
        pq.ParquetWriter(
            pa.BufferOutputStream(), table.schema, filesystem=filesystem
        )


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_parquet_writer_with_caller_provided_filesystem(use_legacy_dataset):
    out = pa.BufferOutputStream()

    class CustomFS(FileSystem):
        def __init__(self):
            self.path = None
            self.mode = None

        def open(self, path, mode='rb'):
            self.path = path
            self.mode = mode
            return out

    fs = CustomFS()
    fname = 'expected_fname.parquet'
    df = _test_dataframe(100)
    table = pa.Table.from_pandas(df, preserve_index=False)

    with pq.ParquetWriter(fname, table.schema, filesystem=fs, version='2.0') \
            as writer:
        writer.write_table(table)

    assert fs.path == fname
    assert fs.mode == 'wb'
    assert out.closed

    buf = out.getvalue()
    table_read = _read_table(
        pa.BufferReader(buf), use_legacy_dataset=use_legacy_dataset)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df_read, df)

    # Should raise ValueError when filesystem is passed with file-like object
    with pytest.raises(ValueError) as err_info:
        pq.ParquetWriter(pa.BufferOutputStream(), table.schema, filesystem=fs)
        expected_msg = ("filesystem passed but where is file-like, so"
                        " there is nothing to open with filesystem.")
        assert str(err_info) == expected_msg

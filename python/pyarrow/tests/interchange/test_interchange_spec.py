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

import ctypes

import pyarrow as pa
import pytest


@pytest.mark.parametrize(
    "test_data",
    [
        {"a": ["foo", "bar"], "b": ["baz", "qux"]},
        {"a": [1.5, 2.5, 3.5], "b": [9.2, 10.5, 11.8]},
        {"A": [1, 2, 3, 4], "B": [1, 2, 3, 4]},
    ],
)
def test_only_one_dtype(test_data):
    columns = list(test_data.keys())
    table = pa.table(test_data)
    df = table.__dataframe__()

    column_size = len(test_data[columns[0]])
    for column in columns:
        null_count = df.get_column_by_name(column).null_count
        assert null_count == 0
        assert isinstance(null_count, int)
        assert df.get_column_by_name(column).size() == column_size
        assert df.get_column_by_name(column).offset == 0


def test_mixed_dtypes():
    table = pa.table(
        {
            "a": [1, 2, 3],  # dtype kind INT = 0
            "b": [3, 4, 5],  # dtype kind INT = 0
            "c": [1.5, 2.5, 3.5],  # dtype kind FLOAT = 2
            "d": [9, 10, 11],  # dtype kind INT = 0
            "e": [True, False, True],  # dtype kind BOOLEAN = 20
            "f": ["a", "", "c"],  # dtype kind STRING = 21
        }
    )
    df = table.__dataframe__()
    # for meanings of dtype[0] see the spec; we cannot import the
    # spec here as this file is expected to be vendored *anywhere*;
    # values for dtype[0] are explained above
    columns = {"a": 0, "b": 0, "c": 2, "d": 0, "e": 20, "f": 21}

    for column, kind in columns.items():
        col = df.get_column_by_name(column)
        assert col.null_count == 0
        assert isinstance(col.null_count, int)
        assert col.size() == 3
        assert col.offset == 0

        assert col.dtype[0] == kind

    assert df.get_column_by_name("c").dtype[1] == 64


def test_na_float():
    table = pa.table({"a": [1.0, None, 2.0]})
    df = table.__dataframe__()
    col = df.get_column_by_name("a")
    assert col.null_count == 1
    assert isinstance(col.null_count, int)


def test_noncategorical():
    table = pa.table({"a": [1, 2, 3]})
    df = table.__dataframe__()
    col = df.get_column_by_name("a")
    with pytest.raises(TypeError, match=".*categorical.*"):
        col.describe_categorical


def test_categorical():
    import pyarrow as pa
    arr = ["Mon", "Tue", "Mon", "Wed", "Mon", "Thu", "Fri", "Sat", "Sun"]
    table = pa.table(
        {"weekday": pa.array(arr).dictionary_encode()}
    )

    col = table.__dataframe__().get_column_by_name("weekday")
    categorical = col.describe_categorical
    assert isinstance(categorical["is_ordered"], bool)
    assert isinstance(categorical["is_dictionary"], bool)


def test_dataframe():
    n = pa.chunked_array([[2, 2, 4], [4, 5, 100]])
    a = pa.chunked_array([["Flamingo", "Parrot", "Cow"],
                         ["Horse", "Brittle stars", "Centipede"]])
    table = pa.table([n, a], names=['n_legs', 'animals'])
    df = table.__dataframe__()

    assert df.num_columns() == 2
    assert df.num_rows() == 6
    assert df.num_chunks() == 2
    assert list(df.column_names()) == ['n_legs', 'animals']
    assert list(df.select_columns((1,)).column_names()) == list(
        df.select_columns_by_name(("animals",)).column_names()
    )


@pytest.mark.parametrize(["size", "n_chunks"], [(10, 3), (12, 3), (12, 5)])
def test_df_get_chunks(size, n_chunks):
    table = pa.table({"x": list(range(size))})
    df = table.__dataframe__()
    chunks = list(df.get_chunks(n_chunks))
    assert len(chunks) == n_chunks
    assert sum(chunk.num_rows() for chunk in chunks) == size


@pytest.mark.parametrize(["size", "n_chunks"], [(10, 3), (12, 3), (12, 5)])
def test_column_get_chunks(size, n_chunks):
    table = pa.table({"x": list(range(size))})
    df = table.__dataframe__()
    chunks = list(df.get_column(0).get_chunks(n_chunks))
    assert len(chunks) == n_chunks
    assert sum(chunk.size() for chunk in chunks) == size


def test_get_columns():
    table = pa.table({"a": [0, 1], "b": [2.5, 3.5]})
    df = table.__dataframe__()
    for col in df.get_columns():
        assert col.size() == 2
        assert col.num_chunks() == 1
    # for meanings of dtype[0] see the spec; we cannot import the
    # spec here as this file is expected to be vendored *anywhere*
    assert df.get_column(0).dtype[0] == 0  # INT
    assert df.get_column(1).dtype[0] == 2  # FLOAT


def test_buffer():
    arr = [0, 1, -1]
    table = pa.table({"a": arr})
    df = table.__dataframe__()
    col = df.get_column(0)
    buf = col.get_buffers()

    dataBuf, dataDtype = buf["data"]

    assert dataBuf.bufsize > 0
    assert dataBuf.ptr != 0
    device, _ = dataBuf.__dlpack_device__()

    # for meanings of dtype[0] see the spec; we cannot import the spec
    # here as this file is expected to be vendored *anywhere*
    assert dataDtype[0] == 0  # INT

    if device == 1:  # CPU-only as we're going to directly read memory here
        bitwidth = dataDtype[1]
        ctype = {
            8: ctypes.c_int8,
            16: ctypes.c_int16,
            32: ctypes.c_int32,
            64: ctypes.c_int64,
        }[bitwidth]

        for idx, truth in enumerate(arr):
            val = ctype.from_address(dataBuf.ptr + idx * (bitwidth // 8)).value
            assert val == truth, f"Buffer at index {idx} mismatch"

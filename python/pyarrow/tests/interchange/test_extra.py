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

from datetime import datetime as dt
import numpy as np
import pyarrow as pa
from pyarrow.vendored.version import Version
import pytest

import pyarrow.interchange as pi
from pyarrow.interchange.column import (
    _PyArrowColumn,
    ColumnNullType,
    DtypeKind,
)

try:
    import pandas as pd
    # import pandas.testing as tm
except ImportError:
    pass


def test_datetime():
    df = pd.DataFrame({"A": [dt(2007, 7, 13), None]})
    table = pa.table(df)
    col = table.__dataframe__().get_column_by_name("A")

    assert col.size == 2
    assert col.offset == 0
    assert col.null_count == 1
    assert col.dtype[0] == DtypeKind.DATETIME
    assert col.describe_null == (ColumnNullType.USE_BITMASK, 0)


@pytest.mark.parametrize(
    ["test_data", "kind"],
    [
        (["foo", "bar"], 21),
        ([1.5, 2.5, 3.5], 2),
        ([1, 2, 3, 4], 0),
    ],
)
def test_array_to_pyarrowcolumn(test_data, kind):
    arr = pa.array(test_data)
    arr_column = _PyArrowColumn(arr)

    assert arr_column._col == arr
    assert arr_column.size == len(test_data)
    assert arr_column.dtype[0] == kind
    assert arr_column.num_chunks() == 1
    assert arr_column.null_count == 0
    assert arr_column.get_buffers()["validity"] is None
    assert len(list(arr_column.get_chunks())) == 1

    for chunk in arr_column.get_chunks():
        assert chunk == arr_column


@pytest.mark.pandas
def test_offset_of_sliced_array():
    # if Version(pd.__version__) < Version("1.5.0"):
    #     pytest.skip("__dataframe__ added to pandas in 1.5.0")

    # from pandas.core.interchange.from_dataframe import (
    #     from_dataframe as pandas_from_dataframe
    # )

    arr = pa.array([1, 2, 3, 4])
    arr_sliced = arr.slice(2, 2)

    # table = pa.table([arr], names=["arr"])
    table_sliced = pa.table([arr_sliced], names=["arr_sliced"])

    col = table_sliced.__dataframe__().get_column(0)
    assert col.offset == 2

    # TODO: Investigate why this part is not working currently!
    # df = pandas_from_dataframe(table)
    # df_sliced = pandas_from_dataframe(table_sliced)

    # tm.assert_series_equal(df["arr"][2:4], df_sliced["arr_sliced"],
    #                        check_index=False, check_names=False)


@pytest.mark.pandas
def test_categorical_roundtrip():
    if Version(pd.__version__) < Version("1.5.0"):
        pytest.skip("__dataframe__ added to pandas in 1.5.0")
    arr = ["Mon", "Tue", "Mon", "Wed", "Mon", "Thu", "Fri", "Sat", "Sun"]
    table = pa.table(
        {"weekday": pa.array(arr).dictionary_encode()}
    )

    pandas_df = table.to_pandas()
    result = pi.from_dataframe(pandas_df)

    # Checking equality for the values
    # As the dtype of the indices is changed from int32 in pa.Table
    # to int64 in pandas interchange protocol implementation
    assert result[0].chunk(0).dictionary == table[0].chunk(0).dictionary

    table_protocol = table.__dataframe__()
    result_protocol = result.__dataframe__()

    assert table_protocol.num_columns() == result_protocol.num_columns()
    assert table_protocol.num_rows() == result_protocol.num_rows()
    assert table_protocol.num_chunks() == result_protocol.num_chunks()
    assert table_protocol.column_names() == result_protocol.column_names()

    col_table = table_protocol.get_column(0)
    col_result = result_protocol.get_column(0)

    assert col_result.dtype[0] == DtypeKind.CATEGORICAL
    assert col_result.dtype[0] == col_table.dtype[0]
    assert col_result.size == col_table.size
    assert col_result.offset == col_table.offset

    desc_cat_table = col_result.describe_categorical
    desc_cat_result = col_result.describe_categorical

    assert desc_cat_table["is_ordered"] == desc_cat_result["is_ordered"]
    assert desc_cat_table["is_dictionary"] == desc_cat_result["is_dictionary"]
    assert isinstance(desc_cat_result["categories"]._col, pa.Array)


@pytest.mark.pandas
@pytest.mark.parametrize(
    "uint", [pa.uint8(), pa.uint16(), pa.uint32()]
)
@pytest.mark.parametrize(
    "int", [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
)
@pytest.mark.parametrize(
    "float, np_float", [
        (pa.float16(), np.float16),
        (pa.float32(), np.float32),
        (pa.float64(), np.float64)
    ]
)
def test_pandas_roundtrip(uint, int, float, np_float):
    if Version(pd.__version__) < Version("1.5.0"):
        pytest.skip("__dataframe__ added to pandas in 1.5.0")
    from datetime import datetime as dt

    arr = [1, 2, 3]
    dt_arr = [dt(2007, 7, 13), dt(2007, 7, 14), dt(2007, 7, 15)]
    table = pa.table(
        {
            "a": pa.array(arr, type=uint),
            "b": pa.array(arr, type=int),
            "c": pa.array(np.array(arr, dtype=np_float), type=float),
            "d": [True, False, True],
            "e": ["a", "", "c"],
            # pandas seems to only support datetime64 in "us" resolution(?)
            # without time zone
            "f": pa.array(dt_arr, type=pa.timestamp('us'))
        }
    )

    pandas_df = table.to_pandas()
    result = pi.from_dataframe(pandas_df)
    assert table.equals(result)

    table_protocol = table.__dataframe__()
    result_protocol = result.__dataframe__()

    assert table_protocol.num_columns() == result_protocol.num_columns()
    assert table_protocol.num_rows() == result_protocol.num_rows()
    assert table_protocol.num_chunks() == result_protocol.num_chunks()
    assert table_protocol.column_names() == result_protocol.column_names()


@pytest.mark.pandas
@pytest.mark.large_memory
def test_large_string():
    if Version(pd.__version__) < Version("1.5.0"):
        pytest.skip("__dataframe__ added to pandas in 1.5.0")

    from pandas.core.interchange.from_dataframe import (
        from_dataframe as pandas_from_dataframe
    )

    data = np.array([b'x'*1024]*(3*1024**2), dtype='object')  # 3GB bytes data
    arr = pa.array(data, type=pa.large_string())
    table = pa.table([arr], names=["large_string"])
    result = pandas_from_dataframe(table)

    assert result["large_string"][0] == data[0].decode()
    assert result.size == 3*1024**2

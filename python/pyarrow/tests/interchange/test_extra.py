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
import pyarrow as pa
import pytest

from pyarrow.interchange.column import (
    _PyArrowColumn,
    ColumnNullType,
    DtypeKind,
)

try:
    import pandas as pd
    import pandas.testing as tm
    from pandas.core.interchange.from_dataframe import from_dataframe
except ImportError:
    pass


def test_datetime():
    df = pd.DataFrame({"A": [dt(2007, 7, 13), None]})
    table = pa.table(df)
    col = table.__dataframe__().get_column_by_name("A")

    assert col.size == 2
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
    arr = pa.array([1, 2, 3, 4])
    arr_sliced = arr.slice(2, 2)

    table = pa.table([arr], names=["arr"])
    table_sliced = pa.table([arr_sliced], names=["arr_sliced"])

    df = from_dataframe(table)
    df_sliced = from_dataframe(table_sliced)

    tm.assert_series_equal(df["arr"][2:4], df_sliced["arr_sliced"],
                           check_index=False, check_names=False)

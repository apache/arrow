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

import pyarrow as pa
import pytest

@pytest.mark.parametrize(
    "test_data",
    [
        {"a": ["foo", "bar"], "b": ["baz", "qux"]},
        {"a": [1.5, 2.5, 3.5], "b": [9.2, 10.5, 11.8]},
        {"A": [1, 2, 3, 4], "B": [1, 2, 3, 4]},
    ],
    ids=["str_data", "float_data", "int_data"],
)
def test_only_one_dtype(test_data):
    columns = list(test_data.keys())
    table = pa.Table.from_pylist([test_data])
    df = table.__dataframe__()

    column_size = len(test_data[columns[0]])
    for column in columns:
        null_count = df.get_column_by_name(column).null_count
        assert null_count == 0
        assert isinstance(null_count, int)
        assert df.get_column_by_name(column).size() == column_size
        assert df.get_column_by_name(column).offset == 0


def test_dataframe():
    n = pa.chunked_array([[2, 2, 4], [4, 5, 100]])
    a = pa.chunked_array([["Flamingo", "Parrot", "Cow"],
                         ["Horse", "Brittle stars", "Centipede"]])
    table = pa.Table.from_arrays([n, a], names=['n_legs', 'animals'])
    df = table.__dataframe__()

    assert df.num_columns() == 2
    assert df.num_rows() == 6
    assert df.num_chunks() == 2
    assert list(df.column_names()) == ['n_legs', 'animals']
    assert list(df.select_columns((1,)).column_names()) == list(
        df.select_columns_by_name(("animals",)).column_names()
    )

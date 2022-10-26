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
    assert list(df.select_columns([1]).column_names()) == list(
        df.select_columns_by_name(["animals"]).column_names()
    )

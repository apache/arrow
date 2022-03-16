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
import pyarrow._exec_plan as ep


def test_joins_corner_cases():
    t1 = pa.Table.from_pydict({
        "colA": [1, 2, 3, 4, 5, 6],
        "col2": ["a", "b", "c", "d", "e", "f"]
    })

    t2 = pa.Table.from_pydict({
        "colB": [1, 2, 3, 4, 5],
        "col3": ["A", "B", "C", "D", "E"]
    })

    with pytest.raises(pa.ArrowInvalid):
        ep.tables_join("left outer", t1, "", t2, "")

    with pytest.raises(TypeError):
        ep.tables_join("left outer", None, "colA", t2, "colB")

    with pytest.raises(ValueError):
        ep.tables_join("super mario join", t1, "colA", t2, "colB")


@pytest.mark.parametrize("jointype,expected", [
    ("left semi", {
        "colA": [1, 2],
        "col2": ["a", "b"]
    }),
    ("right semi", {
        "colB": [2, 1],
        "col3": ["B", "A"]
    }),
    ("left anti", {
        "colA": [6],
        "col2": ["f"]
    }),
    ("right anti", {
        "colB": [99],
        "col3": ["Z"]
    }),
    ("inner", {
        "colA": [1, 2],
        "col2": ["a", "b"],
        "col3": ["A", "B"]
    }),
    ("left outer", {
        "colA": [1, 2, 6],
        "col2": ["a", "b", "f"],
        "col3": ["A", "B", None]
    }),
    ("right outer", {
        "col2": ["a", "b", None],
        "colB": [1, 2, 99],
        "col3": ["A", "B", "Z"]
    }),
    ("full outer", {
        "colA": [1, 2, 6, None],
        "col2": ["a", "b", "f", None],
        "colB": [1, 2, None, 99],
        "col3": ["A", "B", None, "Z"]
    })
])
@pytest.mark.parametrize("use_threads", [True, False])
def test_joins(jointype, expected, use_threads):
    # Allocate table here instead of using parametrize
    # this prevents having arrow allocated memory forever around.
    expected = pa.table(expected)

    t1 = pa.Table.from_pydict({
        "colA": [1, 2, 6],
        "col2": ["a", "b", "f"]
    })

    t2 = pa.Table.from_pydict({
        "colB": [99, 2, 1],
        "col3": ["Z", "B", "A"]
    })

    r = ep.tables_join(jointype, t1, "colA", t2, "colB",
                       use_threads=use_threads)
    assert r == expected

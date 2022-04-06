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

try:
    import pyarrow.dataset as ds
    import pyarrow._exec_plan as ep
except ImportError:
    pass

pytestmark = pytest.mark.dataset


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
        ep._perform_join("left outer", t1, "", t2, "")

    with pytest.raises(TypeError):
        ep._perform_join("left outer", None, "colA", t2, "colB")

    with pytest.raises(ValueError):
        ep._perform_join("super mario join", t1, "colA", t2, "colB")


@pytest.mark.parametrize("jointype,expected", [
    ("left semi", {
        "colA": [1, 2],
        "col2": ["a", "b"]
    }),
    ("right semi", {
        "colB": [1, 2],
        "col3": ["A", "B"]
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
        "colA": [1, 2, 6, 99],
        "col2": ["a", "b", "f", None],
        "col3": ["A", "B", None, "Z"]
    })
])
@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("use_datasets", [False, True])
def test_joins(jointype, expected, use_threads, use_datasets):
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

    if use_datasets:
        t1 = ds.dataset([t1])
        t2 = ds.dataset([t2])

    r = ep._perform_join(jointype, t1, "colA", t2, "colB",
                         use_threads=use_threads, coalesce_keys=True)
    r = r.combine_chunks()
    if "right" in jointype:
        r = r.sort_by("colB")
    else:
        r = r.sort_by("colA")
    assert r == expected


def test_table_join_collisions():
    t1 = pa.table({
        "colA": [1, 2, 6],
        "colB": [10, 20, 60],
        "colVals": ["a", "b", "f"]
    })

    t2 = pa.table({
        "colB": [99, 20, 10],
        "colVals": ["Z", "B", "A"],
        "colUniq": [100, 200, 300],
        "colA": [99, 2, 1],
    })

    result = ep._perform_join(
        "full outer", t1, ["colA", "colB"], t2, ["colA", "colB"])
    assert result.combine_chunks() == pa.table([
        [1, 2, 6, None],
        [10, 20, 60, None],
        ["a", "b", "f", None],
        [10, 20, None, 99],
        ["A", "B", None, "Z"],
        [300, 200, None, 100],
        [1, 2, None, 99],
    ], names=["colA", "colB", "colVals", "colB", "colVals", "colUniq", "colA"])

    result = ep._perform_join("full outer", t1, "colA",
                              t2, "colA", right_suffix="_r",
                              coalesce_keys=False)
    assert result.combine_chunks() == pa.table({
        "colA": [1, 2, 6, None],
        "colB": [10, 20, 60, None],
        "colVals": ["a", "b", "f", None],
        "colB_r": [10, 20, None, 99],
        "colVals_r": ["A", "B", None, "Z"],
        "colUniq": [300, 200, None, 100],
        "colA_r": [1, 2, None, 99],
    })

    result = ep._perform_join("full outer", t1, "colA",
                              t2, "colA", right_suffix="_r",
                              coalesce_keys=True)
    assert result.combine_chunks() == pa.table({
        "colA": [1, 2, 6, 99],
        "colB": [10, 20, 60, None],
        "colVals": ["a", "b", "f", None],
        "colB_r": [10, 20, None, 99],
        "colVals_r": ["A", "B", None, "Z"],
        "colUniq": [300, 200, None, 100]
    })


def test_table_join_keys_order():
    t1 = pa.table({
        "colB": [10, 20, 60],
        "colA": [1, 2, 6],
        "colVals": ["a", "b", "f"]
    })

    t2 = pa.table({
        "colVals": ["Z", "B", "A"],
        "colX": [99, 2, 1],
    })

    result = ep._perform_join("full outer", t1, "colA", t2, "colX",
                              left_suffix="_l", right_suffix="_r",
                              coalesce_keys=True)
    assert result.combine_chunks() == pa.table({
        "colB": [10, 20, 60, None],
        "colA": [1, 2, 6, 99],
        "colVals_l": ["a", "b", "f", None],
        "colVals_r": ["A", "B", None, "Z"],
    })

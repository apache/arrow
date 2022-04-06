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

from typing import List

import pytest

import pyarrow as pa
from pyarrow import compute as pc
from pyarrow.compute import register_scalar_function
from pyarrow.compute import InputType


def get_function_doc(summary: str, desc: str, arg_names: List[str]):
    func_doc = {}
    func_doc["summary"] = summary
    func_doc["description"] = desc
    func_doc["arg_names"] = arg_names
    return func_doc

# scalar unary function data


unary_doc = get_function_doc("add function",
                             "test add function",
                             ["scalar1"])


def unary_function(scalar1):
    return pc.call_function("add", [scalar1, 1])

# scalar binary function data


binary_doc = get_function_doc("y=mx",
                              "find y from y = mx",
                              ["m", "x"])


def binary_function(m, x):
    return pc.call_function("multiply", [m, x])

# scalar ternary function data


ternary_doc = get_function_doc("y=mx+c",
                               "find y from y = mx + c",
                               ["m", "x", "c"])


def ternary_function(m, x, c):
    mx = pc.call_function("multiply", [m, x])
    return pc.call_function("add", [mx, c])

# scalar varargs function data


varargs_doc = get_function_doc("z=ax+by+c",
                               "find z from z = ax + by + c",
                               ["a", "x", "b", "y", "c"])


def varargs_function(*args):
    a, x, b, y, c = args
    ax = pc.call_function("multiply", [a, x])
    by = pc.call_function("multiply", [b, y])
    ax_by = pc.call_function("add", [ax, by])
    return pc.call_function("add", [ax_by, c])


@pytest.fixture
def function_input_types():
    return [
        # scalar data input types
        [
            InputType.scalar(pa.int64()),
        ],
        [
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
        ],
        [
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
        ],
        [
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
        ],
        # array data input types
        [
            InputType.array(pa.int64()),
        ],
        [
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
        ],
        [
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
        ],
        [
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
        ],
    ]


@pytest.fixture
def function_output_types():
    return [
        pa.int64(),
        pa.int64(),
        pa.int64(),
        pa.int64(),
    ]


@pytest.fixture
def function_names():
    return [
        # scalar data function names
        "scalar_y=x+k",
        "scalar_y=mx",
        "scalar_y=mx+c",
        "scalar_z=ax+by+c",
        # array data function names
        "array_y=x+k",
        "array_y=mx",
        "array_y=mx+c",
        "array_z=ax+by+c"
    ]


@pytest.fixture
def function_arities():
    return [
        1,
        2,
        3,
        5,
    ]


@pytest.fixture
def function_docs():
    return [
        unary_doc,
        binary_doc,
        ternary_doc,
        varargs_doc
    ]


@pytest.fixture
def functions():
    return [
        unary_function,
        binary_function,
        ternary_function,
        varargs_function
    ]


@pytest.fixture
def function_inputs():
    return [
        # scalar input data
        [
            pa.scalar(10, pa.int64())
        ],
        [
            pa.scalar(10, pa.int64()),
            pa.scalar(2, pa.int64())
        ],
        [
            pa.scalar(10, pa.int64()),
            pa.scalar(2, pa.int64()),
            pa.scalar(5, pa.int64())
        ],
        [
            pa.scalar(2, pa.int64()),
            pa.scalar(10, pa.int64()),
            pa.scalar(3, pa.int64()),
            pa.scalar(20, pa.int64()),
            pa.scalar(5, pa.int64())
        ],
        # array input data
        [
            pa.array([10, 20], pa.int64())
        ],
        [
            pa.array([10, 20], pa.int64()),
            pa.array([2, 4], pa.int64())
        ],
        [
            pa.array([10, 20], pa.int64()),
            pa.array([2, 4], pa.int64()),
            pa.array([5, 10], pa.int64())
        ],
        [
            pa.array([2, 3], pa.int64()),
            pa.array([10, 20], pa.int64()),
            pa.array([3, 7], pa.int64()),
            pa.array([20, 30], pa.int64()),
            pa.array([5, 10], pa.int64())
        ]
    ]


@pytest.fixture
def expected_outputs():
    return [
        # scalar output data
        pa.scalar(11, pa.int64()),  # 10 + 1
        pa.scalar(20, pa.int64()),  # 10 * 2
        pa.scalar(25, pa.int64()),  # 10 * 2 + 5
        pa.scalar(85, pa.int64()),  # (2 * 10) + (3 * 20) + 5
        # array output data
        pa.array([11, 21], pa.int64()),  # [10 + 1, 20 + 1]
        pa.array([20, 80], pa.int64()),  # [10 * 2, 20 * 4]
        pa.array([25, 90], pa.int64()),  # [(10 * 2) + 5, (20 * 4) + 10]
        # [(2 * 10) + (3 * 20) + 5, (3 * 20) + (7 * 30) + 10]
        pa.array([85, 280], pa.int64())
    ]


def test_scalar_udf_function_with_scalar_data(function_names,
                                              function_arities,
                                              function_input_types,
                                              function_output_types,
                                              function_docs,
                                              functions,
                                              function_inputs,
                                              expected_outputs):

    # Note: 2 * -> used to duplicate the list
    # Because the values are same irrespective of the type i.e scalar or array
    for name, \
        arity, \
        in_types, \
        out_type, \
        doc, \
        function, \
        input, \
        expected_output in zip(function_names,
                               2 * function_arities,
                               function_input_types,
                               2 * function_output_types,
                               2 * function_docs,
                               2 * functions,
                               function_inputs,
                               expected_outputs):

        register_scalar_function(
            name, arity, doc, in_types, out_type, function)

        func = pc.get_function(name)
        assert func.name == name

        result = pc.call_function(name, input)
        assert result == expected_output


def test_udf_input():
    def unary_scalar_function(scalar):
        return pc.call_function("add", [scalar, 1])

    # validate arity
    arity = -1
    func_name = "py_scalar_add_func"
    in_types = [InputType.scalar(pa.int64())]
    out_type = pa.int64()
    doc = get_function_doc("scalar add function", "scalar add function",
                           ["scalar_value"])
    with pytest.raises(ValueError):
        register_scalar_function(func_name, arity, doc, in_types,
                                 out_type, unary_scalar_function)

    # validate function name
    with pytest.raises(TypeError):
        register_scalar_function(None, 1, doc, in_types,
                                 out_type, unary_scalar_function)

    # validate function not matching defined arity config
    def invalid_function(array1, array2):
        return pc.call_function("add", [array1, array2])

    with pytest.raises(pa.lib.ArrowInvalid):
        register_scalar_function("invalid_function", 1, doc, in_types,
                                 out_type, invalid_function)
        pc.call_function("invalid_function", [pa.array([10]), pa.array([20])],
                         options=None, memory_pool=None)

    # validate function
    with pytest.raises(ValueError) as execinfo:
        register_scalar_function("none_function", 1, doc, in_types,
                                 out_type, None)
        assert "callback must be a callable" == execinfo.value

    # validate output type
    with pytest.raises(ValueError) as execinfo:
        register_scalar_function(func_name, 1, doc, in_types,
                                 None, unary_scalar_function)
        assert "Output value type must be defined" == execinfo.value

    # validate input type
    with pytest.raises(ValueError) as execinfo:
        register_scalar_function(func_name, 1, doc, None,
                                 out_type, unary_scalar_function)
        assert "input types must be of type InputType" == execinfo.value


def test_varargs_function_validation():
    def n_add(*values):
        base_val = values[:2]
        res = pc.call_function("add", base_val)
        for other_val in values[2:]:
            res = pc.call_function("add", [res, other_val])
        return res

    func_name = "n_add"
    arity = 2
    in_types = [InputType.array(pa.int64()), InputType.array(pa.int64())]
    out_type = pa.int64()
    doc = get_function_doc("n add function", "add N number of arrays",
                           ["value1", "value2"])
    register_scalar_function(func_name, arity, doc,
                             in_types, out_type, n_add)

    func = pc.get_function(func_name)

    assert func.name == func_name

    with pytest.raises(pa.lib.ArrowInvalid) as execinfo:
        pc.call_function(func_name, [pa.array([1, 10]),
                                     ])
        error_msg = "VarArgs function 'n_add' needs at least 2 arguments"
        +" but attempted to look up kernel(s) with only 1"
        assert error_msg == execinfo.value


def test_function_doc_validation():

    def unary_scalar_function(scalar):
        return pc.call_function("add", [scalar, 1])

    # validate arity
    arity = 1
    in_types = [InputType.scalar(pa.int64())]
    out_type = pa.int64()

    # doc with no summary
    func_doc = {}
    func_doc["description"] = "desc"
    func_doc["arg_names"] = ["scalar1"]
    
    with pytest.raises(ValueError) as execinfo:
        register_scalar_function("no_summary", arity, func_doc, in_types,
                                    out_type, unary_scalar_function)
        expected_expr = "must contain summary, arg_names and a description"
        assert expected_expr in execinfo.value
        
    # doc with no decription
    func_doc = {}
    func_doc["summary"] = "test summary"
    func_doc["arg_names"] = ["scalar1"]
    
    with pytest.raises(ValueError) as execinfo:
        register_scalar_function("no_desc", arity, func_doc, in_types,
                                    out_type, unary_scalar_function)
        expected_expr = "must contain summary, arg_names and a description"
        assert expected_expr in execinfo.value
        
    # doc with no arg_names
    func_doc = {}
    func_doc["summary"] = "test summary"
    func_doc["description"] = "some test func"
    
    with pytest.raises(ValueError) as execinfo:
        register_scalar_function("no_arg_names", arity, func_doc, in_types,
                                    out_type, unary_scalar_function)
        expected_expr = "must contain summary, arg_names and a description"
        assert expected_expr in execinfo.value
        
    # doc with empty dictionary
    func_doc = {}
    with pytest.raises(ValueError) as execinfo:
        register_scalar_function("no_arg_names", arity, func_doc, in_types,
                                    out_type, unary_scalar_function)
        expected_expr = "must contain summary, arg_names and a description"
        assert expected_expr in execinfo.value

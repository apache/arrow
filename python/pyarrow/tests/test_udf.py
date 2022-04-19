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
from pyarrow import compute as pc


unary_doc = {"summary": "add function",
             "description": "test add function"}


def unary_function(scalar1):
    return pc.call_function("add", [scalar1, 1])


binary_doc = {"summary": "y=mx",
              "description": "find y from y = mx"}


def binary_function(m, x):
    return pc.call_function("multiply", [m, x])


ternary_doc = {"summary": "y=mx+c",
               "description": "find y from y = mx + c"}


def ternary_function(m, x, c):
    mx = pc.call_function("multiply", [m, x])
    return pc.call_function("add", [mx, c])


varargs_doc = {"summary": "z=ax+by+c",
               "description": "find z from z = ax + by + c"
               }


def varargs_function(*values):
    base_val = values[:2]
    res = pc.call_function("add", base_val)
    for other_val in values[2:]:
        res = pc.call_function("add", [res, other_val])
    return res


def test_scalar_udf_function_with_scalar_valued_functions():
    function_names = [
        "scalar_y=x+k",
        "scalar_y=mx",
        "scalar_y=mx+c",
        "scalar_z=ax+by+c",
    ]

    function_input_types = [
        {
            "scalar": pc.InputType.scalar(pa.int64()),
        },
        {
            "scalar1": pc.InputType.scalar(pa.int64()),
            "scalar2": pc.InputType.scalar(pa.int64()),
        },
        {
            "scalar1": pc.InputType.scalar(pa.int64()),
            "scalar2": pc.InputType.scalar(pa.int64()),
            "scalar3": pc.InputType.scalar(pa.int64()),
        },
        {
            "scalar1": pc.InputType.scalar(pa.int64()),
            "scalar2": pc.InputType.scalar(pa.int64()),
            "scalar3": pc.InputType.scalar(pa.int64()),
            "scalar4": pc.InputType.scalar(pa.int64()),
            "scalar5": pc.InputType.scalar(pa.int64()),
        },
    ]

    function_output_types = [
        pa.int64(),
        pa.int64(),
        pa.int64(),
        pa.int64(),
    ]

    function_docs = [
        unary_doc,
        binary_doc,
        ternary_doc,
        varargs_doc
    ]

    functions = [
        unary_function,
        binary_function,
        ternary_function,
        varargs_function
    ]

    function_inputs = [
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
    ]

    for name, \
        in_types, \
        out_type, \
        doc, \
        function, \
        input in zip(function_names,
                     function_input_types,
                     function_output_types,
                     function_docs,
                     functions,
                     function_inputs):
        expected_output = function(*input)
        pc.register_scalar_function(function,
                                    name, doc, in_types, out_type)

        func = pc.get_function(name)
        assert func.name == name

        result = pc.call_function(name, input)
        assert result == expected_output


def test_scalar_udf_with_array_data_functions():
    function_names = [
        "array_y=x+k",
        "array_y=mx",
        "array_y=mx+c",
        "array_z=ax+by+c"
    ]

    function_input_types = [
        {
            "array": pc.InputType.array(pa.int64()),
        },
        {
            "array1": pc.InputType.array(pa.int64()),
            "array2": pc.InputType.array(pa.int64()),
        },
        {
            "array1": pc.InputType.array(pa.int64()),
            "array2": pc.InputType.array(pa.int64()),
            "array3": pc.InputType.array(pa.int64()),
        },
        {
            "array1": pc.InputType.array(pa.int64()),
            "array2": pc.InputType.array(pa.int64()),
            "array3": pc.InputType.array(pa.int64()),
            "array4": pc.InputType.array(pa.int64()),
            "array5": pc.InputType.array(pa.int64()),
        },
    ]

    function_output_types = [
        pa.int64(),
        pa.int64(),
        pa.int64(),
        pa.int64(),
    ]

    function_docs = [
        unary_doc,
        binary_doc,
        ternary_doc,
        varargs_doc
    ]

    functions = [
        unary_function,
        binary_function,
        ternary_function,
        varargs_function
    ]

    function_inputs = [
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

    for name, \
        in_types, \
        out_type, \
        doc, \
        function, \
        input in zip(function_names,
                     function_input_types,
                     function_output_types,
                     function_docs,
                     functions,
                     function_inputs):
        expected_output = function(*input)
        pc.register_scalar_function(function,
                                    name, doc, in_types, out_type)

        func = pc.get_function(name)
        assert func.name == name

        result = pc.call_function(name, input)
        assert result == expected_output


def test_udf_input():
    def unary_scalar_function(scalar):
        return pc.call_function("add", [scalar, 1])

    # validate function name
    doc = {
        "summary": "test udf input",
        "description": "parameters are validated"
    }
    in_types = {"scalar": pc.InputType.scalar(pa.int64())}
    out_type = pa.int64()
    with pytest.raises(TypeError):
        pc.register_scalar_function(unary_scalar_function, None, doc, in_types,
                                    out_type)

    # validate function
    with pytest.raises(TypeError, match="Object must be a callable"):
        pc.register_scalar_function(None, "none_function", doc, in_types,
                                    out_type)

    # validate output type
    expected_expr = "DataType expected, got <class 'NoneType'>"
    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function,
                                    "output_function", doc, in_types,
                                    None)

    # validate input type
    expected_expr = r'in_types must be a dictionary of InputType'
    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function,
                                    "input_function", doc, None,
                                    out_type)


def test_varargs_function_validation():
    def n_add(*values):
        base_val = values[:2]
        res = pc.call_function("add", base_val)
        for other_val in values[2:]:
            res = pc.call_function("add", [res, other_val])
        return res

    in_types = {"array1": pc.InputType.array(pa.int64()),
                "array2": pc.InputType.array(pa.int64())
                }
    doc = {"summary": "n add function",
           "description": "add N number of arrays"
           }
    pc.register_scalar_function(n_add, "n_add", doc,
                                in_types, pa.int64())

    func = pc.get_function("n_add")

    assert func.name == "n_add"
    error_msg = "VarArgs function 'n_add' needs at least 2 arguments"
    with pytest.raises(pa.lib.ArrowInvalid, match=error_msg):
        pc.call_function("n_add", [pa.array([1, 10]),
                                   ])


def test_function_doc_validation():

    def unary_scalar_function(scalar):
        return pc.call_function("add", [scalar, 1])

    # validate arity
    in_types = {"scalar": pc.InputType.scalar(pa.int64())}
    out_type = pa.int64()

    # doc with no summary
    func_doc = {
        "description": "desc"
    }

    expected_expr = "Function doc must contain a summary"

    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function, "no_summary",
                                    func_doc, in_types,
                                    out_type)

    # doc with no decription
    func_doc = {
        "summary": "test summary"
    }

    expected_expr = "Function doc must contain a description"

    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function, "no_desc",
                                    func_doc, in_types,
                                    out_type)

    # doc with empty dictionary
    func_doc = {}
    expected_expr = "Function doc must contain a summary"
    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(unary_scalar_function, "empty_dictionary",
                                    func_doc, in_types,
                                    out_type)


def test_non_uniform_input_udfs():

    def unary_scalar_function(scalar1, array1, scalar2):
        coeff = pc.call_function("add", [scalar1, scalar2])
        return pc.call_function("multiply", [coeff, array1])

    in_types = {"scalar1": pc.InputType.scalar(pa.int64()),
                "scalar2": pc.InputType.array(pa.int64()),
                "scalar3": pc.InputType.scalar(pa.int64()),
                }
    func_doc = {
        "summary": "multi type udf",
        "description": "desc"
    }
    pc.register_scalar_function(unary_scalar_function, 
                                "multi_type_udf", func_doc,
                                in_types,
                                pa.int64())

    res = pc.call_function("multi_type_udf",
                           [pa.scalar(10), pa.array([1, 2, 3]), pa.scalar(20)])

    assert res == pa.array([30, 60, 90])


def test_nullary_functions():

    def gen_random():
        import random
        val = random.randint(0, 10)
        return pa.scalar(val)

    func_doc = {
        "summary": "random function",
        "description": "generates a random value"
    }

    pc.register_scalar_function(gen_random, "random_func", func_doc,
                                {},
                                pa.int64())

    res = pc.call_function("random_func", [])
    assert res.as_py() >= 0 and res.as_py() <= 10


def test_output_datatype():
    def add_one(array):
        ar = pc.call_function("add", [array, 1])
        ar = ar.cast(pa.int32())
        return ar

    func_name = "py_add_to_scalar"
    in_types = {"array": pc.InputType.array(pa.int64())}
    out_type = pa.int64()
    doc = {
        "summary": "add function scalar",
        "description": "add function"
    }
    pc.register_scalar_function(add_one, func_name, doc,
                                in_types, out_type)

    func = pc.get_function(func_name)

    assert func.name == func_name

    expected_expr = "Expected output type, int64," \
        + " but function returned type int32"

    with pytest.raises(pa.lib.ArrowTypeError, match=expected_expr):
        pc.call_function(func_name, [pa.array([20, 30])])


def test_output_type():
    def add_one(array):
        return 42

    func_name = "add_to_scalar_as_py"
    in_types = {"array": pc.InputType.array(pa.int64())}
    out_type = pa.int64()
    doc = {
        "summary": "add function scalar",
        "description": "add function"
    }
    pc.register_scalar_function(add_one, func_name, doc,
                                in_types, out_type)

    func = pc.get_function(func_name)

    assert func.name == func_name

    expected_expr = "Unexpected output type: int"

    with pytest.raises(pa.lib.ArrowTypeError, match=expected_expr):
        pc.call_function(func_name, [pa.array([20, 30])])

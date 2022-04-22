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


# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not udf'
# pytestmark = pytest.mark.udf

unary_doc = {"summary": "add function",
             "description": "test add function"}


@pytest.fixture(scope="session")
def udf_context():
    return pc._get_scalar_udf_context(pa.default_memory_pool(), 0)


@pytest.fixture(scope="session")
def unary_func_fixture():
    def unary_function(udf_context, scalar1):
        return pc.call_function("add", [scalar1, 1])
    return unary_function


binary_doc = {"summary": "y=mx",
              "description": "find y from y = mx"}


@pytest.fixture(scope="session")
def binary_func_fixture():
    def binary_function(ctx, m, x):
        return pc.call_function("multiply", [m, x])
    return binary_function


ternary_doc = {"summary": "y=mx+c",
               "description": "find y from y = mx + c"}


@pytest.fixture(scope="session")
def ternary_func_fixture():
    def ternary_function(ctx, m, x, c):
        mx = pc.call_function("multiply", [m, x])
        return pc.call_function("add", [mx, c])
    return ternary_function


varargs_doc = {"summary": "z=ax+by+c",
               "description": "find z from z = ax + by + c"
               }


@pytest.fixture(scope="session")
def varargs_func_fixture():
    def varargs_function(ctx, *values):
        base_val = values[:2]
        res = pc.call_function("add", base_val)
        for other_val in values[2:]:
            res = pc.call_function("add", [res, other_val])
        return res
    return varargs_function


@pytest.fixture(scope="session")
def random_with_udf_ctx_func_fixture():
    def random_with_udf_ctx(context, one, two):
        old_pool = pa.default_memory_pool()
        proxy_pool = pa.proxy_memory_pool(context.memory_pool)
        pa.set_memory_pool(proxy_pool)
        try:
            ans = pc.add(one, two, memory_pool=proxy_pool)
            allocated_before = proxy_pool.bytes_allocated()
            # allocating 64 bytes
            res = pa.array([ans.as_py()], memory_pool=proxy_pool)
            allocated_after = proxy_pool.bytes_allocated()
        finally:
            pa.set_memory_pool(old_pool)
        assert allocated_before == 0
        assert allocated_after == 64
        assert context.batch_length == 2
        return res
    return random_with_udf_ctx


@pytest.fixture(scope="session")
def const_return_func_fixture():
    def const_return(ctx, array):
        return 42
    return const_return


@pytest.fixture(scope="session")
def output_check_func_fixture():
    def output_check(ctx, array):
        ar = pc.call_function("add", [array, 1])
        ar = ar.cast(pa.int32())
        return ar
    return output_check


@pytest.fixture(scope="session")
def nullary_check_func_fixture():
    def nullary_check(ctx):
        import random
        val = random.randint(0, 10)
        return pa.scalar(val)
    return nullary_check


@pytest.fixture(scope="session")
def add_const_func_fixture():
    def add_const(ctx, scalar):
        return pc.call_function("add", [scalar, 1])
    return add_const


@pytest.fixture(scope="session")
def varargs_check_func_fixture():
    def varargs_check(ctx, *values):
        base_val = values[:2]
        res = pc.call_function("add", base_val)
        for other_val in values[2:]:
            res = pc.call_function("add", [res, other_val])
        return res
    return varargs_check


@pytest.fixture(scope="session")
def raise_func_fixture():
    def raise_func(ctx):
        raise ValueError("Test function with raise")
    return raise_func


def check_scalar_function(name,
                          in_types,
                          out_type,
                          doc,
                          function,
                          input,
                          udf_context):
    expected_output = function(udf_context, *input)
    pc.register_scalar_function(function,
                                name, doc, in_types, out_type)

    func = pc.get_function(name)
    assert func.name == name

    result = pc.call_function(name, input)
    assert result == expected_output


def test_scalar_udf_scalar_unary(unary_func_fixture, udf_context):
    check_scalar_function("scalar_y=x+k",
                          {"scalar": pc.InputType.scalar(pa.int64()), },
                          pa.int64(),
                          unary_doc,
                          unary_func_fixture,
                          [pa.scalar(10, pa.int64())],
                          udf_context
                          )


def test_scalar_udf_scalar_binary(binary_func_fixture, udf_context):
    check_scalar_function("scalar_y=mx",
                          {
                              "scalar1": pc.InputType.scalar(pa.int64()),
                              "scalar2": pc.InputType.scalar(pa.int64()),
                          },
                          pa.int64(),
                          binary_doc,
                          binary_func_fixture,
                          [
                              pa.scalar(10, pa.int64()),
                              pa.scalar(2, pa.int64())
                          ],
                          udf_context
                          )


def test_scalar_udf_scalar_ternary(ternary_func_fixture, udf_context):
    check_scalar_function("scalar_y=mx+c",
                          {
                              "scalar1": pc.InputType.scalar(pa.int64()),
                              "scalar2": pc.InputType.scalar(pa.int64()),
                              "scalar3": pc.InputType.scalar(pa.int64()),
                          },
                          pa.int64(),
                          ternary_doc,
                          ternary_func_fixture,
                          [
                              pa.scalar(10, pa.int64()),
                              pa.scalar(2, pa.int64()),
                              pa.scalar(5, pa.int64())
                          ],
                          udf_context
                          )


def test_scalar_udf_scalar_varargs(varargs_func_fixture, udf_context):
    check_scalar_function("scalar_z=ax+by+c",
                          {
                              "scalar1": pc.InputType.scalar(pa.int64()),
                              "scalar2": pc.InputType.scalar(pa.int64()),
                              "scalar3": pc.InputType.scalar(pa.int64()),
                              "scalar4": pc.InputType.scalar(pa.int64()),
                              "scalar5": pc.InputType.scalar(pa.int64()),
                          },
                          pa.int64(),
                          varargs_doc,
                          varargs_func_fixture,
                          [
                              pa.scalar(2, pa.int64()),
                              pa.scalar(10, pa.int64()),
                              pa.scalar(3, pa.int64()),
                              pa.scalar(20, pa.int64()),
                              pa.scalar(5, pa.int64())
                          ],
                          udf_context
                          )


def test_scalar_udf_array_unary(unary_func_fixture, udf_context):
    check_scalar_function("array_y=x+k",
                          {"array": pc.InputType.array(pa.int64()), },
                          pa.int64(),
                          unary_doc,
                          unary_func_fixture,
                          [
                              pa.array([10, 20], pa.int64())
                          ],
                          udf_context
                          )


def test_scalar_udf_array_binary(binary_func_fixture, udf_context):
    check_scalar_function("array_y=mx",
                          {
                              "array1": pc.InputType.array(pa.int64()),
                              "array2": pc.InputType.array(pa.int64()),
                          },
                          pa.int64(),
                          binary_doc,
                          binary_func_fixture,
                          [
                              pa.array([10, 20], pa.int64()),
                              pa.array([2, 4], pa.int64())
                          ],
                          udf_context
                          )


def test_scalar_udf_array_ternary(ternary_func_fixture, udf_context):
    check_scalar_function("array_y=mx+c",
                          {
                              "array1": pc.InputType.array(pa.int64()),
                              "array2": pc.InputType.array(pa.int64()),
                              "array3": pc.InputType.array(pa.int64()),
                          },
                          pa.int64(),
                          ternary_doc,
                          ternary_func_fixture,
                          [
                              pa.array([10, 20], pa.int64()),
                              pa.array([2, 4], pa.int64()),
                              pa.array([5, 10], pa.int64())
                          ],
                          udf_context
                          )


def test_scalar_udf_array_varargs(varargs_func_fixture, udf_context):
    check_scalar_function("array_z=ax+by+c",
                          {
                              "array1": pc.InputType.array(pa.int64()),
                              "array2": pc.InputType.array(pa.int64()),
                              "array3": pc.InputType.array(pa.int64()),
                              "array4": pc.InputType.array(pa.int64()),
                              "array5": pc.InputType.array(pa.int64()),
                          },
                          pa.int64(),
                          varargs_doc,
                          varargs_func_fixture,
                          [
                              pa.array([2, 3], pa.int64()),
                              pa.array([10, 20], pa.int64()),
                              pa.array([3, 7], pa.int64()),
                              pa.array([20, 30], pa.int64()),
                              pa.array([5, 10], pa.int64())
                          ],
                          udf_context
                          )


def test_udf_input(add_const_func_fixture):
    # validate function name
    doc = {
        "summary": "test udf input",
        "description": "parameters are validated"
    }
    in_types = {"scalar": pc.InputType.scalar(pa.int64())}
    out_type = pa.int64()
    with pytest.raises(TypeError):
        pc.register_scalar_function(add_const_func_fixture,
                                    None, doc, in_types,
                                    out_type)

    # validate function
    with pytest.raises(TypeError, match="Object must be a callable"):
        pc.register_scalar_function(None, "test_none_function", doc, in_types,
                                    out_type)

    # validate output type
    expected_expr = "DataType expected, got <class 'NoneType'>"
    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(add_const_func_fixture,
                                    "test_output_function", doc, in_types,
                                    None)

    # validate input type
    expected_expr = r'in_types must be a dictionary of InputType'
    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(add_const_func_fixture,
                                    "test_input_function", doc, None,
                                    out_type)


def test_varargs_function_validation(varargs_check_func_fixture):
    in_types = {"array1": pc.InputType.array(pa.int64()),
                "array2": pc.InputType.array(pa.int64())
                }
    doc = {"summary": "n add function",
           "description": "add N number of arrays"
           }
    pc.register_scalar_function(varargs_check_func_fixture, "test_n_add", doc,
                                in_types, pa.int64())

    func = pc.get_function("test_n_add")

    assert func.name == "test_n_add"
    error_msg = "VarArgs function 'test_n_add' needs at least 2 arguments"
    with pytest.raises(pa.lib.ArrowInvalid, match=error_msg):
        pc.call_function("test_n_add", [pa.array([1, 10]),
                                        ])


def test_function_doc_validation(add_const_func_fixture):
    # validate arity
    in_types = {"scalar": pc.InputType.scalar(pa.int64())}
    out_type = pa.int64()

    # doc with no summary
    func_doc = {
        "description": "desc"
    }

    expected_expr = "Function doc must contain a summary"

    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(add_const_func_fixture, "test_no_summary",
                                    func_doc, in_types,
                                    out_type)

    # doc with no decription
    func_doc = {
        "summary": "test summary"
    }

    expected_expr = "Function doc must contain a description"

    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(add_const_func_fixture, "test_no_desc",
                                    func_doc, in_types,
                                    out_type)

    # doc with empty dictionary
    func_doc = {}
    expected_expr = "Function doc must contain a summary"
    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(add_const_func_fixture,
                                    "test_empty_dictionary",
                                    func_doc, in_types,
                                    out_type)


def test_non_uniform_input_udfs(ternary_func_fixture):
    in_types = {"scalar1": pc.InputType.scalar(pa.int64()),
                "scalar2": pc.InputType.array(pa.int64()),
                "scalar3": pc.InputType.scalar(pa.int64()),
                }
    func_doc = {
        "summary": "multi type udf",
        "description": "desc"
    }
    pc.register_scalar_function(ternary_func_fixture,
                                "test_multi_type_udf", func_doc,
                                in_types,
                                pa.int64())

    res = pc.call_function("test_multi_type_udf",
                           [pa.scalar(10), pa.array([1, 2, 3]), pa.scalar(20)])
    assert res == pa.array([30, 40, 50])


def test_nullary_functions(nullary_check_func_fixture):
    func_doc = {
        "summary": "random function",
        "description": "generates a random value"
    }

    pc.register_scalar_function(nullary_check_func_fixture,
                                "test_random_func", func_doc,
                                {},
                                pa.int64())

    res = pc.call_function("test_random_func", [])
    assert res.as_py() >= 0 and res.as_py() <= 10


def test_output_datatype(output_check_func_fixture):
    func_name = "test_add_to_scalar"
    in_types = {"array": pc.InputType.array(pa.int64())}
    out_type = pa.int64()
    doc = {
        "summary": "add function scalar",
        "description": "add function"
    }
    pc.register_scalar_function(output_check_func_fixture, func_name, doc,
                                in_types, out_type)

    func = pc.get_function(func_name)

    assert func.name == func_name

    expected_expr = "Expected output type, int64," \
        + " but function returned type int32"

    with pytest.raises(pa.lib.ArrowTypeError, match=expected_expr):
        pc.call_function(func_name, [pa.array([20, 30])])


def test_output_value(output_check_func_fixture):
    func_name = "test_output_value"
    in_types = {"array": pc.InputType.array(pa.int64())}
    out_type = {}
    doc = {
        "summary": "test output value",
        "description": "test output"
    }

    expected_expr = "DataType expected, got <class 'dict'>"

    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(output_check_func_fixture, func_name, doc,
                                    in_types, out_type)


def test_output_type(const_return_func_fixture):
    func_name = "test_output_type_func"
    in_types = {"array": pc.InputType.array(pa.int64())}
    out_type = pa.int64()
    doc = {
        "summary": "add function scalar",
        "description": "add function"
    }
    pc.register_scalar_function(const_return_func_fixture, func_name, doc,
                                in_types, out_type)

    func = pc.get_function(func_name)

    assert func.name == func_name

    expected_expr = "Unexpected output type: int"

    with pytest.raises(pa.lib.ArrowTypeError, match=expected_expr):
        pc.call_function(func_name, [pa.array([20, 30])])


def test_input_type(const_return_func_fixture):
    func_name = "test_input_type"
    in_types = {"array": None}
    out_type = pa.int64()
    doc = {
        "summary": "test invalid input type",
        "description": "invalid input function"
    }
    expected_expr = "in_types must be of type InputType"

    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(const_return_func_fixture, func_name, doc,
                                    in_types, out_type)


def test_udf_context(random_with_udf_ctx_func_fixture):
    in_types = {"one": pc.InputType.scalar(pa.int64()),
                "two": pc.InputType.scalar(pa.int64())
                }
    func_doc = {
        "summary": "test udf context",
        "description": "udf context test"
    }
    pc.register_scalar_function(random_with_udf_ctx_func_fixture,
                                "test_udf_context", func_doc,
                                in_types,
                                pa.int64())

    res = pc.call_function("test_udf_context", [pa.scalar(10), pa.scalar(20)])

    assert res[0].as_py() == 30


def test_function_with_raise(raise_func_fixture):
    func_name = "test_raise"
    in_types = {}
    out_type = pa.int64()
    doc = {
        "summary": "test function with raise",
        "description": "function with a raise"
    }
    expected_expr = "Test function with raise"

    pc.register_scalar_function(raise_func_fixture, func_name, doc,
                                in_types, out_type)
    with pytest.raises(ValueError, match=expected_expr):
        pc.call_function("test_raise", [])

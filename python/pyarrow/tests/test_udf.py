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

# UDFs are all tested with a dataset scan
pytestmark = pytest.mark.dataset


try:
    import pyarrow.dataset as ds
except ImportError:
    ds = None

global_batch_length = 10


def mock_udf_context(batch_length=global_batch_length):
    from pyarrow._compute import _get_scalar_udf_context
    return _get_scalar_udf_context(pa.default_memory_pool(), batch_length)


@pytest.fixture(scope="session")
def unary_func_fixture():
    def unary_function(ctx, scalar1):
        return pc.call_function("add", [scalar1, 1])
    func_name = "y=x+k"
    unary_doc = {"summary": "add function",
                 "description": "test add function"}
    pc.register_scalar_function(unary_function,
                                func_name,
                                unary_doc,
                                {"array": pa.int64()},
                                pa.int64())
    return unary_function, func_name


@pytest.fixture(scope="session")
def binary_func_fixture():
    def binary_function(ctx, m, x):
        return pc.call_function("multiply", [m, x])
    func_name = "y=mx"
    binary_doc = {"summary": "y=mx",
                  "description": "find y from y = mx"}
    pc.register_scalar_function(binary_function,
                                func_name,
                                binary_doc,
                                {"m": pa.int64(),
                                 "x": pa.int64(),
                                 },
                                pa.int64())
    return binary_function, func_name


@pytest.fixture(scope="session")
def ternary_func_fixture():
    def ternary_function(ctx, m, x, c):
        mx = pc.call_function("multiply", [m, x])
        return pc.call_function("add", [mx, c])
    ternary_doc = {"summary": "y=mx+c",
                   "description": "find y from y = mx + c"}
    func_name = "y=mx+c"
    pc.register_scalar_function(ternary_function,
                                func_name,
                                ternary_doc,
                                {
                                    "array1": pa.int64(),
                                    "array2": pa.int64(),
                                    "array3": pa.int64(),
                                },
                                pa.int64())
    return ternary_function, func_name


@pytest.fixture(scope="session")
def varargs_func_fixture():
    def varargs_function(ctx, *values):
        base_val = values[:2]
        res = pc.call_function("add", base_val)
        for other_val in values[2:]:
            res = pc.call_function("add", [res, other_val])
        return res
    func_name = "z=ax+by+c"
    varargs_doc = {"summary": "z=ax+by+c",
                   "description": "find z from z = ax + by + c"
                   }
    pc.register_scalar_function(varargs_function,
                                func_name,
                                varargs_doc,
                                {
                                    "array1": pa.int64(),
                                    "array2": pa.int64(),
                                    "array3": pa.int64(),
                                    "array4": pa.int64(),
                                    "array5": pa.int64(),
                                },
                                pa.int64())
    return varargs_function, func_name


@pytest.fixture(scope="session")
def random_with_udf_ctx_func_fixture():
    def random_with_udf_ctx(context, one, two):
        proxy_pool = pa.proxy_memory_pool(context.memory_pool)
        ans = pc.add(one, two, memory_pool=proxy_pool)
        res = pa.array([ans.as_py()], memory_pool=proxy_pool)
        return res
    in_types = {"one": pa.int64(),
                "two": pa.int64(),
                }
    func_doc = {
        "summary": "test udf context",
        "description": "udf context test"
    }
    func_name = "test_udf_context"
    pc.register_scalar_function(random_with_udf_ctx,
                                func_name, func_doc,
                                in_types,
                                pa.int64())
    return random_with_udf_ctx, func_name


@pytest.fixture(scope="session")
def output_check_func_fixture():
    # The objective of this fixture is to evaluate,
    # how the UDF interface respond to unexpected
    # output types. The types chosen at the test
    # end are either of different Arrow data type
    # or non-Arrow type.
    def output_check(ctx, array):
        ar = pc.call_function("add", [array, 1])
        ar = ar.cast(pa.int32())
        return ar
    func_name = "test_output_value"
    in_types = {"array": pa.int64()}
    out_type = pa.int64()
    doc = {
        "summary": "add function scalar",
        "description": "add function"
    }
    pc.register_scalar_function(output_check, func_name, doc,
                                in_types, out_type)
    return output_check, func_name


@pytest.fixture(scope="session")
def nullary_check_func_fixture():
    # this needs to return array values
    def nullary_check(context):
        return pa.array([42] * context.batch_length, type=pa.int64(),
                        memory_pool=context.memory_pool)

    func_doc = {
        "summary": "random function",
        "description": "generates a random value"
    }
    func_name = "test_random_func"
    pc.register_scalar_function(nullary_check,
                                func_name,
                                func_doc,
                                {},
                                pa.int64())

    return nullary_check, func_name


def add_const(ctx, scalar):
    return pc.call_function("add", [scalar, 1])


@pytest.fixture(scope="session")
def output_python_type_func_fixture():
    # This fixture helps to check the response
    # when the function return value is not an Arrow
    # defined data type. Instead here the returned value
    # is of type int in Python.
    def const_return(ctx, scalar):
        return 42

    func_name = "test_output_type"
    in_types = {"array": pa.int64()}
    out_type = pa.int64()
    doc = {
        "summary": "add function scalar",
        "description": "add function"
    }
    pc.register_scalar_function(const_return, func_name, doc,
                                in_types, out_type)
    return const_return, func_name


@pytest.fixture(scope="session")
def varargs_check_func_fixture():
    def varargs_check(ctx, *values):
        base_val = values[:2]
        res = pc.call_function("add", base_val)
        for other_val in values[2:]:
            res = pc.call_function("add", [res, other_val])
        return res
    func_name = "test_varargs_function"
    in_types = {"array1": pa.int64(),
                "array2": pa.int64(),
                }
    doc = {"summary": "n add function",
           "description": "add N number of arrays"
           }
    pc.register_scalar_function(varargs_check, func_name, doc,
                                in_types, pa.int64())

    return varargs_check, func_name


@pytest.fixture(scope="session")
def raise_func_fixture():
    def raise_func(ctx):
        raise ValueError("Test function with raise")
    func_name = "test_raise"
    doc = {
        "summary": "test function with raise",
        "description": "function with a raise"
    }
    pc.register_scalar_function(raise_func, func_name, doc,
                                {}, pa.int64())
    return raise_func, func_name


def check_scalar_function(func_fixture,
                          input,
                          run_in_dataset=True,
                          batch_length=global_batch_length):
    function, name = func_fixture
    expected_output = function(mock_udf_context(batch_length), *input)
    func = pc.get_function(name)
    assert func.name == name

    result = pc.call_function(name, input)

    assert result == expected_output
    if run_in_dataset:
        field_names = [f'field{index}' for index, in_arr in input]
        table = pa.Table.from_arrays(input, field_names)
        dataset = ds.dataset(table)
        func_args = [ds.field(field_name) for field_name in field_names]
        result_table = dataset.to_table(
            columns={'result': ds.field('')._call(name, func_args)})
        assert result_table.column(0).chunks[0] == expected_output


def test_scalar_udf_array_unary(unary_func_fixture):
    check_scalar_function(unary_func_fixture,
                          [
                              pa.array([10, 20], pa.int64())
                          ],
                          mock_udf_context()
                          )


def test_scalar_udf_array_binary(binary_func_fixture):
    check_scalar_function(binary_func_fixture,
                          [
                              pa.array([10, 20], pa.int64()),
                              pa.array([2, 4], pa.int64())
                          ],
                          mock_udf_context()
                          )


def test_scalar_udf_array_ternary(ternary_func_fixture):
    check_scalar_function(ternary_func_fixture,
                          [
                              pa.array([10, 20], pa.int64()),
                              pa.array([2, 4], pa.int64()),
                              pa.array([5, 10], pa.int64())
                          ],
                          mock_udf_context()
                          )


def test_scalar_udf_array_varargs(varargs_func_fixture):
    check_scalar_function(varargs_func_fixture,
                          [
                              pa.array([2, 3], pa.int64()),
                              pa.array([10, 20], pa.int64()),
                              pa.array([3, 7], pa.int64()),
                              pa.array([20, 30], pa.int64()),
                              pa.array([5, 10], pa.int64())
                          ],
                          mock_udf_context
                          )


def test_registration_errors():
    # validate function name
    doc = {
        "summary": "test udf input",
        "description": "parameters are validated"
    }
    in_types = {"scalar": pa.int64()}
    out_type = pa.int64()

    def test_reg_function(context):
        return pa.array([10])

    with pytest.raises(TypeError):
        pc.register_scalar_function(test_reg_function,
                                    None, doc, in_types,
                                    out_type)

    # validate function
    with pytest.raises(TypeError, match="func must be a callable"):
        pc.register_scalar_function(None, "test_none_function", doc, in_types,
                                    out_type)

    # validate output type
    expected_expr = "DataType expected, got <class 'NoneType'>"
    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(test_reg_function,
                                    "test_output_function", doc, in_types,
                                    None)

    # validate input type
    expected_expr = r'in_types must be a dictionary of DataType'
    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(test_reg_function,
                                    "test_input_function", doc, None,
                                    out_type)

    # register an already registered function
    # first registration
    pc.register_scalar_function(test_reg_function,
                                "test_reg_function", doc, {},
                                out_type)
    # second registration
    expected_expr = "Already have a function registered with name:" \
        + " test_reg_function"
    with pytest.raises(pa.lib.ArrowKeyError, match=expected_expr):
        pc.register_scalar_function(test_reg_function,
                                    "test_reg_function", doc, {},
                                    out_type)


def test_varargs_function_validation(varargs_check_func_fixture):
    _function, func_name = varargs_check_func_fixture
    func = pc.get_function(func_name)

    assert func.name == func_name

    error_msg = "VarArgs function 'test_varargs_function'" \
        + " needs at least 2 arguments"

    with pytest.raises(pa.lib.ArrowInvalid, match=error_msg):
        pc.call_function(func_name, [pa.array([1, 10]),
                                     ])


def test_function_doc_validation():
    # validate arity
    in_types = {"scalar": pa.int64()}
    out_type = pa.int64()

    # doc with no summary
    func_doc = {
        "description": "desc"
    }

    expected_expr = "Function doc must contain a summary"

    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(add_const, "test_no_summary",
                                    func_doc, in_types,
                                    out_type)

    # doc with no decription
    func_doc = {
        "summary": "test summary"
    }

    expected_expr = "Function doc must contain a description"

    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(add_const, "test_no_desc",
                                    func_doc, in_types,
                                    out_type)

    # doc with empty dictionary
    func_doc = {}
    expected_expr = "Function doc must contain a summary"
    with pytest.raises(ValueError, match=expected_expr):
        pc.register_scalar_function(add_const,
                                    "test_empty_dictionary",
                                    func_doc, in_types,
                                    out_type)


def test_nullary_functions(nullary_check_func_fixture):
    check_scalar_function(nullary_check_func_fixture, [], False, 1)


def test_output_datatype(output_check_func_fixture):
    function, func_name = output_check_func_fixture
    func = pc.get_function(func_name)

    assert func.name == func_name

    expected_expr = "Expected output type, int64," \
        + " but function returned type int32"

    with pytest.raises(pa.lib.ArrowTypeError, match=expected_expr):
        pc.call_function(func_name, [pa.array([20, 30])])


def test_defined_output_value_type():
    def get_output(ctx, array):
        return pc.call_function("add", [array, 1])
    func_name = "test_output_value"
    in_types = {"array": pa.int64()}
    out_type = {}
    doc = {
        "summary": "test output value",
        "description": "test output"
    }

    expected_expr = "DataType expected, got <class 'dict'>"

    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(output_check_func_fixture, func_name, doc,
                                    in_types, out_type)


def test_output_type(output_python_type_func_fixture):
    _, func_name = output_python_type_func_fixture

    func = pc.get_function(func_name)

    assert func.name == func_name

    expected_expr = "Unexpected output type: int"

    with pytest.raises(pa.lib.ArrowTypeError, match=expected_expr):
        pc.call_function(func_name, [pa.array([20, 30])])


def test_input_type():
    def const_return(ctx, scalar):
        return 42

    func_name = "test_input_type"
    in_types = {"array": None}
    out_type = pa.int64()
    doc = {
        "summary": "test invalid input type",
        "description": "invalid input function"
    }
    expected_expr = "in_types must be of type DataType"

    with pytest.raises(TypeError, match=expected_expr):
        pc.register_scalar_function(const_return, func_name, doc,
                                    in_types, out_type)


def test_udf_context(random_with_udf_ctx_func_fixture):
    proxy_pool = pa.proxy_memory_pool(pa.default_memory_pool())
    _, func_name = random_with_udf_ctx_func_fixture

    res = pc.call_function(func_name,
                           [pa.scalar(10), pa.scalar(20)],
                           memory_pool=proxy_pool)
    assert res[0].as_py() == 30
    assert proxy_pool.bytes_allocated() > 0


def test_function_with_raise(raise_func_fixture):
    _, func_name = raise_func_fixture
    expected_expr = "Test function with raise"
    with pytest.raises(ValueError, match=expected_expr):
        pc.call_function(func_name, [])


def test_non_uniform_input_udfs(ternary_func_fixture):
    function, func_name = ternary_func_fixture
    res = pc.call_function(func_name,
                           [pa.scalar(10), pa.array([1, 2, 3]),
                            pa.scalar(20)])
    assert res == pa.array([30, 40, 50])

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
from pyarrow.compute import register_function
from pyarrow.compute import Arity, InputType


def get_function_doc(summary: str, desc: str, arg_names: List[str],
                     options_class: str, options_required: bool = False):
    func_doc = {}
    func_doc["summary"] = summary
    func_doc["description"] = desc
    func_doc["arg_names"] = arg_names
    func_doc["options_class"] = options_class
    func_doc["options_required"] = False
    return func_doc

# scalar unary function data


unary_doc = get_function_doc("add function",
                             "test add function",
                             ["scalar1"],
                             "None")


def unary_function(scalar1):
    return pc.call_function("add", [scalar1, 1])

# scalar binary function data


binary_doc = get_function_doc("y=mx",
                              "find y from y = mx",
                              ["m", "x"],
                              "None")


def binary_function(m, x):
    return pc.call_function("multiply", [m, x])

# scalar ternary function data


ternary_doc = get_function_doc("y=mx+c",
                               "find y from y = mx + c",
                               ["m", "x", "c"],
                               "None")


def ternary_function(m, x, c):
    mx = pc.call_function("multiply", [m, x])
    return pc.call_function("add", [mx, c])

# scalar varargs function data


varargs_doc = get_function_doc("z=ax+by+c",
                               "find z from z = ax + by + c",
                               ["a", "x", "b", "y", "c"],
                               "None")


def varargs_function(a, x, b, y, c):
    ax = pc.call_function("multiply", [a, x])
    by = pc.call_function("multiply", [b, y])
    ax_by = pc.call_function("add", [ax, by])
    return pc.call_function("add", [ax_by, c])


@pytest.fixture
def function_input_types():
    return [
        # scalar data input types
        [
            InputType.scalar(pa.int64())
        ],
        [
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64())
        ],
        [
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64())
        ],
        [
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64()),
            InputType.scalar(pa.int64())
        ],
        # array data input types
        [
            InputType.array(pa.int64())
        ],
        [
            InputType.array(pa.int64()),
            InputType.array(pa.int64())
        ],
        [
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
            InputType.array(pa.int64())
        ],
        [
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
            InputType.array(pa.int64()),
            InputType.array(pa.int64())
        ]
    ]


@pytest.fixture
def function_output_types():
    return [
        pa.int64(),
        pa.int64(),
        pa.int64(),
        pa.int64()
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
        Arity.unary(),
        Arity.binary(),
        Arity.ternary(),
        Arity.varargs(5),
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

        register_function(name, arity, doc, in_types, out_type, function)

        func = pc.get_function(name)
        assert func.name == name

        result = pc.call_function(name, input)
        assert result == expected_output

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

# import importlib
# import sys

# # list of Cython modules containing tests
# cython_test_modules = ["pyarrow._pyarrow_cpp_tests"]

# for mod in cython_test_modules:
#     # For each callable in `mod` with name `test_*`,
#     # set the result as an attribute of this module.
#     mod = importlib.import_module(mod)
#     for name in dir(mod):
#         item = getattr(mod, name)
#         if callable(item) and name.startswith("test_"):
#             setattr(sys.modules[__name__], name, item)

import pyarrow._pyarrow_cpp_tests as t  # noqa

import pytest
import sys


def test_owned_ref_moves():
    t.test_TestOwnedRefMoves()


def test_owned_ref_no_gil_moves():
    t.test_TestOwnedRefNoGILMoves()


def test_check_py_error_status():
    t.test_TestCheckPyErrorStatus()


def test_check_py_error_status_no_gil():
    t.test_TestCheckPyErrorStatusNoGIL()


def test_restore_py_error_basics():
    t.test_TestRestorePyErrorBasics()


def test_py_buffer_invalid_input_object():
    t.test_TestPyBufferInvalidInputObject()


def test_py_buffer_numpy_array():
    if sys.platform == 'win32':
        pytest.skip("C++ test are skipped on Windows due to "
                    "the Numpy C API instance not being visible")
    t.test_TestPyBufferNumpyArray()


def test_numpy_buffer_numpy_array():
    if sys.platform == 'win32':
        pytest.skip("C++ test are skipped on Windows due to "
                    "the Numpy C API instance not being visible")
    t.test_TestNumPyBufferNumpyArray()


def test_python_decimal_to_string():
    t.test_TestPythonDecimalToString()


def test_infer_precision_and_scale():
    t.test_TestInferPrecisionAndScale()


def test_infer_precision_and_negative_scale():
    t.test_TestInferPrecisionAndNegativeScale()


def test_infer_all_leading_zeros():
    t.test_TestInferAllLeadingZeros()


def test_infer_all_leading_zeros_e_pos():
    t.test_TestInferAllLeadingZerosExponentialNotationPositive()


def test_infer_all_leading_zeros_e_neg():
    t.test_TestInferAllLeadingZerosExponentialNotationNegative()


def test_object_block_write_fails():
    t.test_TestObjectBlockWriteFails()


def test_mixed_type_fails():
    t.test_TestMixedTypeFails()


def test_from_python_decimal_rescale_not_truncateable():
    t.test_TestFromPythonDecimalRescaleNotTruncateable()


def test_from_python_decimal_rescale_truncateable():
    t.test_TestFromPythonDecimalRescaleTruncateable()


def test_from_python_negative_decimal_rescale():
    t.test_TestFromPythonNegativeDecimalRescale()


def test_decimal_128_from_python_integer():
    t.test_TestDecimal128FromPythonInteger()


def test_decimal_256_from_python_integer():
    t.test_TestDecimal256FromPythonInteger()


def test_decimal_128_overflow_fails():
    t.test_TestDecimal128OverflowFails()


def test_decimal_256_overflow_fails():
    t.test_TestDecimal256OverflowFails()


def test_none_and_nan():
    t.test_TestNoneAndNaN()


def test_mixed_precision_and_scale():
    t.test_TestMixedPrecisionAndScale()


def test_simple_inference():
    t.test_TestSimpleInference()


def test_update_with_nan():
    t.test_TestUpdateWithNaN()

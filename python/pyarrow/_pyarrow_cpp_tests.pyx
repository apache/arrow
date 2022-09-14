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

# cython: profile=False, binding=True
# distutils: language = c++

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (check_status)

from decimal import Decimal


def test_PythonDecimalToString():
    cdef:
        c_string decimal_string = b'-39402950693754869342983'
        PyObject* python_object = DecimalFromString(<PyObject*>Decimal, decimal_string)
        c_string string_result

    # Should a check be added here that python_object != nullptr ?

    check_status(PythonDecimalToString(
        python_object,
        &string_result)
    )

    assert string_result == decimal_string


def test_InferPrecisionAndScale():
    cdef:
        c_string decimal_string = b'-394029506937548693.42983'
        PyObject* python_object = DecimalFromString(<PyObject*>Decimal, decimal_string)
        DecimalMetadata metadata
        int32_t expected_precision = <int32_t>(decimal_string.size()) - 2 # 1 for -, 1 for .
        int32_t expected_scale = 5

    check_status(metadata.Update(
        python_object)
    )

    assert expected_precision == metadata.precision()
    assert expected_scale == metadata.scale()


def test_InferPrecisionAndNegativeScale():
    cdef:
        c_string decimal_string = b'-3.94042983E+10'
        PyObject* python_object = DecimalFromString(<PyObject*>Decimal, decimal_string)
        DecimalMetadata metadata
        int32_t expected_precision = 11
        int32_t expected_scale = 0

    check_status(metadata.Update(
        python_object)
    )

    assert expected_precision == metadata.precision()
    assert expected_scale == metadata.scale()


def test_TestInferAllLeadingZeros():
    cdef:
        c_string decimal_string = b'0.001'
        PyObject* python_object = DecimalFromString(<PyObject*>Decimal, decimal_string)
        DecimalMetadata metadata
        int32_t expected_precision = 3
        int32_t expected_scale = 3

    check_status(metadata.Update(
        python_object)
    )

    assert expected_precision == metadata.precision()
    assert expected_scale == metadata.scale()


def test_TestInferAllLeadingZerosExponentialNotationPositive():
    cdef:
        c_string decimal_string = b'0.01E5'
        PyObject* python_object = DecimalFromString(<PyObject*>Decimal, decimal_string)
        DecimalMetadata metadata
        int32_t expected_precision = 4
        int32_t expected_scale = 0

    check_status(metadata.Update(
        python_object)
    )

    assert expected_precision == metadata.precision()
    assert expected_scale == metadata.scale()


def test_TestInferAllLeadingZerosExponentialNotationNegative():
    cdef:
        c_string decimal_string = b'0.01E3'
        PyObject* python_object = DecimalFromString(<PyObject*>Decimal, decimal_string)
        DecimalMetadata metadata
        int32_t expected_precision = 2
        int32_t expected_scale = 0

    check_status(metadata.Update(
        python_object)
    )

    assert expected_precision == metadata.precision()
    assert expected_scale == metadata.scale()

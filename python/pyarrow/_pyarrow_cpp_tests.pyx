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


def test_TestOwnedRefMoves():
    check_status(TestOwnedRefMoves())


def test_TestOwnedRefNoGILMoves():
    check_status(TestOwnedRefNoGILMoves())


def test_TestCheckPyErrorStatus():
    check_status(TestCheckPyErrorStatus())


def test_TestCheckPyErrorStatusNoGIL():
    check_status(TestCheckPyErrorStatusNoGIL())


def test_TestRestorePyErrorBasics():
    check_status(TestRestorePyErrorBasics())


def test_TestPyBufferInvalidInputObject():
    check_status(TestPyBufferInvalidInputObject())


# ifdef _WIN32
def test_TestPyBufferNumpyArray():
    check_status(TestPyBufferNumpyArray())


def test_TestNumPyBufferNumpyArray():
    check_status(TestNumPyBufferNumpyArray())
# endif


def test_TestPythonDecimalToString():
    check_status(TestPythonDecimalToString())


def test_TestInferPrecisionAndScale():
    check_status(TestInferPrecisionAndScale())


def test_TestInferPrecisionAndNegativeScale():
    check_status(TestInferPrecisionAndNegativeScale())


def test_TestInferAllLeadingZeros():
    check_status(TestInferAllLeadingZeros())


def test_TestInferAllLeadingZerosExponentialNotationPositive():
    check_status(TestInferAllLeadingZerosExponentialNotationPositive())


def test_TestInferAllLeadingZerosExponentialNotationNegative():
    check_status(TestInferAllLeadingZerosExponentialNotationNegative())


def test_TestObjectBlockWriteFails():
    check_status(TestObjectBlockWriteFails())


def test_TestMixedTypeFails():
    check_status(TestMixedTypeFails())


def test_TestFromPythonDecimalRescaleNotTruncateable():
    check_status(TestFromPythonDecimalRescaleNotTruncateable())


def test_TestFromPythonDecimalRescaleTruncateable():
    check_status(TestFromPythonDecimalRescaleTruncateable())


def test_TestFromPythonNegativeDecimalRescale():
    check_status(TestFromPythonNegativeDecimalRescale())


def test_TestDecimal128FromPythonInteger():
    check_status(TestDecimal128FromPythonInteger())


def test_TestDecimal256FromPythonInteger():
    check_status(TestDecimal256FromPythonInteger())


def test_TestDecimal128OverflowFails():
    check_status(TestDecimal128OverflowFails())


def test_TestDecimal256OverflowFails():
    check_status(TestDecimal256OverflowFails())


def test_TestNoneAndNaN():
    check_status(TestNoneAndNaN())


def test_TestMixedPrecisionAndScale():
    check_status(TestMixedPrecisionAndScale())


def test_TestMixedPrecisionAndScaleSequenceConvert():
    check_status(TestMixedPrecisionAndScaleSequenceConvert())


def test_TestSimpleInference():
    check_status(TestSimpleInference())


def test_TestUpdateWithNaN():
    check_status(TestUpdateWithNaN())

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "visibility.h"
#include "arrow/type.h"

namespace arrow {
namespace py {

ARROW_PYTHON_EXPORT
Status TestOwnedRefMoves();

ARROW_PYTHON_EXPORT
Status TestOwnedRefNoGILMoves();

ARROW_PYTHON_EXPORT
Status TestCheckPyErrorStatus();

ARROW_PYTHON_EXPORT
Status TestCheckPyErrorStatusNoGIL();

ARROW_PYTHON_EXPORT
Status TestRestorePyErrorBasics();

ARROW_PYTHON_EXPORT
Status TestPyBufferInvalidInputObject();

#ifndef _WIN32
ARROW_PYTHON_EXPORT
Status TestPyBufferNumpyArray();

ARROW_PYTHON_EXPORT
Status TestNumPyBufferNumpyArray();
#endif

ARROW_PYTHON_EXPORT
Status TestPythonDecimalToString();

ARROW_PYTHON_EXPORT
Status TestInferPrecisionAndScale();

ARROW_PYTHON_EXPORT
Status TestInferPrecisionAndNegativeScale();

ARROW_PYTHON_EXPORT
Status TestInferAllLeadingZeros();

ARROW_PYTHON_EXPORT
Status TestInferAllLeadingZerosExponentialNotationPositive();

ARROW_PYTHON_EXPORT
Status TestInferAllLeadingZerosExponentialNotationNegative();

ARROW_PYTHON_EXPORT
Status TestObjectBlockWriteFails();

ARROW_PYTHON_EXPORT
Status TestMixedTypeFails();

ARROW_PYTHON_EXPORT
Status TestFromPythonDecimalRescaleNotTruncateable();

ARROW_PYTHON_EXPORT
Status TestFromPythonDecimalRescaleTruncateable();

ARROW_PYTHON_EXPORT
Status TestFromPythonNegativeDecimalRescale();

ARROW_PYTHON_EXPORT
Status TestDecimal128FromPythonInteger();

ARROW_PYTHON_EXPORT
Status TestDecimal256FromPythonInteger();

ARROW_PYTHON_EXPORT
Status TestDecimal128OverflowFails();

ARROW_PYTHON_EXPORT
Status TestDecimal256OverflowFails();

ARROW_PYTHON_EXPORT
Status TestNoneAndNaN();

ARROW_PYTHON_EXPORT
Status TestMixedPrecisionAndScale();

ARROW_PYTHON_EXPORT
Status TestMixedPrecisionAndScaleSequenceConvert();

ARROW_PYTHON_EXPORT
Status TestSimpleInference();

ARROW_PYTHON_EXPORT
Status TestUpdateWithNaN();

}  // namespace py
}  // namespace arrow

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

// Utilities for converting arrow to python (non-pandas) objects.
#pragma once

#include "arrow/python/common.h"
#include "arrow/python/platform.h"

namespace arrow {

class Array;
struct Scalar;

namespace py {

/// \brief Utility class for converting Arrow to Python obects.
///
/// A class is chosen because in the future some amount of state will be
/// (e.g. imported python classes), doing this once lazily is helpful.
/// A class allows for keeping the state as member variables instead of
/// static variables. It is expected cython code will instantiate this
/// class as a singleton on module class.
class ARROW_PYTHON_EXPORT ArrowToPython {
 public:
  /// \brief Converts the given Array to a PyList object.
  ///
  /// The list consists of the same as calling ToPyObject on each scalar
  /// in the array..
  ///
  /// N.B. This has limited type support.  ARROW-12976 tracks extending the
  /// implementation.
  Result<PyObject*> ToPyList(const Array& array);

  /// \brief Converts the given Scalar the type to its logical equivalent type
  /// in python.
  ///
  /// For instance Decimal128 and Decimal256 would be converted to
  /// decimal.Decimal.
  ///
  /// N.B. This has limited type support.  ARROW-12976 tracks full implementation.
  Result<PyObject*> ToPyObject(const Scalar& scalar);
};

}  // namespace py
}  // namespace arrow

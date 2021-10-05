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

// Functions for converting between pandas's NumPy-based data representation
// and Arrow data structures

#pragma once

#include "arrow/python/common.h"
#include "arrow/python/platform.h"

namespace arrow {

class Array;
class Scalar;

namespace py {

/// \brief Utility class for converting Arrow to Python obects.  A class instead
///
/// A class is chosen because in the future some amount of state will be
/// (e.g. imported python classes), doing this one lazily will be helpful
/// and having members present avoids static C++ variables.
class ARROW_PYTHON_EXPORT ArrowToPython {
 public:
  /// \brief Converts the given Array to a PyList object. Returns NULL if there
  /// is an error converting the Array. The list elements are the same ones
  /// generated via ToLogical()
  ///
  /// N.B. This has limited type support.  ARROW-12976 tracks extending the implementation.
  Result<PyObject*> ToPyList(const Array& array);

  /// \brief Converts the given Scalar the type that is closest to its arrow
  /// representation.
  ///
  /// For instance timestamp would be translated to a integer representing an
  // offset from the unix epoch.
  ///
  /// N.B. This has limited type support.  ARROW-12976 tracks full implementation.
  Result<PyObject*> ToPrimitive(const Scalar& scalar);
};

}  // namespace py
}  // namespace arrow

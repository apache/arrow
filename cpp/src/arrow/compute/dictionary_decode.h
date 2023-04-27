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

#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/function.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

namespace compute {

class ExecContext;

// ----------------------------------------------------------------------
// Convenience invocation APIs for a number of kernels

/// \brief decode a dictionary encoded array to normal array
/// \param[in] value array to cast
/// \param[in] ctx the function execution context, optional
/// \return the resulting array
///
ARROW_EXPORT
Result<std::shared_ptr<Array>> DictionaryDecode(const Array& value,
                                                ExecContext* ctx = NULLPTR) {
  ARROW_ASSIGN_OR_RAISE(Datum result, DictionaryDecode(Datum(value), ctx));
  return result.make_array();
}

/// \brief decode a dictionary encoded array to normal array
/// \param[in] value array to cast
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
Result<Datum> DictionaryDecode(const Datum& value, ExecContext* ctx = NULLPTR) {
  return CallFunction("dictionary_decode", {value}, ctx);
}

}  // namespace compute
}  // namespace arrow

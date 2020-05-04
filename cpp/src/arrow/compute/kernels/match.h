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

#include "arrow/array.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

/// \brief Match examines each slot in the haystack against a needles array.
/// If the value is not found in needles, null will be output.
/// If found, the index of occurrence within needles (ignoring duplicates)
/// will be output.
///
/// For example given haystack = [99, 42, 3, null] and
/// needles = [3, 3, 99], the output will be = [1, null, 0, null]
///
/// Note: Null in the haystack is considered to match
/// a null in the needles array. For example given
/// haystack = [99, 42, 3, null] and needles = [3, 99, null],
/// the output will be = [1, null, 0, 2]
///
/// \param[in] context the FunctionContext
/// \param[in] haystack array-like input
/// \param[in] needles array-like input
/// \param[out] out resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Status Match(FunctionContext* context, const Datum& haystack, const Datum& needles,
             Datum* out);

}  // namespace compute
}  // namespace arrow

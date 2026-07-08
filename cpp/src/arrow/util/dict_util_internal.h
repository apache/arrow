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

#include "arrow/array/data.h"

namespace arrow {
namespace dict_util {

/// \brief Compute the logical null count of a dictionary-encoded array
///
/// This function considers a value to be a logical null if its index is null or if
/// it points to a null entry in the dictionary's validity bitmap. This means that it
/// only accounts for physical nulls in the dictionary. It does not recurse into the
/// dictionary values to find logical nulls, which would matter when those values are
/// themselves dictionary, run-end encoded, or union typed.
int64_t LogicalNullCount(const ArraySpan& span);

/// \brief Populate a bitmap based on the logical nulls in a dictionary-encoded array
///
/// \see LogicalNullCount for how nulls in the dictionary are handled.
///
/// \param set_on_null true if we should set bits corresponding to nulls and false if
/// we should set bits corresponding to non-nulls
ARROW_EXPORT
void SetLogicalNullBits(const ArraySpan& span, uint8_t* out_bitmap, int64_t out_offset,
                        bool set_on_null);

}  // namespace dict_util
}  // namespace arrow

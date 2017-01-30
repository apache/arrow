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

// Functions for comparing Arrow data structures

#ifndef ARROW_COMPARE_H
#define ARROW_COMPARE_H

#include <cstdint>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Status;

/// Returns true if the arrays are exactly equal
Status ARROW_EXPORT ArrayEquals(const Array& left, const Array& right, bool* are_equal);

/// Returns true if the arrays are approximately equal. For non-floating point
/// types, this is equivalent to ArrayEquals(left, right)
Status ARROW_EXPORT ArrayApproxEquals(
    const Array& left, const Array& right, bool* are_equal);

/// Returns true if indicated equal-length segment of arrays is exactly equal
Status ARROW_EXPORT ArrayRangeEquals(const Array& left, const Array& right,
    int32_t start_idx, int32_t end_idx, int32_t other_start_idx, bool* are_equal);

}  // namespace arrow

#endif  // ARROW_COMPARE_H

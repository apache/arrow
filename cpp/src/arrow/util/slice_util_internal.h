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

#include "arrow/util/int_util_overflow.h"

namespace arrow {
namespace internal {

static inline Status CheckSliceParams(int64_t object_length, int64_t slice_offset,
                                      int64_t slice_length, const char* object_name) {
  if (ARROW_PREDICT_FALSE(slice_offset < 0)) {
    return Status::IndexError("Negative ", object_name, " slice offset");
  }
  if (ARROW_PREDICT_FALSE(slice_length < 0)) {
    return Status::IndexError("Negative ", object_name, " slice length");
  }
  int64_t offset_plus_length;
  if (ARROW_PREDICT_FALSE(
          internal::AddWithOverflow(slice_offset, slice_length, &offset_plus_length))) {
    return Status::IndexError(object_name, " slice would overflow");
  }
  if (ARROW_PREDICT_FALSE(offset_plus_length > object_length)) {
    return Status::IndexError(object_name, " slice would exceed ", object_name,
                              " length");
  }
  return Status::OK();
}

}  // namespace internal
}  // namespace arrow

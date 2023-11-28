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

#include "arrow/array/array_nested.h"
#include "arrow/tensor.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/sort.h"

namespace arrow {
namespace internal {

inline Status ComputeStrides(const std::shared_ptr<DataType>& value_type,
                             const std::vector<int64_t>& shape,
                             const std::vector<int64_t>& permutation,
                             std::vector<int64_t>* strides) {
  const auto fixed_width_type =
      internal::checked_pointer_cast<FixedWidthType>(value_type);
  if (permutation.empty()) {
    return internal::ComputeRowMajorStrides(*fixed_width_type.get(), shape, strides);
  }
  const int byte_width = value_type->byte_width();

  int64_t remaining = 0;
  if (!shape.empty() && shape.front() > 0) {
    remaining = byte_width;
    for (auto i : permutation) {
      if (i > 0) {
        if (internal::MultiplyWithOverflow(remaining, shape[i], &remaining)) {
          return Status::Invalid(
              "Strides computed from shape would not fit in 64-bit integer");
        }
      }
    }
  }

  if (remaining == 0) {
    strides->assign(shape.size(), byte_width);
    return Status::OK();
  }

  strides->push_back(remaining);
  for (auto i : permutation) {
    if (i > 0) {
      remaining /= shape[i];
      strides->push_back(remaining);
    }
  }
  Permute(permutation, strides);

  return Status::OK();
}

}  // namespace internal
}  // namespace arrow

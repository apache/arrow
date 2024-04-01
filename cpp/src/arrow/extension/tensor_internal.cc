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

#include "arrow/extension/tensor_internal.h"

#include "arrow/tensor.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/sort.h"

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/print.h"

namespace arrow::internal {

Status IsPermutationValid(const std::vector<int64_t>& permutation) {
  const auto size = static_cast<int64_t>(permutation.size());
  std::vector<uint8_t> dim_seen(size, 0);

  for (const auto p : permutation) {
    if (p < 0 || p >= size || dim_seen[p] != 0) {
      return Status::Invalid(
          "Permutation indices for ", size,
          " dimensional tensors must be unique and within [0, ", size - 1,
          "] range. Got: ", ::arrow::internal::PrintVector{permutation, ","});
    }
    dim_seen[p] = 1;
  }
  return Status::OK();
}

Result<std::vector<int64_t>> ComputeStrides(const std::shared_ptr<DataType>& value_type,
                                            const std::vector<int64_t>& shape,
                                            const std::vector<int64_t>& permutation) {
  const auto& fw_type = checked_cast<const FixedWidthType&>(*value_type);
  std::vector<int64_t> strides;

  if (permutation.empty()) {
    ARROW_DCHECK_OK(internal::ComputeRowMajorStrides(fw_type, shape, &strides));
    return strides;
  }

  auto permuted_shape = std::move(shape);
  auto reverse_permutation = internal::ArgSort(permutation, std::greater<>());
  Permute(permutation, &permuted_shape);
  ARROW_DCHECK_OK(internal::ComputeRowMajorStrides(fw_type, permuted_shape, &strides));
  Permute(reverse_permutation, &strides);
  std::reverse(strides.begin(), strides.end());

  return strides;
}
}  // namespace arrow::internal

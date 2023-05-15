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

#include <cstdint>

#include "arrow/array/data.h"
#include "arrow/compute/api_vector.h"
#include "arrow/result.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/ree_util.h"

// Filtering from and using run-end encoded filter arrays.
//
// Used by vector_selection.cc to implement the actual selection compute kernels.

namespace arrow::compute::internal {

/// \brief Common virtual base class for filter functions that involve run-end
/// encoded arrays on one or both operands.
class REEFilterExec {
 public:
  virtual ~REEFilterExec() = default;

  /// \brief Calculate the physical size of the values nested in the run-end
  /// encoded output.
  virtual Result<int64_t> CalculateOutputSize() = 0;

  virtual Status Exec(ArrayData* out) = 0;
};

Result<std::unique_ptr<REEFilterExec>> MakeREExREEFilterExec(
    MemoryPool* pool, const ArraySpan& values, const ArraySpan& filter,
    const FilterOptions& options);

Result<std::unique_ptr<REEFilterExec>> MakeREExPlainFilterExec(
    MemoryPool* pool, const ArraySpan& values, const ArraySpan& filter,
    const FilterOptions& options);

Result<std::unique_ptr<REEFilterExec>> MakePlainxREEFilterExec(
    MemoryPool* pool, const ArraySpan& values, const ArraySpan& filter,
    const FilterOptions& options);

Status REExREEFilterExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result);

Status REExPlainFilterExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result);

Status PlainxREEFilterExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result);

}  // namespace arrow::compute::internal

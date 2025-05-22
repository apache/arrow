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
#include <vector>

#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/type_fwd.h"

namespace arrow::compute::internal {

// aggregate_basic.cc

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func,
                        SimdLevel::type simd_level = SimdLevel::NONE);

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func,
                      SimdLevel::type simd_level = SimdLevel::NONE);
void AddMinMaxKernel(KernelInit init, internal::detail::GetTypeId get_id,
                     ScalarAggregateFunction* func,
                     SimdLevel::type simd_level = SimdLevel::NONE);

// aggregate_basic_avx2.cc

void AddSumAvx2AggKernels(ScalarAggregateFunction* func);
void AddMeanAvx2AggKernels(ScalarAggregateFunction* func);
void AddMinMaxAvx2AggKernels(ScalarAggregateFunction* func);

// aggregate_basic_avx512.cc

void AddSumAvx512AggKernels(ScalarAggregateFunction* func);
void AddMeanAvx512AggKernels(ScalarAggregateFunction* func);
void AddMinMaxAvx512AggKernels(ScalarAggregateFunction* func);

}  // namespace arrow::compute::internal

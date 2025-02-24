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
#include "arrow/compute/registry.h"
#include "arrow/compute/kernels/registry.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "arrow/compute/function.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/status.h"
#include "arrow/util/config.h"  // For ARROW_COMPUTE
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {
namespace internal {

Status RegisterComputeKernels() {
  auto registry = GetFunctionRegistry();

  // Register additional kernels on libarrow_compute
  // Scalar functions
  RegisterScalarArithmetic(registry);
  RegisterScalarBoolean(registry);
  RegisterScalarComparison(registry);
  RegisterScalarIfElse(registry);
  RegisterScalarNested(registry);
  RegisterScalarRandom(registry);  // Nullary
  RegisterScalarRoundArithmetic(registry);
  RegisterScalarSetLookup(registry);
  RegisterScalarStringAscii(registry);
  RegisterScalarStringUtf8(registry);
  RegisterScalarTemporalBinary(registry);
  RegisterScalarTemporalUnary(registry);
  RegisterScalarValidity(registry);

  // Vector functions
  RegisterVectorArraySort(registry);
  RegisterVectorCumulativeSum(registry);
  RegisterVectorNested(registry);
  RegisterVectorRank(registry);
  RegisterVectorReplace(registry);
  RegisterVectorSelectK(registry);
  RegisterVectorSort(registry);
  RegisterVectorPairwise(registry);
  RegisterVectorSwizzle(registry);

  // Aggregate functions
  RegisterHashAggregateBasic(registry);
  RegisterScalarAggregateBasic(registry);
  RegisterScalarAggregateMode(registry);
  RegisterScalarAggregatePivot(registry.get());
  RegisterScalarAggregateQuantile(registry);
  RegisterScalarAggregateTDigest(registry);
  RegisterScalarAggregateVariance(registry);

  return Status::OK();
}
}  // namespace internal
}  // namespace compute
}  // namespace arrow

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
#include "arrow/compute/initialize.h"

#include "arrow/compute/registry_internal.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/status.h"

namespace arrow::compute {
namespace {

Status RegisterComputeKernels() {
  auto registry = GetFunctionRegistry();

  // Register additional kernels on libarrow_compute
  // Scalar functions
  internal::RegisterScalarArithmetic(registry);
  internal::RegisterScalarBoolean(registry);
  internal::RegisterScalarComparison(registry);
  internal::RegisterScalarIfElse(registry);
  internal::RegisterScalarNested(registry);
  internal::RegisterScalarRandom(registry);  // Nullary
  internal::RegisterScalarRoundArithmetic(registry);
  internal::RegisterScalarSetLookup(registry);
  internal::RegisterScalarStringAscii(registry);
  internal::RegisterScalarStringUtf8(registry);
  internal::RegisterScalarTemporalBinary(registry);
  internal::RegisterScalarTemporalUnary(registry);
  internal::RegisterScalarValidity(registry);

  // Vector functions
  internal::RegisterVectorArraySort(registry);
  internal::RegisterVectorCumulativeSum(registry);
  internal::RegisterVectorNested(registry);
  internal::RegisterVectorRank(registry);
  internal::RegisterVectorReplace(registry);
  internal::RegisterVectorSelectK(registry);
  internal::RegisterVectorSort(registry);
  internal::RegisterVectorRunEndEncode(registry);
  internal::RegisterVectorRunEndDecode(registry);
  internal::RegisterVectorPairwise(registry);
  internal::RegisterVectorStatistics(registry);
  internal::RegisterVectorSwizzle(registry);

  // Aggregate functions
  internal::RegisterHashAggregateBasic(registry);
  internal::RegisterHashAggregateNumeric(registry);
  internal::RegisterHashAggregatePivot(registry);
  internal::RegisterScalarAggregateBasic(registry);
  internal::RegisterScalarAggregateMode(registry);
  internal::RegisterScalarAggregatePivot(registry);
  internal::RegisterScalarAggregateQuantile(registry);
  internal::RegisterScalarAggregateTDigest(registry);
  internal::RegisterScalarAggregateVariance(registry);

  return Status::OK();
}

}  // namespace

Status Initialize() {
  static auto st = RegisterComputeKernels();
  return st;
}

}  // namespace arrow::compute

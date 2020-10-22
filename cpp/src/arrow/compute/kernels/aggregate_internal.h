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

#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {

// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, enable_if_boolean<I>> {
  using Type = UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_signed_integer<I>> {
  using Type = Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_unsigned_integer<I>> {
  using Type = UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_floating_point<I>> {
  using Type = DoubleType;
};

struct ScalarAggregator : public KernelState {
  virtual void Consume(KernelContext* ctx, const ExecBatch& batch) = 0;
  virtual void MergeFrom(KernelContext* ctx, KernelState&& src) = 0;
  virtual void Finalize(KernelContext* ctx, Datum* out) = 0;
};

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFunction* func,
                  SimdLevel::type simd_level = SimdLevel::NONE);

}  // namespace compute
}  // namespace arrow

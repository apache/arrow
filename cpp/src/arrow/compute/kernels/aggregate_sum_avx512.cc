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

#include "arrow/compute/kernels/aggregate_basic_internal.h"

namespace arrow {
namespace compute {
namespace aggregate {

// ----------------------------------------------------------------------
// Sum implementation

// Round size optimized based on data type and compiler
template <typename T>
struct RoundSizeAvx512 {
  static constexpr int64_t size = 32;
};

// Round size set to 64 for float/int32_t/uint32_t
template <>
struct RoundSizeAvx512<float> {
  static constexpr int64_t size = 64;
};

template <>
struct RoundSizeAvx512<int32_t> {
  static constexpr int64_t size = 64;
};

template <>
struct RoundSizeAvx512<uint32_t> {
  static constexpr int64_t size = 64;
};

template <typename ArrowType>
struct SumImplAvx512
    : public SumImpl<RoundSizeAvx512<typename TypeTraits<ArrowType>::CType>::size,
                     ArrowType, SimdLevel::AVX512> {};

template <typename ArrowType>
struct MeanImplAvx512
    : public MeanImpl<RoundSizeAvx512<typename TypeTraits<ArrowType>::CType>::size,
                      ArrowType, SimdLevel::AVX512> {};

std::unique_ptr<KernelState> SumInitAvx512(KernelContext* ctx,
                                           const KernelInitArgs& args) {
  SumLikeInit<SumImplAvx512> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

std::unique_ptr<KernelState> MeanInitAvx512(KernelContext* ctx,
                                            const KernelInitArgs& args) {
  SumLikeInit<MeanImplAvx512> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

void AddSumAvx512AggKernels(ScalarAggregateFunction* func) {
  AddBasicAggKernels(SumInitAvx512, internal::SignedIntTypes(), int64(), func,
                     SimdLevel::AVX512);
  AddBasicAggKernels(SumInitAvx512, internal::UnsignedIntTypes(), uint64(), func,
                     SimdLevel::AVX512);
  AddBasicAggKernels(SumInitAvx512, internal::FloatingPointTypes(), float64(), func,
                     SimdLevel::AVX512);
}

void AddMeanAvx512AggKernels(ScalarAggregateFunction* func) {
  aggregate::AddBasicAggKernels(MeanInitAvx512, internal::NumericTypes(), float64(), func,
                                SimdLevel::AVX512);
}

}  // namespace aggregate
}  // namespace compute
}  // namespace arrow

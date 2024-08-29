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
namespace internal {

// ----------------------------------------------------------------------
// Sum implementation

template <typename ArrowType>
struct SumImplAvx512 : public SumImpl<ArrowType, SimdLevel::AVX512> {
  using SumImpl<ArrowType, SimdLevel::AVX512>::SumImpl;
};

template <typename ArrowType>
struct MeanImplAvx512 : public MeanImpl<ArrowType, SimdLevel::AVX512> {
  using MeanImpl<ArrowType, SimdLevel::AVX512>::MeanImpl;
};

Result<std::unique_ptr<KernelState>> SumInitAvx512(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
  SumLikeInit<SumImplAvx512> visitor(
      ctx, args.inputs[0].GetSharedPtr(),
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

Result<std::unique_ptr<KernelState>> MeanInitAvx512(KernelContext* ctx,
                                                    const KernelInitArgs& args) {
  SumLikeInit<MeanImplAvx512> visitor(
      ctx, args.inputs[0].GetSharedPtr(),
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

// ----------------------------------------------------------------------
// MinMax implementation

Result<std::unique_ptr<KernelState>> MinMaxInitAvx512(KernelContext* ctx,
                                                      const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(TypeHolder out_type,
                        args.kernel->signature->out_type().Resolve(ctx, args.inputs));
  MinMaxInitState<SimdLevel::AVX512> visitor(
      ctx, *args.inputs[0], out_type.GetSharedPtr(),
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

void AddSumAvx512AggKernels(ScalarAggregateFunction* func) {
  AddBasicAggKernels(SumInitAvx512, SignedIntTypes(), int64(), func, SimdLevel::AVX512);
  AddBasicAggKernels(SumInitAvx512, UnsignedIntTypes(), uint64(), func,
                     SimdLevel::AVX512);
  AddBasicAggKernels(SumInitAvx512, FloatingPointTypes(), float64(), func,
                     SimdLevel::AVX512);
}

void AddMeanAvx512AggKernels(ScalarAggregateFunction* func) {
  AddBasicAggKernels(MeanInitAvx512, NumericTypes(), float64(), func, SimdLevel::AVX512);
}

void AddMinMaxAvx512AggKernels(ScalarAggregateFunction* func) {
  // Enable 32/64 int types for avx512 variants, no advantage on 8/16 int.
  AddMinMaxKernels(MinMaxInitAvx512, {int32(), uint32(), int64(), uint64()}, func,
                   SimdLevel::AVX512);
  AddMinMaxKernels(MinMaxInitAvx512, TemporalTypes(), func, SimdLevel::AVX512);
  AddMinMaxKernels(MinMaxInitAvx512, BaseBinaryTypes(), func, SimdLevel::AVX2);
  AddMinMaxKernel(MinMaxInitAvx512, Type::FIXED_SIZE_BINARY, func, SimdLevel::AVX2);
  AddMinMaxKernel(MinMaxInitAvx512, Type::INTERVAL_MONTHS, func, SimdLevel::AVX512);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

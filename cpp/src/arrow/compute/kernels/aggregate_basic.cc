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

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_basic_internal.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace compute {
namespace aggregate {

void AggregateConsume(KernelContext* ctx, const ExecBatch& batch) {
  checked_cast<ScalarAggregator*>(ctx->state())->Consume(ctx, batch);
}

void AggregateMerge(KernelContext* ctx, KernelState&& src, KernelState* dst) {
  checked_cast<ScalarAggregator*>(dst)->MergeFrom(ctx, std::move(src));
}

void AggregateFinalize(KernelContext* ctx, Datum* out) {
  checked_cast<ScalarAggregator*>(ctx->state())->Finalize(ctx, out);
}

// ----------------------------------------------------------------------
// Count implementation

struct CountImpl : public ScalarAggregator {
  explicit CountImpl(CountOptions options) : options(std::move(options)) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    const ArrayData& input = *batch[0].array();
    const int64_t nulls = input.GetNullCount();
    this->nulls += nulls;
    this->non_nulls += input.length - nulls;
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other_state = checked_cast<const CountImpl&>(src);
    this->non_nulls += other_state.non_nulls;
    this->nulls += other_state.nulls;
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    const auto& state = checked_cast<const CountImpl&>(*ctx->state());
    switch (state.options.count_mode) {
      case CountOptions::COUNT_NON_NULL:
        *out = Datum(state.non_nulls);
        break;
      case CountOptions::COUNT_NULL:
        *out = Datum(state.nulls);
        break;
      default:
        ctx->SetStatus(Status::Invalid("Unknown CountOptions encountered"));
        break;
    }
  }

  CountOptions options;
  int64_t non_nulls = 0;
  int64_t nulls = 0;
};

std::unique_ptr<KernelState> CountInit(KernelContext*, const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<CountImpl>(
      static_cast<const CountOptions&>(*args.options));
}

// ----------------------------------------------------------------------
// Sum implementation

// Round size optimized based on data type and compiler
template <typename T>
struct RoundSizeDefault {
  static constexpr int64_t size = 16;
};

// Round size set to 32 for float/int32_t/uint32_t
template <>
struct RoundSizeDefault<float> {
  static constexpr int64_t size = 32;
};

template <>
struct RoundSizeDefault<int32_t> {
  static constexpr int64_t size = 32;
};

template <>
struct RoundSizeDefault<uint32_t> {
  static constexpr int64_t size = 32;
};

template <typename ArrowType>
struct SumImplDefault
    : public SumImpl<RoundSizeDefault<typename TypeTraits<ArrowType>::CType>::size,
                     ArrowType, SimdLevel::NONE> {};

template <typename ArrowType>
struct MeanImplDefault
    : public MeanImpl<RoundSizeDefault<typename TypeTraits<ArrowType>::CType>::size,
                      ArrowType, SimdLevel::NONE> {};

std::unique_ptr<KernelState> SumInit(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<SumImplDefault> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

std::unique_ptr<KernelState> MeanInit(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<MeanImplDefault> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

// ----------------------------------------------------------------------
// MinMax implementation

std::unique_ptr<KernelState> MinMaxInit(KernelContext* ctx, const KernelInitArgs& args) {
  MinMaxInitState<SimdLevel::NONE> visitor(
      ctx, *args.inputs[0].type, args.kernel->signature->out_type().type(),
      static_cast<const MinMaxOptions&>(*args.options));
  return visitor.Create();
}

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFunction* func, SimdLevel::type simd_level) {
  ScalarAggregateKernel kernel(std::move(sig), init, AggregateConsume, AggregateMerge,
                               AggregateFinalize);
  // Set the simd level
  kernel.simd_level = simd_level;
  DCHECK_OK(func->AddKernel(kernel));
}

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func,
                        SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    // array[InT] -> scalar[OutT]
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, simd_level);
  }
}

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func, SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    // array[T] -> scalar[struct<min: T, max: T>]
    auto out_ty = struct_({field("min", ty), field("max", ty)});
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, simd_level);
  }
}

}  // namespace aggregate

namespace internal {
void RegisterScalarAggregateBasic(FunctionRegistry* registry) {
  static auto default_count_options = CountOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("count", Arity::Unary(),
                                                        &default_count_options);

  // Takes any array input, outputs int64 scalar
  InputType any_array(ValueDescr::ARRAY);
  aggregate::AddAggKernel(KernelSignature::Make({any_array}, ValueDescr::Scalar(int64())),
                          aggregate::CountInit, func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary());
  aggregate::AddBasicAggKernels(aggregate::SumInit, {boolean()}, int64(), func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, SignedIntTypes(), int64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, UnsignedIntTypes(), uint64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, FloatingPointTypes(), float64(),
                                func.get());
  // Add the SIMD variants for sum
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddSumAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddSumAvx512AggKernels(func.get());
  }
#endif
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary());
  aggregate::AddBasicAggKernels(aggregate::MeanInit, {boolean()}, float64(), func.get());
  aggregate::AddBasicAggKernels(aggregate::MeanInit, NumericTypes(), float64(),
                                func.get());
  // Add the SIMD variants for mean
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddMeanAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddMeanAvx512AggKernels(func.get());
  }
#endif
  DCHECK_OK(registry->AddFunction(std::move(func)));

  static auto default_minmax_options = MinMaxOptions::Defaults();
  func = std::make_shared<ScalarAggregateFunction>("min_max", Arity::Unary(),
                                                   &default_minmax_options);
  aggregate::AddMinMaxKernels(aggregate::MinMaxInit, {boolean()}, func.get());
  aggregate::AddMinMaxKernels(aggregate::MinMaxInit, NumericTypes(), func.get());
  // Add the SIMD variants for min max
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddMinMaxAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddMinMaxAvx512AggKernels(func.get());
  }
#endif

  DCHECK_OK(registry->AddFunction(std::move(func)));

  DCHECK_OK(registry->AddFunction(aggregate::AddModeAggKernels()));
  DCHECK_OK(registry->AddFunction(aggregate::AddStddevAggKernels()));
  DCHECK_OK(registry->AddFunction(aggregate::AddVarianceAggKernels()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

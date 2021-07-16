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

namespace {

Status AggregateConsume(KernelContext* ctx, const ExecBatch& batch) {
  return checked_cast<ScalarAggregator*>(ctx->state())->Consume(ctx, batch);
}

Status AggregateMerge(KernelContext* ctx, KernelState&& src, KernelState* dst) {
  return checked_cast<ScalarAggregator*>(dst)->MergeFrom(ctx, std::move(src));
}

Status AggregateFinalize(KernelContext* ctx, Datum* out) {
  return checked_cast<ScalarAggregator*>(ctx->state())->Finalize(ctx, out);
}

}  // namespace

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFunction* func, SimdLevel::type simd_level) {
  ScalarAggregateKernel kernel(std::move(sig), init, AggregateConsume, AggregateMerge,
                               AggregateFinalize);
  // Set the simd level
  kernel.simd_level = simd_level;
  DCHECK_OK(func->AddKernel(kernel));
}

namespace aggregate {

// ----------------------------------------------------------------------
// Count implementation

struct CountImpl : public ScalarAggregator {
  explicit CountImpl(ScalarAggregateOptions options) : options(std::move(options)) {}

  Status Consume(KernelContext*, const ExecBatch& batch) override {
    if (batch[0].is_array()) {
      const ArrayData& input = *batch[0].array();
      const int64_t nulls = input.GetNullCount();
      this->nulls += nulls;
      this->non_nulls += input.length - nulls;
    } else {
      const Scalar& input = *batch[0].scalar();
      this->nulls += !input.is_valid * batch.length;
      this->non_nulls += input.is_valid * batch.length;
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other_state = checked_cast<const CountImpl&>(src);
    this->non_nulls += other_state.non_nulls;
    this->nulls += other_state.nulls;
    return Status::OK();
  }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    const auto& state = checked_cast<const CountImpl&>(*ctx->state());
    if (state.options.skip_nulls) {
      *out = Datum(state.non_nulls);
    } else {
      *out = Datum(state.nulls);
    }
    return Status::OK();
  }

  ScalarAggregateOptions options;
  int64_t non_nulls = 0;
  int64_t nulls = 0;
};

Result<std::unique_ptr<KernelState>> CountInit(KernelContext*,
                                               const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<CountImpl>(
      static_cast<const ScalarAggregateOptions&>(*args.options));
}

// ----------------------------------------------------------------------
// Sum implementation

template <typename ArrowType>
struct SumImplDefault : public SumImpl<ArrowType, SimdLevel::NONE> {
  explicit SumImplDefault(const ScalarAggregateOptions& options_) {
    this->options = options_;
  }
};

template <typename ArrowType>
struct MeanImplDefault : public MeanImpl<ArrowType, SimdLevel::NONE> {
  explicit MeanImplDefault(const ScalarAggregateOptions& options_) {
    this->options = options_;
  }
};

Result<std::unique_ptr<KernelState>> SumInit(KernelContext* ctx,
                                             const KernelInitArgs& args) {
  SumLikeInit<SumImplDefault> visitor(
      ctx, *args.inputs[0].type,
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

Result<std::unique_ptr<KernelState>> MeanInit(KernelContext* ctx,
                                              const KernelInitArgs& args) {
  SumLikeInit<MeanImplDefault> visitor(
      ctx, *args.inputs[0].type,
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

// ----------------------------------------------------------------------
// MinMax implementation

Result<std::unique_ptr<KernelState>> MinMaxInit(KernelContext* ctx,
                                                const KernelInitArgs& args) {
  MinMaxInitState<SimdLevel::NONE> visitor(
      ctx, *args.inputs[0].type, args.kernel->signature->out_type().type(),
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

// ----------------------------------------------------------------------
// Any implementation

struct BooleanAnyImpl : public ScalarAggregator {
  explicit BooleanAnyImpl(ScalarAggregateOptions options) : options(std::move(options)) {}

  Status Consume(KernelContext*, const ExecBatch& batch) override {
    // short-circuit if seen a True already
    if (this->any == true) {
      return Status::OK();
    }
    if (batch[0].is_scalar()) {
      const auto& scalar = *batch[0].scalar();
      this->has_nulls = !scalar.is_valid;
      this->any = scalar.is_valid && checked_cast<const BooleanScalar&>(scalar).value;
      return Status::OK();
    }
    const auto& data = *batch[0].array();
    this->has_nulls = data.GetNullCount() > 0;
    arrow::internal::OptionalBinaryBitBlockCounter counter(
        data.buffers[0], data.offset, data.buffers[1], data.offset, data.length);
    int64_t position = 0;
    while (position < data.length) {
      const auto block = counter.NextAndBlock();
      if (block.popcount > 0) {
        this->any = true;
        break;
      }
      position += block.length;
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const BooleanAnyImpl&>(src);
    this->any |= other.any;
    this->has_nulls |= other.has_nulls;
    return Status::OK();
  }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    if (!options.skip_nulls && !this->any && this->has_nulls) {
      out->value = std::make_shared<BooleanScalar>();
    } else {
      out->value = std::make_shared<BooleanScalar>(this->any);
    }
    return Status::OK();
  }

  bool any = false;
  bool has_nulls = false;
  ScalarAggregateOptions options;
};

Result<std::unique_ptr<KernelState>> AnyInit(KernelContext*, const KernelInitArgs& args) {
  const ScalarAggregateOptions options =
      static_cast<const ScalarAggregateOptions&>(*args.options);
  return ::arrow::internal::make_unique<BooleanAnyImpl>(
      static_cast<const ScalarAggregateOptions&>(*args.options));
}

// ----------------------------------------------------------------------
// All implementation

struct BooleanAllImpl : public ScalarAggregator {
  explicit BooleanAllImpl(ScalarAggregateOptions options) : options(std::move(options)) {}

  Status Consume(KernelContext*, const ExecBatch& batch) override {
    // short-circuit if seen a false already
    if (this->all == false) {
      return Status::OK();
    }
    // short-circuit if seen a null already
    if (!options.skip_nulls && this->has_nulls) {
      return Status::OK();
    }
    if (batch[0].is_scalar()) {
      const auto& scalar = *batch[0].scalar();
      this->has_nulls = !scalar.is_valid;
      this->all = !scalar.is_valid || checked_cast<const BooleanScalar&>(scalar).value;
      return Status::OK();
    }
    const auto& data = *batch[0].array();
    this->has_nulls = data.GetNullCount() > 0;
    arrow::internal::OptionalBinaryBitBlockCounter counter(
        data.buffers[1], data.offset, data.buffers[0], data.offset, data.length);
    int64_t position = 0;
    while (position < data.length) {
      const auto block = counter.NextOrNotBlock();
      if (!block.AllSet()) {
        this->all = false;
        break;
      }
      position += block.length;
    }

    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const BooleanAllImpl&>(src);
    this->all &= other.all;
    this->has_nulls |= other.has_nulls;
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    if (!options.skip_nulls && this->all && this->has_nulls) {
      out->value = std::make_shared<BooleanScalar>();
    } else {
      out->value = std::make_shared<BooleanScalar>(this->all);
    }
    return Status::OK();
  }

  bool all = true;
  bool has_nulls = false;
  ScalarAggregateOptions options;
};

Result<std::unique_ptr<KernelState>> AllInit(KernelContext*, const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<BooleanAllImpl>(
      static_cast<const ScalarAggregateOptions&>(*args.options));
}

// ----------------------------------------------------------------------
// Index implementation

template <typename ArgType>
struct IndexImpl : public ScalarAggregator {
  using ArgValue = typename internal::GetViewType<ArgType>::T;

  explicit IndexImpl(IndexOptions options, KernelState* raw_state)
      : options(std::move(options)), seen(0), index(-1) {
    if (auto state = static_cast<IndexImpl<ArgType>*>(raw_state)) {
      seen = state->seen;
      index = state->index;
    }
  }

  Status Consume(KernelContext* ctx, const ExecBatch& batch) override {
    // short-circuit
    if (index >= 0 || !options.value->is_valid) {
      return Status::OK();
    }

    auto input = batch[0].array();
    seen = input->length;
    const ArgValue desired = internal::UnboxScalar<ArgType>::Unbox(*options.value);
    int64_t i = 0;

    ARROW_UNUSED(internal::VisitArrayValuesInline<ArgType>(
        *input,
        [&](ArgValue v) -> Status {
          if (v == desired) {
            index = i;
            return Status::Cancelled("Found");
          } else {
            ++i;
            return Status::OK();
          }
        },
        [&]() -> Status {
          ++i;
          return Status::OK();
        }));

    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const IndexImpl&>(src);
    if (index < 0 && other.index >= 0) {
      index = seen + other.index;
    }
    seen += other.seen;
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    out->value = std::make_shared<Int64Scalar>(index >= 0 ? index : -1);
    return Status::OK();
  }

  const IndexOptions options;
  int64_t seen = 0;
  int64_t index = -1;
};

struct IndexInit {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const IndexOptions& options;
  const DataType& type;

  IndexInit(KernelContext* ctx, const IndexOptions& options, const DataType& type)
      : ctx(ctx), options(options), type(type) {}

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Index kernel not implemented for ", type.ToString());
  }

  Status Visit(const BooleanType&) {
    state.reset(new IndexImpl<BooleanType>(options, ctx->state()));
    return Status::OK();
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    state.reset(new IndexImpl<Type>(options, ctx->state()));
    return Status::OK();
  }

  template <typename Type>
  enable_if_base_binary<Type, Status> Visit(const Type&) {
    state.reset(new IndexImpl<Type>(options, ctx->state()));
    return Status::OK();
  }

  template <typename Type>
  enable_if_date<Type, Status> Visit(const Type&) {
    state.reset(new IndexImpl<Type>(options, ctx->state()));
    return Status::OK();
  }

  template <typename Type>
  enable_if_time<Type, Status> Visit(const Type&) {
    state.reset(new IndexImpl<Type>(options, ctx->state()));
    return Status::OK();
  }

  template <typename Type>
  enable_if_timestamp<Type, Status> Visit(const Type&) {
    state.reset(new IndexImpl<Type>(options, ctx->state()));
    return Status::OK();
  }

  Result<std::unique_ptr<KernelState>> Create() {
    RETURN_NOT_OK(VisitTypeInline(type, this));
    return std::move(state);
  }

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    IndexInit visitor(ctx, static_cast<const IndexOptions&>(*args.options),
                      *args.inputs[0].type);
    return visitor.Create();
  }
};

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

void AddScalarAggKernels(KernelInit init,
                         const std::vector<std::shared_ptr<DataType>>& types,
                         std::shared_ptr<DataType> out_ty,
                         ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    // scalar[InT] -> scalar[OutT]
    auto sig = KernelSignature::Make({InputType::Scalar(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, SimdLevel::NONE);
  }
}

void AddArrayScalarAggKernels(KernelInit init,
                              const std::vector<std::shared_ptr<DataType>>& types,
                              std::shared_ptr<DataType> out_ty,
                              ScalarAggregateFunction* func,
                              SimdLevel::type simd_level = SimdLevel::NONE) {
  AddBasicAggKernels(init, types, out_ty, func, simd_level);
  AddScalarAggKernels(init, types, out_ty, func);
}

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func, SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    // any[T] -> scalar[struct<min: T, max: T>]
    auto out_ty = struct_({field("min", ty), field("max", ty)});
    auto sig = KernelSignature::Make({InputType(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, simd_level);
  }
}

}  // namespace aggregate

namespace internal {
namespace {

const FunctionDoc count_doc{"Count the number of null / non-null values",
                            ("By default, only non-null values are counted.\n"
                             "This can be changed through ScalarAggregateOptions."),
                            {"array"},
                            "ScalarAggregateOptions"};

const FunctionDoc sum_doc{
    "Compute the sum of a numeric array",
    ("Null values are ignored by default. Minimum count of non-null\n"
     "values can be set and null is returned if too few are present.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array"},
    "ScalarAggregateOptions"};

const FunctionDoc mean_doc{
    "Compute the mean of a numeric array",
    ("Null values are ignored by default. Minimum count of non-null\n"
     "values can be set and null is returned if too few are "
     "present.\nThis can be changed through ScalarAggregateOptions.\n"
     "The result is always computed as a double, regardless of the input types."),
    {"array"},
    "ScalarAggregateOptions"};

const FunctionDoc min_max_doc{"Compute the minimum and maximum values of a numeric array",
                              ("Null values are ignored by default.\n"
                               "This can be changed through ScalarAggregateOptions."),
                              {"array"},
                              "ScalarAggregateOptions"};

const FunctionDoc any_doc{"Test whether any element in a boolean array evaluates to true",
                          ("Null values are ignored by default.\n"
                           "If null values are taken into account by setting "
                           "ScalarAggregateOptions parameter skip_nulls = false then "
                           "Kleene logic is used.\n"
                           "See KleeneOr for more details on Kleene logic."),
                          {"array"},
                          "ScalarAggregateOptions"};

const FunctionDoc all_doc{"Test whether all elements in a boolean array evaluate to true",
                          ("Null values are ignored by default.\n"
                           "If null values are taken into account by setting "
                           "ScalarAggregateOptions parameter skip_nulls = false then "
                           "Kleene logic is used.\n"
                           "See KleeneAnd for more details on Kleene logic."),
                          {"array"},
                          "ScalarAggregateOptions"};

const FunctionDoc index_doc{"Find the index of the first occurrence of a given value",
                            ("The result is always computed as an int64_t, regardless\n"
                             "of the offset type of the input array."),
                            {"array"},
                            "IndexOptions"};

}  // namespace

void RegisterScalarAggregateBasic(FunctionRegistry* registry) {
  static auto default_scalar_aggregate_options = ScalarAggregateOptions::Defaults();

  auto func = std::make_shared<ScalarAggregateFunction>(
      "count", Arity::Unary(), &count_doc, &default_scalar_aggregate_options);

  // Takes any array input, outputs int64 scalar
  InputType any_array(ValueDescr::ARRAY);
  AddAggKernel(KernelSignature::Make({any_array}, ValueDescr::Scalar(int64())),
               aggregate::CountInit, func.get());
  AddAggKernel(
      KernelSignature::Make({InputType(ValueDescr::SCALAR)}, ValueDescr::Scalar(int64())),
      aggregate::CountInit, func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary(), &sum_doc,
                                                   &default_scalar_aggregate_options);
  aggregate::AddArrayScalarAggKernels(aggregate::SumInit, {boolean()}, int64(),
                                      func.get());
  aggregate::AddArrayScalarAggKernels(aggregate::SumInit, SignedIntTypes(), int64(),
                                      func.get());
  aggregate::AddArrayScalarAggKernels(aggregate::SumInit, UnsignedIntTypes(), uint64(),
                                      func.get());
  aggregate::AddArrayScalarAggKernels(aggregate::SumInit, FloatingPointTypes(), float64(),
                                      func.get());
  // Add the SIMD variants for sum
#if defined(ARROW_HAVE_RUNTIME_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX512)
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();
#endif
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

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary(), &mean_doc,
                                                   &default_scalar_aggregate_options);
  aggregate::AddArrayScalarAggKernels(aggregate::MeanInit, {boolean()}, float64(),
                                      func.get());
  aggregate::AddArrayScalarAggKernels(aggregate::MeanInit, NumericTypes(), float64(),
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

  func = std::make_shared<ScalarAggregateFunction>(
      "min_max", Arity::Unary(), &min_max_doc, &default_scalar_aggregate_options);
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

  // any
  func = std::make_shared<ScalarAggregateFunction>("any", Arity::Unary(), &any_doc,
                                                   &default_scalar_aggregate_options);
  aggregate::AddArrayScalarAggKernels(aggregate::AnyInit, {boolean()}, boolean(),
                                      func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // all
  func = std::make_shared<ScalarAggregateFunction>("all", Arity::Unary(), &all_doc,
                                                   &default_scalar_aggregate_options);
  aggregate::AddArrayScalarAggKernels(aggregate::AllInit, {boolean()}, boolean(),
                                      func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // index
  func = std::make_shared<ScalarAggregateFunction>("index", Arity::Unary(), &index_doc);
  aggregate::AddBasicAggKernels(aggregate::IndexInit::Init, BaseBinaryTypes(), int64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::IndexInit::Init, PrimitiveTypes(), int64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::IndexInit::Init, TemporalTypes(), int64(),
                                func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

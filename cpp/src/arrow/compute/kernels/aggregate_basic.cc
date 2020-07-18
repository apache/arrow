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

#include <cmath>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_basic_internal.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace compute {
namespace aggregate {

void AggregateConsume(KernelContext* ctx, const ExecBatch& batch) {
  checked_cast<ScalarAggregator*>(ctx->state())->Consume(ctx, batch);
}

void AggregateMerge(KernelContext* ctx, const KernelState& src, KernelState* dst) {
  checked_cast<ScalarAggregator*>(dst)->MergeFrom(ctx, src);
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

  void MergeFrom(KernelContext*, const KernelState& src) override {
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
                     ArrowType> {};

template <typename ArrowType>
struct MeanImplDefault
    : public MeanImpl<RoundSizeDefault<typename TypeTraits<ArrowType>::CType>::size,
                      ArrowType> {};

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

template <typename ArrowType, typename Enable = void>
struct MinMaxState {};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_boolean<ArrowType>> {
  using ThisType = MinMaxState<ArrowType>;
  using T = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
    this->has_values |= rhs.has_values;
    this->min = this->min && rhs.min;
    this->max = this->max || rhs.max;
    return *this;
  }

  void MergeOne(T value) {
    this->min = this->min && value;
    this->max = this->max || value;
  }

  T min = true;
  T max = false;
  bool has_nulls = false;
  bool has_values = false;
};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_integer<ArrowType>> {
  using ThisType = MinMaxState<ArrowType>;
  using T = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
    this->has_values |= rhs.has_values;
    this->min = std::min(this->min, rhs.min);
    this->max = std::max(this->max, rhs.max);
    return *this;
  }

  void MergeOne(T value) {
    this->min = std::min(this->min, value);
    this->max = std::max(this->max, value);
  }

  T min = std::numeric_limits<T>::max();
  T max = std::numeric_limits<T>::min();
  bool has_nulls = false;
  bool has_values = false;
};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_floating_point<ArrowType>> {
  using ThisType = MinMaxState<ArrowType>;
  using T = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
    this->has_values |= rhs.has_values;
    this->min = std::fmin(this->min, rhs.min);
    this->max = std::fmax(this->max, rhs.max);
    return *this;
  }

  void MergeOne(T value) {
    this->min = std::fmin(this->min, value);
    this->max = std::fmax(this->max, value);
  }

  T min = std::numeric_limits<T>::infinity();
  T max = -std::numeric_limits<T>::infinity();
  bool has_nulls = false;
  bool has_values = false;
};

template <typename ArrowType>
struct MinMaxImpl : public ScalarAggregator {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ThisType = MinMaxImpl<ArrowType>;
  using StateType = MinMaxState<ArrowType>;

  MinMaxImpl(const std::shared_ptr<DataType>& out_type, const MinMaxOptions& options)
      : out_type(out_type), options(options) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    StateType local;

    ArrayType arr(batch[0].array());

    const auto null_count = arr.null_count();
    local.has_nulls = null_count > 0;
    local.has_values = (arr.length() - null_count) > 0;

    if (local.has_nulls && options.null_handling == MinMaxOptions::OUTPUT_NULL) {
      this->state = local;
      return;
    }

    if (local.has_nulls) {
      BitmapReader reader(arr.null_bitmap_data(), arr.offset(), arr.length());
      for (int64_t i = 0; i < arr.length(); i++) {
        if (reader.IsSet()) {
          local.MergeOne(arr.Value(i));
        }
        reader.Next();
      }
    } else {
      for (int64_t i = 0; i < arr.length(); i++) {
        local.MergeOne(arr.Value(i));
      }
    }
    this->state = local;
  }

  void MergeFrom(KernelContext*, const KernelState& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->state += other.state;
  }

  void Finalize(KernelContext*, Datum* out) override {
    using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

    std::vector<std::shared_ptr<Scalar>> values;
    if (!state.has_values ||
        (state.has_nulls && options.null_handling == MinMaxOptions::OUTPUT_NULL)) {
      // (null, null)
      values = {std::make_shared<ScalarType>(), std::make_shared<ScalarType>()};
    } else {
      values = {std::make_shared<ScalarType>(state.min),
                std::make_shared<ScalarType>(state.max)};
    }
    out->value = std::make_shared<StructScalar>(std::move(values), this->out_type);
  }

  std::shared_ptr<DataType> out_type;
  MinMaxOptions options;
  MinMaxState<ArrowType> state;
};

struct BooleanMinMaxImpl : public MinMaxImpl<BooleanType> {
  using MinMaxImpl::MinMaxImpl;

  void Consume(KernelContext*, const ExecBatch& batch) override {
    StateType local;
    ArrayType arr(batch[0].array());

    const auto arr_length = arr.length();
    const auto null_count = arr.null_count();
    const auto valid_count = arr_length - null_count;

    local.has_nulls = null_count > 0;
    local.has_values = valid_count > 0;
    if (local.has_nulls && options.null_handling == MinMaxOptions::OUTPUT_NULL) {
      this->state = local;
      return;
    }

    const auto true_count = arr.true_count();
    const auto false_count = valid_count - true_count;
    local.max = true_count > 0;
    local.min = false_count == 0;

    this->state = local;
  }
};

struct MinMaxInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  const std::shared_ptr<DataType>& out_type;
  const MinMaxOptions& options;

  MinMaxInitState(KernelContext* ctx, const DataType& in_type,
                  const std::shared_ptr<DataType>& out_type, const MinMaxOptions& options)
      : ctx(ctx), in_type(in_type), out_type(out_type), options(options) {}

  Status Visit(const DataType&) {
    return Status::NotImplemented("No min/max implemented");
  }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No sum implemented");
  }

  Status Visit(const BooleanType&) {
    state.reset(new BooleanMinMaxImpl(out_type, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    state.reset(new MinMaxImpl<Type>(out_type, options));
    return Status::OK();
  }

  std::unique_ptr<KernelState> Create() {
    ctx->SetStatus(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

std::unique_ptr<KernelState> MinMaxInit(KernelContext* ctx, const KernelInitArgs& args) {
  MinMaxInitState visitor(ctx, *args.inputs[0].type,
                          args.kernel->signature->out_type().type(),
                          static_cast<const MinMaxOptions&>(*args.options));
  return visitor.Create();
}

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFunction* func) {
  DCHECK_OK(func->AddKernel(ScalarAggregateKernel(std::move(sig), init, AggregateConsume,
                                                  AggregateMerge, AggregateFinalize)));
}

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    // array[InT] -> scalar[OutT]
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func);
  }
}

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    // array[T] -> scalar[struct<min: T, max: T>]
    auto out_ty = struct_({field("min", ty), field("max", ty)});
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func);
  }
}

}  // namespace aggregate

namespace internal {
void RegisterScalarAggregateBasic(FunctionRegistry* registry) {
  static auto default_count_options = CountOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("count", Arity::Unary(),
                                                        &default_count_options);

  /// Takes any array input, outputs int64 scalar
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
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary());
  aggregate::AddBasicAggKernels(aggregate::MeanInit, {boolean()}, float64(), func.get());
  aggregate::AddBasicAggKernels(aggregate::MeanInit, NumericTypes(), float64(),
                                func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  static auto default_minmax_options = MinMaxOptions::Defaults();
  func = std::make_shared<ScalarAggregateFunction>("min_max", Arity::Unary(),
                                                   &default_minmax_options);
  aggregate::AddMinMaxKernels(aggregate::MinMaxInit, {boolean()}, func.get());
  aggregate::AddMinMaxKernels(aggregate::MinMaxInit, NumericTypes(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

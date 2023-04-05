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
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/hashing.h"

#include <memory>

namespace arrow {
namespace compute {
namespace internal {

namespace {

Status AggregateConsume(KernelContext* ctx, const ExecSpan& batch) {
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
  ScalarAggregateKernel kernel(std::move(sig), std::move(init), AggregateConsume,
                               AggregateMerge, AggregateFinalize);
  // Set the simd level
  kernel.simd_level = simd_level;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFinalize finalize, ScalarAggregateFunction* func,
                  SimdLevel::type simd_level) {
  ScalarAggregateKernel kernel(std::move(sig), std::move(init), AggregateConsume,
                               AggregateMerge, std::move(finalize));
  // Set the simd level
  kernel.simd_level = simd_level;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

namespace {

// ----------------------------------------------------------------------
// Count implementations

struct CountAllImpl : public ScalarAggregator {
  Status Consume(KernelContext*, const ExecSpan& batch) override {
    this->count += batch.length;
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other_state = checked_cast<const CountAllImpl&>(src);
    this->count += other_state.count;
    return Status::OK();
  }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    const auto& state = checked_cast<const CountAllImpl&>(*ctx->state());
    *out = Datum(state.count);
    return Status::OK();
  }

  int64_t count = 0;
};

struct CountImpl : public ScalarAggregator {
  explicit CountImpl(CountOptions options) : options(std::move(options)) {}

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (options.mode == CountOptions::ALL) {
      this->non_nulls += batch.length;
    } else if (batch[0].is_array()) {
      const ArraySpan& input = batch[0].array;
      const int64_t nulls = input.GetNullCount();
      this->nulls += nulls;
      this->non_nulls += input.length - nulls;
    } else {
      const Scalar& input = *batch[0].scalar;
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
    switch (state.options.mode) {
      case CountOptions::ONLY_VALID:
      case CountOptions::ALL:
        // ALL is equivalent since we don't count the null/non-null
        // separately to avoid potentially computing null count
        *out = Datum(state.non_nulls);
        break;
      case CountOptions::ONLY_NULL:
        *out = Datum(state.nulls);
        break;
      default:
        DCHECK(false) << "unreachable";
    }
    return Status::OK();
  }

  CountOptions options;
  int64_t non_nulls = 0;
  int64_t nulls = 0;
};

Result<std::unique_ptr<KernelState>> CountAllInit(KernelContext*,
                                                  const KernelInitArgs& args) {
  return std::make_unique<CountAllImpl>();
}

Result<std::unique_ptr<KernelState>> CountInit(KernelContext*,
                                               const KernelInitArgs& args) {
  return std::make_unique<CountImpl>(static_cast<const CountOptions&>(*args.options));
}

// ----------------------------------------------------------------------
// Distinct Count implementation

template <typename Type, typename VisitorArgType>
struct CountDistinctImpl : public ScalarAggregator {
  using MemoTable = typename arrow::internal::HashTraits<Type>::MemoTableType;

  explicit CountDistinctImpl(MemoryPool* memory_pool, CountOptions options)
      : options(std::move(options)), memo_table_(new MemoTable(memory_pool, 0)) {}

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (batch[0].is_array()) {
      const ArraySpan& arr = batch[0].array;
      this->has_nulls = arr.GetNullCount() > 0;

      auto visit_null = []() { return Status::OK(); };
      auto visit_value = [&](VisitorArgType arg) {
        int32_t y;
        return memo_table_->GetOrInsert(arg, &y);
      };
      RETURN_NOT_OK(VisitArraySpanInline<Type>(arr, visit_value, visit_null));
    } else {
      const Scalar& input = *batch[0].scalar;
      this->has_nulls = !input.is_valid;

      if (input.is_valid) {
        int32_t unused;
        RETURN_NOT_OK(memo_table_->GetOrInsert(UnboxScalar<Type>::Unbox(input), &unused));
      }
    }

    this->non_nulls = memo_table_->size();
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other_state = checked_cast<const CountDistinctImpl&>(src);
    RETURN_NOT_OK(this->memo_table_->MergeTable(*(other_state.memo_table_)));
    this->non_nulls = this->memo_table_->size();
    this->has_nulls = this->has_nulls || other_state.has_nulls;
    return Status::OK();
  }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    const auto& state = checked_cast<const CountDistinctImpl&>(*ctx->state());
    const int64_t nulls = state.has_nulls ? 1 : 0;
    switch (state.options.mode) {
      case CountOptions::ONLY_VALID:
        *out = Datum(state.non_nulls);
        break;
      case CountOptions::ALL:
        *out = Datum(state.non_nulls + nulls);
        break;
      case CountOptions::ONLY_NULL:
        *out = Datum(nulls);
        break;
      default:
        DCHECK(false) << "unreachable";
    }
    return Status::OK();
  }

  const CountOptions options;
  int64_t non_nulls = 0;
  bool has_nulls = false;
  std::unique_ptr<MemoTable> memo_table_;
};

template <typename Type, typename VisitorArgType>
Result<std::unique_ptr<KernelState>> CountDistinctInit(KernelContext* ctx,
                                                       const KernelInitArgs& args) {
  return std::make_unique<CountDistinctImpl<Type, VisitorArgType>>(
      ctx->memory_pool(), static_cast<const CountOptions&>(*args.options));
}

template <typename Type, typename VisitorArgType = typename Type::c_type>
void AddCountDistinctKernel(InputType type, ScalarAggregateFunction* func) {
  AddAggKernel(KernelSignature::Make({type}, int64()),
               CountDistinctInit<Type, VisitorArgType>, func);
}

void AddCountDistinctKernels(ScalarAggregateFunction* func) {
  // Boolean
  AddCountDistinctKernel<BooleanType>(boolean(), func);
  // Number
  AddCountDistinctKernel<Int8Type>(int8(), func);
  AddCountDistinctKernel<Int16Type>(int16(), func);
  AddCountDistinctKernel<Int32Type>(int32(), func);
  AddCountDistinctKernel<Int64Type>(int64(), func);
  AddCountDistinctKernel<UInt8Type>(uint8(), func);
  AddCountDistinctKernel<UInt16Type>(uint16(), func);
  AddCountDistinctKernel<UInt32Type>(uint32(), func);
  AddCountDistinctKernel<UInt64Type>(uint64(), func);
  AddCountDistinctKernel<HalfFloatType>(float16(), func);
  AddCountDistinctKernel<FloatType>(float32(), func);
  AddCountDistinctKernel<DoubleType>(float64(), func);
  // Date
  AddCountDistinctKernel<Date32Type>(date32(), func);
  AddCountDistinctKernel<Date64Type>(date64(), func);
  // Time
  AddCountDistinctKernel<Time32Type>(match::SameTypeId(Type::TIME32), func);
  AddCountDistinctKernel<Time64Type>(match::SameTypeId(Type::TIME64), func);
  // Timestamp & Duration
  AddCountDistinctKernel<TimestampType>(match::SameTypeId(Type::TIMESTAMP), func);
  AddCountDistinctKernel<DurationType>(match::SameTypeId(Type::DURATION), func);
  // Interval
  AddCountDistinctKernel<MonthIntervalType>(month_interval(), func);
  AddCountDistinctKernel<DayTimeIntervalType>(day_time_interval(), func);
  AddCountDistinctKernel<MonthDayNanoIntervalType>(month_day_nano_interval(), func);
  // Binary & String
  AddCountDistinctKernel<BinaryType, std::string_view>(match::BinaryLike(), func);
  AddCountDistinctKernel<LargeBinaryType, std::string_view>(match::LargeBinaryLike(),
                                                            func);
  // Fixed binary & Decimal
  AddCountDistinctKernel<FixedSizeBinaryType, std::string_view>(
      match::FixedSizeBinaryLike(), func);
}

// ----------------------------------------------------------------------
// Sum implementation

template <typename ArrowType>
struct SumImplDefault : public SumImpl<ArrowType, SimdLevel::NONE> {
  using SumImpl<ArrowType, SimdLevel::NONE>::SumImpl;
};

template <typename ArrowType>
struct MeanImplDefault : public MeanImpl<ArrowType, SimdLevel::NONE> {
  using MeanImpl<ArrowType, SimdLevel::NONE>::MeanImpl;
};

Result<std::unique_ptr<KernelState>> SumInit(KernelContext* ctx,
                                             const KernelInitArgs& args) {
  SumLikeInit<SumImplDefault> visitor(
      ctx, args.inputs[0].GetSharedPtr(),
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

Result<std::unique_ptr<KernelState>> MeanInit(KernelContext* ctx,
                                              const KernelInitArgs& args) {
  MeanKernelInit<MeanImplDefault> visitor(
      ctx, args.inputs[0].GetSharedPtr(),
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

// ----------------------------------------------------------------------
// Product implementation

using arrow::compute::internal::to_unsigned;

template <typename ArrowType>
struct ProductImpl : public ScalarAggregator {
  using ThisType = ProductImpl<ArrowType>;
  using AccType = typename FindAccumulatorType<ArrowType>::Type;
  using ProductType = typename TypeTraits<AccType>::CType;
  using OutputType = typename TypeTraits<AccType>::ScalarType;

  explicit ProductImpl(std::shared_ptr<DataType> out_type,
                       const ScalarAggregateOptions& options)
      : out_type(out_type),
        options(options),
        count(0),
        product(MultiplyTraits<AccType>::one(*out_type)),
        nulls_observed(false) {}

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (batch[0].is_array()) {
      const ArraySpan& data = batch[0].array;
      this->count += data.length - data.GetNullCount();
      this->nulls_observed = this->nulls_observed || data.GetNullCount();

      if (!options.skip_nulls && this->nulls_observed) {
        // Short-circuit
        return Status::OK();
      }

      internal::VisitArrayValuesInline<ArrowType>(
          data,
          [&](typename TypeTraits<ArrowType>::CType value) {
            this->product =
                MultiplyTraits<AccType>::Multiply(*out_type, this->product, value);
          },
          [] {});
    } else {
      const Scalar& data = *batch[0].scalar;
      this->count += data.is_valid * batch.length;
      this->nulls_observed = this->nulls_observed || !data.is_valid;
      if (data.is_valid) {
        for (int64_t i = 0; i < batch.length; i++) {
          auto value = internal::UnboxScalar<ArrowType>::Unbox(data);
          this->product =
              MultiplyTraits<AccType>::Multiply(*out_type, this->product, value);
        }
      }
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->count += other.count;
    this->product =
        MultiplyTraits<AccType>::Multiply(*out_type, this->product, other.product);
    this->nulls_observed = this->nulls_observed || other.nulls_observed;
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    if ((!options.skip_nulls && this->nulls_observed) ||
        (this->count < options.min_count)) {
      out->value = std::make_shared<OutputType>(out_type);
    } else {
      out->value = std::make_shared<OutputType>(this->product, out_type);
    }
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type;
  ScalarAggregateOptions options;
  size_t count;
  ProductType product;
  bool nulls_observed;
};

struct NullProductImpl : public NullImpl<Int64Type> {
  explicit NullProductImpl(const ScalarAggregateOptions& options_)
      : NullImpl<Int64Type>(options_) {}

  std::shared_ptr<Scalar> output_empty() override {
    return std::make_shared<Int64Scalar>(1);
  }
};

struct ProductInit {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  std::shared_ptr<DataType> type;
  const ScalarAggregateOptions& options;

  ProductInit(KernelContext* ctx, std::shared_ptr<DataType> type,
              const ScalarAggregateOptions& options)
      : ctx(ctx), type(type), options(options) {}

  Status Visit(const DataType&) {
    return Status::NotImplemented("No product implemented");
  }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No product implemented");
  }

  Status Visit(const BooleanType&) {
    auto ty = TypeTraits<typename ProductImpl<BooleanType>::AccType>::type_singleton();
    state.reset(new ProductImpl<BooleanType>(ty, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    auto ty = TypeTraits<typename ProductImpl<Type>::AccType>::type_singleton();
    state.reset(new ProductImpl<Type>(ty, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_decimal<Type, Status> Visit(const Type&) {
    state.reset(new ProductImpl<Type>(type, options));
    return Status::OK();
  }

  Status Visit(const NullType&) {
    state.reset(new NullProductImpl(options));
    return Status::OK();
  }

  Result<std::unique_ptr<KernelState>> Create() {
    RETURN_NOT_OK(VisitTypeInline(*type, this));
    return std::move(state);
  }

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    ProductInit visitor(ctx, args.inputs[0].GetSharedPtr(),
                        static_cast<const ScalarAggregateOptions&>(*args.options));
    return visitor.Create();
  }
};

// ----------------------------------------------------------------------
// MinMax implementation

Result<std::unique_ptr<KernelState>> MinMaxInit(KernelContext* ctx,
                                                const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(TypeHolder out_type,
                        args.kernel->signature->out_type().Resolve(ctx, args.inputs));
  MinMaxInitState<SimdLevel::NONE> visitor(
      ctx, *args.inputs[0], out_type.GetSharedPtr(),
      static_cast<const ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

// For "min" and "max" functions: override finalize and return the actual value
template <MinOrMax min_or_max>
void AddMinOrMaxAggKernel(ScalarAggregateFunction* func,
                          ScalarAggregateFunction* min_max_func) {
  auto sig = KernelSignature::Make({InputType::Any()}, FirstType);
  auto init = [min_max_func](
                  KernelContext* ctx,
                  const KernelInitArgs& args) -> Result<std::unique_ptr<KernelState>> {
    ARROW_ASSIGN_OR_RAISE(auto kernel, min_max_func->DispatchExact(args.inputs));
    KernelInitArgs new_args{kernel, args.inputs, args.options};
    return kernel->init(ctx, new_args);
  };

  auto finalize = [](KernelContext* ctx, Datum* out) -> Status {
    Datum temp;
    RETURN_NOT_OK(checked_cast<ScalarAggregator*>(ctx->state())->Finalize(ctx, &temp));
    const auto& result = temp.scalar_as<StructScalar>();
    DCHECK(result.is_valid);
    *out = result.value[static_cast<uint8_t>(min_or_max)];
    return Status::OK();
  };

  // Note SIMD level is always NONE, but the convenience kernel will
  // dispatch to an appropriate implementation
  AddAggKernel(std::move(sig), std::move(init), std::move(finalize), func);
}

// ----------------------------------------------------------------------
// Any implementation

struct BooleanAnyImpl : public ScalarAggregator {
  explicit BooleanAnyImpl(ScalarAggregateOptions options) : options(std::move(options)) {}

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    // short-circuit if seen a True already
    if (this->any == true && this->count >= options.min_count) {
      return Status::OK();
    }
    if (batch[0].is_scalar()) {
      const Scalar& scalar = *batch[0].scalar;
      this->has_nulls = !scalar.is_valid;
      this->any = scalar.is_valid && checked_cast<const BooleanScalar&>(scalar).value;
      this->count += scalar.is_valid;
      return Status::OK();
    }
    const ArraySpan& data = batch[0].array;
    this->has_nulls = data.GetNullCount() > 0;
    this->count += data.length - data.GetNullCount();
    arrow::internal::OptionalBinaryBitBlockCounter counter(
        data.buffers[0].data, data.offset, data.buffers[1].data, data.offset,
        data.length);
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
    this->count += other.count;
    return Status::OK();
  }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    if ((!options.skip_nulls && !this->any && this->has_nulls) ||
        this->count < options.min_count) {
      out->value = std::make_shared<BooleanScalar>();
    } else {
      out->value = std::make_shared<BooleanScalar>(this->any);
    }
    return Status::OK();
  }

  bool any = false;
  bool has_nulls = false;
  int64_t count = 0;
  ScalarAggregateOptions options;
};

Result<std::unique_ptr<KernelState>> AnyInit(KernelContext*, const KernelInitArgs& args) {
  const ScalarAggregateOptions options =
      static_cast<const ScalarAggregateOptions&>(*args.options);
  return std::make_unique<BooleanAnyImpl>(
      static_cast<const ScalarAggregateOptions&>(*args.options));
}

// ----------------------------------------------------------------------
// All implementation

struct BooleanAllImpl : public ScalarAggregator {
  explicit BooleanAllImpl(ScalarAggregateOptions options) : options(std::move(options)) {}

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    // short-circuit if seen a false already
    if (this->all == false && this->count >= options.min_count) {
      return Status::OK();
    }
    // short-circuit if seen a null already
    if (!options.skip_nulls && this->has_nulls) {
      return Status::OK();
    }
    if (batch[0].is_scalar()) {
      const Scalar& scalar = *batch[0].scalar;
      this->has_nulls = !scalar.is_valid;
      this->count += scalar.is_valid;
      this->all = !scalar.is_valid || checked_cast<const BooleanScalar&>(scalar).value;
      return Status::OK();
    }
    const ArraySpan& data = batch[0].array;
    this->has_nulls = data.GetNullCount() > 0;
    this->count += data.length - data.GetNullCount();
    arrow::internal::OptionalBinaryBitBlockCounter counter(
        data.buffers[1].data, data.offset, data.buffers[0].data, data.offset,
        data.length);
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
    this->count += other.count;
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    if ((!options.skip_nulls && this->all && this->has_nulls) ||
        this->count < options.min_count) {
      out->value = std::make_shared<BooleanScalar>();
    } else {
      out->value = std::make_shared<BooleanScalar>(this->all);
    }
    return Status::OK();
  }

  bool all = true;
  bool has_nulls = false;
  int64_t count = 0;
  ScalarAggregateOptions options;
};

Result<std::unique_ptr<KernelState>> AllInit(KernelContext*, const KernelInitArgs& args) {
  return std::make_unique<BooleanAllImpl>(
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

  Status Consume(KernelContext* ctx, const ExecSpan& batch) override {
    // short-circuit
    if (index >= 0 || !options.value->is_valid) {
      return Status::OK();
    }

    const ArgValue desired = internal::UnboxScalar<ArgType>::Unbox(*options.value);

    if (batch[0].is_scalar()) {
      seen = batch.length;
      if (batch[0].scalar->is_valid) {
        const ArgValue v = internal::UnboxScalar<ArgType>::Unbox(*batch[0].scalar);
        if (v == desired) {
          index = 0;
          return Status::Cancelled("Found");
        }
      }
      return Status::OK();
    }

    const ArraySpan& input = batch[0].array;
    seen = input.length;
    int64_t i = 0;

    ARROW_UNUSED(internal::VisitArrayValuesInline<ArgType>(
        input,
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

template <>
struct IndexImpl<NullType> : public ScalarAggregator {
  explicit IndexImpl(IndexOptions, KernelState*) {}

  Status Consume(KernelContext*, const ExecSpan&) override { return Status::OK(); }

  Status MergeFrom(KernelContext*, KernelState&&) override { return Status::OK(); }

  Status Finalize(KernelContext*, Datum* out) override {
    out->value = std::make_shared<Int64Scalar>(-1);
    return Status::OK();
  }
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

  Status Visit(const NullType&) {
    state.reset(new IndexImpl<NullType>(options, ctx->state()));
    return Status::OK();
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

  Status Visit(const FixedSizeBinaryType&) {
    state.reset(new IndexImpl<FixedSizeBinaryType>(options, ctx->state()));
    return Status::OK();
  }

  template <typename Type>
  enable_if_decimal<Type, Status> Visit(const Type&) {
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
    if (!args.options) {
      return Status::Invalid("Must provide IndexOptions for index kernel");
    }
    const auto& options = static_cast<const IndexOptions&>(*args.options);
    if (!options.value) {
      return Status::Invalid("Must provide IndexOptions.value for index kernel");
    } else if (!options.value->type->Equals(*args.inputs[0].type)) {
      return Status::TypeError("Expected IndexOptions.value to be of type ",
                               *args.inputs[0].type, ", but got ", *options.value->type);
    }
    IndexInit visitor(ctx, options, *args.inputs[0].type);
    return visitor.Create();
  }
};

}  // namespace

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func,
                        SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    // array[InT] -> scalar[OutT]
    auto sig = KernelSignature::Make({ty->id()}, out_ty);
    AddAggKernel(std::move(sig), init, func, simd_level);
  }
}

void AddScalarAggKernels(KernelInit init,
                         const std::vector<std::shared_ptr<DataType>>& types,
                         std::shared_ptr<DataType> out_ty,
                         ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    auto sig = KernelSignature::Make({ty->id()}, out_ty);
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

namespace {

Result<TypeHolder> MinMaxType(KernelContext*, const std::vector<TypeHolder>& types) {
  // T -> struct<min: T, max: T>
  auto ty = types.front().GetSharedPtr();
  return struct_({field("min", ty), field("max", ty)});
}

}  // namespace

void AddMinMaxKernel(KernelInit init, internal::detail::GetTypeId get_id,
                     ScalarAggregateFunction* func, SimdLevel::type simd_level) {
  auto sig = KernelSignature::Make({InputType(get_id.id)}, MinMaxType);
  AddAggKernel(std::move(sig), init, func, simd_level);
}

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func, SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    AddMinMaxKernel(init, ty, func, simd_level);
  }
}

namespace {

const FunctionDoc count_all_doc{
    "Count the number of rows", "This version of count takes no arguments.", {}};

const FunctionDoc count_doc{"Count the number of null / non-null values",
                            ("By default, only non-null values are counted.\n"
                             "This can be changed through CountOptions."),
                            {"array"},
                            "CountOptions"};

const FunctionDoc count_distinct_doc{"Count the number of unique values",
                                     ("By default, only non-null values are counted.\n"
                                      "This can be changed through CountOptions."),
                                     {"array"},
                                     "CountOptions"};

const FunctionDoc sum_doc{
    "Compute the sum of a numeric array",
    ("Null values are ignored by default. Minimum count of non-null\n"
     "values can be set and null is returned if too few are present.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array"},
    "ScalarAggregateOptions"};

const FunctionDoc product_doc{
    "Compute the product of values in a numeric array",
    ("Null values are ignored by default. Minimum count of non-null\n"
     "values can be set and null is returned if too few are present.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array"},
    "ScalarAggregateOptions"};

const FunctionDoc mean_doc{
    "Compute the mean of a numeric array",
    ("Null values are ignored by default. Minimum count of non-null\n"
     "values can be set and null is returned if too few are present.\n"
     "This can be changed through ScalarAggregateOptions.\n"
     "The result is a double for integer and floating point arguments,\n"
     "and a decimal with the same bit-width/precision/scale for decimal arguments.\n"
     "For integers and floats, NaN is returned if min_count = 0 and\n"
     "there are no values. For decimals, null is returned instead."),
    {"array"},
    "ScalarAggregateOptions"};

const FunctionDoc min_max_doc{"Compute the minimum and maximum values of a numeric array",
                              ("Null values are ignored by default.\n"
                               "This can be changed through ScalarAggregateOptions."),
                              {"array"},
                              "ScalarAggregateOptions"};

const FunctionDoc min_or_max_doc{
    "Compute the minimum or maximum values of a numeric array",
    ("Null values are ignored by default.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array"},
    "ScalarAggregateOptions"};

const FunctionDoc any_doc{
    "Test whether any element in a boolean array evaluates to true",
    ("Null values are ignored by default.\n"
     "If the `skip_nulls` option is set to false, then Kleene logic is used.\n"
     "See \"kleene_or\" for more details on Kleene logic."),
    {"array"},
    "ScalarAggregateOptions"};

const FunctionDoc all_doc{
    "Test whether all elements in a boolean array evaluate to true",
    ("Null values are ignored by default.\n"
     "If the `skip_nulls` option is set to false, then Kleene logic is used.\n"
     "See \"kleene_and\" for more details on Kleene logic."),
    {"array"},
    "ScalarAggregateOptions"};

const FunctionDoc index_doc{"Find the index of the first occurrence of a given value",
                            ("-1 is returned if the value is not found in the array.\n"
                             "The search value is specified in IndexOptions."),
                            {"array"},
                            "IndexOptions",
                            /*options_required=*/true};

}  // namespace

void RegisterScalarAggregateBasic(FunctionRegistry* registry) {
  static auto default_scalar_aggregate_options = ScalarAggregateOptions::Defaults();
  static auto default_count_options = CountOptions::Defaults();

  auto func = std::make_shared<ScalarAggregateFunction>("count_all", Arity::Nullary(),
                                                        count_all_doc, NULLPTR);

  // Takes no input (counts all rows), outputs int64 scalar
  AddAggKernel(KernelSignature::Make({}, int64()), CountAllInit, func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("count", Arity::Unary(), count_doc,
                                                   &default_count_options);

  // Takes any input, outputs int64 scalar
  InputType any_input;
  AddAggKernel(KernelSignature::Make({any_input}, int64()), CountInit, func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>(
      "count_distinct", Arity::Unary(), count_distinct_doc, &default_count_options);
  // Takes any input, outputs int64 scalar
  AddCountDistinctKernels(func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary(), sum_doc,
                                                   &default_scalar_aggregate_options);
  AddArrayScalarAggKernels(SumInit, {boolean()}, uint64(), func.get());
  AddAggKernel(KernelSignature::Make({Type::DECIMAL128}, FirstType), SumInit, func.get(),
               SimdLevel::NONE);
  AddAggKernel(KernelSignature::Make({Type::DECIMAL256}, FirstType), SumInit, func.get(),
               SimdLevel::NONE);
  AddArrayScalarAggKernels(SumInit, SignedIntTypes(), int64(), func.get());
  AddArrayScalarAggKernels(SumInit, UnsignedIntTypes(), uint64(), func.get());
  AddArrayScalarAggKernels(SumInit, FloatingPointTypes(), float64(), func.get());
  AddArrayScalarAggKernels(SumInit, {null()}, int64(), func.get());
  // Add the SIMD variants for sum
#if defined(ARROW_HAVE_RUNTIME_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX512)
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    AddSumAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    AddSumAvx512AggKernels(func.get());
  }
#endif
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary(), mean_doc,
                                                   &default_scalar_aggregate_options);
  AddArrayScalarAggKernels(MeanInit, {boolean()}, float64(), func.get());
  AddArrayScalarAggKernels(MeanInit, NumericTypes(), float64(), func.get());
  AddAggKernel(KernelSignature::Make({Type::DECIMAL128}, FirstType), MeanInit, func.get(),
               SimdLevel::NONE);
  AddAggKernel(KernelSignature::Make({Type::DECIMAL256}, FirstType), MeanInit, func.get(),
               SimdLevel::NONE);
  AddArrayScalarAggKernels(MeanInit, {null()}, float64(), func.get());
  // Add the SIMD variants for mean
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    AddMeanAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    AddMeanAvx512AggKernels(func.get());
  }
#endif
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("min_max", Arity::Unary(), min_max_doc,
                                                   &default_scalar_aggregate_options);
  AddMinMaxKernels(MinMaxInit, {null(), boolean()}, func.get());
  AddMinMaxKernels(MinMaxInit, NumericTypes(), func.get());
  AddMinMaxKernels(MinMaxInit, TemporalTypes(), func.get());
  AddMinMaxKernels(MinMaxInit, BaseBinaryTypes(), func.get());
  AddMinMaxKernel(MinMaxInit, Type::FIXED_SIZE_BINARY, func.get());
  AddMinMaxKernel(MinMaxInit, Type::INTERVAL_MONTHS, func.get());
  AddMinMaxKernel(MinMaxInit, Type::DECIMAL128, func.get());
  AddMinMaxKernel(MinMaxInit, Type::DECIMAL256, func.get());
  // Add the SIMD variants for min max
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    AddMinMaxAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    AddMinMaxAvx512AggKernels(func.get());
  }
#endif

  auto min_max_func = func.get();
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // Add min/max as convenience functions
  func = std::make_shared<ScalarAggregateFunction>("min", Arity::Unary(), min_or_max_doc,
                                                   &default_scalar_aggregate_options);
  AddMinOrMaxAggKernel<MinOrMax::Min>(func.get(), min_max_func);
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("max", Arity::Unary(), min_or_max_doc,
                                                   &default_scalar_aggregate_options);
  AddMinOrMaxAggKernel<MinOrMax::Max>(func.get(), min_max_func);
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("product", Arity::Unary(), product_doc,
                                                   &default_scalar_aggregate_options);
  AddArrayScalarAggKernels(ProductInit::Init, {boolean()}, uint64(), func.get());
  AddArrayScalarAggKernels(ProductInit::Init, SignedIntTypes(), int64(), func.get());
  AddArrayScalarAggKernels(ProductInit::Init, UnsignedIntTypes(), uint64(), func.get());
  AddArrayScalarAggKernels(ProductInit::Init, FloatingPointTypes(), float64(),
                           func.get());
  AddAggKernel(KernelSignature::Make({Type::DECIMAL128}, FirstType), ProductInit::Init,
               func.get(), SimdLevel::NONE);
  AddAggKernel(KernelSignature::Make({Type::DECIMAL256}, FirstType), ProductInit::Init,
               func.get(), SimdLevel::NONE);
  AddArrayScalarAggKernels(ProductInit::Init, {null()}, int64(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // any
  func = std::make_shared<ScalarAggregateFunction>("any", Arity::Unary(), any_doc,
                                                   &default_scalar_aggregate_options);
  AddArrayScalarAggKernels(AnyInit, {boolean()}, boolean(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // all
  func = std::make_shared<ScalarAggregateFunction>("all", Arity::Unary(), all_doc,
                                                   &default_scalar_aggregate_options);
  AddArrayScalarAggKernels(AllInit, {boolean()}, boolean(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // index
  func = std::make_shared<ScalarAggregateFunction>("index", Arity::Unary(), index_doc);
  AddBasicAggKernels(IndexInit::Init, BaseBinaryTypes(), int64(), func.get());
  AddBasicAggKernels(IndexInit::Init, PrimitiveTypes(), int64(), func.get());
  AddBasicAggKernels(IndexInit::Init, TemporalTypes(), int64(), func.get());
  AddBasicAggKernels(IndexInit::Init,
                     {fixed_size_binary(1), decimal128(1, 0), decimal256(1, 0), null()},
                     int64(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

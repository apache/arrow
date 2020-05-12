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
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"

namespace arrow {
namespace compute {

namespace {

struct ScalarAggregator : public KernelState {
  virtual void Consume(KernelContext* ctx, const ExecBatch& batch) = 0;
  virtual void MergeFrom(KernelContext* ctx, const KernelState& src) = 0;
  virtual void Finalize(KernelContext* ctx, Datum* out) = 0;
};

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
  explicit CountImpl(CountOptions options)
      : options(std::move(options)), non_nulls(0), nulls(0) {}

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
      case CountOptions::COUNT_ALL:
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
  return std::unique_ptr<KernelState>(
      new CountImpl(static_cast<const CountOptions&>(*args.options)));
}

// ----------------------------------------------------------------------
// Sum implementation

template <typename ArrowType,
          typename SumType = typename FindAccumulatorType<ArrowType>::Type>
struct SumState {
  using ThisType = SumState<ArrowType, SumType>;
  using T = typename TypeTraits<ArrowType>::CType;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  // A small number of elements rounded to the next cacheline. This should
  // amount to a maximum of 4 cachelines when dealing with 8 bytes elements.
  static constexpr int64_t kTinyThreshold = 32;
  static_assert(kTinyThreshold >= (2 * CHAR_BIT) + 1,
                "ConsumeSparse requires 3 bytes of null bitmap, and 17 is the"
                "required minimum number of bits/elements to cover 3 bytes.");

  ThisType operator+(const ThisType& rhs) const {
    return ThisType(this->count + rhs.count, this->sum + rhs.sum);
  }

  ThisType& operator+=(const ThisType& rhs) {
    this->count += rhs.count;
    this->sum += rhs.sum;

    return *this;
  }

 public:
  void Consume(const Array& input) {
    const ArrayType& array = static_cast<const ArrayType&>(input);
    if (input.null_count() == 0) {
      (*this) += ConsumeDense(array);
    } else if (input.length() <= kTinyThreshold) {
      // In order to simplify ConsumeSparse implementation (requires at least 3
      // bytes of bitmap data), small arrays are handled differently.
      (*this) += ConsumeTiny(array);
    } else {
      (*this) += ConsumeSparse(array);
    }
  }

  size_t count = 0;
  typename SumType::c_type sum = 0;

 private:
  ThisType ConsumeDense(const ArrayType& array) const {
    ThisType local;
    const auto values = array.raw_values();
    const int64_t length = array.length();

    constexpr int64_t kRoundFactor = 8;
    const int64_t length_rounded = BitUtil::RoundDown(length, kRoundFactor);
    typename SumType::c_type sum_rounded[kRoundFactor] = {0};

    // Unrolled the loop to add the results in parrel
    for (int64_t i = 0; i < length_rounded; i += kRoundFactor) {
      for (int64_t k = 0; k < kRoundFactor; k++) {
        sum_rounded[k] += values[i + k];
      }
    }
    for (int64_t k = 0; k < kRoundFactor; k++) {
      local.sum += sum_rounded[k];
    }

    // The trailing part
    for (int64_t i = length_rounded; i < length; ++i) {
      local.sum += values[i];
    }

    local.count = length;
    return local;
  }

  ThisType ConsumeTiny(const ArrayType& array) const {
    ThisType local;

    BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
    const auto values = array.raw_values();
    for (int64_t i = 0; i < array.length(); i++) {
      if (reader.IsSet()) {
        local.sum += values[i];
        local.count++;
      }
      reader.Next();
    }

    return local;
  }

  // While this is not branchless, gcc needs this to be in a different function
  // for it to generate cmov which ends to be slightly faster than
  // multiplication but safe for handling NaN with doubles.
  inline T MaskedValue(bool valid, T value) const { return valid ? value : 0; }

  inline ThisType UnrolledSum(uint8_t bits, const T* values) const {
    ThisType local;

    if (bits < 0xFF) {
      // Some nulls
      for (size_t i = 0; i < 8; i++) {
        local.sum += MaskedValue(bits & (1U << i), values[i]);
      }
      local.count += BitUtil::kBytePopcount[bits];
    } else {
      // No nulls
      for (size_t i = 0; i < 8; i++) {
        local.sum += values[i];
      }
      local.count += 8;
    }

    return local;
  }

  ThisType ConsumeSparse(const ArrayType& array) const {
    ThisType local;

    // Sliced bitmaps on non-byte positions induce problem with the branchless
    // unrolled technique. Thus extra padding is added on both left and right
    // side of the slice such that both ends are byte-aligned. The first and
    // last bitmap are properly masked to ignore extra values induced by
    // padding.
    //
    // The execution is divided in 3 sections.
    //
    // 1. Compute the sum of the first masked byte.
    // 2. Compute the sum of the middle bytes
    // 3. Compute the sum of the last masked byte.

    const int64_t length = array.length();
    const int64_t offset = array.offset();

    // The number of bytes covering the range, this includes partial bytes.
    // This number bounded by `<= (length / 8) + 2`, e.g. a possible extra byte
    // on the left, and on the right.
    const int64_t covering_bytes = BitUtil::CoveringBytes(offset, length);
    DCHECK_GE(covering_bytes, 3);

    // Align values to the first batch of 8 elements. Note that raw_values() is
    // already adjusted with the offset, thus we rewind a little to align to
    // the closest 8-batch offset.
    const auto values = array.raw_values() - (offset % 8);

    // Align bitmap at the first consumable byte.
    const auto bitmap = array.null_bitmap_data() + BitUtil::RoundDown(offset, 8) / 8;

    // Consume the first (potentially partial) byte.
    const uint8_t first_mask = BitUtil::kTrailingBitmask[offset % 8];
    local += UnrolledSum(bitmap[0] & first_mask, values);

    // Consume the (full) middle bytes. The loop iterates in unit of
    // batches of 8 values and 1 byte of bitmap.
    for (int64_t i = 1; i < covering_bytes - 1; i++) {
      local += UnrolledSum(bitmap[i], &values[i * 8]);
    }

    // Consume the last (potentially partial) byte.
    const int64_t last_idx = covering_bytes - 1;
    const uint8_t last_mask = BitUtil::kPrecedingWrappingBitmask[(offset + length) % 8];
    local += UnrolledSum(bitmap[last_idx] & last_mask, &values[last_idx * 8]);

    return local;
  }
};

template <typename ArrowType>
struct SumImpl : public ScalarAggregator {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ThisType = SumImpl<ArrowType>;
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using OutputType = typename TypeTraits<SumType>::ScalarType;

  void Consume(KernelContext*, const ExecBatch& batch) override {
    this->state.Consume(ArrayType(batch[0].array()));
  }

  void MergeFrom(KernelContext*, const KernelState& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->state += other.state;
  }

  void Finalize(KernelContext*, Datum* out) override {
    if (state.count == 0) {
      out->value = std::make_shared<OutputType>();
    } else {
      out->value = MakeScalar(state.sum);
    }
  }

  SumState<ArrowType> state;
};

template <typename ArrowType>
struct MeanImpl : public SumImpl<ArrowType> {
  void Finalize(KernelContext*, Datum* out) override {
    const bool is_valid = this->state.count > 0;
    const double divisor = static_cast<double>(is_valid ? this->state.count : 1UL);
    const double mean = static_cast<double>(this->state.sum) / divisor;

    if (!is_valid) {
      out->value = std::make_shared<DoubleScalar>();
    } else {
      out->value = std::make_shared<DoubleScalar>(mean);
    }
  }
};

template <template <typename> class KernelClass>
struct SumLikeInit {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& type;

  SumLikeInit(KernelContext* ctx, const DataType& type) : ctx(ctx), type(type) {}

  Status Visit(const DataType&) { return Status::NotImplemented("No sum implemented"); }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No sum implemented");
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    state.reset(new KernelClass<Type>());
    return Status::OK();
  }

  std::unique_ptr<KernelState> Create() {
    ctx->SetStatus(VisitTypeInline(type, this));
    return std::move(state);
  }
};

std::unique_ptr<KernelState> SumInit(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<SumImpl> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

std::unique_ptr<KernelState> MeanInit(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<MeanImpl> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

// ----------------------------------------------------------------------
// MinMax implementation

template <typename ArrowType, typename Enable = void>
struct MinMaxState {};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_integer<ArrowType>> {
  using ThisType = MinMaxState<ArrowType>;
  using T = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
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
};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_floating_point<ArrowType>> {
  using ThisType = MinMaxState<ArrowType>;
  using T = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
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

    local.has_nulls = arr.null_count() > 0;
    if (local.has_nulls && options.null_handling == MinMaxOptions::OUTPUT_NULL) {
      this->state = local;
      return;
    }

    const auto values = arr.raw_values();
    if (arr.null_count() > 0) {
      BitmapReader reader(arr.null_bitmap_data(), arr.offset(), arr.length());
      for (int64_t i = 0; i < arr.length(); i++) {
        if (reader.IsSet()) {
          local.MergeOne(values[i]);
        }
        reader.Next();
      }
    } else {
      for (int64_t i = 0; i < arr.length(); i++) {
        local.MergeOne(values[i]);
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
    if (state.has_nulls && options.null_handling == MinMaxOptions::OUTPUT_NULL) {
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

}  // namespace

namespace internal {

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

void RegisterScalarAggregateBasic(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarAggregateFunction>("count", Arity::Unary());

  /// Takes any array input, outputs int64 scalar
  InputType any_array(ValueDescr::ARRAY);
  AddAggKernel(KernelSignature::Make({any_array}, ValueDescr::Scalar(int64())), CountInit,
               func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary());
  AddBasicAggKernels(SumInit, SignedIntTypes(), int64(), func.get());
  AddBasicAggKernels(SumInit, UnsignedIntTypes(), uint64(), func.get());
  AddBasicAggKernels(SumInit, FloatingPointTypes(), float64(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary());
  AddBasicAggKernels(MeanInit, NumericTypes(), float64(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("minmax", Arity::Unary());
  AddMinMaxKernels(MinMaxInit, NumericTypes(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

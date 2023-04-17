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

#include <cmath>
#include <utility>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/util/align_util.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/decimal.h"

namespace arrow {
namespace compute {
namespace internal {

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func,
                        SimdLevel::type simd_level = SimdLevel::NONE);

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func,
                      SimdLevel::type simd_level = SimdLevel::NONE);
void AddMinMaxKernel(KernelInit init, internal::detail::GetTypeId get_id,
                     ScalarAggregateFunction* func,
                     SimdLevel::type simd_level = SimdLevel::NONE);

// SIMD variants for kernels
void AddSumAvx2AggKernels(ScalarAggregateFunction* func);
void AddMeanAvx2AggKernels(ScalarAggregateFunction* func);
void AddMinMaxAvx2AggKernels(ScalarAggregateFunction* func);

void AddSumAvx512AggKernels(ScalarAggregateFunction* func);
void AddMeanAvx512AggKernels(ScalarAggregateFunction* func);
void AddMinMaxAvx512AggKernels(ScalarAggregateFunction* func);

// ----------------------------------------------------------------------
// Sum implementation

template <typename ArrowType, SimdLevel::type SimdLevel>
struct SumImpl : public ScalarAggregator {
  using ThisType = SumImpl<ArrowType, SimdLevel>;
  using CType = typename TypeTraits<ArrowType>::CType;
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using SumCType = typename TypeTraits<SumType>::CType;
  using OutputType = typename TypeTraits<SumType>::ScalarType;

  SumImpl(std::shared_ptr<DataType> out_type, const ScalarAggregateOptions& options_)
      : out_type(out_type), options(options_) {}

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (batch[0].is_array()) {
      const ArraySpan& data = batch[0].array;
      this->count += data.length - data.GetNullCount();
      this->nulls_observed = this->nulls_observed || data.GetNullCount();

      if (!options.skip_nulls && this->nulls_observed) {
        // Short-circuit
        return Status::OK();
      }

      if (is_boolean_type<ArrowType>::value) {
        this->sum += GetTrueCount(data);
      } else {
        this->sum += SumArray<CType, SumCType, SimdLevel>(data);
      }
    } else {
      const Scalar& data = *batch[0].scalar;
      this->count += data.is_valid * batch.length;
      this->nulls_observed = this->nulls_observed || !data.is_valid;
      if (data.is_valid) {
        this->sum += internal::UnboxScalar<ArrowType>::Unbox(data) * batch.length;
      }
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->count += other.count;
    this->sum += other.sum;
    this->nulls_observed = this->nulls_observed || other.nulls_observed;
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    if ((!options.skip_nulls && this->nulls_observed) ||
        (this->count < options.min_count)) {
      out->value = std::make_shared<OutputType>(out_type);
    } else {
      out->value = std::make_shared<OutputType>(this->sum, out_type);
    }
    return Status::OK();
  }

  size_t count = 0;
  bool nulls_observed = false;
  SumCType sum = 0;
  std::shared_ptr<DataType> out_type;
  ScalarAggregateOptions options;
};

template <typename ArrowType>
struct NullImpl : public ScalarAggregator {
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  explicit NullImpl(const ScalarAggregateOptions& options_) : options(options_) {}

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (batch[0].is_scalar() || batch[0].array.GetNullCount() > 0) {
      // If the batch is a scalar or an array with elements, set is_empty to false
      is_empty = false;
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const NullImpl&>(src);
    this->is_empty &= other.is_empty;
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    if ((options.skip_nulls || this->is_empty) && options.min_count == 0) {
      // Return 0 if the remaining data is empty
      out->value = output_empty();
    } else {
      out->value = MakeNullScalar(TypeTraits<ArrowType>::type_singleton());
    }
    return Status::OK();
  }

  virtual std::shared_ptr<Scalar> output_empty() = 0;

  bool is_empty = true;
  ScalarAggregateOptions options;
};

template <typename ArrowType>
struct NullSumImpl : public NullImpl<ArrowType> {
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  explicit NullSumImpl(const ScalarAggregateOptions& options_)
      : NullImpl<ArrowType>(options_) {}

  std::shared_ptr<Scalar> output_empty() override {
    return std::make_shared<ScalarType>(0);
  }
};

template <typename ArrowType, SimdLevel::type SimdLevel>
struct MeanImpl : public SumImpl<ArrowType, SimdLevel> {
  using SumImpl<ArrowType, SimdLevel>::SumImpl;

  template <typename T = ArrowType>
  enable_if_decimal<T, Status> FinalizeImpl(Datum* out) {
    using SumCType = typename SumImpl<ArrowType, SimdLevel>::SumCType;
    using OutputType = typename SumImpl<ArrowType, SimdLevel>::OutputType;
    if ((!options.skip_nulls && this->nulls_observed) ||
        (this->count < options.min_count) || (this->count == 0)) {
      out->value = std::make_shared<OutputType>(this->out_type);
    } else {
      SumCType quotient, remainder;
      ARROW_ASSIGN_OR_RAISE(std::tie(quotient, remainder), this->sum.Divide(this->count));
      // Round the decimal result based on the remainder
      remainder.Abs();
      if (remainder * 2 >= this->count) {
        if (this->sum >= 0) {
          quotient += 1;
        } else {
          quotient -= 1;
        }
      }
      out->value = std::make_shared<OutputType>(quotient, this->out_type);
    }
    return Status::OK();
  }
  template <typename T = ArrowType>
  enable_if_t<!is_decimal_type<T>::value, Status> FinalizeImpl(Datum* out) {
    if ((!options.skip_nulls && this->nulls_observed) ||
        (this->count < options.min_count)) {
      out->value = std::make_shared<DoubleScalar>();
    } else {
      const double mean = static_cast<double>(this->sum) / this->count;
      out->value = std::make_shared<DoubleScalar>(mean);
    }
    return Status::OK();
  }
  Status Finalize(KernelContext*, Datum* out) override { return FinalizeImpl(out); }

  using SumImpl<ArrowType, SimdLevel>::options;
};

template <template <typename> class KernelClass>
struct SumLikeInit {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  std::shared_ptr<DataType> type;
  const ScalarAggregateOptions& options;

  SumLikeInit(KernelContext* ctx, std::shared_ptr<DataType> type,
              const ScalarAggregateOptions& options)
      : ctx(ctx), type(type), options(options) {}

  Status Visit(const DataType&) { return Status::NotImplemented("No sum implemented"); }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No sum implemented");
  }

  Status Visit(const BooleanType&) {
    auto ty = TypeTraits<typename KernelClass<BooleanType>::SumType>::type_singleton();
    state.reset(new KernelClass<BooleanType>(ty, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    auto ty = TypeTraits<typename KernelClass<Type>::SumType>::type_singleton();
    state.reset(new KernelClass<Type>(ty, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_decimal<Type, Status> Visit(const Type&) {
    state.reset(new KernelClass<Type>(type, options));
    return Status::OK();
  }

  virtual Status Visit(const NullType&) {
    state.reset(new NullSumImpl<Int64Type>(options));
    return Status::OK();
  }

  Result<std::unique_ptr<KernelState>> Create() {
    RETURN_NOT_OK(VisitTypeInline(*type, this));
    return std::move(state);
  }
};

template <template <typename> class KernelClass>
struct MeanKernelInit : public SumLikeInit<KernelClass> {
  MeanKernelInit(KernelContext* ctx, std::shared_ptr<DataType> type,
                 const ScalarAggregateOptions& options)
      : SumLikeInit<KernelClass>(ctx, type, options) {}

  Status Visit(const NullType&) override {
    this->state.reset(new NullSumImpl<DoubleType>(this->options));
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// MinMax implementation

template <typename ArrowType, SimdLevel::type SimdLevel, typename Enable = void>
struct MinMaxState {};

template <typename ArrowType, SimdLevel::type SimdLevel>
struct MinMaxState<ArrowType, SimdLevel, enable_if_boolean<ArrowType>> {
  using ThisType = MinMaxState<ArrowType, SimdLevel>;
  using T = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
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
};

template <typename ArrowType, SimdLevel::type SimdLevel>
struct MinMaxState<ArrowType, SimdLevel, enable_if_integer<ArrowType>> {
  using ThisType = MinMaxState<ArrowType, SimdLevel>;
  using T = typename ArrowType::c_type;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

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

template <typename ArrowType, SimdLevel::type SimdLevel>
struct MinMaxState<ArrowType, SimdLevel, enable_if_floating_point<ArrowType>> {
  using ThisType = MinMaxState<ArrowType, SimdLevel>;
  using T = typename ArrowType::c_type;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

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

template <typename ArrowType, SimdLevel::type SimdLevel>
struct MinMaxState<ArrowType, SimdLevel, enable_if_decimal<ArrowType>> {
  using ThisType = MinMaxState<ArrowType, SimdLevel>;
  using T = typename TypeTraits<ArrowType>::CType;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  MinMaxState() : min(T::GetMaxSentinel()), max(T::GetMinSentinel()) {}

  ThisType& operator+=(const ThisType& rhs) {
    this->has_nulls |= rhs.has_nulls;
    this->min = std::min(this->min, rhs.min);
    this->max = std::max(this->max, rhs.max);
    return *this;
  }

  void MergeOne(std::string_view value) {
    MergeOne(T(reinterpret_cast<const uint8_t*>(value.data())));
  }

  void MergeOne(const T value) {
    this->min = std::min(this->min, value);
    this->max = std::max(this->max, value);
  }

  T min;
  T max;
  bool has_nulls = false;
};

template <typename ArrowType, SimdLevel::type SimdLevel>
struct MinMaxState<ArrowType, SimdLevel,
                   enable_if_t<is_base_binary_type<ArrowType>::value ||
                               std::is_same<ArrowType, FixedSizeBinaryType>::value>> {
  using ThisType = MinMaxState<ArrowType, SimdLevel>;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  ThisType& operator+=(const ThisType& rhs) {
    if (!this->seen && rhs.seen) {
      this->min = rhs.min;
      this->max = rhs.max;
    } else if (this->seen && rhs.seen) {
      if (this->min > rhs.min) {
        this->min = rhs.min;
      }
      if (this->max < rhs.max) {
        this->max = rhs.max;
      }
    }
    this->has_nulls |= rhs.has_nulls;
    this->seen |= rhs.seen;
    return *this;
  }

  void MergeOne(std::string_view value) {
    if (!seen) {
      this->min = std::string(value);
      this->max = std::string(value);
    } else {
      if (value < std::string_view(this->min)) {
        this->min = std::string(value);
      } else if (value > std::string_view(this->max)) {
        this->max = std::string(value);
      }
    }
    this->seen = true;
  }

  std::string min;
  std::string max;
  bool has_nulls = false;
  bool seen = false;
};

template <typename ArrowType, SimdLevel::type SimdLevel>
struct MinMaxImpl : public ScalarAggregator {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ThisType = MinMaxImpl<ArrowType, SimdLevel>;
  using StateType = MinMaxState<ArrowType, SimdLevel>;

  MinMaxImpl(std::shared_ptr<DataType> out_type, ScalarAggregateOptions options)
      : out_type(std::move(out_type)), options(std::move(options)), count(0) {
    this->options.min_count = std::max<uint32_t>(1, this->options.min_count);
  }

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (batch[0].is_array()) {
      return ConsumeArray(batch[0].array);
    }
    return ConsumeScalar(*batch[0].scalar);
  }

  Status ConsumeScalar(const Scalar& scalar) {
    StateType local;
    local.has_nulls = !scalar.is_valid;
    this->count += scalar.is_valid;

    if (!local.has_nulls || options.skip_nulls) {
      local.MergeOne(internal::UnboxScalar<ArrowType>::Unbox(scalar));
    }

    this->state += local;
    return Status::OK();
  }

  Status ConsumeArray(const ArraySpan& arr_span) {
    StateType local;

    ArrayType arr(arr_span.ToArrayData());

    const auto null_count = arr.null_count();
    local.has_nulls = null_count > 0;
    this->count += arr.length() - null_count;

    if (!local.has_nulls) {
      for (int64_t i = 0; i < arr.length(); i++) {
        local.MergeOne(arr.GetView(i));
      }
    } else if (local.has_nulls && options.skip_nulls) {
      local += ConsumeWithNulls(arr);
    }

    this->state += local;
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->state += other.state;
    this->count += other.count;
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    const auto& struct_type = checked_cast<const StructType&>(*out_type);
    const auto& child_type = struct_type.field(0)->type();

    std::vector<std::shared_ptr<Scalar>> values;
    // Physical type != result type
    if ((state.has_nulls && !options.skip_nulls) || (this->count < options.min_count)) {
      // (null, null)
      auto null_scalar = MakeNullScalar(child_type);
      values = {null_scalar, null_scalar};
    } else {
      ARROW_ASSIGN_OR_RAISE(auto min_scalar,
                            MakeScalar(child_type, std::move(state.min)));
      ARROW_ASSIGN_OR_RAISE(auto max_scalar,
                            MakeScalar(child_type, std::move(state.max)));
      values = {std::move(min_scalar), std::move(max_scalar)};
    }
    out->value = std::make_shared<StructScalar>(std::move(values), this->out_type);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type;
  ScalarAggregateOptions options;
  int64_t count;
  MinMaxState<ArrowType, SimdLevel> state;

 private:
  StateType ConsumeWithNulls(const ArrayType& arr) const {
    StateType local;
    const int64_t length = arr.length();
    int64_t offset = arr.offset();
    const uint8_t* bitmap = arr.null_bitmap_data();
    int64_t idx = 0;

    const auto p = arrow::internal::BitmapWordAlign<1>(bitmap, offset, length);
    // First handle the leading bits
    const int64_t leading_bits = p.leading_bits;
    while (idx < leading_bits) {
      if (bit_util::GetBit(bitmap, offset)) {
        local.MergeOne(arr.GetView(idx));
      }
      idx++;
      offset++;
    }

    // The aligned parts scanned with BitBlockCounter
    arrow::internal::BitBlockCounter data_counter(bitmap, offset, length - leading_bits);
    auto current_block = data_counter.NextWord();
    while (idx < length) {
      if (current_block.AllSet()) {  // All true values
        int run_length = 0;
        // Scan forward until a block that has some false values (or the end)
        while (current_block.length > 0 && current_block.AllSet()) {
          run_length += current_block.length;
          current_block = data_counter.NextWord();
        }
        for (int64_t i = 0; i < run_length; i++) {
          local.MergeOne(arr.GetView(idx + i));
        }
        idx += run_length;
        offset += run_length;
        // The current_block already computed, advance to next loop
        continue;
      } else if (!current_block.NoneSet()) {  // Some values are null
        BitmapReader reader(arr.null_bitmap_data(), offset, current_block.length);
        for (int64_t i = 0; i < current_block.length; i++) {
          if (reader.IsSet()) {
            local.MergeOne(arr.GetView(idx + i));
          }
          reader.Next();
        }

        idx += current_block.length;
        offset += current_block.length;
      } else {  // All null values
        idx += current_block.length;
        offset += current_block.length;
      }
      current_block = data_counter.NextWord();
    }

    return local;
  }
};

template <SimdLevel::type SimdLevel>
struct BooleanMinMaxImpl : public MinMaxImpl<BooleanType, SimdLevel> {
  using StateType = MinMaxState<BooleanType, SimdLevel>;
  using ArrayType = typename TypeTraits<BooleanType>::ArrayType;
  using MinMaxImpl<BooleanType, SimdLevel>::MinMaxImpl;
  using MinMaxImpl<BooleanType, SimdLevel>::options;

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (ARROW_PREDICT_FALSE(batch[0].is_scalar())) {
      return ConsumeScalar(checked_cast<const BooleanScalar&>(*batch[0].scalar));
    }
    StateType local;
    ArrayType arr(batch[0].array.ToArrayData());

    const auto arr_length = arr.length();
    const auto null_count = arr.null_count();
    const auto valid_count = arr_length - null_count;

    local.has_nulls = null_count > 0;
    this->count += valid_count;
    if (!local.has_nulls || options.skip_nulls) {
      const auto true_count = arr.true_count();
      const auto false_count = valid_count - true_count;
      local.max = true_count > 0;
      local.min = false_count == 0;
    }

    this->state += local;
    return Status::OK();
  }

  Status ConsumeScalar(const BooleanScalar& scalar) {
    StateType local;

    local.has_nulls = !scalar.is_valid;
    this->count += scalar.is_valid;
    if (!local.has_nulls || options.skip_nulls) {
      const int true_count = scalar.is_valid && scalar.value;
      const int false_count = scalar.is_valid && !scalar.value;
      local.max = true_count > 0;
      local.min = false_count == 0;
    }

    this->state += local;
    return Status::OK();
  }
};

struct NullMinMaxImpl : public ScalarAggregator {
  Status Consume(KernelContext*, const ExecSpan& batch) override { return Status::OK(); }

  Status MergeFrom(KernelContext*, KernelState&& src) override { return Status::OK(); }

  Status Finalize(KernelContext*, Datum* out) override {
    std::vector<std::shared_ptr<Scalar>> values{std::make_shared<NullScalar>(),
                                                std::make_shared<NullScalar>()};
    out->value = std::make_shared<StructScalar>(
        std::move(values), struct_({field("min", null()), field("max", null())}));
    return Status::OK();
  }
};

template <SimdLevel::type SimdLevel>
struct MinMaxInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  std::shared_ptr<DataType> out_type;
  const ScalarAggregateOptions& options;

  MinMaxInitState(KernelContext* ctx, const DataType& in_type,
                  const std::shared_ptr<DataType>& out_type,
                  const ScalarAggregateOptions& options)
      : ctx(ctx), in_type(in_type), out_type(out_type), options(options) {}

  Status Visit(const DataType& ty) {
    return Status::NotImplemented("No min/max implemented for ", ty);
  }

  Status Visit(const HalfFloatType& ty) {
    return Status::NotImplemented("No min/max implemented for ", ty);
  }

  Status Visit(const NullType&) {
    state.reset(new NullMinMaxImpl());
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    state.reset(new BooleanMinMaxImpl<SimdLevel>(out_type, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_physical_integer<Type, Status> Visit(const Type&) {
    using PhysicalType = typename Type::PhysicalType;
    state.reset(new MinMaxImpl<PhysicalType, SimdLevel>(out_type, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_floating_point<Type, Status> Visit(const Type&) {
    state.reset(new MinMaxImpl<Type, SimdLevel>(out_type, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_base_binary<Type, Status> Visit(const Type&) {
    state.reset(new MinMaxImpl<Type, SimdLevel>(out_type, options));
    return Status::OK();
  }

  template <typename Type>
  enable_if_fixed_size_binary<Type, Status> Visit(const Type&) {
    state.reset(new MinMaxImpl<Type, SimdLevel>(out_type, options));
    return Status::OK();
  }

  Result<std::unique_ptr<KernelState>> Create() {
    RETURN_NOT_OK(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/align_util.h"
#include "arrow/util/bit_block_counter.h"

namespace arrow {
namespace compute {
namespace aggregate {

struct ScalarAggregator : public KernelState {
  virtual void Consume(KernelContext* ctx, const ExecBatch& batch) = 0;
  virtual void MergeFrom(KernelContext* ctx, const KernelState& src) = 0;
  virtual void Finalize(KernelContext* ctx, Datum* out) = 0;
};

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func);

// ----------------------------------------------------------------------
// Sum implementation

template <int64_t kRoundSize, typename ArrowType>
struct SumState {
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using ThisType = SumState<kRoundSize, ArrowType>;
  using T = typename TypeTraits<ArrowType>::CType;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

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
      (*this) += ConsumeNoNulls(array);
    } else {
      (*this) += ConsumeWithNulls(array);
    }
  }

  size_t count = 0;
  typename SumType::c_type sum = 0;

 private:
  template <int64_t kNoNullsRoundSize>
  ThisType ConsumeNoNulls(const T* values, const int64_t length) const {
    ThisType local;
    const int64_t length_rounded = BitUtil::RoundDown(length, kNoNullsRoundSize);
    typename SumType::c_type sum_rounded[kNoNullsRoundSize] = {0};

    // Unroll the loop to add the results in parallel
    for (int64_t i = 0; i < length_rounded; i += kNoNullsRoundSize) {
      for (int64_t k = 0; k < kNoNullsRoundSize; k++) {
        sum_rounded[k] += values[i + k];
      }
    }
    for (int64_t k = 0; k < kNoNullsRoundSize; k++) {
      local.sum += sum_rounded[k];
    }

    // The trailing part
    for (int64_t i = length_rounded; i < length; ++i) {
      local.sum += values[i];
    }

    local.count = length;
    return local;
  }

  ThisType ConsumeNoNulls(const ArrayType& array) const {
    const auto values = array.raw_values();
    const int64_t length = array.length();

    return ConsumeNoNulls<kRoundSize>(values, length);
  }

  // While this is not branchless, gcc needs this to be in a different function
  // for it to generate cmov which tends to be slightly faster than
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

  ThisType ConsumeWithNulls(const ArrayType& array) const {
    ThisType local;
    const T* values = array.raw_values();
    const int64_t length = array.length();
    int64_t offset = array.offset();
    const uint8_t* bitmap = array.null_bitmap_data();
    int64_t idx = 0;

    const auto p = arrow::internal::BitmapWordAlign<1>(bitmap, offset, length);
    // First handle the leading bits
    const int64_t leading_bits = p.leading_bits;
    while (idx < leading_bits) {
      if (BitUtil::GetBit(bitmap, offset)) {
        local.sum += values[idx];
        local.count++;
      }
      idx++;
      offset++;
    }

    // The aligned parts scanned with BitBlockCounter
    constexpr int64_t kBatchSize = arrow::internal::BitBlockCounter::kWordBits;
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
        // Aggregate the no nulls parts
        if (run_length >= kRoundSize * 8) {
          local += ConsumeNoNulls<kRoundSize>(&values[idx], run_length);
        } else {
          local += ConsumeNoNulls<8>(&values[idx], run_length);
        }
        idx += run_length;
        offset += run_length;
        // The current_block already computed, advance to next loop
        continue;
      } else if (!current_block.NoneSet()) {  // Some values are null
        const int64_t idx_byte = BitUtil::BytesForBits(offset);
        const uint8_t* aligned_bitmap = &bitmap[idx_byte];
        const T* aligned_values = &values[idx];

        if (kBatchSize == current_block.length) {
          for (int64_t i = 0; i < kBatchSize / 8; i++) {
            local += UnrolledSum(aligned_bitmap[i], &aligned_values[i * 8]);
          }
        } else {  // The end part
          for (int64_t i = 0; i < current_block.length; i++) {
            if (BitUtil::GetBit(bitmap, offset)) {
              local.sum += values[idx];
              local.count++;
            }
            idx++;
            offset++;
          }
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

template <int64_t kRoundSize>
struct SumState<kRoundSize, BooleanType> {
  using SumType = typename FindAccumulatorType<BooleanType>::Type;
  using ThisType = SumState<kRoundSize, BooleanType>;

  ThisType& operator+=(const ThisType& rhs) {
    this->count += rhs.count;
    this->sum += rhs.sum;
    return *this;
  }

 public:
  void Consume(const Array& input) {
    const BooleanArray& array = static_cast<const BooleanArray&>(input);
    count += array.length() - array.null_count();
    sum += array.true_count();
  }

  size_t count = 0;
  typename SumType::c_type sum = 0;
};

template <uint64_t kRoundSize, typename ArrowType>
struct SumImpl : public ScalarAggregator {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ThisType = SumImpl<kRoundSize, ArrowType>;
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

  SumState<kRoundSize, ArrowType> state;
};

template <int64_t kRoundSize, typename ArrowType>
struct MeanImpl : public SumImpl<kRoundSize, ArrowType> {
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

  Status Visit(const BooleanType&) {
    state.reset(new KernelClass<BooleanType>());
    return Status::OK();
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

}  // namespace aggregate
}  // namespace compute
}  // namespace arrow

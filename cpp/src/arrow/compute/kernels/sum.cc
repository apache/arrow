// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
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

#include "arrow/compute/kernels/sum.h"

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregate.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

template <typename ArrowType,
          typename SumType = typename FindAccumulatorType<ArrowType>::Type>
struct SumState {
  using ThisType = SumState<ArrowType, SumType>;

  ThisType operator+(const ThisType& rhs) const {
    return ThisType(this->count + rhs.count, this->sum + rhs.sum);
  }

  ThisType& operator+=(const ThisType& rhs) {
    this->count += rhs.count;
    this->sum += rhs.sum;

    return *this;
  }

  std::shared_ptr<Scalar> AsScalar() const {
    using ScalarType = typename TypeTraits<SumType>::ScalarType;
    return std::make_shared<ScalarType>(this->sum);
  }

  size_t count = 0;
  typename SumType::c_type sum = 0;
};

constexpr int64_t CoveringBytes(int64_t offset, int64_t length) {
  return (BitUtil::RoundUp(length + offset, 8) - BitUtil::RoundDown(offset, 8)) / 8;
}

static_assert(CoveringBytes(0, 8) == 1, "");
static_assert(CoveringBytes(0, 9) == 2, "");
static_assert(CoveringBytes(1, 7) == 1, "");
static_assert(CoveringBytes(1, 8) == 2, "");
static_assert(CoveringBytes(2, 19) == 3, "");
static_assert(CoveringBytes(7, 18) == 4, "");

template <typename ArrowType, typename StateType = SumState<ArrowType>>
class SumAggregateFunction final : public AggregateFunctionStaticState<StateType> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  static constexpr int64_t kTinyThreshold = 32;
  static_assert(kTinyThreshold > 18,
                "ConsumeSparse requires at least 18 elements to fit 3 bytes");

 public:
  Status Consume(const Array& input, StateType* state) const override {
    const ArrayType& array = static_cast<const ArrayType&>(input);

    if (input.null_count() == 0) {
      *state = ConsumeDense(array);
    } else if (input.length() <= kTinyThreshold) {
      // In order to simplify ConsumeSparse implementation (requires at least 3
      // bytes of bitmap data), small arrays are handled differently.
      *state = ConsumeTiny(array);
    } else {
      *state = ConsumeSparse(array);
    }

    return Status::OK();
  }

  Status Merge(const StateType& src, StateType* dst) const override {
    *dst += src;
    return Status::OK();
  }

  Status Finalize(const StateType& src, Datum* output) const override {
    auto boxed = src.AsScalar();
    if (src.count == 0) {
      // TODO(wesm): Currently null, but fix this
      boxed->is_valid = false;
    }
    *output = boxed;
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override {
    return TypeTraits<typename FindAccumulatorType<ArrowType>::Type>::type_singleton();
  }

 private:
  StateType ConsumeDense(const ArrayType& array) const {
    StateType local;

    const auto values = array.raw_values();
    const int64_t length = array.length();
    for (int64_t i = 0; i < length; i++) {
      local.sum += values[i];
    }

    local.count = length;

    return local;
  }

  StateType ConsumeTiny(const ArrayType& array) const {
    StateType local;

    internal::BitmapReader reader(array.null_bitmap_data(), array.offset(),
                                  array.length());
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

  inline StateType UnrolledSum(uint8_t bits, const CType* values) const {
    StateType local;

    if (bits < 0xFF) {
#define SUM_SHIFT(ITEM) values[ITEM] * static_cast<CType>(((bits >> ITEM) & 1U))
      // Some nulls
      local.sum += SUM_SHIFT(0);
      local.sum += SUM_SHIFT(1);
      local.sum += SUM_SHIFT(2);
      local.sum += SUM_SHIFT(3);
      local.sum += SUM_SHIFT(4);
      local.sum += SUM_SHIFT(5);
      local.sum += SUM_SHIFT(6);
      local.sum += SUM_SHIFT(7);
      local.count += BitUtil::kBytePopcount[bits];
#undef SUM_SHIFT
    } else {
      // No nulls
      for (size_t i = 0; i < 8; i++) {
        local.sum += values[i];
      }
      local.count += 8;
    }

    return local;
  }

  StateType ConsumeSparse(const ArrayType& array) const {
    StateType local;

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
    const int64_t covering_bytes = CoveringBytes(offset, length);

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

#define SUM_AGG_FN_CASE(T)                              \
  case T::type_id:                                      \
    return std::static_pointer_cast<AggregateFunction>( \
        std::make_shared<SumAggregateFunction<T>>());

std::shared_ptr<AggregateFunction> MakeSumAggregateFunction(const DataType& type,
                                                            FunctionContext* ctx) {
  switch (type.id()) {
    SUM_AGG_FN_CASE(UInt8Type);
    SUM_AGG_FN_CASE(Int8Type);
    SUM_AGG_FN_CASE(UInt16Type);
    SUM_AGG_FN_CASE(Int16Type);
    SUM_AGG_FN_CASE(UInt32Type);
    SUM_AGG_FN_CASE(Int32Type);
    SUM_AGG_FN_CASE(UInt64Type);
    SUM_AGG_FN_CASE(Int64Type);
    SUM_AGG_FN_CASE(FloatType);
    SUM_AGG_FN_CASE(DoubleType);
    default:
      return nullptr;
  }

#undef SUM_AGG_FN_CASE
}

static Status GetSumKernel(FunctionContext* ctx, const DataType& type,
                           std::shared_ptr<AggregateUnaryKernel>& kernel) {
  std::shared_ptr<AggregateFunction> aggregate = MakeSumAggregateFunction(type, ctx);
  if (!aggregate) return Status::Invalid("No sum for type ", type);

  kernel = std::make_shared<AggregateUnaryKernel>(aggregate);

  return Status::OK();
}

Status Sum(FunctionContext* ctx, const Datum& value, Datum* out) {
  std::shared_ptr<AggregateUnaryKernel> kernel;

  auto data_type = value.type();
  if (data_type == nullptr)
    return Status::Invalid("Datum must be array-like");
  else if (!is_integer(data_type->id()) && !is_floating(data_type->id()))
    return Status::Invalid("Datum must contain a NumericType");

  RETURN_NOT_OK(GetSumKernel(ctx, *data_type, kernel));

  return kernel->Call(ctx, value, out);
}

Status Sum(FunctionContext* ctx, const Array& array, Datum* out) {
  return Sum(ctx, array.data(), out);
}

}  // namespace compute
}  // namespace arrow

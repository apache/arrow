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

#include <memory>
#include <type_traits>

#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregate.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"

namespace arrow {

class Array;
class DataType;

namespace compute {

// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, enable_if_signed_integer<I>> {
  using Type = Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_unsigned_integer<I>> {
  using Type = UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_floating_point<I>> {
  using Type = DoubleType;
};

template <typename ArrowType, typename StateType>
class SumAggregateFunction final : public AggregateFunctionStaticState<StateType> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  // A small number of elements rounded to the next cacheline. This should
  // amount to a maximum of 4 cachelines when dealing with 8 bytes elements.
  static constexpr int64_t kTinyThreshold = 32;
  static_assert(kTinyThreshold >= (2 * CHAR_BIT) + 1,
                "ConsumeSparse requires 3 bytes of null bitmap, and 17 is the"
                "required minimum number of bits/elements to cover 3 bytes.");

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
    *output = src.Finalize();
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return StateType::out_type(); }

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

  // While this is not branchless, gcc needs this to be in a different function
  // for it to generate cmov which ends to be slightly faster than
  // multiplication but safe for handling NaN with doubles.
  inline CType MaskedValue(bool valid, CType value) const { return valid ? value : 0; }

  inline StateType UnrolledSum(uint8_t bits, const CType* values) const {
    StateType local;

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
};  // namespace compute

}  // namespace compute
}  // namespace arrow

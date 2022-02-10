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

#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/array/builder_union.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename Type, typename Enable = void>
struct CopyDataUtils {};

template <>
struct CopyDataUtils<BooleanType> {
  static void CopyScalar(const Scalar& scalar, const int64_t length,
                         uint8_t* raw_out_values, const int64_t out_offset) {
    const bool value = UnboxScalar<BooleanType>::Unbox(scalar);
    bit_util::SetBitsTo(raw_out_values, out_offset, length, value);
  }

  static void CopyArray(const DataType&, const uint8_t* in_values,
                        const int64_t in_offset, const int64_t length,
                        uint8_t* raw_out_values, const int64_t out_offset) {
    arrow::internal::CopyBitmap(in_values, in_offset, length, raw_out_values, out_offset);
  }

  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const ArrayData& in, const int64_t in_offset,
                       const int64_t length) {
    const auto in_arr = in.GetValues<uint8_t>(1, /*absolute_offset=*/0);
    arrow::internal::CopyBitmap(in_arr, in_offset + in.offset, length, out, out_offset);
  }

  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const Scalar& in, const int64_t in_offset, const int64_t length) {
    bit_util::SetBitsTo(
        out, out_offset, length,
        in.is_valid ? checked_cast<const BooleanScalar&>(in).value : false);
  }
};

template <>
struct CopyDataUtils<FixedSizeBinaryType> {
  static void CopyScalar(const Scalar& values, const int64_t length,
                         uint8_t* raw_out_values, const int64_t out_offset) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(*values.type).byte_width();
    uint8_t* next = raw_out_values + (width * out_offset);
    const auto& scalar =
        checked_cast<const arrow::internal::PrimitiveScalarBase&>(values);
    // Scalar may have null value buffer
    if (!scalar.is_valid) {
      std::memset(next, 0x00, width * length);
    } else {
      util::string_view view = scalar.view();
      DCHECK_EQ(view.size(), static_cast<size_t>(width));
      for (int i = 0; i < length; i++) {
        std::memcpy(next, view.data(), width);
        next += width;
      }
    }
  }

  static void CopyArray(const DataType& type, const uint8_t* in_values,
                        const int64_t in_offset, const int64_t length,
                        uint8_t* raw_out_values, const int64_t out_offset) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(type).byte_width();
    uint8_t* next = raw_out_values + (width * out_offset);
    std::memcpy(next, in_values + in_offset * width, length * width);
  }

  static void CopyData(const DataType& ty, uint8_t* out, const int64_t out_offset,
                       const ArrayData& in, const int64_t in_offset,
                       const int64_t length) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(ty).byte_width();
    uint8_t* begin = out + (out_offset * width);
    const auto in_arr = in.GetValues<uint8_t>(1, (in_offset + in.offset) * width);
    std::memcpy(begin, in_arr, length * width);
  }

  static void CopyData(const DataType& ty, uint8_t* out, const int64_t out_offset,
                       const Scalar& in, const int64_t in_offset, const int64_t length) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(ty).byte_width();
    uint8_t* begin = out + (out_offset * width);
    const auto& scalar = checked_cast<const arrow::internal::PrimitiveScalarBase&>(in);
    // Null scalar may have null value buffer
    if (!scalar.is_valid) return;
    const util::string_view buffer = scalar.view();
    DCHECK_GE(buffer.size(), static_cast<size_t>(width));
    for (int i = 0; i < length; i++) {
      std::memcpy(begin, buffer.data(), width);
      begin += width;
    }
  }
};

template <typename Type>
struct CopyDataUtils<
    Type, enable_if_t<is_number_type<Type>::value || is_interval_type<Type>::value>> {
  using CType = typename TypeTraits<Type>::CType;
  static void CopyScalar(const Scalar& scalar, const int64_t length,
                         uint8_t* raw_out_values, const int64_t out_offset) {
    CType* out_values = reinterpret_cast<CType*>(raw_out_values);
    const CType value = UnboxScalar<Type>::Unbox(scalar);
    std::fill(out_values + out_offset, out_values + out_offset + length, value);
  }
  static void CopyArray(const DataType&, const uint8_t* in_values,
                        const int64_t in_offset, const int64_t length,
                        uint8_t* raw_out_values, const int64_t out_offset) {
    std::memcpy(raw_out_values + out_offset * sizeof(CType),
                in_values + in_offset * sizeof(CType), length * sizeof(CType));
  }

  using T = typename TypeTraits<Type>::CType;

  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const ArrayData& in, const int64_t in_offset,
                       const int64_t length) {
    const auto in_arr = in.GetValues<uint8_t>(1, (in_offset + in.offset) * sizeof(T));
    std::memcpy(out + (out_offset * sizeof(T)), in_arr, length * sizeof(T));
  }

  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const Scalar& in, const int64_t in_offset, const int64_t length) {
    T* begin = reinterpret_cast<T*>(out + (out_offset * sizeof(T)));
    T* end = begin + length;
    std::fill(begin, end, UnboxScalar<Type>::Unbox(in));
  }
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow

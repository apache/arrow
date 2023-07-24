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

#include "arrow/compute/kernels/codegen_internal.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename Type, typename Enable = void>
struct CopyDataUtils {};

template <>
struct CopyDataUtils<BooleanType> {
  static void CopyData(const DataType&, const Scalar& in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    bit_util::SetBitsTo(
        out, out_offset, length,
        in.is_valid ? checked_cast<const BooleanScalar&>(in).value : false);
  }

  static void CopyData(const DataType&, const uint8_t* in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    arrow::internal::CopyBitmap(in, in_offset, length, out, out_offset);
  }

  static void CopyData(const DataType&, const ArraySpan& in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    const auto in_arr = in.GetValues<uint8_t>(1, /*absolute_offset=*/0);
    CopyData(*in.type, in_arr, in_offset, out, out_offset, length);
  }
};

template <>
struct CopyDataUtils<FixedSizeBinaryType> {
  static void CopyData(const DataType& ty, const Scalar& in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    const int32_t width = ty.byte_width();
    uint8_t* begin = out + (width * out_offset);
    const auto& scalar = checked_cast<const arrow::internal::PrimitiveScalarBase&>(in);
    // Null scalar may have null value buffer
    if (!scalar.is_valid) {
      std::memset(begin, 0x00, width * length);
    } else {
      const std::string_view buffer = scalar.view();
      DCHECK_GE(buffer.size(), static_cast<size_t>(width));
      for (int i = 0; i < length; i++) {
        std::memcpy(begin, buffer.data(), width);
        begin += width;
      }
    }
  }

  static void CopyData(const DataType& ty, const uint8_t* in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    const int32_t width = ty.byte_width();
    uint8_t* begin = out + (width * out_offset);
    std::memcpy(begin, in + in_offset * width, length * width);
  }

  static void CopyData(const DataType& ty, const ArraySpan& in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    const int32_t width = ty.byte_width();
    const auto in_arr = in.GetValues<uint8_t>(1, in.offset * width);
    CopyData(ty, in_arr, in_offset, out, out_offset, length);
  }
};

template <typename Type>
struct CopyDataUtils<
    Type, enable_if_t<is_number_type<Type>::value || is_interval_type<Type>::value>> {
  using CType = typename TypeTraits<Type>::CType;

  static void CopyData(const DataType&, const Scalar& in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    CType* begin = reinterpret_cast<CType*>(out) + out_offset;
    CType* end = begin + length;
    std::fill(begin, end, UnboxScalar<Type>::Unbox(in));
  }

  static void CopyData(const DataType&, const uint8_t* in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    std::memcpy(out + out_offset * sizeof(CType), in + in_offset * sizeof(CType),
                length * sizeof(CType));
  }

  static void CopyData(const DataType&, const ArraySpan& in, const int64_t in_offset,
                       uint8_t* out, const int64_t out_offset, const int64_t length) {
    const auto in_arr = in.GetValues<uint8_t>(1, in.offset * sizeof(CType));
    CopyData(*in.type, in_arr, in_offset, out, out_offset, length);
  }
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow

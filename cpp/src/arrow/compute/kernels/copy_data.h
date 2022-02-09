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
struct CopyFixedWidth {};
template <>
struct CopyFixedWidth<BooleanType> {
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
};

template <typename Type>
struct CopyFixedWidth<
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
};

template <typename Type>
struct CopyFixedWidth<Type, enable_if_fixed_size_binary<Type>> {
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
};

template <typename Type, typename Enable = void>
struct ReplaceWithMask {};

template <typename Type>
struct ReplaceWithMask<Type,
                       enable_if_t<is_number_type<Type>::value ||
                                   std::is_same<Type, MonthDayNanoIntervalType>::value>> {
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

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                        output);
  }

  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                       output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_boolean<Type>> {
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

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                        output);
  }
  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                       output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_fixed_size_binary<Type>> {
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

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                        output);
  }

  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                       output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_null<Type>> {
  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    *output = array;
    return Status::OK();
  }
  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    *output = array;
    return Status::OK();
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    if (!mask.is_valid) {
      // Output = null
      ARROW_ASSIGN_OR_RAISE(
          auto replacement_array,
          MakeArrayOfNull(array.type, array.length, ctx->memory_pool()));
      *output = *replacement_array->data();
    } else if (mask.value) {
      // Output = replacement
      if (replacements.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(auto replacement_array,
                              MakeArrayFromScalar(*replacements.scalar(), array.length,
                                                  ctx->memory_pool()));
        *output = *replacement_array->data();
      } else {
        const ArrayData& replacement_array = *replacements.array();
        if (replacement_array.length < array.length) {
          return ReplacementArrayTooShort(array.length, replacement_array.length);
        }
        *output = replacement_array;
        output->length = array.length;
      }
    } else {
      // Output = input
      *output = array;
    }
    return Status::OK();
  }
  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    BuilderType builder(array.type, ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(array.length));
    RETURN_NOT_OK(builder.ReserveData(array.buffers[2]->size()));
    int64_t source_offset = 0;
    int64_t replacements_offset = 0;
    RETURN_NOT_OK(VisitArrayDataInline<BooleanType>(
        mask,
        [&](bool replace) {
          if (replace && replacements.is_scalar()) {
            const Scalar& scalar = *replacements.scalar();
            if (scalar.is_valid) {
              RETURN_NOT_OK(builder.Append(UnboxScalar<Type>::Unbox(scalar)));
            } else {
              RETURN_NOT_OK(builder.AppendNull());
            }
          } else {
            const ArrayData& source = replace ? *replacements.array() : array;
            const int64_t offset = replace ? replacements_offset++ : source_offset;
            if (!source.MayHaveNulls() ||
                bit_util::GetBit(source.buffers[0]->data(), source.offset + offset)) {
              const uint8_t* data = source.buffers[2]->data();
              const offset_type* offsets = source.GetValues<offset_type>(1);
              const offset_type offset0 = offsets[offset];
              const offset_type offset1 = offsets[offset + 1];
              RETURN_NOT_OK(builder.Append(data + offset0, offset1 - offset0));
            } else {
              RETURN_NOT_OK(builder.AppendNull());
            }
          }
          source_offset++;
          return Status::OK();
        },
        [&]() {
          RETURN_NOT_OK(builder.AppendNull());
          source_offset++;
          return Status::OK();
        }));
    std::shared_ptr<Array> temp_output;
    RETURN_NOT_OK(builder.Finish(&temp_output));
    *output = *temp_output->data();
    // Builder type != logical type due to GenerateTypeAgnosticVarBinaryBase
    output->type = array.type;
    return Status::OK();
  }
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow
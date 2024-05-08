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

#include <cstdint>
#include <optional>
#include <utility>

#include "arrow/array/data.h"
#include "arrow/compute/kernel.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/small_vector.h"

namespace arrow::util {

using ::arrow::internal::checked_cast;

bool IsFixedWidthLike(const ArraySpan& source, bool force_null_count,
                      bool exclude_dictionary) {
  return IsFixedWidthLike(source, force_null_count,
                          [exclude_dictionary](const DataType& type) {
                            return !exclude_dictionary || type.id() != Type::DICTIONARY;
                          });
}

static int64_t FixedWidthInBytesFallback(const FixedSizeListType& fixed_size_list_type) {
  auto* fsl = &fixed_size_list_type;
  int64_t list_size = fsl->list_size();
  for (auto type = fsl->value_type().get();;) {
    if (type->id() == Type::FIXED_SIZE_LIST) {
      fsl = checked_cast<const FixedSizeListType*>(type);
      list_size *= fsl->list_size();
      type = fsl->value_type().get();
      continue;
    }
    if (type->id() != Type::BOOL && is_fixed_width(type->id())) {
      const int64_t flat_byte_width = list_size * type->byte_width();
      DCHECK_GE(flat_byte_width, 0);
      return flat_byte_width;
    }
    break;
  }
  return -1;
}

int64_t FixedWidthInBytes(const DataType& type) {
  auto type_id = type.id();
  if (is_fixed_width(type_id)) {
    const int32_t num_bits = type.bit_width();
    return (type_id == Type::BOOL) ? -1 : num_bits / 8;
  }
  if (type_id == Type::FIXED_SIZE_LIST) {
    auto& fsl = ::arrow::internal::checked_cast<const FixedSizeListType&>(type);
    return FixedWidthInBytesFallback(fsl);
  }
  return -1;
}

int64_t FixedWidthInBits(const DataType& type) {
  auto type_id = type.id();
  if (is_fixed_width(type_id)) {
    return type.bit_width();
  }
  const int64_t byte_width = FixedWidthInBytes(type);
  if (ARROW_PREDICT_FALSE(byte_width < 0)) {
    return -1;
  }
  return byte_width * 8;
}

namespace internal {

Status PreallocateFixedWidthArrayData(::arrow::compute::KernelContext* ctx,
                                      int64_t length, const ArraySpan& source,
                                      bool allocate_validity, ArrayData* out) {
  DCHECK(!source.MayHaveNulls() || allocate_validity)
      << "allocate_validity cannot be false if source may have nulls";
  DCHECK_EQ(source.type->id(), out->type->id());
  auto* type = source.type;
  out->length = length;
  if (type->id() == Type::FIXED_SIZE_LIST) {
    out->buffers.resize(1);
    out->child_data = {std::make_shared<ArrayData>()};
  } else {
    out->buffers.resize(2);
  }
  if (allocate_validity) {
    ARROW_ASSIGN_OR_RAISE(out->buffers[0], ctx->AllocateBitmap(length));
  }

  if (type->id() == Type::BOOL) {
    ARROW_ASSIGN_OR_RAISE(out->buffers[1], ctx->AllocateBitmap(length));
    return Status::OK();
  }
  if (is_fixed_width(type->id())) {
    if (type->id() == Type::DICTIONARY) {
      return Status::NotImplemented(
          "PreallocateFixedWidthArrayData: DICTIONARY type allocation: ", *type);
    }
    ARROW_ASSIGN_OR_RAISE(out->buffers[1],
                          ctx->Allocate(length * source.type->byte_width()));
    return Status::OK();
  }
  if (type->id() == Type::FIXED_SIZE_LIST) {
    auto& fsl_type = checked_cast<const FixedSizeListType&>(*type);
    auto& value_type = fsl_type.value_type();
    if (ARROW_PREDICT_FALSE(value_type->id() == Type::BOOL)) {
      return Status::Invalid("PreallocateFixedWidthArrayData: Invalid type: ", fsl_type);
    }
    if (ARROW_PREDICT_FALSE(value_type->id() == Type::DICTIONARY)) {
      return Status::NotImplemented(
          "PreallocateFixedWidthArrayData: DICTIONARY type allocation: ", *type);
    }
    if (source.child_data[0].MayHaveNulls()) {
      return Status::Invalid(
          "PreallocateFixedWidthArrayData: "
          "FixedSizeList may have null values in child array: ",
          fsl_type);
    }
    auto* child_values = out->child_data[0].get();
    child_values->type = value_type;
    return PreallocateFixedWidthArrayData(ctx, length * fsl_type.list_size(),
                                          /*source=*/source.child_data[0],
                                          /*allocate_validity=*/false,
                                          /*out=*/child_values);
  }
  return Status::Invalid("PreallocateFixedWidthArrayData: Invalid type: ", *type);
}

}  // namespace internal

/// \pre same as OffsetPointerOfFixedWidthValues
/// \pre source.type->id() != Type::BOOL
static const uint8_t* OffsetPointerOfFixedWidthValuesFallback(const ArraySpan& source) {
  using OffsetAndListSize = std::pair<int64_t, int64_t>;
  auto get_offset = [](auto pair) { return pair.first; };
  auto get_list_size = [](auto pair) { return pair.second; };
  ::arrow::internal::SmallVector<OffsetAndListSize, 1> stack;

  DCHECK_NE(source.type->id(), Type::BOOL);

  int64_t list_size = 1;
  auto* array = &source;
  while (array->type->id() == Type::FIXED_SIZE_LIST) {
    list_size *= checked_cast<const FixedSizeListType*>(array->type)->list_size();
    stack.emplace_back(array->offset, list_size);
    array = &array->child_data[0];
  }
  // Now that innermost values were reached, pop the stack and calculate the offset
  // in bytes of the innermost values buffer by considering the offset at each
  // level of nesting.
  DCHECK(array->type->id() != Type::BOOL && is_fixed_width(*array->type));
  DCHECK(array == &source || !array->MayHaveNulls())
      << "OffsetPointerOfFixedWidthValues: array is expected to be flat or have no "
         "nulls in the arrays nested by FIXED_SIZE_LIST.";
  int64_t value_width = array->type->byte_width();
  int64_t offset_in_bytes = array->offset * value_width;
  for (auto it = stack.rbegin(); it != stack.rend(); ++it) {
    value_width *= get_list_size(*it);
    offset_in_bytes += get_offset(*it) * value_width;
  }
  return value_width < 0 ? nullptr : array->GetValues<uint8_t>(1, offset_in_bytes);
}

const uint8_t* OffsetPointerOfFixedWidthValues(const ArraySpan& source) {
  auto type_id = source.type->id();
  if (is_fixed_width(type_id)) {
    if (ARROW_PREDICT_FALSE(type_id == Type::BOOL)) {
      // BOOL arrays are bit-packed, thus a byte-aligned pointer cannot be produced in the
      // general case. Returning something for BOOL arrays that happen to byte-align
      // because offset=0 would create too much confusion.
      return nullptr;
    }
    return source.GetValues<uint8_t>(1, 0) + source.offset * source.type->byte_width();
  }
  return OffsetPointerOfFixedWidthValuesFallback(source);
}

/// \brief Get the mutable pointer to the fixed-width values of an array
///        allocated by PreallocateFixedWidthArrayData.
///
/// \pre mutable_array->offset and the offset of child array (if it's a
///      FixedSizeList) MUST be 0 (recursively).
/// \pre IsFixedWidthLike(ArraySpan(mutable_array)) or the more restrictive
///      is_fixed_width(*mutable_array->type) MUST be true
/// \return The mutable pointer to the fixed-width byte blocks of the array. If
///         pre-conditions are not satisfied, the return values is undefined.
uint8_t* MutableFixedWidthValuesPointer(ArrayData* mutable_array) {
  auto type_id = mutable_array->type->id();
  if (type_id == Type::FIXED_SIZE_LIST) {
    auto* array = mutable_array;
    do {
      DCHECK_EQ(array->offset, 0);
      DCHECK_EQ(array->child_data.size(), 1) << array->type->ToString(true) << " part of "
                                             << mutable_array->type->ToString(true);
      array = array->child_data[0].get();
    } while (array->type->id() == Type::FIXED_SIZE_LIST);
    DCHECK_EQ(array->offset, 0);
    DCHECK(array->type->id() != Type::BOOL && is_fixed_width(*array->type));
    return array->GetMutableValues<uint8_t>(1, 0);
  }
  DCHECK_EQ(mutable_array->offset, 0);
  // BOOL is allowed here only because the offset is expected to be 0,
  // so the byte-aligned pointer also points to the first *bit* of the buffer.
  DCHECK(is_fixed_width(type_id));
  return mutable_array->GetMutableValues<uint8_t>(1, 0);
}

}  // namespace arrow::util

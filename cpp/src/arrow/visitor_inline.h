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

// Private header, not to be exported

#pragma once

#include <utility>

#include "arrow/array.h"
#include "arrow/extension_type.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/functional.h"
#include "arrow/util/string_view.h"

namespace arrow {

#define ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(ACTION) \
  ACTION(Int8);                                      \
  ACTION(UInt8);                                     \
  ACTION(Int16);                                     \
  ACTION(UInt16);                                    \
  ACTION(Int32);                                     \
  ACTION(UInt32);                                    \
  ACTION(Int64);                                     \
  ACTION(UInt64)

#define ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(ACTION) \
  ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(ACTION);      \
  ACTION(HalfFloat);                                 \
  ACTION(Float);                                     \
  ACTION(Double)

#define ARROW_GENERATE_FOR_ALL_TYPES(ACTION)    \
  ACTION(Null);                                 \
  ACTION(Boolean);                              \
  ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(ACTION); \
  ACTION(String);                               \
  ACTION(Binary);                               \
  ACTION(LargeString);                          \
  ACTION(LargeBinary);                          \
  ACTION(FixedSizeBinary);                      \
  ACTION(Duration);                             \
  ACTION(Date32);                               \
  ACTION(Date64);                               \
  ACTION(Timestamp);                            \
  ACTION(Time32);                               \
  ACTION(Time64);                               \
  ACTION(MonthInterval);                        \
  ACTION(DayTimeInterval);                      \
  ACTION(Decimal128);                           \
  ACTION(List);                                 \
  ACTION(LargeList);                            \
  ACTION(Map);                                  \
  ACTION(FixedSizeList);                        \
  ACTION(Struct);                               \
  ACTION(SparseUnion);                          \
  ACTION(DenseUnion);                           \
  ACTION(Dictionary);                           \
  ACTION(Extension)

#define TYPE_VISIT_INLINE(TYPE_CLASS) \
  case TYPE_CLASS##Type::type_id:     \
    return visitor->Visit(internal::checked_cast<const TYPE_CLASS##Type&>(type));

template <typename VISITOR>
inline Status VisitTypeInline(const DataType& type, VISITOR* visitor) {
  switch (type.id()) {
    ARROW_GENERATE_FOR_ALL_TYPES(TYPE_VISIT_INLINE);
    default:
      break;
  }
  return Status::NotImplemented("Type not implemented");
}

#undef TYPE_VISIT_INLINE

#define TYPE_ID_VISIT_INLINE(TYPE_CLASS)            \
  case TYPE_CLASS##Type::type_id: {                 \
    const TYPE_CLASS##Type* concrete_ptr = nullptr; \
    return visitor->Visit(concrete_ptr);            \
  }

// Calls `visitor` with a nullptr of the corresponding concrete type class
template <typename VISITOR>
inline Status VisitTypeIdInline(Type::type id, VISITOR* visitor) {
  switch (id) {
    ARROW_GENERATE_FOR_ALL_TYPES(TYPE_ID_VISIT_INLINE);
    default:
      break;
  }
  return Status::NotImplemented("Type not implemented");
}

#undef TYPE_ID_VISIT_INLINE

#define ARRAY_VISIT_INLINE(TYPE_CLASS)                                                   \
  case TYPE_CLASS##Type::type_id:                                                        \
    return visitor->Visit(                                                               \
        internal::checked_cast<const typename TypeTraits<TYPE_CLASS##Type>::ArrayType&>( \
            array));

template <typename VISITOR>
inline Status VisitArrayInline(const Array& array, VISITOR* visitor) {
  switch (array.type_id()) {
    ARROW_GENERATE_FOR_ALL_TYPES(ARRAY_VISIT_INLINE);
    default:
      break;
  }
  return Status::NotImplemented("Type not implemented");
}

namespace internal {

template <typename T, typename Enable = void>
struct ArrayDataInlineVisitor {};

namespace detail {

template <typename VisitNotNull, typename VisitNull>
Status VisitBitBlocks(const std::shared_ptr<Buffer>& bitmap_buf, int64_t offset,
                      int64_t length, VisitNotNull&& visit_not_null,
                      VisitNull&& visit_null) {
  const uint8_t* bitmap = nullptr;
  if (bitmap_buf != nullptr) {
    bitmap = bitmap_buf->data();
  }
  internal::OptionalBitBlockCounter bit_counter(bitmap, offset, length);
  int64_t position = 0;
  while (position < length) {
    internal::BitBlockCount block = bit_counter.NextBlock();
    if (block.AllSet()) {
      for (int64_t i = 0; i < block.length; ++i, ++position) {
        ARROW_RETURN_NOT_OK(visit_not_null(position));
      }
    } else if (block.NoneSet()) {
      for (int64_t i = 0; i < block.length; ++i, ++position) {
        ARROW_RETURN_NOT_OK(visit_null());
      }
    } else {
      for (int64_t i = 0; i < block.length; ++i, ++position) {
        if (BitUtil::GetBit(bitmap, offset + position)) {
          ARROW_RETURN_NOT_OK(visit_not_null(position));
        } else {
          ARROW_RETURN_NOT_OK(visit_null());
        }
      }
    }
  }
  return Status::OK();
}

template <typename VisitNotNull, typename VisitNull>
void VisitBitBlocksVoid(const std::shared_ptr<Buffer>& bitmap_buf, int64_t offset,
                        int64_t length, VisitNotNull&& visit_not_null,
                        VisitNull&& visit_null) {
  const uint8_t* bitmap = nullptr;
  if (bitmap_buf != nullptr) {
    bitmap = bitmap_buf->data();
  }
  internal::OptionalBitBlockCounter bit_counter(bitmap, offset, length);
  int64_t position = 0;
  while (position < length) {
    internal::BitBlockCount block = bit_counter.NextBlock();
    if (block.AllSet()) {
      for (int64_t i = 0; i < block.length; ++i, ++position) {
        visit_not_null(position);
      }
    } else if (block.NoneSet()) {
      for (int64_t i = 0; i < block.length; ++i, ++position) {
        visit_null();
      }
    } else {
      for (int64_t i = 0; i < block.length; ++i, ++position) {
        if (BitUtil::GetBit(bitmap, offset + position)) {
          visit_not_null(position);
        } else {
          visit_null();
        }
      }
    }
  }
}

}  // namespace detail

// Numeric and primitive C-compatible types
template <typename T>
struct ArrayDataInlineVisitor<T, enable_if_has_c_type<T>> {
  using c_type = typename T::c_type;

  template <typename ValidFunc, typename NullFunc>
  static Status VisitStatus(const ArrayData& arr, ValidFunc&& valid_func,
                            NullFunc&& null_func) {
    const c_type* data = arr.GetValues<c_type>(1);
    auto visit_valid = [&](int64_t i) { return valid_func(data[i]); };
    return detail::VisitBitBlocks(arr.buffers[0], arr.offset, arr.length,
                                  std::move(visit_valid),
                                  std::forward<NullFunc>(null_func));
  }

  template <typename ValidFunc, typename NullFunc>
  static void VisitVoid(const ArrayData& arr, ValidFunc&& valid_func,
                        NullFunc&& null_func) {
    using c_type = typename T::c_type;
    const c_type* data = arr.GetValues<c_type>(1);
    auto visit_valid = [&](int64_t i) { valid_func(data[i]); };
    detail::VisitBitBlocksVoid(arr.buffers[0], arr.offset, arr.length,
                               std::move(visit_valid), std::forward<NullFunc>(null_func));
  }
};

// Boolean
template <>
struct ArrayDataInlineVisitor<BooleanType> {
  using c_type = bool;

  template <typename ValidFunc, typename NullFunc>
  static Status VisitStatus(const ArrayData& arr, ValidFunc&& valid_func,
                            NullFunc&& null_func) {
    int64_t offset = arr.offset;
    const uint8_t* data = arr.buffers[1]->data();
    return detail::VisitBitBlocks(
        arr.buffers[0], offset, arr.length,
        [&](int64_t i) { return valid_func(BitUtil::GetBit(data, offset + i)); },
        std::forward<NullFunc>(null_func));
  }

  template <typename ValidFunc, typename NullFunc>
  static void VisitVoid(const ArrayData& arr, ValidFunc&& valid_func,
                        NullFunc&& null_func) {
    int64_t offset = arr.offset;
    const uint8_t* data = arr.buffers[1]->data();
    detail::VisitBitBlocksVoid(
        arr.buffers[0], offset, arr.length,
        [&](int64_t i) { valid_func(BitUtil::GetBit(data, offset + i)); },
        std::forward<NullFunc>(null_func));
  }
};

// Binary, String...
template <typename T>
struct ArrayDataInlineVisitor<T, enable_if_base_binary<T>> {
  using c_type = util::string_view;

  template <typename ValidFunc, typename NullFunc>
  static Status VisitStatus(const ArrayData& arr, ValidFunc&& valid_func,
                            NullFunc&& null_func) {
    using offset_type = typename T::offset_type;
    constexpr char empty_value = 0;

    const offset_type* offsets = arr.GetValues<offset_type>(1);
    const char* data;
    if (!arr.buffers[2]) {
      data = &empty_value;
    } else {
      // Do not apply the array offset to the values array; the value_offsets
      // index the non-sliced values array.
      data = arr.GetValues<char>(2, /*absolute_offset=*/0);
    }
    offset_type cur_offset = *offsets++;
    return detail::VisitBitBlocks(
        arr.buffers[0], arr.offset, arr.length,
        [&](int64_t i) {
          ARROW_UNUSED(i);
          auto value = util::string_view(data + cur_offset, *offsets - cur_offset);
          cur_offset = *offsets++;
          return valid_func(value);
        },
        [&]() {
          cur_offset = *offsets++;
          return null_func();
        });
  }

  template <typename ValidFunc, typename NullFunc>
  static void VisitVoid(const ArrayData& arr, ValidFunc&& valid_func,
                        NullFunc&& null_func) {
    using offset_type = typename T::offset_type;
    constexpr uint8_t empty_value = 0;

    const offset_type* offsets = arr.GetValues<offset_type>(1);
    const uint8_t* data;
    if (!arr.buffers[2]) {
      data = &empty_value;
    } else {
      // Do not apply the array offset to the values array; the value_offsets
      // index the non-sliced values array.
      data = arr.GetValues<uint8_t>(2, /*absolute_offset=*/0);
    }

    detail::VisitBitBlocksVoid(
        arr.buffers[0], arr.offset, arr.length,
        [&](int64_t i) {
          auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                         offsets[i + 1] - offsets[i]);
          valid_func(value);
        },
        std::forward<NullFunc>(null_func));
  }
};

// FixedSizeBinary, Decimal128
template <typename T>
struct ArrayDataInlineVisitor<T, enable_if_fixed_size_binary<T>> {
  using c_type = util::string_view;

  template <typename ValidFunc, typename NullFunc>
  static Status VisitStatus(const ArrayData& arr, ValidFunc&& valid_func,
                            NullFunc&& null_func) {
    const auto& fw_type = internal::checked_cast<const FixedSizeBinaryType&>(*arr.type);

    const int32_t byte_width = fw_type.byte_width();
    const char* data = arr.GetValues<char>(1,
                                           /*absolute_offset=*/arr.offset * byte_width);

    return detail::VisitBitBlocks(
        arr.buffers[0], arr.offset, arr.length,
        [&](int64_t i) {
          auto value = util::string_view(data, byte_width);
          data += byte_width;
          return valid_func(value);
        },
        [&]() {
          data += byte_width;
          return null_func();
        });
  }

  template <typename ValidFunc, typename NullFunc>
  static void VisitVoid(const ArrayData& arr, ValidFunc&& valid_func,
                        NullFunc&& null_func) {
    const auto& fw_type = internal::checked_cast<const FixedSizeBinaryType&>(*arr.type);

    const int32_t byte_width = fw_type.byte_width();
    const char* data = arr.GetValues<char>(1,
                                           /*absolute_offset=*/arr.offset * byte_width);

    detail::VisitBitBlocksVoid(
        arr.buffers[0], arr.offset, arr.length,
        [&](int64_t i) {
          valid_func(util::string_view(data, byte_width));
          data += byte_width;
        },
        [&]() {
          data += byte_width;
          null_func();
        });
  }
};

}  // namespace internal

// Visit an array's data values, in order, without overhead.
//
// The given `ValidFunc` should be a callable with either of these signatures:
// - void(scalar_type)
// - Status(scalar_type)
//
// The `NullFunc` should have the same return type as `ValidFunc`.
//
// ... where `scalar_type` depends on the array data type:
// - the type's `c_type`, if any
// - for boolean arrays, a `bool`
// - for binary, string and fixed-size binary arrays, a `util::string_view`

template <typename T, typename ValidFunc, typename NullFunc>
typename internal::call_traits::enable_if_return<ValidFunc, Status>::type
VisitArrayDataInline(const ArrayData& arr, ValidFunc&& valid_func, NullFunc&& null_func) {
  return internal::ArrayDataInlineVisitor<T>::VisitStatus(
      arr, std::forward<ValidFunc>(valid_func), std::forward<NullFunc>(null_func));
}

template <typename T, typename ValidFunc, typename NullFunc>
typename internal::call_traits::enable_if_return<ValidFunc, void>::type
VisitArrayDataInline(const ArrayData& arr, ValidFunc&& valid_func, NullFunc&& null_func) {
  return internal::ArrayDataInlineVisitor<T>::VisitVoid(
      arr, std::forward<ValidFunc>(valid_func), std::forward<NullFunc>(null_func));
}

// Visit an array's data values, in order, without overhead.
//
// The Visit method's `visitor` argument should be an object with two public methods:
// - Status VisitNull()
// - Status VisitValue(<scalar>)
//
// The scalar value's type depends on the array data type:
// - the type's `c_type`, if any
// - for boolean arrays, a `bool`
// - for binary, string and fixed-size binary arrays, a `util::string_view`

template <typename T>
struct ArrayDataVisitor {
  using InlineVisitorType = internal::ArrayDataInlineVisitor<T>;
  using c_type = typename InlineVisitorType::c_type;

  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    return InlineVisitorType::VisitStatus(
        arr, [visitor](c_type v) { return visitor->VisitValue(v); },
        [visitor]() { return visitor->VisitNull(); });
  }
};

#define SCALAR_VISIT_INLINE(TYPE_CLASS) \
  case TYPE_CLASS##Type::type_id:       \
    return visitor->Visit(internal::checked_cast<const TYPE_CLASS##Scalar&>(scalar));

template <typename VISITOR>
inline Status VisitScalarInline(const Scalar& scalar, VISITOR* visitor) {
  switch (scalar.type->id()) {
    ARROW_GENERATE_FOR_ALL_TYPES(SCALAR_VISIT_INLINE);
    default:
      break;
  }
  return Status::NotImplemented("Scalar visitor for type not implemented ",
                                scalar.type->ToString());
}

#undef TYPE_VISIT_INLINE

// Visit a null bitmap, in order, without overhead.
//
// The given `ValidFunc` should be a callable with either of these signatures:
// - void()
// - Status()
//
// The `NullFunc` should have the same return type as `ValidFunc`.

template <typename ValidFunc, typename NullFunc>
typename internal::call_traits::enable_if_return<ValidFunc, Status>::type
VisitNullBitmapInline(const uint8_t* valid_bits, int64_t valid_bits_offset,
                      int64_t num_values, int64_t null_count, ValidFunc&& valid_func,
                      NullFunc&& null_func) {
  ARROW_UNUSED(null_count);
  internal::OptionalBitBlockCounter bit_counter(valid_bits, valid_bits_offset,
                                                num_values);
  int64_t position = 0;
  int64_t offset_position = valid_bits_offset;
  while (position < num_values) {
    internal::BitBlockCount block = bit_counter.NextBlock();
    if (block.AllSet()) {
      for (int64_t i = 0; i < block.length; ++i) {
        ARROW_RETURN_NOT_OK(valid_func());
      }
    } else if (block.NoneSet()) {
      for (int64_t i = 0; i < block.length; ++i) {
        ARROW_RETURN_NOT_OK(null_func());
      }
    } else {
      for (int64_t i = 0; i < block.length; ++i) {
        ARROW_RETURN_NOT_OK(BitUtil::GetBit(valid_bits, offset_position + i)
                                ? valid_func()
                                : null_func());
      }
    }
    position += block.length;
    offset_position += block.length;
  }
  return Status::OK();
}

template <typename ValidFunc, typename NullFunc>
typename internal::call_traits::enable_if_return<ValidFunc, void>::type
VisitNullBitmapInline(const uint8_t* valid_bits, int64_t valid_bits_offset,
                      int64_t num_values, int64_t null_count, ValidFunc&& valid_func,
                      NullFunc&& null_func) {
  ARROW_UNUSED(null_count);
  internal::OptionalBitBlockCounter bit_counter(valid_bits, valid_bits_offset,
                                                num_values);
  int64_t position = 0;
  int64_t offset_position = valid_bits_offset;
  while (position < num_values) {
    internal::BitBlockCount block = bit_counter.NextBlock();
    if (block.AllSet()) {
      for (int64_t i = 0; i < block.length; ++i) {
        valid_func();
      }
    } else if (block.NoneSet()) {
      for (int64_t i = 0; i < block.length; ++i) {
        null_func();
      }
    } else {
      for (int64_t i = 0; i < block.length; ++i) {
        BitUtil::GetBit(valid_bits, offset_position + i) ? valid_func() : null_func();
      }
    }
    position += block.length;
    offset_position += block.length;
  }
}

}  // namespace arrow

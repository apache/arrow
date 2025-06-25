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

#include <cstdint>

#include "arrow/array/data.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"

namespace arrow::compute {
// XXX: remove dependency on compute::KernelContext
class KernelContext;
}  // namespace arrow::compute

namespace arrow::util {

/// \brief Checks if the given array has a fixed-width type or if it's an array of
/// fixed-size list that can be flattened to an array of fixed-width values.
///
/// Fixed-width types are the ones defined by the is_fixed_width() predicate in
/// type_traits.h. They are all the types that passes any of the following
/// predicates:
///
///  - is_primitive()
///  - is_fixed_size_binary()
///  - is_dictionary()
///
/// At least 3 types in this set require special care:
///  - `Type::BOOL` is fixed-width, but it's a 1-bit type and pointers to first bit
///    in boolean buffers are not always aligned to byte boundaries.
///  - `Type::DICTIONARY` is fixed-width because the indices are fixed-width, but the
///    dictionary values are not necessarily fixed-width and have to be managed
///    by separate operations.
///  - Type::FIXED_SIZE_BINARY unlike other fixed-width types, fixed-size binary
///    values are defined by a size attribute that is not known at compile time.
///    The other types have power-of-2 byte widths, while fixed-size binary can
///    have any byte width including 0.
///
/// Additionally, we say that a type is "fixed-width like" if it's a fixed-width as
/// defined above, or if it's a fixed-size list (or nested fixed-size lists) and
/// the innermost type is fixed-width and the following restrictions also apply:
///  - Only the top-level array may have nulls, all the inner array have to be completely
///    free of nulls so we don't need to manage internal validity bitmaps.
///
/// \param source The array to check
/// \param force_null_count If true, GetNullCount() is used instead of null_count
/// \param exclude_bool_and_dictionary If true, BOOL and DICTIONARY are excluded from
///                                    the is_fixed_width() types. Default: false.
ARROW_EXPORT bool IsFixedWidthLike(const ArraySpan& source, bool force_null_count = false,
                                   bool exclude_bool_and_dictionary = false);

// Take the following `fixed_size_list<fixed_size_list<int32, 2>, 3>` array as an
// example:
//
//     [
//       [[1, 2], [3,  4], [ 5,  6]],
//       null,
//       [[7, 8], [9, 10], [11, 12]]
//     ]
//
// in memory, it would look like:
//
//     {
//        type: fixed_size_list<fixed_size_list<int32, 2>, 3>,
//        length: 3,
//        null_count: 1,
//        offset: 0,
//        buffers: [
//          0: [0b00000101]
//        ],
//        child_data: [
//          0: {
//            type: fixed_size_list<int32, 2>,
//            length: 9,
//            null_count: 0,
//            offset: 0,
//            buffers: [0: NULL],
//            child_data: [
//              0: {
//                type: int32,
//                length: 18,
//                null_count: 0,
//                offset: 0,
//                buffers: [
//                  0: NULL,
//                  1: [ 1,  2,  3,  4,  5,  6,
//                       0,  0,  0,  0,  0,  0
//                       7,  8,  9, 10, 11, 12 ]
//                ],
//                child_data: []
//              }
//            ]
//          }
//        ]
//     }
//
// This layout fits the fixed-width like definition because the innermost type
// is byte-aligned fixed-width (int32 = 4 bytes) and the internal arrays don't
// have nulls. The validity bitmap is only needed at the top-level array.
//
// Writing to this array can be done in the same way writing to a flat fixed-width
// array is done, by:
// 1. Updating the validity bitmap at the top-level array if nulls are present.
// 2. Updating a continuous fixed-width block of memory through a single pointer.
//
// The length of this block of memory is the product of the list sizes in the
// `FixedSizeList` types and the byte width of the innermost fixed-width type:
//
//     3 * 2 * 4 = 24 bytes
//
// Writing the `[[1, 2], [3, 4], [5, 6]]` value at a given index can be done by
// simply setting the validity bit to 1 and writing the 24-byte sequence of
// integers `[1, 2, 3, 4, 5, 6]` to the memory block at `byte_ptr + index * 24`.
//
// The length of the top-level array fully defines the lengths that all the nested
// arrays must have, which makes defining all the lengths as easy as defining the
// length of the top-level array.
//
//     length = 3
//     child_data[0].length == 3 * 3 == 9
//     child_data[0].child_data[0].length == 3 * 3 * 2 == 18
//
//     child_data[0].child_data[0].buffers[1].size() >=
//       (3 * (3 * 2 * sizeof(int32)) == 3 * 24 == 72)
//
// Dealing with offsets is a bit involved. Let's say the array described above has
// the offsets 2, 5, and 7:
//
//     {
//        type: fixed_size_list<fixed_size_list<int32, 2>, 3>,
//        offset: 2,
//        ...
//        child_data: [
//          0: {
//            type: fixed_size_list<int32, 2>,
//            offset: 5,
//            ...
//            child_data: [
//              0: {
//                type: int32,
//                offset: 7,
//                buffers: [
//                  0: NULL,
//                  1: [ 1, 1, 1, 1, 1, 1, 1,      // 7 values skipped
//                       0,1, 0,1, 0,1, 0,1, 0,1,  // 5 [x,x] values skipped
//
//                       0,0,0,0,0,1,  //
//                       0,0,0,0,0,1,  // 2 [[x,x], [x,x], [x,x]] values skipped
//
//                       1,  2,  3,  4,  5,  6,  //
//                       0,  0,  0,  0,  0,  0   // the actual values
//                       7,  8,  9, 10, 11, 12   //
//                     ]
//                ],
//              }
//            ]
//          }
//        ]
//     }
//
// The offset of the innermost values buffer, in bytes, is calculated as:
//
//     ((2 * 3) + (5 * 2) + 7) * sizeof(int32) = 29 * 4 bytes = 116 bytes
//
// In general, the formula to calculate the offset of the innermost values buffer is:
//
//     ((off_0 * fsl_size_0) + (off_1 * fsl_size_1) + ... + innermost_off)
//        * sizeof(innermost_type)
//
// `OffsetPointerOfFixedByteWidthValues()` can calculate this byte offset and return
// the pointer to the first relevant byte of the innermost values buffer.

/// \brief Checks if the given array has a fixed-width type or if it's an array of
/// fixed-size list that can be flattened to an array of fixed-width values.
///
/// \param source The array to check
/// \param force_null_count If true, GetNullCount() is used instead of null_count
/// \param extra_predicate A DataType predicate that can be used to further
///                        restrict the types that are considered fixed-width
template <class ExtraPred>
inline bool IsFixedWidthLike(const ArraySpan& source, bool force_null_count,
                             ExtraPred extra_predicate) {
  const auto* type = source.type;
  // BOOL is considered fixed-width if not nested under FIXED_SIZE_LIST.
  if (is_fixed_width(type->id()) && extra_predicate(*type)) {
    return true;
  }
  if (type->id() == Type::FIXED_SIZE_LIST) {
    // All the inner arrays must not contain any nulls.
    const auto* values = &source.child_data[0];
    while ((force_null_count ? values->GetNullCount() : values->null_count) == 0) {
      type = values->type;
      if (type->id() == Type::FIXED_SIZE_LIST) {
        values = &values->child_data[0];
        continue;
      }
      return is_fixed_width(type->id()) && extra_predicate(*type);
    }
  }
  return false;
}

/// \brief Get the fixed-width in bytes of a type if it is a fixed-width like
/// type, but not BOOL.
///
/// If the array is a FixedSizeList (of any level of nesting), the byte width of
/// the values is the product of all fixed-list sizes and the byte width of the
/// innermost fixed-width value type.
///
/// IsFixedWidthLike(array) performs more checks than this function and should
/// be used to guarantee that, if type is not BOOL, this function will not return -1.
///
/// NOTE: this function translates `DataType::bit_width()` to bytes differently from
/// `DataType::byte_width()`. `DataType::byte_width()` will return 0 for
/// BOOL, while this function will return `-1`. This is done because 0 is
/// a valid return value for FIXED_SIZE_LIST with size 0 or `FIXED_SIZE_BINARY` with
/// size 0.
///
/// \pre The instance of the array where this type is from must pass
///      `IsFixedWidthLike(array)` and should not be BOOL.
/// \return The fixed-byte width of the values or -1 if the type is BOOL or not
///         fixed-width like. 0 is a valid return value as fixed-size-lists
///         and fixed-size-binary with size 0 are allowed.
ARROW_EXPORT int64_t FixedWidthInBytes(const DataType& type);

/// \brief Get the fixed-width in bits of a type if it is a fixed-width like
/// type.
///
/// If the array is a FixedSizeList (of any level of nesting), the bit width of
/// the values is the product of all fixed-list sizes and the bit width of the
/// innermost fixed-width value type.
///
/// \return The bit-width of the values or -1
/// \see FixedWidthInBytes
ARROW_EXPORT int64_t FixedWidthInBits(const DataType& type);

namespace internal {

/// \brief Allocate an ArrayData for a type that is fixed-width like.
///
/// This function performs the same checks performed by
/// `IsFixedWidthLike(source, false, false)`. If `source.type` is not a simple
/// fixed-width type, caller should make sure it passes the
/// `IsFixedWidthLike(source)` checks. That guarantees that it's possible to
/// allocate an array that can serve as a destination for a kernel that writes values
/// through a single pointer to fixed-width byte blocks.
///
/// \param[in] length The length of the array to allocate (unrelated to the length of
///                   the source array)
/// \param[in] source The source array that carries the type information and the
///                   validity bitmaps that are relevant for the type validation
///                   when the source is a FixedSizeList.
/// \see IsFixedWidthLike
ARROW_EXPORT Status PreallocateFixedWidthArrayData(::arrow::compute::KernelContext* ctx,
                                                   int64_t length,
                                                   const ArraySpan& source,
                                                   bool allocate_validity,
                                                   ArrayData* out);

}  // namespace internal

/// \brief Get the 0-7 residual offset in bits and the pointer to the fixed-width
/// values of a fixed-width like array.
///
/// For byte-aligned types, the offset is always 0.
///
/// \pre `IsFixedWidthLike(source)` or the more restrictive
///      is_fixed_width(*mutable_array->type) SHOULD be true
/// \return A pair with the residual offset in bits (0-7) and the pointer
///         to the fixed-width values.
ARROW_EXPORT std::pair<int, const uint8_t*> OffsetPointerOfFixedBitWidthValues(
    const ArraySpan& source);

/// \brief Get the pointer to the fixed-width values of a fixed-width like array.
///
/// \pre `IsFixedWidthLike(source)` should be true and BOOL should be excluded
///      as each bool is 1-bit width making it impossible to produce a
///      byte-aligned pointer to the values in the general case.
ARROW_EXPORT const uint8_t* OffsetPointerOfFixedByteWidthValues(const ArraySpan& source);

/// \brief Get the mutable pointer to the fixed-width values of an array
///        allocated by PreallocateFixedWidthArrayData.
///
/// \pre mutable_array->offset and the offset of child array (if it's a
///      FixedSizeList) MUST be 0 (recursively).
/// \pre IsFixedWidthLike(ArraySpan(mutable_array)) or the more restrictive
///      is_fixed_width(*mutable_array->type) MUST be true
/// \return The mutable pointer to the fixed-width byte blocks of the array. If
///         pre-conditions are not satisfied, the return values is undefined.
ARROW_EXPORT uint8_t* MutableFixedWidthValuesPointer(ArrayData* mutable_array);

}  // namespace arrow::util

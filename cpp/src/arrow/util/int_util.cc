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

#include "arrow/util/int_util.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "arrow/array/data.h"
#include "arrow/datum.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/string.h"
#include "arrow/util/ubsan.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace internal {

using internal::checked_cast;

static constexpr uint64_t max_uint8 =
    static_cast<uint64_t>(std::numeric_limits<uint8_t>::max());
static constexpr uint64_t max_uint16 =
    static_cast<uint64_t>(std::numeric_limits<uint16_t>::max());
static constexpr uint64_t max_uint32 =
    static_cast<uint64_t>(std::numeric_limits<uint32_t>::max());
static constexpr uint64_t max_uint64 = std::numeric_limits<uint64_t>::max();

static constexpr uint64_t mask_uint8 = ~0xffULL;
static constexpr uint64_t mask_uint16 = ~0xffffULL;
static constexpr uint64_t mask_uint32 = ~0xffffffffULL;

//
// Unsigned integer width detection
//

static const uint64_t max_uints[] = {0, max_uint8, max_uint16, 0,         max_uint32,
                                     0, 0,         0,          max_uint64};

// Check if we would need to expand the underlying storage type
static inline uint8_t ExpandedUIntWidth(uint64_t val, uint8_t current_width) {
  // Optimize for the common case where width doesn't change
  if (ARROW_PREDICT_TRUE(val <= max_uints[current_width])) {
    return current_width;
  }
  if (current_width == 1 && val <= max_uint8) {
    return 1;
  } else if (current_width <= 2 && val <= max_uint16) {
    return 2;
  } else if (current_width <= 4 && val <= max_uint32) {
    return 4;
  } else {
    return 8;
  }
}

uint8_t DetectUIntWidth(const uint64_t* values, int64_t length, uint8_t min_width) {
  uint8_t width = min_width;
  if (min_width < 8) {
    auto p = values;
    const auto end = p + length;
    while (p <= end - 16) {
      // This is probably SIMD-izable
      auto u = p[0];
      auto v = p[1];
      auto w = p[2];
      auto x = p[3];
      u |= p[4];
      v |= p[5];
      w |= p[6];
      x |= p[7];
      u |= p[8];
      v |= p[9];
      w |= p[10];
      x |= p[11];
      u |= p[12];
      v |= p[13];
      w |= p[14];
      x |= p[15];
      p += 16;
      width = ExpandedUIntWidth(u | v | w | x, width);
      if (ARROW_PREDICT_FALSE(width == 8)) {
        break;
      }
    }
    if (p <= end - 8) {
      auto u = p[0];
      auto v = p[1];
      auto w = p[2];
      auto x = p[3];
      u |= p[4];
      v |= p[5];
      w |= p[6];
      x |= p[7];
      p += 8;
      width = ExpandedUIntWidth(u | v | w | x, width);
    }
    while (p < end) {
      width = ExpandedUIntWidth(*p++, width);
    }
  }
  return width;
}

uint8_t DetectUIntWidth(const uint64_t* values, const uint8_t* valid_bytes,
                        int64_t length, uint8_t min_width) {
  if (valid_bytes == nullptr) {
    return DetectUIntWidth(values, length, min_width);
  }
  uint8_t width = min_width;
  if (min_width < 8) {
    auto p = values;
    const auto end = p + length;
    auto b = valid_bytes;

#define MASK(p, b, i) p[i] * (b[i] != 0)

    while (p <= end - 8) {
      // This is probably be SIMD-izable
      auto u = MASK(p, b, 0);
      auto v = MASK(p, b, 1);
      auto w = MASK(p, b, 2);
      auto x = MASK(p, b, 3);
      u |= MASK(p, b, 4);
      v |= MASK(p, b, 5);
      w |= MASK(p, b, 6);
      x |= MASK(p, b, 7);
      b += 8;
      p += 8;
      width = ExpandedUIntWidth(u | v | w | x, width);
      if (ARROW_PREDICT_FALSE(width == 8)) {
        break;
      }
    }
    uint64_t mask = 0;
    while (p < end) {
      mask |= MASK(p, b, 0);
      ++b;
      ++p;
    }
    width = ExpandedUIntWidth(mask, width);

#undef MASK
  }
  return width;
}

//
// Signed integer width detection
//

uint8_t DetectIntWidth(const int64_t* values, int64_t length, uint8_t min_width) {
  if (min_width == 8) {
    return min_width;
  }
  uint8_t width = min_width;

  auto p = values;
  const auto end = p + length;
  // Strategy: to determine whether `x` is between -0x80 and 0x7f,
  // we determine whether `x + 0x80` is between 0x00 and 0xff.  The
  // latter can be done with a simple AND mask with ~0xff and, more
  // importantly, can be computed in a single step over multiple ORed
  // values (so we can branch once every N items instead of once every item).
  // This strategy could probably lend itself to explicit SIMD-ization,
  // if more performance is needed.
  constexpr uint64_t addend8 = 0x80ULL;
  constexpr uint64_t addend16 = 0x8000ULL;
  constexpr uint64_t addend32 = 0x80000000ULL;

  auto test_one_item = [&](uint64_t addend, uint64_t test_mask) -> bool {
    auto v = *p++;
    if (ARROW_PREDICT_FALSE(((v + addend) & test_mask) != 0)) {
      --p;
      return false;
    } else {
      return true;
    }
  };

  auto test_four_items = [&](uint64_t addend, uint64_t test_mask) -> bool {
    auto mask = (p[0] + addend) | (p[1] + addend) | (p[2] + addend) | (p[3] + addend);
    p += 4;
    if (ARROW_PREDICT_FALSE((mask & test_mask) != 0)) {
      p -= 4;
      return false;
    } else {
      return true;
    }
  };

  if (width == 1) {
    while (p <= end - 4) {
      if (!test_four_items(addend8, mask_uint8)) {
        width = 2;
        goto width2;
      }
    }
    while (p < end) {
      if (!test_one_item(addend8, mask_uint8)) {
        width = 2;
        goto width2;
      }
    }
    return 1;
  }
width2:
  if (width == 2) {
    while (p <= end - 4) {
      if (!test_four_items(addend16, mask_uint16)) {
        width = 4;
        goto width4;
      }
    }
    while (p < end) {
      if (!test_one_item(addend16, mask_uint16)) {
        width = 4;
        goto width4;
      }
    }
    return 2;
  }
width4:
  if (width == 4) {
    while (p <= end - 4) {
      if (!test_four_items(addend32, mask_uint32)) {
        width = 8;
        goto width8;
      }
    }
    while (p < end) {
      if (!test_one_item(addend32, mask_uint32)) {
        width = 8;
        goto width8;
      }
    }
    return 4;
  }
width8:
  return 8;
}

uint8_t DetectIntWidth(const int64_t* values, const uint8_t* valid_bytes, int64_t length,
                       uint8_t min_width) {
  if (valid_bytes == nullptr) {
    return DetectIntWidth(values, length, min_width);
  }

  if (min_width == 8) {
    return min_width;
  }
  uint8_t width = min_width;

  auto p = values;
  const auto end = p + length;
  auto b = valid_bytes;
  // Strategy is similar to the no-nulls case above, but we also
  // have to zero any incoming items that have a zero validity byte.
  constexpr uint64_t addend8 = 0x80ULL;
  constexpr uint64_t addend16 = 0x8000ULL;
  constexpr uint64_t addend32 = 0x80000000ULL;

#define MASK(p, b, addend, i) (p[i] + addend) * (b[i] != 0)

  auto test_one_item = [&](uint64_t addend, uint64_t test_mask) -> bool {
    auto v = MASK(p, b, addend, 0);
    ++b;
    ++p;
    if (ARROW_PREDICT_FALSE((v & test_mask) != 0)) {
      --b;
      --p;
      return false;
    } else {
      return true;
    }
  };

  auto test_eight_items = [&](uint64_t addend, uint64_t test_mask) -> bool {
    auto mask1 = MASK(p, b, addend, 0) | MASK(p, b, addend, 1) | MASK(p, b, addend, 2) |
                 MASK(p, b, addend, 3);
    auto mask2 = MASK(p, b, addend, 4) | MASK(p, b, addend, 5) | MASK(p, b, addend, 6) |
                 MASK(p, b, addend, 7);
    b += 8;
    p += 8;
    if (ARROW_PREDICT_FALSE(((mask1 | mask2) & test_mask) != 0)) {
      b -= 8;
      p -= 8;
      return false;
    } else {
      return true;
    }
  };

#undef MASK

  if (width == 1) {
    while (p <= end - 8) {
      if (!test_eight_items(addend8, mask_uint8)) {
        width = 2;
        goto width2;
      }
    }
    while (p < end) {
      if (!test_one_item(addend8, mask_uint8)) {
        width = 2;
        goto width2;
      }
    }
    return 1;
  }
width2:
  if (width == 2) {
    while (p <= end - 8) {
      if (!test_eight_items(addend16, mask_uint16)) {
        width = 4;
        goto width4;
      }
    }
    while (p < end) {
      if (!test_one_item(addend16, mask_uint16)) {
        width = 4;
        goto width4;
      }
    }
    return 2;
  }
width4:
  if (width == 4) {
    while (p <= end - 8) {
      if (!test_eight_items(addend32, mask_uint32)) {
        width = 8;
        goto width8;
      }
    }
    while (p < end) {
      if (!test_one_item(addend32, mask_uint32)) {
        width = 8;
        goto width8;
      }
    }
    return 4;
  }
width8:
  return 8;
}

template <typename Source, typename Dest>
static inline void CastIntsInternal(const Source* src, Dest* dest, int64_t length) {
  while (length >= 4) {
    dest[0] = static_cast<Dest>(src[0]);
    dest[1] = static_cast<Dest>(src[1]);
    dest[2] = static_cast<Dest>(src[2]);
    dest[3] = static_cast<Dest>(src[3]);
    length -= 4;
    src += 4;
    dest += 4;
  }
  while (length > 0) {
    *dest++ = static_cast<Dest>(*src++);
    --length;
  }
}

void DowncastInts(const int64_t* source, int8_t* dest, int64_t length) {
  CastIntsInternal(source, dest, length);
}

void DowncastInts(const int64_t* source, int16_t* dest, int64_t length) {
  CastIntsInternal(source, dest, length);
}

void DowncastInts(const int64_t* source, int32_t* dest, int64_t length) {
  CastIntsInternal(source, dest, length);
}

void DowncastInts(const int64_t* source, int64_t* dest, int64_t length) {
  memcpy(dest, source, length * sizeof(int64_t));
}

void DowncastUInts(const uint64_t* source, uint8_t* dest, int64_t length) {
  CastIntsInternal(source, dest, length);
}

void DowncastUInts(const uint64_t* source, uint16_t* dest, int64_t length) {
  CastIntsInternal(source, dest, length);
}

void DowncastUInts(const uint64_t* source, uint32_t* dest, int64_t length) {
  CastIntsInternal(source, dest, length);
}

void DowncastUInts(const uint64_t* source, uint64_t* dest, int64_t length) {
  memcpy(dest, source, length * sizeof(int64_t));
}

void UpcastInts(const int32_t* source, int64_t* dest, int64_t length) {
  CastIntsInternal(source, dest, length);
}

template <typename InputInt, typename OutputInt>
void TransposeInts(const InputInt* src, OutputInt* dest, int64_t length,
                   const int32_t* transpose_map) {
  while (length >= 4) {
    dest[0] = static_cast<OutputInt>(transpose_map[src[0]]);
    dest[1] = static_cast<OutputInt>(transpose_map[src[1]]);
    dest[2] = static_cast<OutputInt>(transpose_map[src[2]]);
    dest[3] = static_cast<OutputInt>(transpose_map[src[3]]);
    length -= 4;
    src += 4;
    dest += 4;
  }
  while (length > 0) {
    *dest++ = static_cast<OutputInt>(transpose_map[*src++]);
    --length;
  }
}

#define INSTANTIATE(SRC, DEST)                       \
  template ARROW_TEMPLATE_EXPORT void TransposeInts( \
      const SRC* source, DEST* dest, int64_t length, const int32_t* transpose_map);

#define INSTANTIATE_ALL_DEST(DEST) \
  INSTANTIATE(uint8_t, DEST)       \
  INSTANTIATE(int8_t, DEST)        \
  INSTANTIATE(uint16_t, DEST)      \
  INSTANTIATE(int16_t, DEST)       \
  INSTANTIATE(uint32_t, DEST)      \
  INSTANTIATE(int32_t, DEST)       \
  INSTANTIATE(uint64_t, DEST)      \
  INSTANTIATE(int64_t, DEST)

#define INSTANTIATE_ALL()        \
  INSTANTIATE_ALL_DEST(uint8_t)  \
  INSTANTIATE_ALL_DEST(int8_t)   \
  INSTANTIATE_ALL_DEST(uint16_t) \
  INSTANTIATE_ALL_DEST(int16_t)  \
  INSTANTIATE_ALL_DEST(uint32_t) \
  INSTANTIATE_ALL_DEST(int32_t)  \
  INSTANTIATE_ALL_DEST(uint64_t) \
  INSTANTIATE_ALL_DEST(int64_t)

INSTANTIATE_ALL()

#undef INSTANTIATE
#undef INSTANTIATE_ALL
#undef INSTANTIATE_ALL_DEST

namespace {

template <typename SrcType>
struct TransposeIntsDest {
  const SrcType* src;
  uint8_t* dest;
  int64_t dest_offset;
  int64_t length;
  const int32_t* transpose_map;

  template <typename T>
  enable_if_integer<T, Status> Visit(const T&) {
    using DestType = typename T::c_type;
    TransposeInts(src, reinterpret_cast<DestType*>(dest) + dest_offset, length,
                  transpose_map);
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::TypeError("TransposeInts received non-integer dest_type");
  }

  Status operator()(const DataType& type) { return VisitTypeInline(type, this); }
};

struct TransposeIntsSrc {
  const uint8_t* src;
  uint8_t* dest;
  int64_t src_offset;
  int64_t dest_offset;
  int64_t length;
  const int32_t* transpose_map;
  const DataType& dest_type;

  template <typename T>
  enable_if_integer<T, Status> Visit(const T&) {
    using SrcType = typename T::c_type;
    return TransposeIntsDest<SrcType>{reinterpret_cast<const SrcType*>(src) + src_offset,
                                      dest, dest_offset, length,
                                      transpose_map}(dest_type);
  }

  Status Visit(const DataType& type) {
    return Status::TypeError("TransposeInts received non-integer dest_type");
  }

  Status operator()(const DataType& type) { return VisitTypeInline(type, this); }
};

};  // namespace

Status TransposeInts(const DataType& src_type, const DataType& dest_type,
                     const uint8_t* src, uint8_t* dest, int64_t src_offset,
                     int64_t dest_offset, int64_t length, const int32_t* transpose_map) {
  TransposeIntsSrc transposer{src,    dest,          src_offset, dest_offset,
                              length, transpose_map, dest_type};
  return transposer(src_type);
}

template <typename IndexCType, bool IsSigned = std::is_signed<IndexCType>::value>
static Status CheckIndexBoundsImpl(const ArraySpan& values, uint64_t upper_limit) {
  // For unsigned integers, if the values array is larger than the maximum
  // index value (e.g. especially for UINT8 / UINT16), then there is no need to
  // boundscheck.
  if (!IsSigned &&
      upper_limit > static_cast<uint64_t>(std::numeric_limits<IndexCType>::max())) {
    return Status::OK();
  }

  const IndexCType* values_data = values.GetValues<IndexCType>(1);
  const uint8_t* bitmap = values.buffers[0].data;
  auto IsOutOfBounds = [&](IndexCType val) -> bool {
    return ((IsSigned && val < 0) ||
            (val >= 0 && static_cast<uint64_t>(val) >= upper_limit));
  };
  return VisitSetBitRuns(
      bitmap, values.offset, values.length, [&](int64_t offset, int64_t length) {
        bool block_out_of_bounds = false;
        for (int64_t i = 0; i < length; ++i) {
          block_out_of_bounds |= IsOutOfBounds(values_data[offset + i]);
        }
        if (ARROW_PREDICT_FALSE(block_out_of_bounds)) {
          for (int64_t i = 0; i < length; ++i) {
            if (IsOutOfBounds(values_data[offset + i])) {
              return Status::IndexError("Index ", ToChars(values_data[offset + i]),
                                        " out of bounds");
            }
          }
        }
        return Status::OK();
      });
}

/// \brief Branchless boundschecking of the values. Processes batches of
/// values at a time and shortcircuits when encountering an out-of-bounds
/// index in a batch
Status CheckIndexBounds(const ArraySpan& values, uint64_t upper_limit) {
  switch (values.type->id()) {
    case Type::INT8:
      return CheckIndexBoundsImpl<int8_t>(values, upper_limit);
    case Type::INT16:
      return CheckIndexBoundsImpl<int16_t>(values, upper_limit);
    case Type::INT32:
      return CheckIndexBoundsImpl<int32_t>(values, upper_limit);
    case Type::INT64:
      return CheckIndexBoundsImpl<int64_t>(values, upper_limit);
    case Type::UINT8:
      return CheckIndexBoundsImpl<uint8_t>(values, upper_limit);
    case Type::UINT16:
      return CheckIndexBoundsImpl<uint16_t>(values, upper_limit);
    case Type::UINT32:
      return CheckIndexBoundsImpl<uint32_t>(values, upper_limit);
    case Type::UINT64:
      return CheckIndexBoundsImpl<uint64_t>(values, upper_limit);
    default:
      return Status::Invalid("Invalid index type for boundschecking");
  }
}

// ----------------------------------------------------------------------
// Utilities for casting from one integer type to another

namespace {

template <typename InType, typename CType = typename InType::c_type>
Status IntegersInRange(const ArraySpan& values, CType bound_lower, CType bound_upper) {
  if (std::numeric_limits<CType>::lowest() >= bound_lower &&
      std::numeric_limits<CType>::max() <= bound_upper) {
    return Status::OK();
  }

  auto IsOutOfBounds = [&](CType val) -> bool {
    return val < bound_lower || val > bound_upper;
  };
  auto IsOutOfBoundsMaybeNull = [&](CType val, bool is_valid) -> bool {
    return is_valid && (val < bound_lower || val > bound_upper);
  };
  auto GetErrorMessage = [&](CType val) {
    return Status::Invalid("Integer value ", ToChars(val),
                           " not in range: ", ToChars(bound_lower), " to ",
                           ToChars(bound_upper));
  };

  const CType* values_data = values.GetValues<CType>(1);
  const uint8_t* bitmap = values.buffers[0].data;
  OptionalBitBlockCounter values_bit_counter(bitmap, values.offset, values.length);
  int64_t position = 0;
  int64_t offset_position = values.offset;
  while (position < values.length) {
    BitBlockCount block = values_bit_counter.NextBlock();
    bool block_out_of_bounds = false;
    if (block.popcount == block.length) {
      // Fast path: branchless
      int64_t i = 0;
      for (int64_t chunk = 0; chunk < block.length / 8; ++chunk) {
        // Let the compiler unroll this
        for (int j = 0; j < 8; ++j) {
          block_out_of_bounds |= IsOutOfBounds(values_data[i++]);
        }
      }
      for (; i < block.length; ++i) {
        block_out_of_bounds |= IsOutOfBounds(values_data[i]);
      }
    } else if (block.popcount > 0) {
      // Values have nulls, must only boundscheck non-null values
      int64_t i = 0;
      for (int64_t chunk = 0; chunk < block.length / 8; ++chunk) {
        // Let the compiler unroll this
        for (int j = 0; j < 8; ++j) {
          block_out_of_bounds |= IsOutOfBoundsMaybeNull(
              values_data[i], bit_util::GetBit(bitmap, offset_position + i));
          ++i;
        }
      }
      for (; i < block.length; ++i) {
        block_out_of_bounds |= IsOutOfBoundsMaybeNull(
            values_data[i], bit_util::GetBit(bitmap, offset_position + i));
      }
    }
    if (ARROW_PREDICT_FALSE(block_out_of_bounds)) {
      if (values.GetNullCount() > 0) {
        for (int64_t i = 0; i < block.length; ++i) {
          if (IsOutOfBoundsMaybeNull(values_data[i],
                                     bit_util::GetBit(bitmap, offset_position + i))) {
            return GetErrorMessage(values_data[i]);
          }
        }
      } else {
        for (int64_t i = 0; i < block.length; ++i) {
          if (IsOutOfBounds(values_data[i])) {
            return GetErrorMessage(values_data[i]);
          }
        }
      }
    }
    values_data += block.length;
    position += block.length;
    offset_position += block.length;
  }
  return Status::OK();
}

template <typename Type>
Status CheckIntegersInRangeImpl(const ArraySpan& values, const Scalar& bound_lower,
                                const Scalar& bound_upper) {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  return IntegersInRange<Type>(values, checked_cast<const ScalarType&>(bound_lower).value,
                               checked_cast<const ScalarType&>(bound_upper).value);
}

}  // namespace

Status CheckIntegersInRange(const ArraySpan& values, const Scalar& bound_lower,
                            const Scalar& bound_upper) {
  Type::type type_id = values.type->id();
  if (bound_lower.type->id() != type_id || bound_upper.type->id() != type_id ||
      !bound_lower.is_valid || !bound_upper.is_valid) {
    return Status::Invalid("Scalar bound types must be non-null and same type as data");
  }

  switch (type_id) {
    case Type::INT8:
      return CheckIntegersInRangeImpl<Int8Type>(values, bound_lower, bound_upper);
    case Type::INT16:
      return CheckIntegersInRangeImpl<Int16Type>(values, bound_lower, bound_upper);
    case Type::INT32:
      return CheckIntegersInRangeImpl<Int32Type>(values, bound_lower, bound_upper);
    case Type::INT64:
      return CheckIntegersInRangeImpl<Int64Type>(values, bound_lower, bound_upper);
    case Type::UINT8:
      return CheckIntegersInRangeImpl<UInt8Type>(values, bound_lower, bound_upper);
    case Type::UINT16:
      return CheckIntegersInRangeImpl<UInt16Type>(values, bound_lower, bound_upper);
    case Type::UINT32:
      return CheckIntegersInRangeImpl<UInt32Type>(values, bound_lower, bound_upper);
    case Type::UINT64:
      return CheckIntegersInRangeImpl<UInt64Type>(values, bound_lower, bound_upper);
    default:
      return Status::TypeError("Invalid index type for boundschecking");
  }
}

namespace {

template <typename O, typename I, typename Enable = void>
struct is_number_downcast {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_number_downcast<
    O, I, enable_if_t<is_number_type<O>::value && is_number_type<I>::value>> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      ((!std::is_same<O, I>::value) &&
       // Both types are of the same sign-ness.
       ((std::is_signed<O_T>::value == std::is_signed<I_T>::value) &&
        // Both types are of the same integral-ness.
        (std::is_floating_point<O_T>::value == std::is_floating_point<I_T>::value)) &&
       // Smaller output size
       (sizeof(O_T) < sizeof(I_T)));
};

template <typename O, typename I, typename Enable = void>
struct is_number_upcast {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_number_upcast<
    O, I, enable_if_t<is_number_type<O>::value && is_number_type<I>::value>> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      ((!std::is_same<O, I>::value) &&
       // Both types are of the same sign-ness.
       ((std::is_signed<O_T>::value == std::is_signed<I_T>::value) &&
        // Both types are of the same integral-ness.
        (std::is_floating_point<O_T>::value == std::is_floating_point<I_T>::value)) &&
       // Larger output size
       (sizeof(O_T) > sizeof(I_T)));
};

template <typename O, typename I, typename Enable = void>
struct is_integral_signed_to_unsigned {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_integral_signed_to_unsigned<
    O, I, enable_if_t<is_integer_type<O>::value && is_integer_type<I>::value>> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      ((!std::is_same<O, I>::value) &&
       ((std::is_unsigned<O_T>::value && std::is_signed<I_T>::value)));
};

template <typename O, typename I, typename Enable = void>
struct is_integral_unsigned_to_signed {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_integral_unsigned_to_signed<
    O, I, enable_if_t<is_integer_type<O>::value && is_integer_type<I>::value>> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      ((!std::is_same<O, I>::value) &&
       ((std::is_signed<O_T>::value && std::is_unsigned<I_T>::value)));
};

// This set of functions SafeMinimum/SafeMaximum would be simplified with
// C++17 and `if constexpr`.

// clang-format doesn't handle this construct properly. Thus the macro, but it
// also improves readability.
//
// The effective return type of the function is always `I::c_type`, this is
// just how enable_if works with functions.
#define RET_TYPE(TRAIT) enable_if_t<TRAIT<O, I>::value, typename I::c_type>

template <typename O, typename I>
constexpr RET_TYPE(std::is_same) SafeMinimum() {
  using out_type = typename O::c_type;

  return std::numeric_limits<out_type>::lowest();
}

template <typename O, typename I>
constexpr RET_TYPE(std::is_same) SafeMaximum() {
  using out_type = typename O::c_type;

  return std::numeric_limits<out_type>::max();
}

template <typename O, typename I>
constexpr RET_TYPE(is_number_downcast) SafeMinimum() {
  using out_type = typename O::c_type;

  return std::numeric_limits<out_type>::lowest();
}

template <typename O, typename I>
constexpr RET_TYPE(is_number_downcast) SafeMaximum() {
  using out_type = typename O::c_type;

  return std::numeric_limits<out_type>::max();
}

template <typename O, typename I>
constexpr RET_TYPE(is_number_upcast) SafeMinimum() {
  using in_type = typename I::c_type;
  return std::numeric_limits<in_type>::lowest();
}

template <typename O, typename I>
constexpr RET_TYPE(is_number_upcast) SafeMaximum() {
  using in_type = typename I::c_type;
  return std::numeric_limits<in_type>::max();
}

template <typename O, typename I>
constexpr RET_TYPE(is_integral_unsigned_to_signed) SafeMinimum() {
  return 0;
}

template <typename O, typename I>
constexpr RET_TYPE(is_integral_unsigned_to_signed) SafeMaximum() {
  using in_type = typename I::c_type;
  using out_type = typename O::c_type;

  // Equality is missing because in_type::max() > out_type::max() when types
  // are of the same width.
  return static_cast<in_type>(sizeof(in_type) < sizeof(out_type)
                                  ? std::numeric_limits<in_type>::max()
                                  : std::numeric_limits<out_type>::max());
}

template <typename O, typename I>
constexpr RET_TYPE(is_integral_signed_to_unsigned) SafeMinimum() {
  return 0;
}

template <typename O, typename I>
constexpr RET_TYPE(is_integral_signed_to_unsigned) SafeMaximum() {
  using in_type = typename I::c_type;
  using out_type = typename O::c_type;

  return static_cast<in_type>(sizeof(in_type) <= sizeof(out_type)
                                  ? std::numeric_limits<in_type>::max()
                                  : std::numeric_limits<out_type>::max());
}

#undef RET_TYPE

#define GET_MIN_MAX_CASE(TYPE, OUT_TYPE)    \
  case Type::TYPE:                          \
    *min = SafeMinimum<OUT_TYPE, InType>(); \
    *max = SafeMaximum<OUT_TYPE, InType>(); \
    break

template <typename InType, typename T = typename InType::c_type>
void GetSafeMinMax(Type::type out_type, T* min, T* max) {
  switch (out_type) {
    GET_MIN_MAX_CASE(INT8, Int8Type);
    GET_MIN_MAX_CASE(INT16, Int16Type);
    GET_MIN_MAX_CASE(INT32, Int32Type);
    GET_MIN_MAX_CASE(INT64, Int64Type);
    GET_MIN_MAX_CASE(UINT8, UInt8Type);
    GET_MIN_MAX_CASE(UINT16, UInt16Type);
    GET_MIN_MAX_CASE(UINT32, UInt32Type);
    GET_MIN_MAX_CASE(UINT64, UInt64Type);
    default:
      break;
  }
}

template <typename Type, typename CType = typename Type::c_type,
          typename ScalarType = typename TypeTraits<Type>::ScalarType>
Status IntegersCanFitImpl(const ArraySpan& values, const DataType& target_type) {
  CType bound_min{}, bound_max{};
  GetSafeMinMax<Type>(target_type.id(), &bound_min, &bound_max);
  return CheckIntegersInRange(values, ScalarType(bound_min), ScalarType(bound_max));
}

}  // namespace

Status IntegersCanFit(const ArraySpan& values, const DataType& target_type) {
  if (!is_integer(target_type.id())) {
    return Status::Invalid("Target type is not an integer type: ", target_type);
  }

  switch (values.type->id()) {
    case Type::INT8:
      return IntegersCanFitImpl<Int8Type>(values, target_type);
    case Type::INT16:
      return IntegersCanFitImpl<Int16Type>(values, target_type);
    case Type::INT32:
      return IntegersCanFitImpl<Int32Type>(values, target_type);
    case Type::INT64:
      return IntegersCanFitImpl<Int64Type>(values, target_type);
    case Type::UINT8:
      return IntegersCanFitImpl<UInt8Type>(values, target_type);
    case Type::UINT16:
      return IntegersCanFitImpl<UInt16Type>(values, target_type);
    case Type::UINT32:
      return IntegersCanFitImpl<UInt32Type>(values, target_type);
    case Type::UINT64:
      return IntegersCanFitImpl<UInt64Type>(values, target_type);
    default:
      return Status::TypeError("Invalid index type for boundschecking");
  }
}

Status IntegersCanFit(const Scalar& scalar, const DataType& target_type) {
  if (!is_integer(scalar.type->id())) {
    return Status::Invalid("Scalar is not an integer");
  } else if (!scalar.is_valid) {
    return Status::OK();
  }

  ArraySpan span(scalar);
  return IntegersCanFit(span, target_type);
}

}  // namespace internal
}  // namespace arrow

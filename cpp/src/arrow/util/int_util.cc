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
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

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
inline uint8_t ExpandedUIntWidth(uint64_t val, uint8_t current_width) {
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
inline void DowncastIntsInternal(const Source* src, Dest* dest, int64_t length) {
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
  DowncastIntsInternal(source, dest, length);
}

void DowncastInts(const int64_t* source, int16_t* dest, int64_t length) {
  DowncastIntsInternal(source, dest, length);
}

void DowncastInts(const int64_t* source, int32_t* dest, int64_t length) {
  DowncastIntsInternal(source, dest, length);
}

void DowncastInts(const int64_t* source, int64_t* dest, int64_t length) {
  memcpy(dest, source, length * sizeof(int64_t));
}

void DowncastUInts(const uint64_t* source, uint8_t* dest, int64_t length) {
  DowncastIntsInternal(source, dest, length);
}

void DowncastUInts(const uint64_t* source, uint16_t* dest, int64_t length) {
  DowncastIntsInternal(source, dest, length);
}

void DowncastUInts(const uint64_t* source, uint32_t* dest, int64_t length) {
  DowncastIntsInternal(source, dest, length);
}

void DowncastUInts(const uint64_t* source, uint64_t* dest, int64_t length) {
  memcpy(dest, source, length * sizeof(int64_t));
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

#define INSTANTIATE(SRC, DEST)              \
  template ARROW_EXPORT void TransposeInts( \
      const SRC* source, DEST* dest, int64_t length, const int32_t* transpose_map);

#define INSTANTIATE_ALL_DEST(DEST) \
  INSTANTIATE(int8_t, DEST)        \
  INSTANTIATE(int16_t, DEST)       \
  INSTANTIATE(int32_t, DEST)       \
  INSTANTIATE(int64_t, DEST)

#define INSTANTIATE_ALL()       \
  INSTANTIATE_ALL_DEST(int8_t)  \
  INSTANTIATE_ALL_DEST(int16_t) \
  INSTANTIATE_ALL_DEST(int32_t) \
  INSTANTIATE_ALL_DEST(int64_t)

INSTANTIATE_ALL()

#undef INSTANTIATE
#undef INSTANTIATE_ALL
#undef INSTANTIATE_ALL_DEST

template <typename IndexCType, bool IsSigned = std::is_signed<IndexCType>::value>
Status IndexBoundsCheckImpl(const ArrayData& indices, uint64_t upper_limit) {
  // For unsigned integers, if the values array is larger than the maximum
  // index value (e.g. especially for UINT8 / UINT16), then there is no need to
  // boundscheck.
  if (!IsSigned &&
      upper_limit > static_cast<uint64_t>(std::numeric_limits<IndexCType>::max())) {
    return Status::OK();
  }

  const IndexCType* indices_data = indices.GetValues<IndexCType>(1);
  const uint8_t* bitmap = nullptr;
  if (indices.buffers[0]) {
    bitmap = indices.buffers[0]->data();
  }
  auto IsOutOfBounds = [&](int64_t i) -> bool {
    return (
        (IsSigned && indices_data[i] < 0) ||
        (indices_data[i] >= 0 && static_cast<uint64_t>(indices_data[i]) >= upper_limit));
  };
  OptionalBitBlockCounter indices_bit_counter(bitmap, indices.offset, indices.length);
  int64_t position = 0;
  while (position < indices.length) {
    BitBlockCount block = indices_bit_counter.NextBlock();
    bool block_out_of_bounds = false;
    if (block.popcount == block.length) {
      // Fast path: branchless
      for (int64_t i = 0; i < block.length; ++i) {
        block_out_of_bounds |= IsOutOfBounds(i);
      }
    } else if (block.popcount > 0) {
      // Indices have nulls, must only boundscheck non-null values
      for (int64_t i = 0; i < block.length; ++i) {
        if (BitUtil::GetBit(bitmap, indices.offset + position + i)) {
          block_out_of_bounds |= IsOutOfBounds(i);
        }
      }
    }
    if (ARROW_PREDICT_FALSE(block_out_of_bounds)) {
      if (indices.GetNullCount() > 0) {
        for (int64_t i = 0; i < block.length; ++i) {
          if (BitUtil::GetBit(bitmap, indices.offset + position + i)) {
            if (IsOutOfBounds(i)) {
              return Status::IndexError("Index ", static_cast<int64_t>(indices_data[i]),
                                        " out of bounds");
            }
          }
        }
      } else {
        for (int64_t i = 0; i < block.length; ++i) {
          if (IsOutOfBounds(i)) {
            return Status::IndexError("Index ", static_cast<int64_t>(indices_data[i]),
                                      " out of bounds");
          }
        }
      }
    }
    indices_data += block.length;
    position += block.length;
  }
  return Status::OK();
}

/// \brief Branchless boundschecking of the indices. Processes batches of
/// indices at a time and shortcircuits when encountering an out-of-bounds
/// index in a batch
Status IndexBoundsCheck(const ArrayData& indices, uint64_t upper_limit) {
  switch (indices.type->id()) {
    case Type::INT8:
      return IndexBoundsCheckImpl<int8_t>(indices, upper_limit);
    case Type::INT16:
      return IndexBoundsCheckImpl<int16_t>(indices, upper_limit);
    case Type::INT32:
      return IndexBoundsCheckImpl<int32_t>(indices, upper_limit);
    case Type::INT64:
      return IndexBoundsCheckImpl<int64_t>(indices, upper_limit);
    case Type::UINT8:
      return IndexBoundsCheckImpl<uint8_t>(indices, upper_limit);
    case Type::UINT16:
      return IndexBoundsCheckImpl<uint16_t>(indices, upper_limit);
    case Type::UINT32:
      return IndexBoundsCheckImpl<uint32_t>(indices, upper_limit);
    case Type::UINT64:
      return IndexBoundsCheckImpl<uint64_t>(indices, upper_limit);
    default:
      return Status::Invalid("Invalid index type for boundschecking");
  }
}

}  // namespace internal
}  // namespace arrow

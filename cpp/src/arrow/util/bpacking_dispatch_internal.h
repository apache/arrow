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

#include <cstring>
#include <type_traits>

#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow::internal {

/// Unpack a zero bit packed array.
template <typename Uint>
int unpack_null(const uint8_t* in, Uint* out, int batch_size) {
  std::memset(out, 0, batch_size * sizeof(Uint));
  return batch_size;
}

/// Unpack a packed array where packed and unpacked values have exactly the same number of
/// bits.
template <typename Uint>
int unpack_full(const uint8_t* in, Uint* out, int batch_size) {
  if constexpr (ARROW_LITTLE_ENDIAN == 1) {
    std::memcpy(out, in, batch_size * sizeof(Uint));
  } else {
    using bit_util::FromLittleEndian;
    using util::SafeLoadAs;

    for (int k = 0; k < batch_size; k += 1) {
      out[k] = FromLittleEndian(SafeLoadAs<Uint>(in + (k * sizeof(Uint))));
    }
  }
  return batch_size;
}

/// Unpack a packed array, delegating to a Unpacker struct.
///
/// @tparam kPackedBitWidth The width in bits of the values in the packed array.
/// @tparam Unpacker The struct providing information and an ``unpack`` method to unpack a
///                  fixed amount of values (usually constrained by SIMD batch sizes and
///                  byte alignment).
/// @tparam UnpackedUInt The type in which we unpack the values.
template <int kPackedBitWidth, template <typename, int> typename Unpacker,
          typename UnpackedUInt>
int unpack_width(const uint8_t* in, UnpackedUInt* out, int batch_size) {
  using UnpackerForWidth = Unpacker<UnpackedUInt, kPackedBitWidth>;

  constexpr auto kValuesUnpacked = UnpackerForWidth::kValuesUnpacked;
  batch_size = batch_size / kValuesUnpacked * kValuesUnpacked;
  int num_loops = batch_size / kValuesUnpacked;

  for (int i = 0; i < num_loops; ++i) {
    in = UnpackerForWidth::unpack(in, out + i * kValuesUnpacked);
  }

  return batch_size;
}

template <template <typename, int> typename Unpacker, typename UnpackedUint>
static int unpack_jump(const uint8_t* in, UnpackedUint* out, int batch_size,
                       int num_bits) {
  if constexpr (std::is_same_v<UnpackedUint, bool>) {
    switch (num_bits) {
      case 0:
        return unpack_null(in, out, batch_size);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size);
    }
  } else if constexpr (sizeof(UnpackedUint) == 1) {
    switch (num_bits) {
      case 0:
        return unpack_null(in, out, batch_size);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size);
      case 2:
        return unpack_width<2, Unpacker>(in, out, batch_size);
      case 3:
        return unpack_width<3, Unpacker>(in, out, batch_size);
      case 4:
        return unpack_width<4, Unpacker>(in, out, batch_size);
      case 5:
        return unpack_width<5, Unpacker>(in, out, batch_size);
      case 6:
        return unpack_width<6, Unpacker>(in, out, batch_size);
      case 7:
        return unpack_width<7, Unpacker>(in, out, batch_size);
      case 8:
        return unpack_full(in, out, batch_size);
    }
  } else if constexpr (sizeof(UnpackedUint) == 2) {
    switch (num_bits) {
      case 0:
        return unpack_null(in, out, batch_size);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size);
      case 2:
        return unpack_width<2, Unpacker>(in, out, batch_size);
      case 3:
        return unpack_width<3, Unpacker>(in, out, batch_size);
      case 4:
        return unpack_width<4, Unpacker>(in, out, batch_size);
      case 5:
        return unpack_width<5, Unpacker>(in, out, batch_size);
      case 6:
        return unpack_width<6, Unpacker>(in, out, batch_size);
      case 7:
        return unpack_width<7, Unpacker>(in, out, batch_size);
      case 8:
        return unpack_width<8, Unpacker>(in, out, batch_size);
      case 9:
        return unpack_width<9, Unpacker>(in, out, batch_size);
      case 10:
        return unpack_width<10, Unpacker>(in, out, batch_size);
      case 11:
        return unpack_width<11, Unpacker>(in, out, batch_size);
      case 12:
        return unpack_width<12, Unpacker>(in, out, batch_size);
      case 13:
        return unpack_width<13, Unpacker>(in, out, batch_size);
      case 14:
        return unpack_width<14, Unpacker>(in, out, batch_size);
      case 15:
        return unpack_width<15, Unpacker>(in, out, batch_size);
      case 16:
        return unpack_full(in, out, batch_size);
    }
  } else if constexpr (sizeof(UnpackedUint) == 4) {
    switch (num_bits) {
      case 0:
        return unpack_null(in, out, batch_size);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size);
      case 2:
        return unpack_width<2, Unpacker>(in, out, batch_size);
      case 3:
        return unpack_width<3, Unpacker>(in, out, batch_size);
      case 4:
        return unpack_width<4, Unpacker>(in, out, batch_size);
      case 5:
        return unpack_width<5, Unpacker>(in, out, batch_size);
      case 6:
        return unpack_width<6, Unpacker>(in, out, batch_size);
      case 7:
        return unpack_width<7, Unpacker>(in, out, batch_size);
      case 8:
        return unpack_width<8, Unpacker>(in, out, batch_size);
      case 9:
        return unpack_width<9, Unpacker>(in, out, batch_size);
      case 10:
        return unpack_width<10, Unpacker>(in, out, batch_size);
      case 11:
        return unpack_width<11, Unpacker>(in, out, batch_size);
      case 12:
        return unpack_width<12, Unpacker>(in, out, batch_size);
      case 13:
        return unpack_width<13, Unpacker>(in, out, batch_size);
      case 14:
        return unpack_width<14, Unpacker>(in, out, batch_size);
      case 15:
        return unpack_width<15, Unpacker>(in, out, batch_size);
      case 16:
        return unpack_width<16, Unpacker>(in, out, batch_size);
      case 17:
        return unpack_width<17, Unpacker>(in, out, batch_size);
      case 18:
        return unpack_width<18, Unpacker>(in, out, batch_size);
      case 19:
        return unpack_width<19, Unpacker>(in, out, batch_size);
      case 20:
        return unpack_width<20, Unpacker>(in, out, batch_size);
      case 21:
        return unpack_width<21, Unpacker>(in, out, batch_size);
      case 22:
        return unpack_width<22, Unpacker>(in, out, batch_size);
      case 23:
        return unpack_width<23, Unpacker>(in, out, batch_size);
      case 24:
        return unpack_width<24, Unpacker>(in, out, batch_size);
      case 25:
        return unpack_width<25, Unpacker>(in, out, batch_size);
      case 26:
        return unpack_width<26, Unpacker>(in, out, batch_size);
      case 27:
        return unpack_width<27, Unpacker>(in, out, batch_size);
      case 28:
        return unpack_width<28, Unpacker>(in, out, batch_size);
      case 29:
        return unpack_width<29, Unpacker>(in, out, batch_size);
      case 30:
        return unpack_width<30, Unpacker>(in, out, batch_size);
      case 31:
        return unpack_width<31, Unpacker>(in, out, batch_size);
      case 32:
        return unpack_full(in, out, batch_size);
    }
  } else if constexpr (sizeof(UnpackedUint) == 8) {
    switch (num_bits) {
      case 0:
        return unpack_null(in, out, batch_size);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size);
      case 2:
        return unpack_width<2, Unpacker>(in, out, batch_size);
      case 3:
        return unpack_width<3, Unpacker>(in, out, batch_size);
      case 4:
        return unpack_width<4, Unpacker>(in, out, batch_size);
      case 5:
        return unpack_width<5, Unpacker>(in, out, batch_size);
      case 6:
        return unpack_width<6, Unpacker>(in, out, batch_size);
      case 7:
        return unpack_width<7, Unpacker>(in, out, batch_size);
      case 8:
        return unpack_width<8, Unpacker>(in, out, batch_size);
      case 9:
        return unpack_width<9, Unpacker>(in, out, batch_size);
      case 10:
        return unpack_width<10, Unpacker>(in, out, batch_size);
      case 11:
        return unpack_width<11, Unpacker>(in, out, batch_size);
      case 12:
        return unpack_width<12, Unpacker>(in, out, batch_size);
      case 13:
        return unpack_width<13, Unpacker>(in, out, batch_size);
      case 14:
        return unpack_width<14, Unpacker>(in, out, batch_size);
      case 15:
        return unpack_width<15, Unpacker>(in, out, batch_size);
      case 16:
        return unpack_width<16, Unpacker>(in, out, batch_size);
      case 17:
        return unpack_width<17, Unpacker>(in, out, batch_size);
      case 18:
        return unpack_width<18, Unpacker>(in, out, batch_size);
      case 19:
        return unpack_width<19, Unpacker>(in, out, batch_size);
      case 20:
        return unpack_width<20, Unpacker>(in, out, batch_size);
      case 21:
        return unpack_width<21, Unpacker>(in, out, batch_size);
      case 22:
        return unpack_width<22, Unpacker>(in, out, batch_size);
      case 23:
        return unpack_width<23, Unpacker>(in, out, batch_size);
      case 24:
        return unpack_width<24, Unpacker>(in, out, batch_size);
      case 25:
        return unpack_width<25, Unpacker>(in, out, batch_size);
      case 26:
        return unpack_width<26, Unpacker>(in, out, batch_size);
      case 27:
        return unpack_width<27, Unpacker>(in, out, batch_size);
      case 28:
        return unpack_width<28, Unpacker>(in, out, batch_size);
      case 29:
        return unpack_width<29, Unpacker>(in, out, batch_size);
      case 30:
        return unpack_width<30, Unpacker>(in, out, batch_size);
      case 31:
        return unpack_width<31, Unpacker>(in, out, batch_size);
      case 32:
        return unpack_width<32, Unpacker>(in, out, batch_size);
      case 33:
        return unpack_width<33, Unpacker>(in, out, batch_size);
      case 34:
        return unpack_width<34, Unpacker>(in, out, batch_size);
      case 35:
        return unpack_width<35, Unpacker>(in, out, batch_size);
      case 36:
        return unpack_width<36, Unpacker>(in, out, batch_size);
      case 37:
        return unpack_width<37, Unpacker>(in, out, batch_size);
      case 38:
        return unpack_width<38, Unpacker>(in, out, batch_size);
      case 39:
        return unpack_width<39, Unpacker>(in, out, batch_size);
      case 40:
        return unpack_width<40, Unpacker>(in, out, batch_size);
      case 41:
        return unpack_width<41, Unpacker>(in, out, batch_size);
      case 42:
        return unpack_width<42, Unpacker>(in, out, batch_size);
      case 43:
        return unpack_width<43, Unpacker>(in, out, batch_size);
      case 44:
        return unpack_width<44, Unpacker>(in, out, batch_size);
      case 45:
        return unpack_width<45, Unpacker>(in, out, batch_size);
      case 46:
        return unpack_width<46, Unpacker>(in, out, batch_size);
      case 47:
        return unpack_width<47, Unpacker>(in, out, batch_size);
      case 48:
        return unpack_width<48, Unpacker>(in, out, batch_size);
      case 49:
        return unpack_width<49, Unpacker>(in, out, batch_size);
      case 50:
        return unpack_width<50, Unpacker>(in, out, batch_size);
      case 51:
        return unpack_width<51, Unpacker>(in, out, batch_size);
      case 52:
        return unpack_width<52, Unpacker>(in, out, batch_size);
      case 53:
        return unpack_width<53, Unpacker>(in, out, batch_size);
      case 54:
        return unpack_width<54, Unpacker>(in, out, batch_size);
      case 55:
        return unpack_width<55, Unpacker>(in, out, batch_size);
      case 56:
        return unpack_width<56, Unpacker>(in, out, batch_size);
      case 57:
        return unpack_width<57, Unpacker>(in, out, batch_size);
      case 58:
        return unpack_width<58, Unpacker>(in, out, batch_size);
      case 59:
        return unpack_width<59, Unpacker>(in, out, batch_size);
      case 60:
        return unpack_width<60, Unpacker>(in, out, batch_size);
      case 61:
        return unpack_width<61, Unpacker>(in, out, batch_size);
      case 62:
        return unpack_width<62, Unpacker>(in, out, batch_size);
      case 63:
        return unpack_width<63, Unpacker>(in, out, batch_size);
      case 64:
        return unpack_full(in, out, batch_size);
    }
  }
  ARROW_DCHECK(false) << "Unsupported num_bits";
  return 0;
}
}  // namespace arrow::internal

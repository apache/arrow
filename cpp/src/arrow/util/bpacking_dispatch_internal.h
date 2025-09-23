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

#include <cstring>

#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow::internal {

template <typename Uint>

int unpack_null(const uint8_t* in, Uint* out, int batch_size) {
  std::memset(out, 0, batch_size * sizeof(Uint));
  return batch_size;
}

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

template <int kBit, template <typename, int> typename Unpacker, typename Uint>
int unpack(const uint8_t* in, Uint* out, int batch_size) {
  using UnpackerForWidth = Unpacker<Uint, kBit>;

  constexpr auto kValuesUnpacked = UnpackerForWidth::kValuesUnpacked;
  batch_size = batch_size / kValuesUnpacked * kValuesUnpacked;
  int num_loops = batch_size / kValuesUnpacked;

  for (int i = 0; i < num_loops; ++i) {
    in = UnpackerForWidth::unpack(in, out + i * kValuesUnpacked);
  }

  return batch_size;
}

template <template <typename, int> typename Unpacker>
static int unpack_jump32(const uint8_t* in, uint32_t* out, int batch_size, int num_bits) {
  switch (num_bits) {
    case 0:
      return unpack_null(in, out, batch_size);
    case 1:
      return unpack<1, Unpacker>(in, out, batch_size);
    case 2:
      return unpack<2, Unpacker>(in, out, batch_size);
    case 3:
      return unpack<3, Unpacker>(in, out, batch_size);
    case 4:
      return unpack<4, Unpacker>(in, out, batch_size);
    case 5:
      return unpack<5, Unpacker>(in, out, batch_size);
    case 6:
      return unpack<6, Unpacker>(in, out, batch_size);
    case 7:
      return unpack<7, Unpacker>(in, out, batch_size);
    case 8:
      return unpack<8, Unpacker>(in, out, batch_size);
    case 9:
      return unpack<9, Unpacker>(in, out, batch_size);
    case 10:
      return unpack<10, Unpacker>(in, out, batch_size);
    case 11:
      return unpack<11, Unpacker>(in, out, batch_size);
    case 12:
      return unpack<12, Unpacker>(in, out, batch_size);
    case 13:
      return unpack<13, Unpacker>(in, out, batch_size);
    case 14:
      return unpack<14, Unpacker>(in, out, batch_size);
    case 15:
      return unpack<15, Unpacker>(in, out, batch_size);
    case 16:
      return unpack<16, Unpacker>(in, out, batch_size);
    case 17:
      return unpack<17, Unpacker>(in, out, batch_size);
    case 18:
      return unpack<18, Unpacker>(in, out, batch_size);
    case 19:
      return unpack<19, Unpacker>(in, out, batch_size);
    case 20:
      return unpack<20, Unpacker>(in, out, batch_size);
    case 21:
      return unpack<21, Unpacker>(in, out, batch_size);
    case 22:
      return unpack<22, Unpacker>(in, out, batch_size);
    case 23:
      return unpack<23, Unpacker>(in, out, batch_size);
    case 24:
      return unpack<24, Unpacker>(in, out, batch_size);
    case 25:
      return unpack<25, Unpacker>(in, out, batch_size);
    case 26:
      return unpack<26, Unpacker>(in, out, batch_size);
    case 27:
      return unpack<27, Unpacker>(in, out, batch_size);
    case 28:
      return unpack<28, Unpacker>(in, out, batch_size);
    case 29:
      return unpack<29, Unpacker>(in, out, batch_size);
    case 30:
      return unpack<30, Unpacker>(in, out, batch_size);
    case 31:
      return unpack<31, Unpacker>(in, out, batch_size);
    case 32:
      return unpack_full(in, out, batch_size);
    default:
      ARROW_DCHECK(false) << "Unsupported num_bits";
  }

  return 0;
}

template <template <typename, int> typename Unpacker>
static int unpack_jump64(const uint8_t* in, uint64_t* out, int batch_size, int num_bits) {
  switch (num_bits) {
    case 0:
      return unpack_null(in, out, batch_size);
    case 1:
      return unpack<1, Unpacker>(in, out, batch_size);
    case 2:
      return unpack<2, Unpacker>(in, out, batch_size);
    case 3:
      return unpack<3, Unpacker>(in, out, batch_size);
    case 4:
      return unpack<4, Unpacker>(in, out, batch_size);
    case 5:
      return unpack<5, Unpacker>(in, out, batch_size);
    case 6:
      return unpack<6, Unpacker>(in, out, batch_size);
    case 7:
      return unpack<7, Unpacker>(in, out, batch_size);
    case 8:
      return unpack<8, Unpacker>(in, out, batch_size);
    case 9:
      return unpack<9, Unpacker>(in, out, batch_size);
    case 10:
      return unpack<10, Unpacker>(in, out, batch_size);
    case 11:
      return unpack<11, Unpacker>(in, out, batch_size);
    case 12:
      return unpack<12, Unpacker>(in, out, batch_size);
    case 13:
      return unpack<13, Unpacker>(in, out, batch_size);
    case 14:
      return unpack<14, Unpacker>(in, out, batch_size);
    case 15:
      return unpack<15, Unpacker>(in, out, batch_size);
    case 16:
      return unpack<16, Unpacker>(in, out, batch_size);
    case 17:
      return unpack<17, Unpacker>(in, out, batch_size);
    case 18:
      return unpack<18, Unpacker>(in, out, batch_size);
    case 19:
      return unpack<19, Unpacker>(in, out, batch_size);
    case 20:
      return unpack<20, Unpacker>(in, out, batch_size);
    case 21:
      return unpack<21, Unpacker>(in, out, batch_size);
    case 22:
      return unpack<22, Unpacker>(in, out, batch_size);
    case 23:
      return unpack<23, Unpacker>(in, out, batch_size);
    case 24:
      return unpack<24, Unpacker>(in, out, batch_size);
    case 25:
      return unpack<25, Unpacker>(in, out, batch_size);
    case 26:
      return unpack<26, Unpacker>(in, out, batch_size);
    case 27:
      return unpack<27, Unpacker>(in, out, batch_size);
    case 28:
      return unpack<28, Unpacker>(in, out, batch_size);
    case 29:
      return unpack<29, Unpacker>(in, out, batch_size);
    case 30:
      return unpack<30, Unpacker>(in, out, batch_size);
    case 31:
      return unpack<31, Unpacker>(in, out, batch_size);
    case 32:
      return unpack<32, Unpacker>(in, out, batch_size);
    case 33:
      return unpack<33, Unpacker>(in, out, batch_size);
    case 34:
      return unpack<34, Unpacker>(in, out, batch_size);
    case 35:
      return unpack<35, Unpacker>(in, out, batch_size);
    case 36:
      return unpack<36, Unpacker>(in, out, batch_size);
    case 37:
      return unpack<37, Unpacker>(in, out, batch_size);
    case 38:
      return unpack<38, Unpacker>(in, out, batch_size);
    case 39:
      return unpack<39, Unpacker>(in, out, batch_size);
    case 40:
      return unpack<40, Unpacker>(in, out, batch_size);
    case 41:
      return unpack<41, Unpacker>(in, out, batch_size);
    case 42:
      return unpack<42, Unpacker>(in, out, batch_size);
    case 43:
      return unpack<43, Unpacker>(in, out, batch_size);
    case 44:
      return unpack<44, Unpacker>(in, out, batch_size);
    case 45:
      return unpack<45, Unpacker>(in, out, batch_size);
    case 46:
      return unpack<46, Unpacker>(in, out, batch_size);
    case 47:
      return unpack<47, Unpacker>(in, out, batch_size);
    case 48:
      return unpack<48, Unpacker>(in, out, batch_size);
    case 49:
      return unpack<49, Unpacker>(in, out, batch_size);
    case 50:
      return unpack<50, Unpacker>(in, out, batch_size);
    case 51:
      return unpack<51, Unpacker>(in, out, batch_size);
    case 52:
      return unpack<52, Unpacker>(in, out, batch_size);
    case 53:
      return unpack<53, Unpacker>(in, out, batch_size);
    case 54:
      return unpack<54, Unpacker>(in, out, batch_size);
    case 55:
      return unpack<55, Unpacker>(in, out, batch_size);
    case 56:
      return unpack<56, Unpacker>(in, out, batch_size);
    case 57:
      return unpack<57, Unpacker>(in, out, batch_size);
    case 58:
      return unpack<58, Unpacker>(in, out, batch_size);
    case 59:
      return unpack<59, Unpacker>(in, out, batch_size);
    case 60:
      return unpack<60, Unpacker>(in, out, batch_size);
    case 61:
      return unpack<61, Unpacker>(in, out, batch_size);
    case 62:
      return unpack<62, Unpacker>(in, out, batch_size);
    case 63:
      return unpack<63, Unpacker>(in, out, batch_size);
    case 64:
      return unpack_full(in, out, batch_size);
    default:
      ARROW_DCHECK(false) << "Unsupported num_bits";
  }
  return 0;
}

}  // namespace arrow::internal

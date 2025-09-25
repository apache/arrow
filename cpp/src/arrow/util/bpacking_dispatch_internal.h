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
#include <utility>

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

template <template <typename, int> typename Unpacker, typename UnpackedUint, int... Is>
constexpr auto make_unpack_jump_table_impl(std::integer_sequence<int, Is...>) {
  return std::array{
      &unpack_null<UnpackedUint>,
      &unpack_width<Is + 1, Unpacker, UnpackedUint>...,
      &unpack_full<UnpackedUint>,
  };
}

template <template <typename, int> typename Unpacker, typename UnpackedUint>
constexpr auto make_unpack_jump_table() {
  return make_unpack_jump_table_impl<Unpacker, UnpackedUint>(
      std::make_integer_sequence<int, 8 * sizeof(UnpackedUint) - 1>());
}

template <template <typename, int> typename Unpacker, typename UnpackedUint>
constexpr auto get_unpack_fn(int packed_width) {
  constexpr auto kJumpTable = make_unpack_jump_table<Unpacker, UnpackedUint>();
  return kJumpTable[packed_width];
}

template <template <typename, int> typename Unpacker, typename UnpackedUint>
static int unpack_jump(const uint8_t* in, UnpackedUint* out, int batch_size,
                       int num_bits) {
  ARROW_DCHECK_GE(num_bits, 0);
  ARROW_DCHECK_LE(num_bits, static_cast<int>(8 * sizeof(UnpackedUint)));
  return get_unpack_fn<Unpacker, UnpackedUint>(num_bits)(in, out, batch_size);
}
}  // namespace arrow::internal

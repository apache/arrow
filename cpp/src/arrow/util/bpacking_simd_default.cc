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

#if defined(ARROW_HAVE_NEON)
#  define UNPACK_PLATFORM unpack_neon
#elif defined(ARROW_HAVE_SSE4_2)
#  define UNPACK_PLATFORM unpack_sse4_2
#endif

#if defined(UNPACK_PLATFORM)

#  include "arrow/util/bpacking_dispatch_internal.h"
#  include "arrow/util/bpacking_simd_impl_internal.h"
#  include "arrow/util/bpacking_simd_internal.h"

namespace arrow::internal {

template <typename UnpackedUint, int kPackedBitSize>
using Simd128Kernel = Kernel<UnpackedUint, kPackedBitSize, 128>;

template <typename Uint>
void UNPACK_PLATFORM(const uint8_t* in, Uint* out, int batch_size, int num_bits,
                     int bit_offset) {
  return unpack_jump<Simd128Kernel>(in, out, batch_size, num_bits, bit_offset);
}

template void UNPACK_PLATFORM<bool>(const uint8_t*, bool*, int, int, int);
template void UNPACK_PLATFORM<uint8_t>(const uint8_t*, uint8_t*, int, int, int);
template void UNPACK_PLATFORM<uint16_t>(const uint8_t*, uint16_t*, int, int, int);
template void UNPACK_PLATFORM<uint32_t>(const uint8_t*, uint32_t*, int, int, int);
template void UNPACK_PLATFORM<uint64_t>(const uint8_t*, uint64_t*, int, int, int);

}  // namespace arrow::internal

#endif

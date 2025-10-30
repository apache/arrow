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

#include "arrow/util/bpacking_dispatch_internal.h"
#if defined(ARROW_HAVE_NEON) || defined(ARROW_HAVE_SSE4_2)
#  include "arrow/util/bpacking_simd128_generated_internal.h"
#endif
#include "arrow/util/bpacking_simd_internal.h"

namespace arrow::internal {

#if defined(ARROW_HAVE_NEON)

template <typename Uint>
void unpack_neon(const uint8_t* in, Uint* out, int batch_size, int num_bits,
                 int bit_offset) {
  return unpack_jump<Simd128UnpackerForWidth>(in, out, batch_size, num_bits, bit_offset);
}

template void unpack_neon<bool>(const uint8_t*, bool*, int, int, int);
template void unpack_neon<uint8_t>(const uint8_t*, uint8_t*, int, int, int);
template void unpack_neon<uint16_t>(const uint8_t*, uint16_t*, int, int, int);
template void unpack_neon<uint32_t>(const uint8_t*, uint32_t*, int, int, int);
template void unpack_neon<uint64_t>(const uint8_t*, uint64_t*, int, int, int);

#elif defined(ARROW_HAVE_SSE4_2)

template <typename Uint>
void unpack_sse4_2(const uint8_t* in, Uint* out, int batch_size, int num_bits,
                   int bit_offset) {
  return unpack_jump<Simd128UnpackerForWidth>(in, out, batch_size, num_bits, bit_offset);
}

template void unpack_sse4_2<bool>(const uint8_t*, bool*, int, int, int);
template void unpack_sse4_2<uint8_t>(const uint8_t*, uint8_t*, int, int, int);
template void unpack_sse4_2<uint16_t>(const uint8_t*, uint16_t*, int, int, int);
template void unpack_sse4_2<uint32_t>(const uint8_t*, uint32_t*, int, int, int);
template void unpack_sse4_2<uint64_t>(const uint8_t*, uint64_t*, int, int, int);

#endif

}  // namespace arrow::internal

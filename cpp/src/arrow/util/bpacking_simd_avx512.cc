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
#include "arrow/util/bpacking_simd512_generated_internal.h"
#include "arrow/util/bpacking_simd_internal.h"

namespace arrow::internal {

template <typename Uint>
int unpack_avx512(const uint8_t* in, Uint* out, int batch_size, int num_bits) {
  return unpack_jump<Simd512UnpackerForWidth>(in, out, batch_size, num_bits);
}

template int unpack_avx512<bool>(const uint8_t*, bool*, int, int);
template int unpack_avx512<uint8_t>(const uint8_t*, uint8_t*, int, int);
template int unpack_avx512<uint16_t>(const uint8_t*, uint16_t*, int, int);
template int unpack_avx512<uint32_t>(const uint8_t*, uint32_t*, int, int);
template int unpack_avx512<uint64_t>(const uint8_t*, uint64_t*, int, int);

}  // namespace arrow::internal

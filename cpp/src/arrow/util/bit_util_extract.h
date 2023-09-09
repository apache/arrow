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

#include "arrow/util/bit_util.h"
#include "arrow/util/simd.h"

namespace arrow::bit_util {

namespace detail {

uint64_t ExtractBitsSoftwareFallback(uint64_t bitmap, uint64_t select_bitmap);

/// \brief A software emulation of _pext_u64
inline uint64_t ExtractBitsSoftware(uint64_t bitmap, uint64_t select_bitmap) {
  // These checks should be inline and are likely to be common cases.
  if (select_bitmap == ~uint64_t{0}) {
    return bitmap;
  } else if (select_bitmap == 0) {
    return 0;
  }

  return ExtractBitsSoftwareFallback(bitmap, select_bitmap);
}

}  // namespace detail

#ifdef ARROW_HAVE_BMI2

// Use _pext_u64 on 64-bit builds, _pext_u32 on 32-bit builds,
#if UINTPTR_MAX == 0xFFFFFFFF

using extract_bitmap_t = uint32_t;
inline extract_bitmap_t ExtractBits(extract_bitmap_t bitmap,
                                    extract_bitmap_t select_bitmap) {
  return _pext_u32(bitmap, select_bitmap);
}

#else

using extract_bitmap_t = uint64_t;
inline extract_bitmap_t ExtractBits(extract_bitmap_t bitmap,
                                    extract_bitmap_t select_bitmap) {
  return _pext_u64(bitmap, select_bitmap);
}

#endif

#else  // !defined(ARROW_HAVE_BMI2)

// Use 64-bit pext emulation when BMI2 isn't available.
using extract_bitmap_t = uint64_t;
inline extract_bitmap_t ExtractBits(extract_bitmap_t bitmap,
                                    extract_bitmap_t select_bitmap) {
  return detail::ExtractBitsSoftware(bitmap, select_bitmap);
}

#endif

}  // namespace arrow::bit_util

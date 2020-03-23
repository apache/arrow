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

#ifdef ARROW_HAVE_NEON
#include <arm_neon.h>
#endif

#ifdef ARROW_HAVE_ARMV8_CRC
#include <arm_acle.h>
#endif

namespace arrow {

#ifdef ARROW_HAVE_ARMV8_CRC

static inline uint32_t ARMCE_crc32_u8(uint32_t crc, uint8_t v) {
  return __crc32cb(crc, v);
}

static inline uint32_t ARMCE_crc32_u16(uint32_t crc, uint16_t v) {
  return __crc32ch(crc, v);
}

static inline uint32_t ARMCE_crc32_u32(uint32_t crc, uint32_t v) {
  return __crc32cw(crc, v);
}

static inline uint32_t ARMCE_crc32_u64(uint32_t crc, uint64_t v) {
  return __crc32cd(crc, v);
}

#endif  // ARROW_HAVE_ARMV8_CRC

}  // namespace arrow

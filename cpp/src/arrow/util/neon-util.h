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

namespace arrow {

#if defined(__aarch64__) || defined(__AARCH64__)

  #ifdef __ARM_FEATURE_CRC32
    #define ARROW_HAVE_ARM_CRC
    #include <arm_acle.h>

    #ifdef __ARM_FEATURE_CRYPTO
      #include <arm_neon.h>
      #define ARROW_HAVE_ARMV8_CRYPTO

      #define CRC32CX(crc, value) (crc) = __crc32cd((crc), (value))
      #define CRC32CW(crc, value) (crc) = __crc32cw((crc), (value))
      #define CRC32CH(crc, value) (crc) = __crc32ch((crc), (value))
      #define CRC32CB(crc, value) (crc) = __crc32cb((crc), (value))

      #define CRC32C3X8(buffer, ITR) \
        crc1 = __crc32cd(crc1, *((const uint64_t *)buffer + 42*1 + (ITR)));\
        crc2 = __crc32cd(crc2, *((const uint64_t *)buffer + 42*2 + (ITR)));\
        crc0 = __crc32cd(crc0, *((const uint64_t *)buffer + 42*0 + (ITR)));

      #define CRC32C7X3X8(buffer, ITR) do {\
        CRC32C3X8(buffer, ((ITR) * 7 + 0)) \
        CRC32C3X8(buffer, ((ITR) * 7 + 1)) \
        CRC32C3X8(buffer, ((ITR) * 7 + 2)) \
        CRC32C3X8(buffer, ((ITR) * 7 + 3)) \
        CRC32C3X8(buffer, ((ITR) * 7 + 4)) \
        CRC32C3X8(buffer, ((ITR) * 7 + 5)) \
        CRC32C3X8(buffer, ((ITR) * 7 + 6)) \
      } while(0)

      #define PREF4X64L1(buffer, PREF_OFFSET, ITR) \
        __asm__("PRFM PLDL1KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 0)*64));\
        __asm__("PRFM PLDL1KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 1)*64));\
        __asm__("PRFM PLDL1KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 2)*64));\
        __asm__("PRFM PLDL1KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 3)*64));

      #define PREF1KL1(buffer, PREF_OFFSET) \
        PREF4X64L1(buffer,(PREF_OFFSET), 0) \
        PREF4X64L1(buffer,(PREF_OFFSET), 4) \
        PREF4X64L1(buffer,(PREF_OFFSET), 8) \
        PREF4X64L1(buffer,(PREF_OFFSET), 12)

      #define PREF4X64L2(buffer, PREF_OFFSET, ITR) \
        __asm__("PRFM PLDL2KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 0)*64));\
        __asm__("PRFM PLDL2KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 1)*64));\
        __asm__("PRFM PLDL2KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 2)*64));\
        __asm__("PRFM PLDL2KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 3)*64));

      #define PREF1KL2(buffer, PREF_OFFSET) \
        PREF4X64L2(buffer,(PREF_OFFSET), 0) \
        PREF4X64L2(buffer,(PREF_OFFSET), 4) \
        PREF4X64L2(buffer,(PREF_OFFSET), 8) \
        PREF4X64L2(buffer,(PREF_OFFSET), 12)
    #endif //__ARM_FEATURE_CRYPTO

  #endif // __ARM_FEATURE_CRC32

#endif //defined(__aarch64__) || defined(__AARCH64__)

#if defined(__GNUC__) && defined(__linux__) && defined(ARROW_HAVE_ARM_CRC)

#include <asm/hwcap.h>
#include <sys/auxv.h>
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif
static inline uint32_t crc32c_runtime_check(void) {
  uint64_t auxv = getauxval(AT_HWCAP);
  return (auxv & HWCAP_CRC32) != 0;
}

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

#endif  // defined(__GNUC__) && defined(__linux__) && defined(ARROW_HAVE_ARM_CRC)

}  // namespace arrow

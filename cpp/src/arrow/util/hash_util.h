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

// From Apache Impala (incubating) as of 2016-02-22

// XXX(ARROW-6468): this header is now unused.  We keep CRC hash implementations
// around in case they're useful some day (Parquet checksumming?).

#ifndef ARROW_UTIL_HASH_UTIL_H
#define ARROW_UTIL_HASH_UTIL_H

#include <cassert>
#include <cstdint>

#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/neon_util.h"
#include "arrow/util/sse_util.h"

static inline uint32_t HW_crc32_u8(uint32_t crc, uint8_t v) {
  DCHECK(false) << "Hardware CRC support is not enabled";
  return 0;
}

static inline uint32_t HW_crc32_u16(uint32_t crc, uint16_t v) {
  DCHECK(false) << "Hardware CRC support is not enabled";
  return 0;
}

static inline uint32_t HW_crc32_u32(uint32_t crc, uint32_t v) {
  DCHECK(false) << "Hardware CRC support is not enabled";
  return 0;
}

static inline uint32_t HW_crc32_u64(uint32_t crc, uint64_t v) {
  DCHECK(false) << "Hardware CRC support is not enabled";
  return 0;
}

#ifdef ARROW_HAVE_SSE4_2
#define HW_crc32_u8 SSE4_crc32_u8
#define HW_crc32_u16 SSE4_crc32_u16
#define HW_crc32_u32 SSE4_crc32_u32
#define HW_crc32_u64 SSE4_crc32_u64
#elif defined(ARROW_HAVE_ARM_CRC)
#define HW_crc32_u8 ARMCE_crc32_u8
#define HW_crc32_u16 ARMCE_crc32_u16
#define HW_crc32_u32 ARMCE_crc32_u32
#define HW_crc32_u64 ARMCE_crc32_u64
#endif

namespace arrow {

/// Utility class to compute hash values.
class HashUtil {
 public:
#if defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_ARM_CRC)
  static constexpr bool have_hardware_crc32 = true;
#else
  static constexpr bool have_hardware_crc32 = false;
#endif

#ifdef ARROW_HAVE_ARMV8_CRYPTO
/* Crc32c Parallel computation
 *   Algorithm comes from Intel whitepaper:
 *   crc-iscsi-polynomial-crc32-instruction-paper
 *
 * Input data is divided into three equal-sized blocks
 *   Three parallel blocks (crc0, crc1, crc2) for 1024 Bytes
 *   One Block: 42(BLK_LENGTH) * 8(step length: crc32c_u64) bytes
 */
#define BLK_LENGTH 42
  static uint32_t Armv8CrcHashParallel(const void* data, int32_t nbytes, uint32_t crc) {
    const uint8_t* buf8;
    const uint64_t* buf64 = reinterpret_cast<const uint64_t*>(data);
    int32_t length = nbytes;

    while (length >= 1024) {
      uint64_t t0, t1;
      uint32_t crc0 = 0, crc1 = 0, crc2 = 0;

      /* parallel computation params:
       *   k0 = CRC32(x ^ (42 * 8 * 8 * 2 - 1));
       *   k1 = CRC32(x ^ (42 * 8 * 8 - 1));
       */
      uint32_t k0 = 0xe417f38a, k1 = 0x8f158014;

      /* First 8 byte for better pipelining */
      crc0 = ARMCE_crc32_u64(crc, *buf64++);

      /* 3 blocks crc32c parallel computation
       *
       * 42 * 8 * 3 = 1008 (bytes)
       */
      for (int i = 0; i < BLK_LENGTH; i++, buf64++) {
        crc0 = ARMCE_crc32_u64(crc0, *buf64);
        crc1 = ARMCE_crc32_u64(crc1, *(buf64 + BLK_LENGTH));
        crc2 = ARMCE_crc32_u64(crc2, *(buf64 + (BLK_LENGTH * 2)));
      }
      buf64 += (BLK_LENGTH * 2);

      /* Last 8 bytes */
      crc = ARMCE_crc32_u64(crc2, *buf64++);

      t0 = (uint64_t)vmull_p64(crc0, k0);
      t1 = (uint64_t)vmull_p64(crc1, k1);

      /* Merge (crc0, crc1, crc2) -> crc */
      crc1 = ARMCE_crc32_u64(0, t1);
      crc ^= crc1;
      crc0 = ARMCE_crc32_u64(0, t0);
      crc ^= crc0;

      length -= 1024;
    }

    buf8 = reinterpret_cast<const uint8_t*>(buf64);
    while (length >= 8) {
      crc = ARMCE_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(buf8));
      buf8 += 8;
      length -= 8;
    }

    /* The following is more efficient than the straight loop */
    if (length >= 4) {
      crc = ARMCE_crc32_u32(crc, *reinterpret_cast<const uint32_t*>(buf8));
      buf8 += 4;
      length -= 4;
    }

    if (length >= 2) {
      crc = ARMCE_crc32_u16(crc, *reinterpret_cast<const uint16_t*>(buf8));
      buf8 += 2;
      length -= 2;
    }

    if (length >= 1) crc = ARMCE_crc32_u8(crc, *(buf8));

    return crc;
  }
#endif

  /// Compute the Crc32 hash for data using SSE4/ArmCRC instructions.  The input hash
  /// parameter is the current hash/seed value.
  /// This should only be called if SSE/ArmCRC is supported.
  /// This is ~4x faster than Fnv/Boost Hash.
  /// TODO: crc32 hashes with different seeds do not result in different hash functions.
  /// The resulting hashes are correlated.
  static uint32_t CrcHash(const void* data, int32_t nbytes, uint32_t hash) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(data);
    const uint8_t* end = p + nbytes;

#if ARROW_BITNESS >= 64
    while (p <= end - 8) {
      hash = HW_crc32_u64(hash, *reinterpret_cast<const uint64_t*>(p));
      p += 8;
    }
#endif

    while (p <= end - 4) {
      hash = HW_crc32_u32(hash, *reinterpret_cast<const uint32_t*>(p));
      p += 4;
    }
    while (p < end) {
      hash = HW_crc32_u8(hash, *p);
      ++p;
    }

    // The lower half of the CRC hash has has poor uniformity, so swap the halves
    // for anyone who only uses the first several bits of the hash.
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  /// A variant of CRC32 hashing that computes two independent running CRCs
  /// over interleaved halves of the input, giving out a 64-bit integer.
  /// The result's quality should be improved by a finalization step.
  ///
  /// In addition to producing more bits of output, this should be twice
  /// faster than CrcHash on CPUs that can overlap several independent
  /// CRC computations.
  static uint64_t DoubleCrcHash(const void* data, int32_t nbytes, uint64_t hash) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(data);

    uint32_t h1 = static_cast<uint32_t>(hash >> 32);
    uint32_t h2 = static_cast<uint32_t>(hash);

#if ARROW_BITNESS >= 64
    while (nbytes >= 16) {
      h1 = HW_crc32_u64(h1, *reinterpret_cast<const uint64_t*>(p));
      h2 = HW_crc32_u64(h2, *reinterpret_cast<const uint64_t*>(p + 8));
      nbytes -= 16;
      p += 16;
    }
    if (nbytes >= 8) {
      h1 = HW_crc32_u32(h1, *reinterpret_cast<const uint32_t*>(p));
      h2 = HW_crc32_u32(h2, *reinterpret_cast<const uint32_t*>(p + 4));
      nbytes -= 8;
      p += 8;
    }
#else
    while (nbytes >= 8) {
      h1 = HW_crc32_u32(h1, *reinterpret_cast<const uint32_t*>(p));
      h2 = HW_crc32_u32(h2, *reinterpret_cast<const uint32_t*>(p + 4));
      nbytes -= 8;
      p += 8;
    }
#endif

    if (nbytes >= 4) {
      h1 = HW_crc32_u16(h1, *reinterpret_cast<const uint16_t*>(p));
      h2 = HW_crc32_u16(h2, *reinterpret_cast<const uint16_t*>(p + 2));
      nbytes -= 4;
      p += 4;
    }
    switch (nbytes) {
      case 3:
        h1 = HW_crc32_u8(h1, p[2]);
        // fallthrough
      case 2:
        h2 = HW_crc32_u8(h2, p[1]);
        // fallthrough
      case 1:
        h1 = HW_crc32_u8(h1, p[0]);
        // fallthrough
      case 0:
        break;
      default:
        assert(0);
    }

    // A finalization step is recommended to mix up the result's bits
    return (static_cast<uint64_t>(h1) << 32) + h2;
  }
};

}  // namespace arrow

#endif  // ARROW_UTIL_HASH_UTIL_H

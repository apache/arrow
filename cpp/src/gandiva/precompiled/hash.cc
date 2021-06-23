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

extern "C" {

#include <string.h>

#include "./types.h"

static inline gdv_uint64 rotate_left(gdv_uint64 val, int distance) {
  return (val << distance) | (val >> (64 - distance));
}

//
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain.
// See http://smhasher.googlecode.com/svn/trunk/MurmurHash3.cpp
// MurmurHash3_x64_128
//
static inline gdv_uint64 fmix64(gdv_uint64 k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccduLL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53uLL;
  k ^= k >> 33;
  return k;
}

static inline gdv_uint64 murmur3_64(gdv_uint64 val, gdv_int32 seed) {
  gdv_uint64 h1 = seed;
  gdv_uint64 h2 = seed;

  gdv_uint64 c1 = 0x87c37b91114253d5ull;
  gdv_uint64 c2 = 0x4cf5ad432745937full;

  int length = 8;
  gdv_uint64 k1 = 0;

  k1 = val;
  k1 *= c1;
  k1 = rotate_left(k1, 31);
  k1 *= c2;
  h1 ^= k1;

  h1 ^= length;
  h2 ^= length;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;

  // h2 += h1;
  // murmur3_128 should return 128 bit (h1,h2), now we return only 64bits,
  return h1;
}

static inline gdv_uint32 murmur3_32(gdv_uint64 val, gdv_int32 seed) {
  gdv_uint64 c1 = 0xcc9e2d51ull;
  gdv_uint64 c2 = 0x1b873593ull;
  int length = 8;
  static gdv_uint64 UINT_MASK = 0xffffffffull;
  gdv_uint64 lh1 = seed & UINT_MASK;
  for (int i = 0; i < 2; i++) {
    gdv_uint64 lk1 = ((val >> i * 32) & UINT_MASK);
    lk1 *= c1;
    lk1 &= UINT_MASK;

    lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >> 17);

    lk1 *= c2;
    lk1 &= UINT_MASK;

    lh1 ^= lk1;
    lh1 = ((lh1 << 13) & UINT_MASK) | (lh1 >> 19);

    lh1 = lh1 * 5 + 0xe6546b64L;
    lh1 = UINT_MASK & lh1;
  }
  lh1 ^= length;

  lh1 ^= lh1 >> 16;
  lh1 *= 0x85ebca6bull;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 13;
  lh1 *= 0xc2b2ae35ull;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 16;

  return static_cast<gdv_uint32>(lh1);
}

static inline gdv_uint64 double_to_long_bits(double value) {
  gdv_uint64 result;
  memcpy(&result, &value, sizeof(result));
  return result;
}

FORCE_INLINE gdv_int64 hash64(double val, gdv_int64 seed) {
  return murmur3_64(double_to_long_bits(val), static_cast<gdv_int32>(seed));
}

FORCE_INLINE gdv_int32 hash32(double val, gdv_int32 seed) {
  return murmur3_32(double_to_long_bits(val), seed);
}

// Wrappers for all the numeric/data/time arrow types

#define HASH64_WITH_SEED_OP(NAME, TYPE)                                        \
  FORCE_INLINE                                                                 \
  gdv_int64 NAME##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid, gdv_int64 seed, \
                          gdv_boolean seed_isvalid) {                          \
    if (!is_valid) {                                                           \
      return seed;                                                             \
    }                                                                          \
    return hash64(static_cast<double>(in), seed);                              \
  }

#define HASH32_WITH_SEED_OP(NAME, TYPE)                                        \
  FORCE_INLINE                                                                 \
  gdv_int32 NAME##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid, gdv_int32 seed, \
                          gdv_boolean seed_isvalid) {                          \
    if (!is_valid) {                                                           \
      return seed;                                                             \
    }                                                                          \
    return hash32(static_cast<double>(in), seed);                              \
  }

#define HASH64_OP(NAME, TYPE)                                    \
  FORCE_INLINE                                                   \
  gdv_int64 NAME##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid) { \
    return is_valid ? hash64(static_cast<double>(in), 0) : 0;    \
  }

#define HASH32_OP(NAME, TYPE)                                    \
  FORCE_INLINE                                                   \
  gdv_int32 NAME##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid) { \
    return is_valid ? hash32(static_cast<double>(in), 0) : 0;    \
  }

// Expand inner macro for all numeric types.
#define NUMERIC_BOOL_DATE_TYPES(INNER, NAME) \
  INNER(NAME, int8)                          \
  INNER(NAME, int16)                         \
  INNER(NAME, int32)                         \
  INNER(NAME, int64)                         \
  INNER(NAME, uint8)                         \
  INNER(NAME, uint16)                        \
  INNER(NAME, uint32)                        \
  INNER(NAME, uint64)                        \
  INNER(NAME, float32)                       \
  INNER(NAME, float64)                       \
  INNER(NAME, boolean)                       \
  INNER(NAME, date64)                        \
  INNER(NAME, date32)                        \
  INNER(NAME, time32)                        \
  INNER(NAME, timestamp)

NUMERIC_BOOL_DATE_TYPES(HASH32_OP, hash)
NUMERIC_BOOL_DATE_TYPES(HASH32_OP, hash32)
NUMERIC_BOOL_DATE_TYPES(HASH32_OP, hash32AsDouble)
NUMERIC_BOOL_DATE_TYPES(HASH32_WITH_SEED_OP, hash32WithSeed)
NUMERIC_BOOL_DATE_TYPES(HASH32_WITH_SEED_OP, hash32AsDoubleWithSeed)

NUMERIC_BOOL_DATE_TYPES(HASH64_OP, hash64)
NUMERIC_BOOL_DATE_TYPES(HASH64_OP, hash64AsDouble)
NUMERIC_BOOL_DATE_TYPES(HASH64_WITH_SEED_OP, hash64WithSeed)
NUMERIC_BOOL_DATE_TYPES(HASH64_WITH_SEED_OP, hash64AsDoubleWithSeed)

#undef NUMERIC_BOOL_DATE_TYPES

static inline gdv_uint64 murmur3_64_buf(const gdv_uint8* key, gdv_int32 len,
                                        gdv_int32 seed) {
  gdv_uint64 h1 = seed;
  gdv_uint64 h2 = seed;
  gdv_uint64 c1 = 0x87c37b91114253d5ull;
  gdv_uint64 c2 = 0x4cf5ad432745937full;

  const gdv_uint64* blocks = reinterpret_cast<const gdv_uint64*>(key);
  int nblocks = len / 16;
  for (int i = 0; i < nblocks; i++) {
    gdv_uint64 k1 = blocks[i * 2 + 0];
    gdv_uint64 k2 = blocks[i * 2 + 1];

    k1 *= c1;
    k1 = rotate_left(k1, 31);
    k1 *= c2;
    h1 ^= k1;
    h1 = rotate_left(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;
    k2 *= c2;
    k2 = rotate_left(k2, 33);
    k2 *= c1;
    h2 ^= k2;
    h2 = rotate_left(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }

  // tail
  gdv_uint64 k1 = 0;
  gdv_uint64 k2 = 0;

  const gdv_uint8* tail = reinterpret_cast<const gdv_uint8*>(key + nblocks * 16);
  switch (len & 15) {
    case 15:
      k2 = static_cast<gdv_uint64>(tail[14]) << 48;
    case 14:
      k2 ^= static_cast<gdv_uint64>(tail[13]) << 40;
    case 13:
      k2 ^= static_cast<gdv_uint64>(tail[12]) << 32;
    case 12:
      k2 ^= static_cast<gdv_uint64>(tail[11]) << 24;
    case 11:
      k2 ^= static_cast<gdv_uint64>(tail[10]) << 16;
    case 10:
      k2 ^= static_cast<gdv_uint64>(tail[9]) << 8;
    case 9:
      k2 ^= static_cast<gdv_uint64>(tail[8]);
      k2 *= c2;
      k2 = rotate_left(k2, 33);
      k2 *= c1;
      h2 ^= k2;
    case 8:
      k1 ^= static_cast<gdv_uint64>(tail[7]) << 56;
    case 7:
      k1 ^= static_cast<gdv_uint64>(tail[6]) << 48;
    case 6:
      k1 ^= static_cast<gdv_uint64>(tail[5]) << 40;
    case 5:
      k1 ^= static_cast<gdv_uint64>(tail[4]) << 32;
    case 4:
      k1 ^= static_cast<gdv_uint64>(tail[3]) << 24;
    case 3:
      k1 ^= static_cast<gdv_uint64>(tail[2]) << 16;
    case 2:
      k1 ^= static_cast<gdv_uint64>(tail[1]) << 8;
    case 1:
      k1 ^= static_cast<gdv_uint64>(tail[0]) << 0;
      k1 *= c1;
      k1 = rotate_left(k1, 31);
      k1 *= c2;
      h1 ^= k1;
  }

  h1 ^= len;
  h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;
  // h2 += h1;
  // returning 64-bits of the 128-bit hash.
  return h1;
}

static gdv_uint32 murmur3_32_buf(const gdv_uint8* key, gdv_int32 len, gdv_int32 seed) {
  static const gdv_uint64 c1 = 0xcc9e2d51ull;
  static const gdv_uint64 c2 = 0x1b873593ull;
  static const gdv_uint64 UINT_MASK = 0xffffffffull;
  gdv_uint64 lh1 = seed;
  const gdv_uint32* blocks = reinterpret_cast<const gdv_uint32*>(key);
  int nblocks = len / 4;
  const gdv_uint8* tail = reinterpret_cast<const gdv_uint8*>(key + nblocks * 4);
  for (int i = 0; i < nblocks; i++) {
    gdv_uint64 lk1 = static_cast<gdv_uint64>(blocks[i]);

    // k1 *= c1;
    lk1 *= c1;
    lk1 &= UINT_MASK;

    lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >> 17);

    lk1 *= c2;
    lk1 = lk1 & UINT_MASK;
    lh1 ^= lk1;
    lh1 = ((lh1 << 13) & UINT_MASK) | (lh1 >> 19);

    lh1 = lh1 * 5 + 0xe6546b64ull;
    lh1 = UINT_MASK & lh1;
  }

  // tail
  gdv_uint64 lk1 = 0;

  switch (len & 3) {
    case 3:
      lk1 = (tail[2] & 0xff) << 16;
    case 2:
      lk1 |= (tail[1] & 0xff) << 8;
    case 1:
      lk1 |= (tail[0] & 0xff);
      lk1 *= c1;
      lk1 = UINT_MASK & lk1;
      lk1 = ((lk1 << 15) & UINT_MASK) | (lk1 >> 17);

      lk1 *= c2;
      lk1 = lk1 & UINT_MASK;

      lh1 ^= lk1;
  }

  // finalization
  lh1 ^= len;

  lh1 ^= lh1 >> 16;
  lh1 *= 0x85ebca6b;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 13;

  lh1 *= 0xc2b2ae35;
  lh1 = UINT_MASK & lh1;
  lh1 ^= lh1 >> 16;

  return static_cast<gdv_uint32>(lh1 & UINT_MASK);
}

FORCE_INLINE gdv_int64 hash64_buf(const gdv_uint8* buf, int len, gdv_int64 seed) {
  return murmur3_64_buf(buf, len, static_cast<gdv_int32>(seed));
}

FORCE_INLINE gdv_int32 hash32_buf(const gdv_uint8* buf, int len, gdv_int32 seed) {
  return murmur3_32_buf(buf, len, seed);
}

// Wrappers for the varlen types

#define HASH64_BUF_WITH_SEED_OP(NAME, TYPE)                                   \
  FORCE_INLINE                                                                \
  gdv_int64 NAME##_##TYPE(gdv_##TYPE in, gdv_int32 len, gdv_boolean is_valid, \
                          gdv_int64 seed, gdv_boolean seed_isvalid) {         \
    if (!is_valid) {                                                          \
      return seed;                                                            \
    }                                                                         \
    return hash64_buf(reinterpret_cast<const uint8_t*>(in), len, seed);       \
  }

#define HASH32_BUF_WITH_SEED_OP(NAME, TYPE)                                   \
  FORCE_INLINE                                                                \
  gdv_int32 NAME##_##TYPE(gdv_##TYPE in, gdv_int32 len, gdv_boolean is_valid, \
                          gdv_int32 seed, gdv_boolean seed_isvalid) {         \
    if (!is_valid) {                                                          \
      return seed;                                                            \
    }                                                                         \
    return hash32_buf(reinterpret_cast<const uint8_t*>(in), len, seed);       \
  }

#define HASH64_BUF_OP(NAME, TYPE)                                                   \
  FORCE_INLINE                                                                      \
  gdv_int64 NAME##_##TYPE(gdv_##TYPE in, gdv_int32 len, gdv_boolean is_valid) {     \
    return is_valid ? hash64_buf(reinterpret_cast<const uint8_t*>(in), len, 0) : 0; \
  }

#define HASH32_BUF_OP(NAME, TYPE)                                                   \
  FORCE_INLINE                                                                      \
  gdv_int32 NAME##_##TYPE(gdv_##TYPE in, gdv_int32 len, gdv_boolean is_valid) {     \
    return is_valid ? hash32_buf(reinterpret_cast<const uint8_t*>(in), len, 0) : 0; \
  }

// Expand inner macro for all non-numeric types.
#define VAR_LEN_TYPES(INNER, NAME) \
  INNER(NAME, utf8)                \
  INNER(NAME, binary)

VAR_LEN_TYPES(HASH32_BUF_OP, hash)
VAR_LEN_TYPES(HASH32_BUF_OP, hash32)
VAR_LEN_TYPES(HASH32_BUF_OP, hash32AsDouble)
VAR_LEN_TYPES(HASH32_BUF_WITH_SEED_OP, hash32WithSeed)
VAR_LEN_TYPES(HASH32_BUF_WITH_SEED_OP, hash32AsDoubleWithSeed)

VAR_LEN_TYPES(HASH64_BUF_OP, hash64)
VAR_LEN_TYPES(HASH64_BUF_OP, hash64AsDouble)
VAR_LEN_TYPES(HASH64_BUF_WITH_SEED_OP, hash64WithSeed)
VAR_LEN_TYPES(HASH64_BUF_WITH_SEED_OP, hash64AsDoubleWithSeed)

#undef HASH32_BUF_OP
#undef HASH32_BUF_WITH_SEED_OP
#undef HASH32_OP
#undef HASH32_WITH_SEED_OP
#undef HASH64_BUF_OP
#undef HASH64_BUF_WITH_SEED_OP
#undef HASH64_OP
#undef HASH64_WITH_SEED_OP

}  // extern "C"

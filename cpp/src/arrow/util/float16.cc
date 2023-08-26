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

#include <ostream>

#include "arrow/util/float16.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {

namespace {

// --------------------------------------------------------
// Binary conversions
// --------------------------------------------------------
// These routines are partially adapted from Numpy's C implementation
//
// Some useful metrics for conversions between different precisions:
// |-----------------------------------------|
// | precision | half    | single  | double  |
// |-----------------------------------------|
// | mantissa  | 10 bits | 23 bits | 52 bits |
// | exponent  | 5 bits  | 8 bits  | 11 bits |
// | sign      | 1 bit   | 1 bit   | 1 bit   |
// | exp bias  | 15      | 127     | 1023    |
// |-----------------------------------------|

// Converts a IEEE binary32 into a binary16. Rounds to nearest with ties to zero
uint16_t Binary32BitsToBinary16Bits(uint32_t f_bits) {
  // Sign mask for output binary16
  const uint16_t h_sign = uint16_t((f_bits >> 16) & 0x8000);

  // Exponent mask for input binary32
  const uint32_t f_exp = f_bits & 0x7f800000u;
  // Exponents as signed pre-shifted values for convenience. Here, we need to re-bias the
  // binary32 exponent for a binary16. If, after re-biasing, the binary16 exponent falls
  // outside of the range [1,30] then we need to handle the under/overflow case specially.
  const int16_t f_biased_exp = int16_t(f_exp >> 23);
  const int16_t unbiased_exp = f_biased_exp - 127;
  const int16_t h_biased_exp = unbiased_exp + 15;

  // Mantissa mask for input binary32
  const uint32_t f_mant = f_bits & 0x007fffffu;

  // Handle exponent overflow, NaN, and +/-Inf
  if (h_biased_exp >= 0x1f) {
    // The binary32 is a NaN representation
    if (f_biased_exp == 0xff && f_mant != 0) {
      uint16_t h_mant = uint16_t(f_mant >> 13);
      // If the mantissa bit(s) indicating NaN were shifted out, add one back. Otherwise,
      // the result would be infinity.
      if (h_mant == 0) {
        h_mant = 0x1;
      }
      return uint16_t(h_sign | 0x7c00u | h_mant);
    }

    // Clamp to +/-infinity
    return uint16_t(h_sign | 0x7c00u);
  }

  // Handle exponent underflow, subnormals, and +/-0
  if (h_biased_exp <= 0) {
    // If the underflow exceeds the number of bits in a binary16 mantissa (10) then we
    // can't round, so just clamp to 0. Note that this also weeds out any binary32 values
    // that are subnormal - including +/-0;
    if (h_biased_exp < -10) {
      return h_sign;
    }

    // Convert to a rounded subnormal value starting with the mantissa. Since the input
    // binary32 is known to be normal at this point, we need to prepend its implicit
    // leading bit - which also necessitates an additional right-shift.
    uint32_t rounded_mant = 0x800000u | f_mant;
    rounded_mant >>= (1 - h_biased_exp);

    // Here, we implement rounding to nearest (with ties to even)
    //
    // By now, our new mantissa has two conceptual ranges:
    //  - The lower 13 bits, which will be shifted out
    //  - The upper 10 bits, which will become the binary16's mantissa
    //
    // We define a "rounding bit", which is the most significant bit to be dropped
    // (0x1000). "Rounding to nearest" basically just means that we add 1 to the rounding
    // bit. If it's set, then the bit will cascade upwards into the 10-bit mantissa (and
    // potentially the exponent).
    //
    // The only time where we may NOT do this is when a "tie" occurs - i.e. when the
    // rounding bit is set but all of the lower bits are 0. In that case, we don't add 1
    // if the retained mantissa is "even" (its least significant bit is 0).
    if ((rounded_mant & 0x3fffu) != 0x1000u || (f_mant & 0x7ffu) != 0) {
      rounded_mant += 0x1000u;
    }

    const uint16_t h_mant = uint16_t(rounded_mant >> 13);
    return h_sign + h_mant;
  }

  const uint16_t h_exp = uint16_t(h_biased_exp) << 10;

  // See comment on rounding behavior above
  uint32_t rounded_mant = f_mant;
  if ((rounded_mant & 0x3fffu) != 0x1000u) {
    rounded_mant += 0x1000u;
  }

  const uint16_t h_mant = uint16_t(rounded_mant >> 13);
  // Note that we ADD (rather than OR) the components because we want the carryover bit
  // from rounding the mantissa to cascade through the exponent (it shouldn't affect the
  // sign bit though).
  return h_sign + h_exp + h_mant;
}

// Converts a IEEE binary16 into a binary32
uint32_t Binary16BitsToBinary32Bits(uint16_t h_bits) {
  // Sign mask for output binary32
  const uint32_t f_sign = uint32_t(h_bits & 0x8000u) << 16;

  // Exponent mask for input binary16
  const uint16_t h_exp = h_bits & 0x7c00;
  // Mantissa mask for input binary16
  const uint16_t h_mant = h_bits & 0x3ffu;

  switch (h_exp) {
    // Handle Inf and NaN
    case 0x7c00u:
      return f_sign | 0x7f800000u | (uint32_t(h_mant) << 13);
    // Handle zeros and subnormals
    case 0x0000u: {
      // Input is +/-0
      if (h_mant == 0) {
        return f_sign;
      }
      // Subnormal binary16 to normal binary32
      //
      // Start with an f32-biased exponent of 2^-15. We then decrement it until the most
      // significant set bit is left-shifted out - as it doesn't get explicitly stored in
      // normalized floating point values. Instead, its existence is implied by the new
      // exponent.
      uint32_t f_exp = 127 - 15;
      uint32_t f_mant = uint32_t(h_mant) << 1;
      while ((f_mant & 0x0400u) == 0) {
        --f_exp;
        f_mant <<= 1;
      }
      f_exp <<= 23;
      f_mant = (f_mant & 0x03ffu) << 13;
      return f_sign | f_exp | f_mant;
    } break;
    // Handle normals
    default:
      // Equivalent to adding (127 - 15) to the exponent and shifting everything by 13.
      return f_sign | ((uint32_t(h_bits & 0x7fffu) + 0x1c000u) << 13);
  }
}

}  // namespace

float Float16::ToFloat() const {
  const uint32_t f_bits = Binary16BitsToBinary32Bits(value_);
  return SafeCopy<float>(f_bits);
}

Float16 Float16::FromFloat(float f) {
  const uint32_t f_bits = SafeCopy<uint32_t>(f);
  return Float16{Binary32BitsToBinary16Bits(f_bits)};
}

std::ostream& operator<<(std::ostream& os, Float16 arg) { return (os << arg.ToFloat()); }

}  // namespace util
}  // namespace arrow

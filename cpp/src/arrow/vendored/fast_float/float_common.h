#ifndef FASTFLOAT_FLOAT_COMMON_H
#define FASTFLOAT_FLOAT_COMMON_H

#include <cfloat>
#include <cstdint>
#include <cassert>

#if (defined(__i386) || defined(__i386__) || defined(_M_IX86)   \
     || defined(__arm__)                                        \
     || defined(__MINGW32__))
#define FASTFLOAT_32BIT
#elif (defined(__x86_64) || defined(__x86_64__) || defined(_M_X64)   \
       || defined(__amd64) || defined(__aarch64__) || defined(_M_ARM64) \
       || defined(__MINGW64__)                                          \
       || defined(__s390x__)                                            \
       || (defined(__ppc64__) || defined(__PPC64__) || defined(__ppc64le__) || defined(__PPC64LE__)))
#define FASTFLOAT_64BIT
#else
#error Unknown platform
#endif

#if ((defined(_WIN32) || defined(_WIN64)) && !defined(__clang__))
#include <intrin.h>
#endif

#if defined(_MSC_VER) && !defined(__clang__)
#define FASTFLOAT_VISUAL_STUDIO 1
#endif

#ifdef _WIN32
#define FASTFLOAT_IS_BIG_ENDIAN 0
#else
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <machine/endian.h>
#else
#include <endian.h>
#endif
#
#ifndef __BYTE_ORDER__
// safe choice
#define FASTFLOAT_IS_BIG_ENDIAN 0
#endif
#
#ifndef __ORDER_LITTLE_ENDIAN__
// safe choice
#define FASTFLOAT_IS_BIG_ENDIAN 0
#endif
#
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define FASTFLOAT_IS_BIG_ENDIAN 0
#else
#define FASTFLOAT_IS_BIG_ENDIAN 1
#endif
#endif

#ifdef FASTFLOAT_VISUAL_STUDIO
#define fastfloat_really_inline __forceinline
#else
#define fastfloat_really_inline inline __attribute__((always_inline))
#endif

namespace arrow_vendored {
namespace fast_float {

// Compares two ASCII strings in a case insensitive manner.
inline bool fastfloat_strncasecmp(const char *input1, const char *input2,
                                  size_t length) {
  char running_diff{0};
  for (size_t i = 0; i < length; i++) {
    running_diff |= (input1[i] ^ input2[i]);
  }
  return (running_diff == 0) || (running_diff == 32);
}

#ifndef FLT_EVAL_METHOD
#error "FLT_EVAL_METHOD should be defined, please include cfloat."
#endif

inline bool is_space(uint8_t c) {
  static const bool table[] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  return table[c];
}

namespace {
constexpr uint32_t max_digits = 768;
constexpr uint32_t max_digit_without_overflow = 19;
constexpr int32_t decimal_point_range = 2047;
} // namespace

struct value128 {
  uint64_t low;
  uint64_t high;
  value128(uint64_t _low, uint64_t _high) : low(_low), high(_high) {}
  value128() : low(0), high(0) {}
};

/* result might be undefined when input_num is zero */
fastfloat_really_inline int leading_zeroes(uint64_t input_num) {
  assert(input_num > 0);
#ifdef FASTFLOAT_VISUAL_STUDIO
  #if defined(_M_X64) || defined(_M_ARM64)
  unsigned long leading_zero = 0;
  // Search the mask data from most significant bit (MSB)
  // to least significant bit (LSB) for a set bit (1).
  _BitScanReverse64(&leading_zero, input_num);
  return (int)(63 - leading_zero);
  #else
  int last_bit = 0;
  if(input_num & uint64_t(0xffffffff00000000)) input_num >>= 32, last_bit |= 32;
  if(input_num & uint64_t(        0xffff0000)) input_num >>= 16, last_bit |= 16;
  if(input_num & uint64_t(            0xff00)) input_num >>=  8, last_bit |=  8;
  if(input_num & uint64_t(              0xf0)) input_num >>=  4, last_bit |=  4;
  if(input_num & uint64_t(               0xc)) input_num >>=  2, last_bit |=  2;
  if(input_num & uint64_t(               0x2)) input_num >>=  1, last_bit |=  1;
  return 63 - last_bit;
  #endif
#else
  return __builtin_clzll(input_num);
#endif
}

#ifdef FASTFLOAT_32BIT

#if (!defined(_WIN32)) || defined(__MINGW32__)
// slow emulation routine for 32-bit
fastfloat_really_inline uint64_t __emulu(uint32_t x, uint32_t y) {
    return x * (uint64_t)y;
}
#endif

// slow emulation routine for 32-bit
#if !defined(__MINGW64__)
fastfloat_really_inline uint64_t _umul128(uint64_t ab, uint64_t cd,
                                          uint64_t *hi) {
  uint64_t ad = __emulu((uint32_t)(ab >> 32), (uint32_t)cd);
  uint64_t bd = __emulu((uint32_t)ab, (uint32_t)cd);
  uint64_t adbc = ad + __emulu((uint32_t)ab, (uint32_t)(cd >> 32));
  uint64_t adbc_carry = !!(adbc < ad);
  uint64_t lo = bd + (adbc << 32);
  *hi = __emulu((uint32_t)(ab >> 32), (uint32_t)(cd >> 32)) + (adbc >> 32) +
        (adbc_carry << 32) + !!(lo < bd);
  return lo;
}
#endif // !__MINGW64__

#endif // FASTFLOAT_32BIT


// compute 64-bit a*b
fastfloat_really_inline value128 full_multiplication(uint64_t a,
                                                     uint64_t b) {
  value128 answer;
#ifdef _M_ARM64
  // ARM64 has native support for 64-bit multiplications, no need to emulate
  answer.high = __umulh(a, b);
  answer.low = a * b;
#elif defined(FASTFLOAT_32BIT) || (defined(_WIN64))
  answer.low = _umul128(a, b, &answer.high); // _umul128 not available on ARM64
#elif defined(FASTFLOAT_64BIT)
  __uint128_t r = ((__uint128_t)a) * b;
  answer.low = uint64_t(r);
  answer.high = uint64_t(r >> 64);
#else
  #error Not implemented
#endif
  return answer;
}


struct adjusted_mantissa {
  uint64_t mantissa{0};
  int power2{0}; // a negative value indicate an invalid result
  adjusted_mantissa() = default;
  // bool operator==(const adjusted_mantissa &o) const = default;
  bool operator==(const adjusted_mantissa &o) const {
    return mantissa == o.mantissa && power2 == o.power2;
  }
};

struct decimal {
  uint32_t num_digits{0};
  int32_t decimal_point{0};
  bool negative{false};
  bool truncated{false};
  uint8_t digits[max_digits];
  decimal() = default;
  // Copies are not allowed since this is a fat object.
  decimal(const decimal &) = delete;
  // Copies are not allowed since this is a fat object.
  decimal &operator=(const decimal &) = delete;
  // Moves are allowed:
  decimal(decimal &&) = default;
  decimal &operator=(decimal &&other) = default;
  // Generates a mantissa by truncating to 19 digits.
  // This function should be reasonably fast.
  // Note that the user is responsible to ensure that digits are
  // initialized to zero when there are fewer than 19.
  inline uint64_t to_truncated_mantissa() {
#if FASTFLOAT_IS_BIG_ENDIAN == 1
    uint64_t mantissa = 0;
    for (uint32_t i = 0; i < max_digit_without_overflow;
         i++) {
      mantissa = mantissa * 10 + digits[i]; // can be accelerated
    }
    return mantissa;
#else
    uint64_t val;
    // 8 first digits
    ::memcpy(&val, digits, sizeof(uint64_t));
    val = val * 2561 >> 8;
    val = (val & 0x00FF00FF00FF00FF) * 6553601 >> 16;
    uint64_t mantissa =
        uint32_t((val & 0x0000FFFF0000FFFF) * 42949672960001 >> 32);
    // 8 more digits for a total of 16
    ::memcpy(&val, digits + sizeof(uint64_t), sizeof(uint64_t));
    val = val * 2561 >> 8;
    val = (val & 0x00FF00FF00FF00FF) * 6553601 >> 16;
    uint32_t eight_digits_value =
        uint32_t((val & 0x0000FFFF0000FFFF) * 42949672960001 >> 32);
    mantissa = 100000000 * mantissa + eight_digits_value;
    for (uint32_t i = 2 * sizeof(uint64_t); i < max_digit_without_overflow;
         i++) {
      mantissa = mantissa * 10 + digits[i]; // can be accelerated
    }
    return mantissa;
#endif
  }
  // Generate san exponent matching to_truncated_mantissa()
  inline int32_t to_truncated_exponent() {
    return decimal_point - int32_t(max_digit_without_overflow);
  }
};

constexpr static double powers_of_ten_double[] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,  1e10, 1e11,
    1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};
constexpr static float powers_of_ten_float[] = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5,
                                                1e6, 1e7, 1e8, 1e9, 1e10};

template <typename T> struct binary_format {
  static constexpr int mantissa_explicit_bits();
  static constexpr int minimum_exponent();
  static constexpr int infinite_power();
  static constexpr int sign_index();
  static constexpr int min_exponent_fast_path();
  static constexpr int max_exponent_fast_path();
  static constexpr int max_exponent_round_to_even();
  static constexpr int min_exponent_round_to_even();
  static constexpr uint64_t max_mantissa_fast_path();
  static constexpr T exact_power_of_ten(int64_t power);
};

template <> constexpr int binary_format<double>::mantissa_explicit_bits() {
  return 52;
}
template <> constexpr int binary_format<float>::mantissa_explicit_bits() {
  return 23;
}

template <> constexpr int binary_format<double>::max_exponent_round_to_even() {
  return 23;
}

template <> constexpr int binary_format<float>::max_exponent_round_to_even() {
  return 10;
}

template <> constexpr int binary_format<double>::min_exponent_round_to_even() {
  return -4;
}

template <> constexpr int binary_format<float>::min_exponent_round_to_even() {
  return -17;
}

template <> constexpr int binary_format<double>::minimum_exponent() {
  return -1023;
}
template <> constexpr int binary_format<float>::minimum_exponent() {
  return -127;
}

template <> constexpr int binary_format<double>::infinite_power() {
  return 0x7FF;
}
template <> constexpr int binary_format<float>::infinite_power() {
  return 0xFF;
}

template <> constexpr int binary_format<double>::sign_index() { return 63; }
template <> constexpr int binary_format<float>::sign_index() { return 31; }

template <> constexpr int binary_format<double>::min_exponent_fast_path() {
#if (FLT_EVAL_METHOD != 1) && (FLT_EVAL_METHOD != 0)
  return 0;
#else
  return -22;
#endif
}
template <> constexpr int binary_format<float>::min_exponent_fast_path() {
#if (FLT_EVAL_METHOD != 1) && (FLT_EVAL_METHOD != 0)
  return 0;
#else
  return -10;
#endif
}

template <> constexpr int binary_format<double>::max_exponent_fast_path() {
  return 22;
}
template <> constexpr int binary_format<float>::max_exponent_fast_path() {
  return 10;
}

template <> constexpr uint64_t binary_format<double>::max_mantissa_fast_path() {
  return uint64_t(2) << mantissa_explicit_bits();
}
template <> constexpr uint64_t binary_format<float>::max_mantissa_fast_path() {
  return uint64_t(2) << mantissa_explicit_bits();
}

template <>
constexpr double binary_format<double>::exact_power_of_ten(int64_t power) {
  return powers_of_ten_double[power];
}
template <>
constexpr float binary_format<float>::exact_power_of_ten(int64_t power) {

  return powers_of_ten_float[power];
}

// for convenience:
#include <ostream>
inline std::ostream &operator<<(std::ostream &out, const fast_float::decimal &d) {
  out << "0.";
  for (size_t i = 0; i < d.num_digits; i++) {
    out << int32_t(d.digits[i]);
  }
  out << " * 10 ** " << d.decimal_point;
  return out;
}

} // namespace fast_float
} // namespace arrow_vendored

#endif

#ifndef FASTFLOAT_FLOAT_COMMON_H
#define FASTFLOAT_FLOAT_COMMON_H

#include <cfloat>
#include <cstdint>
#include <cassert>
#include <cstring>
#include <type_traits>

#ifdef __has_include
#if __has_include(<version>)
#include <version>
#endif
#endif

#if __cpp_lib_bit_cast >= 201806L
#include <bit>
#define FASTFLOAT_HAS_BIT_CAST 1
#else
#define FASTFLOAT_HAS_BIT_CAST 0
#endif

#if __cpp_lib_is_constant_evaluated >= 201811L
#define FASTFLOAT_HAS_IS_CONSTANT_EVALUATED 1
#else
#define FASTFLOAT_HAS_IS_CONSTANT_EVALUATED 0
#endif

#if (defined(__x86_64) || defined(__x86_64__) || defined(_M_X64)   \
       || defined(__amd64) || defined(__aarch64__) || defined(_M_ARM64) \
       || defined(__MINGW64__)                                          \
       || defined(__s390x__)                                            \
       || (defined(__ppc64__) || defined(__PPC64__) || defined(__ppc64le__) || defined(__PPC64LE__)) )
#define FASTFLOAT_64BIT 1
#elif (defined(__i386) || defined(__i386__) || defined(_M_IX86)   \
     || defined(__arm__) || defined(_M_ARM) || defined(__ppc__)   \
     || defined(__MINGW32__) || defined(__EMSCRIPTEN__))
#define FASTFLOAT_32BIT 1
#else
  // Need to check incrementally, since SIZE_MAX is a size_t, avoid overflow.
  // We can never tell the register width, but the SIZE_MAX is a good approximation.
  // UINTPTR_MAX and INTPTR_MAX are optional, so avoid them for max portability.
  #if SIZE_MAX == 0xffff
    #error Unknown platform (16-bit, unsupported)
  #elif SIZE_MAX == 0xffffffff
    #define FASTFLOAT_32BIT 1
  #elif SIZE_MAX == 0xffffffffffffffff
    #define FASTFLOAT_64BIT 1
  #else
    #error Unknown platform (not 32-bit, not 64-bit?)
  #endif
#endif

#if ((defined(_WIN32) || defined(_WIN64)) && !defined(__clang__))
#include <intrin.h>
#endif

#if defined(_MSC_VER) && !defined(__clang__)
#define FASTFLOAT_VISUAL_STUDIO 1
#endif

#if defined __BYTE_ORDER__ && defined __ORDER_BIG_ENDIAN__
#define FASTFLOAT_IS_BIG_ENDIAN (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#elif defined _WIN32
#define FASTFLOAT_IS_BIG_ENDIAN 0
#else
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <machine/endian.h>
#elif defined(sun) || defined(__sun)
#include <sys/byteorder.h>
#else
#ifdef __has_include
#if __has_include(<endian.h>)
#include <endian.h>
#endif //__has_include(<endian.h>)
#endif //__has_include
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

#ifndef FASTFLOAT_ASSERT
#define FASTFLOAT_ASSERT(x)  { ((void)(x)); }
#endif

#ifndef FASTFLOAT_DEBUG_ASSERT
#define FASTFLOAT_DEBUG_ASSERT(x) { ((void)(x)); }
#endif

// rust style `try!()` macro, or `?` operator
#define FASTFLOAT_TRY(x) { if (!(x)) return false; }

// Testing for https://wg21.link/N3652, adopted in C++14
#if __cpp_constexpr >= 201304
#define FASTFLOAT_CONSTEXPR14 constexpr
#else
#define FASTFLOAT_CONSTEXPR14
#endif

// Testing for relevant C++20 constexpr library features
#if FASTFLOAT_HAS_IS_CONSTANT_EVALUATED \
    && FASTFLOAT_HAS_BIT_CAST \
    && __cpp_lib_constexpr_algorithms >= 201806L /*For std::copy and std::fill*/
#define FASTFLOAT_CONSTEXPR20 constexpr
#else
#define FASTFLOAT_CONSTEXPR20
#endif

namespace arrow_vendored {
namespace fast_float {

fastfloat_really_inline constexpr bool cpp20_and_in_constexpr() {
#if FASTFLOAT_HAS_IS_CONSTANT_EVALUATED
  return std::is_constant_evaluated();
#else
  return false;
#endif
}

// Compares two ASCII strings in a case insensitive manner.
inline FASTFLOAT_CONSTEXPR14 bool
fastfloat_strncasecmp(const char *input1, const char *input2, size_t length) {
  char running_diff{0};
  for (size_t i = 0; i < length; i++) {
    running_diff |= (input1[i] ^ input2[i]);
  }
  return (running_diff == 0) || (running_diff == 32);
}

#ifndef FLT_EVAL_METHOD
#error "FLT_EVAL_METHOD should be defined, please include cfloat."
#endif

// a pointer and a length to a contiguous block of memory
template <typename T>
struct span {
  const T* ptr;
  size_t length;
  constexpr span(const T* _ptr, size_t _length) : ptr(_ptr), length(_length) {}
  constexpr span() : ptr(nullptr), length(0) {}

  constexpr size_t len() const noexcept {
    return length;
  }

  FASTFLOAT_CONSTEXPR14 const T& operator[](size_t index) const noexcept {
    FASTFLOAT_DEBUG_ASSERT(index < length);
    return ptr[index];
  }
};

struct value128 {
  uint64_t low;
  uint64_t high;
  constexpr value128(uint64_t _low, uint64_t _high) : low(_low), high(_high) {}
  constexpr value128() : low(0), high(0) {}
};

/* Helper C++11 constexpr generic implementation of leading_zeroes */
fastfloat_really_inline constexpr
int leading_zeroes_generic(uint64_t input_num, int last_bit = 0) {
  return (
    ((input_num & uint64_t(0xffffffff00000000)) && (input_num >>= 32, last_bit |= 32)),
    ((input_num & uint64_t(        0xffff0000)) && (input_num >>= 16, last_bit |= 16)),
    ((input_num & uint64_t(            0xff00)) && (input_num >>=  8, last_bit |=  8)),
    ((input_num & uint64_t(              0xf0)) && (input_num >>=  4, last_bit |=  4)),
    ((input_num & uint64_t(               0xc)) && (input_num >>=  2, last_bit |=  2)),
    ((input_num & uint64_t(               0x2)) && (input_num >>=  1, last_bit |=  1)),
    63 - last_bit
  );
}

/* result might be undefined when input_num is zero */
fastfloat_really_inline FASTFLOAT_CONSTEXPR20
int leading_zeroes(uint64_t input_num) {
  assert(input_num > 0);
  if (cpp20_and_in_constexpr()) {
    return leading_zeroes_generic(input_num);
  }
#ifdef FASTFLOAT_VISUAL_STUDIO
  #if defined(_M_X64) || defined(_M_ARM64)
  unsigned long leading_zero = 0;
  // Search the mask data from most significant bit (MSB)
  // to least significant bit (LSB) for a set bit (1).
  _BitScanReverse64(&leading_zero, input_num);
  return (int)(63 - leading_zero);
  #else
  return leading_zeroes_generic(input_num);
  #endif
#else
  return __builtin_clzll(input_num);
#endif
}

// slow emulation routine for 32-bit
fastfloat_really_inline constexpr uint64_t emulu(uint32_t x, uint32_t y) {
    return x * (uint64_t)y;
}

fastfloat_really_inline FASTFLOAT_CONSTEXPR14
uint64_t umul128_generic(uint64_t ab, uint64_t cd, uint64_t *hi) {
  uint64_t ad = emulu((uint32_t)(ab >> 32), (uint32_t)cd);
  uint64_t bd = emulu((uint32_t)ab, (uint32_t)cd);
  uint64_t adbc = ad + emulu((uint32_t)ab, (uint32_t)(cd >> 32));
  uint64_t adbc_carry = !!(adbc < ad);
  uint64_t lo = bd + (adbc << 32);
  *hi = emulu((uint32_t)(ab >> 32), (uint32_t)(cd >> 32)) + (adbc >> 32) +
        (adbc_carry << 32) + !!(lo < bd);
  return lo;
}

#ifdef FASTFLOAT_32BIT

// slow emulation routine for 32-bit
#if !defined(__MINGW64__)
fastfloat_really_inline FASTFLOAT_CONSTEXPR14
uint64_t _umul128(uint64_t ab, uint64_t cd, uint64_t *hi) {
  return umul128_generic(ab, cd, hi);
}
#endif // !__MINGW64__

#endif // FASTFLOAT_32BIT


// compute 64-bit a*b
fastfloat_really_inline FASTFLOAT_CONSTEXPR20
value128 full_multiplication(uint64_t a, uint64_t b) {
  if (cpp20_and_in_constexpr()) {
    value128 answer;
    answer.low = umul128_generic(a, b, &answer.high);
    return answer;
  }
  value128 answer;
#if defined(_M_ARM64) && !defined(__MINGW32__)
  // ARM64 has native support for 64-bit multiplications, no need to emulate
  // But MinGW on ARM64 doesn't have native support for 64-bit multiplications
  answer.high = __umulh(a, b);
  answer.low = a * b;
#elif defined(FASTFLOAT_32BIT) || (defined(_WIN64) && !defined(__clang__))
  answer.low = _umul128(a, b, &answer.high); // _umul128 not available on ARM64
#elif defined(FASTFLOAT_64BIT)
  __uint128_t r = ((__uint128_t)a) * b;
  answer.low = uint64_t(r);
  answer.high = uint64_t(r >> 64);
#else
  answer.low = umul128_generic(a, b, &answer.high);
#endif
  return answer;
}

struct adjusted_mantissa {
  uint64_t mantissa{0};
  int32_t power2{0}; // a negative value indicates an invalid result
  adjusted_mantissa() = default;
  constexpr bool operator==(const adjusted_mantissa &o) const {
    return mantissa == o.mantissa && power2 == o.power2;
  }
  constexpr bool operator!=(const adjusted_mantissa &o) const {
    return mantissa != o.mantissa || power2 != o.power2;
  }
};

// Bias so we can get the real exponent with an invalid adjusted_mantissa.
constexpr static int32_t invalid_am_bias = -0x8000;

constexpr static double powers_of_ten_double[] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,  1e10, 1e11,
    1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};
constexpr static float powers_of_ten_float[] = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f,
                                                1e6f, 1e7f, 1e8f, 1e9f, 1e10f};
// used for max_mantissa_double and max_mantissa_float
constexpr uint64_t constant_55555 = 5 * 5 * 5 * 5 * 5;
// Largest integer value v so that (5**index * v) <= 1<<53.
// 0x10000000000000 == 1 << 53
constexpr static uint64_t max_mantissa_double[] = {
      0x10000000000000,
      0x10000000000000 / 5,
      0x10000000000000 / (5 * 5),
      0x10000000000000 / (5 * 5 * 5),
      0x10000000000000 / (5 * 5 * 5 * 5),
      0x10000000000000 / (constant_55555),
      0x10000000000000 / (constant_55555 * 5),
      0x10000000000000 / (constant_55555 * 5 * 5),
      0x10000000000000 / (constant_55555 * 5 * 5 * 5),
      0x10000000000000 / (constant_55555 * 5 * 5 * 5 * 5),
      0x10000000000000 / (constant_55555 * constant_55555),
      0x10000000000000 / (constant_55555 * constant_55555 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * 5 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * 5 * 5 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * 5 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * 5 * 5 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * 5 * 5 * 5 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * constant_55555),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * constant_55555 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * constant_55555 * 5 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * constant_55555 * 5 * 5 * 5),
      0x10000000000000 / (constant_55555 * constant_55555 * constant_55555 * constant_55555 * 5 * 5 * 5 * 5)};
  // Largest integer value v so that (5**index * v) <= 1<<24.
  // 0x1000000 == 1<<24
  constexpr static uint64_t max_mantissa_float[] = {
      0x1000000,
      0x1000000 / 5,
      0x1000000 / (5 * 5),
      0x1000000 / (5 * 5 * 5),
      0x1000000 / (5 * 5 * 5 * 5),
      0x1000000 / (constant_55555),
      0x1000000 / (constant_55555 * 5),
      0x1000000 / (constant_55555 * 5 * 5),
      0x1000000 / (constant_55555 * 5 * 5 * 5),
      0x1000000 / (constant_55555 * 5 * 5 * 5 * 5),
      0x1000000 / (constant_55555 * constant_55555),
      0x1000000 / (constant_55555 * constant_55555 * 5)};

template <typename T> struct binary_format {
  using equiv_uint = typename std::conditional<sizeof(T) == 4, uint32_t, uint64_t>::type;

  static inline constexpr int mantissa_explicit_bits();
  static inline constexpr int minimum_exponent();
  static inline constexpr int infinite_power();
  static inline constexpr int sign_index();
  static inline constexpr int min_exponent_fast_path(); // used when fegetround() == FE_TONEAREST
  static inline constexpr int max_exponent_fast_path();
  static inline constexpr int max_exponent_round_to_even();
  static inline constexpr int min_exponent_round_to_even();
  static inline constexpr uint64_t max_mantissa_fast_path(int64_t power);
  static inline constexpr uint64_t max_mantissa_fast_path(); // used when fegetround() == FE_TONEAREST
  static inline constexpr int largest_power_of_ten();
  static inline constexpr int smallest_power_of_ten();
  static inline constexpr T exact_power_of_ten(int64_t power);
  static inline constexpr size_t max_digits();
  static inline constexpr equiv_uint exponent_mask();
  static inline constexpr equiv_uint mantissa_mask();
  static inline constexpr equiv_uint hidden_bit_mask();
};

template <> inline constexpr int binary_format<double>::min_exponent_fast_path() {
#if (FLT_EVAL_METHOD != 1) && (FLT_EVAL_METHOD != 0)
  return 0;
#else
  return -22;
#endif
}

template <> inline constexpr int binary_format<float>::min_exponent_fast_path() {
#if (FLT_EVAL_METHOD != 1) && (FLT_EVAL_METHOD != 0)
  return 0;
#else
  return -10;
#endif
}

template <> inline constexpr int binary_format<double>::mantissa_explicit_bits() {
  return 52;
}
template <> inline constexpr int binary_format<float>::mantissa_explicit_bits() {
  return 23;
}

template <> inline constexpr int binary_format<double>::max_exponent_round_to_even() {
  return 23;
}

template <> inline constexpr int binary_format<float>::max_exponent_round_to_even() {
  return 10;
}

template <> inline constexpr int binary_format<double>::min_exponent_round_to_even() {
  return -4;
}

template <> inline constexpr int binary_format<float>::min_exponent_round_to_even() {
  return -17;
}

template <> inline constexpr int binary_format<double>::minimum_exponent() {
  return -1023;
}
template <> inline constexpr int binary_format<float>::minimum_exponent() {
  return -127;
}

template <> inline constexpr int binary_format<double>::infinite_power() {
  return 0x7FF;
}
template <> inline constexpr int binary_format<float>::infinite_power() {
  return 0xFF;
}

template <> inline constexpr int binary_format<double>::sign_index() { return 63; }
template <> inline constexpr int binary_format<float>::sign_index() { return 31; }

template <> inline constexpr int binary_format<double>::max_exponent_fast_path() {
  return 22;
}
template <> inline constexpr int binary_format<float>::max_exponent_fast_path() {
  return 10;
}
template <> inline constexpr uint64_t binary_format<double>::max_mantissa_fast_path() {
  return uint64_t(2) << mantissa_explicit_bits();
}
template <> inline constexpr uint64_t binary_format<double>::max_mantissa_fast_path(int64_t power) {
  // caller is responsible to ensure that
  // power >= 0 && power <= 22
  //
  return max_mantissa_double[power];
}
template <> inline constexpr uint64_t binary_format<float>::max_mantissa_fast_path() {
  return uint64_t(2) << mantissa_explicit_bits();
}
template <> inline constexpr uint64_t binary_format<float>::max_mantissa_fast_path(int64_t power) {
  // caller is responsible to ensure that
  // power >= 0 && power <= 10
  //
  return max_mantissa_float[power];
}

template <>
inline constexpr double binary_format<double>::exact_power_of_ten(int64_t power) {
  return powers_of_ten_double[power];
}
template <>
inline constexpr float binary_format<float>::exact_power_of_ten(int64_t power) {

  return powers_of_ten_float[power];
}


template <>
inline constexpr int binary_format<double>::largest_power_of_ten() {
  return 308;
}
template <>
inline constexpr int binary_format<float>::largest_power_of_ten() {
  return 38;
}

template <>
inline constexpr int binary_format<double>::smallest_power_of_ten() {
  return -342;
}
template <>
inline constexpr int binary_format<float>::smallest_power_of_ten() {
  return -65;
}

template <> inline constexpr size_t binary_format<double>::max_digits() {
  return 769;
}
template <> inline constexpr size_t binary_format<float>::max_digits() {
  return 114;
}

template <> inline constexpr binary_format<float>::equiv_uint
    binary_format<float>::exponent_mask() {
  return 0x7F800000;
}
template <> inline constexpr binary_format<double>::equiv_uint
    binary_format<double>::exponent_mask() {
  return 0x7FF0000000000000;
}

template <> inline constexpr binary_format<float>::equiv_uint
    binary_format<float>::mantissa_mask() {
  return 0x007FFFFF;
}
template <> inline constexpr binary_format<double>::equiv_uint
    binary_format<double>::mantissa_mask() {
  return 0x000FFFFFFFFFFFFF;
}

template <> inline constexpr binary_format<float>::equiv_uint
    binary_format<float>::hidden_bit_mask() {
  return 0x00800000;
}
template <> inline constexpr binary_format<double>::equiv_uint
    binary_format<double>::hidden_bit_mask() {
  return 0x0010000000000000;
}

template<typename T>
fastfloat_really_inline FASTFLOAT_CONSTEXPR20
void to_float(bool negative, adjusted_mantissa am, T &value) {
  using uint = typename binary_format<T>::equiv_uint;
  uint word = (uint)am.mantissa;
  word |= uint(am.power2) << binary_format<T>::mantissa_explicit_bits();
  word |= uint(negative) << binary_format<T>::sign_index();
#if FASTFLOAT_HAS_BIT_CAST
  value = std::bit_cast<T>(word);
#else
  ::memcpy(&value, &word, sizeof(T));
#endif
}

#if FASTFLOAT_SKIP_WHITE_SPACE // disabled by default
template <typename = void>
struct space_lut {
  static constexpr bool value[] = {
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
};

template <typename T>
constexpr bool space_lut<T>::value[];

inline constexpr bool is_space(uint8_t c) { return space_lut<>::value[c]; }
#endif
} // namespace fast_float
} // namespace arrow_vendored

#endif

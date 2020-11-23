#ifndef FASTFLOAT_ASCII_NUMBER_H
#define FASTFLOAT_ASCII_NUMBER_H

#include <cstdio>
#include <cctype>
#include <cstdint>
#include <cstring>

#include "float_common.h"

namespace arrow_vendored {
namespace fast_float {

// Next function can be micro-optimized, but compilers are entirely
// able to optimize it well.
fastfloat_really_inline bool is_integer(char c)  noexcept  { return c >= '0' && c <= '9'; }


// credit: https://johnnylee-sde.github.io/Fast-numeric-string-to-int/
fastfloat_really_inline uint32_t parse_eight_digits_unrolled(const char *chars)  noexcept  {
  uint64_t val;
  ::memcpy(&val, chars, sizeof(uint64_t));
  val = (val & 0x0F0F0F0F0F0F0F0F) * 2561 >> 8;
  val = (val & 0x00FF00FF00FF00FF) * 6553601 >> 16;
  return uint32_t((val & 0x0000FFFF0000FFFF) * 42949672960001 >> 32);
}

fastfloat_really_inline bool is_made_of_eight_digits_fast(uint64_t val)  noexcept  {
  return (((val & 0xF0F0F0F0F0F0F0F0) |
           (((val + 0x0606060606060606) & 0xF0F0F0F0F0F0F0F0) >> 4)) ==
          0x3333333333333333);
}


fastfloat_really_inline bool is_made_of_eight_digits_fast(const char *chars)  noexcept  {
  uint64_t val;
  ::memcpy(&val, chars, 8);
  return is_made_of_eight_digits_fast(val);
}

struct parsed_number_string {
  int64_t exponent;
  uint64_t mantissa;
  const char *lastmatch;
  bool negative;
  bool valid;
  bool too_many_digits;
};


// Assuming that you use no more than 19 digits, this will
// parse an ASCII string.
fastfloat_really_inline
parsed_number_string parse_number_string(const char *p, const char *pend, chars_format fmt) noexcept {
  parsed_number_string answer;
  answer.valid = false;
  answer.negative = (*p == '-');
  if ((*p == '-') || (*p == '+')) {
    ++p;
    if (p == pend) {
      return answer;
    }
    if (!is_integer(*p) && (*p != '.')) { // a  sign must be followed by an integer or the dot
      return answer;
    }
  }
  const char *const start_digits = p;

  uint64_t i = 0; // an unsigned int avoids signed overflows (which are bad)

  while ((p != pend) && is_integer(*p)) {
    // a multiplication by 10 is cheaper than an arbitrary integer
    // multiplication
    i = 10 * i +
        uint64_t(*p - '0'); // might overflow, we will handle the overflow later
    ++p;
  }
  int64_t exponent = 0;
  if ((p != pend) && (*p == '.')) {
    ++p;
    const char *first_after_period = p;
#if FASTFLOAT_IS_BIG_ENDIAN == 0
    // Fast approach only tested under little endian systems
    if ((p + 8 <= pend) && is_made_of_eight_digits_fast(p)) {
      i = i * 100000000 + parse_eight_digits_unrolled(p); // in rare cases, this will overflow, but that's ok
      p += 8;
      if ((p + 8 <= pend) && is_made_of_eight_digits_fast(p)) {
        i = i * 100000000 + parse_eight_digits_unrolled(p); // in rare cases, this will overflow, but that's ok
        p += 8;
      }
    }
#endif
    while ((p != pend) && is_integer(*p)) {
      uint8_t digit = uint8_t(*p - '0');
      ++p;
      i = i * 10 + digit; // in rare cases, this will overflow, but that's ok
    }
    exponent = first_after_period - p;
  }
  // we must have encountered at least one integer!
  if ((start_digits == p) || ((start_digits == p - 1) && (*start_digits == '.') )) {
    return answer;
  }

  int32_t digit_count =
      int32_t(p - start_digits - 1); // used later to guard against overflows
 
  if ((fmt & chars_format::scientific) && (p != pend) && (('e' == *p) || ('E' == *p))) {
    const char * location_of_e = p;
    int64_t exp_number = 0;            // exponential part
    ++p;
    bool neg_exp = false;
    if ((p != pend) && ('-' == *p)) {
      neg_exp = true;
      ++p;
    } else if ((p != pend) && ('+' == *p)) {
      ++p;
    }
    if ((p == pend) || !is_integer(*p)) {
      if(!(fmt & chars_format::fixed)) {
        // We are in error.
        return answer;
      }
      // Otherwise, we will be ignoring the 'e'.
      p = location_of_e;
    } else {
      while ((p != pend) && is_integer(*p)) {
        uint8_t digit = uint8_t(*p - '0');
        if (exp_number < 0x10000) {
          exp_number = 10 * exp_number + digit;
        }
        ++p;
      }
      exponent += (neg_exp ? -exp_number : exp_number);
    }
  } else {
    // If it scientific and not fixed, we have to bail out.
    if((fmt & chars_format::scientific) && !(fmt & chars_format::fixed)) { return answer; }
  }
  answer.lastmatch = p;
  answer.valid = true;

  // If we frequently had to deal with long strings of digits,
  // we could extend our code by using a 128-bit integer instead
  // of a 64-bit integer. However, this is uncommon.
  if (((digit_count >= 19))) { // this is uncommon
    // It is possible that the integer had an overflow.
    // We have to handle the case where we have 0.0000somenumber.
    const char *start = start_digits;
    while (*start == '0' || (*start == '.')) {
      start++;
    }
    // we over-decrement by one when there is a decimal separator
    digit_count -= int(start - start_digits);
    if (digit_count >= 19) {
      answer.mantissa = 0xFFFFFFFFFFFFFFFF; // important: we don't want the mantissa to be used in a fast path uninitialized.
      answer.too_many_digits = true;
      return answer;
    }
  }
  answer.too_many_digits = false;
  answer.exponent = exponent;
  answer.mantissa = i;
  return answer;
}


// This should always succeed since it follows a call to parse_number_string
// This function could be optimized. In particular, we could stop after 19 digits
// and try to bail out. Furthermore, we should be able to recover the computed
// exponent from the pass in parse_number_string.
fastfloat_really_inline decimal parse_decimal(const char *p, const char *pend) noexcept {
  decimal answer;
  answer.num_digits = 0;
  answer.decimal_point = 0;
  answer.truncated = false;
  // any whitespace has been skipped.
  answer.negative = (*p == '-');
  if ((*p == '-') || (*p == '+')) {
    ++p;
  }
  // skip leading zeroes
  while ((p != pend) && (*p == '0')) {
    ++p;
  }
  while ((p != pend) && is_integer(*p)) {
    if (answer.num_digits < max_digits) {
      answer.digits[answer.num_digits] = uint8_t(*p - '0');
    }
    answer.num_digits++;
    ++p;
  }
  if ((p != pend) && (*p == '.')) {
    ++p;
    const char *first_after_period = p;
    // if we have not yet encountered a zero, we have to skip it as well
    if(answer.num_digits == 0) {
      // skip zeros
      while ((p != pend) && (*p == '0')) {
       ++p;
      }
    }
#if FASTFLOAT_IS_BIG_ENDIAN == 0
    // We expect that this loop will often take the bulk of the running time
    // because when a value has lots of digits, these digits often
    while ((p + 8 <= pend) && (answer.num_digits + 8 < max_digits)) {
      uint64_t val;
      ::memcpy(&val, p, sizeof(uint64_t));
      if(! is_made_of_eight_digits_fast(val)) { break; }
      // We have eight digits, process them in one go!
      val -= 0x3030303030303030;
      ::memcpy(answer.digits + answer.num_digits, &val, sizeof(uint64_t));
      answer.num_digits += 8;
      p += 8;
    }
#endif
    while ((p != pend) && is_integer(*p)) {
      if (answer.num_digits < max_digits) {
        answer.digits[answer.num_digits] = uint8_t(*p - '0');
      }
      answer.num_digits++;
      ++p;
    }
    answer.decimal_point = int32_t(first_after_period - p);
  }
  if ((p != pend) && (('e' == *p) || ('E' == *p))) {
    ++p;
    bool neg_exp = false;
    if ((p != pend) && ('-' == *p)) {
      neg_exp = true;
      ++p;
    } else if ((p != pend) && ('+' == *p)) {
      ++p;
    }
    int32_t exp_number = 0; // exponential part
    while ((p != pend) && is_integer(*p)) {
      uint8_t digit = uint8_t(*p - '0');
      if (exp_number < 0x10000) {
        exp_number = 10 * exp_number + digit;
      }    
      ++p;
    }
    answer.decimal_point += (neg_exp ? -exp_number : exp_number);
  }
  answer.decimal_point += int32_t(answer.num_digits);
  if(answer.num_digits > max_digits) {
    answer.truncated = true;
    answer.num_digits = max_digits;
  }
  // In very rare cases, we may have fewer than 19 digits, we want to be able to reliably
  // assume that all digits up to max_digit_without_overflow have been initialized.
  for(uint32_t i = answer.num_digits; i < max_digit_without_overflow; i++) { answer.digits[i] = 0; }

  return answer;
}
} // namespace fast_float
} // namespace arrow_vendored

#endif

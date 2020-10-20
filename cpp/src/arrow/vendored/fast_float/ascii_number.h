#ifndef FASTFLOAT_ASCII_NUMBER_H
#define FASTFLOAT_ASCII_NUMBER_H

#include <cstdio>
#include <cctype>
#include <cstdint>
#include <cstring>

#include "float_common.h"

namespace arrow_vendored {
namespace fast_float {

fastfloat_really_inline bool is_integer(char c)  noexcept  { return (c >= '0' && c <= '9'); }


// credit: https://johnnylee-sde.github.io/Fast-numeric-string-to-int/
fastfloat_really_inline uint32_t parse_eight_digits_unrolled(const char *chars)  noexcept  {
  uint64_t val;
  memcpy(&val, chars, sizeof(uint64_t));
  val = (val & 0x0F0F0F0F0F0F0F0F) * 2561 >> 8;
  val = (val & 0x00FF00FF00FF00FF) * 6553601 >> 16;
  return uint32_t((val & 0x0000FFFF0000FFFF) * 42949672960001 >> 32);
}

fastfloat_really_inline bool is_made_of_eight_digits_fast(const char *chars)  noexcept  {
  uint64_t val;
  memcpy(&val, chars, 8);
  return (((val & 0xF0F0F0F0F0F0F0F0) |
           (((val + 0x0606060606060606) & 0xF0F0F0F0F0F0F0F0) >> 4)) ==
          0x3333333333333333);
}


fastfloat_really_inline uint32_t parse_four_digits_unrolled(const char *chars)  noexcept  {
  uint32_t val;
  memcpy(&val, chars, sizeof(uint32_t));
  val = (val & 0x0F0F0F0F) * 2561 >> 8;
  return (val & 0x00FF00FF) * 6553601 >> 16;
}

fastfloat_really_inline bool is_made_of_four_digits_fast(const char *chars)  noexcept  {
  uint32_t val;
  memcpy(&val, chars, 4);
  return (((val & 0xF0F0F0F0) |
           (((val + 0x06060606) & 0xF0F0F0F0) >> 4)) ==
          0x33333333);
}

struct parsed_number_string {
  int64_t exponent;
  uint64_t mantissa;
  const char *lastmatch;
  bool negative;
  bool valid;
  bool too_many_digits;
};


// Assuming that you use no more than 17 digits, this will
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
        (*p - '0'); // might overflow, we will handle the overflow later
    ++p;
  }
  int64_t exponent = 0;
  if ((p != pend) && (*p == '.')) {
    ++p;
    const char *first_after_period = p;
    if ((p + 8 <= pend) && is_made_of_eight_digits_fast(p)) {
      i = i * 100000000 + parse_eight_digits_unrolled(p); // in rare cases, this will overflow, but that's ok
      p += 8;
      if ((p + 8 <= pend) && is_made_of_eight_digits_fast(p)) {
        i = i * 100000000 + parse_eight_digits_unrolled(p); // in rare cases, this will overflow, but that's ok
        p += 8;
      }
    }
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
  
  if ((p != pend) && (('e' == *p) || ('E' == *p))) {
    if((fmt & chars_format::fixed) && !(fmt & chars_format::scientific)) { return answer; } 
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
      return answer;
    }
    while ((p != pend) && is_integer(*p)) {
      uint8_t digit = uint8_t(*p - '0');
      if (exp_number < 0x10000) {
        exp_number = 10 * exp_number + digit;
      }
      ++p;
    }
    exponent += (neg_exp ? -exp_number : exp_number);
  } else {
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

// This should always succeed since it follows a call to parse_number_string.
// It assumes that there are more than 19 mantissa digits to parse.
parsed_number_string parse_truncated_decimal(const char *&p, const char *pend)  noexcept  {
  parsed_number_string answer;
  answer.valid = true;
  answer.negative = (*p == '-');
  if ((*p == '-') || (*p == '+')) {
    ++p;
  }
  size_t number_of_digits{0};


  uint64_t i = 0; 

  while ((p != pend) && is_integer(*p)) {
    // a multiplication by 10 is cheaper than an arbitrary integer
    // multiplication
    if(number_of_digits < 19) {

      uint8_t digit = uint8_t(*p - '0');
      i = 10 * i + digit;
      number_of_digits ++;
    }
    ++p;
  }
  int64_t exponent = 0;
  if ((p != pend) && (*p == '.')) {
    ++p;
    const char *first_after_period = p;
   
    while ((p != pend) && is_integer(*p)) {
      if(number_of_digits < 19) {
        uint8_t digit = uint8_t(*p - '0');
        i = i * 10 + digit;
        number_of_digits ++;
      } else if (exponent == 0) {
        exponent = first_after_period - p;
      }
      ++p;
    }
  }

  if ((p != pend) && (('e' == *p) || ('E' == *p))) {
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
      return answer;
    }
    while ((p != pend) && is_integer(*p)) {
      uint8_t digit = uint8_t(*p - '0');
      if (exp_number < 0x10000) {
        exp_number = 10 * exp_number + digit;
      }
      ++p;
    }
    exponent += (neg_exp ? -exp_number : exp_number);
  } 
  answer.lastmatch = p;
  answer.valid = true;
  answer.too_many_digits = true; // assumed
  answer.exponent = exponent;
  answer.mantissa = i;
  return answer;
}


// This should always succeed since it follows a call to parse_number_string.
decimal parse_decimal(const char *&p, const char *pend)  noexcept  {
  decimal answer;
  answer.num_digits = 0;
  answer.decimal_point = 0;
  answer.negative = false;
  answer.truncated = false;
  // skip leading whitespace
  while (fast_float::is_space(*p)) {
    p++;
  }
  answer.negative = (*p == '-');
  if ((*p == '-') || (*p == '+')) {
    ++p;
  }

  while ((p != pend) && (*p == '0')) {
    ++p;
  }
  while ((p != pend) && is_integer(*p)) {
    if (answer.num_digits + 1 < max_digits) {
      answer.digits[answer.num_digits++] = uint8_t(*p - '0');
    } else {
      answer.truncated = true;
    }
    ++p;
  }
  const char *first_after_period{};
  if ((p != pend) && (*p == '.')) {
    ++p;
    first_after_period = p;
    // if we have not yet encountered a zero, we have to skip it as well
    if(answer.num_digits == 0) {
      // skip zeros
      while ((p != pend) && (*p == '0')) {
       ++p;
      }
    }
    while ((p != pend) && is_integer(*p)) {
      if (answer.num_digits + 1 < max_digits) {
        answer.digits[answer.num_digits++] = uint8_t(*p - '0');
      } else {
        answer.truncated = true;
      }
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
  answer.decimal_point += answer.num_digits;
  return answer;
}
} // namespace fast_float
}  // namespace arrow_vendored

#endif

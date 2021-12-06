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

// String functions
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

extern "C" {

#include <algorithm>
#include <cinttypes>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "./types.h"

FORCE_INLINE
gdv_int32 octet_length_utf8(const gdv_utf8 input, gdv_int32 length) { return length; }

FORCE_INLINE
gdv_int32 bit_length_utf8(const gdv_utf8 input, gdv_int32 length) { return length * 8; }

FORCE_INLINE
gdv_int32 octet_length_binary(const gdv_binary input, gdv_int32 length) { return length; }

FORCE_INLINE
gdv_int32 bit_length_binary(const gdv_binary input, gdv_int32 length) {
  return length * 8;
}

FORCE_INLINE
int match_string(const char* input, gdv_int32 input_len, gdv_int32 start_pos,
                 const char* delim, gdv_int32 delim_len) {
  for (int i = start_pos; i < input_len; i++) {
    int left_chars = input_len - i;
    if ((left_chars >= delim_len) && memcmp(input + i, delim, delim_len) == 0) {
      return i + delim_len;
    }
  }

  return -1;
}

FORCE_INLINE
gdv_int32 mem_compare(const char* left, gdv_int32 left_len, const char* right,
                      gdv_int32 right_len) {
  int min = left_len;
  if (right_len < min) {
    min = right_len;
  }

  int cmp_ret = memcmp(left, right, min);
  if (cmp_ret != 0) {
    return cmp_ret;
  } else {
    return left_len - right_len;
  }
}

// Expand inner macro for all varlen types.
#define VAR_LEN_OP_TYPES(INNER, NAME, OP) \
  INNER(NAME, utf8, OP)                   \
  INNER(NAME, binary, OP)

// Relational binary fns : left, right params are same, return is bool.
#define BINARY_RELATIONAL(NAME, TYPE, OP)                                    \
  FORCE_INLINE                                                               \
  bool NAME##_##TYPE##_##TYPE(const gdv_##TYPE left, gdv_int32 left_len,     \
                              const gdv_##TYPE right, gdv_int32 right_len) { \
    return mem_compare(left, left_len, right, right_len) OP 0;               \
  }

VAR_LEN_OP_TYPES(BINARY_RELATIONAL, equal, ==)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, not_equal, !=)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, less_than, <)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, less_than_or_equal_to, <=)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, greater_than, >)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, greater_than_or_equal_to, >=)

#undef BINARY_RELATIONAL
#undef VAR_LEN_OP_TYPES

// Expand inner macro for all varlen types.
#define VAR_LEN_TYPES(INNER, NAME) \
  INNER(NAME, utf8)                \
  INNER(NAME, binary)

FORCE_INLINE
int to_binary_from_hex(char ch) {
  if (ch >= 'A' && ch <= 'F') {
    return 10 + (ch - 'A');
  } else if (ch >= 'a' && ch <= 'f') {
    return 10 + (ch - 'a');
  }
  return ch - '0';
}

FORCE_INLINE
bool starts_with_utf8_utf8(const char* data, gdv_int32 data_len, const char* prefix,
                           gdv_int32 prefix_len) {
  return ((data_len >= prefix_len) && (memcmp(data, prefix, prefix_len) == 0));
}

FORCE_INLINE
bool ends_with_utf8_utf8(const char* data, gdv_int32 data_len, const char* suffix,
                         gdv_int32 suffix_len) {
  return ((data_len >= suffix_len) &&
          (memcmp(data + data_len - suffix_len, suffix, suffix_len) == 0));
}

FORCE_INLINE
bool is_substr_utf8_utf8(const char* data, int32_t data_len, const char* substr,
                         int32_t substr_len) {
  for (int32_t i = 0; i <= data_len - substr_len; ++i) {
    if (memcmp(data + i, substr, substr_len) == 0) {
      return true;
    }
  }
  return false;
}

FORCE_INLINE
gdv_int32 utf8_char_length(char c) {
  if ((signed char)c >= 0) {  // 1-byte char (0x00 ~ 0x7F)
    return 1;
  } else if ((c & 0xE0) == 0xC0) {  // 2-byte char
    return 2;
  } else if ((c & 0xF0) == 0xE0) {  // 3-byte char
    return 3;
  } else if ((c & 0xF8) == 0xF0) {  // 4-byte char
    return 4;
  }
  // invalid char
  return 0;
}

FORCE_INLINE
void set_error_for_invalid_utf(int64_t execution_context, char val) {
  char const* fmt = "unexpected byte \\%02hhx encountered while decoding utf8 string";
  int size = static_cast<int>(strlen(fmt)) + 64;
  char* error = reinterpret_cast<char*>(malloc(size));
  snprintf(error, size, fmt, (unsigned char)val);
  gdv_fn_context_set_error_msg(execution_context, error);
  free(error);
}

FORCE_INLINE
bool validate_utf8_following_bytes(const char* data, int32_t data_len,
                                   int32_t char_index) {
  for (int j = 1; j < data_len; ++j) {
    if ((data[char_index + j] & 0xC0) != 0x80) {  // bytes following head-byte of glyph
      return false;
    }
  }
  return true;
}

// Count the number of utf8 characters
// return 0 for invalid/incomplete input byte sequences
FORCE_INLINE
gdv_int32 utf8_length(gdv_int64 context, const char* data, gdv_int32 data_len) {
  int char_len = 0;
  int count = 0;
  for (int i = 0; i < data_len; i += char_len) {
    char_len = utf8_char_length(data[i]);
    if (char_len == 0 || i + char_len > data_len) {  // invalid byte or incomplete glyph
      set_error_for_invalid_utf(context, data[i]);
      return 0;
    }
    for (int j = 1; j < char_len; ++j) {
      if ((data[i + j] & 0xC0) != 0x80) {  // bytes following head-byte of glyph
        set_error_for_invalid_utf(context, data[i + j]);
        return 0;
      }
    }
    ++count;
  }
  return count;
}

// Count the number of utf8 characters, ignoring invalid char, considering size 1
FORCE_INLINE
gdv_int32 utf8_length_ignore_invalid(const char* data, gdv_int32 data_len) {
  int char_len = 0;
  int count = 0;
  for (int i = 0; i < data_len; i += char_len) {
    char_len = utf8_char_length(data[i]);
    if (char_len == 0 || i + char_len > data_len) {  // invalid byte or incomplete glyph
      // if invalid byte or incomplete glyph, ignore it
      char_len = 1;
    }
    for (int j = 1; j < char_len; ++j) {
      if ((data[i + j] & 0xC0) != 0x80) {  // bytes following head-byte of glyph
        char_len += 1;
      }
    }
    ++count;
  }
  return count;
}

// Get the byte position corresponding to a character position for a non-empty utf8
// sequence
FORCE_INLINE
gdv_int32 utf8_byte_pos(gdv_int64 context, const char* str, gdv_int32 str_len,
                        gdv_int32 char_pos) {
  int char_len = 0;
  int byte_index = 0;
  for (gdv_int32 char_index = 0; char_index < char_pos && byte_index < str_len;
       char_index++) {
    char_len = utf8_char_length(str[byte_index]);
    if (char_len == 0 ||
        byte_index + char_len > str_len) {  // invalid byte or incomplete glyph
      set_error_for_invalid_utf(context, str[byte_index]);
      return -1;
    }
    byte_index += char_len;
  }
  return byte_index;
}

#define UTF8_LENGTH(NAME, TYPE)                                                 \
  FORCE_INLINE                                                                  \
  gdv_int32 NAME##_##TYPE(gdv_int64 context, gdv_##TYPE in, gdv_int32 in_len) { \
    return utf8_length(context, in, in_len);                                    \
  }

UTF8_LENGTH(char_length, utf8)
UTF8_LENGTH(length, utf8)
UTF8_LENGTH(lengthUtf8, binary)

// set max/min str length for space_int32, space_int64, lpad_utf8_int32_utf8
// and rpad_utf8_int32_utf8 to avoid exceptions
static const gdv_int32 max_str_length = 65536;
static const gdv_int32 min_str_length = 0;
// Returns a string of 'n' spaces.
#define SPACE_STR(IN_TYPE)                                                              \
  GANDIVA_EXPORT                                                                        \
  const char* space_##IN_TYPE(gdv_int64 ctx, gdv_##IN_TYPE n, int32_t* out_len) {       \
    n = std::min(static_cast<gdv_##IN_TYPE>(max_str_length), n);                        \
    n = std::max(static_cast<gdv_##IN_TYPE>(min_str_length), n);                        \
    gdv_int32 n_times = static_cast<gdv_int32>(n);                                      \
    if (n_times <= 0) {                                                                 \
      *out_len = 0;                                                                     \
      return "";                                                                        \
    }                                                                                   \
    char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(ctx, n_times));     \
    if (ret == nullptr) {                                                               \
      gdv_fn_context_set_error_msg(ctx, "Could not allocate memory for output string"); \
      *out_len = 0;                                                                     \
      return "";                                                                        \
    }                                                                                   \
    for (int i = 0; i < n_times; i++) {                                                 \
      ret[i] = ' ';                                                                     \
    }                                                                                   \
    *out_len = n_times;                                                                 \
    return ret;                                                                         \
  }

SPACE_STR(int32)
SPACE_STR(int64)

// Reverse a utf8 sequence
FORCE_INLINE
const char* reverse_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                         int32_t* out_len) {
  if (data_len == 0) {
    *out_len = 0;
    return "";
  }

  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, data_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  gdv_int32 char_len;
  for (gdv_int32 i = 0; i < data_len; i += char_len) {
    char_len = utf8_char_length(data[i]);

    if (char_len == 0 || i + char_len > data_len) {  // invalid byte or incomplete glyph
      set_error_for_invalid_utf(context, data[i]);
      *out_len = 0;
      return "";
    }

    for (gdv_int32 j = 0; j < char_len; ++j) {
      if (j > 0 && (data[i + j] & 0xC0) != 0x80) {  // bytes following head-byte of glyph
        set_error_for_invalid_utf(context, data[i + j]);
        *out_len = 0;
        return "";
      }
      ret[data_len - i - char_len + j] = data[i + j];
    }
  }
  *out_len = data_len;
  return ret;
}

// Trims whitespaces from the left end of the input utf8 sequence
FORCE_INLINE
const char* ltrim_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                       int32_t* out_len) {
  if (data_len == 0) {
    *out_len = 0;
    return "";
  }

  gdv_int32 start = 0;
  // start denotes the first position of non-space characters in the input string
  while (start < data_len && data[start] == ' ') {
    ++start;
  }

  *out_len = data_len - start;
  return data + start;
}

// Trims whitespaces from the right end of the input utf8 sequence
FORCE_INLINE
const char* rtrim_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                       int32_t* out_len) {
  if (data_len == 0) {
    *out_len = 0;
    return "";
  }

  gdv_int32 end = data_len - 1;
  // end denotes the last position of non-space characters in the input string
  while (end >= 0 && data[end] == ' ') {
    --end;
  }

  *out_len = end + 1;
  return data;
}

// Trims whitespaces from both the ends of the input utf8 sequence
FORCE_INLINE
const char* btrim_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                       int32_t* out_len) {
  if (data_len == 0) {
    *out_len = 0;
    return "";
  }

  gdv_int32 start = 0, end = data_len - 1;
  // start and end denote the first and last positions of non-space
  // characters in the input string respectively
  while (start <= end && data[start] == ' ') {
    ++start;
  }
  while (end >= start && data[end] == ' ') {
    --end;
  }

  // string has some leading/trailing spaces and some non-space characters
  *out_len = end - start + 1;
  return data + start;
}

// Trims characters present in the trim text from the left end of the base text
FORCE_INLINE
const char* ltrim_utf8_utf8(gdv_int64 context, const char* basetext,
                            gdv_int32 basetext_len, const char* trimtext,
                            gdv_int32 trimtext_len, int32_t* out_len) {
  if (basetext_len == 0) {
    *out_len = 0;
    return "";
  } else if (trimtext_len == 0) {
    *out_len = basetext_len;
    return basetext;
  }

  gdv_int32 start_ptr, char_len;
  // scan the base text from left to right and increment the start pointer till
  // there is a character which is not present in the trim text
  for (start_ptr = 0; start_ptr < basetext_len; start_ptr += char_len) {
    char_len = utf8_char_length(basetext[start_ptr]);
    if (char_len == 0 || start_ptr + char_len > basetext_len) {
      // invalid byte or incomplete glyph
      set_error_for_invalid_utf(context, basetext[start_ptr]);
      *out_len = 0;
      return "";
    }
    if (!is_substr_utf8_utf8(trimtext, trimtext_len, basetext + start_ptr, char_len)) {
      break;
    }
  }

  *out_len = basetext_len - start_ptr;
  return basetext + start_ptr;
}

// Trims characters present in the trim text from the right end of the base text
FORCE_INLINE
const char* rtrim_utf8_utf8(gdv_int64 context, const char* basetext,
                            gdv_int32 basetext_len, const char* trimtext,
                            gdv_int32 trimtext_len, int32_t* out_len) {
  if (basetext_len == 0) {
    *out_len = 0;
    return "";
  } else if (trimtext_len == 0) {
    *out_len = basetext_len;
    return basetext;
  }

  gdv_int32 char_len, end_ptr, byte_cnt = 1;
  // scan the base text from right to left and decrement the end pointer till
  // there is a character which is not present in the trim text
  for (end_ptr = basetext_len - 1; end_ptr >= 0; --end_ptr) {
    char_len = utf8_char_length(basetext[end_ptr]);
    if (char_len == 0) {  // trailing bytes of multibyte character
      ++byte_cnt;
      continue;
    }
    // this is the first byte of a character, hence check if char_len = char_cnt
    if (byte_cnt != char_len) {  // invalid byte or incomplete glyph
      set_error_for_invalid_utf(context, basetext[end_ptr]);
      *out_len = 0;
      return "";
    }
    byte_cnt = 1;  // reset the counter*/
    if (!is_substr_utf8_utf8(trimtext, trimtext_len, basetext + end_ptr, char_len)) {
      break;
    }
  }

  // when all characters in the basetext are part of the trimtext
  if (end_ptr == -1) {
    *out_len = 0;
    return "";
  }

  end_ptr += utf8_char_length(basetext[end_ptr]);  // point to the next character
  *out_len = end_ptr;
  return basetext;
}

// Trims characters present in the trim text from both ends of the base text
FORCE_INLINE
const char* btrim_utf8_utf8(gdv_int64 context, const char* basetext,
                            gdv_int32 basetext_len, const char* trimtext,
                            gdv_int32 trimtext_len, int32_t* out_len) {
  if (basetext_len == 0) {
    *out_len = 0;
    return "";
  } else if (trimtext_len == 0) {
    *out_len = basetext_len;
    return basetext;
  }

  gdv_int32 start_ptr, end_ptr, char_len, byte_cnt = 1;
  // scan the base text from left to right and increment the start and decrement the
  // end pointers till there are characters which are not present in the trim text
  for (start_ptr = 0; start_ptr < basetext_len; start_ptr += char_len) {
    char_len = utf8_char_length(basetext[start_ptr]);
    if (char_len == 0 || start_ptr + char_len > basetext_len) {
      // invalid byte or incomplete glyph
      set_error_for_invalid_utf(context, basetext[start_ptr]);
      *out_len = 0;
      return "";
    }
    if (!is_substr_utf8_utf8(trimtext, trimtext_len, basetext + start_ptr, char_len)) {
      break;
    }
  }
  for (end_ptr = basetext_len - 1; end_ptr >= start_ptr; --end_ptr) {
    char_len = utf8_char_length(basetext[end_ptr]);
    if (char_len == 0) {  // trailing byte in multibyte character
      ++byte_cnt;
      continue;
    }
    // this is the first byte of a character, hence check if char_len = char_cnt
    if (byte_cnt != char_len) {  // invalid byte or incomplete glyph
      set_error_for_invalid_utf(context, basetext[end_ptr]);
      *out_len = 0;
      return "";
    }
    byte_cnt = 1;  // reset the counter*/
    if (!is_substr_utf8_utf8(trimtext, trimtext_len, basetext + end_ptr, char_len)) {
      break;
    }
  }

  // when all characters are trimmed, start_ptr has been incremented to basetext_len and
  // end_ptr still points to basetext_len - 1, hence we need to handle this case
  if (start_ptr > end_ptr) {
    *out_len = 0;
    return "";
  }

  end_ptr += utf8_char_length(basetext[end_ptr]);  // point to the next character
  *out_len = end_ptr - start_ptr;
  return basetext + start_ptr;
}

FORCE_INLINE
gdv_boolean compare_lower_strings(const char* base_str, gdv_int32 base_str_len,
                                  const char* str, gdv_int32 str_len) {
  if (base_str_len != str_len) {
    return false;
  }
  for (int i = 0; i < str_len; i++) {
    // convert char to lower
    char cur = str[i];
    // 'A' - 'Z' : 0x41 - 0x5a
    // 'a' - 'z' : 0x61 - 0x7a
    if (cur >= 0x41 && cur <= 0x5a) {
      cur = static_cast<char>(cur + 0x20);
    }
    // if the character does not match, break the flow
    if (cur != base_str[i]) break;
    // if the character matches and it is the last iteration, return true
    if (i == str_len - 1) return true;
  }
  return false;
}

// Try to cast the received string ('0', '1', 'true', 'false'), ignoring leading
// and trailing spaces, also ignoring lower and upper case.
FORCE_INLINE
gdv_boolean castBIT_utf8(gdv_int64 context, const char* data, gdv_int32 data_len) {
  if (data_len <= 0) {
    gdv_fn_context_set_error_msg(context, "Invalid value for boolean.");
    return false;
  }

  // trim leading and trailing spaces
  int32_t trimmed_len;
  int32_t start = 0, end = data_len - 1;
  while (start <= end && data[start] == ' ') {
    ++start;
  }
  while (end >= start && data[end] == ' ') {
    --end;
  }
  trimmed_len = end - start + 1;
  const char* trimmed_data = data + start;

  // compare received string with the valid bool string values '1', '0', 'true', 'false'
  if (trimmed_len == 1) {
    // case for '0' and '1' value
    if (trimmed_data[0] == '1') return true;
    if (trimmed_data[0] == '0') return false;
  } else if (trimmed_len == 4) {
    // case for matching 'true'
    if (compare_lower_strings("true", 4, trimmed_data, trimmed_len)) return true;
  } else if (trimmed_len == 5) {
    // case for matching 'false'
    if (compare_lower_strings("false", 5, trimmed_data, trimmed_len)) return false;
  }
  // if no 'true', 'false', '0' or '1' value is found, set an error
  gdv_fn_context_set_error_msg(context, "Invalid value for boolean.");
  return false;
}

FORCE_INLINE
const char* castVARCHAR_bool_int64(gdv_int64 context, gdv_boolean value,
                                   gdv_int64 out_len, gdv_int32* out_length) {
  gdv_int32 len = static_cast<gdv_int32>(out_len);
  if (len < 0) {
    gdv_fn_context_set_error_msg(context, "Output buffer length can't be negative");
    *out_length = 0;
    return "";
  }
  const char* out =
      reinterpret_cast<const char*>(gdv_fn_context_arena_malloc(context, 5));
  out = value ? "true" : "false";
  *out_length = value ? ((len > 4) ? 4 : len) : ((len > 5) ? 5 : len);
  return out;
}

// Truncates the string to given length
#define CAST_VARCHAR_FROM_VARLEN_TYPE(TYPE)                                            \
  FORCE_INLINE                                                                         \
  const char* castVARCHAR_##TYPE##_int64(gdv_int64 context, const char* data,          \
                                         gdv_int32 data_len, int64_t out_len,          \
                                         int32_t* out_length) {                        \
    int32_t len = static_cast<int32_t>(out_len);                                       \
                                                                                       \
    if (len < 0) {                                                                     \
      gdv_fn_context_set_error_msg(context, "Output buffer length can't be negative"); \
      *out_length = 0;                                                                 \
      return "";                                                                       \
    }                                                                                  \
                                                                                       \
    if (len >= data_len || len == 0) {                                                 \
      *out_length = data_len;                                                          \
      return data;                                                                     \
    }                                                                                  \
                                                                                       \
    int32_t remaining = len;                                                           \
    int32_t index = 0;                                                                 \
    bool is_multibyte = false;                                                         \
    do {                                                                               \
      /* In utf8, MSB of a single byte unicode char is always 0,                       \
       * whereas for a multibyte character the MSB of each byte is 1.                  \
       * So for a single byte char, a bitwise-and with x80 (10000000) will be 0        \
       * and it won't be 0 for bytes of a multibyte char.                              \
       */                                                                              \
      char* data_ptr = const_cast<char*>(data);                                        \
                                                                                       \
      /* advance byte by byte till the 8-byte boundary then advance 8 bytes */         \
      auto num_bytes = reinterpret_cast<uintptr_t>(data_ptr) & 0x07;                   \
      num_bytes = (8 - num_bytes) & 0x07;                                              \
      while (num_bytes > 0) {                                                          \
        uint8_t* ptr = reinterpret_cast<uint8_t*>(data_ptr + index);                   \
        if ((*ptr & 0x80) != 0) {                                                      \
          is_multibyte = true;                                                         \
          break;                                                                       \
        }                                                                              \
        index++;                                                                       \
        remaining--;                                                                   \
        num_bytes--;                                                                   \
      }                                                                                \
      if (is_multibyte) break;                                                         \
      while (remaining >= 8) {                                                         \
        uint64_t* ptr = reinterpret_cast<uint64_t*>(data_ptr + index);                 \
        if ((*ptr & 0x8080808080808080) != 0) {                                        \
          is_multibyte = true;                                                         \
          break;                                                                       \
        }                                                                              \
        index += 8;                                                                    \
        remaining -= 8;                                                                \
      }                                                                                \
      if (is_multibyte) break;                                                         \
      if (remaining >= 4) {                                                            \
        uint32_t* ptr = reinterpret_cast<uint32_t*>(data_ptr + index);                 \
        if ((*ptr & 0x80808080) != 0) break;                                           \
        index += 4;                                                                    \
        remaining -= 4;                                                                \
      }                                                                                \
      while (remaining > 0) {                                                          \
        uint8_t* ptr = reinterpret_cast<uint8_t*>(data_ptr + index);                   \
        if ((*ptr & 0x80) != 0) {                                                      \
          is_multibyte = true;                                                         \
          break;                                                                       \
        }                                                                              \
        index++;                                                                       \
        remaining--;                                                                   \
      }                                                                                \
      if (is_multibyte) break;                                                         \
      /* reached here; all are single byte characters */                               \
      *out_length = len;                                                               \
      return data;                                                                     \
    } while (false);                                                                   \
                                                                                       \
    /* detected multibyte utf8 characters; slow path */                                \
    int32_t byte_pos =                                                                 \
        utf8_byte_pos(context, data + index, data_len - index, len - index);           \
    if (byte_pos < 0) {                                                                \
      *out_length = 0;                                                                 \
      return "";                                                                       \
    }                                                                                  \
                                                                                       \
    *out_length = index + byte_pos;                                                    \
    return data;                                                                       \
  }

CAST_VARCHAR_FROM_VARLEN_TYPE(utf8)
CAST_VARCHAR_FROM_VARLEN_TYPE(binary)

#undef CAST_VARCHAR_FROM_VARLEN_TYPE

// Add functions for castVARBINARY
#define CAST_VARBINARY_FROM_STRING_AND_BINARY(TYPE)                                    \
  GANDIVA_EXPORT                                                                       \
  const char* castVARBINARY_##TYPE##_int64(gdv_int64 context, const char* data,        \
                                           gdv_int32 data_len, int64_t out_len,        \
                                           int32_t* out_length) {                      \
    int32_t len = static_cast<int32_t>(out_len);                                       \
    if (len < 0) {                                                                     \
      gdv_fn_context_set_error_msg(context, "Output buffer length can't be negative"); \
      *out_length = 0;                                                                 \
      return "";                                                                       \
    }                                                                                  \
                                                                                       \
    if (len >= data_len || len == 0) {                                                 \
      *out_length = data_len;                                                          \
    } else {                                                                           \
      *out_length = len;                                                               \
    }                                                                                  \
    return data;                                                                       \
  }

CAST_VARBINARY_FROM_STRING_AND_BINARY(utf8)
CAST_VARBINARY_FROM_STRING_AND_BINARY(binary)

#undef CAST_VARBINARY_FROM_STRING_AND_BINARY

#define IS_NULL(NAME, TYPE)                                                \
  FORCE_INLINE                                                             \
  bool NAME##_##TYPE(gdv_##TYPE in, gdv_int32 len, gdv_boolean is_valid) { \
    return !is_valid;                                                      \
  }

VAR_LEN_TYPES(IS_NULL, isnull)

#undef IS_NULL

#define IS_NOT_NULL(NAME, TYPE)                                            \
  FORCE_INLINE                                                             \
  bool NAME##_##TYPE(gdv_##TYPE in, gdv_int32 len, gdv_boolean is_valid) { \
    return is_valid;                                                       \
  }

VAR_LEN_TYPES(IS_NOT_NULL, isnotnull)

#undef IS_NOT_NULL
#undef VAR_LEN_TYPES

/*
 We follow Oracle semantics for offset:
 - If position is positive, then the first glyph in the substring is determined by
 counting that many glyphs forward from the beginning of the input. (i.e., for position ==
 1 the first glyph in the substring will be identical to the first glyph in the input)

 - If position is negative, then the first glyph in the substring is determined by
 counting that many glyphs backward from the end of the input. (i.e., for position == -1
 the first glyph in the substring will be identical to the last glyph in the input)

 - If position is 0 then it is treated as 1.
 */
FORCE_INLINE
const char* substr_utf8_int64_int64(gdv_int64 context, const char* input,
                                    gdv_int32 in_data_len, gdv_int64 position,
                                    gdv_int64 substring_length, gdv_int32* out_data_len) {
  if (substring_length <= 0 || input == nullptr || in_data_len <= 0) {
    *out_data_len = 0;
    return "";
  }

  gdv_int64 in_glyphs_count =
      static_cast<gdv_int64>(utf8_length(context, input, in_data_len));

  // in_glyphs_count is zero if input has invalid glyphs
  if (in_glyphs_count == 0) {
    *out_data_len = 0;
    return "";
  }

  gdv_int64 from_glyph;  // from_glyph==0 indicates the first glyph of the input
  if (position > 0) {
    from_glyph = position - 1;
  } else if (position < 0) {
    from_glyph = in_glyphs_count + position;
  } else {
    from_glyph = 0;
  }

  if (from_glyph < 0 || from_glyph >= in_glyphs_count) {
    *out_data_len = 0;
    return "";
  }

  gdv_int64 out_glyphs_count = substring_length;
  if (substring_length > in_glyphs_count - from_glyph) {
    out_glyphs_count = in_glyphs_count - from_glyph;
  }

  gdv_int64 in_data_len64 = static_cast<gdv_int64>(in_data_len);
  gdv_int64 start_pos = 0;
  gdv_int64 end_pos = in_data_len64;

  gdv_int64 current_glyph = 0;
  gdv_int64 pos = 0;
  while (pos < in_data_len64) {
    if (current_glyph == from_glyph) {
      start_pos = pos;
    }
    pos += static_cast<gdv_int64>(utf8_char_length(input[pos]));
    if (current_glyph - from_glyph + 1 == out_glyphs_count) {
      end_pos = pos;
    }
    current_glyph++;
  }

  if (end_pos > in_data_len64 || end_pos > INT_MAX) {
    end_pos = in_data_len64;
  }

  *out_data_len = static_cast<gdv_int32>(end_pos - start_pos);
  char* ret =
      reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_data_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_data_len = 0;
    return "";
  }
  memcpy(ret, input + start_pos, *out_data_len);
  return ret;
}

FORCE_INLINE
const char* substr_utf8_int64(gdv_int64 context, const char* input, gdv_int32 in_len,
                              gdv_int64 offset64, gdv_int32* out_len) {
  return substr_utf8_int64_int64(context, input, in_len, offset64, in_len, out_len);
}

FORCE_INLINE
const char* repeat_utf8_int32(gdv_int64 context, const char* in, gdv_int32 in_len,
                              gdv_int32 repeat_number, gdv_int32* out_len) {
  // if the repeat number is zero, then return empty string
  if (repeat_number == 0 || in_len <= 0) {
    *out_len = 0;
    return "";
  }
  // if the repeat number is a negative number, an error is set on context
  if (repeat_number < 0) {
    gdv_fn_context_set_error_msg(context, "Repeat number can't be negative");
    *out_len = 0;
    return "";
  }
  *out_len = repeat_number * in_len;
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  for (int i = 0; i < repeat_number; ++i) {
    memcpy(ret + (i * in_len), in, in_len);
  }
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8(gdv_int64 context, const char* left, gdv_int32 left_len,
                             bool left_validity, const char* right, gdv_int32 right_len,
                             bool right_validity, gdv_int32* out_len) {
  if (!left_validity) {
    left_len = 0;
  }
  if (!right_validity) {
    right_len = 0;
  }
  return concatOperator_utf8_utf8(context, left, left_len, right, right_len, out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8(gdv_int64 context, const char* left,
                                     gdv_int32 left_len, const char* right,
                                     gdv_int32 right_len, gdv_int32* out_len) {
  *out_len = left_len + right_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, left, left_len);
  memcpy(ret + left_len, right, right_len);
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8_utf8(gdv_int64 context, const char* in1, gdv_int32 in1_len,
                                  bool in1_validity, const char* in2, gdv_int32 in2_len,
                                  bool in2_validity, const char* in3, gdv_int32 in3_len,
                                  bool in3_validity, gdv_int32* out_len) {
  if (!in1_validity) {
    in1_len = 0;
  }
  if (!in2_validity) {
    in2_len = 0;
  }
  if (!in3_validity) {
    in3_len = 0;
  }
  return concatOperator_utf8_utf8_utf8(context, in1, in1_len, in2, in2_len, in3, in3_len,
                                       out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8_utf8(gdv_int64 context, const char* in1,
                                          gdv_int32 in1_len, const char* in2,
                                          gdv_int32 in2_len, const char* in3,
                                          gdv_int32 in3_len, gdv_int32* out_len) {
  *out_len = in1_len + in2_len + in3_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, in1, in1_len);
  memcpy(ret + in1_len, in2, in2_len);
  memcpy(ret + in1_len + in2_len, in3, in3_len);
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8_utf8_utf8(gdv_int64 context, const char* in1,
                                       gdv_int32 in1_len, bool in1_validity,
                                       const char* in2, gdv_int32 in2_len,
                                       bool in2_validity, const char* in3,
                                       gdv_int32 in3_len, bool in3_validity,
                                       const char* in4, gdv_int32 in4_len,
                                       bool in4_validity, gdv_int32* out_len) {
  if (!in1_validity) {
    in1_len = 0;
  }
  if (!in2_validity) {
    in2_len = 0;
  }
  if (!in3_validity) {
    in3_len = 0;
  }
  if (!in4_validity) {
    in4_len = 0;
  }
  return concatOperator_utf8_utf8_utf8_utf8(context, in1, in1_len, in2, in2_len, in3,
                                            in3_len, in4, in4_len, out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8_utf8_utf8(gdv_int64 context, const char* in1,
                                               gdv_int32 in1_len, const char* in2,
                                               gdv_int32 in2_len, const char* in3,
                                               gdv_int32 in3_len, const char* in4,
                                               gdv_int32 in4_len, gdv_int32* out_len) {
  *out_len = in1_len + in2_len + in3_len + in4_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, in1, in1_len);
  memcpy(ret + in1_len, in2, in2_len);
  memcpy(ret + in1_len + in2_len, in3, in3_len);
  memcpy(ret + in1_len + in2_len + in3_len, in4, in4_len);
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    gdv_int32* out_len) {
  if (!in1_validity) {
    in1_len = 0;
  }
  if (!in2_validity) {
    in2_len = 0;
  }
  if (!in3_validity) {
    in3_len = 0;
  }
  if (!in4_validity) {
    in4_len = 0;
  }
  if (!in5_validity) {
    in5_len = 0;
  }
  return concatOperator_utf8_utf8_utf8_utf8_utf8(context, in1, in1_len, in2, in2_len, in3,
                                                 in3_len, in4, in4_len, in5, in5_len,
                                                 out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, gdv_int32* out_len) {
  *out_len = in1_len + in2_len + in3_len + in4_len + in5_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, in1, in1_len);
  memcpy(ret + in1_len, in2, in2_len);
  memcpy(ret + in1_len + in2_len, in3, in3_len);
  memcpy(ret + in1_len + in2_len + in3_len, in4, in4_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len, in5, in5_len);
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, gdv_int32* out_len) {
  if (!in1_validity) {
    in1_len = 0;
  }
  if (!in2_validity) {
    in2_len = 0;
  }
  if (!in3_validity) {
    in3_len = 0;
  }
  if (!in4_validity) {
    in4_len = 0;
  }
  if (!in5_validity) {
    in5_len = 0;
  }
  if (!in6_validity) {
    in6_len = 0;
  }
  return concatOperator_utf8_utf8_utf8_utf8_utf8_utf8(context, in1, in1_len, in2, in2_len,
                                                      in3, in3_len, in4, in4_len, in5,
                                                      in5_len, in6, in6_len, out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, gdv_int32* out_len) {
  *out_len = in1_len + in2_len + in3_len + in4_len + in5_len + in6_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, in1, in1_len);
  memcpy(ret + in1_len, in2, in2_len);
  memcpy(ret + in1_len + in2_len, in3, in3_len);
  memcpy(ret + in1_len + in2_len + in3_len, in4, in4_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len, in5, in5_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len, in6, in6_len);
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, const char* in7,
    gdv_int32 in7_len, bool in7_validity, gdv_int32* out_len) {
  if (!in1_validity) {
    in1_len = 0;
  }
  if (!in2_validity) {
    in2_len = 0;
  }
  if (!in3_validity) {
    in3_len = 0;
  }
  if (!in4_validity) {
    in4_len = 0;
  }
  if (!in5_validity) {
    in5_len = 0;
  }
  if (!in6_validity) {
    in6_len = 0;
  }
  if (!in7_validity) {
    in7_len = 0;
  }
  return concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      context, in1, in1_len, in2, in2_len, in3, in3_len, in4, in4_len, in5, in5_len, in6,
      in6_len, in7, in7_len, out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, const char* in7, gdv_int32 in7_len, gdv_int32* out_len) {
  *out_len = in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, in1, in1_len);
  memcpy(ret + in1_len, in2, in2_len);
  memcpy(ret + in1_len + in2_len, in3, in3_len);
  memcpy(ret + in1_len + in2_len + in3_len, in4, in4_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len, in5, in5_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len, in6, in6_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len, in7, in7_len);
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, const char* in7,
    gdv_int32 in7_len, bool in7_validity, const char* in8, gdv_int32 in8_len,
    bool in8_validity, gdv_int32* out_len) {
  if (!in1_validity) {
    in1_len = 0;
  }
  if (!in2_validity) {
    in2_len = 0;
  }
  if (!in3_validity) {
    in3_len = 0;
  }
  if (!in4_validity) {
    in4_len = 0;
  }
  if (!in5_validity) {
    in5_len = 0;
  }
  if (!in6_validity) {
    in6_len = 0;
  }
  if (!in7_validity) {
    in7_len = 0;
  }
  if (!in8_validity) {
    in8_len = 0;
  }
  return concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      context, in1, in1_len, in2, in2_len, in3, in3_len, in4, in4_len, in5, in5_len, in6,
      in6_len, in7, in7_len, in8, in8_len, out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, const char* in7, gdv_int32 in7_len, const char* in8,
    gdv_int32 in8_len, gdv_int32* out_len) {
  *out_len =
      in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len + in8_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, in1, in1_len);
  memcpy(ret + in1_len, in2, in2_len);
  memcpy(ret + in1_len + in2_len, in3, in3_len);
  memcpy(ret + in1_len + in2_len + in3_len, in4, in4_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len, in5, in5_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len, in6, in6_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len, in7, in7_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len, in8,
         in8_len);
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, const char* in7,
    gdv_int32 in7_len, bool in7_validity, const char* in8, gdv_int32 in8_len,
    bool in8_validity, const char* in9, gdv_int32 in9_len, bool in9_validity,
    gdv_int32* out_len) {
  if (!in1_validity) {
    in1_len = 0;
  }
  if (!in2_validity) {
    in2_len = 0;
  }
  if (!in3_validity) {
    in3_len = 0;
  }
  if (!in4_validity) {
    in4_len = 0;
  }
  if (!in5_validity) {
    in5_len = 0;
  }
  if (!in6_validity) {
    in6_len = 0;
  }
  if (!in7_validity) {
    in7_len = 0;
  }
  if (!in8_validity) {
    in8_len = 0;
  }
  if (!in9_validity) {
    in9_len = 0;
  }
  return concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      context, in1, in1_len, in2, in2_len, in3, in3_len, in4, in4_len, in5, in5_len, in6,
      in6_len, in7, in7_len, in8, in8_len, in9, in9_len, out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, const char* in7, gdv_int32 in7_len, const char* in8,
    gdv_int32 in8_len, const char* in9, gdv_int32 in9_len, gdv_int32* out_len) {
  *out_len = in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len +
             in8_len + in9_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, in1, in1_len);
  memcpy(ret + in1_len, in2, in2_len);
  memcpy(ret + in1_len + in2_len, in3, in3_len);
  memcpy(ret + in1_len + in2_len + in3_len, in4, in4_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len, in5, in5_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len, in6, in6_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len, in7, in7_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len, in8,
         in8_len);
  memcpy(
      ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len + in8_len,
      in9, in9_len);
  return ret;
}

FORCE_INLINE
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, const char* in7,
    gdv_int32 in7_len, bool in7_validity, const char* in8, gdv_int32 in8_len,
    bool in8_validity, const char* in9, gdv_int32 in9_len, bool in9_validity,
    const char* in10, gdv_int32 in10_len, bool in10_validity, gdv_int32* out_len) {
  if (!in1_validity) {
    in1_len = 0;
  }
  if (!in2_validity) {
    in2_len = 0;
  }
  if (!in3_validity) {
    in3_len = 0;
  }
  if (!in4_validity) {
    in4_len = 0;
  }
  if (!in5_validity) {
    in5_len = 0;
  }
  if (!in6_validity) {
    in6_len = 0;
  }
  if (!in7_validity) {
    in7_len = 0;
  }
  if (!in8_validity) {
    in8_len = 0;
  }
  if (!in9_validity) {
    in9_len = 0;
  }
  if (!in10_validity) {
    in10_len = 0;
  }
  return concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      context, in1, in1_len, in2, in2_len, in3, in3_len, in4, in4_len, in5, in5_len, in6,
      in6_len, in7, in7_len, in8, in8_len, in9, in9_len, in10, in10_len, out_len);
}

FORCE_INLINE
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, const char* in7, gdv_int32 in7_len, const char* in8,
    gdv_int32 in8_len, const char* in9, gdv_int32 in9_len, const char* in10,
    gdv_int32 in10_len, gdv_int32* out_len) {
  *out_len = in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len +
             in8_len + in9_len + in10_len;
  if (*out_len <= 0) {
    *out_len = 0;
    return "";
  }
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, in1, in1_len);
  memcpy(ret + in1_len, in2, in2_len);
  memcpy(ret + in1_len + in2_len, in3, in3_len);
  memcpy(ret + in1_len + in2_len + in3_len, in4, in4_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len, in5, in5_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len, in6, in6_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len, in7, in7_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len, in8,
         in8_len);
  memcpy(
      ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len + in8_len,
      in9, in9_len);
  memcpy(ret + in1_len + in2_len + in3_len + in4_len + in5_len + in6_len + in7_len +
             in8_len + in9_len,
         in10, in10_len);
  return ret;
}

// Returns the numeric value of the first character of str.
GANDIVA_EXPORT
gdv_int32 ascii_utf8(const char* data, gdv_int32 data_len) {
  if (data_len == 0) {
    return 0;
  }
  return static_cast<gdv_int32>(data[0]);
}

// Returns the ASCII character having the binary equivalent to A.
// If A is larger than 256 the result is equivalent to chr(A % 256).
FORCE_INLINE
const char* chr_int32(gdv_int64 context, gdv_int32 in, gdv_int32* out_len) {
  in = in % 256;
  *out_len = 1;

  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  ret[0] = char(in);
  return ret;
}

// Returns the ASCII character having the binary equivalent to A.
// If A is larger than 256 the result is equivalent to chr(A % 256).
FORCE_INLINE
const char* chr_int64(gdv_int64 context, gdv_int64 in, gdv_int32* out_len) {
  in = in % 256;
  *out_len = 1;

  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  ret[0] = char(in);
  return ret;
}

FORCE_INLINE
const char* convert_fromUTF8_binary(gdv_int64 context, const char* bin_in, gdv_int32 len,
                                    gdv_int32* out_len) {
  *out_len = len;
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, bin_in, *out_len);
  return ret;
}

FORCE_INLINE
const char* convert_replace_invalid_fromUTF8_binary(int64_t context, const char* text_in,
                                                    int32_t text_len,
                                                    const char* char_to_replace,
                                                    int32_t char_to_replace_len,
                                                    int32_t* out_len) {
  if (char_to_replace_len > 1) {
    gdv_fn_context_set_error_msg(context, "Replacement of multiple bytes not supported");
    *out_len = 0;
    return "";
  }
  // actually the convert_replace function replaces invalid chars with an ASCII
  // character so the output length will be the same as the input length
  *out_len = text_len;
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  int32_t valid_bytes_to_cpy = 0;
  int32_t out_byte_counter = 0;
  int32_t in_byte_counter = 0;
  int32_t char_len;
  // scan the base text from left to right and increment the start pointer till
  // looking for invalid chars to substitute
  for (int text_index = 0; text_index < text_len; text_index += char_len) {
    char_len = utf8_char_length(text_in[text_index]);
    // only memory copy the bytes when detect invalid char
    if (char_len == 0 || text_index + char_len > text_len ||
        !validate_utf8_following_bytes(text_in, char_len, text_index)) {
      // define char_len = 1 to increase text_index by 1 (as ASCII char fits in 1 byte)
      char_len = 1;
      // first copy the valid bytes until now and then replace the invalid character
      memcpy(ret + out_byte_counter, text_in + in_byte_counter, valid_bytes_to_cpy);
      // if the replacement char is empty, the invalid char should be ignored
      if (char_to_replace_len == 0) {
        out_byte_counter += valid_bytes_to_cpy;
      } else {
        ret[out_byte_counter + valid_bytes_to_cpy] = char_to_replace[0];
        out_byte_counter += valid_bytes_to_cpy + char_len;
      }
      in_byte_counter += valid_bytes_to_cpy + char_len;
      valid_bytes_to_cpy = 0;
      continue;
    }
    valid_bytes_to_cpy += char_len;
  }
  // if invalid chars were not found, return the original string
  if (out_byte_counter == 0 && in_byte_counter == 0) return text_in;
  // if there are still valid bytes to copy, do it
  if (valid_bytes_to_cpy != 0) {
    memcpy(ret + out_byte_counter, text_in + in_byte_counter, valid_bytes_to_cpy);
  }
  // the out length will be the out bytes copied + the missing end bytes copied
  *out_len = valid_bytes_to_cpy + out_byte_counter;
  return ret;
}

// The function reverse a char array in-place
static inline void reverse_char_buf(char* buf, int32_t len) {
  char temp;

  for (int32_t i = 0; i < len / 2; i++) {
    int32_t pos_swp = len - (1 + i);
    temp = buf[pos_swp];
    buf[pos_swp] = buf[i];
    buf[i] = temp;
  }
}

// Converts a double variable to binary
FORCE_INLINE
const char* convert_toDOUBLE(int64_t context, double value, int32_t* out_len) {
  *out_len = sizeof(value);
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not allocate memory for the output string");

    *out_len = 0;
    return "";
  }

  memcpy(ret, &value, *out_len);

  return ret;
}

FORCE_INLINE
const char* convert_toDOUBLE_be(int64_t context, double value, int32_t* out_len) {
  // The function behaves like convert_toDOUBLE, but always return the result
  // in big endian format
  char* ret = const_cast<char*>(convert_toDOUBLE(context, value, out_len));

#if ARROW_LITTLE_ENDIAN
  reverse_char_buf(ret, *out_len);
#endif

  return ret;
}

// Converts a float variable to binary
FORCE_INLINE
const char* convert_toFLOAT(int64_t context, float value, int32_t* out_len) {
  *out_len = sizeof(value);
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not allocate memory for the output string");

    *out_len = 0;
    return "";
  }

  memcpy(ret, &value, *out_len);

  return ret;
}

FORCE_INLINE
const char* convert_toFLOAT_be(int64_t context, float value, int32_t* out_len) {
  // The function behaves like convert_toFLOAT, but always return the result
  // in big endian format
  char* ret = const_cast<char*>(convert_toFLOAT(context, value, out_len));

#if ARROW_LITTLE_ENDIAN
  reverse_char_buf(ret, *out_len);
#endif

  return ret;
}

// Converts a bigint(int with 64 bits) variable to binary
FORCE_INLINE
const char* convert_toBIGINT(int64_t context, int64_t value, int32_t* out_len) {
  *out_len = sizeof(value);
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not allocate memory for the output string");

    *out_len = 0;
    return "";
  }

  memcpy(ret, &value, *out_len);

  return ret;
}

FORCE_INLINE
const char* convert_toBIGINT_be(int64_t context, int64_t value, int32_t* out_len) {
  // The function behaves like convert_toBIGINT, but always return the result
  // in big endian format
  char* ret = const_cast<char*>(convert_toBIGINT(context, value, out_len));

#if ARROW_LITTLE_ENDIAN
  reverse_char_buf(ret, *out_len);
#endif

  return ret;
}

// Converts an integer(with 32 bits) variable to binary
FORCE_INLINE
const char* convert_toINT(int64_t context, int32_t value, int32_t* out_len) {
  *out_len = sizeof(value);
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not allocate memory for the output string");

    *out_len = 0;
    return "";
  }

  memcpy(ret, &value, *out_len);

  return ret;
}

FORCE_INLINE
const char* convert_toINT_be(int64_t context, int32_t value, int32_t* out_len) {
  // The function behaves like convert_toINT, but always return the result
  // in big endian format
  char* ret = const_cast<char*>(convert_toINT(context, value, out_len));

#if ARROW_LITTLE_ENDIAN
  reverse_char_buf(ret, *out_len);
#endif

  return ret;
}

// Converts a boolean variable to binary
FORCE_INLINE
const char* convert_toBOOLEAN(int64_t context, bool value, int32_t* out_len) {
  *out_len = sizeof(value);
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not allocate memory for the output string");

    *out_len = 0;
    return "";
  }

  memcpy(ret, &value, *out_len);

  return ret;
}

// Converts a time variable to binary
FORCE_INLINE
const char* convert_toTIME_EPOCH(int64_t context, int32_t value, int32_t* out_len) {
  return convert_toINT(context, value, out_len);
}

FORCE_INLINE
const char* convert_toTIME_EPOCH_be(int64_t context, int32_t value, int32_t* out_len) {
  // The function behaves as convert_toTIME_EPOCH, but
  // returns the bytes in big endian format
  return convert_toINT_be(context, value, out_len);
}

// Converts a timestamp variable to binary
FORCE_INLINE
const char* convert_toTIMESTAMP_EPOCH(int64_t context, int64_t timestamp,
                                      int32_t* out_len) {
  return convert_toBIGINT(context, timestamp, out_len);
}

FORCE_INLINE
const char* convert_toTIMESTAMP_EPOCH_be(int64_t context, int64_t timestamp,
                                         int32_t* out_len) {
  // The function behaves as convert_toTIMESTAMP_EPOCH, but
  // returns the bytes in big endian format
  return convert_toBIGINT_be(context, timestamp, out_len);
}

// Converts a date variable to binary
FORCE_INLINE
const char* convert_toDATE_EPOCH(int64_t context, int64_t date, int32_t* out_len) {
  return convert_toBIGINT(context, date, out_len);
}

FORCE_INLINE
const char* convert_toDATE_EPOCH_be(int64_t context, int64_t date, int32_t* out_len) {
  // The function behaves as convert_toDATE_EPOCH, but
  // returns the bytes in big endian format
  return convert_toBIGINT_be(context, date, out_len);
}

// Converts a string variable to binary
FORCE_INLINE
const char* convert_toUTF8(int64_t context, const char* value, int32_t value_len,
                           int32_t* out_len) {
  *out_len = value_len;
  return value;
}

// Calculate the levenshtein distance between two string values
FORCE_INLINE
gdv_int32 levenshtein(int64_t context, const char* in1, int32_t in1_len, const char* in2,
                      int32_t in2_len) {
  if (in1_len < 0 || in2_len < 0) {
    gdv_fn_context_set_error_msg(context, "String length must be greater than 0");
    return 0;
  }

  // Check input size 0
  if (in1_len == 0) {
    return in2_len;
  }
  if (in2_len == 0) {
    return in1_len;
  }

  // arr_larger and arr_smaller is one pointer for entrys
  const char* arr_larger;
  const char* arr_smaller;
  // len_larger and len_smaller is one copy from lengths
  int len_larger;
  int len_smaller;

  if (in1_len < in2_len) {
    len_larger = in2_len;
    arr_larger = in2;

    len_smaller = in1_len;
    arr_smaller = in1;
  } else {
    len_larger = in1_len;
    arr_larger = in1;

    len_smaller = in2_len;
    arr_smaller = in2;
  }

  int* ptr =
      reinterpret_cast<int*>(gdv_fn_context_arena_malloc(context, (len_smaller + 1) * 2));
  if (ptr == nullptr) {
    gdv_fn_context_set_error_msg(context, "String length must be greater than 0");
    return 0;
  }

  // MEMORY ADRESS MALLOC
  // v0 -> (0, ..., &ptr[in2_len])
  // v1 -> (in2_len+1, ..., &ptr[in2_len * 2])
  int* v0;
  int* v1;
  int* aux;
  v0 = &ptr[0];
  v1 = &ptr[len_smaller + 1];

  // Initializate v0
  for (int i = 0; i <= len_smaller; i++) {
    v0[i] = i;
  }

  // Initialize interactive mode
  for (int i = 0; i < len_larger; i++) {
    // The first element to V1 is [i + 1]
    // For edit distance you can delete (i+1) chars from in1 to match empty in2 position
    v1[0] = i + 1;

    for (int j = 0; j < len_smaller; j++) {
      // Calculate costs to modify
      int deletionCost = v0[j + 1] + 1;
      int insertionCost = v1[j] + 1;
      int substitutionCost = v0[j] + 1;

      if (arr_larger[i] == arr_smaller[j]) {
        substitutionCost = v0[j];
      }

      // Catch the minor cost
      int min;
      min = deletionCost;

      if (min > substitutionCost) {
        min = substitutionCost;
      }
      if (min > insertionCost) {
        min = insertionCost;
      }

      // Set the minor cost to v1
      v1[j + 1] = min;
    }

    // Swaping v0 and v1
    aux = v0;
    v0 = v1;
    v1 = aux;
  }
  // The results of v1 are now in v0, Levenshtein value is in v0[n]
  return v0[len_smaller];
}

// Search for a string within another string
// Same as "locate(substr, str)", except for the reverse order of the arguments.
FORCE_INLINE
gdv_int32 strpos_utf8_utf8(gdv_int64 context, const char* str, gdv_int32 str_len,
                           const char* sub_str, gdv_int32 sub_str_len) {
  return locate_utf8_utf8_int32(context, sub_str, sub_str_len, str, str_len, 1);
}

// Search for a string within another string
FORCE_INLINE
gdv_int32 locate_utf8_utf8(gdv_int64 context, const char* sub_str, gdv_int32 sub_str_len,
                           const char* str, gdv_int32 str_len) {
  return locate_utf8_utf8_int32(context, sub_str, sub_str_len, str, str_len, 1);
}

// Search for a string within another string starting at position start-pos (1-indexed)
FORCE_INLINE
gdv_int32 locate_utf8_utf8_int32(gdv_int64 context, const char* sub_str,
                                 gdv_int32 sub_str_len, const char* str,
                                 gdv_int32 str_len, gdv_int32 start_pos) {
  if (start_pos < 1) {
    gdv_fn_context_set_error_msg(context, "Start position must be greater than 0");
    return 0;
  }

  if (str_len == 0 || sub_str_len == 0) {
    return 0;
  }

  gdv_int32 byte_pos = utf8_byte_pos(context, str, str_len, start_pos - 1);
  if (byte_pos < 0 || byte_pos >= str_len) {
    return 0;
  }
  for (gdv_int32 i = byte_pos; i <= str_len - sub_str_len; ++i) {
    if (memcmp(str + i, sub_str, sub_str_len) == 0) {
      return utf8_length(context, str, i) + 1;
    }
  }
  return 0;
}

FORCE_INLINE
const char* replace_with_max_len_utf8_utf8_utf8(gdv_int64 context, const char* text,
                                                gdv_int32 text_len, const char* from_str,
                                                gdv_int32 from_str_len,
                                                const char* to_str, gdv_int32 to_str_len,
                                                gdv_int32 max_length,
                                                gdv_int32* out_len) {
  // if from_str is empty or its length exceeds that of original string,
  // return the original string
  if (from_str_len <= 0 || from_str_len > text_len) {
    *out_len = text_len;
    return text;
  }

  bool found = false;
  gdv_int32 text_index = 0;
  char* out;
  gdv_int32 out_index = 0;
  gdv_int32 last_match_index =
      0;  // defer copying string from last_match_index till next match is found

  for (; text_index <= text_len - from_str_len;) {
    if (memcmp(text + text_index, from_str, from_str_len) == 0) {
      if (out_index + text_index - last_match_index + to_str_len > max_length) {
        gdv_fn_context_set_error_msg(context, "Buffer overflow for output string");
        *out_len = 0;
        return "";
      }
      if (!found) {
        // found match for first time
        out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, max_length));
        if (out == nullptr) {
          gdv_fn_context_set_error_msg(context,
                                       "Could not allocate memory for output string");
          *out_len = 0;
          return "";
        }
        found = true;
      }
      // first copy the part deferred till now
      memcpy(out + out_index, text + last_match_index, (text_index - last_match_index));
      out_index += text_index - last_match_index;
      // then copy the target string
      memcpy(out + out_index, to_str, to_str_len);
      out_index += to_str_len;

      text_index += from_str_len;
      last_match_index = text_index;
    } else {
      text_index++;
    }
  }

  if (!found) {
    *out_len = text_len;
    return text;
  }

  if (out_index + text_len - last_match_index > max_length) {
    gdv_fn_context_set_error_msg(context, "Buffer overflow for output string");
    *out_len = 0;
    return "";
  }
  memcpy(out + out_index, text + last_match_index, text_len - last_match_index);
  out_index += text_len - last_match_index;
  *out_len = out_index;
  return out;
}

FORCE_INLINE
const char* replace_utf8_utf8_utf8(gdv_int64 context, const char* text,
                                   gdv_int32 text_len, const char* from_str,
                                   gdv_int32 from_str_len, const char* to_str,
                                   gdv_int32 to_str_len, gdv_int32* out_len) {
  return replace_with_max_len_utf8_utf8_utf8(context, text, text_len, from_str,
                                             from_str_len, to_str, to_str_len, 65535,
                                             out_len);
}

// Returns the quoted string (Includes escape character for any single quotes)
// E.g. DONT  -> 'DONT'
//      DON'T -> 'DON\'T'
FORCE_INLINE
const char* quote_utf8(gdv_int64 context, const char* in, gdv_int32 in_len,
                       gdv_int32* out_len) {
  if (in_len <= 0) {
    *out_len = 0;
    return "";
  }
  // try to allocate double size output string (worst case)
  auto out =
      reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, (in_len * 2) + 2));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  // The output string should start with a single quote
  out[0] = '\'';
  gdv_int32 counter = 1;
  for (int i = 0; i < in_len; i++) {
    if (memcmp(in + i, "'", 1) == 0) {
      out[counter] = '\\';
      counter++;
      out[counter] = '\'';
    } else {
      out[counter] = in[i];
    }
    counter++;
  }
  out[counter] = '\'';
  *out_len = counter + 1;
  return out;
}

FORCE_INLINE
gdv_int32 evaluate_return_char_length(gdv_int32 text_len, gdv_int32 actual_text_len,
                                      gdv_int32 return_length, const char* fill_text,
                                      gdv_int32 fill_text_len) {
  gdv_int32 fill_actual_text_len = utf8_length_ignore_invalid(fill_text, fill_text_len);
  gdv_int32 repeat_times = (return_length - actual_text_len) / fill_actual_text_len;
  gdv_int32 return_char_length = repeat_times * fill_text_len + text_len;
  gdv_int32 mod = (return_length - actual_text_len) % fill_actual_text_len;
  gdv_int32 char_len = 0;
  gdv_int32 fill_index = 0;
  for (gdv_int32 i = 0; i < mod; i++) {
    char_len = utf8_char_length(fill_text[fill_index]);
    fill_index += char_len;
    return_char_length += char_len;
  }
  return return_char_length;
}

FORCE_INLINE
const char* lpad_utf8_int32_utf8(gdv_int64 context, const char* text, gdv_int32 text_len,
                                 gdv_int32 return_length, const char* fill_text,
                                 gdv_int32 fill_text_len, gdv_int32* out_len) {
  // if the text length or the defined return length (number of characters to return)
  // is <=0, then return an empty string.
  return_length = std::min(max_str_length, return_length);
  return_length = std::max(min_str_length, return_length);
  if (text_len == 0 || return_length <= 0) {
    *out_len = 0;
    return "";
  }

  // count the number of utf8 characters on text, ignoring invalid bytes
  int actual_text_len = utf8_length_ignore_invalid(text, text_len);

  if (return_length == actual_text_len ||
      (return_length > actual_text_len && fill_text_len == 0)) {
    // case where the return length is same as the text's length, or if it need to
    // fill into text but "fill_text" is empty, then return text directly.
    *out_len = text_len;
    return text;
  } else if (return_length < actual_text_len) {
    // case where it truncates the result on return length.
    *out_len = utf8_byte_pos(context, text, text_len, return_length);
    return text;
  } else {
    // case (return_length > actual_text_len)
    // case where it needs to copy "fill_text" on the string left. The total number
    // of chars to copy is given by (return_length -  actual_text_len)
    gdv_int32 return_char_length = evaluate_return_char_length(
        text_len, actual_text_len, return_length, fill_text, fill_text_len);
    char* ret = reinterpret_cast<gdv_binary>(
        gdv_fn_context_arena_malloc(context, return_char_length));
    if (ret == nullptr) {
      gdv_fn_context_set_error_msg(context,
                                   "Could not allocate memory for output string");
      *out_len = 0;
      return "";
    }
    // try to fulfill the return string with the "fill_text" continuously
    int32_t copied_chars_count = 0;
    int32_t copied_chars_position = 0;
    while (copied_chars_count < return_length - actual_text_len) {
      int32_t char_len;
      int32_t fill_index;
      // for each char, evaluate its length to consider it when mem copying
      for (fill_index = 0; fill_index < fill_text_len; fill_index += char_len) {
        if (copied_chars_count >= return_length - actual_text_len) {
          break;
        }
        char_len = utf8_char_length(fill_text[fill_index]);
        // ignore invalid char on the fill text, considering it as size 1
        if (char_len == 0) char_len += 1;
        copied_chars_count++;
      }
      memcpy(ret + copied_chars_position, fill_text, fill_index);
      copied_chars_position += fill_index;
    }
    // after fulfilling the text, copy the main string
    memcpy(ret + copied_chars_position, text, text_len);
    *out_len = copied_chars_position + text_len;
    return ret;
  }
}

FORCE_INLINE
const char* rpad_utf8_int32_utf8(gdv_int64 context, const char* text, gdv_int32 text_len,
                                 gdv_int32 return_length, const char* fill_text,
                                 gdv_int32 fill_text_len, gdv_int32* out_len) {
  // if the text length or the defined return length (number of characters to return)
  // is <=0, then return an empty string.
  return_length = std::min(max_str_length, return_length);
  return_length = std::max(min_str_length, return_length);
  if (text_len == 0 || return_length <= 0) {
    *out_len = 0;
    return "";
  }

  // count the number of utf8 characters on text, ignoring invalid bytes
  int actual_text_len = utf8_length_ignore_invalid(text, text_len);

  if (return_length == actual_text_len ||
      (return_length > actual_text_len && fill_text_len == 0)) {
    // case where the return length is same as the text's length, or if it need to
    // fill into text but "fill_text" is empty, then return text directly.
    *out_len = text_len;
    return text;
  } else if (return_length < actual_text_len) {
    // case where it truncates the result on return length.
    *out_len = utf8_byte_pos(context, text, text_len, return_length);
    return text;
  } else {
    // case (return_length > actual_text_len)
    // case where it needs to copy "fill_text" on the string right
    gdv_int32 return_char_length = evaluate_return_char_length(
        text_len, actual_text_len, return_length, fill_text, fill_text_len);
    char* ret = reinterpret_cast<gdv_binary>(
        gdv_fn_context_arena_malloc(context, return_char_length));
    if (ret == nullptr) {
      gdv_fn_context_set_error_msg(context,
                                   "Could not allocate memory for output string");
      *out_len = 0;
      return "";
    }
    // fulfill the initial text copying the main input string
    memcpy(ret, text, text_len);
    // try to fulfill the return string with the "fill_text" continuously
    int32_t copied_chars_count = 0;
    int32_t copied_chars_position = 0;
    while (actual_text_len + copied_chars_count < return_length) {
      int32_t char_len;
      int32_t fill_length;
      // for each char, evaluate its length to consider it when mem copying
      for (fill_length = 0; fill_length < fill_text_len; fill_length += char_len) {
        if (actual_text_len + copied_chars_count >= return_length) {
          break;
        }
        char_len = utf8_char_length(fill_text[fill_length]);
        // ignore invalid char on the fill text, considering it as size 1
        if (char_len == 0) char_len += 1;
        copied_chars_count++;
      }
      memcpy(ret + text_len + copied_chars_position, fill_text, fill_length);
      copied_chars_position += fill_length;
    }
    *out_len = copied_chars_position + text_len;
    return ret;
  }
}

FORCE_INLINE
const char* lpad_utf8_int32(gdv_int64 context, const char* text, gdv_int32 text_len,
                            gdv_int32 return_length, gdv_int32* out_len) {
  return lpad_utf8_int32_utf8(context, text, text_len, return_length, " ", 1, out_len);
}

FORCE_INLINE
const char* rpad_utf8_int32(gdv_int64 context, const char* text, gdv_int32 text_len,
                            gdv_int32 return_length, gdv_int32* out_len) {
  return rpad_utf8_int32_utf8(context, text, text_len, return_length, " ", 1, out_len);
}

FORCE_INLINE
const char* split_part(gdv_int64 context, const char* text, gdv_int32 text_len,
                       const char* delimiter, gdv_int32 delim_len, gdv_int32 index,
                       gdv_int32* out_len) {
  *out_len = 0;
  if (index < 1) {
    char error_message[100];
    snprintf(error_message, sizeof(error_message),
             "Index in split_part must be positive, value provided was %d", index);
    gdv_fn_context_set_error_msg(context, error_message);
    return "";
  }

  if (delim_len == 0 || text_len == 0) {
    // output will just be text if no delimiter is provided
    *out_len = text_len;
    return text;
  }

  int i = 0, match_no = 1;

  while (i < text_len) {
    // find the position where delimiter matched for the first time
    int match_pos = match_string(text, text_len, i, delimiter, delim_len);
    if (match_pos == -1 && match_no != index) {
      // reached the end without finding a match.
      return "";
    } else {
      // Found a match. If the match number is index then return this match
      if (match_no == index) {
        int end_pos = match_pos - delim_len;

        if (match_pos == -1) {
          // end position should be last position of the string as we have the last
          // delimiter
          end_pos = text_len;
        }

        *out_len = end_pos - i;
        return text + i;
      } else {
        i = match_pos;
        match_no++;
      }
    }
  }

  return "";
}

// Returns the x leftmost characters of a given string. Cases:
//     LEFT("TestString", 10) => "TestString"
//     LEFT("TestString", 3) => "Tes"
//     LEFT("TestString", -3) => "TestStr"
FORCE_INLINE
const char* left_utf8_int32(gdv_int64 context, const char* text, gdv_int32 text_len,
                            gdv_int32 number, gdv_int32* out_len) {
  // returns the 'number' left most characters of a given text
  if (text_len == 0 || number == 0) {
    *out_len = 0;
    return "";
  }

  // iterate over the utf8 string validating each character
  int char_len;
  int char_count = 0;
  int byte_index = 0;
  for (int i = 0; i < text_len; i += char_len) {
    char_len = utf8_char_length(text[i]);
    if (char_len == 0 || i + char_len > text_len) {  // invalid byte or incomplete glyph
      set_error_for_invalid_utf(context, text[i]);
      *out_len = 0;
      return "";
    }
    for (int j = 1; j < char_len; ++j) {
      if ((text[i + j] & 0xC0) != 0x80) {  // bytes following head-byte of glyph
        set_error_for_invalid_utf(context, text[i + j]);
        *out_len = 0;
        return "";
      }
    }
    byte_index += char_len;
    ++char_count;
    // Define the rules to stop the iteration over the string
    // case where left('abc', 5) -> 'abc'
    if (number > 0 && char_count == number) break;
    // case where left('abc', -5) ==> ''
    if (number < 0 && char_count == number + text_len) break;
  }

  *out_len = byte_index;
  return text;
}

// Returns the x rightmost characters of a given string. Cases:
//     RIGHT("TestString", 10) => "TestString"
//     RIGHT("TestString", 3) => "ing"
//     RIGHT("TestString", -3) => "tString"
FORCE_INLINE
const char* right_utf8_int32(gdv_int64 context, const char* text, gdv_int32 text_len,
                             gdv_int32 number, gdv_int32* out_len) {
  // returns the 'number' left most characters of a given text
  if (text_len == 0 || number == 0) {
    *out_len = 0;
    return "";
  }

  // initially counts the number of utf8 characters in the defined text
  int32_t char_count = utf8_length(context, text, text_len);
  // char_count is zero if input has invalid utf8 char
  if (char_count == 0) {
    *out_len = 0;
    return "";
  }

  int32_t start_char_pos;  // the char result start position (inclusive)
  int32_t end_char_len;    // the char result end position (inclusive)
  if (number > 0) {
    // case where right('abc', 5) ==> 'abc' start_char_pos=1.
    start_char_pos = (char_count > number) ? char_count - number : 0;
    end_char_len = char_count - start_char_pos;
  } else {
    start_char_pos = number * -1;
    end_char_len = char_count - start_char_pos;
  }

  // calculate the start byte position and the output length
  int32_t start_byte_pos = utf8_byte_pos(context, text, text_len, start_char_pos);
  *out_len = utf8_byte_pos(context, text, text_len, end_char_len);

  // try to allocate memory for the response
  char* ret =
      reinterpret_cast<gdv_binary>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(ret, text + start_byte_pos, *out_len);
  return ret;
}

FORCE_INLINE
const char* binary_string(gdv_int64 context, const char* text, gdv_int32 text_len,
                          gdv_int32* out_len) {
  gdv_binary ret =
      reinterpret_cast<gdv_binary>(gdv_fn_context_arena_malloc(context, text_len));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  if (text_len == 0) {
    *out_len = 0;
    return "";
  }

  // converting hex encoded string to normal string
  int j = 0;
  for (int i = 0; i < text_len; i++, j++) {
    if (text[i] == '\\' && i + 3 < text_len &&
        (text[i + 1] == 'x' || text[i + 1] == 'X')) {
      char hd1 = text[i + 2];
      char hd2 = text[i + 3];
      if (isxdigit(hd1) && isxdigit(hd2)) {
        // [a-fA-F0-9]
        ret[j] = to_binary_from_hex(hd1) * 16 + to_binary_from_hex(hd2);
        i += 3;
      } else {
        ret[j] = text[i];
      }
    } else {
      ret[j] = text[i];
    }
  }
  *out_len = j;
  return ret;
}

#define CAST_INT_BIGINT_VARBINARY(OUT_TYPE, TYPE_NAME)                                 \
  FORCE_INLINE                                                                         \
  OUT_TYPE                                                                             \
  cast##TYPE_NAME##_varbinary(gdv_int64 context, const char* in, int32_t in_len) {     \
    if (in_len == 0) {                                                                 \
      gdv_fn_context_set_error_msg(context, "Can't cast an empty string.");            \
      return -1;                                                                       \
    }                                                                                  \
    char sign = in[0];                                                                 \
                                                                                       \
    bool negative = false;                                                             \
    if (sign == '-') {                                                                 \
      negative = true;                                                                 \
      /* Ignores the sign char in the hexadecimal string */                            \
      in++;                                                                            \
      in_len--;                                                                        \
    }                                                                                  \
                                                                                       \
    if (negative && in_len == 0) {                                                     \
      gdv_fn_context_set_error_msg(context,                                            \
                                   "Can't cast hexadecimal with only a minus sign.");  \
      return -1;                                                                       \
    }                                                                                  \
                                                                                       \
    OUT_TYPE result = 0;                                                               \
    int digit;                                                                         \
                                                                                       \
    int read_index = 0;                                                                \
    while (read_index < in_len) {                                                      \
      char c1 = in[read_index];                                                        \
      if (isxdigit(c1)) {                                                              \
        digit = to_binary_from_hex(c1);                                                \
                                                                                       \
        OUT_TYPE next = result * 16 - digit;                                           \
                                                                                       \
        if (next > result) {                                                           \
          gdv_fn_context_set_error_msg(context, "Integer overflow.");                  \
          return -1;                                                                   \
        }                                                                              \
        result = next;                                                                 \
        read_index++;                                                                  \
      } else {                                                                         \
        gdv_fn_context_set_error_msg(context,                                          \
                                     "The hexadecimal given has invalid characters."); \
        return -1;                                                                     \
      }                                                                                \
    }                                                                                  \
    if (!negative) {                                                                   \
      result *= -1;                                                                    \
                                                                                       \
      if (result < 0) {                                                                \
        gdv_fn_context_set_error_msg(context, "Integer overflow.");                    \
        return -1;                                                                     \
      }                                                                                \
    }                                                                                  \
    return result;                                                                     \
  }

CAST_INT_BIGINT_VARBINARY(int32_t, INT)
CAST_INT_BIGINT_VARBINARY(int64_t, BIGINT)

#undef CAST_INT_BIGINT_VARBINARY

// Produces the binary representation of a string y characters long derived by starting
// at offset 'x' and considering the defined length 'y'. Notice that the offset index
// may be a negative number (starting from the end of the string), or a positive number
// starting on index 1. Cases:
//     BYTE_SUBSTR("TestString", 1, 10) => "TestString"
//     BYTE_SUBSTR("TestString", 5, 10) => "String"
//     BYTE_SUBSTR("TestString", -6, 10) => "String"
//     BYTE_SUBSTR("TestString", -600, 10) => "TestString"
FORCE_INLINE
const char* byte_substr_binary_int32_int32(gdv_int64 context, const char* text,
                                           gdv_int32 text_len, gdv_int32 offset,
                                           gdv_int32 length, gdv_int32* out_len) {
  // the first offset position for a string is 1, so not consider offset == 0
  // also, the length should be always a positive number
  if (text_len == 0 || offset == 0 || length <= 0) {
    *out_len = 0;
    return "";
  }

  char* ret =
      reinterpret_cast<gdv_binary>(gdv_fn_context_arena_malloc(context, text_len));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  int32_t startPos = 0;
  if (offset >= 0) {
    startPos = offset - 1;
  } else if (text_len + offset >= 0) {
    startPos = text_len + offset;
  }

  // calculate end position from length and truncate to upper value bounds
  if (startPos + length > text_len) {
    *out_len = text_len - startPos;
  } else {
    *out_len = length;
  }

  memcpy(ret, text + startPos, *out_len);
  return ret;
}

FORCE_INLINE
const char* concat_ws_utf8_utf8(int64_t context, const char* separator,
                                int32_t separator_len, const char* word1,
                                int32_t word1_len, const char* word2, int32_t word2_len,
                                int32_t* out_len) {
  if (word1_len < 0 || word2_len < 0 || separator_len < 0) {
    gdv_fn_context_set_error_msg(context, "All words can not be null.");
    *out_len = 0;
    return "";
  }

  *out_len = word1_len + separator_len + word2_len;
  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  char* tmp = out;
  memcpy(tmp, word1, word1_len);
  tmp += word1_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word2, word2_len);

  return out;
}

FORCE_INLINE
const char* concat_ws_utf8_utf8_utf8(int64_t context, const char* separator,
                                     int32_t separator_len, const char* word1,
                                     int32_t word1_len, const char* word2,
                                     int32_t word2_len, const char* word3,
                                     int32_t word3_len, int32_t* out_len) {
  if (word1_len < 0 || word2_len < 0 || word3_len < 0 || separator_len < 0) {
    gdv_fn_context_set_error_msg(context, "All words can not be null.");
    *out_len = 0;
    return "";
  }

  *out_len = word1_len + word2_len + word3_len + (2 * separator_len);
  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  char* tmp = out;
  memcpy(tmp, word1, word1_len);
  tmp += word1_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word2, word2_len);
  tmp += word2_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word3, word3_len);

  return out;
}

FORCE_INLINE
const char* concat_ws_utf8_utf8_utf8_utf8(int64_t context, const char* separator,
                                          int32_t separator_len, const char* word1,
                                          int32_t word1_len, const char* word2,
                                          int32_t word2_len, const char* word3,
                                          int32_t word3_len, const char* word4,
                                          int32_t word4_len, int32_t* out_len) {
  if (word1_len < 0 || word2_len < 0 || word3_len < 0 || word4_len < 0 ||
      separator_len < 0) {
    gdv_fn_context_set_error_msg(context, "All words can not be null.");
    *out_len = 0;
    return "";
  }

  *out_len = word1_len + word2_len + word3_len + word4_len + (3 * separator_len);
  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  char* tmp = out;
  memcpy(tmp, word1, word1_len);
  tmp += word1_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word2, word2_len);
  tmp += word2_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word3, word3_len);
  tmp += word3_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word4, word4_len);

  return out;
}

FORCE_INLINE
const char* concat_ws_utf8_utf8_utf8_utf8_utf8(int64_t context, const char* separator,
                                               int32_t separator_len, const char* word1,
                                               int32_t word1_len, const char* word2,
                                               int32_t word2_len, const char* word3,
                                               int32_t word3_len, const char* word4,
                                               int32_t word4_len, const char* word5,
                                               int32_t word5_len, int32_t* out_len) {
  if (word1_len < 0 || word2_len < 0 || word3_len < 0 || word4_len < 0 || word5_len < 0 ||
      separator_len < 0) {
    gdv_fn_context_set_error_msg(context, "All words can not be null.");
    *out_len = 0;
    return "";
  }

  *out_len =
      word1_len + word2_len + word3_len + word4_len + word5_len + (4 * separator_len);
  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  char* tmp = out;
  memcpy(tmp, word1, word1_len);
  tmp += word1_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word2, word2_len);
  tmp += word2_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word3, word3_len);
  tmp += word3_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word4, word4_len);
  tmp += word4_len;
  memcpy(tmp, separator, separator_len);
  tmp += separator_len;
  memcpy(tmp, word5, word5_len);

  return out;
}

FORCE_INLINE
const char* elt_int32_utf8_utf8(int32_t pos, bool pos_validity, const char* word1,
                                int32_t word1_len, bool in1_validity, const char* word2,
                                int32_t word2_len, bool in2_validity, bool* out_valid,
                                int32_t* out_len) {
  *out_valid = true;

  switch (pos) {
    case 1:
      *out_len = word1_len;
      return word1;
      break;
    case 2:
      *out_len = word2_len;
      return word2;
      break;
    default:
      *out_len = 0;
      *out_valid = false;
      return nullptr;
  }
}

FORCE_INLINE
const char* elt_int32_utf8_utf8_utf8(int32_t pos, bool pos_validity, const char* word1,
                                     int32_t word1_len, bool word1_validity,
                                     const char* word2, int32_t word2_len,
                                     bool word2_validity, const char* word3,
                                     int32_t word3_len, bool word3_validity,
                                     bool* out_valid, int32_t* out_len) {
  *out_valid = true;

  switch (pos) {
    case 1:
      *out_len = word1_len;
      return word1;
      break;
    case 2:
      *out_len = word2_len;
      return word2;
      break;
    case 3:
      *out_len = word3_len;
      return word3;
      break;
    default:
      *out_len = 0;
      *out_valid = false;
      return nullptr;
  }
}

FORCE_INLINE
const char* elt_int32_utf8_utf8_utf8_utf8(
    int32_t pos, bool pos_validity, const char* word1, int32_t word1_len,
    bool word1_validity, const char* word2, int32_t word2_len, bool word2_validity,
    const char* word3, int32_t word3_len, bool word3_validity, const char* word4,
    int32_t word4_len, bool word4_validity, bool* out_valid, int32_t* out_len) {
  *out_valid = true;

  switch (pos) {
    case 1:
      *out_len = word1_len;
      return word1;
      break;
    case 2:
      *out_len = word2_len;
      return word2;
      break;
    case 3:
      *out_len = word3_len;
      return word3;
      break;
    case 4:
      *out_len = word4_len;
      return word4;
      break;
    default:
      *out_len = 0;
      *out_valid = false;
      return nullptr;
  }
}

FORCE_INLINE
const char* elt_int32_utf8_utf8_utf8_utf8_utf8(
    int32_t pos, bool pos_validity, const char* word1, int32_t word1_len,
    bool word1_validity, const char* word2, int32_t word2_len, bool word2_validity,
    const char* word3, int32_t word3_len, bool word3_validity, const char* word4,
    int32_t word4_len, bool word4_validity, const char* word5, int32_t word5_len,
    bool word5_validity, bool* out_valid, int32_t* out_len) {
  *out_valid = true;

  switch (pos) {
    case 1:
      *out_len = word1_len;
      return word1;
      break;
    case 2:
      *out_len = word2_len;
      return word2;
      break;
    case 3:
      *out_len = word3_len;
      return word3;
      break;
    case 4:
      *out_len = word4_len;
      return word4;
      break;
    case 5:
      *out_len = word5_len;
      return word5;
      break;
    default:
      *out_len = 0;
      *out_valid = false;
      return nullptr;
  }
}

// Gets a binary object and returns its hexadecimal representation. That representation
// maps each byte in the input to a 2-length string containing a hexadecimal number.
// - Examples:
//     - foo -> 666F6F = 66[f] 6F[o] 6F[o]
//     - bar -> 626172 = 62[b] 61[a] 72[r]
FORCE_INLINE
const char* to_hex_binary(int64_t context, const char* text, int32_t text_len,
                          int32_t* out_len) {
  if (text_len == 0) {
    *out_len = 0;
    return "";
  }

  auto ret =
      reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, text_len * 2 + 1));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  uint32_t ret_index = 0;
  uint32_t max_len = static_cast<uint32_t>(text_len) * 2;
  uint32_t max_char_to_write = 4;

  for (gdv_int32 i = 0; i < text_len; i++) {
    DCHECK(ret_index >= 0 && ret_index < max_len);

    int32_t ch = static_cast<int32_t>(text[i]) & 0xFF;

    ret_index += snprintf(ret + ret_index, max_char_to_write, "%02X", ch);
  }

  *out_len = static_cast<int32_t>(ret_index);
  return ret;
}

FORCE_INLINE
const char* to_hex_int64(int64_t context, int64_t data, int32_t* out_len) {
  const int64_t hex_long_max_size = 2 * sizeof(int64_t);
  auto ret =
      reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, hex_long_max_size));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  snprintf(ret, hex_long_max_size + 1, "%" PRIX64, data);

  *out_len = static_cast<int32_t>(strlen(ret));
  return ret;
}

FORCE_INLINE
const char* to_hex_int32(int64_t context, int32_t data, int32_t* out_len) {
  const int32_t max_size = 2 * sizeof(int32_t);
  auto ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, max_size));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  snprintf(ret, max_size + 1, "%" PRIX32, data);

  *out_len = static_cast<int32_t>(strlen(ret));
  return ret;
}

FORCE_INLINE
const char* from_hex_utf8(int64_t context, const char* text, int32_t text_len,
                          int32_t* out_len) {
  if (text_len == 0) {
    *out_len = 0;
    return "";
  }

  // the input string should have a length multiple of two
  if (text_len % 2 != 0) {
    gdv_fn_context_set_error_msg(
        context, "Error parsing hex string, length was not a multiple of two.");
    *out_len = 0;
    return "";
  }

  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, text_len / 2));

  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  // converting hex encoded string to normal string
  int32_t j = 0;
  for (int32_t i = 0; i < text_len; i += 2) {
    char b1 = text[i];
    char b2 = text[i + 1];
    if (isxdigit(b1) && isxdigit(b2)) {
      // [a-fA-F0-9]
      ret[j++] = to_binary_from_hex(b1) * 16 + to_binary_from_hex(b2);
    } else {
      gdv_fn_context_set_error_msg(
          context, "Error parsing hex string, one or more bytes are not valid.");
      *out_len = 0;
      return "";
    }
  }
  *out_len = j;
  return ret;
}

// Array that maps each letter from the alphabet to its corresponding number for the
// soundex algorithm. ABCDEFGHIJKLMNOPQRSTUVWXYZ -> 01230120022455012623010202
static char mappings[] = {'0', '1', '2', '3', '0', '1', '2', '0', '0',
                          '2', '2', '4', '5', '5', '0', '1', '2', '6',
                          '2', '3', '0', '1', '0', '2', '0', '2'};

// Returns the soundex code for a given string
//
// The soundex function evaluates expression and returns the most significant letter in
// the input string followed by a phonetic code. Characters that are not alphabetic are
// ignored. If expression evaluates to the null value, null is returned.
//
// The soundex algorith works with the following steps:
//    1. Retain the first letter of the string and drop all other occurrences of a, e, i,
//    o, u, y, h, w.
//    2. Replace consonants with digits as follows (after the first letter):
//        b, f, p, v → 1
//        c, g, j, k, q, s, x, z → 2
//        d, t → 3
//        l → 4
//        m, n → 5
//        r → 6
//    3. If two or more letters with the same number are adjacent in the original name
//    (before step 1), only retain the first letter; also two letters with the same number
//    separated by 'h' or 'w' are coded as a single number, whereas such letters separated
//    by a vowel are coded twice. This rule also applies to the first letter.
//    4. If the string have too few letters in the word that you can't assign three
//    numbers, append with zeros until there are three numbers. If you have four or more
//    numbers, retain only the first three.
FORCE_INLINE
const char* soundex_utf8(gdv_int64 ctx, const char* in, gdv_int32 in_len,
                         int32_t* out_len) {
  if (in_len <= 0) {
    *out_len = 0;
    return "";
  }

  // The soundex code is composed by one letter and three numbers
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(ctx, 4));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(ctx, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  int si = 1;
  unsigned char c;

  int start_idx = 0;
  for (int i = 0; i < in_len; ++i) {
    if (isalpha(in[i]) > 0) {
      ret[0] = toupper(in[i]);
      start_idx = i + 1;
      break;
    }
  }

  for (int i = start_idx, l = in_len; i < l; i++) {
    if (isalpha(in[i]) > 0) {
      c = toupper(in[i]) - 65;
      if (mappings[c] != '0') {
        if (mappings[c] != ret[si - 1]) {
          ret[si] = mappings[c];
          si++;
        }

        if (si > 3) break;
      }
    }
  }

  if (si <= 3) {
    while (si <= 3) {
      ret[si] = '0';
      si++;
    }
  }
  *out_len = 4;
  return ret;
}
}  // extern "C"

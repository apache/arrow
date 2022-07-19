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

#include "gandiva/gdv_function_stubs.h"

#include <utf8proc.h>

#include <boost/crc.hpp>
#include <string>
#include <vector>

#include "arrow/util/base64.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/double_conversion.h"
#include "arrow/util/value_parsing.h"

#include "gandiva/encrypt_utils.h"
#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/in_holder.h"
#include "gandiva/interval_holder.h"
#include "gandiva/random_generator_holder.h"
#include "gandiva/to_date_holder.h"

/// Stub functions that can be accessed from LLVM or the pre-compiled library.

extern "C" {

static char mask_array[256] = {
    (char)0,  (char)1,  (char)2,  (char)3,   (char)4,   (char)5,   (char)6,   (char)7,
    (char)8,  (char)9,  (char)10, (char)11,  (char)12,  (char)13,  (char)14,  (char)15,
    (char)16, (char)17, (char)18, (char)19,  (char)20,  (char)21,  (char)22,  (char)23,
    (char)24, (char)25, (char)26, (char)27,  (char)28,  (char)29,  (char)30,  (char)31,
    (char)32, (char)33, (char)34, (char)35,  (char)36,  (char)37,  (char)38,  (char)39,
    (char)40, (char)41, (char)42, (char)43,  (char)44,  (char)45,  (char)46,  (char)47,
    'n',      'n',      'n',      'n',       'n',       'n',       'n',       'n',
    'n',      'n',      (char)58, (char)59,  (char)60,  (char)61,  (char)62,  (char)63,
    (char)64, 'X',      'X',      'X',       'X',       'X',       'X',       'X',
    'X',      'X',      'X',      'X',       'X',       'X',       'X',       'X',
    'X',      'X',      'X',      'X',       'X',       'X',       'X',       'X',
    'X',      'X',      'X',      (char)91,  (char)92,  (char)93,  (char)94,  (char)95,
    (char)96, 'x',      'x',      'x',       'x',       'x',       'x',       'x',
    'x',      'x',      'x',      'x',       'x',       'x',       'x',       'x',
    'x',      'x',      'x',      'x',       'x',       'x',       'x',       'x',
    'x',      'x',      'x',      (char)123, (char)124, (char)125, (char)126, (char)127};

double gdv_fn_random(int64_t ptr) {
  gandiva::RandomGeneratorHolder* holder =
      reinterpret_cast<gandiva::RandomGeneratorHolder*>(ptr);
  return (*holder)();
}

double gdv_fn_random_with_seed(int64_t ptr, int32_t seed, bool seed_validity) {
  gandiva::RandomGeneratorHolder* holder =
      reinterpret_cast<gandiva::RandomGeneratorHolder*>(ptr);
  return (*holder)();
}

bool gdv_fn_in_expr_lookup_int32(int64_t ptr, int32_t value, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<int32_t>* holder = reinterpret_cast<gandiva::InHolder<int32_t>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_int64(int64_t ptr, int64_t value, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<int64_t>* holder = reinterpret_cast<gandiva::InHolder<int64_t>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_decimal(int64_t ptr, int64_t value_high, int64_t value_low,
                                   int32_t precision, int32_t scale, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::DecimalScalar128 value(value_high, value_low, precision, scale);
  gandiva::InHolder<gandiva::DecimalScalar128>* holder =
      reinterpret_cast<gandiva::InHolder<gandiva::DecimalScalar128>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_float(int64_t ptr, float value, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<float>* holder = reinterpret_cast<gandiva::InHolder<float>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_double(int64_t ptr, double value, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<double>* holder = reinterpret_cast<gandiva::InHolder<double>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_utf8(int64_t ptr, const char* data, int data_len,
                                bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<std::string>* holder =
      reinterpret_cast<gandiva::InHolder<std::string>*>(ptr);
  return holder->HasValue(arrow::util::string_view(data, data_len));
}

int32_t gdv_fn_populate_varlen_vector(int64_t context_ptr, int8_t* data_ptr,
                                      int32_t* offsets, int64_t slot,
                                      const char* entry_buf, int32_t entry_len) {
  auto buffer = reinterpret_cast<arrow::ResizableBuffer*>(data_ptr);
  int32_t offset = static_cast<int32_t>(buffer->size());

  // This also sets the size in the buffer.
  auto status = buffer->Resize(offset + entry_len, false /*shrink*/);
  if (!status.ok()) {
    gandiva::ExecutionContext* context =
        reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);

    context->set_error_msg(status.message().c_str());
    return -1;
  }

  // append the new entry.
  memcpy(buffer->mutable_data() + offset, entry_buf, entry_len);

  // update offsets buffer.
  offsets[slot] = offset;
  offsets[slot + 1] = offset + entry_len;
  return 0;
}

#define CRC_FUNCTION(TYPE)                                                          \
  GANDIVA_EXPORT                                                                    \
  int64_t gdv_fn_crc_32_##TYPE(int64_t ctx, const char* input, int32_t input_len) { \
    if (input_len < 0) {                                                            \
      gdv_fn_context_set_error_msg(ctx, "Input length can't be negative");          \
      return 0;                                                                     \
    }                                                                               \
    boost::crc_32_type result;                                                      \
    result.process_bytes(input, input_len);                                         \
    return result.checksum();                                                       \
  }
CRC_FUNCTION(utf8)
CRC_FUNCTION(binary)

int32_t gdv_fn_dec_from_string(int64_t context, const char* in, int32_t in_length,
                               int32_t* precision_from_str, int32_t* scale_from_str,
                               int64_t* dec_high_from_str, uint64_t* dec_low_from_str) {
  arrow::Decimal128 dec;
  auto status = arrow::Decimal128::FromString(std::string(in, in_length), &dec,
                                              precision_from_str, scale_from_str);
  if (!status.ok()) {
    gdv_fn_context_set_error_msg(context, status.message().data());
    return -1;
  }
  *dec_high_from_str = dec.high_bits();
  *dec_low_from_str = dec.low_bits();
  return 0;
}

char* gdv_fn_dec_to_string(int64_t context, int64_t x_high, uint64_t x_low,
                           int32_t x_scale, int32_t* dec_str_len) {
  arrow::Decimal128 dec(arrow::BasicDecimal128(x_high, x_low));
  std::string dec_str = dec.ToString(x_scale);
  *dec_str_len = static_cast<int32_t>(dec_str.length());
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *dec_str_len));
  if (ret == nullptr) {
    std::string err_msg = "Could not allocate memory for string: " + dec_str;
    gdv_fn_context_set_error_msg(context, err_msg.data());
    return nullptr;
  }
  memcpy(ret, dec_str.data(), *dec_str_len);
  return ret;
}

GANDIVA_EXPORT
const char* gdv_fn_base64_encode_binary(int64_t context, const char* in, int32_t in_len,
                                        int32_t* out_len) {
  if (in_len < 0) {
    gdv_fn_context_set_error_msg(context, "Buffer length can not be negative");
    *out_len = 0;
    return "";
  }
  if (in_len == 0) {
    *out_len = 0;
    return "";
  }
  // use arrow method to encode base64 string
  std::string encoded_str =
      arrow::util::base64_encode(arrow::util::string_view(in, in_len));
  *out_len = static_cast<int32_t>(encoded_str.length());
  // allocate memory for response
  char* ret = reinterpret_cast<char*>(
      gdv_fn_context_arena_malloc(context, static_cast<int32_t>(*out_len)));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory");
    *out_len = 0;
    return "";
  }
  memcpy(ret, encoded_str.data(), *out_len);
  return ret;
}

GANDIVA_EXPORT
const char* gdv_fn_base64_decode_utf8(int64_t context, const char* in, int32_t in_len,
                                      int32_t* out_len) {
  if (in_len < 0) {
    gdv_fn_context_set_error_msg(context, "Buffer length can not be negative");
    *out_len = 0;
    return "";
  }
  if (in_len == 0) {
    *out_len = 0;
    return "";
  }
  // use arrow method to decode base64 string
  std::string decoded_str =
      arrow::util::base64_decode(arrow::util::string_view(in, in_len));
  *out_len = static_cast<int32_t>(decoded_str.length());
  // allocate memory for response
  char* ret = reinterpret_cast<char*>(
      gdv_fn_context_arena_malloc(context, static_cast<int32_t>(*out_len)));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory");
    *out_len = 0;
    return "";
  }
  memcpy(ret, decoded_str.data(), *out_len);
  return ret;
}

#define CAST_NUMERIC_FROM_VARLEN_TYPES(OUT_TYPE, ARROW_TYPE, TYPE_NAME, INNER_TYPE)  \
  GANDIVA_EXPORT                                                                     \
  OUT_TYPE gdv_fn_cast##TYPE_NAME##_##INNER_TYPE(int64_t context, const char* data,  \
                                                 int32_t len) {                      \
    OUT_TYPE val = 0;                                                                \
    /* trim leading and trailing spaces */                                           \
    int32_t trimmed_len;                                                             \
    int32_t start = 0, end = len - 1;                                                \
    while (start <= end && data[start] == ' ') {                                     \
      ++start;                                                                       \
    }                                                                                \
    while (end >= start && data[end] == ' ') {                                       \
      --end;                                                                         \
    }                                                                                \
    trimmed_len = end - start + 1;                                                   \
    const char* trimmed_data = data + start;                                         \
    if (!arrow::internal::ParseValue<ARROW_TYPE>(trimmed_data, trimmed_len, &val)) { \
      std::string err =                                                              \
          "Failed to cast the string " + std::string(data, len) + " to " #OUT_TYPE;  \
      gdv_fn_context_set_error_msg(context, err.c_str());                            \
    }                                                                                \
    return val;                                                                      \
  }

#define CAST_NUMERIC_FROM_STRING(OUT_TYPE, ARROW_TYPE, TYPE_NAME) \
  CAST_NUMERIC_FROM_VARLEN_TYPES(OUT_TYPE, ARROW_TYPE, TYPE_NAME, utf8)

CAST_NUMERIC_FROM_STRING(int32_t, arrow::Int32Type, INT)
CAST_NUMERIC_FROM_STRING(int64_t, arrow::Int64Type, BIGINT)
CAST_NUMERIC_FROM_STRING(float, arrow::FloatType, FLOAT4)
CAST_NUMERIC_FROM_STRING(double, arrow::DoubleType, FLOAT8)

#undef CAST_NUMERIC_FROM_STRING

#define CAST_NUMERIC_FROM_VARBINARY(OUT_TYPE, ARROW_TYPE, TYPE_NAME) \
  CAST_NUMERIC_FROM_VARLEN_TYPES(OUT_TYPE, ARROW_TYPE, TYPE_NAME, varbinary)

CAST_NUMERIC_FROM_VARBINARY(int32_t, arrow::Int32Type, INT)
CAST_NUMERIC_FROM_VARBINARY(int64_t, arrow::Int64Type, BIGINT)
CAST_NUMERIC_FROM_VARBINARY(float, arrow::FloatType, FLOAT4)
CAST_NUMERIC_FROM_VARBINARY(double, arrow::DoubleType, FLOAT8)

#undef CAST_NUMERIC_STRING

#undef GDV_FN_CAST_VARCHAR_INTEGER
#undef GDV_FN_CAST_VARCHAR_REAL

// Divide x by m as if x is an unsigned 64-bit integer. Examples:
// unsignedLongDiv(-1, 2) == Long.MAX_VALUE unsignedLongDiv(6, 3) == 2
// unsignedLongDiv(0, 5) == 0
// param x: is treated as unsigned
// param m: is treated as signed
GDV_FORCE_INLINE
int64_t unsigned_long_div(int64_t x, int32_t m) {
  if (x >= 0) {
    return x / m;
  }
  // Let uval be the value of the unsigned long with the same bits as x
  // Two's complement => x = uval - 2*MAX - 2
  // => uval = x + 2*MAX + 2
  // Now, use the fact: (a+b)/c = a/c + b/c + (a%c+b%c)/c
  return x / m + 2 * (LONG_MAX / m) + 2 / m + (x % m + 2 * (LONG_MAX % m) + 2 % m) / m;
}

// Convert value[] into a long. On overflow, return -1 (as mySQL does). If a
// negative digit is found, ignore the suffix starting there.
// param radix: must be between MIN_RADIX and MAX_RADIX
// param fromPos: is the first element that should be considered
// return the result should be treated as an unsigned 64-bit integer.
GDV_FORCE_INLINE
int64_t encode(int32_t radix, int32_t fromPos, const char* value, int32_t valueLen) {
  int64_t val = 0;
  int64_t bound = unsigned_long_div(-1 - radix, radix);

  for (int i = fromPos; i < valueLen && value[i] >= 0; i++) {
    if (val >= bound) {
      if (unsigned_long_div(-1 - value[i], radix) < val) {
        return -1;
      }
    }
    val = val * radix + value[i];
  }
  return val;
}

// Decode val into value[].
// param val: is treated as an unsigned 64-bit integer.
// param radix: must be between MIN_RADIX and MAX_RADIX
GDV_FORCE_INLINE
void decode(uint64_t val, int32_t radix, char* value, int32_t valueLen) {
  for (int i = 0; i < valueLen; i++) {
    value[i] = static_cast<char>(0);
  }

  for (int i = valueLen - 1; val != 0; i--) {
    uint64_t q = unsigned_long_div(val, radix);
    value[i] = static_cast<char>((val - q * radix));
    val = q;
  }
}

// From Decimal to Any Base
GDV_FORCE_INLINE
char character_for_digit(int32_t value, int32_t radix) {
  // This function is similar to Character.forDigit in Java
  int digit = 0;
  digit = value % radix;
  if (digit < 10) {
    return static_cast<char>(digit + '0');
  } else {
    return static_cast<char>(digit + 'A' - 10);
  }
}

// From any base to Decimal
GDV_FORCE_INLINE
int64_t character_digit(char value, int32_t radix, int32_t& valid_entry) {
  // This function is similar to Character.digit in Java
  if ((radix <= 0) || (radix > 36)) {
    valid_entry = -1;
    return -1;
  }

  if (radix <= 10) {
    if (value >= '0' && value < '0' + radix) {
      return value - '0';
    } else {
      valid_entry = -1;
      return -1;
    }
  } else if (value >= '0' && value <= '9') {
    return value - '0';
  } else if (value >= 'a' && value < 'a' + radix - 10) {
    return value - 'a' + 10;
  } else if (value >= 'A' && value < 'A' + radix - 10) {
    return value - 'A' + 10;
  }
  valid_entry = -1;
  return -1;
}

// Convert the bytes in value[] to the corresponding chars.
// param radix: must be between MIN_RADIX and MAX_RADIX
// param fromPos: is the first element that should be considered
GDV_FORCE_INLINE
void byte2char(int32_t radix, int32_t fromPos, char* value, int32_t valueLen) {
  for (int i = fromPos; i < valueLen; i++) {
    value[i] = static_cast<char>(character_for_digit(value[i], radix));
  }
}

// Convert the chars in value[] to the corresponding integers. Convert invalid
// characters to -1.
// param radix: must be between MIN_RADIX and MAX_RADIX
// param fromPos: is the first element that should be considered
GDV_FORCE_INLINE
void char2byte(int32_t radix, int32_t fromPos, char* value, int32_t valueLen,
               int32_t* valid_entry) {
  for (int i = fromPos; i < valueLen; i++) {
    value[i] = static_cast<char>(character_digit(value[i], radix, *valid_entry));
    if (*valid_entry != 1) {
      break;
    }
  }
}

GANDIVA_EXPORT
const char* conv_int64_int32_int32(int64_t context, int64_t in, int32_t from_base,
                                   int32_t to_base, int32_t* out_len) {
  std::string to_utf8 = std::to_string(in);
  char* in_utf8 = &to_utf8[0];
  auto in_utf8_len = static_cast<int32_t>(to_utf8.length());

  return conv_utf8_int32_int32(context, in_utf8, in_utf8_len, from_base, to_base,
                               out_len);
}

GANDIVA_EXPORT
const char* conv_int32_int32_int32(int64_t context, int32_t in, int32_t from_base,
                                   int32_t to_base, int32_t* out_len) {
  std::string to_utf8 = std::to_string(in);
  char* in_utf8 = &to_utf8[0];
  auto in_utf8_len = static_cast<int32_t>(to_utf8.length());

  return conv_utf8_int32_int32(context, in_utf8, in_utf8_len, from_base, to_base,
                               out_len);
}

GANDIVA_EXPORT
const char* conv_utf8_int32_int32(int64_t context, const char* in, int32_t in_len,
                                  int32_t from_base, int32_t to_base, int32_t* out_len) {
  if (in_len <= 0) {
    *out_len = 0;
    return "";
  }

  int32_t valueLen = 64;
  char* value = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, valueLen));
  char* num = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, in_len));

  if (value == nullptr || num == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  int fromBs = from_base;
  int toBs = to_base;

  if (fromBs < -36 || fromBs > 36 || fromBs == 0 || fromBs == 1 || abs(toBs) < -36 ||
      abs(toBs) > 36 || abs(toBs) == 0 || abs(toBs) == 1) {
    // Checking if the variable is in range limit
    gdv_fn_context_set_error_msg(context,
                                 "The numerical limit of this variable is out range");
    *out_len = 0;
    return "";
  }

  // Copying entry to new variable, for apply manipulations
  memcpy(num, in, in_len);

  // Validating if the entry is negative
  gdv_boolean negative = (num[0] == '-');
  int first = 0;
  if (negative) {
    first = 1;
  }

  for (int i = 1; i <= in_len - first; i++) {
    // Making a copy the Num array in ending of Value array
    value[valueLen - i] = num[in_len - i];
  }

  // Char to byte, this function calls one function similar to Character.digit in Java
  int32_t valid_entry = 1;
  char2byte(fromBs, valueLen - in_len + first, value, valueLen, &valid_entry);

  // If valid_entry returns -1, had any problem with the entry, it is out of base or have
  // invalid characters
  if (valid_entry != 1) {
    gdv_fn_context_set_error_msg(context, "This entry is invalid");
    *out_len = 0;
    return "";
  }

  // Return a long value with the entry value converted to base 10
  int64_t val = encode(fromBs, valueLen - in_len + first, value, valueLen);

  if (negative && toBs > 0) {
    if (val < 0) {
      val = -1;
    } else {
      val = -val;
    }
  }

  if (toBs < 0 && val < 0) {
    val = -val;
    negative = true;
  }

  decode(val, abs(toBs), value, valueLen);

  for (first = 0; first < valueLen - 1 && value[first] == 0; first++) {
    // Find the first non-zero digit or the last digits if all are zero.
    {}
  }

  // Byte to char, this function calls one function similar to Character.forDigit in Java
  byte2char(abs(toBs), first, value, valueLen);

  if (negative && toBs < 0) {
    // Add signal if the entry value is negative
    value[--first] = '-';
  }

  *out_len = valueLen - first;
  return &value[first];
}

static constexpr int64_t kAesBlockSize = 16;  // bytes

GANDIVA_EXPORT
const char* gdv_fn_aes_encrypt(int64_t context, const char* data, int32_t data_len,
                               const char* key_data, int32_t key_data_len,
                               int32_t* out_len) {
  if (data_len < 0) {
    gdv_fn_context_set_error_msg(context, "Invalid data length to be encrypted");
    *out_len = 0;
    return "";
  }

  *out_len =
      static_cast<int32_t>(arrow::bit_util::RoundUpToPowerOf2(data_len, kAesBlockSize));
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    std::string err_msg =
        "Could not allocate memory for returning aes encrypt cypher text";
    gdv_fn_context_set_error_msg(context, err_msg.data());
    return nullptr;
  }

  try {
    *out_len = gandiva::aes_encrypt(data, data_len, key_data,
                                    reinterpret_cast<unsigned char*>(ret));
  } catch (const std::runtime_error& e) {
    gdv_fn_context_set_error_msg(context, e.what());
    return nullptr;
  }

  return ret;
}

GANDIVA_EXPORT
const char* gdv_fn_aes_decrypt(int64_t context, const char* data, int32_t data_len,
                               const char* key_data, int32_t key_data_len,
                               int32_t* out_len) {
  if (data_len < 0) {
    gdv_fn_context_set_error_msg(context, "Invalid data length to be decrypted");
    *out_len = 0;
    return "";
  }

  *out_len =
      static_cast<int32_t>(arrow::bit_util::RoundUpToPowerOf2(data_len, kAesBlockSize));
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    std::string err_msg =
        "Could not allocate memory for returning aes encrypt cypher text";
    gdv_fn_context_set_error_msg(context, err_msg.data());
    return nullptr;
  }

  try {
    *out_len = gandiva::aes_decrypt(data, data_len, key_data,
                                    reinterpret_cast<unsigned char*>(ret));
  } catch (const std::runtime_error& e) {
    gdv_fn_context_set_error_msg(context, e.what());
    return nullptr;
  }

  return ret;
}

GANDIVA_EXPORT
const char* gdv_mask_first_n_utf8_int32(int64_t context, const char* data,
                                        int32_t data_len, int32_t n_to_mask,
                                        int32_t* out_len) {
  if (data_len <= 0) {
    *out_len = 0;
    return nullptr;
  }

  if (n_to_mask > data_len) {
    n_to_mask = data_len;
  }

  *out_len = data_len;

  if (n_to_mask <= 0) {
    return data;
  }

  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return nullptr;
  }

  int bytes_masked;
  for (bytes_masked = 0; bytes_masked < n_to_mask; bytes_masked++) {
    unsigned char char_single_byte = data[bytes_masked];
    if (char_single_byte > 127) {
      // found a multi-byte utf-8 char
      break;
    }
    out[bytes_masked] = mask_array[char_single_byte];
  }

  int chars_masked = bytes_masked;
  int out_idx = bytes_masked;

  // Handle multibyte utf8 characters
  utf8proc_int32_t utf8_char;
  while ((chars_masked < n_to_mask) && (bytes_masked < data_len)) {
    auto char_len =
        utf8proc_iterate(reinterpret_cast<const utf8proc_uint8_t*>(data + bytes_masked),
                         data_len, &utf8_char);

    if (char_len < 0) {
      gdv_fn_context_set_error_msg(context, utf8proc_errmsg(char_len));
      *out_len = 0;
      return nullptr;
    }

    switch (utf8proc_category(utf8_char)) {
      case 1:
        out[out_idx] = 'X';
        out_idx++;
        break;
      case 2:
        out[out_idx] = 'x';
        out_idx++;
        break;
      case 9:
        out[out_idx] = 'n';
        out_idx++;
        break;
      case 10:
        out[out_idx] = 'n';
        out_idx++;
        break;
      default:
        memcpy(out + out_idx, data + bytes_masked, char_len);
        out_idx += static_cast<int>(char_len);
        break;
    }
    bytes_masked += static_cast<int>(char_len);
    chars_masked++;
  }

  // Correct the out_len after masking multibyte characters with single byte characters
  *out_len = *out_len - (bytes_masked - out_idx);

  if (bytes_masked < data_len) {
    memcpy(out + out_idx, data + bytes_masked, data_len - bytes_masked);
  }

  return out;
}

GANDIVA_EXPORT
const char* gdv_mask_last_n_utf8_int32(int64_t context, const char* data,
                                       int32_t data_len, int32_t n_to_mask,
                                       int32_t* out_len) {
  if (data_len <= 0) {
    *out_len = 0;
    return nullptr;
  }

  if (n_to_mask > data_len) {
    n_to_mask = data_len;
  }

  *out_len = data_len;

  if (n_to_mask <= 0) {
    return data;
  }

  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return nullptr;
  }

  bool has_multi_byte = false;
  for (int i = 0; i < data_len; i++) {
    unsigned char char_single_byte = data[i];
    if (char_single_byte > 127) {
      // found a multi-byte utf-8 char
      has_multi_byte = true;
      break;
    }
  }

  if (!has_multi_byte) {
    int start_idx = data_len - n_to_mask;
    memcpy(out, data, start_idx);
    for (int i = start_idx; i < data_len; ++i) {
      unsigned char char_single_byte = data[i];
      out[i] = mask_array[char_single_byte];
    }
    *out_len = data_len;
    return out;
  }

  utf8proc_int32_t utf8_char_buffer;
  int num_of_chars = static_cast<int>(
      utf8proc_decompose(reinterpret_cast<const utf8proc_uint8_t*>(data), data_len,
                         &utf8_char_buffer, 1, UTF8PROC_STABLE));

  if (num_of_chars < 0) {
    gdv_fn_context_set_error_msg(context, utf8proc_errmsg(num_of_chars));
    *out_len = 0;
    return nullptr;
  }

  utf8proc_int32_t utf8_char;
  int chars_counter = 0;
  int bytes_read = 0;
  while ((bytes_read < data_len) && (chars_counter < (num_of_chars - n_to_mask))) {
    auto char_len =
        utf8proc_iterate(reinterpret_cast<const utf8proc_uint8_t*>(data + bytes_read),
                         data_len, &utf8_char);
    chars_counter++;
    bytes_read += static_cast<int>(char_len);
  }

  int out_idx = bytes_read;
  int offset_idx = bytes_read;

  // Populate the first chars, that are not masked
  memcpy(out, data, offset_idx);

  while (bytes_read < data_len) {
    auto char_len =
        utf8proc_iterate(reinterpret_cast<const utf8proc_uint8_t*>(data + bytes_read),
                         data_len, &utf8_char);
    switch (utf8proc_category(utf8_char)) {
      case 1:
        out[out_idx] = 'X';
        out_idx++;
        break;
      case 2:
        out[out_idx] = 'x';
        out_idx++;
        break;
      case 9:
        out[out_idx] = 'n';
        out_idx++;
        break;
      case 10:
        out[out_idx] = 'n';
        out_idx++;
        break;
      default:
        memcpy(out + out_idx, data + bytes_read, char_len);
        out_idx += static_cast<int>(char_len);
        break;
    }
    bytes_read += static_cast<int>(char_len);
  }

  *out_len = out_idx;

  return out;
}

GANDIVA_EXPORT
const char* mask_utf8_utf8_utf8_utf8(int64_t context, const char* data, int32_t data_len,
                                     const char* upper, int32_t upper_length,
                                     const char* lower, int32_t lower_length,
                                     const char* num, int32_t num_length,
                                     int32_t* out_len) {
  if (data_len <= 0) {
    *out_len = 0;
    return nullptr;
  }

  int32_t max_length =
      std::max(upper_length, std::max(lower_length, num_length)) * data_len;
  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, max_length));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return nullptr;
  }

  bool has_multi_byte = false;
  for (int i = 0; i < data_len; i++) {
    unsigned char char_single_byte = data[i];
    if (char_single_byte > 127) {
      // found a multi-byte utf-8 char
      has_multi_byte = true;
      break;
    }
  }

  if (!has_multi_byte) {
    int out_index = 0;
    for (int i = 0; i < data_len; ++i) {
      unsigned char char_single_byte = data[i];
      if (char_single_byte >= 'A' && char_single_byte <= 'Z') {
        memcpy(out + out_index, upper, upper_length);
        out_index += upper_length;
      } else if (char_single_byte >= 'a' && char_single_byte <= 'z') {
        memcpy(out + out_index, lower, lower_length);
        out_index += lower_length;
      } else if (isdigit(char_single_byte)) {
        memcpy(out + out_index, num, num_length);
        out_index += num_length;
      } else {
        out[out_index] = char_single_byte;
        out_index++;
      }
    }
    *out_len = out_index;
    return out;
  }

  utf8proc_int32_t utf8_char;
  int bytes_read = 0;
  int32_t out_index = 0;
  while (bytes_read < data_len) {
    auto char_len =
        utf8proc_iterate(reinterpret_cast<const utf8proc_uint8_t*>(data + bytes_read),
                         data_len, &utf8_char);
    switch (utf8proc_category(utf8_char)) {
      case UTF8PROC_CATEGORY_LU:
        memcpy(out + out_index, upper, upper_length);
        out_index += upper_length;
        break;
      case UTF8PROC_CATEGORY_LT:
        memcpy(out + out_index, upper, upper_length);
        out_index += upper_length;
        break;
      case UTF8PROC_CATEGORY_LL:
        memcpy(out + out_index, lower, lower_length);
        out_index += lower_length;
        break;
      case UTF8PROC_CATEGORY_LO:
        memcpy(out + out_index, lower, lower_length);
        out_index += lower_length;
        break;
      case UTF8PROC_CATEGORY_ND:
        memcpy(out + out_index, num, num_length);
        out_index += num_length;
        break;
      case UTF8PROC_CATEGORY_NL:
        memcpy(out + out_index, num, num_length);
        out_index += num_length;
        break;
      case UTF8PROC_CATEGORY_NO:
        memcpy(out + out_index, num, num_length);
        out_index += num_length;
        break;
      default:
        memcpy(out + out_index, data + bytes_read, char_len);
        out_index += static_cast<int>(char_len);
        break;
    }
    bytes_read += static_cast<int>(char_len);
  }
  *out_len = out_index;
  return out;
}

GANDIVA_EXPORT
const char* mask_utf8_utf8_utf8(int64_t context, const char* in, int32_t length,
                                const char* upper, int32_t upper_len, const char* lower,
                                int32_t lower_len, int32_t* out_len) {
  return mask_utf8_utf8_utf8_utf8(context, in, length, upper, upper_len, lower, lower_len,
                                  "n", 1, out_len);
}

GANDIVA_EXPORT
const char* mask_utf8_utf8(int64_t context, const char* in, int32_t length,
                           const char* upper, int32_t upper_len, int32_t* out_len) {
  return mask_utf8_utf8_utf8_utf8(context, in, length, upper, upper_len, "x", 1, "n", 1,
                                  out_len);
}

GANDIVA_EXPORT
const char* mask_utf8(int64_t context, const char* in, int32_t length, int32_t* out_len) {
  return mask_utf8_utf8_utf8_utf8(context, in, length, "X", 1, "x", 1, "n", 1, out_len);
}

int64_t gdv_fn_to_date_utf8_utf8(int64_t context_ptr, int64_t holder_ptr,
                                 const char* data, int data_len, bool in1_validity,
                                 const char* pattern, int pattern_len, bool in2_validity,
                                 bool* out_valid) {
  gandiva::ExecutionContext* context =
      reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  gandiva::ToDateHolder* holder = reinterpret_cast<gandiva::ToDateHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

int64_t gdv_fn_to_date_utf8_utf8_int32(int64_t context_ptr, int64_t holder_ptr,
                                       const char* data, int data_len, bool in1_validity,
                                       const char* pattern, int pattern_len,
                                       bool in2_validity, int32_t suppress_errors,
                                       bool in3_validity, bool* out_valid) {
  gandiva::ExecutionContext* context =
      reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  gandiva::ToDateHolder* holder = reinterpret_cast<gandiva::ToDateHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

int64_t gdv_fn_cast_intervalday_utf8(int64_t context_ptr, int64_t holder_ptr,
                                     const char* data, int data_len, bool in1_validity,
                                     bool* out_valid) {
  auto* context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  auto* holder = reinterpret_cast<gandiva::IntervalDaysHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

int64_t gdv_fn_cast_intervalday_utf8_int32(int64_t context_ptr, int64_t holder_ptr,
                                           const char* data, int data_len,
                                           bool in1_validity, int32_t /*suppress_errors*/,
                                           bool /*in3_validity*/, bool* out_valid) {
  auto* context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  auto* holder = reinterpret_cast<gandiva::IntervalDaysHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

int32_t gdv_fn_cast_intervalyear_utf8(int64_t context_ptr, int64_t holder_ptr,
                                      const char* data, int data_len, bool in1_validity,
                                      bool* out_valid) {
  auto* context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  auto* holder = reinterpret_cast<gandiva::IntervalYearsHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

int32_t gdv_fn_cast_intervalyear_utf8_int32(int64_t context_ptr, int64_t holder_ptr,
                                            const char* data, int data_len,
                                            bool in1_validity,
                                            int32_t /*suppress_errors*/,
                                            bool /*in3_validity*/, bool* out_valid) {
  auto* context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  auto* holder = reinterpret_cast<gandiva::IntervalYearsHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

GANDIVA_EXPORT
gdv_timestamp to_utc_timezone_timestamp(int64_t context, gdv_timestamp time_miliseconds,
                                        const char* timezone, gdv_int32 length) {
  using arrow_vendored::date::locate_zone;
  using arrow_vendored::date::sys_time;
  using std::chrono::milliseconds;

  sys_time<milliseconds> tp{milliseconds{time_miliseconds}};
  try {
    const auto local_tz = locate_zone(std::string(timezone, length));
    gdv_timestamp offset = local_tz->get_info(tp).offset.count() * 1000;
    return time_miliseconds - static_cast<gdv_timestamp>(offset);
  } catch (...) {
    std::string e_msg = std::string(timezone, length) + " is an invalid time zone name.";
    gdv_fn_context_set_error_msg(context, e_msg.c_str());
    return 0;
  }
}

GANDIVA_EXPORT
gdv_timestamp from_utc_timezone_timestamp(gdv_int64 context,
                                          gdv_timestamp time_miliseconds,
                                          const char* timezone, gdv_int32 length) {
  using arrow_vendored::date::make_zoned;
  using arrow_vendored::date::sys_time;
  using std::chrono::milliseconds;

  sys_time<milliseconds> tp{milliseconds{time_miliseconds}};
  const auto utc_tz = make_zoned(std::string("Etc/UTC"), tp);
  try {
    const auto local_tz = make_zoned(std::string(timezone, length), utc_tz);
    gdv_timestamp offset = local_tz.get_time_zone()->get_info(tp).offset.count() * 1000;
    return time_miliseconds + static_cast<gdv_timestamp>(offset);
  } catch (...) {
    std::string e_msg = std::string(timezone, length) + " is an invalid time zone name.";
    gdv_fn_context_set_error_msg(context, e_msg.c_str());
    return 0;
  }
}

GANDIVA_EXPORT
const char* gdv_mask_show_first_n_utf8_int32(int64_t context, const char* data,
                                             int32_t data_len, int32_t n_to_show,
                                             int32_t* out_len) {
  utf8proc_int32_t utf8_char_buffer;
  int num_of_chars = static_cast<int>(
      utf8proc_decompose(reinterpret_cast<const utf8proc_uint8_t*>(data), data_len,
                         &utf8_char_buffer, 1, UTF8PROC_STABLE));

  if (num_of_chars < 0) {
    gdv_fn_context_set_error_msg(context, utf8proc_errmsg(num_of_chars));
    *out_len = 0;
    return nullptr;
  }

  int32_t n_to_mask = num_of_chars - n_to_show;
  return gdv_mask_last_n_utf8_int32(context, data, data_len, n_to_mask, out_len);
}

GANDIVA_EXPORT
const char* gdv_mask_show_last_n_utf8_int32(int64_t context, const char* data,
                                            int32_t data_len, int32_t n_to_show,
                                            int32_t* out_len) {
  utf8proc_int32_t utf8_char_buffer;
  int num_of_chars = static_cast<int>(
      utf8proc_decompose(reinterpret_cast<const utf8proc_uint8_t*>(data), data_len,
                         &utf8_char_buffer, 1, UTF8PROC_STABLE));

  if (num_of_chars < 0) {
    gdv_fn_context_set_error_msg(context, utf8proc_errmsg(num_of_chars));
    *out_len = 0;
    return nullptr;
  }

  int32_t n_to_mask = num_of_chars - n_to_show;
  return gdv_mask_first_n_utf8_int32(context, data, data_len, n_to_mask, out_len);
}
}

namespace gandiva {

void ExportedStubFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_fn_random
  args = {types->i64_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random));

  args = {types->i64_type(), types->i32_type(), types->i1_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random_with_seed", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random_with_seed));

  // gdv_fn_dec_from_string
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // const char* in
      types->i32_type(),      // int32_t in_length
      types->i32_ptr_type(),  // int32_t* precision_from_str
      types->i32_ptr_type(),  // int32_t* scale_from_str
      types->i64_ptr_type(),  // int64_t* dec_high_from_str
      types->i64_ptr_type(),  // int64_t* dec_low_from_str
  };

  engine->AddGlobalMappingForFunc("gdv_fn_dec_from_string",
                                  types->i32_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_dec_from_string));

  // gdv_fn_dec_to_string
  args = {
      types->i64_type(),      // context
      types->i64_type(),      // int64_t x_high
      types->i64_type(),      // int64_t x_low
      types->i32_type(),      // int32_t x_scale
      types->i64_ptr_type(),  // int64_t* dec_str_len
  };

  engine->AddGlobalMappingForFunc("gdv_fn_dec_to_string",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_dec_to_string));

  // gdv_fn_in_expr_lookup_int32
  args = {types->i64_type(),  // int64_t in holder ptr
          types->i32_type(),  // int32 value
          types->i1_type()};  // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_int32",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_int32));

  // gdv_fn_in_expr_lookup_int64
  args = {types->i64_type(),  // int64_t in holder ptr
          types->i64_type(),  // int64 value
          types->i1_type()};  // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_int64",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_int64));

  // gdv_fn_in_expr_lookup_decimal
  args = {types->i64_type(),  // int64_t in holder ptr
          types->i64_type(),  // high decimal value
          types->i64_type(),  // low decimal value
          types->i32_type(),  // decimal precision value
          types->i32_type(),  // decimal scale value
          types->i1_type()};  // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_decimal",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_decimal));

  // gdv_fn_in_expr_lookup_utf8
  args = {types->i64_type(),     // int64_t in holder ptr
          types->i8_ptr_type(),  // const char* value
          types->i32_type(),     // int value_len
          types->i1_type()};     // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_utf8));
  // gdv_fn_in_expr_lookup_float
  args = {types->i64_type(),    // int64_t in holder ptr
          types->float_type(),  // float value
          types->i1_type()};    // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_float",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_float));
  // gdv_fn_in_expr_lookup_double
  args = {types->i64_type(),     // int64_t in holder ptr
          types->double_type(),  // double value
          types->i1_type()};     // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_double",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_double));
  // gdv_fn_populate_varlen_vector
  args = {types->i64_type(),      // int64_t execution_context
          types->i8_ptr_type(),   // int8_t* data ptr
          types->i32_ptr_type(),  // int32_t* offsets ptr
          types->i64_type(),      // int64_t slot
          types->i8_ptr_type(),   // const char* entry_buf
          types->i32_type()};     // int32_t entry__len

  engine->AddGlobalMappingForFunc("gdv_fn_populate_varlen_vector",
                                  types->i32_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_populate_varlen_vector));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castINT_utf8", types->i32_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castINT_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castBIGINT_utf8", types->i64_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castBIGINT_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT4_utf8", types->float_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT4_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT8_utf8", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT8_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castINT_varbinary", types->i32_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castINT_varbinary));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castBIGINT_varbinary", types->i64_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castBIGINT_varbinary));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT4_varbinary", types->float_type(),
                                  args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT4_varbinary));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT8_varbinary", types->double_type(),
                                  args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT8_varbinary));

  // gdv_fn_base64_encode_utf8
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // in
      types->i32_type(),      // in_len
      types->i32_ptr_type(),  // out_len
  };

  engine->AddGlobalMappingForFunc("gdv_fn_base64_encode_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_base64_encode_binary));

  // gdv_fn_base64_decode_utf8
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // in
      types->i32_type(),      // in_len
      types->i32_ptr_type(),  // out_len
  };

  engine->AddGlobalMappingForFunc("gdv_fn_base64_decode_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_base64_decode_utf8));

  // gdv_fn_aes_encrypt
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_length
      types->i8_ptr_type(),  // key_data
      types->i32_type(),     // key_data_length
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_aes_encrypt",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_aes_encrypt));

  // gdv_fn_aes_decrypt
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_length
      types->i8_ptr_type(),  // key_data
      types->i32_type(),     // key_data_length
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_aes_decrypt",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_aes_decrypt));

  // gdv_mask_first_n and gdv_mask_last_n
  std::vector<llvm::Type*> mask_args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_length
      types->i32_type(),     // n_to_mask
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("gdv_mask_first_n_utf8_int32",
                                  types->i8_ptr_type() /*return_type*/, mask_args,
                                  reinterpret_cast<void*>(gdv_mask_first_n_utf8_int32));

  engine->AddGlobalMappingForFunc("gdv_mask_last_n_utf8_int32",
                                  types->i8_ptr_type() /*return_type*/, mask_args,
                                  reinterpret_cast<void*>(gdv_mask_last_n_utf8_int32));

  // gdv_fn_crc_32_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type()      // value_length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_crc_32_utf8", types->i64_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(gdv_fn_crc_32_utf8));

  // gdv_fn_crc_32_binary
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type()      // value_length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_crc_32_binary",
                                  types->i64_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_crc_32_binary));

  // gdv_fn_to_date_utf8_utf8
  args = {types->i64_type(),                   // int64_t execution_context
          types->i64_type(),                   // int64_t holder_ptr
          types->i8_ptr_type(),                // const char* data
          types->i32_type(),                   // int data_len
          types->i1_type(),                    // bool in1_validity
          types->i8_ptr_type(),                // const char* pattern
          types->i32_type(),                   // int pattern_len
          types->i1_type(),                    // bool in2_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc("gdv_fn_to_date_utf8_utf8",
                                  types->i64_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_to_date_utf8_utf8));

  // gdv_fn_to_date_utf8_utf8_int32
  args = {types->i64_type(),                   // int64_t execution_context
          types->i64_type(),                   // int64_t holder_ptr
          types->i8_ptr_type(),                // const char* data
          types->i32_type(),                   // int data_len
          types->i1_type(),                    // bool in1_validity
          types->i8_ptr_type(),                // const char* pattern
          types->i32_type(),                   // int pattern_len
          types->i1_type(),                    // bool in2_validity
          types->i32_type(),                   // int32_t suppress_errors
          types->i1_type(),                    // bool in3_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc(
      "gdv_fn_to_date_utf8_utf8_int32", types->i64_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_to_date_utf8_utf8_int32));

  // gdv_fn_cast_intervalday_utf8
  args = {
      types->i64_type(),                 // context
      types->i64_type(),                 // holder
      types->i8_ptr_type(),              // data
      types->i32_type(),                 // data_len
      types->i1_type(),                  // data validity
      types->ptr_type(types->i8_type())  // out validity
  };

  engine->AddGlobalMappingForFunc("gdv_fn_cast_intervalday_utf8",
                                  types->i64_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_cast_intervalday_utf8));

  // gdv_fn_cast_intervalday_utf8_int32
  args = {
      types->i64_type(),                 // context
      types->i64_type(),                 // holder
      types->i8_ptr_type(),              // data
      types->i32_type(),                 // data_len
      types->i1_type(),                  // data validity
      types->i32_type(),                 // suppress_error
      types->i1_type(),                  // suppress_error validity
      types->ptr_type(types->i8_type())  // out validity
  };

  engine->AddGlobalMappingForFunc(
      "gdv_fn_cast_intervalday_utf8_int32", types->i64_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_cast_intervalday_utf8_int32));

  // gdv_fn_cast_intervalyear_utf8
  args = {
      types->i64_type(),                 // context
      types->i64_type(),                 // holder
      types->i8_ptr_type(),              // data
      types->i32_type(),                 // data_len
      types->i1_type(),                  // data validity
      types->ptr_type(types->i8_type())  // out validity
  };

  engine->AddGlobalMappingForFunc("gdv_fn_cast_intervalyear_utf8",
                                  types->i32_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_cast_intervalyear_utf8));

  // gdv_fn_cast_intervalyear_utf8_int32
  args = {
      types->i64_type(),                 // context
      types->i64_type(),                 // holder
      types->i8_ptr_type(),              // data
      types->i32_type(),                 // data_len
      types->i1_type(),                  // data validity
      types->i32_type(),                 // suppress_error
      types->i1_type(),                  // suppress_error validity
      types->ptr_type(types->i8_type())  // out validity
  };

  engine->AddGlobalMappingForFunc(
      "gdv_fn_cast_intervalyear_utf8_int32", types->i32_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_cast_intervalyear_utf8_int32));

  // to_utc_timezone_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // timestamp
      types->i8_ptr_type(),  // timezone
      types->i32_type()      // length
  };

  engine->AddGlobalMappingForFunc("to_utc_timezone_timestamp",
                                  types->i64_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(to_utc_timezone_timestamp));

  // from_utc_timezone_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // timestamp
      types->i8_ptr_type(),  // timezone
      types->i32_type()      // length
  };

  engine->AddGlobalMappingForFunc("from_utc_timezone_timestamp",
                                  types->i64_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(from_utc_timezone_timestamp));

  // mask-show-n
  mask_args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_length
      types->i32_type(),     // n_to_show
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc(
      "gdv_mask_show_first_n_utf8_int32", types->i8_ptr_type() /*return_type*/, mask_args,
      reinterpret_cast<void*>(gdv_mask_show_first_n_utf8_int32));

  engine->AddGlobalMappingForFunc(
      "gdv_mask_show_last_n_utf8_int32", types->i8_ptr_type() /*return_type*/, mask_args,
      reinterpret_cast<void*>(gdv_mask_show_last_n_utf8_int32));

  // mask_utf8_utf8_utf8_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_len
      types->i8_ptr_type(),  // upper
      types->i32_type(),     // upper_len
      types->i8_ptr_type(),  // lower
      types->i32_type(),     // lower_len
      types->i8_ptr_type(),  // num
      types->i32_type(),     // num_len
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("mask_utf8_utf8_utf8_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(mask_utf8_utf8_utf8_utf8));

  // mask_utf8_utf8_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_len
      types->i8_ptr_type(),  // upper
      types->i32_type(),     // upper_len
      types->i8_ptr_type(),  // lower
      types->i32_type(),     // lower_len
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("mask_utf8_utf8_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(mask_utf8_utf8_utf8));

  // mask_utf8_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_len
      types->i8_ptr_type(),  // upper
      types->i32_type(),     // upper_len
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("mask_utf8_utf8", types->i8_ptr_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(mask_utf8_utf8));

  // mask_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_len
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("mask_utf8", types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(mask_utf8));

  // conv_function
  // conv_function_int64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // data
      types->i32_type(),     // in_base
      types->i32_type(),     // out_base
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("conv_int64_int32_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(conv_int64_int32_int32));

  // conv_function_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // data
      types->i32_type(),     // in_base
      types->i32_type(),     // out_base
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("conv_int32_int32_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(conv_int32_int32_int32));

  // conv_function_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // data
      types->i32_type(),     // data_length
      types->i32_type(),     // in_base
      types->i32_type(),     // out_base
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("conv_utf8_int32_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(conv_utf8_int32_int32));
}
}  // namespace gandiva

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

GDV_FORCE_INLINE
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

GANDIVA_EXPORT
const char* translate_utf8_utf8_utf8(int64_t context, const char* in, int32_t in_len,
                                     const char* from, int32_t from_len, const char* to,
                                     int32_t to_len, int32_t* out_len) {
  if (in_len <= 0) {
    *out_len = 0;
    return "";
  }

  if (from_len <= 0) {
    *out_len = in_len;
    return in;
  }

  // This variable is to control if there are multi-byte utf8 entries
  bool has_multi_byte = false;

  // This variable is to store the final result
  char* result;
  int result_len;

  // Searching multi-bytes in In
  for (int i = 0; i < in_len; i++) {
    unsigned char char_single_byte = in[i];
    if (char_single_byte > 127) {
      // found a multi-byte utf-8 char
      has_multi_byte = true;
      break;
    }
  }

  // Searching multi-bytes in From
  for (int i = 0; i < from_len; i++) {
    unsigned char char_single_byte = from[i];
    if (char_single_byte > 127) {
      // found a multi-byte utf-8 char
      has_multi_byte = true;
      break;
    }
  }

  // Searching multi-bytes in To
  for (int i = 0; i < to_len; i++) {
    unsigned char char_single_byte = to[i];
    if (char_single_byte > 127) {
      // found a multi-byte utf-8 char
      has_multi_byte = true;
      break;
    }
  }

  // If there are no multibytes in the input, work only with char
  if (!has_multi_byte) {
    // This variable is for receive the substitutions
    result = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, in_len));

    if (result == nullptr) {
      gdv_fn_context_set_error_msg(context,
                                   "Could not allocate memory for output string");
      *out_len = 0;
      return "";
    }
    result_len = 0;

    // Creating a Map to mark substitutions to make
    std::unordered_map<char, char> subs_list;

    // This variable is for controlling the position in entry TO, for never repeat the
    // changes
    int start_compare;

    if (to_len > 0) {
      start_compare = 0;
    } else {
      start_compare = -1;
    }

    // If the position in TO is out of range, this variable will be associated to map
    // list, to mark deletion positions
    const char empty = '\0';

    for (int in_for = 0; in_for < in_len; in_for++) {
      if (subs_list.find(in[in_for]) != subs_list.end()) {
        if (subs_list[in[in_for]] != empty) {
          // If exist in map, only add the correspondent value in result
          result[result_len] = subs_list[in[in_for]];
          result_len++;
        }
      } else {
        for (int from_for = 0; from_for <= from_len; from_for++) {
          if (from_for == from_len) {
            // If it's not in the FROM list, just add it to the map and the result.
            subs_list.insert(std::pair<char, char>(in[in_for], in[in_for]));
            result[result_len] = in[in_for];
            result_len++;
            break;
          }
          if (in[in_for] != from[from_for]) {
            // If this character does not exist in FROM list, don't need treatment
            continue;
          } else if (start_compare == -1 || start_compare == to_len) {
            // If exist but the start_compare is out of range, add to map as empty, to
            // deletion later
            subs_list.insert(std::pair<char, char>(in[in_for], empty));
            break;
          } else {
            // If exist and the start_compare is in range, add to map with the
            // corresponding TO in position start_compare
            subs_list.insert(std::pair<char, char>(in[in_for], to[start_compare]));
            result[result_len] = subs_list[in[in_for]];
            result_len++;
            start_compare++;
            break;  // for ignore duplicates entries in FROM, ex: ("adad")
          }
        }
      }
    }
  }

  // If there are no multibytes in the input, work with std::strings
  else {
    // This variable is for receive the substitutions, malloc is in_len * 4 to receive
    // possible inputs with 4 bytes
    result = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, in_len * 4));

    if (result == nullptr) {
      gdv_fn_context_set_error_msg(context,
                                   "Could not allocate memory for output string");
      *out_len = 0;
      return "";
    }
    result_len = 0;

    // This map is std::string to store multi-bytes entries
    std::unordered_map<std::string, std::string> subs_list;

    // This variable is for controlling the position in entry TO, for never repeat the
    // changes
    int start_compare;

    if (to_len > 0) {
      start_compare = 0;
    } else {
      start_compare = -1;
    }

    // If the position in TO is out of range, this variable will be associated to map
    // list, to mark deletion positions
    const std::string empty = "";

    // This variables is to control len of multi-bytes entries
    int len_char_in = 0;
    int len_char_from = 0;
    int len_char_to = 0;

    for (int in_for = 0; in_for < in_len; in_for += len_char_in) {
      // Updating len to char in this position
      len_char_in = utf8_char_length(in[in_for]);
      // Making copy to std::string with length for this char position
      std::string insert_copy_key(in + in_for, len_char_in);
      if (subs_list.find(insert_copy_key) != subs_list.end()) {
        if (subs_list[insert_copy_key] != empty) {
          // If exist in map, only add the correspondent value in result
          memcpy(result + result_len, subs_list[insert_copy_key].c_str(),
                 subs_list[insert_copy_key].length());
          result_len += subs_list[insert_copy_key].length();
        }
      } else {
        for (int from_for = 0; from_for <= from_len; from_for += len_char_from) {
          // Updating len to char in this position
          len_char_from = utf8_char_length(from[from_for]);
          // Making copy to std::string with length for this char position
          std::string copy_from_compare(from + from_for, len_char_from);
          if (from_for == from_len) {
            // If it's not in the FROM list, just add it to the map and the result.
            std::string insert_copy_value(in + in_for, len_char_in);
            // Insert in map to next loops
            subs_list.insert(
                std::pair<std::string, std::string>(insert_copy_key, insert_copy_value));
            memcpy(result + result_len, subs_list[insert_copy_key].c_str(),
                   subs_list[insert_copy_key].length());
            result_len += subs_list[insert_copy_key].length();
            break;
          }

          if (insert_copy_key != copy_from_compare) {
            // If this character does not exist in FROM list, don't need treatment
            continue;
          } else if (start_compare == -1 || start_compare >= to_len) {
            // If exist but the start_compare is out of range, add to map as empty, to
            // deletion later
            subs_list.insert(std::pair<std::string, std::string>(insert_copy_key, empty));
            break;
          } else {
            // If exist and the start_compare is in range, add to map with the
            // corresponding TO in position start_compare
            len_char_to = utf8_char_length(to[start_compare]);
            std::string insert_copy_value(to + start_compare, len_char_to);
            // Insert in map to next loops
            subs_list.insert(
                std::pair<std::string, std::string>(insert_copy_key, insert_copy_value));
            memcpy(result + result_len, subs_list[insert_copy_key].c_str(),
                   subs_list[insert_copy_key].length());
            result_len += subs_list[insert_copy_key].length();
            start_compare += len_char_to;
            break;  // for ignore duplicates entries in FROM, ex: ("adad")
          }
        }
      }
    }
  }

  *out_len = result_len;
  return result;
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

  // translate_utf8_utf8_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("translate_utf8_utf8_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(translate_utf8_utf8_utf8));
}
}  // namespace gandiva

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

#include <string>
#include <vector>

#include "arrow/util/base64.h"
#include "arrow/util/double_conversion.h"
#include "arrow/util/formatting.h"
#include "arrow/util/string_view.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"
#include "gandiva/encrypt_utils.h"
#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/formatting_utils.h"
#include "gandiva/hash_utils.h"
#include "gandiva/in_holder.h"
#include "gandiva/like_holder.h"
#include "gandiva/precompiled/types.h"
#include "gandiva/random_generator_holder.h"
#include "gandiva/replace_holder.h"
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

bool gdv_fn_like_utf8_utf8(int64_t ptr, const char* data, int data_len,
                           const char* pattern, int pattern_len) {
  gandiva::LikeHolder* holder = reinterpret_cast<gandiva::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

bool gdv_fn_like_utf8_utf8_utf8(int64_t ptr, const char* data, int data_len,
                                const char* pattern, int pattern_len,
                                const char* escape_char, int escape_char_len) {
  gandiva::LikeHolder* holder = reinterpret_cast<gandiva::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

bool gdv_fn_ilike_utf8_utf8(int64_t ptr, const char* data, int data_len,
                            const char* pattern, int pattern_len) {
  gandiva::LikeHolder* holder = reinterpret_cast<gandiva::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

const char* gdv_fn_regexp_replace_utf8_utf8(
    int64_t ptr, int64_t holder_ptr, const char* data, int32_t data_len,
    const char* /*pattern*/, int32_t /*pattern_len*/, const char* replace_string,
    int32_t replace_string_len, int32_t* out_length) {
  gandiva::ExecutionContext* context = reinterpret_cast<gandiva::ExecutionContext*>(ptr);

  gandiva::ReplaceHolder* holder = reinterpret_cast<gandiva::ReplaceHolder*>(holder_ptr);

  return (*holder)(context, data, data_len, replace_string, replace_string_len,
                   out_length);
}

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

#define MD5_HASH_FUNCTION(TYPE)                                                    \
  GANDIVA_EXPORT                                                                   \
  const char* gdv_fn_md5_##TYPE(int64_t context, gdv_##TYPE value, bool validity,  \
                                int32_t* out_length) {                             \
    if (!validity) {                                                               \
      return gandiva::gdv_md5_hash(context, NULLPTR, 0, out_length);               \
    }                                                                              \
    auto value_as_long = gandiva::gdv_double_to_long((double)value);               \
    const char* result = gandiva::gdv_md5_hash(context, &value_as_long,            \
                                               sizeof(value_as_long), out_length); \
                                                                                   \
    return result;                                                                 \
  }

#define MD5_HASH_FUNCTION_BUF(TYPE)                                                      \
  GANDIVA_EXPORT                                                                         \
  const char* gdv_fn_md5_##TYPE(int64_t context, gdv_##TYPE value, int32_t value_length, \
                                bool value_validity, int32_t* out_length) {              \
    if (!value_validity) {                                                               \
      return gandiva::gdv_md5_hash(context, NULLPTR, 0, out_length);                     \
    }                                                                                    \
    return gandiva::gdv_md5_hash(context, value, value_length, out_length);              \
  }

#define SHA1_HASH_FUNCTION(TYPE)                                                    \
  GANDIVA_EXPORT                                                                    \
  const char* gdv_fn_sha1_##TYPE(int64_t context, gdv_##TYPE value, bool validity,  \
                                 int32_t* out_length) {                             \
    if (!validity) {                                                                \
      return gandiva::gdv_sha1_hash(context, NULLPTR, 0, out_length);               \
    }                                                                               \
    auto value_as_long = gandiva::gdv_double_to_long((double)value);                \
    const char* result = gandiva::gdv_sha1_hash(context, &value_as_long,            \
                                                sizeof(value_as_long), out_length); \
                                                                                    \
    return result;                                                                  \
  }

#define SHA1_HASH_FUNCTION_BUF(TYPE)                                         \
  GANDIVA_EXPORT                                                             \
  const char* gdv_fn_sha1_##TYPE(int64_t context, gdv_##TYPE value,          \
                                 int32_t value_length, bool value_validity,  \
                                 int32_t* out_length) {                      \
    if (!value_validity) {                                                   \
      return gandiva::gdv_sha1_hash(context, NULLPTR, 0, out_length);        \
    }                                                                        \
    return gandiva::gdv_sha1_hash(context, value, value_length, out_length); \
  }

#define SHA256_HASH_FUNCTION(TYPE)                                                    \
  GANDIVA_EXPORT                                                                      \
  const char* gdv_fn_sha256_##TYPE(int64_t context, gdv_##TYPE value, bool validity,  \
                                   int32_t* out_length) {                             \
    if (!validity) {                                                                  \
      return gandiva::gdv_sha256_hash(context, NULLPTR, 0, out_length);               \
    }                                                                                 \
    auto value_as_long = gandiva::gdv_double_to_long((double)value);                  \
    const char* result = gandiva::gdv_sha256_hash(context, &value_as_long,            \
                                                  sizeof(value_as_long), out_length); \
    return result;                                                                    \
  }

#define SHA256_HASH_FUNCTION_BUF(TYPE)                                         \
  GANDIVA_EXPORT                                                               \
  const char* gdv_fn_sha256_##TYPE(int64_t context, gdv_##TYPE value,          \
                                   int32_t value_length, bool value_validity,  \
                                   int32_t* out_length) {                      \
    if (!value_validity) {                                                     \
      return gandiva::gdv_sha256_hash(context, NULLPTR, 0, out_length);        \
    }                                                                          \
                                                                               \
    return gandiva::gdv_sha256_hash(context, value, value_length, out_length); \
  }

// Expand inner macro for all numeric types.
#define SHA_NUMERIC_BOOL_DATE_PARAMS(INNER) \
  INNER(int8)                               \
  INNER(int16)                              \
  INNER(int32)                              \
  INNER(int64)                              \
  INNER(uint8)                              \
  INNER(uint16)                             \
  INNER(uint32)                             \
  INNER(uint64)                             \
  INNER(float32)                            \
  INNER(float64)                            \
  INNER(boolean)                            \
  INNER(date64)                             \
  INNER(date32)                             \
  INNER(time32)                             \
  INNER(timestamp)

// Expand inner macro for all numeric types.
#define SHA_VAR_LEN_PARAMS(INNER) \
  INNER(utf8)                     \
  INNER(binary)

SHA_NUMERIC_BOOL_DATE_PARAMS(MD5_HASH_FUNCTION)
SHA_VAR_LEN_PARAMS(MD5_HASH_FUNCTION_BUF)

SHA_NUMERIC_BOOL_DATE_PARAMS(SHA256_HASH_FUNCTION)
SHA_VAR_LEN_PARAMS(SHA256_HASH_FUNCTION_BUF)

SHA_NUMERIC_BOOL_DATE_PARAMS(SHA1_HASH_FUNCTION)
SHA_VAR_LEN_PARAMS(SHA1_HASH_FUNCTION_BUF)

#undef SHA_NUMERIC_BOOL_DATE_PARAMS
#undef SHA_VAR_LEN_PARAMS

// Add functions for decimal128
GANDIVA_EXPORT
const char* gdv_fn_md5_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                  int32_t /*x_precision*/, int32_t /*x_scale*/,
                                  gdv_boolean x_isvalid, int32_t* out_length) {
  if (!x_isvalid) {
    return gandiva::gdv_md5_hash(context, NULLPTR, 0, out_length);
  }

  const gandiva::BasicDecimal128 decimal_128(x_high, x_low);
  return gandiva::gdv_md5_hash(context, decimal_128.ToBytes().data(), 16, out_length);
}

GANDIVA_EXPORT
const char* gdv_fn_sha256_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                     int32_t /*x_precision*/, int32_t /*x_scale*/,
                                     gdv_boolean x_isvalid, int32_t* out_length) {
  if (!x_isvalid) {
    return gandiva::gdv_sha256_hash(context, NULLPTR, 0, out_length);
  }

  const gandiva::BasicDecimal128 decimal_128(x_high, x_low);
  return gandiva::gdv_sha256_hash(context, decimal_128.ToBytes().data(), 16, out_length);
}

GANDIVA_EXPORT
const char* gdv_fn_sha1_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                   int32_t /*x_precision*/, int32_t /*x_scale*/,
                                   gdv_boolean x_isvalid, int32_t* out_length) {
  if (!x_isvalid) {
    return gandiva::gdv_sha1_hash(context, NULLPTR, 0, out_length);
  }

  const gandiva::BasicDecimal128 decimal_128(x_high, x_low);
  return gandiva::gdv_sha1_hash(context, decimal_128.ToBytes().data(), 16, out_length);
}

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

#define GDV_FN_CAST_VARLEN_TYPE_FROM_TYPE(IN_TYPE, CAST_NAME, ARROW_TYPE)         \
  GANDIVA_EXPORT                                                                  \
  const char* gdv_fn_cast##CAST_NAME##_##IN_TYPE##_int64(                         \
      int64_t context, gdv_##IN_TYPE value, int64_t len, int32_t * out_len) {     \
    if (len < 0) {                                                                \
      gdv_fn_context_set_error_msg(context, "Buffer length can not be negative"); \
      *out_len = 0;                                                               \
      return "";                                                                  \
    }                                                                             \
    if (len == 0) {                                                               \
      *out_len = 0;                                                               \
      return "";                                                                  \
    }                                                                             \
    arrow::internal::StringFormatter<arrow::ARROW_TYPE> formatter;                \
    char* ret = reinterpret_cast<char*>(                                          \
        gdv_fn_context_arena_malloc(context, static_cast<int32_t>(len)));         \
    if (ret == nullptr) {                                                         \
      gdv_fn_context_set_error_msg(context, "Could not allocate memory");         \
      *out_len = 0;                                                               \
      return "";                                                                  \
    }                                                                             \
    arrow::Status status = formatter(value, [&](arrow::util::string_view v) {     \
      int64_t size = static_cast<int64_t>(v.size());                              \
      *out_len = static_cast<int32_t>(len < size ? len : size);                   \
      memcpy(ret, v.data(), *out_len);                                            \
      return arrow::Status::OK();                                                 \
    });                                                                           \
    if (!status.ok()) {                                                           \
      std::string err = "Could not cast " + std::to_string(value) + " to string"; \
      gdv_fn_context_set_error_msg(context, err.c_str());                         \
      *out_len = 0;                                                               \
      return "";                                                                  \
    }                                                                             \
    return ret;                                                                   \
  }

#define GDV_FN_CAST_VARLEN_TYPE_FROM_REAL(IN_TYPE, CAST_NAME, ARROW_TYPE)         \
  GANDIVA_EXPORT                                                                  \
  const char* gdv_fn_cast##CAST_NAME##_##IN_TYPE##_int64(                         \
      int64_t context, gdv_##IN_TYPE value, int64_t len, int32_t * out_len) {     \
    if (len < 0) {                                                                \
      gdv_fn_context_set_error_msg(context, "Buffer length can not be negative"); \
      *out_len = 0;                                                               \
      return "";                                                                  \
    }                                                                             \
    if (len == 0) {                                                               \
      *out_len = 0;                                                               \
      return "";                                                                  \
    }                                                                             \
    gandiva::GdvStringFormatter<arrow::ARROW_TYPE> formatter;                     \
    char* ret = reinterpret_cast<char*>(                                          \
        gdv_fn_context_arena_malloc(context, static_cast<int32_t>(len)));         \
    if (ret == nullptr) {                                                         \
      gdv_fn_context_set_error_msg(context, "Could not allocate memory");         \
      *out_len = 0;                                                               \
      return "";                                                                  \
    }                                                                             \
    arrow::Status status = formatter(value, [&](arrow::util::string_view v) {     \
      int64_t size = static_cast<int64_t>(v.size());                              \
      *out_len = static_cast<int32_t>(len < size ? len : size);                   \
      memcpy(ret, v.data(), *out_len);                                            \
      return arrow::Status::OK();                                                 \
    });                                                                           \
    if (!status.ok()) {                                                           \
      std::string err = "Could not cast " + std::to_string(value) + " to string"; \
      gdv_fn_context_set_error_msg(context, err.c_str());                         \
      *out_len = 0;                                                               \
      return "";                                                                  \
    }                                                                             \
    return ret;                                                                   \
  }

#define CAST_VARLEN_TYPE_FROM_NUMERIC(VARLEN_TYPE)                   \
  GDV_FN_CAST_VARLEN_TYPE_FROM_TYPE(int32, VARLEN_TYPE, Int32Type)   \
  GDV_FN_CAST_VARLEN_TYPE_FROM_TYPE(int64, VARLEN_TYPE, Int64Type)   \
  GDV_FN_CAST_VARLEN_TYPE_FROM_TYPE(date64, VARLEN_TYPE, Date64Type) \
  GDV_FN_CAST_VARLEN_TYPE_FROM_REAL(float32, VARLEN_TYPE, FloatType) \
  GDV_FN_CAST_VARLEN_TYPE_FROM_REAL(float64, VARLEN_TYPE, DoubleType)

CAST_VARLEN_TYPE_FROM_NUMERIC(VARCHAR)
CAST_VARLEN_TYPE_FROM_NUMERIC(VARBINARY)

#undef CAST_VARLEN_TYPE_FROM_NUMERIC
#undef GDV_FN_CAST_VARLEN_TYPE_FROM_TYPE
#undef GDV_FN_CAST_VARLEN_TYPE_FROM_REAL
#undef GDV_FN_CAST_VARCHAR_INTEGER
#undef GDV_FN_CAST_VARCHAR_REAL

GDV_FORCE_INLINE
int32_t gdv_fn_utf8_char_length(char c) {
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

GDV_FORCE_INLINE
void gdv_fn_set_error_for_invalid_utf8(int64_t execution_context, char val) {
  char const* fmt = "unexpected byte \\%02hhx encountered while decoding utf8 string";
  int size = static_cast<int>(strlen(fmt)) + 64;
  char* error = reinterpret_cast<char*>(malloc(size));
  snprintf(error, size, fmt, (unsigned char)val);
  gdv_fn_context_set_error_msg(execution_context, error);
  free(error);
}

// Convert an utf8 string to its corresponding uppercase string
GANDIVA_EXPORT
const char* gdv_fn_upper_utf8(int64_t context, const char* data, int32_t data_len,
                              int32_t* out_len) {
  if (data_len == 0) {
    *out_len = 0;
    return "";
  }

  // If it is a single-byte character (ASCII), corresponding uppercase is always 1-byte
  // long; if it is >= 2 bytes long, uppercase can be at most 4 bytes long, so length of
  // the output can be at most twice the length of the input
  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, 2 * data_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  int32_t char_len, out_char_len, out_idx = 0;
  uint32_t char_codepoint;

  for (int32_t i = 0; i < data_len; i += char_len) {
    char_len = gdv_fn_utf8_char_length(data[i]);
    // For single byte characters:
    // If it is a lowercase ASCII character, set the output to its corresponding uppercase
    // character; else, set the output to the read character
    if (char_len == 1) {
      char cur = data[i];
      // 'A' - 'Z' : 0x41 - 0x5a
      // 'a' - 'z' : 0x61 - 0x7a
      if (cur >= 0x61 && cur <= 0x7a) {
        out[out_idx++] = static_cast<char>(cur - 0x20);
      } else {
        out[out_idx++] = cur;
      }
      continue;
    }

    // Control reaches here when we encounter a multibyte character
    const auto* in_char = (const uint8_t*)(data + i);

    // Decode the multibyte character
    bool is_valid_utf8_char =
        arrow::util::UTF8Decode((const uint8_t**)&in_char, &char_codepoint);

    // If it is an invalid utf8 character, UTF8Decode evaluates to false
    if (!is_valid_utf8_char) {
      gdv_fn_set_error_for_invalid_utf8(context, data[i]);
      *out_len = 0;
      return "";
    }

    // Convert the encoded codepoint to its uppercase codepoint
    int32_t upper_codepoint = utf8proc_toupper(char_codepoint);

    // UTF8Encode advances the pointer by the number of bytes present in the uppercase
    // character
    auto* out_char = (uint8_t*)(out + out_idx);
    uint8_t* out_char_start = out_char;

    // Encode the uppercase character
    out_char = arrow::util::UTF8Encode(out_char, upper_codepoint);

    out_char_len = static_cast<int32_t>(out_char - out_char_start);
    out_idx += out_char_len;
  }

  *out_len = out_idx;
  return out;
}

// Convert an utf8 string to its corresponding lowercase string
GANDIVA_EXPORT
const char* gdv_fn_lower_utf8(int64_t context, const char* data, int32_t data_len,
                              int32_t* out_len) {
  if (data_len == 0) {
    *out_len = 0;
    return "";
  }

  // If it is a single-byte character (ASCII), corresponding lowercase is always 1-byte
  // long; if it is >= 2 bytes long, lowercase can be at most 4 bytes long, so length of
  // the output can be at most twice the length of the input
  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, 2 * data_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  int32_t char_len, out_char_len, out_idx = 0;
  uint32_t char_codepoint;

  for (int32_t i = 0; i < data_len; i += char_len) {
    char_len = gdv_fn_utf8_char_length(data[i]);
    // For single byte characters:
    // If it is an uppercase ASCII character, set the output to its corresponding
    // lowercase character; else, set the output to the read character
    if (char_len == 1) {
      char cur = data[i];
      // 'A' - 'Z' : 0x41 - 0x5a
      // 'a' - 'z' : 0x61 - 0x7a
      if (cur >= 0x41 && cur <= 0x5a) {
        out[out_idx++] = static_cast<char>(cur + 0x20);
      } else {
        out[out_idx++] = cur;
      }
      continue;
    }

    // Control reaches here when we encounter a multibyte character
    const auto* in_char = (const uint8_t*)(data + i);

    // Decode the multibyte character
    bool is_valid_utf8_char =
        arrow::util::UTF8Decode((const uint8_t**)&in_char, &char_codepoint);

    // If it is an invalid utf8 character, UTF8Decode evaluates to false
    if (!is_valid_utf8_char) {
      gdv_fn_set_error_for_invalid_utf8(context, data[i]);
      *out_len = 0;
      return "";
    }

    // Convert the encoded codepoint to its lowercase codepoint
    int32_t lower_codepoint = utf8proc_tolower(char_codepoint);

    // UTF8Encode advances the pointer by the number of bytes present in the lowercase
    // character
    auto* out_char = (uint8_t*)(out + out_idx);
    uint8_t* out_char_start = out_char;

    // Encode the lowercase character
    out_char = arrow::util::UTF8Encode(out_char, lower_codepoint);

    out_char_len = static_cast<int32_t>(out_char - out_char_start);
    out_idx += out_char_len;
  }

  *out_len = out_idx;
  return out;
}

// Any codepoint, except the ones for lowercase letters, uppercase letters,
// titlecase letters, decimal digits and letter numbers categories will be
// considered as word separators.
//
// The Unicode characters also are divided between categories. This link
// https://www.compart.com/en/unicode/category shows
// more information about characters categories.
GDV_FORCE_INLINE
bool gdv_fn_is_codepoint_for_space(uint32_t val) {
  auto category = utf8proc_category(val);

  return category != utf8proc_category_t::UTF8PROC_CATEGORY_LU &&
         category != utf8proc_category_t::UTF8PROC_CATEGORY_LL &&
         category != utf8proc_category_t::UTF8PROC_CATEGORY_LT &&
         category != utf8proc_category_t::UTF8PROC_CATEGORY_NL &&
         category != utf8proc_category_t ::UTF8PROC_CATEGORY_ND;
}

// For a given text, initialize the first letter after a word-separator and lowercase
// the others e.g:
//     - "IT is a tEXt str" -> "It Is A Text Str"
GANDIVA_EXPORT
const char* gdv_fn_initcap_utf8(int64_t context, const char* data, int32_t data_len,
                                int32_t* out_len) {
  if (data_len == 0) {
    *out_len = data_len;
    return "";
  }

  // If it is a single-byte character (ASCII), corresponding uppercase is always 1-byte
  // long; if it is >= 2 bytes long, uppercase can be at most 4 bytes long, so length of
  // the output can be at most twice the length of the input
  char* out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, 2 * data_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  int32_t char_len = 0;
  int32_t out_char_len = 0;
  int32_t out_idx = 0;
  uint32_t char_codepoint;

  // Any character is considered as space, except if it is alphanumeric
  bool last_char_was_space = true;

  for (int32_t i = 0; i < data_len; i += char_len) {
    // An optimization for single byte characters:
    if (static_cast<signed char>(data[i]) >= 0) {  // 1-byte char (0x00 ~ 0x7F)
      char_len = 1;
      char cur = data[i];

      if (cur >= 0x61 && cur <= 0x7a && last_char_was_space) {
        // Check if the character is the first one of the word and it is
        // lowercase -> 'a' - 'z' : 0x61 - 0x7a.
        // Then turn it into uppercase -> 'A' - 'Z' : 0x41 - 0x5a
        out[out_idx++] = static_cast<char>(cur - 0x20);
        last_char_was_space = false;
      } else if (cur >= 0x41 && cur <= 0x5a && !last_char_was_space) {
        out[out_idx++] = static_cast<char>(cur + 0x20);
      } else {
        // Check if the ASCII character is not an alphanumeric character:
        // '0' - '9': 0x30 - 0x39
        // 'a' - 'z' : 0x61 - 0x7a
        // 'A' - 'Z' : 0x41 - 0x5a
        last_char_was_space = (cur < 0x30) || (cur > 0x39 && cur < 0x41) ||
                              (cur > 0x5a && cur < 0x61) || (cur > 0x7a);
        out[out_idx++] = cur;
      }
      continue;
    }

    char_len = gdv_fn_utf8_char_length(data[i]);

    // Control reaches here when we encounter a multibyte character
    const auto* in_char = (const uint8_t*)(data + i);

    // Decode the multibyte character
    bool is_valid_utf8_char =
        arrow::util::UTF8Decode((const uint8_t**)&in_char, &char_codepoint);

    // If it is an invalid utf8 character, UTF8Decode evaluates to false
    if (!is_valid_utf8_char) {
      gdv_fn_set_error_for_invalid_utf8(context, data[i]);
      *out_len = 0;
      return "";
    }

    bool is_char_space = gdv_fn_is_codepoint_for_space(char_codepoint);

    int32_t formatted_codepoint;
    if (last_char_was_space && !is_char_space) {
      formatted_codepoint = utf8proc_toupper(char_codepoint);
    } else {
      formatted_codepoint = utf8proc_tolower(char_codepoint);
    }

    // UTF8Encode advances the pointer by the number of bytes present in the character
    auto* out_char = (uint8_t*)(out + out_idx);
    uint8_t* out_char_start = out_char;

    // Encode the character
    out_char = arrow::util::UTF8Encode(out_char, formatted_codepoint);

    out_char_len = static_cast<int32_t>(out_char - out_char_start);
    out_idx += out_char_len;

    last_char_was_space = is_char_space;
  }

  *out_len = out_idx;
  return out;
}

GANDIVA_EXPORT
const char* gdv_fn_aes_encrypt(int64_t context, const char* data, int32_t data_len,
                               const char* key_data, int32_t key_data_len,
                               int32_t* out_len) {
  if (data_len < 0) {
    gdv_fn_context_set_error_msg(context, "Invalid data length to be encrypted");
    *out_len = 0;
    return "";
  }

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
                         &utf8_char_buffer, 4, UTF8PROC_STABLE));

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
}

namespace gandiva {

void ExportedStubFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_fn_castVARBINARY_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // int32_t value
      types->i64_type(),     // int64_t out value length
      types->i32_ptr_type()  // int32_t out_length
  };

  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARBINARY_int32_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARBINARY_int32_int64));

  // gdv_fn_castVARBINARY_int64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // int64_t value
      types->i64_type(),     // int64_t out value length
      types->i32_ptr_type()  // int32_t out_length
  };

  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARBINARY_int64_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARBINARY_int64_int64));

  // gdv_fn_castVARBINARY_float32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // float value
      types->i64_type(),     // int64_t out value length
      types->i64_ptr_type()  // int32_t out_length
  };

  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARBINARY_float32_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARBINARY_float32_int64));

  // gdv_fn_castVARBINARY_float64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // double value
      types->i64_type(),     // int64_t out value length
      types->i32_ptr_type()  // int32_t out_length
  };

  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARBINARY_float64_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARBINARY_float64_int64));

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

  // gdv_fn_like_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type()};    // int pattern_len

  engine->AddGlobalMappingForFunc("gdv_fn_like_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_like_utf8_utf8));

  // gdv_fn_like_utf8_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type(),     // int pattern_len
          types->i8_ptr_type(),  // const char* escape_char
          types->i32_type()};    // int escape_char_len

  engine->AddGlobalMappingForFunc("gdv_fn_like_utf8_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_like_utf8_utf8_utf8));

  // gdv_fn_ilike_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type()};    // int pattern_len

  engine->AddGlobalMappingForFunc("gdv_fn_ilike_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_ilike_utf8_utf8));

  // gdv_fn_regexp_replace_utf8_utf8
  args = {types->i64_type(),       // int64_t ptr
          types->i64_type(),       // int64_t holder_ptr
          types->i8_ptr_type(),    // const char* data
          types->i32_type(),       // int data_len
          types->i8_ptr_type(),    // const char* pattern
          types->i32_type(),       // int pattern_len
          types->i8_ptr_type(),    // const char* replace_string
          types->i32_type(),       // int32_t replace_string_len
          types->i32_ptr_type()};  // int32_t* out_length

  engine->AddGlobalMappingForFunc(
      "gdv_fn_regexp_replace_utf8_utf8", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_regexp_replace_utf8_utf8));

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

  // gdv_fn_random
  args = {types->i64_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random));

  args = {types->i64_type(), types->i32_type(), types->i1_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random_with_seed", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random_with_seed));

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

  // gdv_fn_castVARCHAR_int32_int64
  args = {types->i64_type(),       // int64_t execution_context
          types->i32_type(),       // int32_t value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_int32_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_int32_int64));

  // gdv_fn_castVARCHAR_int64_int64
  args = {types->i64_type(),       // int64_t execution_context
          types->i64_type(),       // int64_t value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_int64_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_int64_int64));

  // gdv_fn_castVARCHAR_milliseconds
  args = {types->i64_type(),       // int64_t execution_context
          types->i64_type(),       // gdv_date64 value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_date64_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_date64_int64));

  // gdv_fn_castVARCHAR_float32_int64
  args = {types->i64_type(),       // int64_t execution_context
          types->float_type(),     // float value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_float32_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_float32_int64));

  // gdv_fn_castVARCHAR_float64_int64
  args = {types->i64_type(),       // int64_t execution_context
          types->double_type(),    // double value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_float64_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_float64_int64));

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

  // gdv_fn_md5_int8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_int8", types->i8_ptr_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(gdv_fn_md5_int8));

  // gdv_fn_md5_int16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_int16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_int16));

  // gdv_fn_md5_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_int32));

  // gdv_fn_md5_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_int64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_int64));

  // gdv_fn_md5_uint8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_uint8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_uint8));

  // gdv_fn_md5_uint16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_uint16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_uint16));

  // gdv_fn_md5_uint32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_uint32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_uint32));

  // gdv_fn_md5_uint64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_uint64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_uint64));

  // gdv_fn_md5_float32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_float32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_float32));

  // gdv_fn_md5_float64
  args = {
      types->i64_type(),     // context
      types->double_type(),  // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_float64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_float64));

  // gdv_fn_md5_boolean
  args = {
      types->i64_type(),     // context
      types->i1_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_boolean",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_boolean));

  // gdv_fn_md5_date64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_date64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_date64));

  // gdv_fn_md5_date32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_date32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_date32));

  // gdv_fn_md5_time32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_time32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_time32));

  // gdv_fn_md5_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_timestamp",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_timestamp));

  // gdv_fn_md5_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_md5_utf8", types->i8_ptr_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(gdv_fn_md5_utf8));

  // gdv_fn_md5_from_binary
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_md5_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_binary));

  // gdv_fn_sha1_int8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_int8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_int8));

  // gdv_fn_sha1_int16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_int16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_int16));

  // gdv_fn_sha1_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_int32));

  // gdv_fn_sha1_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_int64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_int64));

  // gdv_fn_sha1_uint8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_uint8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_uint8));

  // gdv_fn_sha1_uint16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_uint16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_uint16));

  // gdv_fn_sha1_uint32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_uint32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_uint32));

  // gdv_fn_sha1_uint64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_uint64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_uint64));

  // gdv_fn_sha1_float32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_float32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_float32));

  // gdv_fn_sha1_float64
  args = {
      types->i64_type(),     // context
      types->double_type(),  // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_float64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_float64));

  // gdv_fn_sha1_boolean
  args = {
      types->i64_type(),     // context
      types->i1_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_boolean",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_boolean));

  // gdv_fn_sha1_date64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_date64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_date64));

  // gdv_fn_sha1_date32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_date32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_date32));

  // gdv_fn_sha1_time32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_time32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_time32));

  // gdv_fn_sha1_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_timestamp",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_timestamp));

  // gdv_fn_sha1_from_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha1_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_utf8));

  // gdv_fn_sha1_from_binary
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha1_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_binary));

  // gdv_fn_sha256_int8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_int8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_int8));

  // gdv_fn_sha256_int16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_int16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_int16));

  // gdv_fn_sha256_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_int32));

  // gdv_fn_sha256_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_int64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_int64));

  // gdv_fn_sha256_uint8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_uint8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_uint8));

  // gdv_fn_sha256_uint16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_uint16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_uint16));

  // gdv_fn_sha256_uint32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_uint32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_uint32));

  // gdv_fn_sha256_uint64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_uint64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_uint64));

  // gdv_fn_sha256_float32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_float32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_float32));

  // gdv_fn_sha256_float64
  args = {
      types->i64_type(),     // context
      types->double_type(),  // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_float64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_float64));

  // gdv_fn_sha256_boolean
  args = {
      types->i64_type(),     // context
      types->i1_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_boolean",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_boolean));

  // gdv_fn_sha256_date64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_date64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_date64));

  // gdv_fn_sha256_date32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_date32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_date32));

  // gdv_fn_sha256_time32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_time32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_time32));

  // gdv_fn_sha256_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_timestamp",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_timestamp));

  // gdv_fn_hash_sha256_from_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha256_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_utf8));

  // gdv_fn_hash_sha256_from_binary
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha256_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_binary));

  // gdv_fn_sha1_decimal128
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // high_bits
      types->i64_type(),     // low_bits
      types->i32_type(),     // precision
      types->i32_type(),     // scale
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha1_decimal128",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_decimal128));
  // gdv_fn_sha256_decimal128
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // high_bits
      types->i64_type(),     // low_bits
      types->i32_type(),     // precision
      types->i32_type(),     // scale
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha256_decimal128",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_decimal128));

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

  // gdv_fn_MD5_decimal128
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // high_bits
      types->i64_type(),     // low_bits
      types->i32_type(),     // precision
      types->i32_type(),     // scale
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_md5_decimal128",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_decimal128));

  // gdv_fn_upper_utf8
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // data
      types->i32_type(),      // data_len
      types->i32_ptr_type(),  // out_len
  };

  engine->AddGlobalMappingForFunc("gdv_fn_upper_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_upper_utf8));
  // gdv_fn_lower_utf8
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // data
      types->i32_type(),      // data_len
      types->i32_ptr_type(),  // out_len
  };

  engine->AddGlobalMappingForFunc("gdv_fn_lower_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_lower_utf8));

  // gdv_fn_initcap_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i32_ptr_type()  // out_length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_initcap_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_initcap_utf8));

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
}
}  // namespace gandiva

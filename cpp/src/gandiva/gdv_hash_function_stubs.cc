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

//#pragma once

#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/formatting_utils.h"
#include "gandiva/gdv_function_stubs.h"
#include "gandiva/hash_utils.h"
#include "gandiva/in_holder.h"
#include "gandiva/precompiled/types.h"

extern "C" {

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

#define SHA512_HASH_FUNCTION(TYPE)                                                    \
  GANDIVA_EXPORT                                                                      \
  const char* gdv_fn_sha512_##TYPE(int64_t context, gdv_##TYPE value, bool validity,  \
                                   int32_t* out_length) {                             \
    if (!validity) {                                                                  \
      return gandiva::gdv_sha512_hash(context, NULLPTR, 0, out_length);               \
    }                                                                                 \
    auto value_as_long = gandiva::gdv_double_to_long((double)value);                  \
    const char* result = gandiva::gdv_sha512_hash(context, &value_as_long,            \
                                                  sizeof(value_as_long), out_length); \
    return result;                                                                    \
  }

#define SHA512_HASH_FUNCTION_BUF(TYPE)                                         \
  GANDIVA_EXPORT                                                               \
  const char* gdv_fn_sha512_##TYPE(int64_t context, gdv_##TYPE value,          \
                                   int32_t value_length, bool value_validity,  \
                                   int32_t* out_length) {                      \
    if (!value_validity) {                                                     \
      return gandiva::gdv_sha512_hash(context, NULLPTR, 0, out_length);        \
    }                                                                          \
                                                                               \
    return gandiva::gdv_sha512_hash(context, value, value_length, out_length); \
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

SHA_NUMERIC_BOOL_DATE_PARAMS(SHA512_HASH_FUNCTION)
SHA_VAR_LEN_PARAMS(SHA512_HASH_FUNCTION_BUF)

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
const char* gdv_fn_sha512_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                     int32_t /*x_precision*/, int32_t /*x_scale*/,
                                     gdv_boolean x_isvalid, int32_t* out_length) {
  if (!x_isvalid) {
    return gandiva::gdv_sha512_hash(context, NULLPTR, 0, out_length);
  }

  const gandiva::BasicDecimal128 decimal_128(x_high, x_low);
  return gandiva::gdv_sha512_hash(context, decimal_128.ToBytes().data(), 16, out_length);
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
}

namespace gandiva {

void ExportedHashFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

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

  // gdv_fn_sha512_int8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_int8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_int8));

  // gdv_fn_sha512_int16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_int16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_int16));

  // gdv_fn_sha512_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_int32));

  // gdv_fn_sha512_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_int64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_int64));

  // gdv_fn_sha512_uint8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_uint8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_uint8));

  // gdv_fn_sha512_uint16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_uint16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_uint16));

  // gdv_fn_sha512_uint32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_uint32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_uint32));

  // gdv_fn_sha512_uint64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_uint64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_uint64));

  // gdv_fn_sha512_float32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_float32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_float32));

  // gdv_fn_sha512_float64
  args = {
      types->i64_type(),     // context
      types->double_type(),  // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_float64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_float64));

  // gdv_fn_sha512_boolean
  args = {
      types->i64_type(),     // context
      types->i1_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_boolean",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_boolean));

  // gdv_fn_sha512_date64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_date64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_date64));

  // gdv_fn_sha512_date32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_date32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_date32));

  // gdv_fn_sha512_time32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_time32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_time32));

  // gdv_fn_sha512_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha512_timestamp",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_timestamp));

  // gdv_fn_hash_sha512_from_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha512_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_utf8));

  // gdv_fn_hash_sha512_from_binary
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha512_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_binary));

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

  // gdv_fn_sha512_decimal128
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // high_bits
      types->i64_type(),     // low_bits
      types->i32_type(),     // precision
      types->i32_type(),     // scale
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha512_decimal128",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha512_decimal128));

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
}
}  // namespace gandiva

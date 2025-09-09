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

#include "gandiva/precompiled/decimal_ops.h"
#include "gandiva/precompiled/types.h"

extern "C" {

FORCE_INLINE
void add_large_decimal128_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                     int32_t x_scale, int64_t y_high, uint64_t y_low,
                                     int32_t y_precision, int32_t y_scale,
                                     int32_t out_precision, int32_t out_scale,
                                     int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x(x_high, x_low, x_precision, x_scale);
  gandiva::BasicDecimalScalar128 y(y_high, y_low, y_precision, y_scale);

  arrow::BasicDecimal128 out = gandiva::decimalops::Add(x, y, out_precision, out_scale);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void multiply_decimal128_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                    int32_t x_scale, int64_t y_high, uint64_t y_low,
                                    int32_t y_precision, int32_t y_scale,
                                    int32_t out_precision, int32_t out_scale,
                                    int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x(x_high, x_low, x_precision, x_scale);
  gandiva::BasicDecimalScalar128 y(y_high, y_low, y_precision, y_scale);
  bool overflow;

  // TODO ravindra: generate error on overflows (ARROW-4570).
  arrow::BasicDecimal128 out =
      gandiva::decimalops::Multiply(x, y, out_precision, out_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void divide_decimal128_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                  int32_t x_precision, int32_t x_scale, int64_t y_high,
                                  uint64_t y_low, int32_t y_precision, int32_t y_scale,
                                  int32_t out_precision, int32_t out_scale,
                                  int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x(x_high, x_low, x_precision, x_scale);
  gandiva::BasicDecimalScalar128 y(y_high, y_low, y_precision, y_scale);
  bool overflow;

  // TODO ravindra: generate error on overflows (ARROW-4570).
  arrow::BasicDecimal128 out =
      gandiva::decimalops::Divide(context, x, y, out_precision, out_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void mod_decimal128_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                               int32_t x_precision, int32_t x_scale, int64_t y_high,
                               uint64_t y_low, int32_t y_precision, int32_t y_scale,
                               int32_t out_precision, int32_t out_scale,
                               int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x(x_high, x_low, x_precision, x_scale);
  gandiva::BasicDecimalScalar128 y(y_high, y_low, y_precision, y_scale);
  bool overflow;

  // TODO ravindra: generate error on overflows (ARROW-4570).
  arrow::BasicDecimal128 out =
      gandiva::decimalops::Mod(context, x, y, out_precision, out_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
int32_t compare_decimal128_decimal128_internal(int64_t x_high, uint64_t x_low,
                                               int32_t x_precision, int32_t x_scale,
                                               int64_t y_high, uint64_t y_low,
                                               int32_t y_precision, int32_t y_scale) {
  gandiva::BasicDecimalScalar128 x(x_high, x_low, x_precision, x_scale);
  gandiva::BasicDecimalScalar128 y(y_high, y_low, y_precision, y_scale);

  return gandiva::decimalops::Compare(x, y);
}

FORCE_INLINE
void abs_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision, int32_t x_scale,
                    int32_t out_precision, int32_t out_scale, int64_t* out_high,
                    uint64_t* out_low) {
  gandiva::BasicDecimal128 x(x_high, x_low);
  x.Abs();
  *out_high = x.high_bits();
  *out_low = x.low_bits();
}

FORCE_INLINE
void ceil_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision, int32_t x_scale,
                     int32_t out_precision, int32_t out_scale, int64_t* out_high,
                     uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Ceil(x, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void floor_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                      int32_t x_scale, int32_t out_precision, int32_t out_scale,
                      int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Floor(x, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void round_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                      int32_t x_scale, int32_t out_precision, int32_t out_scale,
                      int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Round(x, out_precision, 0, 0, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void round_decimal128_int32(int64_t x_high, uint64_t x_low, int32_t x_precision,
                            int32_t x_scale, int32_t rounding_scale,
                            int32_t out_precision, int32_t out_scale, int64_t* out_high,
                            uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out =
      gandiva::decimalops::Round(x, out_precision, out_scale, rounding_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void truncate_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                         int32_t x_scale, int32_t out_precision, int32_t out_scale,
                         int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Truncate(x, out_precision, 0, 0, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void truncate_decimal128_int32(int64_t x_high, uint64_t x_low, int32_t x_precision,
                               int32_t x_scale, int32_t rounding_scale,
                               int32_t out_precision, int32_t out_scale,
                               int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Truncate(x, out_precision, out_scale, rounding_scale,
                                           &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
double castFLOAT8_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                             int32_t x_scale) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  return gandiva::decimalops::ToDouble(x, &overflow);
}

FORCE_INLINE
int64_t castBIGINT_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                              int32_t x_scale) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  return gandiva::decimalops::ToInt64(x, &overflow);
}

FORCE_INLINE
void castDECIMAL_int64(int64_t in, int32_t x_precision, int32_t x_scale,
                       int64_t* out_high, uint64_t* out_low) {
  bool overflow = false;
  auto out = gandiva::decimalops::FromInt64(in, x_precision, x_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void castDECIMAL_int32(int32_t in, int32_t x_precision, int32_t x_scale,
                       int64_t* out_high, uint64_t* out_low) {
  castDECIMAL_int64(in, x_precision, x_scale, out_high, out_low);
}

FORCE_INLINE
void castDECIMAL_float64(double in, int32_t x_precision, int32_t x_scale,
                         int64_t* out_high, uint64_t* out_low) {
  bool overflow = false;
  auto out = gandiva::decimalops::FromDouble(in, x_precision, x_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void castDECIMAL_float32(float in, int32_t x_precision, int32_t x_scale,
                         int64_t* out_high, uint64_t* out_low) {
  castDECIMAL_float64(in, x_precision, x_scale, out_high, out_low);
}

FORCE_INLINE
bool castDecimal_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                          int32_t x_scale, int32_t out_precision, int32_t out_scale,
                          int64_t* out_high, int64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);
  bool overflow = false;
  auto out = gandiva::decimalops::Convert(x, out_precision, out_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
  return overflow;
}

FORCE_INLINE
void castDECIMAL_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                            int32_t x_scale, int32_t out_precision, int32_t out_scale,
                            int64_t* out_high, int64_t* out_low) {
  castDecimal_internal(x_high, x_low, x_precision, x_scale, out_precision, out_scale,
                       out_high, out_low);
}

FORCE_INLINE
void castDECIMALNullOnOverflow_decimal128(int64_t x_high, uint64_t x_low,
                                          int32_t x_precision, int32_t x_scale,
                                          bool x_isvalid, bool* out_valid,
                                          int32_t out_precision, int32_t out_scale,
                                          int64_t* out_high, int64_t* out_low) {
  *out_valid = true;

  if (!x_isvalid) {
    *out_valid = false;
    return;
  }

  if (castDecimal_internal(x_high, x_low, x_precision, x_scale, out_precision, out_scale,
                           out_high, out_low)) {
    *out_valid = false;
  }
}

FORCE_INLINE
int32_t hash32_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                          int32_t x_scale, gdv_boolean x_isvalid) {
  return x_isvalid
             ? hash32_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, 0)
             : 0;
}

FORCE_INLINE
int32_t hash_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                        int32_t x_scale, gdv_boolean x_isvalid) {
  return hash32_decimal128(x_high, x_low, x_precision, x_scale, x_isvalid);
}

FORCE_INLINE
int64_t hash64_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                          int32_t x_scale, gdv_boolean x_isvalid) {
  return x_isvalid
             ? hash64_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, 0)
             : 0;
}

FORCE_INLINE
int32_t hash32WithSeed_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                  int32_t x_scale, gdv_boolean x_isvalid, int32_t seed,
                                  gdv_boolean seed_isvalid) {
  if (!x_isvalid) {
    return seed;
  }
  return hash32_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, seed);
}

FORCE_INLINE
int64_t hash64WithSeed_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                  int32_t x_scale, gdv_boolean x_isvalid, int64_t seed,
                                  gdv_boolean seed_isvalid) {
  if (!x_isvalid) {
    return seed;
  }
  return hash64_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, seed);
}

FORCE_INLINE
int32_t hash32AsDouble_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                  int32_t x_scale, gdv_boolean x_isvalid) {
  return x_isvalid
             ? hash32_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, 0)
             : 0;
}

FORCE_INLINE
int64_t hash64AsDouble_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                  int32_t x_scale, gdv_boolean x_isvalid) {
  return x_isvalid
             ? hash64_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, 0)
             : 0;
}

FORCE_INLINE
int32_t hash32AsDoubleWithSeed_decimal128(int64_t x_high, uint64_t x_low,
                                          int32_t x_precision, int32_t x_scale,
                                          gdv_boolean x_isvalid, int32_t seed,
                                          gdv_boolean seed_isvalid) {
  if (!x_isvalid) {
    return seed;
  }
  return hash32_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, seed);
}

FORCE_INLINE
int64_t hash64AsDoubleWithSeed_decimal128(int64_t x_high, uint64_t x_low,
                                          int32_t x_precision, int32_t x_scale,
                                          gdv_boolean x_isvalid, int64_t seed,
                                          gdv_boolean seed_isvalid) {
  if (!x_isvalid) {
    return seed;
  }
  return hash64_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, seed);
}

FORCE_INLINE
gdv_boolean isnull_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                              int32_t x_scale, gdv_boolean x_isvalid) {
  return !x_isvalid;
}

FORCE_INLINE
gdv_boolean isnotnull_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                 int32_t x_scale, gdv_boolean x_isvalid) {
  return x_isvalid;
}

FORCE_INLINE
gdv_boolean isnumeric_decimal128(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                 int32_t x_scale, gdv_boolean x_isvalid) {
  return x_isvalid;
}

FORCE_INLINE
gdv_boolean is_not_distinct_from_decimal128_decimal128(
    int64_t x_high, uint64_t x_low, int32_t x_precision, int32_t x_scale,
    gdv_boolean x_isvalid, int64_t y_high, uint64_t y_low, int32_t y_precision,
    int32_t y_scale, gdv_boolean y_isvalid) {
  if (x_isvalid != y_isvalid) {
    return false;
  }
  if (!x_isvalid) {
    return true;
  }
  return 0 == compare_decimal128_decimal128_internal(x_high, x_low, x_precision, x_scale,
                                                     y_high, y_low, y_precision, y_scale);
}

FORCE_INLINE
gdv_boolean is_distinct_from_decimal128_decimal128(int64_t x_high, uint64_t x_low,
                                                   int32_t x_precision, int32_t x_scale,
                                                   gdv_boolean x_isvalid, int64_t y_high,
                                                   uint64_t y_low, int32_t y_precision,
                                                   int32_t y_scale,
                                                   gdv_boolean y_isvalid) {
  return !is_not_distinct_from_decimal128_decimal128(x_high, x_low, x_precision, x_scale,
                                                     x_isvalid, y_high, y_low,
                                                     y_precision, y_scale, y_isvalid);
}

FORCE_INLINE
void castDECIMAL_utf8(int64_t context, const char* in, int32_t in_length,
                      int32_t out_precision, int32_t out_scale, int64_t* out_high,
                      uint64_t* out_low) {
  int64_t dec_high_from_str;
  uint64_t dec_low_from_str;
  int32_t precision_from_str;
  int32_t scale_from_str;
  int32_t status =
      gdv_fn_dec_from_string(context, in, in_length, &precision_from_str, &scale_from_str,
                             &dec_high_from_str, &dec_low_from_str);
  if (status != 0) {
    *out_high = 0;
    *out_low = 0;
    return;
  }

  gandiva::BasicDecimalScalar128 x({dec_high_from_str, dec_low_from_str},
                                   precision_from_str, scale_from_str);
  bool overflow = false;
  auto out = gandiva::decimalops::Convert(x, out_precision, out_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
char* castVARCHAR_decimal128_int64(int64_t context, int64_t x_high, uint64_t x_low,
                                   int32_t x_precision, int32_t x_scale,
                                   int64_t out_len_param, int32_t* out_length) {
  int32_t full_dec_str_len;
  char* dec_str =
      gdv_fn_dec_to_string(context, x_high, x_low, x_scale, &full_dec_str_len);
  int32_t trunc_dec_str_len =
      out_len_param < full_dec_str_len ? out_len_param : full_dec_str_len;
  *out_length = trunc_dec_str_len;
  return dec_str;
}

}  // extern "C"

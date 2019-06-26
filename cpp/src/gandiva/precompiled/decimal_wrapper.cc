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
void multiply_internal_decimal128_decimal128(int64_t x_high, uint64_t x_low,
                                             int32_t x_precision, int32_t x_scale,
                                             int64_t y_high, uint64_t y_low,
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
void divide_internal_decimal128_decimal128(
    int64_t context, int64_t x_high, uint64_t x_low, int32_t x_precision, int32_t x_scale,
    int64_t y_high, uint64_t y_low, int32_t y_precision, int32_t y_scale,
    int32_t out_precision, int32_t out_scale, int64_t* out_high, uint64_t* out_low) {
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
void mod_internal_decimal128_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                        int32_t x_precision, int32_t x_scale,
                                        int64_t y_high, uint64_t y_low,
                                        int32_t y_precision, int32_t y_scale,
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
int32_t compare_internal_decimal128_decimal128(int64_t x_high, uint64_t x_low,
                                               int32_t x_precision, int32_t x_scale,
                                               int64_t y_high, uint64_t y_low,
                                               int32_t y_precision, int32_t y_scale) {
  gandiva::BasicDecimalScalar128 x(x_high, x_low, x_precision, x_scale);
  gandiva::BasicDecimalScalar128 y(y_high, y_low, y_precision, y_scale);

  return gandiva::decimalops::Compare(x, y);
}

FORCE_INLINE
void abs_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                             int32_t x_scale, int32_t out_precision, int32_t out_scale,
                             int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimal128 x(x_high, x_low);
  x.Abs();
  *out_high = x.high_bits();
  *out_low = x.low_bits();
}

FORCE_INLINE
void ceil_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                              int32_t x_scale, int32_t out_precision, int32_t out_scale,
                              int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Ceil(x, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void floor_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                               int32_t x_scale, int32_t out_precision, int32_t out_scale,
                               int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Floor(x, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void round_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                               int32_t x_scale, int32_t out_precision, int32_t out_scale,
                               int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Round(x, 0, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void round_decimal128_int32_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                     int32_t x_scale, int32_t rounding_scale,
                                     int32_t out_precision, int32_t out_scale,
                                     int64_t* out_high, uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Round(x, rounding_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void truncate_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                  int32_t x_scale, int32_t out_precision,
                                  int32_t out_scale, int64_t* out_high,
                                  uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Truncate(x, 0, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void truncate_decimal128_int32_internal(int64_t x_high, uint64_t x_low,
                                        int32_t x_precision, int32_t x_scale,
                                        int32_t rounding_scale, int32_t out_precision,
                                        int32_t out_scale, int64_t* out_high,
                                        uint64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  auto out = gandiva::decimalops::Truncate(x, rounding_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
double castFLOAT8_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                      int32_t x_scale) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  return gandiva::decimalops::ToDouble(x, &overflow);
}

FORCE_INLINE
int64_t castBIGINT_decimal128_internal(int64_t x_high, uint64_t x_low,
                                       int32_t x_precision, int32_t x_scale) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);

  bool overflow = false;
  return gandiva::decimalops::ToInt64(x, &overflow);
}

FORCE_INLINE
void castDECIMAL_int64_internal(int64_t in, int32_t x_precision, int32_t x_scale,
                                int64_t* out_high, uint64_t* out_low) {
  bool overflow = false;
  auto out = gandiva::decimalops::FromInt64(in, x_precision, x_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void castDECIMAL_float64_internal(double in, int32_t x_precision, int32_t x_scale,
                                  int64_t* out_high, uint64_t* out_low) {
  bool overflow = false;
  auto out = gandiva::decimalops::FromDouble(in, x_precision, x_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
void castDECIMAL_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                     int32_t x_scale, int32_t out_precision,
                                     int32_t out_scale, int64_t* out_high,
                                     int64_t* out_low) {
  gandiva::BasicDecimalScalar128 x({x_high, x_low}, x_precision, x_scale);
  bool overflow = false;
  auto out = gandiva::decimalops::Convert(x, out_precision, out_scale, &overflow);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

FORCE_INLINE
int32_t hash32_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                   int32_t x_scale, boolean x_isvalid) {
  return x_isvalid
             ? hash32_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, 0)
             : 0;
}

FORCE_INLINE
int32_t hash_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                 int32_t x_scale, boolean x_isvalid) {
  return hash32_decimal128_internal(x_high, x_low, x_precision, x_scale, x_isvalid);
}

FORCE_INLINE
int64_t hash64_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                   int32_t x_scale, boolean x_isvalid) {
  return x_isvalid
             ? hash64_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, 0)
             : 0;
}

FORCE_INLINE
int32_t hash32WithSeed_decimal128_internal(int64_t x_high, uint64_t x_low,
                                           int32_t x_precision, int32_t x_scale,
                                           boolean x_isvalid, int32_t seed,
                                           boolean seed_isvalid) {
  if (!x_isvalid) {
    return seed;
  }
  return hash32_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, seed);
}

FORCE_INLINE
int64_t hash64WithSeed_decimal128_internal(int64_t x_high, uint64_t x_low,
                                           int32_t x_precision, int32_t x_scale,
                                           boolean x_isvalid, int64_t seed,
                                           boolean seed_isvalid) {
  if (!x_isvalid) {
    return seed;
  }
  return hash64_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, seed);
}

FORCE_INLINE
int32_t hash32AsDouble_decimal128_internal(int64_t x_high, uint64_t x_low,
                                           int32_t x_precision, int32_t x_scale,
                                           boolean x_isvalid) {
  return x_isvalid
             ? hash32_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, 0)
             : 0;
}

FORCE_INLINE
int64_t hash64AsDouble_decimal128_internal(int64_t x_high, uint64_t x_low,
                                           int32_t x_precision, int32_t x_scale,
                                           boolean x_isvalid) {
  return x_isvalid
             ? hash64_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, 0)
             : 0;
}

FORCE_INLINE
int32_t hash32AsDoubleWithSeed_decimal128_internal(int64_t x_high, uint64_t x_low,
                                                   int32_t x_precision, int32_t x_scale,
                                                   boolean x_isvalid, int32_t seed,
                                                   boolean seed_isvalid) {
  if (!x_isvalid) {
    return seed;
  }
  return hash32_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, seed);
}

FORCE_INLINE
int64_t hash64AsDoubleWithSeed_decimal128_internal(int64_t x_high, uint64_t x_low,
                                                   int32_t x_precision, int32_t x_scale,
                                                   boolean x_isvalid, int64_t seed,
                                                   boolean seed_isvalid) {
  if (!x_isvalid) {
    return seed;
  }
  return hash64_buf(gandiva::BasicDecimal128(x_high, x_low).ToBytes().data(), 16, seed);
}

FORCE_INLINE
boolean isnull_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                   int32_t x_scale, boolean x_isvalid) {
  return !x_isvalid;
}

FORCE_INLINE
boolean isnotnull_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                      int32_t x_scale, boolean x_isvalid) {
  return x_isvalid;
}

FORCE_INLINE
boolean isnumeric_decimal128_internal(int64_t x_high, uint64_t x_low, int32_t x_precision,
                                      int32_t x_scale, boolean x_isvalid) {
  return x_isvalid;
}

FORCE_INLINE
boolean is_not_distinct_from_decimal128_decimal128_internal(
    int64_t x_high, uint64_t x_low, int32_t x_precision, int32_t x_scale,
    boolean x_isvalid, int64_t y_high, uint64_t y_low, int32_t y_precision,
    int32_t y_scale, boolean y_isvalid) {
  if (x_isvalid != y_isvalid) {
    return false;
  }
  if (!x_isvalid) {
    return true;
  }
  return 0 == compare_internal_decimal128_decimal128(x_high, x_low, x_precision, x_scale,
                                                     y_high, y_low, y_precision, y_scale);
}

FORCE_INLINE
boolean is_distinct_from_decimal128_decimal128_internal(
    int64_t x_high, uint64_t x_low, int32_t x_precision, int32_t x_scale,
    boolean x_isvalid, int64_t y_high, uint64_t y_low, int32_t y_precision,
    int32_t y_scale, boolean y_isvalid) {
  return !is_not_distinct_from_decimal128_decimal128_internal(
      x_high, x_low, x_precision, x_scale, x_isvalid, y_high, y_low, y_precision, y_scale,
      y_isvalid);
}

}  // extern "C"

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

#pragma once

#include <cstdint>

#include "gandiva/gdv_function_stubs.h"

// Use the same names as in arrow data types. Makes it easy to write pre-processor macros.
using gdv_boolean = bool;
using gdv_int8 = int8_t;
using gdv_int16 = int16_t;
using gdv_int32 = int32_t;
using gdv_int64 = int64_t;
using gdv_uint8 = uint8_t;
using gdv_uint16 = uint16_t;
using gdv_uint32 = uint32_t;
using gdv_uint64 = uint64_t;
using gdv_float32 = float;
using gdv_float64 = double;
using gdv_date64 = int64_t;
using gdv_date32 = int32_t;
using gdv_time32 = int32_t;
using gdv_timestamp = int64_t;
using gdv_utf8 = char*;
using gdv_binary = char*;
using gdv_day_time_interval = int64_t;

#ifdef GANDIVA_UNIT_TEST
// unit tests may be compiled without O2, so inlining may not happen.
#define FORCE_INLINE
#else
#define FORCE_INLINE __attribute__((always_inline))
#endif

extern "C" {

bool bitMapGetBit(const unsigned char* bmap, int64_t position);
void bitMapSetBit(unsigned char* bmap, int64_t position, bool value);
void bitMapClearBitIfFalse(unsigned char* bmap, int64_t position, bool value);

gdv_int64 extractMillennium_timestamp(gdv_timestamp millis);
gdv_int64 extractCentury_timestamp(gdv_timestamp millis);
gdv_int64 extractDecade_timestamp(gdv_timestamp millis);
gdv_int64 extractYear_timestamp(gdv_timestamp millis);
gdv_int64 extractDoy_timestamp(gdv_timestamp millis);
gdv_int64 extractQuarter_timestamp(gdv_timestamp millis);
gdv_int64 extractMonth_timestamp(gdv_timestamp millis);
gdv_int64 extractWeek_timestamp(gdv_timestamp millis);
gdv_int64 extractDow_timestamp(gdv_timestamp millis);
gdv_int64 extractDay_timestamp(gdv_timestamp millis);
gdv_int64 extractHour_timestamp(gdv_timestamp millis);
gdv_int64 extractMinute_timestamp(gdv_timestamp millis);
gdv_int64 extractSecond_timestamp(gdv_timestamp millis);
gdv_int64 extractHour_time32(gdv_int32 millis_in_day);
gdv_int64 extractMinute_time32(gdv_int32 millis_in_day);
gdv_int64 extractSecond_time32(gdv_int32 millis_in_day);

gdv_int32 hash32(double val, gdv_int32 seed);
gdv_int32 hash32_buf(const gdv_uint8* buf, int len, gdv_int32 seed);
gdv_int64 hash64(double val, gdv_int64 seed);
gdv_int64 hash64_buf(const gdv_uint8* buf, int len, gdv_int64 seed);

gdv_int32 timestampdiffMonth_timestamp_timestamp(gdv_timestamp, gdv_timestamp);

gdv_int64 timestampaddSecond_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddMinute_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddHour_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddDay_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddWeek_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddMonth_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddQuarter_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddYear_int32_timestamp(gdv_int32, gdv_timestamp);

gdv_int64 timestampaddSecond_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 timestampaddMinute_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 timestampaddHour_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 timestampaddDay_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 timestampaddWeek_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 timestampaddMonth_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 timestampaddQuarter_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 timestampaddYear_timestamp_int32(gdv_timestamp, gdv_int32);

gdv_int64 timestampaddSecond_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddMinute_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddHour_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddDay_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddWeek_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddMonth_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddQuarter_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddYear_int64_timestamp(gdv_int64, gdv_timestamp);

gdv_int64 timestampaddSecond_timestamp_int64(gdv_timestamp, gdv_int64);
gdv_int64 timestampaddMinute_timestamp_int64(gdv_timestamp, gdv_int64);
gdv_int64 timestampaddHour_timestamp_int64(gdv_timestamp, gdv_int64);
gdv_int64 timestampaddDay_timestamp_int64(gdv_timestamp, gdv_int64);
gdv_int64 timestampaddWeek_timestamp_int64(gdv_timestamp, gdv_int64);
gdv_int64 timestampaddMonth_timestamp_int64(gdv_timestamp, gdv_int64);
gdv_int64 timestampaddQuarter_timestamp_int64(gdv_timestamp, gdv_int64);
gdv_int64 timestampaddYear_timestamp_int64(gdv_timestamp, gdv_int64);

gdv_int64 date_add_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 add_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 add_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 date_add_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_timestamp add_date64_int64(gdv_date64, gdv_int64);

gdv_timestamp to_timestamp_int32(gdv_int32);
gdv_timestamp to_timestamp_int64(gdv_int64);
gdv_timestamp to_timestamp_float32(gdv_float32);
gdv_timestamp to_timestamp_float64(gdv_float64);

gdv_time32 to_time_int32(gdv_int32);
gdv_time32 to_time_int64(gdv_int64);
gdv_time32 to_time_float32(gdv_float32);
gdv_time32 to_time_float64(gdv_float64);

gdv_int64 date_sub_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 subtract_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 date_diff_timestamp_int64(gdv_timestamp, gdv_int64);

gdv_boolean castBIT_utf8(gdv_int64 context, const char* data, gdv_int32 data_len);

bool is_distinct_from_timestamp_timestamp(gdv_int64, bool, gdv_int64, bool);
bool is_not_distinct_from_int32_int32(gdv_int32, bool, gdv_int32, bool);

gdv_int64 date_trunc_Second_date64(gdv_date64);
gdv_int64 date_trunc_Minute_date64(gdv_date64);
gdv_int64 date_trunc_Hour_date64(gdv_date64);
gdv_int64 date_trunc_Day_date64(gdv_date64);
gdv_int64 date_trunc_Month_date64(gdv_date64);
gdv_int64 date_trunc_Quarter_date64(gdv_date64);
gdv_int64 date_trunc_Year_date64(gdv_date64);
gdv_int64 date_trunc_Decade_date64(gdv_date64);
gdv_int64 date_trunc_Century_date64(gdv_date64);
gdv_int64 date_trunc_Millennium_date64(gdv_date64);

gdv_int64 date_trunc_Week_timestamp(gdv_timestamp);
double months_between_timestamp_timestamp(gdv_uint64, gdv_uint64);

gdv_int32 mem_compare(const char* left, gdv_int32 left_len, const char* right,
                      gdv_int32 right_len);

gdv_int32 mod_int64_int32(gdv_int64 left, gdv_int32 right);
gdv_float64 mod_float64_float64(gdv_int64 context, gdv_float64 left, gdv_float64 right);

gdv_int64 divide_int64_int64(gdv_int64 context, gdv_int64 in1, gdv_int64 in2);

gdv_int64 div_int64_int64(gdv_int64 context, gdv_int64 in1, gdv_int64 in2);
gdv_float32 div_float32_float32(gdv_int64 context, gdv_float32 in1, gdv_float32 in2);
gdv_float64 div_float64_float64(gdv_int64 context, gdv_float64 in1, gdv_float64 in2);

gdv_float32 round_float32(gdv_float32);
gdv_float64 round_float64(gdv_float64);
gdv_float64 bround_float64(gdv_float64);
gdv_float32 round_float32_int32(gdv_float32 number, gdv_int32 out_scale);
gdv_float64 round_float64_int32(gdv_float64 number, gdv_int32 out_scale);
gdv_float64 get_scale_multiplier(gdv_int32);
gdv_int32 round_int32_int32(gdv_int32 number, gdv_int32 precision);
gdv_int64 round_int64_int32(gdv_int64 number, gdv_int32 precision);
gdv_int32 round_int32(gdv_int32);
gdv_int64 round_int64(gdv_int64);
gdv_int64 get_power_of_10(gdv_int32);

const char* bin_int32(int64_t context, gdv_int32 value, int32_t* out_len);
const char* bin_int64(int64_t context, gdv_int64 value, int32_t* out_len);

gdv_float64 cbrt_int32(gdv_int32);
gdv_float64 cbrt_int64(gdv_int64);
gdv_float64 cbrt_float32(gdv_float32);
gdv_float64 cbrt_float64(gdv_float64);

gdv_float64 exp_int32(gdv_int32);
gdv_float64 exp_int64(gdv_int64);
gdv_float64 exp_float32(gdv_float32);
gdv_float64 exp_float64(gdv_float64);

gdv_float64 log_int32(gdv_int32);
gdv_float64 log_int64(gdv_int64);
gdv_float64 log_float32(gdv_float32);
gdv_float64 log_float64(gdv_float64);

gdv_float64 log10_int32(gdv_int32);
gdv_float64 log10_int64(gdv_int64);
gdv_float64 log10_float32(gdv_float32);
gdv_float64 log10_float64(gdv_float64);

gdv_float64 sin_int32(gdv_int32);
gdv_float64 sin_int64(gdv_int64);
gdv_float64 sin_float32(gdv_float32);
gdv_float64 sin_float64(gdv_float64);
gdv_float64 cos_int32(gdv_int32);
gdv_float64 cos_int64(gdv_int64);
gdv_float64 cos_float32(gdv_float32);
gdv_float64 cos_float64(gdv_float64);
gdv_float64 asin_int32(gdv_int32);
gdv_float64 asin_int64(gdv_int64);
gdv_float64 asin_float32(gdv_float32);
gdv_float64 asin_float64(gdv_float64);
gdv_float64 acos_int32(gdv_int32);
gdv_float64 acos_int64(gdv_int64);
gdv_float64 acos_float32(gdv_float32);
gdv_float64 acos_float64(gdv_float64);
gdv_float64 tan_int32(gdv_int32);
gdv_float64 tan_int64(gdv_int64);
gdv_float64 tan_float32(gdv_float32);
gdv_float64 tan_float64(gdv_float64);
gdv_float64 atan_int32(gdv_int32);
gdv_float64 atan_int64(gdv_int64);
gdv_float64 atan_float32(gdv_float32);
gdv_float64 atan_float64(gdv_float64);
gdv_float64 sinh_int32(gdv_int32);
gdv_float64 sinh_int64(gdv_int64);
gdv_float64 sinh_float32(gdv_float32);
gdv_float64 sinh_float64(gdv_float64);
gdv_float64 cosh_int32(gdv_int32);
gdv_float64 cosh_int64(gdv_int64);
gdv_float64 cosh_float32(gdv_float32);
gdv_float64 cosh_float64(gdv_float64);
gdv_float64 tanh_int32(gdv_int32);
gdv_float64 tanh_int64(gdv_int64);
gdv_float64 tanh_float32(gdv_float32);
gdv_float64 tanh_float64(gdv_float64);
gdv_float64 atan2_int32_int32(gdv_int32 in1, gdv_int32 in2);
gdv_float64 atan2_int64_int64(gdv_int64 in1, gdv_int64 in2);
gdv_float64 atan2_float32_float32(gdv_float32 in1, gdv_float32 in2);
gdv_float64 atan2_float64_float64(gdv_float64 in1, gdv_float64 in2);
gdv_float64 cot_float32(gdv_float32);
gdv_float64 cot_float64(gdv_float64);
gdv_float64 radians_int32(gdv_int32);
gdv_float64 radians_int64(gdv_int64);
gdv_float64 radians_float32(gdv_float32);
gdv_float64 radians_float64(gdv_float64);
gdv_float64 degrees_int32(gdv_int32);
gdv_float64 degrees_int64(gdv_int64);
gdv_float64 degrees_float32(gdv_float32);
gdv_float64 degrees_float64(gdv_float64);

gdv_int32 bitwise_and_int32_int32(gdv_int32 in1, gdv_int32 in2);
gdv_int64 bitwise_and_int64_int64(gdv_int64 in1, gdv_int64 in2);
gdv_int32 bitwise_or_int32_int32(gdv_int32 in1, gdv_int32 in2);
gdv_int64 bitwise_or_int64_int64(gdv_int64 in1, gdv_int64 in2);
gdv_int32 bitwise_xor_int32_int32(gdv_int32 in1, gdv_int32 in2);
gdv_int64 bitwise_xor_int64_int64(gdv_int64 in1, gdv_int64 in2);
gdv_int32 bitwise_not_int32(gdv_int32);
gdv_int64 bitwise_not_int64(gdv_int64);

gdv_float64 power_float64_float64(gdv_float64, gdv_float64);

gdv_float64 log_int32_int32(gdv_int64 context, gdv_int32 base, gdv_int32 value);

bool starts_with_utf8_utf8(const char* data, gdv_int32 data_len, const char* prefix,
                           gdv_int32 prefix_len);
bool ends_with_utf8_utf8(const char* data, gdv_int32 data_len, const char* suffix,
                         gdv_int32 suffix_len);
bool is_substr_utf8_utf8(const char* data, gdv_int32 data_len, const char* substr,
                         gdv_int32 substr_len);

gdv_int32 utf8_length(gdv_int64 context, const char* data, gdv_int32 data_len);

gdv_int32 utf8_last_char_pos(gdv_int64 context, const char* data, gdv_int32 data_len);

gdv_date64 castDATE_utf8(int64_t execution_context, const char* input, gdv_int32 length);

gdv_date64 castDATE_int64(gdv_int64 date);

gdv_date64 castDATE_date32(gdv_date32 date);

gdv_date32 castDATE_int32(gdv_int32 date);

gdv_timestamp castTIMESTAMP_utf8(int64_t execution_context, const char* input,
                                 gdv_int32 length);
gdv_timestamp castTIMESTAMP_date64(gdv_date64);
gdv_timestamp castTIMESTAMP_int64(gdv_int64);
gdv_date64 castDATE_timestamp(gdv_timestamp);
gdv_time32 castTIME_timestamp(gdv_timestamp timestamp_in_millis);
const char* castVARCHAR_timestamp_int64(int64_t, gdv_timestamp, gdv_int64, gdv_int32*);
gdv_date64 last_day_from_timestamp(gdv_date64 millis);

gdv_int64 truncate_int64_int32(gdv_int64 in, gdv_int32 out_scale);

const char* repeat_utf8_int32(gdv_int64 context, const char* in, gdv_int32 in_len,
                              gdv_int32 repeat_times, gdv_int32* out_len);

const char* substr_utf8_int64_int64(gdv_int64 context, const char* input,
                                    gdv_int32 in_len, gdv_int64 offset64,
                                    gdv_int64 length, gdv_int32* out_len);
const char* substr_utf8_int64(gdv_int64 context, const char* input, gdv_int32 in_len,
                              gdv_int64 offset64, gdv_int32* out_len);

const char* concat_utf8_utf8(gdv_int64 context, const char* left, gdv_int32 left_len,
                             bool left_validity, const char* right, gdv_int32 right_len,
                             bool right_validity, gdv_int32* out_len);
const char* concat_utf8_utf8_utf8(gdv_int64 context, const char* in1, gdv_int32 in1_len,
                                  bool in1_validity, const char* in2, gdv_int32 in2_len,
                                  bool in2_validity, const char* in3, gdv_int32 in3_len,
                                  bool in3_validity, gdv_int32* out_len);
const char* concat_utf8_utf8_utf8_utf8(gdv_int64 context, const char* in1,
                                       gdv_int32 in1_len, bool in1_validity,
                                       const char* in2, gdv_int32 in2_len,
                                       bool in2_validity, const char* in3,
                                       gdv_int32 in3_len, bool in3_validity,
                                       const char* in4, gdv_int32 in4_len,
                                       bool in4_validity, gdv_int32* out_len);
const char* space_int32(gdv_int64 ctx, gdv_int32 n, int32_t* out_len);
const char* space_int64(gdv_int64 ctx, gdv_int64 n, int32_t* out_len);
const char* concat_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    gdv_int32* out_len);
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, gdv_int32* out_len);
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, const char* in7,
    gdv_int32 in7_len, bool in7_validity, gdv_int32* out_len);
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, const char* in7,
    gdv_int32 in7_len, bool in7_validity, const char* in8, gdv_int32 in8_len,
    bool in8_validity, gdv_int32* out_len);
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, const char* in7,
    gdv_int32 in7_len, bool in7_validity, const char* in8, gdv_int32 in8_len,
    bool in8_validity, const char* in9, gdv_int32 in9_len, bool in9_validity,
    gdv_int32* out_len);
const char* concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, bool in1_validity,
    const char* in2, gdv_int32 in2_len, bool in2_validity, const char* in3,
    gdv_int32 in3_len, bool in3_validity, const char* in4, gdv_int32 in4_len,
    bool in4_validity, const char* in5, gdv_int32 in5_len, bool in5_validity,
    const char* in6, gdv_int32 in6_len, bool in6_validity, const char* in7,
    gdv_int32 in7_len, bool in7_validity, const char* in8, gdv_int32 in8_len,
    bool in8_validity, const char* in9, gdv_int32 in9_len, bool in9_validity,
    const char* in10, gdv_int32 in10_len, bool in10_validity, gdv_int32* out_len);

const char* concatOperator_utf8_utf8(gdv_int64 context, const char* left,
                                     gdv_int32 left_len, const char* right,
                                     gdv_int32 right_len, gdv_int32* out_len);
const char* concatOperator_utf8_utf8_utf8(gdv_int64 context, const char* in1,
                                          gdv_int32 in1_len, const char* in2,
                                          gdv_int32 in2_len, const char* in3,
                                          gdv_int32 in3_len, gdv_int32* out_len);
const char* concatOperator_utf8_utf8_utf8_utf8(gdv_int64 context, const char* in1,
                                               gdv_int32 in1_len, const char* in2,
                                               gdv_int32 in2_len, const char* in3,
                                               gdv_int32 in3_len, const char* in4,
                                               gdv_int32 in4_len, gdv_int32* out_len);
const char* concatOperator_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, gdv_int32* out_len);
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, gdv_int32* out_len);
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, const char* in7, gdv_int32 in7_len, gdv_int32* out_len);
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, const char* in7, gdv_int32 in7_len, const char* in8,
    gdv_int32 in8_len, gdv_int32* out_len);
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, const char* in7, gdv_int32 in7_len, const char* in8,
    gdv_int32 in8_len, const char* in9, gdv_int32 in9_len, gdv_int32* out_len);
const char* concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
    gdv_int64 context, const char* in1, gdv_int32 in1_len, const char* in2,
    gdv_int32 in2_len, const char* in3, gdv_int32 in3_len, const char* in4,
    gdv_int32 in4_len, const char* in5, gdv_int32 in5_len, const char* in6,
    gdv_int32 in6_len, const char* in7, gdv_int32 in7_len, const char* in8,
    gdv_int32 in8_len, const char* in9, gdv_int32 in9_len, const char* in10,
    gdv_int32 in10_len, gdv_int32* out_len);

const char* castVARCHAR_binary_int64(gdv_int64 context, const char* data,
                                     gdv_int32 data_len, int64_t out_len,
                                     int32_t* out_length);

const char* castVARCHAR_utf8_int64(gdv_int64 context, const char* data,
                                   gdv_int32 data_len, int64_t out_len,
                                   int32_t* out_length);

const char* castVARBINARY_utf8_int64(gdv_int64 context, const char* data,
                                     gdv_int32 data_len, int64_t out_len,
                                     int32_t* out_length);

const char* castVARBINARY_binary_int64(gdv_int64 context, const char* data,
                                       gdv_int32 data_len, int64_t out_len,
                                       int32_t* out_length);

const char* reverse_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                         int32_t* out_len);

const char* ltrim_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                       int32_t* out_len);

const char* rtrim_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                       int32_t* out_len);

const char* btrim_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                       int32_t* out_len);

const char* ltrim_utf8_utf8(gdv_int64 context, const char* basetext,
                            gdv_int32 basetext_len, const char* trimtext,
                            gdv_int32 trimtext_len, int32_t* out_len);

const char* rtrim_utf8_utf8(gdv_int64 context, const char* basetext,
                            gdv_int32 basetext_len, const char* trimtext,
                            gdv_int32 trimtext_len, int32_t* out_len);

const char* btrim_utf8_utf8(gdv_int64 context, const char* basetext,
                            gdv_int32 basetext_len, const char* trimtext,
                            gdv_int32 trimtext_len, int32_t* out_len);

gdv_int32 ascii_utf8(const char* data, gdv_int32 data_len);

gdv_int32 locate_utf8_utf8(gdv_int64 context, const char* sub_str, gdv_int32 sub_str_len,
                           const char* str, gdv_int32 str_len);

gdv_int32 strpos_utf8_utf8(gdv_int64 context, const char* str, gdv_int32 str_len,
                           const char* sub_str, gdv_int32 sub_str_len);

gdv_int32 locate_utf8_utf8_int32(gdv_int64 context, const char* sub_str,
                                 gdv_int32 sub_str_len, const char* str,
                                 gdv_int32 str_len, gdv_int32 start_pos);

const char* lpad_utf8_int32_utf8(gdv_int64 context, const char* text, gdv_int32 text_len,
                                 gdv_int32 return_length, const char* fill_text,
                                 gdv_int32 fill_text_len, gdv_int32* out_len);

const char* rpad_utf8_int32_utf8(gdv_int64 context, const char* text, gdv_int32 text_len,
                                 gdv_int32 return_length, const char* fill_text,
                                 gdv_int32 fill_text_len, gdv_int32* out_len);

const char* lpad_utf8_int32(gdv_int64 context, const char* text, gdv_int32 text_len,
                            gdv_int32 return_length, gdv_int32* out_len);

const char* rpad_utf8_int32(gdv_int64 context, const char* text, gdv_int32 text_len,
                            gdv_int32 return_length, gdv_int32* out_len);

const char* replace_with_max_len_utf8_utf8_utf8(gdv_int64 context, const char* text,
                                                gdv_int32 text_len, const char* from_str,
                                                gdv_int32 from_str_len,
                                                const char* to_str, gdv_int32 to_str_len,
                                                gdv_int32 max_length, gdv_int32* out_len);

const char* replace_utf8_utf8_utf8(gdv_int64 context, const char* text,
                                   gdv_int32 text_len, const char* from_str,
                                   gdv_int32 from_str_len, const char* to_str,
                                   gdv_int32 to_str_len, gdv_int32* out_len);

const char* convert_replace_invalid_fromUTF8_binary(int64_t context, const char* text_in,
                                                    int32_t text_len,
                                                    const char* char_to_replace,
                                                    int32_t char_to_replace_len,
                                                    int32_t* out_len);

const char* convert_toDOUBLE(int64_t context, double value, int32_t* out_len);

const char* convert_toDOUBLE_be(int64_t context, double value, int32_t* out_len);

const char* convert_toFLOAT(int64_t context, float value, int32_t* out_len);

const char* convert_toFLOAT_be(int64_t context, float value, int32_t* out_len);

const char* convert_toBIGINT(int64_t context, int64_t value, int32_t* out_len);

const char* convert_toBIGINT_be(int64_t context, int64_t value, int32_t* out_len);

const char* convert_toINT(int64_t context, int32_t value, int32_t* out_len);

const char* convert_toINT_be(int64_t context, int32_t value, int32_t* out_len);

const char* convert_toBOOLEAN(int64_t context, bool value, int32_t* out_len);

const char* convert_toTIME_EPOCH(int64_t context, int32_t value, int32_t* out_len);

const char* convert_toTIME_EPOCH_be(int64_t context, int32_t value, int32_t* out_len);

const char* convert_toTIMESTAMP_EPOCH(int64_t context, int64_t timestamp,
                                      int32_t* out_len);
const char* convert_toTIMESTAMP_EPOCH_be(int64_t context, int64_t timestamp,
                                         int32_t* out_len);

const char* convert_toDATE_EPOCH(int64_t context, int64_t date, int32_t* out_len);

const char* convert_toDATE_EPOCH_be(int64_t context, int64_t date, int32_t* out_len);

const char* convert_toUTF8(int64_t context, const char* value, int32_t value_len,
                           int32_t* out_len);

const char* split_part(gdv_int64 context, const char* text, gdv_int32 text_len,
                       const char* splitter, gdv_int32 split_len, gdv_int32 index,
                       gdv_int32* out_len);

const char* byte_substr_binary_int32_int32(gdv_int64 context, const char* text,
                                           gdv_int32 text_len, gdv_int32 offset,
                                           gdv_int32 length, gdv_int32* out_len);

const char* castVARCHAR_bool_int64(gdv_int64 context, gdv_boolean value,
                                   gdv_int64 out_len, gdv_int32* out_length);

const char* castVARCHAR_int32_int64(int64_t context, int32_t value, int64_t len,
                                    int32_t* out_len);

const char* castVARCHAR_int64_int64(int64_t context, int64_t value, int64_t len,
                                    int32_t* out_len);

const char* castVARCHAR_float32_int64(int64_t context, float value, int64_t len,
                                      int32_t* out_len);

const char* castVARCHAR_float64_int64(int64_t context, double value, int64_t len,
                                      int32_t* out_len);

const char* left_utf8_int32(gdv_int64 context, const char* text, gdv_int32 text_len,
                            gdv_int32 number, gdv_int32* out_len);

const char* right_utf8_int32(gdv_int64 context, const char* text, gdv_int32 text_len,
                             gdv_int32 number, gdv_int32* out_len);

const char* binary_string(gdv_int64 context, const char* text, gdv_int32 text_len,
                          gdv_int32* out_len);

int32_t castINT_utf8(int64_t context, const char* data, int32_t len);

int64_t castBIGINT_utf8(int64_t context, const char* data, int32_t len);

float castFLOAT4_utf8(int64_t context, const char* data, int32_t len);

double castFLOAT8_utf8(int64_t context, const char* data, int32_t len);

int32_t castINT_float32(gdv_float32 value);

int32_t castINT_float64(gdv_float64 value);

int64_t castBIGINT_float32(gdv_float32 value);

int64_t castBIGINT_float64(gdv_float64 value);

int64_t castBIGINT_daytimeinterval(gdv_day_time_interval in);

int32_t castINT_year_interval(gdv_month_interval in);

int64_t castBIGINT_year_interval(gdv_month_interval in);

gdv_day_time_interval castNULLABLEINTERVALDAY_int32(gdv_int32 in);

gdv_day_time_interval castNULLABLEINTERVALDAY_int64(gdv_int64 in);

gdv_month_interval castNULLABLEINTERVALYEAR_int32(int64_t context, gdv_int32 in);

gdv_month_interval castNULLABLEINTERVALYEAR_int64(int64_t context, gdv_int64 in);

const char* concat_ws_utf8_utf8(int64_t context, const char* separator,
                                int32_t separator_len, const char* word1,
                                int32_t word1_len, const char* word2, int32_t word2_len,
                                int32_t* out_len);

const char* concat_ws_utf8_utf8_utf8(int64_t context, const char* separator,
                                     int32_t separator_len, const char* word1,
                                     int32_t word1_len, const char* word2,
                                     int32_t word2_len, const char* word3,
                                     int32_t word3_len, int32_t* out_len);

const char* concat_ws_utf8_utf8_utf8_utf8(int64_t context, const char* separator,
                                          int32_t separator_len, const char* word1,
                                          int32_t word1_len, const char* word2,
                                          int32_t word2_len, const char* word3,
                                          int32_t word3_len, const char* word4,
                                          int32_t word4_len, int32_t* out_len);

const char* concat_ws_utf8_utf8_utf8_utf8_utf8(int64_t context, const char* separator,
                                               int32_t separator_len, const char* word1,
                                               int32_t word1_len, const char* word2,
                                               int32_t word2_len, const char* word3,
                                               int32_t word3_len, const char* word4,
                                               int32_t word4_len, const char* word5,
                                               int32_t word5_len, int32_t* out_len);

}  // extern "C"

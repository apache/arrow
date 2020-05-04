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

gdv_int64 timestampaddSecond_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddMinute_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddHour_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddDay_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddWeek_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddMonth_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddQuarter_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 timestampaddYear_int32_timestamp(gdv_int32, gdv_timestamp);

gdv_int64 timestampaddSecond_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddMinute_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddHour_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddDay_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddWeek_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddMonth_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddQuarter_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 timestampaddYear_int64_timestamp(gdv_int64, gdv_timestamp);

gdv_int64 date_add_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 add_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_int64 add_int32_timestamp(gdv_int32, gdv_timestamp);
gdv_int64 date_add_int64_timestamp(gdv_int64, gdv_timestamp);
gdv_timestamp add_date64_int64(gdv_date64, gdv_int64);

gdv_int64 date_sub_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 subtract_timestamp_int32(gdv_timestamp, gdv_int32);
gdv_int64 date_diff_timestamp_int64(gdv_timestamp, gdv_int64);

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

gdv_float64 power_float64_float64(gdv_float64, gdv_float64);

gdv_float64 log_int32_int32(gdv_int64 context, gdv_int32 base, gdv_int32 value);

bool starts_with_utf8_utf8(const char* data, gdv_int32 data_len, const char* prefix,
                           gdv_int32 prefix_len);
bool ends_with_utf8_utf8(const char* data, gdv_int32 data_len, const char* suffix,
                         gdv_int32 suffix_len);
bool is_substr_utf8_utf8(const char* data, gdv_int32 data_len, const char* substr,
                         gdv_int32 substr_len);

gdv_int32 utf8_length(gdv_int64 context, const char* data, gdv_int32 data_len);

gdv_date64 castDATE_utf8(int64_t execution_context, const char* input, gdv_int32 length);

gdv_date64 castDATE_int64(gdv_int64 date);

gdv_date64 castDATE_date32(gdv_date32 date);

gdv_date32 castDATE_int32(gdv_int32 date);

gdv_timestamp castTIMESTAMP_utf8(int64_t execution_context, const char* input,
                                 gdv_int32 length);
gdv_timestamp castTIMESTAMP_date64(gdv_date64);
const char* castVARCHAR_timestamp_int64(int64_t, gdv_timestamp, gdv_int64, gdv_int32*);

gdv_int64 truncate_int64_int32(gdv_int64 in, gdv_int32 out_scale);

const char* substr_utf8_int64_int64(gdv_int64 context, const char* input,
                                    gdv_int32 in_len, gdv_int64 offset64,
                                    gdv_int64 length, gdv_int32* out_len);
const char* substr_utf8_int64(gdv_int64 context, const char* input, gdv_int32 in_len,
                              gdv_int64 offset64, gdv_int32* out_len);
const char* concatOperator_utf8_utf8(gdv_int64 context, const char* left,
                                     gdv_int32 left_len, const char* right,
                                     gdv_int32 right_len, gdv_int32* out_len);

const char* castVARCHAR_utf8_int64(gdv_int64 context, const char* data,
                                   gdv_int32 data_len, int64_t out_len,
                                   int32_t* out_length);

const char* lower_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                       int32_t* out_length);

const char* reverse_utf8(gdv_int64 context, const char* data, gdv_int32 data_len,
                         int32_t* out_len);

gdv_int32 locate_utf8_utf8(gdv_int64 context, const char* sub_str, gdv_int32 sub_str_len,
                           const char* str, gdv_int32 str_len);

gdv_int32 locate_utf8_utf8_int32(gdv_int64 context, const char* sub_str,
                                 gdv_int32 sub_str_len, const char* str,
                                 gdv_int32 str_len, gdv_int32 start_pos);

const char* replace_with_max_len_utf8_utf8_utf8(gdv_int64 context, const char* text,
                                                gdv_int32 text_len, const char* from_str,
                                                gdv_int32 from_str_len,
                                                const char* to_str, gdv_int32 to_str_len,
                                                gdv_int32 max_length, gdv_int32* out_len);

const char* replace_utf8_utf8_utf8(gdv_int64 context, const char* text,
                                   gdv_int32 text_len, const char* from_str,
                                   gdv_int32 from_str_len, const char* to_str,
                                   gdv_int32 to_str_len, gdv_int32* out_len);
}  // extern "C"

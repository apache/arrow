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

#ifndef PRECOMPILED_TYPES_H
#define PRECOMPILED_TYPES_H

#include <cstdint>
#include "gandiva/gdv_function_stubs.h"

// Use the same names as in arrow data types. Makes it easy to write pre-processor macros.
using boolean = bool;
using int8 = int8_t;
using int16 = int16_t;
using int32 = int32_t;
using int64 = int64_t;
using uint8 = uint8_t;
using uint16 = uint16_t;
using uint32 = uint32_t;
using uint64 = uint64_t;
using float32 = float;
using float64 = double;
using date64 = int64_t;
using time32 = int32_t;
using timestamp = int64_t;
using utf8 = char*;
using binary = char*;

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

int64 extractMillennium_timestamp(timestamp millis);
int64 extractCentury_timestamp(timestamp millis);
int64 extractDecade_timestamp(timestamp millis);
int64 extractYear_timestamp(timestamp millis);
int64 extractDoy_timestamp(timestamp millis);
int64 extractQuarter_timestamp(timestamp millis);
int64 extractMonth_timestamp(timestamp millis);
int64 extractWeek_timestamp(timestamp millis);
int64 extractDow_timestamp(timestamp millis);
int64 extractDay_timestamp(timestamp millis);
int64 extractHour_timestamp(timestamp millis);
int64 extractMinute_timestamp(timestamp millis);
int64 extractSecond_timestamp(timestamp millis);
int64 extractHour_time32(int32 millis_in_day);
int64 extractMinute_time32(int32 millis_in_day);
int64 extractSecond_time32(int32 millis_in_day);

int32 hash32(double val, int32 seed);
int32 hash32_buf(const uint8* buf, int len, int32 seed);
int64 hash64(double val, int64 seed);
int64 hash64_buf(const uint8* buf, int len, int64 seed);

int64 timestampaddSecond_int32_timestamp(int32, timestamp);
int64 timestampaddMinute_int32_timestamp(int32, timestamp);
int64 timestampaddHour_int32_timestamp(int32, timestamp);
int64 timestampaddDay_int32_timestamp(int32, timestamp);
int64 timestampaddWeek_int32_timestamp(int32, timestamp);
int64 timestampaddMonth_int32_timestamp(int32, timestamp);
int64 timestampaddQuarter_int32_timestamp(int32, timestamp);
int64 timestampaddYear_int32_timestamp(int32, timestamp);

int64 timestampaddSecond_int64_timestamp(int64, timestamp);
int64 timestampaddMinute_int64_timestamp(int64, timestamp);
int64 timestampaddHour_int64_timestamp(int64, timestamp);
int64 timestampaddDay_int64_timestamp(int64, timestamp);
int64 timestampaddWeek_int64_timestamp(int64, timestamp);
int64 timestampaddMonth_int64_timestamp(int64, timestamp);
int64 timestampaddQuarter_int64_timestamp(int64, timestamp);
int64 timestampaddYear_int64_timestamp(int64, timestamp);

int64 date_add_int32_timestamp(int32, timestamp);
int64 add_int64_timestamp(int64, timestamp);
int64 add_int32_timestamp(int32, timestamp);
int64 date_add_int64_timestamp(int64, timestamp);
timestamp add_date64_int64(date64, int64);

int64 date_sub_int32_timestamp(int32, timestamp);
int64 subtract_int32_timestamp(int32, timestamp);
int64 date_diff_int64_timestamp(int64, timestamp);

bool is_distinct_from_timestamp_timestamp(int64, bool, int64, bool);
bool is_not_distinct_from_int32_int32(int32, bool, int32, bool);

int64 date_trunc_Second_date64(date64);
int64 date_trunc_Minute_date64(date64);
int64 date_trunc_Hour_date64(date64);
int64 date_trunc_Day_date64(date64);
int64 date_trunc_Month_date64(date64);
int64 date_trunc_Quarter_date64(date64);
int64 date_trunc_Year_date64(date64);
int64 date_trunc_Decade_date64(date64);
int64 date_trunc_Century_date64(date64);
int64 date_trunc_Millennium_date64(date64);

int64 date_trunc_Week_timestamp(timestamp);
double months_between_timestamp_timestamp(uint64, uint64);

int32 mem_compare(const char* left, int32 left_len, const char* right, int32 right_len);

int32 mod_int64_int32(int64 left, int32 right);
float64 mod_float64_float64(int64 context, float64 left, float64 right);

int64 divide_int64_int64(int64 context, int64 in1, int64 in2);

int64 div_int64_int64(int64 context, int64 in1, int64 in2);
float32 div_float32_float32(int64 context, float32 in1, float32 in2);
float64 div_float64_float64(int64 context, float64 in1, float64 in2);

float64 cbrt_int32(int32);
float64 cbrt_int64(int64);
float64 cbrt_float32(float32);
float64 cbrt_float64(float64);

float64 exp_int32(int32);
float64 exp_int64(int64);
float64 exp_float32(float32);
float64 exp_float64(float64);

float64 log_int32(int32);
float64 log_int64(int64);
float64 log_float32(float32);
float64 log_float64(float64);

float64 log10_int32(int32);
float64 log10_int64(int64);
float64 log10_float32(float32);
float64 log10_float64(float64);

float64 power_float64_float64(float64, float64);

float64 log_int32_int32(int64 context, int32 base, int32 value);

bool starts_with_utf8_utf8(const char* data, int32 data_len, const char* prefix,
                           int32 prefix_len);
bool ends_with_utf8_utf8(const char* data, int32 data_len, const char* suffix,
                         int32 suffix_len);

int32 utf8_length(int64 context, const char* data, int32 data_len);

date64 castDATE_utf8(int64_t execution_context, const char* input, int32 length);

timestamp castTIMESTAMP_utf8(int64_t execution_context, const char* input, int32 length);
timestamp castTIMESTAMP_date64(date64);

int64 truncate_int64_int32(int64 in, int32 out_scale);

const char* substr_utf8_int64_int64(int64 context, const char* input, int32 in_len,
                                    int64 offset64, int64 length, int32* out_len);
const char* substr_utf8_int64(int64 context, const char* input, int32 in_len,
                              int64 offset64, int32* out_len);
const char* concatOperator_utf8_utf8(int64 context, const char* left, int32 left_len,
                                     const char* right, int32 right_len, int32* out_len);
}  // extern "C"

#endif  // PRECOMPILED_TYPES_H

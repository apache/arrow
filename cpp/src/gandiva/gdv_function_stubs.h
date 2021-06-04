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

#include "gandiva/visibility.h"

/// Stub functions that can be accessed from LLVM.
extern "C" {

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

bool gdv_fn_like_utf8_utf8(int64_t ptr, const char* data, int data_len,
                           const char* pattern, int pattern_len);

bool gdv_fn_like_utf8_utf8_utf8(int64_t ptr, const char* data, int data_len,
                                const char* pattern, int pattern_len,
                                const char* escape_char, int escape_char_len);

int64_t gdv_fn_to_date_utf8_utf8_int32(int64_t context, int64_t ptr, const char* data,
                                       int data_len, bool in1_validity,
                                       const char* pattern, int pattern_len,
                                       bool in2_validity, int32_t suppress_errors,
                                       bool in3_validity, bool* out_valid);

void gdv_fn_context_set_error_msg(int64_t context_ptr, const char* err_msg);

uint8_t* gdv_fn_context_arena_malloc(int64_t context_ptr, int32_t data_len);

void gdv_fn_context_arena_reset(int64_t context_ptr);

bool in_expr_lookup_int32(int64_t ptr, int32_t value, bool in_validity);

bool in_expr_lookup_int64(int64_t ptr, int64_t value, bool in_validity);

bool in_expr_lookup_utf8(int64_t ptr, const char* data, int data_len, bool in_validity);

int gdv_fn_time_with_zone(int* time_fields, const char* zone, int zone_len,
                          int64_t* ret_time);

GANDIVA_EXPORT
const char* gdv_fn_sha256_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                     int32_t x_precision, int32_t x_scale,
                                     gdv_boolean x_isvalid, int32_t* out_length);

GANDIVA_EXPORT
const char* gdv_fn_sha1_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                   int32_t x_precision, int32_t x_scale,
                                   gdv_boolean x_isvalid, int32_t* out_length);

int32_t gdv_fn_dec_from_string(int64_t context, const char* in, int32_t in_length,
                               int32_t* precision_from_str, int32_t* scale_from_str,
                               int64_t* dec_high_from_str, uint64_t* dec_low_from_str);

char* gdv_fn_dec_to_string(int64_t context, int64_t x_high, uint64_t x_low,
                           int32_t x_scale, int32_t* dec_str_len);

GANDIVA_EXPORT
int32_t gdv_fn_castINT_utf8(int64_t context, const char* data, int32_t data_len);

GANDIVA_EXPORT
int64_t gdv_fn_castBIGINT_utf8(int64_t context, const char* data, int32_t data_len);

GANDIVA_EXPORT
float gdv_fn_castFLOAT4_utf8(int64_t context, const char* data, int32_t data_len);

GANDIVA_EXPORT
double gdv_fn_castFLOAT8_utf8(int64_t context, const char* data, int32_t data_len);

GANDIVA_EXPORT
const char* gdv_fn_castVARCHAR_int32_int64(int64_t context, int32_t value, int64_t len,
                                           int32_t* out_len);
GANDIVA_EXPORT
const char* gdv_fn_castVARCHAR_int64_int64(int64_t context, int64_t value, int64_t len,
                                           int32_t* out_len);
GANDIVA_EXPORT
const char* gdv_fn_castVARCHAR_float32_int64(int64_t context, float value, int64_t len,
                                             int32_t* out_len);
GANDIVA_EXPORT
const char* gdv_fn_castVARCHAR_float64_int64(int64_t context, double value, int64_t len,
                                             int32_t* out_len);

GANDIVA_EXPORT
int32_t gdv_fn_utf8_char_length(char c);

GANDIVA_EXPORT
void gdv_fn_set_error_for_invalid_utf8(int64_t execution_context, char val);

GANDIVA_EXPORT
const char* gdv_fn_upper_utf8(int64_t context, const char* data, int32_t data_len,
                              int32_t* out_len);

GANDIVA_EXPORT
const char* gdv_fn_lower_utf8(int64_t context, const char* data, int32_t data_len,
                              int32_t* out_len);

GANDIVA_EXPORT
bool gdv_fn_is_codepoint_for_space(uint32_t val);

GANDIVA_EXPORT
const char* gdv_fn_initcap_utf8(int64_t context, const char* data, int32_t data_len,
                                int32_t* out_len);
}

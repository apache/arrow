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

/// Stub functions to deal with extra large decimals that can be accessed from LLVM-IR
/// code.
extern "C" {

void gdv_xlarge_multiply_and_scale_down(int64_t x_high, uint64_t x_low, int64_t y_high,
                                        uint64_t y_low, int32_t reduce_scale_by,
                                        int64_t* out_high, uint64_t* out_low,
                                        bool* overflow);

void gdv_xlarge_scale_up_and_divide(int64_t x_high, uint64_t x_low, int64_t y_high,
                                    uint64_t y_low, int32_t increase_scale_by,
                                    int64_t* out_high, uint64_t* out_low, bool* overflow);

void gdv_xlarge_mod(int64_t x_high, uint64_t x_low, int32_t x_scale, int64_t y_high,
                    uint64_t y_low, int32_t y_scale, int64_t* out_high,
                    uint64_t* out_low);

int32_t gdv_xlarge_compare(int64_t x_high, uint64_t x_low, int32_t x_scale,
                           int64_t y_high, uint64_t y_low, int32_t y_scale);
}

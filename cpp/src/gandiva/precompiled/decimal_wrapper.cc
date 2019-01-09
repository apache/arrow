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
  gandiva::Decimal128Full x(x_high, x_low, x_precision, x_scale);
  gandiva::Decimal128Full y(y_high, y_low, y_precision, y_scale);

  arrow::Decimal128 out = gandiva::decimalops::Add(x, y, out_precision, out_scale);
  *out_high = out.high_bits();
  *out_low = out.low_bits();
}

}  // extern "C"

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

#include "arrow/buffer.h"

namespace arrow {
namespace compute {
namespace internal {

// An internal data structure for unpacking a primitive argument to pass to a
// kernel implementation
struct PrimitiveArg {
  const uint8_t* is_valid;
  // If the bit_width is a multiple of 8 (i.e. not boolean), then "data" should
  // be shifted by offset * (bit_width / 8). For bit-packed data, the offset
  // must be used when indexing.
  const uint8_t* data;
  int bit_width;
  int64_t length;
  int64_t offset;
  // This may be kUnknownNullCount if the null_count has not yet been computed,
  // so use null_count != 0 to determine "may have nulls".
  int64_t null_count;
};

// Get validity bitmap data or return nullptr if there is no validity buffer
const uint8_t* GetValidityBitmap(const ArrayData& data);

int GetBitWidth(const DataType& type);

// Reduce code size by dealing with the unboxing of the kernel inputs once
// rather than duplicating compiled code to do all these in each kernel.
PrimitiveArg GetPrimitiveArg(const ArrayData& arr);

}  // namespace internal
}  // namespace compute
}  // namespace arrow

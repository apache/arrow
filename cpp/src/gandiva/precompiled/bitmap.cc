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

// BitMap functions

#include "arrow/util/bit-util.h"

extern "C" {

#include "./types.h"

#define BITS_TO_BYTES(x) ((x + 7) / 8)
#define BITS_TO_WORDS(x) ((x + 63) / 64)

#define POS_TO_BYTE_INDEX(p) (p / 8)
#define POS_TO_BIT_INDEX(p) (p % 8)

FORCE_INLINE
bool bitMapGetBit(const uint8_t* bmap, int64_t position) {
  return arrow::BitUtil::GetBit(bmap, position);
}

FORCE_INLINE
void bitMapSetBit(uint8_t* bmap, int64_t position, bool value) {
  arrow::BitUtil::SetBitTo(bmap, position, value);
}

// Clear the bit if value = false. Does nothing if value = true.
FORCE_INLINE
void bitMapClearBitIfFalse(uint8_t* bmap, int64_t position, bool value) {
  if (!value) {
    arrow::BitUtil::ClearBit(bmap, position);
  }
}

}  // extern "C"

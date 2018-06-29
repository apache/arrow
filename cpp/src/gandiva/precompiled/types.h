// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef PRECOMPILED_TYPES_H
#define PRECOMPILED_TYPES_H

#include <stdint.h>

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
using time64 = int64_t;
using timestamp = int64_t;

#ifdef GANDIVA_UNIT_TEST
// unit tests may be compiled without O2, so inlining may not happen.
#define FORCE_INLINE
#else
#define FORCE_INLINE __attribute__((always_inline))
#endif

// Declarations : used in testing

extern "C" {

bool bitMapGetBit(const unsigned char *bmap, int position);
void bitMapSetBit(unsigned char *bmap, int position, bool value);
void bitMapClearBitIfFalse(unsigned char *bmap, int position, bool value);

int64 extractYear_timestamp(timestamp millis);
int64 extractMonth_timestamp(timestamp millis);
int64 extractDay_timestamp(timestamp millis);
int64 extractHour_timestamp(timestamp millis);
int64 extractMinute_timestamp(timestamp millis);

} // extern "C"

#endif //PRECOMPILED_TYPES_H

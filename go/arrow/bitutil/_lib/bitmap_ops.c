// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "../../../internal/utils/_lib/arch.h"
#include <stdint.h>

// like elsewhere in this repo, this .c file gets compiled into optimized
// assembly and then converted to go plan9 assembly via c2goasm so we can
// call these functions. see the Makefile in the parent directory.

void FULL_NAME(bitmap_aligned_and)(const uint8_t* left, const uint8_t* right, uint8_t* out, const int64_t nbytes) {
    for (int64_t i = 0; i < nbytes; ++i) {
        out[i] = left[i] & right[i];
    }
}

void FULL_NAME(bitmap_aligned_or)(const uint8_t* left, const uint8_t* right, uint8_t* out, const int64_t nbytes) {
    for (int64_t i = 0; i < nbytes; ++i) {
        out[i] = left[i] | right[i];
    }
}

void FULL_NAME(bitmap_aligned_and_not)(const uint8_t* left, const uint8_t* right, uint8_t* out, const int64_t nbytes) {
    for (int64_t i = 0; i < nbytes; ++i) {
        out[i] = left[i] & ~right[i];
    }
}

void FULL_NAME(bitmap_aligned_xor)(const uint8_t* left, const uint8_t* right, uint8_t* out, const int64_t nbytes) {
    for (int64_t i = 0; i < nbytes; ++i) {
        out[i] = left[i] ^ right[i];
    }
}

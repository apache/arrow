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

#include <arch.h>
#include <stdint.h>

#define CREATE_TRANSPOSE(SRC, DEST) \
    void FULL_NAME(transpose_ ## SRC ## _ ## DEST)(const SRC ## _t* src, DEST ## _t* dest, int length, const int32_t* transpose_map) { \
        while (length >= 4) {                                       \
            dest[0] = (DEST ## _t)(transpose_map[src[0]]);          \
            dest[1] = (DEST ## _t)(transpose_map[src[1]]);          \
            dest[2] = (DEST ## _t)(transpose_map[src[2]]);          \
            dest[3] = (DEST ## _t)(transpose_map[src[3]]);          \
            length -= 4;                                            \
            src += 4;                                               \
            dest += 4;                                              \
        }                                                           \
        while (length > 0) {                                        \
            *dest++ = (DEST ## _t)(transpose_map[*src++]);          \
            --length;                                               \
        }                                                           \
    }

#define CREATE_TRANSPOSE_ALL_DEST(DEST) \
    CREATE_TRANSPOSE(uint8, DEST)     \
    CREATE_TRANSPOSE(int8, DEST)      \
    CREATE_TRANSPOSE(uint16, DEST)    \
    CREATE_TRANSPOSE(int16, DEST)     \
    CREATE_TRANSPOSE(uint32, DEST)    \
    CREATE_TRANSPOSE(int32, DEST)     \
    CREATE_TRANSPOSE(uint64, DEST)    \
    CREATE_TRANSPOSE(int64, DEST)

#define CREATE_TRANSPOSE_ALL()        \
    CREATE_TRANSPOSE_ALL_DEST(uint8)  \
    CREATE_TRANSPOSE_ALL_DEST(int8)   \
    CREATE_TRANSPOSE_ALL_DEST(uint16) \
    CREATE_TRANSPOSE_ALL_DEST(int16)  \
    CREATE_TRANSPOSE_ALL_DEST(uint32) \
    CREATE_TRANSPOSE_ALL_DEST(int32)  \
    CREATE_TRANSPOSE_ALL_DEST(uint64) \
    CREATE_TRANSPOSE_ALL_DEST(int64)

CREATE_TRANSPOSE_ALL()

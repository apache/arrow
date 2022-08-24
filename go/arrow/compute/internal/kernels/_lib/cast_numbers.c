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

#define CREATE_CAST(SRC, SRCNAME, DEST, DESTNAME) \
    void FULL_NAME(cast_numeric_ ## SRCNAME ## _ ## DESTNAME)(const SRC* in, DEST* out, const int len) { \
        for (int i = 0; i < len; ++i) { \
            out[i] = (DEST)(in[i]);     \
        }                               \
    }

#define CREATE_CAST_ALL_DEST(DEST, DESTNAME)  \
    CREATE_CAST(uint8_t, uint8, DEST, DESTNAME)     \
    CREATE_CAST(int8_t, int8, DEST, DESTNAME)       \
    CREATE_CAST(uint16_t, uint16, DEST, DESTNAME)   \
    CREATE_CAST(int16_t, int16, DEST, DESTNAME)     \
    CREATE_CAST(uint32_t, uint32, DEST, DESTNAME)   \
    CREATE_CAST(int32_t, int32, DEST, DESTNAME)     \
    CREATE_CAST(uint64_t, uint64, DEST, DESTNAME)   \
    CREATE_CAST(int64_t, int64, DEST, DESTNAME)     \
    CREATE_CAST(float, float32, DEST, DESTNAME)     \
    CREATE_CAST(double, float64, DEST, DESTNAME)

#define CREATE_CAST_ALL()       \
    CREATE_CAST_ALL_DEST(uint8_t, uint8)    \
    CREATE_CAST_ALL_DEST(int8_t, int8)      \
    CREATE_CAST_ALL_DEST(uint16_t, uint16)  \
    CREATE_CAST_ALL_DEST(int16_t, int16)    \
    CREATE_CAST_ALL_DEST(uint32_t, uint32)  \
    CREATE_CAST_ALL_DEST(int32_t, int32)    \
    CREATE_CAST_ALL_DEST(uint64_t, uint64)  \
    CREATE_CAST_ALL_DEST(int64_t, int64)    \
    CREATE_CAST_ALL_DEST(float, float32)    \
    CREATE_CAST_ALL_DEST(double, float64)

CREATE_CAST_ALL()
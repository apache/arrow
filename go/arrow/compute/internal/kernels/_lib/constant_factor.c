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

#define CREATE_CONSTANT_FACTOR(SRC, DEST) \
    void FULL_NAME(multiply_constant_##SRC##_##DEST)(const SRC##_t* src, DEST##_t* dest, const int len, const int64_t factor) { \
        for (int i = 0; i < len; ++i) {            \
            dest[i] = (DEST##_t)(src[i] * factor); \
        }                                          \
    }                                              \
    void FULL_NAME(divide_constant_##SRC##_##DEST)(const SRC##_t* src, DEST##_t* dest, const int len, const int64_t factor) { \
        for (int i = 0; i < len; ++i) {            \
            dest[i] = (DEST##_t)(src[i] / factor); \
        }                                          \
    }

CREATE_CONSTANT_FACTOR(int32, int32)
CREATE_CONSTANT_FACTOR(int32, int64)
CREATE_CONSTANT_FACTOR(int64, int32)
CREATE_CONSTANT_FACTOR(int64, int64)
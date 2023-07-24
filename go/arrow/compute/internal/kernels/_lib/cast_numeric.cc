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
#include "types.h"

template <typename I, typename O>
static inline void FULL_NAME(cast_tmpl_numeric)(const I* in, O* out, const int len) {
    for (int i = 0; i < len; ++i) {
        out[i] = static_cast<O>(in[i]);
    }
}

template <typename I>
static inline void FULL_NAME(cast_type_numeric_impl)(const arrtype otype, const I* in, void* out, const int len) {
    switch (otype) {
    case arrtype::UINT8:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<uint8_t*>(out), len);
        break;
    case arrtype::INT8:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<int8_t*>(out), len);
        break;
    case arrtype::UINT16:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<uint16_t*>(out), len);
        break;
    case arrtype::INT16:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<int16_t*>(out), len);
        break;
    case arrtype::UINT32:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<uint32_t*>(out), len);
        break;
    case arrtype::INT32:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<int32_t*>(out), len);
        break;
    case arrtype::UINT64:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<uint64_t*>(out), len);
        break;
    case arrtype::INT64:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<int64_t*>(out), len);
        break;
    case arrtype::FLOAT32:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<float*>(out), len);
        break;
    case arrtype::FLOAT64:
        FULL_NAME(cast_tmpl_numeric)(in, reinterpret_cast<double*>(out), len);
        break;
    default:
        break;
    }
}

extern "C" void FULL_NAME(cast_type_numeric)(const int itype, const int otype, const void* input, void* output, const int len) {
    const auto in = static_cast<arrtype>(itype);
    const auto out = static_cast<arrtype>(otype);

    switch (in) {    
    case arrtype::UINT8:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const uint8_t*>(input), output, len);
        break;
    case arrtype::INT8:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const int8_t*>(input), output, len);
        break;
    case arrtype::UINT16:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const uint16_t*>(input), output, len);
        break;    
    case arrtype::INT16:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const int16_t*>(input), output, len);
        break;    
    case arrtype::UINT32:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const uint32_t*>(input), output, len);
        break;
    case arrtype::INT32:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const int32_t*>(input), output, len);
        break;    
    case arrtype::UINT64:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const uint64_t*>(input), output, len);
        break;    
    case arrtype::INT64:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const int64_t*>(input), output, len);
        break;    
    case arrtype::FLOAT32:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const float*>(input), output, len);
        break;    
    case arrtype::FLOAT64:
        FULL_NAME(cast_type_numeric_impl)(out, reinterpret_cast<const double*>(input), output, len);
        break;    
    default:
        break;
    }
}
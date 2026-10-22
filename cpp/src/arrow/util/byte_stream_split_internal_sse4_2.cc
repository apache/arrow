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

#include "arrow/util/byte_stream_split_internal.h"
#include "arrow/util/math_internal.h"
#include "arrow/util/simd.h"

#include <xsimd/types/xsimd_sse4_2_register.hpp>
#include <xsimd/xsimd.hpp>

#include <cstdint>

namespace arrow::util::internal {

template void ByteStreamSplitDecodeSimd<xsimd::sse4_2, 2>(const uint8_t*, int, int64_t,
                                                          int64_t, uint8_t*);
template void ByteStreamSplitDecodeSimd<xsimd::sse4_2, 4>(const uint8_t*, int, int64_t,
                                                          int64_t, uint8_t*);
template void ByteStreamSplitDecodeSimd<xsimd::sse4_2, 8>(const uint8_t*, int, int64_t,
                                                          int64_t, uint8_t*);

template void ByteStreamSplitEncodeSimd<xsimd::sse4_2, 2>(const uint8_t*, int,
                                                          const int64_t, uint8_t*);
template void ByteStreamSplitEncodeSimd<xsimd::sse4_2, 4>(const uint8_t*, int,
                                                          const int64_t, uint8_t*);
template void ByteStreamSplitEncodeSimd<xsimd::sse4_2, 8>(const uint8_t*, int,
                                                          const int64_t, uint8_t*);

}  // namespace arrow::util::internal

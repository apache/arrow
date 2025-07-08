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
#include "arrow/util/dispatch_internal.h"

#include <array>

namespace arrow::util::internal {

using ::arrow::internal::DispatchLevel;
using ::arrow::internal::DynamicDispatch;

/************************
 *  Decode dispatching  *
 ************************/

template <int kNumStreams>
struct ByteStreamSplitDecodeDynamic {
  using FunctionType = decltype(&ByteStreamSplitDecodeScalar<kNumStreams>);
  using Implementation = std::pair<DispatchLevel, FunctionType>;

  constexpr static auto implementations() {
    return std::array {
      Implementation {
        DispatchLevel::NONE,
#if defined(ARROW_HAVE_NEON)
            // We always expect Neon to be available on Arm64
            &ByteStreamSplitDecodeSimd<xsimd::neon64, kNumStreams>,
#elif defined(ARROW_HAVE_SSE4_2)
            // We always expect SSE4.2 to be available on x86_64
            &ByteStreamSplitDecodeSimd<xsimd::sse4_2, kNumStreams>,
#else
            &ByteStreamSplitDecodeScalar<kNumStreams>,
#endif
      }
      ,
#if defined(ARROW_HAVE_RUNTIME_AVX2)
          Implementation{
              DispatchLevel::AVX2,
              &ByteStreamSplitDecodeSimd<xsimd::avx2, kNumStreams>,
          },
#endif
    };
  }
};

template <int kNumStreams>
void ByteStreamSplitDecodeSimdDispatch(const uint8_t* data, int width, int64_t num_values,
                                       int64_t stride, uint8_t* out) {
  static const DynamicDispatch<ByteStreamSplitDecodeDynamic<kNumStreams>> dispatch;
  return dispatch.func(data, width, num_values, stride, out);
}

template void ByteStreamSplitDecodeSimdDispatch<2>(const uint8_t*, int, int64_t, int64_t,
                                                   uint8_t*);
template void ByteStreamSplitDecodeSimdDispatch<4>(const uint8_t*, int, int64_t, int64_t,
                                                   uint8_t*);
template void ByteStreamSplitDecodeSimdDispatch<8>(const uint8_t*, int, int64_t, int64_t,
                                                   uint8_t*);

/************************
 *  Encode dispatching  *
 ************************/

template <int kNumStreams>
struct ByteStreamSplitEncodeDynamic {
  using FunctionType = decltype(&ByteStreamSplitEncodeScalar<kNumStreams>);
  using Implementation = std::pair<DispatchLevel, FunctionType>;

  constexpr static auto implementations() {
    return std::array {
      Implementation {
        DispatchLevel::NONE,
#if defined(ARROW_HAVE_NEON)
            // We always expect Neon to be available on Arm64
            &ByteStreamSplitEncodeSimd<xsimd::neon64, kNumStreams>,
#elif defined(ARROW_HAVE_SSE4_2)
            // We always expect SSE4.2 to be available on x86_64
            &ByteStreamSplitEncodeSimd<xsimd::sse4_2, kNumStreams>,
#else
            &ByteStreamSplitEncodeScalar<kNumStreams>,
#endif
      }
      ,
#if defined(ARROW_HAVE_RUNTIME_AVX2)
          Implementation{DispatchLevel::AVX2, &ByteStreamSplitEncodeAvx2<kNumStreams>},
#endif
    };
  }
};

template <int kNumStreams>
void ByteStreamSplitEncodeSimdDispatch(const uint8_t* raw_values, int width,
                                       const int64_t num_values,
                                       uint8_t* output_buffer_raw) {
  static const DynamicDispatch<ByteStreamSplitEncodeDynamic<kNumStreams>> dispatch;
  return dispatch.func(raw_values, width, num_values, output_buffer_raw);
}

template void ByteStreamSplitEncodeSimdDispatch<2>(const uint8_t*, int, const int64_t,
                                                   uint8_t*);
template void ByteStreamSplitEncodeSimdDispatch<4>(const uint8_t*, int, const int64_t,
                                                   uint8_t*);
template void ByteStreamSplitEncodeSimdDispatch<8>(const uint8_t*, int, const int64_t,
                                                   uint8_t*);

}  // namespace arrow::util::internal

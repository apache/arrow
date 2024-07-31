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

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/byte_stream_split_internal.h"

namespace arrow::util::internal {

using ByteStreamSplitTypes =
    ::testing::Types<int8_t, int16_t, int32_t, int64_t, std::array<uint8_t, 3>>;

template <typename Func>
struct NamedFunc {
  std::string name;
  Func func;

  friend std::ostream& operator<<(std::ostream& os, const NamedFunc& func) {
    os << func.name;
    return os;
  }
};

// A simplistic reference implementation for validation
void ReferenceByteStreamSplitEncode(const uint8_t* src, int width,
                                    const int64_t num_values, uint8_t* dest) {
  for (int64_t i = 0; i < num_values; ++i) {
    for (int stream = 0; stream < width; ++stream) {
      dest[stream * num_values + i] = *src++;
    }
  }
}

template <typename T>
class TestByteStreamSplitSpecialized : public ::testing::Test {
 public:
  static constexpr int kWidth = static_cast<int>(sizeof(T));

  using EncodeFunc = NamedFunc<std::function<decltype(ByteStreamSplitEncode)>>;
  using DecodeFunc = NamedFunc<std::function<decltype(ByteStreamSplitDecode)>>;

  void SetUp() override {
    decode_funcs_ = MakeDecodeFuncs();
    encode_funcs_ = MakeEncodeFuncs();
  }

  void TestRoundtrip(int64_t num_values) {
    // Test one-shot roundtrip among all encode/decode function combinations
    ARROW_SCOPED_TRACE("num_values = ", num_values);
    const auto input = MakeRandomInput(num_values);
    std::vector<uint8_t> encoded(num_values * kWidth);
    std::vector<T> decoded(num_values);

    for (const auto& encode_func : encode_funcs_) {
      ARROW_SCOPED_TRACE("encode_func = ", encode_func);
      encoded.assign(encoded.size(), 0);
      encode_func.func(reinterpret_cast<const uint8_t*>(input.data()), kWidth, num_values,
                       encoded.data());
      for (const auto& decode_func : decode_funcs_) {
        ARROW_SCOPED_TRACE("decode_func = ", decode_func);
        decoded.assign(decoded.size(), T{});
        decode_func.func(encoded.data(), kWidth, num_values, /*stride=*/num_values,
                         reinterpret_cast<uint8_t*>(decoded.data()));
        ASSERT_EQ(decoded, input);
      }
    }
  }

  void TestPiecewiseDecode(int64_t num_values) {
    // Test chunked decoding against the reference encode function
    ARROW_SCOPED_TRACE("num_values = ", num_values);
    const auto input = MakeRandomInput(num_values);
    std::vector<uint8_t> encoded(num_values * kWidth);
    ReferenceEncode(reinterpret_cast<const uint8_t*>(input.data()), num_values,
                    encoded.data());
    std::vector<T> decoded(num_values);

    std::default_random_engine gen(seed_++);
    std::uniform_int_distribution<int64_t> chunk_size_dist(1, 123);

    for (const auto& decode_func : decode_funcs_) {
      ARROW_SCOPED_TRACE("decode_func = ", decode_func);
      decoded.assign(decoded.size(), T{});

      int64_t offset = 0;
      while (offset < num_values) {
        auto chunk_size = std::min<int64_t>(num_values - offset, chunk_size_dist(gen));
        decode_func.func(encoded.data() + offset, kWidth, chunk_size,
                         /*stride=*/num_values,
                         reinterpret_cast<uint8_t*>(decoded.data() + offset));
        offset += chunk_size;
      }
      ASSERT_EQ(offset, num_values);
      ASSERT_EQ(decoded, input);
    }
  }

 protected:
  static void ReferenceEncode(const uint8_t* raw_values, const int64_t num_values,
                              uint8_t* output_buffer_raw) {
    ReferenceByteStreamSplitEncode(raw_values, kWidth, num_values, output_buffer_raw);
  }

  static std::vector<T> MakeRandomInput(int64_t num_values) {
    std::vector<T> input(num_values);
    random_bytes(kWidth * num_values, seed_++, reinterpret_cast<uint8_t*>(input.data()));
    return input;
  }

  template <bool kSimdImplemented = (kWidth == 4 || kWidth == 8)>
  static std::vector<DecodeFunc> MakeDecodeFuncs() {
    std::vector<DecodeFunc> funcs;
    funcs.push_back({"scalar_dynamic", &ByteStreamSplitDecodeScalarDynamic});
    funcs.push_back({"scalar", &ByteStreamSplitDecodeScalar<kWidth>});
#if defined(ARROW_HAVE_SIMD_SPLIT)
    if constexpr (kSimdImplemented) {
      funcs.push_back({"simd", &ByteStreamSplitDecodeSimd<kWidth>});
      funcs.push_back({"simd128", &ByteStreamSplitDecodeSimd128<kWidth>});
#if defined(ARROW_HAVE_AVX2)
      funcs.push_back({"avx2", &ByteStreamSplitDecodeAvx2<kWidth>});
#endif
    }
#endif  // defined(ARROW_HAVE_SIMD_SPLIT)
    return funcs;
  }

  template <bool kSimdImplemented = (kWidth == 4 || kWidth == 8)>
  static std::vector<EncodeFunc> MakeEncodeFuncs() {
    std::vector<EncodeFunc> funcs;
    funcs.push_back({"reference", &ReferenceByteStreamSplitEncode});
    funcs.push_back({"scalar_dynamic", &ByteStreamSplitEncodeScalarDynamic});
    funcs.push_back({"scalar", &ByteStreamSplitEncodeScalar<kWidth>});
#if defined(ARROW_HAVE_SIMD_SPLIT)
    if constexpr (kSimdImplemented) {
      funcs.push_back({"simd", &ByteStreamSplitEncodeSimd<kWidth>});
      funcs.push_back({"simd128", &ByteStreamSplitEncodeSimd128<kWidth>});
#if defined(ARROW_HAVE_AVX2)
      funcs.push_back({"avx2", &ByteStreamSplitEncodeAvx2<kWidth>});
#endif
    }
#endif  // defined(ARROW_HAVE_SIMD_SPLIT)
    return funcs;
  }

  std::vector<EncodeFunc> encode_funcs_;
  std::vector<DecodeFunc> decode_funcs_;

  static inline uint32_t seed_ = 42;
};

TYPED_TEST_SUITE(TestByteStreamSplitSpecialized, ByteStreamSplitTypes);

TYPED_TEST(TestByteStreamSplitSpecialized, RoundtripSmall) {
  for (int64_t num_values : {1, 5, 7, 12, 19, 31, 32}) {
    this->TestRoundtrip(num_values);
  }
}

TYPED_TEST(TestByteStreamSplitSpecialized, RoundtripMidsized) {
  for (int64_t num_values : {126, 127, 128, 129, 133, 200}) {
    this->TestRoundtrip(num_values);
  }
}

TYPED_TEST(TestByteStreamSplitSpecialized, PiecewiseDecode) {
  this->TestPiecewiseDecode(/*num_values=*/500);
}

}  // namespace arrow::util::internal

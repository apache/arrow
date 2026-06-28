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

#include "arrow/util/fastlanes/fastlanes_for.h"

#include <cstdint>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/util/fastlanes/fastlanes_kernels.h"

namespace arrow {
namespace util {
namespace fastlanes {

// Round-trip checker. Decoder output is in transposed order: per
// 1024-block, output[t] should equal input[fromTransposed32(t)].
void CheckRoundTrip(const std::vector<int32_t>& input) {
  ASSERT_EQ(input.size() % FastLanesForCodec::kChunkSize, 0u);
  std::vector<uint8_t> encoded(FastLanesForCodec::MaxEncodedSize(input.size()));
  auto encoded_size = FastLanesForCodec::Encode(input.data(), input.size(),
                                                 encoded.data());
  ASSERT_TRUE(encoded_size.ok());

  std::vector<int32_t> decoded(input.size(), 0xdeadbeef);
  auto status = FastLanesForCodec::Decode(decoded.data(), input.size(),
                                           encoded.data(), *encoded_size);
  ASSERT_TRUE(status.ok()) << status.ToString();

  const int64_t num_chunks =
      static_cast<int64_t>(input.size()) / FastLanesForCodec::kChunkSize;
  for (int64_t chunk = 0; chunk < num_chunks; ++chunk) {
    for (int64_t block = 0; block < 2; ++block) {
      const int64_t base = chunk * FastLanesForCodec::kChunkSize + block * 1024;
      for (size_t t = 0; t < 1024; ++t) {
        ASSERT_EQ(decoded[base + t], input[base + fromTransposed32(t)])
            << "chunk=" << chunk << " block=" << block << " t=" << t;
      }
    }
  }
}

TEST(FastLanesForRoundTrip, NarrowRange) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int32_t> dist(1000, 1500);
  std::vector<int32_t> in(2048);
  for (auto& v : in) v = dist(rng);
  CheckRoundTrip(in);
}

TEST(FastLanesForRoundTrip, SingleValue) {
  std::vector<int32_t> in(2048, 12345);  // bit_width = 0 path
  CheckRoundTrip(in);
}

TEST(FastLanesForRoundTrip, FullRange) {
  std::mt19937 rng(7);
  std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int32_t>::min(),
                                              std::numeric_limits<int32_t>::max());
  std::vector<int32_t> in(2048);
  for (auto& v : in) v = dist(rng);
  CheckRoundTrip(in);
}

TEST(FastLanesForRoundTrip, MultipleChunks) {
  std::mt19937 rng(99);
  std::uniform_int_distribution<int32_t> dist(-100, 100);
  std::vector<int32_t> in(8 * 2048);  // 8 chunks
  for (auto& v : in) v = dist(rng);
  CheckRoundTrip(in);
}

TEST(FastLanesForRoundTrip, BoundaryValues) {
  for (int32_t mn : {-1000, 0, 1000}) {
    for (int32_t span : {0, 1, 127, 128, 65535, 1 << 20}) {
      std::vector<int32_t> in(2048);
      std::mt19937 rng(static_cast<unsigned>(mn + span));
      std::uniform_int_distribution<int32_t> dist(mn, mn + span);
      for (auto& v : in) v = dist(rng);
      SCOPED_TRACE(testing::Message() << "min=" << mn << " span=" << span);
      CheckRoundTrip(in);
    }
  }
}

// DecodeFlat produces output in ORIGINAL input order (output[i] == input[i]).
// Confirms the FL_ORDER scatter on decode inverts the FL_ORDER gather on
// encode.
TEST(FastLanesForRoundTrip, DecodeFlatIsIdentity) {
  std::mt19937 rng(12345);
  std::uniform_int_distribution<int32_t> dist(-1000, 1000);
  std::vector<int32_t> in(4 * 2048);
  for (auto& v : in) v = dist(rng);

  std::vector<uint8_t> encoded(FastLanesForCodec::MaxEncodedSize(in.size()));
  auto encoded_size = FastLanesForCodec::Encode(in.data(), in.size(),
                                                 encoded.data());
  ASSERT_TRUE(encoded_size.ok());

  std::vector<int32_t> decoded(in.size(), 0xdeadbeef);
  auto status = FastLanesForCodec::DecodeFlat(decoded.data(), in.size(),
                                               encoded.data(), *encoded_size);
  ASSERT_TRUE(status.ok()) << status.ToString();

  for (size_t i = 0; i < in.size(); ++i) {
    ASSERT_EQ(decoded[i], in[i]) << "i=" << i;
  }
}

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow

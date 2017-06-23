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

#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/test-common.h"
#include "arrow/util/compression.h"

using std::string;
using std::vector;

namespace arrow {

template <typename T>
void CheckCodecRoundtrip(const vector<uint8_t>& data) {
  // create multiple compressors to try to break them
  T c1;
  T c2;

  int max_compressed_len = static_cast<int>(c1.MaxCompressedLen(data.size(), &data[0]));
  std::vector<uint8_t> compressed(max_compressed_len);
  std::vector<uint8_t> decompressed(data.size());

  // compress with c1
  int64_t actual_size;
  ASSERT_OK(c1.Compress(
      data.size(), &data[0], max_compressed_len, &compressed[0], &actual_size));
  compressed.resize(actual_size);

  // decompress with c2
  ASSERT_OK(c2.Decompress(
      compressed.size(), &compressed[0], decompressed.size(), &decompressed[0]));

  ASSERT_EQ(data, decompressed);

  // compress with c2
  int64_t actual_size2;
  ASSERT_OK(c2.Compress(
      data.size(), &data[0], max_compressed_len, &compressed[0], &actual_size2));
  ASSERT_EQ(actual_size2, actual_size);

  // decompress with c1
  ASSERT_OK(c1.Decompress(
      compressed.size(), &compressed[0], decompressed.size(), &decompressed[0]));

  ASSERT_EQ(data, decompressed);
}

template <typename T>
void CheckCodec() {
  int sizes[] = {10000, 100000};
  for (int data_size : sizes) {
    vector<uint8_t> data(data_size);
    test::random_bytes(data_size, 1234, data.data());
    CheckCodecRoundtrip<T>(data);
  }
}

TEST(TestCompressors, Snappy) {
  CheckCodec<SnappyCodec>();
}

TEST(TestCompressors, Brotli) {
  CheckCodec<BrotliCodec>();
}

TEST(TestCompressors, GZip) {
  CheckCodec<GZipCodec>();
}

}  // namespace arrow

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

#include <gtest/gtest.h>
#include <cstdint>
#include <string>
#include <vector>

#include "parquet/compression/codec.h"
#include "parquet/util/test-common.h"

using std::string;
using std::vector;

namespace parquet {

template <typename T>
void CheckCodecRoundtrip(const vector<uint8_t>& data) {
  // create multiple compressors to try to break them
  T c1;
  T c2;

  int max_compressed_len = c1.MaxCompressedLen(data.size(), &data[0]);
  std::vector<uint8_t> compressed(max_compressed_len);
  std::vector<uint8_t> decompressed(data.size());

  // compress with c1
  int actual_size = c1.Compress(data.size(), &data[0], max_compressed_len,
      &compressed[0]);
  compressed.resize(actual_size);

  // decompress with c2
  c2.Decompress(compressed.size(), &compressed[0],
      decompressed.size(), &decompressed[0]);

  ASSERT_TRUE(test::vector_equal(data, decompressed));

  // compress with c2
  int actual_size2 = c2.Compress(data.size(), &data[0], max_compressed_len,
      &compressed[0]);
  ASSERT_EQ(actual_size2, actual_size);

  // decompress with c1
  c1.Decompress(compressed.size(), &compressed[0],
      decompressed.size(), &decompressed[0]);

  ASSERT_TRUE(test::vector_equal(data, decompressed));
}

template <typename T>
void CheckCodec() {
  int sizes[] = {10000, 100000};
  for (int data_size : sizes) {
    vector<uint8_t> data;
    test::random_bytes(data_size, 1234, &data);
    CheckCodecRoundtrip<T>(data);
  }
}

TEST(TestCompressors, Snappy) {
  CheckCodec<SnappyCodec>();
}

TEST(TestCompressors, Lz4) {
  CheckCodec<Lz4Codec>();
}

TEST(TestCompressors, GZip) {
  CheckCodec<GZipCodec>();
}

} // namespace parquet

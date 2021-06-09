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

#include "arrow/util/bitmap.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include <numeric>
#include <random>

#include "arrow/buffer.h"

namespace arrow {
namespace internal {

void random_bool_vector(std::vector<bool>& vec, int64_t size, double p = 0.5) {
  vec.reserve(size);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::bernoulli_distribution d(p);

  for (int n = 0; n < size; ++n) {
    vec.push_back(d(gen));
  }
}

void VerifyBoolOutput(const Bitmap& bitmap, const std::vector<bool>& expected) {
  arrow::BooleanBuilder boolean_builder;
  ASSERT_OK(boolean_builder.AppendValues(expected));
  ASSERT_OK_AND_ASSIGN(auto arr, boolean_builder.Finish());

  ASSERT_TRUE(BitmapEquals(bitmap.buffer()->data(), bitmap.offset(),
                           arr->data()->buffers[1]->data(), 0, expected.size()));
}

class TestBitmapVisit : public ::testing::Test {};

TEST_F(TestBitmapVisit, OutputZeroOffset) {
  int64_t bits = 1000, part = bits / 4;

  std::vector<bool> data;
  random_bool_vector(data, bits);

  arrow::BooleanBuilder boolean_builder;
  ASSERT_OK(boolean_builder.AppendValues(data));
  ASSERT_OK_AND_ASSIGN(auto arrow_data, boolean_builder.Finish());

  std::shared_ptr<Buffer>& arrow_buffer = arrow_data->data()->buffers[1];

  Bitmap bm0(arrow_buffer, 0, part);
  Bitmap bm1 = bm0.Slice(part * 1, part);  // this goes beyond bm0's len
  Bitmap bm2 = bm0.Slice(part * 2, part);  // this goes beyond bm0's len

  ASSERT_OK_AND_ASSIGN(auto out, AllocateBitmap(part));
  Bitmap out_bm(out, 0, part);

  // (bm0 & bm1) | bm2
  std::array<Bitmap, 3> bms{bm0, bm1, bm2};
  Bitmap::VisitWordsAndWrite(
      bms,
      [](std::array<uint64_t, 3>& words) { return (words[0] & words[1]) | words[2]; },
      &out_bm);

  std::vector<bool> v0(data.begin(), data.begin() + part);
  std::vector<bool> v1(data.begin() + part * 1, data.begin() + part * 2);
  std::vector<bool> v2(data.begin() + part * 2, data.begin() + part * 3);
  std::vector<bool> v3(part);
  // v3 = v0 & v1
  std::transform(v0.begin(), v0.end(), v1.begin(), v3.begin(), std::logical_and<bool>());
  // v3 |= v2
  std::transform(v3.begin(), v3.end(), v2.begin(), v3.begin(), std::logical_or<bool>());

  VerifyBoolOutput(out_bm, v3);
}

TEST_F(TestBitmapVisit, OutputNonZeroOffset) {
  int64_t bits = 1000, part = bits / 4;

  std::vector<bool> data;
  random_bool_vector(data, bits);

  arrow::BooleanBuilder boolean_builder;
  ASSERT_OK(boolean_builder.AppendValues(data));
  ASSERT_OK_AND_ASSIGN(auto arrow_data, boolean_builder.Finish());

  std::shared_ptr<Buffer>& arrow_buffer = arrow_data->data()->buffers[1];

  Bitmap bm0(arrow_buffer, 0, part);
  Bitmap bm1 = bm0.Slice(part * 1, part);  // this goes beyond bm0's len
  Bitmap bm2 = bm0.Slice(part * 2, part);  // this goes beyond bm0's len

  // allocate lager buffer but only use the last `part`
  ASSERT_OK_AND_ASSIGN(auto out, AllocateBitmap(part * 2));
  Bitmap out_bm(out, part, part);

  // (bm0 & bm1) | bm2
  std::array<Bitmap, 3> bms{bm0, bm1, bm2};
  Bitmap::VisitWordsAndWrite(
      bms,
      [](std::array<uint64_t, 3>& words) { return (words[0] & words[1]) | words[2]; },
      &out_bm);

  std::vector<bool> v0(data.begin(), data.begin() + part);
  std::vector<bool> v1(data.begin() + part * 1, data.begin() + part * 2);
  std::vector<bool> v2(data.begin() + part * 2, data.begin() + part * 3);
  std::vector<bool> v3(part);
  // v3 = v0 & v1
  std::transform(v0.begin(), v0.end(), v1.begin(), v3.begin(), std::logical_and<bool>());
  // v3 |= v2
  std::transform(v3.begin(), v3.end(), v2.begin(), v3.begin(), std::logical_or<bool>());

  VerifyBoolOutput(out_bm, v3);
}

}  // namespace internal
}  // namespace arrow
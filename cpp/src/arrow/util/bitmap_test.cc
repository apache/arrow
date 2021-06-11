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

std::string VectorToString(const std::vector<bool>& v) {
  std::string out(v.size() + +((v.size() - 1) / 8), ' ');
  for (size_t i = 0; i < v.size(); ++i) {
    out[i + (i / 8)] = v[i] ? '1' : '0';
  }
  return out;
}

void VerifyBoolOutput(const Bitmap& bitmap, const std::vector<bool>& expected) {
  arrow::BooleanBuilder boolean_builder;
  ASSERT_OK(boolean_builder.AppendValues(expected));
  ASSERT_OK_AND_ASSIGN(auto arr, boolean_builder.Finish());

  ASSERT_TRUE(BitmapEquals(bitmap.buffer()->data(), bitmap.offset(),
                           arr->data()->buffers[1]->data(), 0, expected.size()))
      << "exp: " << VectorToString(expected) << "\ngot: " << bitmap.ToString();
}

void RunOutputNoOffset(int part) {
  int64_t bits = 4 * part;
  std::vector<bool> data;
  random_bool_vector(data, bits);

  arrow::BooleanBuilder boolean_builder;
  ASSERT_OK(boolean_builder.AppendValues(data));
  ASSERT_OK_AND_ASSIGN(auto arrow_data, boolean_builder.Finish());

  std::shared_ptr<Buffer>& arrow_buffer = arrow_data->data()->buffers[1];

  Bitmap bm0(arrow_buffer, 0, part);
  Bitmap bm1 = bm0.Slice(part * 1, part);  // this goes beyond bm0's len
  Bitmap bm2 = bm0.Slice(part * 2, part);  // this goes beyond bm0's len

  std::array<Bitmap, 2> out_bms;
  ASSERT_OK_AND_ASSIGN(auto out0, AllocateBitmap(part));
  ASSERT_OK_AND_ASSIGN(auto out1, AllocateBitmap(part));
  out_bms[0] = Bitmap(out0, 0, part);
  out_bms[1] = Bitmap(out1, 0, part);

  std::vector<bool> v0(data.begin(), data.begin() + part);
  std::vector<bool> v1(data.begin() + part * 1, data.begin() + part * 2);
  std::vector<bool> v2(data.begin() + part * 2, data.begin() + part * 3);

  // out0 = bm0 & bm1, out1= bm0 | bm2
  std::array<Bitmap, 3> in_bms{bm0, bm1, bm2};
  Bitmap::VisitWordsAndWrite(
      in_bms, &out_bms,
      [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 2>* out) {
        out->at(0) = in[0] & in[1];
        out->at(1) = in[0] | in[2];
      });

  std::vector<bool> out_v0(part);
  std::vector<bool> out_v1(part);
  // v3 = v0 & v1
  std::transform(v0.begin(), v0.end(), v1.begin(), out_v0.begin(),
                 std::logical_and<bool>());
  // v3 |= v2
  std::transform(v0.begin(), v0.end(), v2.begin(), out_v1.begin(),
                 std::logical_or<bool>());

  //  std::cout << "v0: " << VectorToString(v0)<< "\n";
  //  std::cout << "b0: " << bm0.ToString()<< "\n";
  //  std::cout << "v1: " << VectorToString(v1)<< "\n";
  //  std::cout << "b1: " << bm1.ToString()<< "\n";
  //  std::cout << "v2: " << VectorToString(v2) << "\n";
  //  std::cout << "b2: " << bm2.ToString() << "\n";

  VerifyBoolOutput(out_bms[0], out_v0);
  VerifyBoolOutput(out_bms[1], out_v1);
}

void RunOutputWithOffset(int64_t part) {
  int64_t bits = part * 4;
  std::vector<bool> data;
  random_bool_vector(data, bits);

  arrow::BooleanBuilder boolean_builder;
  ASSERT_OK(boolean_builder.AppendValues(data));
  ASSERT_OK_AND_ASSIGN(auto arrow_data, boolean_builder.Finish());

  std::shared_ptr<Buffer>& arrow_buffer = arrow_data->data()->buffers[1];

  Bitmap bm0(arrow_buffer, 0, part);
  Bitmap bm1(arrow_buffer, part * 1, part);
  Bitmap bm2(arrow_buffer, part * 2, part);

  std::array<Bitmap, 2> out_bms;
  ASSERT_OK_AND_ASSIGN(auto out, AllocateBitmap(part * 4));
  out_bms[0] = Bitmap(out, part, part);
  out_bms[1] = Bitmap(out, part * 2, part);

  std::vector<bool> v0(data.begin(), data.begin() + part);
  std::vector<bool> v1(data.begin() + part * 1, data.begin() + part * 2);
  std::vector<bool> v2(data.begin() + part * 2, data.begin() + part * 3);

  //  std::cout << "v0: " << VectorToString(v0) << "\n";
  //  std::cout << "b0: " << bm0.ToString() << "\n";
  //  std::cout << "v1: " << VectorToString(v1) << "\n";
  //  std::cout << "b1: " << bm1.ToString() << "\n";
  //  std::cout << "v2: " << VectorToString(v2) << "\n";
  //  std::cout << "b2: " << bm2.ToString() << "\n";

  std::vector<bool> out_v0(part);
  std::vector<bool> out_v1(part);
  // v3 = v0 & v1
  std::transform(v0.begin(), v0.end(), v1.begin(), out_v0.begin(),
                 std::logical_and<bool>());
  // v3 |= v2
  std::transform(v0.begin(), v0.end(), v2.begin(), out_v1.begin(),
                 std::logical_or<bool>());

  //  std::cout << "out0: " << VectorToString(out_v0) << "\n";
  //  std::cout << "out1: " << VectorToString(out_v1) << "\n";

  // out0 = bm0 & bm1, out1= bm0 | bm2
  std::array<Bitmap, 3> in_bms{bm0, bm1, bm2};
  Bitmap::VisitWordsAndWrite(
      in_bms, &out_bms,
      [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 2>* out) {
        out->at(0) = in[0] & in[1];
        out->at(1) = in[0] | in[2];
      });

  VerifyBoolOutput(out_bms[0], out_v0);
  VerifyBoolOutput(out_bms[1], out_v1);
}

class TestBitmapVisitOutputNoOffset : public ::testing::TestWithParam<int32_t> {};

TEST_P(TestBitmapVisitOutputNoOffset, Test1) {
  auto part = GetParam();
  RunOutputNoOffset(part);
}

INSTANTIATE_TEST_SUITE_P(General, TestBitmapVisitOutputNoOffset,
                         testing::Values(199, 256, 1000));

INSTANTIATE_TEST_SUITE_P(EdgeCases, TestBitmapVisitOutputNoOffset,
                         testing::Values(5, 13, 21, 29, 37, 41, 51, 59, 64, 97));

INSTANTIATE_TEST_SUITE_P(EdgeCases2, TestBitmapVisitOutputNoOffset,
                         testing::Values(8, 16, 24, 32, 40, 48, 56, 64));

class TestBitmapVisitOutputWithOffset : public ::testing::TestWithParam<int32_t> {};

TEST_P(TestBitmapVisitOutputWithOffset, Test2) {
  auto part = GetParam();
  RunOutputWithOffset(part);
}

INSTANTIATE_TEST_SUITE_P(General, TestBitmapVisitOutputWithOffset,
                         testing::Values(199, 256, 1000));

INSTANTIATE_TEST_SUITE_P(EdgeCases, TestBitmapVisitOutputWithOffset,
                         testing::Values(7, 15, 23, 31, 39, 47, 55, 63, 73, 97));

INSTANTIATE_TEST_SUITE_P(EdgeCases2, TestBitmapVisitOutputWithOffset,
                         testing::Values(8, 16, 24, 32, 40, 48, 56, 64));

}  // namespace internal
}  // namespace arrow

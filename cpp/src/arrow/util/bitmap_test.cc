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

class TestBitmapVisit : public ::testing::Test {};

TEST_F(TestBitmapVisit, SingleWriterOutputZeroOffset) {
  // choosing part = 199, a prime, so that shifts are falling in-between bytes
  int64_t part = 199, bits = part * 4;

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

  auto visitor = [](const std::array<uint64_t, 3>& in_words, uint64_t& out_words) {
    out_words = (in_words[0] & in_words[1]) | in_words[2];
  };

  // (bm0 & bm1) | bm2
  Bitmap::VisitWordsAndWrite(
      {bm0, bm1, bm2}, std::forward<Bitmap::SingleOutputVisitor<3, uint64_t>>(visitor),
      out_bm);

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

TEST_F(TestBitmapVisit, SingleWriterOutputNonZeroOffset) {
  // choosing part = 199, a prime
  int64_t part = 199, bits = part * 4;

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

  auto visitor = [](const std::array<uint64_t, 3>& in_words, uint64_t& out_words) {
    out_words = (in_words[0] & in_words[1]) | in_words[2];
  };

  // (bm0 & bm1) | bm2
  Bitmap::VisitWordsAndWrite(
      {bm0, bm1, bm2}, std::forward<Bitmap::SingleOutputVisitor<3, uint64_t>>(visitor),
      out_bm);

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

TEST_F(TestBitmapVisit, MultiWriterOutputZeroOffset) {
  // choosing part = 199, a prime
  int64_t part = 199, bits = part * 4;

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
  auto visitor_func = [](const std::array<uint64_t, 3>& in,
                         std::array<uint64_t, 2>& out) {
    out[0] = in[0] & in[1];
    out[1] = in[0] | in[2];
  };

  Bitmap::VisitWordsAndWrite(
      {bm0, bm1, bm2},
      std::forward<Bitmap::MultiOutputVisitor<3, 2, uint64_t>>(visitor_func), out_bms);

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

TEST_F(TestBitmapVisit, MultiWriterOutputNonZeroOffset) {
  // choosing part = 199, a prime
  int64_t part = 199, bits = part * 4;

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
  ASSERT_OK_AND_ASSIGN(auto out, AllocateBitmap(part * 4));
  out_bms[0] = Bitmap(out, part, part);
  out_bms[1] = Bitmap(out, part * 2, part);

  std::vector<bool> v0(data.begin(), data.begin() + part);
  std::vector<bool> v1(data.begin() + part * 1, data.begin() + part * 2);
  std::vector<bool> v2(data.begin() + part * 2, data.begin() + part * 3);

  //  std::cout << "v0: " << VectorToString(v0)<< "\n";
  //  std::cout << "b0: " << bm0.ToString() << "\n";
  //  std::cout << "v1: " << VectorToString(v1) << "\n";
  //  std::cout << "b1: " << bm1.ToString() << "\n";
  //  std::cout << "v2: " << VectorToString(v2) << "\n";
  //  std::cout << "b2: " << bm2.ToString() << "\n";

  // out0 = bm0 & bm1, out1= bm0 | bm2
  auto visitor_func = [](const std::array<uint64_t, 3>& in,
                         std::array<uint64_t, 2>& out) {
    out[0] = in[0] & in[1];
    out[1] = in[0] | in[2];
  };

  Bitmap::VisitWordsAndWrite(
      {bm0, bm1, bm2},
      std::forward<Bitmap::MultiOutputVisitor<3, 2, uint64_t>>(visitor_func), out_bms);

  std::vector<bool> out_v0(part);
  std::vector<bool> out_v1(part);
  // v3 = v0 & v1
  std::transform(v0.begin(), v0.end(), v1.begin(), out_v0.begin(),
                 std::logical_and<bool>());
  // v3 |= v2
  std::transform(v0.begin(), v0.end(), v2.begin(), out_v1.begin(),
                 std::logical_or<bool>());

  VerifyBoolOutput(out_bms[0], out_v0);
  VerifyBoolOutput(out_bms[1], out_v1);
}

}  // namespace internal
}  // namespace arrow
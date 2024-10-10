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

#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::compute {

// ----------------------------------------------------------------------
// ReverseIndices tests

TEST(ReverseIndices, Invalid) {
  {
    auto indices = ArrayFromJSON(int32(), "[]");
    ReverseIndicesOptions options{0, utf8()};
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of reverse_indices must be integer, got string",
        CallFunction("reverse_indices", {indices}, &options));
  }
}

TEST(ReverseIndices, DefaultOptions) {
  {
    ReverseIndicesOptions options;
    ASSERT_EQ(options.output_length, -1);
    ASSERT_EQ(options.output_type, nullptr);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[0]");
    ReverseIndicesOptions options;
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_indices", {indices}, &options));
    ASSERT_EQ(result.length(), 1);
    ASSERT_EQ(result.type()->id(), Type::INT32);
  }
}

TEST(ReverseIndices, Basic) {
  {
    auto indices = ArrayFromJSON(int32(), "[9, 7, 5, 3, 1, 0, 2, 4, 6, 8]");
    auto expected = ArrayFromJSON(int8(), "[5, 4, 6, 3, 7, 2, 8, 1, 9, 0]");
    ReverseIndicesOptions options{10, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_indices", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 2]");
    auto expected = ArrayFromJSON(int8(), "[null, 0, 1, null, null, null, null]");
    ReverseIndicesOptions options{7, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_indices", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 2]");
    auto expected = ArrayFromJSON(int8(), "[]");
    ReverseIndicesOptions options{0, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_indices", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 0]");
    auto expected = ArrayFromJSON(int8(), "[1]");
    ReverseIndicesOptions options{1, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_indices", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[1, 2]");
    auto expected = ArrayFromJSON(int8(), "[null]");
    ReverseIndicesOptions options{1, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_indices", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ArrayFromJSON(int32(), "[]");
    auto expected = ArrayFromJSON(int8(), "[null, null, null, null, null, null, null]");
    ReverseIndicesOptions options{7, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_indices", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
}

TEST(ReverseIndices, Overflow) {
  {
    auto indices = ConstantArrayGenerator::Zeroes(127, int8());
    auto expected = ArrayFromJSON(int8(), "[126]");
    ReverseIndicesOptions options{1, int8()};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("reverse_indices", {indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto indices = ConstantArrayGenerator::Zeroes(128, int8());
    ReverseIndicesOptions options{1, int8()};
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Output type int8 of reverse_indices is "
                               "insufficient to store indices of length 128",
                               CallFunction("reverse_indices", {indices}, &options));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto indices, MakeArrayOfNull(int8(), 128));
    auto expected = ArrayFromJSON(int8(), "[null]");
    ReverseIndicesOptions options{1, int8()};
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Output type int8 of reverse_indices is "
                               "insufficient to store indices of length 128",
                               CallFunction("reverse_indices", {indices}, &options));
  }
}

// ----------------------------------------------------------------------
// Permute tests

TEST(Permute, Basic) {
  {
    auto values = ArrayFromJSON(int64(), "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]");
    auto indices = ArrayFromJSON(int64(), "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]");
    auto expected = ArrayFromJSON(int64(), "[19, 18, 17, 16, 15, 14, 13, 12, 11, 10]");
    PermuteOptions options{10};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("permute", {values, indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto values = ArrayFromJSON(int64(), "[0, 0, 0, 1, 1, 1]");
    auto indices = ArrayFromJSON(int64(), "[0, 3, 6, 1, 4, 7]");
    auto expected = ArrayFromJSON(int64(), "[0, 1, null, 0, 1, null, 0, 1, null]");
    PermuteOptions options{9};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("permute", {values, indices}, &options));
    AssertDatumsEqual(expected, result);
  }
}

};  // namespace arrow::compute

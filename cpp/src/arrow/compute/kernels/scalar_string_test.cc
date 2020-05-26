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

#include <memory>

#include <gtest/gtest.h>

#include "arrow/compute/api_scalar.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

typedef ::testing::Types<StringType, LargeStringType> StringTypes;

template <typename TestType>
class TestStringKernels : public ::testing::Test {
 protected:
  using OffsetType = typename TypeTraits<TestType>::OffsetType;

  void CheckUnary(std::string func_name, std::string json_input,
                  std::shared_ptr<DataType> out_ty, std::string json_expected) {
    auto input = ArrayFromJSON(string_type(), json_input);
    auto expected = ArrayFromJSON(out_ty, json_expected);
    ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {input}));
    AssertArraysEqual(*expected, *out.make_array(), /*verbose=*/true);

    // Check all the scalars
    for (int64_t i = 0; i < input->length(); ++i) {
      ASSERT_OK_AND_ASSIGN(auto val, input->GetScalar(i));
      ASSERT_OK_AND_ASSIGN(auto ex_val, expected->GetScalar(i));
      ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {val}));
      AssertScalarsEqual(*ex_val, *out.scalar(), /*verbose=*/true);
    }
  }

  std::shared_ptr<DataType> string_type() {
    return TypeTraits<TestType>::type_singleton();
  }

  std::shared_ptr<DataType> offset_type() {
    return TypeTraits<OffsetType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestStringKernels, StringTypes);

TYPED_TEST(TestStringKernels, AsciiLength) {
  this->CheckUnary("ascii_length", "[\"aaa\", null, \"\", \"b\"]", this->offset_type(),
                   "[3, null, 0, 1]");
}

TYPED_TEST(TestStringKernels, AsciiUpper) {
  this->CheckUnary("ascii_upper", "[\"aAa&\", null, \"\", \"b\"]", this->string_type(),
                   "[\"AAA&\", null, \"\", \"B\"]");
}

}  // namespace compute
}  // namespace arrow

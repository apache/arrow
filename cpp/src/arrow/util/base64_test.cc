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

#include "arrow/testing/gtest_util.h"
#include "arrow/util/base64.h"


namespace arrow {
namespace util {

TEST(Base64DecodeTest, ValidInputs) {
  ASSERT_OK_AND_ASSIGN(auto empty, arrow::util::base64_decode(""));
  EXPECT_EQ(empty, "");

  ASSERT_OK_AND_ASSIGN(auto two_paddings, arrow::util::base64_decode("Zg=="));
  EXPECT_EQ(two_paddings, "f");

  ASSERT_OK_AND_ASSIGN(auto one_padding, arrow::util::base64_decode("Zm8="));
  EXPECT_EQ(one_padding, "fo");

  ASSERT_OK_AND_ASSIGN(auto no_padding, arrow::util::base64_decode("Zm9v"));
  EXPECT_EQ(no_padding, "foo");

  ASSERT_OK_AND_ASSIGN(auto multiblock, arrow::util::base64_decode("SGVsbG8gd29ybGQ="));
  EXPECT_EQ(multiblock, "Hello world");
}

TEST(Base64DecodeTest, BinaryOutput) {
  // 'A' maps to index 0 — same zero value used for padding slots
  // verifies the 'A' bug is not present
  ASSERT_OK_AND_ASSIGN(auto all_A, arrow::util::base64_decode("AAAA"));
  EXPECT_EQ(all_A, std::string("\x00\x00\x00", 3));

  // Arbitrary non-ASCII output bytes
  ASSERT_OK_AND_ASSIGN(auto binary, arrow::util::base64_decode("AP8A"));
  EXPECT_EQ(binary, std::string("\x00\xff\x00", 3));
}

TEST(Base64DecodeTest, InvalidLength) {
  ASSERT_RAISES_WITH_MESSAGE(
    Invalid,
    "Invalid: Invalid base64 input: length is not a multiple of 4",
    arrow::util::base64_decode("abc"));
}

TEST(Base64DecodeTest, InvalidCharacters) {
  ASSERT_RAISES(Invalid, arrow::util::base64_decode("ab$="));

  // Non-ASCII byte
  std::string non_ascii = std::string("abc") + static_cast<char>(0xFF);
  ASSERT_RAISES(Invalid, arrow::util::base64_decode(non_ascii));

  // Corruption mid-string across multiple blocks
  ASSERT_RAISES(Invalid, arrow::util::base64_decode("aGVs$G8gd29ybGQ="));
}

TEST(Base64DecodeTest, InvalidPadding) {
  // Padding in wrong position within block
  ASSERT_RAISES(Invalid, arrow::util::base64_decode("ab=c"));

  // 3 padding characters — exceeds maximum of 2
  ASSERT_RAISES(Invalid, arrow::util::base64_decode("a==="));

  // 4 padding characters
  ASSERT_RAISES(Invalid, arrow::util::base64_decode("===="));

  // Padding in non-final block across multiple blocks
  ASSERT_RAISES(Invalid, arrow::util::base64_decode("Zm8=Zm8="));
}

}
}

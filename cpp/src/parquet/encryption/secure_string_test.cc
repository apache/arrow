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
#include <vector>

#include "parquet/encryption/secure_string.h"

namespace parquet::encryption::test {

std::string_view StringArea(const std::string& string) {
  return {string.data(), string.capacity()};
}

void AssertSecurelyCleared(std::string_view area) {
  // the entire area is filled with zeros
  std::string zeros(area.size(), '\0');
  ASSERT_EQ(area, std::string_view(zeros));
}

void AssertSecurelyCleared(const std::string& string) {
  AssertSecurelyCleared(StringArea(string));
}

TEST(TestSecureString, SecureClearString) {
  // short string
  {
    std::string tiny("abc");
    auto old_area = StringArea(tiny);
    SecureString::SecureClear(&tiny);
    AssertSecurelyCleared(tiny);
    AssertSecurelyCleared(old_area);
  }

  // long string
  {
    std::string large(1024, 'x');
    large.resize(512, 'y');
    auto old_area = StringArea(large);
    SecureString::SecureClear(&large);
    AssertSecurelyCleared(large);
    AssertSecurelyCleared(old_area);
  }

  // empty string
  {
    // this creates an empty string with some non-zero characters in the string buffer
    // we test that all those characters are securely cleared
    std::string empty("abcdef");
    empty.resize(0);
    auto old_area = StringArea(empty);
    SecureString::SecureClear(&empty);
    AssertSecurelyCleared(empty);
    AssertSecurelyCleared(old_area);
  }
}

TEST(TestSecureString, Construct) {
  // move constructing from a string securely clears that string
  std::string string("hello world");
  auto old_string = StringArea(string);
  SecureString secret_from_string(std::move(string));
  AssertSecurelyCleared(string);
  AssertSecurelyCleared(old_string);
  ASSERT_FALSE(secret_from_string.empty());

  // move constructing from a secure string securely clears that secure string
  auto old_secret_from_string =
      std::string_view(secret_from_string.as_view().data(), secret_from_string.length());
  SecureString secret_from_move_secret(std::move(secret_from_string));
  ASSERT_TRUE(secret_from_string.empty());
  AssertSecurelyCleared(old_secret_from_string);
  ASSERT_FALSE(secret_from_move_secret.empty());

  // copy constructing from a secure string does not modify that secure string
  SecureString secret_from_secret(secret_from_move_secret);
  ASSERT_FALSE(secret_from_move_secret.empty());
  ASSERT_FALSE(secret_from_secret.empty());
  ASSERT_EQ(secret_from_secret, secret_from_move_secret);
}

TEST(TestSecureString, Assign) {
  // move assigning from a string securely clears that string
  std::string string("hello world");
  auto old_string = StringArea(string);
  SecureString secret_from_string("a secret");
  auto old_secret_from_string =
      std::string_view(secret_from_string.as_view().data(), secret_from_string.length());
  secret_from_string = std::move(string);
  AssertSecurelyCleared(string);
  AssertSecurelyCleared(old_string);
  if (old_secret_from_string.data() != secret_from_string.as_view().data()) {
    AssertSecurelyCleared(old_secret_from_string);
  }
  ASSERT_FALSE(secret_from_string.empty());

  // move assigning from a secure string securely clears that secure string
  auto new_secret_from_string =
      std::string_view(secret_from_string.as_view().data(), secret_from_string.length());
  SecureString secret_from_move_secret("another secret");
  auto old_secret_from_move_secret = std::string_view(
      secret_from_move_secret.as_view().data(), secret_from_move_secret.length());
  secret_from_move_secret = std::move(secret_from_string);
  ASSERT_TRUE(secret_from_string.empty());
  AssertSecurelyCleared(new_secret_from_string);
  if (old_secret_from_move_secret.data() != secret_from_move_secret.as_view().data()) {
    AssertSecurelyCleared(old_secret_from_move_secret);
  }
  ASSERT_FALSE(secret_from_move_secret.empty());

  // assigning from a secure string does not modify that secure string
  SecureString secret_from_secret;
  secret_from_secret = secret_from_move_secret;
  ASSERT_FALSE(secret_from_move_secret.empty());
  ASSERT_FALSE(secret_from_secret.empty());
  ASSERT_EQ(secret_from_secret, secret_from_move_secret);
}

TEST(TestSecureString, Compare) {
  ASSERT_TRUE(SecureString("") == SecureString(""));
  ASSERT_FALSE(SecureString("") != SecureString(""));

  ASSERT_TRUE(SecureString("hello world") == SecureString("hello world"));
  ASSERT_FALSE(SecureString("hello world") != SecureString("hello world"));

  ASSERT_FALSE(SecureString("hello world") == SecureString("hello worlds"));
  ASSERT_TRUE(SecureString("hello world") != SecureString("hello worlds"));
}

TEST(TestSecureString, Cardinality) {
  ASSERT_TRUE(SecureString("").empty());
  ASSERT_EQ(SecureString("").size(), 0);
  ASSERT_EQ(SecureString("").length(), 0);

  ASSERT_FALSE(SecureString("hello world").empty());
  ASSERT_EQ(SecureString("hello world").size(), 11);
  ASSERT_EQ(SecureString("hello world").length(), 11);
}

TEST(TestSecureString, AsSpan) {
  SecureString secret("hello world");
  const SecureString& const_secret(secret);
  auto const_span = const_secret.as_span();
  auto mutual_span = secret.as_span();

  std::string expected = "hello world";
  ::arrow::util::span expected_span = {reinterpret_cast<uint8_t*>(expected.data()),
                                       expected.size()};
  ASSERT_EQ(const_span, expected_span);
  ASSERT_EQ(mutual_span, expected_span);

  // modify secret through mutual span
  // the const span shares the same secret, so it is changed as well
  mutual_span[0] = 'H';
  expected_span[0] = 'H';
  ASSERT_EQ(const_span, expected_span);
  ASSERT_EQ(mutual_span, expected_span);
}

TEST(TestSecureString, AsView) {
  const SecureString secret = SecureString("hello world");
  const std::string_view view = secret.as_view();
  ASSERT_EQ(view, "hello world");
}

}  // namespace parquet::encryption::test

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

void assert_securely_cleared(const std::string& string) {
  // the entire buffer of the string is filled with zeros
  std::vector<char> zeros(string.capacity());
  ::arrow::util::span actual(string.data(), string.capacity());
  ::arrow::util::span expected(zeros.data(), zeros.size());
  ASSERT_EQ(actual, expected);

  // the string is empty
  ASSERT_TRUE(string.empty());
}

TEST(TestSecureString, SecureClearString) {
  // short string
  {
    std::string tiny("abc");
    SecureString::SecureClear(tiny);
    assert_securely_cleared(tiny);
  }

  // long string
  {
    std::string large(1024, 'x');
    large.resize(1024, 'y');
    SecureString::SecureClear(large);
    assert_securely_cleared(large);
  }

  // empty string
  {
    // this creates an empty string with some non-zero characters in the string buffer
    // we test that all those characters are securely cleared
    std::string empty("abcdef");
    empty.resize(0);
    SecureString::SecureClear(empty);
    assert_securely_cleared(empty);
  }
}

TEST(TestSecureString, Construct) {
  // move constructing from a string securely clears that string
  std::string string("hello world");
  SecureString secret_from_string(std::move(string));
  assert_securely_cleared(string);
  ASSERT_FALSE(secret_from_string.empty());

  // move constructing from a secure string securely clears that secure string
  // Note: there is no way to test the secure clearing of the moved secure string
  SecureString secret_from_move_secret(std::move(secret_from_string));
  ASSERT_TRUE(secret_from_string.empty());
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
  SecureString secret_from_string;
  secret_from_string = std::move(string);
  assert_securely_cleared(string);
  ASSERT_FALSE(secret_from_string.empty());

  // move assigning from a secure string securely clears that secure string
  // Note: there is no way to test the secure clearing of the moved secure string
  SecureString secret_from_move_secret;
  secret_from_move_secret = std::move(secret_from_string);
  ASSERT_TRUE(secret_from_string.empty());
  ASSERT_FALSE(secret_from_move_secret.empty());

  // assigning from a secure string does not modify that secure string
  SecureString secret_from_secret;
  secret_from_secret = secret_from_move_secret;
  ASSERT_FALSE(secret_from_move_secret.empty());
  ASSERT_FALSE(secret_from_secret.empty());
  ASSERT_EQ(secret_from_secret, secret_from_move_secret);
}

}  // namespace parquet::encryption::test

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
#include <algorithm>
#include <vector>

#include "parquet/encryption/secure_string.h"

namespace parquet::encryption::test {

std::string_view StringArea(const std::string& string) {
  return {string.data(), string.capacity()};
}

void AssertSecurelyCleared(const std::string_view area) {
  // the entire area is filled with zeros
  std::string zeros(area.size(), '\0');
  ASSERT_EQ(area, std::string_view(zeros));
}

void AssertSecurelyCleared(const std::string& string) {
  AssertSecurelyCleared(StringArea(string));
}

/**
 * Checks the area has been securely cleared after some position.
 */
void AssertSecurelyCleared(const std::string_view area, const size_t pos) {
  // the area after pos is filled with zeros
  if (pos < area.size()) {
    std::string zeros(area.size() - pos, '\0');
    ASSERT_EQ(area.substr(pos), std::string_view(zeros));
  }
}

/**
 * Checks the area has been securely cleared from the secret value.
 * Assumes the area has been released, so it might have been reclaimed and changed after
 * cleaning. We cannot check for all-zeros, best we can check here is no secret character
 * has leaked. If by any chance the modification produced a former key character at the right
 * position, this will be false negative / flaky. Therefore, we check for three consecutive
 * secret characters before we fail.
 */
void AssertSecurelyCleared(const std::string_view area, const std::string& secret_value) {
  auto leaks = 0;
  for (size_t i = 0; i < secret_value.size(); i++) {
    if (area[i] == secret_value[i]) {
      leaks++;
    } else {
      if (leaks >= 3) {
        break;
      }
      leaks = 0;
    }
  }
  if (leaks >= 3) {
      FAIL() << leaks << " characters of secret leaked into " << area;
  }
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
  // move-constructing from a string securely clears that string
  std::string string("hello world");
  auto old_string = StringArea(string);
  SecureString secret_from_string(std::move(string));
  AssertSecurelyCleared(string);
  AssertSecurelyCleared(old_string);
  ASSERT_FALSE(secret_from_string.empty());

  // move-constructing from a secure string securely clears that secure string
  auto old_secret_from_string_view = secret_from_string.as_view();
  auto old_secret_from_string_value = std::string(secret_from_string.as_view());
  SecureString secret_from_move_secret(std::move(secret_from_string));
  ASSERT_TRUE(secret_from_string.empty());
  AssertSecurelyCleared(old_secret_from_string_view);
  ASSERT_FALSE(secret_from_move_secret.empty());
  ASSERT_EQ(secret_from_move_secret.as_view(),
            std::string_view(old_secret_from_string_value));

  // copy-constructing from a secure string does not modify that secure string
  SecureString secret_from_secret(secret_from_move_secret);
  ASSERT_FALSE(secret_from_move_secret.empty());
  ASSERT_EQ(secret_from_move_secret.as_view(),
            std::string_view(old_secret_from_string_value));
  ASSERT_FALSE(secret_from_secret.empty());
  ASSERT_EQ(secret_from_secret, secret_from_move_secret);
}

TEST(TestSecureString, Assign) {
  // we initialize with the first string and iteratively assign the subsequent values
  // the first two values are local (15 chars and less), the remainder are non-local
  // strings (larger than 15 chars) memory management of short and long strings behaves
  // differently
  std::vector<std::string> test_strings = {"secret", "another secret",
                                           "a much longer secret", std::string(1024, 'x')};

  // assert test string configuration
  ASSERT_GE(test_strings.size(), 4);
  for (size_t i = 1; i < test_strings.size(); i++) {
    // we expect first two strings to be local strings
    if (i <= 1) {
      ASSERT_LT(test_strings[i].size(), 15 / sizeof(char));
    } else {
      ASSERT_GE(test_strings[i].size(), 15 / sizeof(char));
    }
    // the strings are increasing in size
    if (i > 0) {
      ASSERT_TRUE(test_strings[i].size() > test_strings[i - 1].size());
    }
  }

  std::vector<std::string> reverse_strings = std::vector(test_strings);
  reverse(reverse_strings.begin(), reverse_strings.end());

  for (auto vec : {test_strings, reverse_strings}) {
    auto init_string = vec[0];
    auto strings = std::vector<std::string>(vec.begin() + 1, vec.end());

    {
      // an initialized secure string
      std::string init_string_copy(init_string);
      SecureString secret_from_string(std::move(init_string_copy));

      // move-assigning from a string securely clears that string
      // the earlier value of the secure string is securely cleared
      for (auto string : strings) {
        auto string_copy = std::string(string);
        auto old_string_copy_area = StringArea(string_copy);
        ASSERT_FALSE(string.empty());
        ASSERT_FALSE(string_copy.empty());
        auto old_secret_from_string_area = secret_from_string.as_view();
        auto old_secret_from_string_value = std::string(secret_from_string.as_view());

        secret_from_string = std::move(string_copy);

        ASSERT_FALSE(string.empty());
        ASSERT_TRUE(string_copy.empty());
        AssertSecurelyCleared(string_copy);
        auto secret_from_string_view = secret_from_string.as_view();
        // the secure string can reuse the string_copy's string buffer after assignment
        // then, string_copy's string buffer is obviously not cleared
        if (secret_from_string_view.data() != old_string_copy_area.data()) {
          AssertSecurelyCleared(old_string_copy_area, string);
        }
        ASSERT_FALSE(secret_from_string.empty());
        ASSERT_EQ(secret_from_string.size(), string.size());
        ASSERT_EQ(secret_from_string.length(), string.length());
        ASSERT_EQ(secret_from_string_view, std::string_view(string));
        if (secret_from_string_view.data() == old_secret_from_string_area.data()) {
          // when secure string reuses the buffer, the old value must be cleared
          AssertSecurelyCleared(old_secret_from_string_area, secret_from_string.size());
        } else {
          // when secure string has a new buffer, the old buffer must be cleared
          AssertSecurelyCleared(old_secret_from_string_area,
                                old_secret_from_string_value);
        }
      }
    }

    {
      // an initialized secure string
      std::string init_string_copy(init_string);
      SecureString secret_from_move_secret(std::move(init_string_copy));

      // move-assigning from a secure string securely clears that secure string
      // the earlier value of the secure string is securely cleared
      for (auto string : strings) {
        auto string_copy = std::string(string);
        SecureString secret_string(std::move(string_copy));
        ASSERT_FALSE(string.empty());
        ASSERT_TRUE(string_copy.empty());
        ASSERT_FALSE(secret_string.empty());
        auto old_secret_string_area = secret_string.as_view();
        auto old_secret_string_value = std::string(secret_string.as_view());
        auto old_secret_from_move_secret_area = secret_from_move_secret.as_view();
        auto old_secret_from_move_secret_value =
            std::string(secret_from_move_secret.as_view());

        secret_from_move_secret = std::move(secret_string);

        ASSERT_TRUE(secret_string.empty());
        // the secure string can reuse the string_copy's string buffer after assignment
        // then, string_copy's string buffer is obviously not cleared
        if (old_secret_string_area.data() != secret_from_move_secret.as_view().data()) {
          AssertSecurelyCleared(old_secret_string_area,
                                old_secret_from_move_secret_value);
        }
        ASSERT_FALSE(secret_from_move_secret.empty());
        ASSERT_EQ(secret_from_move_secret.size(), string.size());
        ASSERT_EQ(secret_from_move_secret.length(), string.length());
        ASSERT_EQ(secret_from_move_secret.as_view(), std::string_view(string));
        if (old_secret_from_move_secret_area.data() ==
            secret_from_move_secret.as_view().data()) {
          // when secure string reuses the buffer, the old value must be cleared
          AssertSecurelyCleared(old_secret_from_move_secret_area,
                                secret_from_move_secret.size());
        } else {
          // when secure string has a new buffer, the old buffer must be cleared
          AssertSecurelyCleared(old_secret_from_move_secret_area,
                                old_secret_from_move_secret_value);
        }
      }
    }

    {
      // an initialized secure string
      std::string init_string_copy(init_string);
      SecureString secret_from_copy_secret(std::move(init_string_copy));

      for (auto string : strings) {
        // copy-assigning from a secure string does not modify that secure string
        // the earlier value of the secure string is securely cleared
        auto string_copy = std::string(string);
        SecureString secret_string(std::move(string_copy));
        ASSERT_FALSE(string.empty());
        ASSERT_TRUE(string_copy.empty());
        ASSERT_FALSE(secret_string.empty());
        auto old_secret_from_copy_secret_area = secret_from_copy_secret.as_view();
        auto old_secret_from_copy_secret_value =
            std::string(secret_from_copy_secret.as_view());

        secret_from_copy_secret = secret_string;

        ASSERT_FALSE(secret_string.empty());
        ASSERT_FALSE(secret_from_copy_secret.empty());
        ASSERT_EQ(secret_from_copy_secret.size(), string.size());
        ASSERT_EQ(secret_from_copy_secret.length(), string.length());
        ASSERT_EQ(secret_from_copy_secret.as_view(), std::string_view(string));
        if (old_secret_from_copy_secret_area.data() ==
            secret_from_copy_secret.as_view().data()) {
          // when secure string reuses the buffer, the old value must be cleared
          AssertSecurelyCleared(old_secret_from_copy_secret_area,
                                secret_from_copy_secret.size());
        } else {
          // when secure string has a new buffer, the old buffer must be cleared
          AssertSecurelyCleared(old_secret_from_copy_secret_area,
                                old_secret_from_copy_secret_value);
        }
      }
    }
  }
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

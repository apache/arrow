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

#include "arrow/util/secure_string.h"

namespace arrow::util::test {

std::string_view StringArea(const std::string& string) {
  return {string.data(), string.capacity()};
}

// same as GTest ASSERT_PRED_FORMAT2 macro, but without the outer GTEST_ASSERT_
#define COMPARE(val1, val2) \
  ::testing::internal::EqHelper::Compare(#val1, #val2, val1, val2)

::testing::AssertionResult AssertSecurelyCleared(const std::string_view area) {
  // the entire area is filled with zeros
  std::string zeros(area.size(), '\0');
  return COMPARE(area, std::string_view(zeros));
}

::testing::AssertionResult AssertSecurelyCleared(const std::string& string) {
  return AssertSecurelyCleared(StringArea(string));
}

/**
 * Checks the area has been securely cleared after some position.
 */
::testing::AssertionResult AssertSecurelyCleared(const std::string_view area,
                                                 const size_t pos) {
  // the area after pos is filled with zeros
  if (pos < area.size()) {
    std::string zeros(area.size() - pos, '\0');
    return COMPARE(area.substr(pos), std::string_view(zeros));
  }
  return ::testing::AssertionSuccess();
}

/**
 * Checks the area has been securely cleared from the secret value.
 * Assumes the area has been released, so it might have been reclaimed and changed after
 * cleaning. We cannot check for all-zeros, best we can check here is no secret character
 * has leaked. If by any chance the modification produced a former key character at the
 * right position, this will be false negative / flaky. Therefore, we check for three
 * consecutive secret characters before we fail.
 */
::testing::AssertionResult AssertSecurelyCleared(const std::string_view area,
                                                 const std::string& secret_value) {
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
    return ::testing::AssertionFailure()
           << leaks << " characters of secret leaked into " << area;
  }
  return ::testing::AssertionSuccess();
}

TEST(TestSecureString, AssertSecurelyCleared) {
  // This tests AssertSecurelyCleared helper methods is actually able to identify secret
  // leakage. It retrieves assertion results and asserts result type and message.
  testing::AssertionResult result = testing::AssertionSuccess();

  // check short string with all zeros
  auto short_zeros = std::string(8, '\0');
  short_zeros.resize(short_zeros.capacity(), '\0');  // for string buffers longer than 8
  short_zeros.resize(8);  // now the entire string buffer has zeros
  // checks the entire string buffer (capacity)
  ASSERT_TRUE(AssertSecurelyCleared(short_zeros));
  // checks only 10 bytes (length)
  ASSERT_TRUE(AssertSecurelyCleared(std::string_view(short_zeros)));

  // check long string with all zeros
  auto long_zeros = std::string(1000, '\0');
  long_zeros.resize(long_zeros.capacity(), '\0');  // for longer string buffers
  long_zeros.resize(1000);  // now the entire string buffer has zeros
  // checks the entire string buffer (capacity)
  ASSERT_TRUE(AssertSecurelyCleared(long_zeros));
  // checks only 1000 bytes (length)
  ASSERT_TRUE(AssertSecurelyCleared(std::string_view(long_zeros)));

  // check short string with zeros and non-zeros after string length
  auto short_some_zeros = std::string(short_zeros.length() + 3, '*');
  short_some_zeros = short_zeros;
  result = AssertSecurelyCleared(short_some_zeros);
  ASSERT_FALSE(result);
  ASSERT_EQ(std::string(result.message()),
            "Expected equality of these values:\n"
            "  area\n"
            "    Which is: \"\\0\\0\\0\\0\\0\\0\\0\\0\\0**\\0\\0\\0\\0\"\n"
            "  std::string_view(zeros)\n"
            "    Which is: \"\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\"");

  // check long string with zeros and non-zeros after string length
  auto long_some_zeros = std::string(long_zeros.length() + 10, '*');
  long_some_zeros = long_zeros;
  result = AssertSecurelyCleared(long_some_zeros);
  ASSERT_FALSE(result);
  ASSERT_EQ(std::string(result.message()), "");

  auto no_zeros = std::string("abcdefghijklmnopqrstuvwxyz");
  result = AssertSecurelyCleared(no_zeros);
  ASSERT_FALSE(result);
  ASSERT_EQ(std::string(result.message()),
            "Expected equality of these values:\n"
            "  area\n"
            "    Which is: \"abcdefghijklmnopqrstuvwxyz\"\n"
            "  std::string_view(zeros)\n"
            "    Which is: "
            "\"\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\"
            "0\\0\"");

  auto some_zeros = no_zeros;
  some_zeros = std::string(10, '\0');
  ASSERT_TRUE(AssertSecurelyCleared(some_zeros, 10));
  result = AssertSecurelyCleared(some_zeros);
  ASSERT_FALSE(result);
  ASSERT_EQ(std::string(result.message()),
            "Expected equality of these values:\n"
            "  area\n"
            "    Which is: \"\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0lmnopqrstuvwxyz\"\n"
            "  std::string_view(zeros)\n"
            "    Which is: "
            "\"\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\"
            "0\\0\"");

  ASSERT_TRUE(AssertSecurelyCleared(some_zeros, "12345678901234567890123456"));
  result = AssertSecurelyCleared(StringArea(some_zeros), no_zeros);
  ASSERT_FALSE(result);
  ASSERT_EQ(std::string(result.message()),
            "15 characters of secret leaked into "
            "\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0lmnopqrstuvwxyz");
}

TEST(TestSecureString, SecureClearString) {
  // short string
  {
    std::string tiny("abc");
    auto old_area = StringArea(tiny);
    SecureString::SecureClear(&tiny);
    ASSERT_TRUE(AssertSecurelyCleared(tiny));
    ASSERT_TRUE(AssertSecurelyCleared(old_area));
  }

  // long string
  {
    std::string large(1024, 'x');
    large.resize(512, 'y');
    auto old_area = StringArea(large);
    SecureString::SecureClear(&large);
    ASSERT_TRUE(AssertSecurelyCleared(large));
    ASSERT_TRUE(AssertSecurelyCleared(old_area));
  }

  // empty string
  {
    // this creates an empty string with some non-zero characters in the string buffer
    // we test that all those characters are securely cleared
    std::string empty("abcdef");
    empty.resize(0);
    auto old_area = StringArea(empty);
    SecureString::SecureClear(&empty);
    ASSERT_TRUE(AssertSecurelyCleared(empty));
    ASSERT_TRUE(AssertSecurelyCleared(old_area));
  }
}

TEST(TestSecureString, Construct) {
  // move-constructing from a string securely clears that string
  std::string string("hello world");
  auto old_string = StringArea(string);
  SecureString secret_from_string(std::move(string));
  ASSERT_TRUE(AssertSecurelyCleared(string));
  ASSERT_TRUE(AssertSecurelyCleared(old_string));
  ASSERT_FALSE(secret_from_string.empty());

  // move-constructing from a secure string securely clears that secure string
  auto old_secret_from_string_view = secret_from_string.as_view();
  auto old_secret_from_string_value = std::string(secret_from_string.as_view());
  SecureString secret_from_move_secret(std::move(secret_from_string));
  ASSERT_TRUE(secret_from_string.empty());
  ASSERT_TRUE(AssertSecurelyCleared(old_secret_from_string_view));
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
  // We initialize with the first string and iteratively assign the subsequent values.
  // The first two values are local (very short strings), the remainder are non-local
  // strings. Memory management of short and long strings behaves differently.
  std::vector<std::string> test_strings = {
      "secret", "another secret", "a much longer secret", std::string(1024, 'x')};

  std::vector<std::string> reverse_strings = std::vector(test_strings);
  std::reverse(reverse_strings.begin(), reverse_strings.end());

  for (auto vec : {test_strings, reverse_strings}) {
    auto init_string = vec[0];
    auto strings = std::vector<std::string>(vec.begin() + 1, vec.end());

    {
      // an initialized secure string
      std::string init_string_copy(init_string);
      SecureString secret_from_string(std::move(init_string_copy));

      // move-assigning from a string securely clears that string
      // the earlier value of the secure string is securely cleared
      for (const auto& string : strings) {
        auto string_copy = std::string(string);
        auto old_string_copy_area = StringArea(string_copy);
        ASSERT_FALSE(string.empty());
        ASSERT_FALSE(string_copy.empty());
        auto old_secret_from_string_area = secret_from_string.as_view();
        auto old_secret_from_string_value = std::string(secret_from_string.as_view());

        secret_from_string = std::move(string_copy);

        ASSERT_FALSE(string.empty());
        ASSERT_TRUE(string_copy.empty());
        ASSERT_TRUE(AssertSecurelyCleared(string_copy));
        auto secret_from_string_view = secret_from_string.as_view();
        // the secure string can reuse the string_copy's string buffer after assignment
        // then, string_copy's string buffer is obviously not cleared
        if (secret_from_string_view.data() != old_string_copy_area.data()) {
          ASSERT_TRUE(AssertSecurelyCleared(old_string_copy_area, string));
        }
        ASSERT_FALSE(secret_from_string.empty());
        ASSERT_EQ(secret_from_string.size(), string.size());
        ASSERT_EQ(secret_from_string.length(), string.length());
        ASSERT_EQ(secret_from_string_view, std::string_view(string));
        if (secret_from_string_view.data() == old_secret_from_string_area.data()) {
          // when secure string reuses the buffer, the old value must be cleared
          ASSERT_TRUE(AssertSecurelyCleared(old_secret_from_string_area,
                                            secret_from_string.size()));
        } else {
          // when secure string has a new buffer, the old buffer must be cleared
          ASSERT_TRUE(AssertSecurelyCleared(old_secret_from_string_area,
                                            old_secret_from_string_value));
        }
      }
    }

    {
      // an initialized secure string
      std::string init_string_copy(init_string);
      SecureString secret_from_move_secret(std::move(init_string_copy));

      // move-assigning from a secure string securely clears that secure string
      // the earlier value of the secure string is securely cleared
      for (const auto& string : strings) {
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
        auto secret_from_move_secret_view = secret_from_move_secret.as_view();
        // the secure string can reuse the string_copy's string buffer after assignment
        // then, string_copy's string buffer is obviously not cleared
        if (old_secret_string_area.data() != secret_from_move_secret_view.data()) {
          ASSERT_TRUE(AssertSecurelyCleared(old_secret_string_area,
                                            old_secret_from_move_secret_value));
        }
        ASSERT_FALSE(secret_from_move_secret.empty());
        ASSERT_EQ(secret_from_move_secret.size(), string.size());
        ASSERT_EQ(secret_from_move_secret.length(), string.length());
        ASSERT_EQ(secret_from_move_secret_view, std::string_view(string));
        if (old_secret_from_move_secret_area.data() ==
            secret_from_move_secret_view.data()) {
          // when secure string reuses the buffer, the old value must be cleared
          ASSERT_TRUE(AssertSecurelyCleared(old_secret_from_move_secret_area,
                                            secret_from_move_secret.size()));
        } else {
          // when secure string has a new buffer, the old buffer must be cleared
          ASSERT_TRUE(AssertSecurelyCleared(old_secret_from_move_secret_area,
                                            old_secret_from_move_secret_value));
        }
      }
    }

    {
      // an initialized secure string
      std::string init_string_copy(init_string);
      SecureString secret_from_copy_secret(std::move(init_string_copy));

      for (const auto& string : strings) {
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
          ASSERT_TRUE(AssertSecurelyCleared(old_secret_from_copy_secret_area,
                                            secret_from_copy_secret.size()));
        } else {
          // when secure string has a new buffer, the old buffer must be cleared
          ASSERT_TRUE(AssertSecurelyCleared(old_secret_from_copy_secret_area,
                                            old_secret_from_copy_secret_value));
        }
      }
    }
  }
}

TEST(TestSecureString, Deconstruct) {
#if defined(ARROW_VALGRIND) || defined(ARROW_USE_ASAN)
  GTEST_SKIP() << "Test accesses deallocated memory";
#else
  // We use a very short and a very long string as memory management of short and long
  // strings behaves differently.
  std::vector<std::string> strings = {"short secret", std::string(1024, 'x')};

  for (auto& string : strings) {
    auto old_string_value = string;
    std::string_view view;
    {
      // construct secret
      auto secret = SecureString(std::move(string));
      // memorize view
      view = secret.as_view();
      // deconstruct secret on leaving this context
    }
    // assert secret memory is cleared on deconstruction
    ASSERT_TRUE(AssertSecurelyCleared(view, old_string_value));
    // so is the string (tested more thoroughly elsewhere)
    ASSERT_TRUE(AssertSecurelyCleared(string));
  }
#endif
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
  auto mutable_span = secret.as_span();

  std::string expected = "hello world";
  span expected_span = {reinterpret_cast<uint8_t*>(expected.data()), expected.size()};
  ASSERT_EQ(const_span, expected_span);
  ASSERT_EQ(mutable_span, expected_span);

  // modify secret through mutual span
  // the const span shares the same secret, so it is changed as well
  mutable_span[0] = 'H';
  expected_span[0] = 'H';
  ASSERT_EQ(const_span, expected_span);
  ASSERT_EQ(mutable_span, expected_span);
}

TEST(TestSecureString, AsView) {
  const SecureString secret = SecureString("hello world");
  const std::string_view view = secret.as_view();
  ASSERT_EQ(view, "hello world");
}

}  // namespace arrow::util::test

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

#include "gandiva/encrypt_utils_common.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/opensslv.h>

namespace {

// ERR_raise() only exists since OpenSSL 3.0
void RaiseOpenSSLError(int reason) {
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
  ERR_raise(ERR_LIB_EVP, reason);
#else
  ERR_put_error(ERR_LIB_EVP, 0, reason, __FILE__, __LINE__);
#endif
}

}  // namespace

// Test that get_openssl_error_string returns "Unknown OpenSSL error" when queue is empty
TEST(TestOpenSSLErrorUtils, TestEmptyErrorQueue) {
  // Clear any existing errors
  ERR_clear_error();

  std::string error_string = gandiva::get_openssl_error_string();

  EXPECT_EQ(error_string, "Unknown OpenSSL error");
}

// Test that get_openssl_error_string captures a single error
TEST(TestOpenSSLErrorUtils, TestSingleError) {
  // Clear any existing errors
  ERR_clear_error();

  // Add a single error to the queue
  RaiseOpenSSLError(EVP_R_UNSUPPORTED_ALGORITHM);

  std::string error_string = gandiva::get_openssl_error_string();

  // Verify that the error string is not empty and not the default message
  EXPECT_NE(error_string, "Unknown OpenSSL error");
  EXPECT_GT(error_string.length(), 0);
}

// Test that get_openssl_error_string captures multiple errors
TEST(TestOpenSSLErrorUtils, TestMultipleErrors) {
  // Clear any existing errors
  ERR_clear_error();

  // Populate the OpenSSL error queue with multiple errors
  RaiseOpenSSLError(EVP_R_UNSUPPORTED_ALGORITHM);
  RaiseOpenSSLError(EVP_R_INVALID_KEY_LENGTH);
  RaiseOpenSSLError(EVP_R_INVALID_OPERATION);

  // Call our function to get all errors
  std::string error_string = gandiva::get_openssl_error_string();

  // Verify that the error string is not empty
  EXPECT_NE(error_string, "Unknown OpenSSL error");

  // Verify that all errors are captured (they should be separated by "; ")
  // The exact error messages depend on OpenSSL version, so we just check
  // that we got multiple errors (indicated by the separator)
  EXPECT_THAT(error_string, testing::HasSubstr(";"));

  // Verify the error string contains meaningful content (not just separators)
  EXPECT_GT(error_string.length(), 10);
}

// Test that error queue is properly drained after calling get_openssl_error_string
TEST(TestOpenSSLErrorUtils, TestErrorQueueDrained) {
  // Clear any existing errors
  ERR_clear_error();

  // Add errors to the queue
  RaiseOpenSSLError(EVP_R_UNSUPPORTED_ALGORITHM);
  RaiseOpenSSLError(EVP_R_INVALID_KEY_LENGTH);

  // Call our function to get all errors
  std::string error_string = gandiva::get_openssl_error_string();

  // Verify we got errors
  EXPECT_NE(error_string, "Unknown OpenSSL error");

  // Now call it again - the queue should be empty
  std::string second_call = gandiva::get_openssl_error_string();

  EXPECT_EQ(second_call, "Unknown OpenSSL error");
}

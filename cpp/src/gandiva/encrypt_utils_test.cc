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

#include "gandiva/encrypt_utils.h"

#include <gtest/gtest.h>

#include <unordered_set>

#include "gandiva/execution_context.h"

TEST(TestShaEncryptUtils, TestAesEncryptDecrypt) {
  gandiva::ExecutionContext ctx;
  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  auto* key = (unsigned char*)"12345abcde";
  auto* to_encrypt = (unsigned char*)"some test string";

  auto to_encrypt_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_1[64];

  int32_t cipher_1_len = gandiva::aes_encrypt(ctx_ptr, to_encrypt, to_encrypt_len, key, cipher_1);

  unsigned char decrypted_1[64];
  int32_t decrypted_1_len = gandiva::aes_decrypt(ctx_ptr, cipher_1, cipher_1_len, key, decrypted_1);
  
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_1), decrypted_1_len));

  key = (unsigned char*)"abcde12345";
  to_encrypt = (unsigned char*)"some\ntest\nstring";

  to_encrypt_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_2[64];

  int32_t cipher_2_len = gandiva::aes_encrypt(ctx_ptr, to_encrypt, to_encrypt_len, key, cipher_2);

  unsigned char decrypted_2[64];
  int32_t decrypted_2_len = gandiva::aes_decrypt(ctx_ptr, cipher_2, cipher_2_len, key, decrypted_2);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_2), decrypted_2_len));

}

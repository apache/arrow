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

TEST(TestShaEncryptUtils, TestAesEncryptDecrypt) {
  // 16 bytes key
  auto* key = "12345678abcdefgh";
  auto* to_encrypt = "some test string";

  auto key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  auto to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_1[64];

  int32_t cipher_1_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, key_len, cipher_1);

  unsigned char decrypted_1[64];
  int32_t decrypted_1_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_1),
                                                 cipher_1_len, key, key_len, decrypted_1);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_1), decrypted_1_len));

  // 24 bytes key
  key = "12345678abcdefgh12345678";
  to_encrypt = "some\ntest\nstring";

  key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_2[64];

  int32_t cipher_2_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, key_len, cipher_2);

  unsigned char decrypted_2[64];
  int32_t decrypted_2_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_2),
                                                 cipher_2_len, key, key_len, decrypted_2);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_2), decrypted_2_len));

  // 32 bytes key
  key = "12345678abcdefgh12345678abcdefgh";
  to_encrypt = "New\ntest\nstring";

  key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_3[64];

  int32_t cipher_3_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, key_len, cipher_3);

  unsigned char decrypted_3[64];
  int32_t decrypted_3_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_3),
                                                 cipher_3_len, key, key_len, decrypted_3);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_3), decrypted_3_len));

  // check exception
  char cipher[64] = "JBB7oJAQuqhDCx01fvBRi8PcljW1+nbnOSMk+R0Sz7E==";
  int32_t cipher_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(cipher)));
  unsigned char plain_text[64];

  key = "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh";
  to_encrypt = "New\ntest\nstring";

  key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_4[64];
  ASSERT_THROW({
        gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, key_len, cipher_4);
    }, std::runtime_error);

  ASSERT_THROW({
        gandiva::aes_decrypt(cipher, cipher_len, key, key_len, plain_text);
    }, std::runtime_error);

  key = "12345678";
  to_encrypt = "New\ntest\nstring";

  key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_5[64];
  ASSERT_THROW({
        gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, key_len, cipher_5);
    }, std::runtime_error);
  ASSERT_THROW({
        gandiva::aes_decrypt(cipher, cipher_len, key, key_len, plain_text);
    }, std::runtime_error);  
}

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
  // 8 bytes key
  auto* key = "1234abcd";
  auto* to_encrypt = "some test string";

  auto to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_1[64];

  int32_t cipher_1_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, cipher_1);

  unsigned char decrypted_1[64];
  int32_t decrypted_1_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_1),
                                                 cipher_1_len, key, decrypted_1);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_1), decrypted_1_len));

  // 16 bytes key
  key = "12345678abcdefgh";
  to_encrypt = "some\ntest\nstring";

  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_2[64];

  int32_t cipher_2_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, cipher_2);

  unsigned char decrypted_2[64];
  int32_t decrypted_2_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_2),
                                                 cipher_2_len, key, decrypted_2);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_2), decrypted_2_len));

  // 32 bytes key
  key = "12345678abcdefgh12345678abcdefgh";
  to_encrypt = "New\ntest\nstring";

  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_3[64];

  int32_t cipher_3_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, cipher_3);

  unsigned char decrypted_3[64];
  int32_t decrypted_3_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_3),
                                                 cipher_3_len, key, decrypted_3);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_3), decrypted_3_len));

  // 64 bytes key
  key = "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh";
  to_encrypt = "New\ntest\nstring";

  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_4[64];

  int32_t cipher_4_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, cipher_4);

  unsigned char decrypted_4[64];
  int32_t decrypted_4_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_4),
                                                 cipher_4_len, key, decrypted_4);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_4), decrypted_4_len));

  // 128 bytes key
  key =
      "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12"
      "345678abcdefgh12345678abcdefgh12345678abcdefgh";
  to_encrypt = "A much more longer string then the previous one, but without newline";

  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_5[128];

  int32_t cipher_5_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, cipher_5);

  unsigned char decrypted_5[128];
  int32_t decrypted_5_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_5),
                                                 cipher_5_len, key, decrypted_5);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_5), decrypted_5_len));

  // 192 bytes key
  key =
      "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12"
      "345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh1234"
      "5678abcdefgh12345678abcdefgh";
  to_encrypt =
      "A much more longer string then the previous one, but with \nnewline, pretty cool, "
      "right?";

  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_6[256];

  int32_t cipher_6_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, cipher_6);

  unsigned char decrypted_6[256];
  int32_t decrypted_6_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_6),
                                                 cipher_6_len, key, decrypted_6);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_6), decrypted_6_len));

  // 256 bytes key
  key =
      "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12"
      "345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh1234"
      "5678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh123456"
      "78abcdefgh";
  to_encrypt =
      "A much more longer string then the previous one, but with \nnewline, pretty cool, "
      "right?";

  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_7[256];

  int32_t cipher_7_len = gandiva::aes_encrypt(to_encrypt, to_encrypt_len, key, cipher_7);

  unsigned char decrypted_7[256];
  int32_t decrypted_7_len = gandiva::aes_decrypt(reinterpret_cast<const char*>(cipher_7),
                                                 cipher_7_len, key, decrypted_7);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_7), decrypted_7_len));
}

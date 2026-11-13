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

#include "gandiva/encrypt_utils_ecb.h"

#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <string>

namespace {

std::string FromHex(const std::string& hex) {
  std::string bytes;
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    bytes.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
  }
  return bytes;
}

// NIST SP 800-38A, AES-128 key and the four-block plaintext shared by F.1 and F.2
const char kKey[] = "2b7e151628aed2a6abf7158809cf4f3c";
const char kPlaintext[] =
    "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e51"
    "30c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710";

}  // namespace

TEST(TestAesEcbEncryptUtils, TestAesEncryptDecrypt) {
  // 16 bytes key
  auto* key = "12345678abcdefgh";
  auto* to_encrypt = "some test string";

  auto key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  auto to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_1[64];

  int32_t cipher_1_len =
      gandiva::aes_encrypt_ecb(to_encrypt, to_encrypt_len, key, key_len, true, cipher_1);

  unsigned char decrypted_1[64];
  int32_t decrypted_1_len =
      gandiva::aes_decrypt_ecb(reinterpret_cast<const char*>(cipher_1), cipher_1_len, key,
                               key_len, true, decrypted_1);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_1), decrypted_1_len));

  // 24 bytes key
  key = "12345678abcdefgh12345678";
  to_encrypt = "some\ntest\nstring";

  key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_2[64];

  int32_t cipher_2_len =
      gandiva::aes_encrypt_ecb(to_encrypt, to_encrypt_len, key, key_len, true, cipher_2);

  unsigned char decrypted_2[64];
  int32_t decrypted_2_len =
      gandiva::aes_decrypt_ecb(reinterpret_cast<const char*>(cipher_2), cipher_2_len, key,
                               key_len, true, decrypted_2);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_2), decrypted_2_len));

  // 32 bytes key
  key = "12345678abcdefgh12345678abcdefgh";
  to_encrypt = "New\ntest\nstring";

  key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_3[64];

  int32_t cipher_3_len =
      gandiva::aes_encrypt_ecb(to_encrypt, to_encrypt_len, key, key_len, true, cipher_3);

  unsigned char decrypted_3[64];
  int32_t decrypted_3_len =
      gandiva::aes_decrypt_ecb(reinterpret_cast<const char*>(cipher_3), cipher_3_len, key,
                               key_len, true, decrypted_3);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(to_encrypt), to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted_3), decrypted_3_len));

  // check exception
  char cipher[64] = "JBB7oJAQuqhDCx01fvBRi8PcljW1+nbnOSMk+R0Sz7E==";
  int32_t cipher_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(cipher)));
  unsigned char plain_text[64];

  key = "12345678abcdefgh12345678abcdefgh12345678abcdefgh12345678abcdefgh";
  to_encrypt = "New\ntest\nstring";

  key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_4[64];
  ASSERT_THROW(
      {
        gandiva::aes_encrypt_ecb(to_encrypt, to_encrypt_len, key, key_len, true,
                                 cipher_4);
      },
      std::runtime_error);

  ASSERT_THROW(
      { gandiva::aes_decrypt_ecb(cipher, cipher_len, key, key_len, true, plain_text); },
      std::runtime_error);

  key = "12345678";
  to_encrypt = "New\ntest\nstring";

  key_len = static_cast<int32_t>(strlen(reinterpret_cast<const char*>(key)));
  to_encrypt_len =
      static_cast<int32_t>(strlen(reinterpret_cast<const char*>(to_encrypt)));
  unsigned char cipher_5[64];
  ASSERT_THROW(
      {
        gandiva::aes_encrypt_ecb(to_encrypt, to_encrypt_len, key, key_len, true,
                                 cipher_5);
      },
      std::runtime_error);
  ASSERT_THROW(
      { gandiva::aes_decrypt_ecb(cipher, cipher_len, key, key_len, true, plain_text); },
      std::runtime_error);
}

// NIST SP 800-38A, F.1.1 (ECB-AES128.Encrypt) and F.1.2 (ECB-AES128.Decrypt)
TEST(TestAesEcbEncryptUtils, TestKnownAnswerVectors) {
  const std::string key = FromHex(kKey);
  const std::string plain = FromHex(kPlaintext);
  const std::string expected = FromHex(
      "3ad77bb40d7a3660a89ecaf32466ef97f5d3d58503b9699de785895a96fdbaaf"
      "43b1cd7f598ece23881b00e3ed0306887b0c785e27e8ad3f8223207104725dd4");

  std::string cipher(plain.size() + 16, '\0');
  const int32_t cipher_len =
      gandiva::aes_encrypt_ecb(plain.data(), static_cast<int32_t>(plain.size()),
                               key.data(), static_cast<int32_t>(key.size()), false,
                               reinterpret_cast<unsigned char*>(cipher.data()));
  ASSERT_EQ(cipher_len, static_cast<int32_t>(expected.size()));
  EXPECT_EQ(cipher.substr(0, cipher_len), expected);

  std::string decrypted(expected.size() + 16, '\0');
  const int32_t decrypted_len =
      gandiva::aes_decrypt_ecb(expected.data(), static_cast<int32_t>(expected.size()),
                               key.data(), static_cast<int32_t>(key.size()), false,
                               reinterpret_cast<unsigned char*>(decrypted.data()));
  ASSERT_EQ(decrypted_len, static_cast<int32_t>(plain.size()));
  EXPECT_EQ(decrypted.substr(0, decrypted_len), plain);
}

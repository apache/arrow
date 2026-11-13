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

#include "gandiva/encrypt_utils_cbc.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <string>

// Test PKCS#7 padding with 16-byte key
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

TEST(TestAesCbcEncryptUtils, TestAesEncryptDecryptPkcs7_16) {
  auto* key = "12345678abcdefgh";
  auto* iv = "1234567890123456";
  auto* to_encrypt = "some test string";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[64];

  int32_t cipher_len = gandiva::aes_encrypt_cbc(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, true, cipher);

  unsigned char decrypted[64];
  int32_t decrypted_len =
      gandiva::aes_decrypt_cbc(reinterpret_cast<const char*>(cipher), cipher_len, key,
                               key_len, iv, iv_len, true, decrypted);

  EXPECT_EQ(std::string(to_encrypt, to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted), decrypted_len));
}

// Test PKCS#7 padding with 24-byte key
TEST(TestAesCbcEncryptUtils, TestAesEncryptDecryptPkcs7_24) {
  auto* key = "12345678abcdefgh12345678";
  auto* iv = "1234567890123456";
  auto* to_encrypt = "some\ntest\nstring";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[64];

  int32_t cipher_len = gandiva::aes_encrypt_cbc(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, true, cipher);

  unsigned char decrypted[64];
  int32_t decrypted_len =
      gandiva::aes_decrypt_cbc(reinterpret_cast<const char*>(cipher), cipher_len, key,
                               key_len, iv, iv_len, true, decrypted);

  EXPECT_EQ(std::string(to_encrypt, to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted), decrypted_len));
}

// Test PKCS#7 padding with 32-byte key
TEST(TestAesCbcEncryptUtils, TestAesEncryptDecryptPkcs7_32) {
  auto* key = "12345678abcdefgh12345678abcdefgh";
  auto* iv = "1234567890123456";
  auto* to_encrypt = "New\ntest\nstring";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[64];

  int32_t cipher_len = gandiva::aes_encrypt_cbc(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, true, cipher);

  unsigned char decrypted[64];
  int32_t decrypted_len =
      gandiva::aes_decrypt_cbc(reinterpret_cast<const char*>(cipher), cipher_len, key,
                               key_len, iv, iv_len, true, decrypted);

  EXPECT_EQ(std::string(to_encrypt, to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted), decrypted_len));
}

// Test no-padding mode with block-aligned data (16 bytes)
TEST(TestAesCbcEncryptUtils, TestAesEncryptDecryptNoPadding_16) {
  auto* key = "12345678abcdefgh";
  auto* iv = "1234567890123456";
  auto* to_encrypt = "1234567890123456";  // Exactly 16 bytes

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[64];

  int32_t cipher_len = gandiva::aes_encrypt_cbc(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, false, cipher);

  unsigned char decrypted[64];
  int32_t decrypted_len =
      gandiva::aes_decrypt_cbc(reinterpret_cast<const char*>(cipher), cipher_len, key,
                               key_len, iv, iv_len, false, decrypted);

  EXPECT_EQ(std::string(to_encrypt, to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted), decrypted_len));
}

// Test invalid IV length
TEST(TestAesCbcEncryptUtils, TestInvalidIVLength) {
  auto* key = "12345678abcdefgh";
  auto* iv = "short";  // Too short
  auto* to_encrypt = "test";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[64];

  try {
    gandiva::aes_encrypt_cbc(to_encrypt, to_encrypt_len, key, key_len, iv, iv_len, true,
                             cipher);
    FAIL() << "Expected std::runtime_error";
  } catch (const std::runtime_error& e) {
    EXPECT_THAT(e.what(), testing::HasSubstr("Invalid IV length for AES-CBC"));
  }
}

// Test invalid key length
TEST(TestAesCbcEncryptUtils, TestInvalidKeyLength) {
  auto* key = "short";  // Too short
  auto* iv = "1234567890123456";
  auto* to_encrypt = "test";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[64];

  try {
    gandiva::aes_encrypt_cbc(to_encrypt, to_encrypt_len, key, key_len, iv, iv_len, true,
                             cipher);
    FAIL() << "Expected std::runtime_error";
  } catch (const std::runtime_error& e) {
    EXPECT_THAT(e.what(), testing::HasSubstr("Unsupported key length for AES-CBC"));
  }
}

// NIST SP 800-38A, F.2.1 (CBC-AES128.Encrypt) and F.2.2 (CBC-AES128.Decrypt)
TEST(TestAesCbcEncryptUtils, TestKnownAnswerVectors) {
  const std::string key = FromHex(kKey);
  const std::string iv = FromHex("000102030405060708090a0b0c0d0e0f");
  const std::string plain = FromHex(kPlaintext);
  const std::string expected = FromHex(
      "7649abac8119b246cee98e9b12e9197d5086cb9b507219ee95db113a917678b2"
      "73bed6b8e3c1743b7116e69e222295163ff1caa1681fac09120eca307586e1a7");

  std::string cipher(plain.size() + 16, '\0');
  const int32_t cipher_len = gandiva::aes_encrypt_cbc(
      plain.data(), static_cast<int32_t>(plain.size()), key.data(),
      static_cast<int32_t>(key.size()), iv.data(), static_cast<int32_t>(iv.size()), false,
      reinterpret_cast<unsigned char*>(cipher.data()));
  ASSERT_EQ(cipher_len, static_cast<int32_t>(expected.size()));
  EXPECT_EQ(cipher.substr(0, cipher_len), expected);

  std::string decrypted(expected.size() + 16, '\0');
  const int32_t decrypted_len = gandiva::aes_decrypt_cbc(
      expected.data(), static_cast<int32_t>(expected.size()), key.data(),
      static_cast<int32_t>(key.size()), iv.data(), static_cast<int32_t>(iv.size()), false,
      reinterpret_cast<unsigned char*>(decrypted.data()));
  ASSERT_EQ(decrypted_len, static_cast<int32_t>(plain.size()));
  EXPECT_EQ(decrypted.substr(0, decrypted_len), plain);
}

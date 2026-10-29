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

#include "gandiva/encrypt_utils_gcm.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <stdexcept>
#include <string>

// Test IV-only GCM with 16-byte key
TEST(TestAesGcmEncryptUtils, TestAesEncryptDecryptIvOnly_16) {
  auto* key = "12345678abcdefgh";
  auto* iv = "123456789012";  // 12-byte IV
  auto* to_encrypt = "some test string";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[128];

  int32_t cipher_len = gandiva::aes_encrypt_gcm(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, nullptr, 0, cipher);

  // Ciphertext should be plaintext_len + 16 (tag)
  EXPECT_EQ(cipher_len, to_encrypt_len + 16);

  unsigned char decrypted[128];
  int32_t decrypted_len =
      gandiva::aes_decrypt_gcm(reinterpret_cast<const char*>(cipher), cipher_len, key,
                               key_len, iv, iv_len, nullptr, 0, decrypted);

  EXPECT_EQ(std::string(to_encrypt, to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted), decrypted_len));
}

// Test IV + AAD GCM with 16-byte key
TEST(TestAesGcmEncryptUtils, TestAesEncryptDecryptWithAad_16) {
  auto* key = "12345678abcdefgh";
  auto* iv = "123456789012";
  auto* to_encrypt = "some test string";
  auto* aad = "additional authenticated data";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  auto aad_len = static_cast<int32_t>(strlen(aad));
  unsigned char cipher[128];

  int32_t cipher_len = gandiva::aes_encrypt_gcm(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, aad, aad_len, cipher);

  EXPECT_EQ(cipher_len, to_encrypt_len + 16);

  unsigned char decrypted[128];
  int32_t decrypted_len =
      gandiva::aes_decrypt_gcm(reinterpret_cast<const char*>(cipher), cipher_len, key,
                               key_len, iv, iv_len, aad, aad_len, decrypted);

  EXPECT_EQ(std::string(to_encrypt, to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted), decrypted_len));
}

// Test IV-only GCM with 24-byte key
TEST(TestAesGcmEncryptUtils, TestAesEncryptDecryptIvOnly_24) {
  auto* key = "12345678abcdefgh12345678";
  auto* iv = "123456789012";
  auto* to_encrypt = "test data";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[128];

  int32_t cipher_len = gandiva::aes_encrypt_gcm(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, nullptr, 0, cipher);

  unsigned char decrypted[128];
  int32_t decrypted_len =
      gandiva::aes_decrypt_gcm(reinterpret_cast<const char*>(cipher), cipher_len, key,
                               key_len, iv, iv_len, nullptr, 0, decrypted);

  EXPECT_EQ(std::string(to_encrypt, to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted), decrypted_len));
}

// Test IV-only GCM with 32-byte key
TEST(TestAesGcmEncryptUtils, TestAesEncryptDecryptIvOnly_32) {
  auto* key = "12345678abcdefgh12345678abcdefgh";
  auto* iv = "123456789012";
  auto* to_encrypt = "another test";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[128];

  int32_t cipher_len = gandiva::aes_encrypt_gcm(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, nullptr, 0, cipher);

  unsigned char decrypted[128];
  int32_t decrypted_len =
      gandiva::aes_decrypt_gcm(reinterpret_cast<const char*>(cipher), cipher_len, key,
                               key_len, iv, iv_len, nullptr, 0, decrypted);

  EXPECT_EQ(std::string(to_encrypt, to_encrypt_len),
            std::string(reinterpret_cast<const char*>(decrypted), decrypted_len));
}

// Test tag verification failure
TEST(TestAesGcmEncryptUtils, TestTagVerificationFailure) {
  auto* key = "12345678abcdefgh";
  auto* iv = "123456789012";
  auto* to_encrypt = "some test string";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[128];

  int32_t cipher_len = gandiva::aes_encrypt_gcm(to_encrypt, to_encrypt_len, key, key_len,
                                                iv, iv_len, nullptr, 0, cipher);

  // Corrupt the tag (last byte)
  cipher[cipher_len - 1] ^= 0xFF;

  unsigned char decrypted[128];
  EXPECT_THROW(gandiva::aes_decrypt_gcm(reinterpret_cast<const char*>(cipher), cipher_len,
                                        key, key_len, iv, iv_len, nullptr, 0, decrypted),
               std::runtime_error);
}

// Test invalid IV length
TEST(TestAesGcmEncryptUtils, TestInvalidIvLength) {
  auto* key = "12345678abcdefgh";
  auto* iv = "";  // Empty IV
  auto* to_encrypt = "some test string";

  auto key_len = static_cast<int32_t>(strlen(key));
  auto iv_len = static_cast<int32_t>(strlen(iv));
  auto to_encrypt_len = static_cast<int32_t>(strlen(to_encrypt));
  unsigned char cipher[128];

  EXPECT_THROW(gandiva::aes_encrypt_gcm(to_encrypt, to_encrypt_len, key, key_len, iv,
                                        iv_len, nullptr, 0, cipher),
               std::runtime_error);
}

namespace {

std::string FromHex(const std::string& hex) {
  std::string bytes;
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    bytes.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
  }
  return bytes;
}

// Known-answer vectors from "The Galois/Counter Mode of Operation (GCM)" (McGrew and
// Viega), test cases 4-6. They share key, plaintext and AAD, and differ in IV length.
struct GcmVector {
  const char* iv;
  const char* ciphertext;
  const char* tag;
};

const char kGcmKey[] = "feffe9928665731c6d6a8f9467308308";
const char kGcmPlaintext[] =
    "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a72"
    "1c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39";
const char kGcmAad[] = "feedfacedeadbeeffeedfacedeadbeefabaddad2";

const GcmVector kGcmVectors[] = {
    // 12-byte IV
    {"cafebabefacedbaddecaf888",
     "42831ec2217774244b7221b784d0d49ce3aa212f2c02a4e035c17e2329aca12e"
     "21d514b25466931c7d8f6a5aac84aa051ba30b396a0aac973d58e091",
     "5bc94fbc3221a5db94fae95ae7121a47"},
    // 8-byte IV
    {"cafebabefacedbad",
     "61353b4c2806934a777ff51fa22a4755699b2a714fcdc6f83766e5f97b6c7423"
     "73806900e49f24b22b097544d4896b424989b5e1ebac0f07c23f4598",
     "3612d2e79e3b0785561be14aaca2fccb"},
    // 60-byte IV: only produces the right output if the whole IV is honoured
    {"9313225df88406e555909c5aff5269aa6a7a9538534f7da1e4c303d2a318a728"
     "c3c0c95156809539fcf0e2429a6b525416aedbf5a0de6a57a637b39b",
     "8ce24998625615b603a033aca13fb894be9112a5c3a211a8ba262a3cca7e2ca7"
     "01e4a9a4fba43c90ccdcb281d48c7c6fd62875d2aca417034c34aee5",
     "619cc5aefffe0bfa462af43c1699d050"},
};

}  // namespace

// Interoperability: output must match published GCM vectors for every IV length
TEST(TestAesGcmEncryptUtils, TestKnownAnswerVectors) {
  const std::string key = FromHex(kGcmKey);
  const std::string plaintext = FromHex(kGcmPlaintext);
  const std::string aad = FromHex(kGcmAad);

  for (const auto& vec : kGcmVectors) {
    SCOPED_TRACE(std::string("IV: ") + vec.iv);
    const std::string iv = FromHex(vec.iv);
    const std::string expected = FromHex(vec.ciphertext) + FromHex(vec.tag);

    std::string cipher(plaintext.size() + gandiva::GCM_TAG_LENGTH, '\0');
    int32_t cipher_len = gandiva::aes_encrypt_gcm(
        plaintext.data(), static_cast<int32_t>(plaintext.size()), key.data(),
        static_cast<int32_t>(key.size()), iv.data(), static_cast<int32_t>(iv.size()),
        aad.data(), static_cast<int32_t>(aad.size()),
        reinterpret_cast<unsigned char*>(cipher.data()));
    ASSERT_EQ(cipher_len, static_cast<int32_t>(expected.size()));
    EXPECT_EQ(cipher, expected);

    std::string decrypted(expected.size(), '\0');
    int32_t decrypted_len = gandiva::aes_decrypt_gcm(
        expected.data(), static_cast<int32_t>(expected.size()), key.data(),
        static_cast<int32_t>(key.size()), iv.data(), static_cast<int32_t>(iv.size()),
        aad.data(), static_cast<int32_t>(aad.size()),
        reinterpret_cast<unsigned char*>(decrypted.data()));
    ASSERT_EQ(decrypted_len, static_cast<int32_t>(plaintext.size()));
    EXPECT_EQ(decrypted.substr(0, decrypted_len), plaintext);
  }
}

// Every byte of a long IV must influence the output (regression: IV bytes beyond the
// default 12 were silently ignored)
TEST(TestAesGcmEncryptUtils, TestLongIvIsFullyUsed) {
  const std::string key = FromHex(kGcmKey);
  const std::string plaintext = "some test string";
  std::string iv_a(16, 'a');
  std::string iv_b = iv_a;
  iv_b[15] = 'b';  // differs only past the 12th byte

  auto encrypt = [&](const std::string& iv) {
    std::string cipher(plaintext.size() + gandiva::GCM_TAG_LENGTH, '\0');
    gandiva::aes_encrypt_gcm(plaintext.data(), static_cast<int32_t>(plaintext.size()),
                             key.data(), static_cast<int32_t>(key.size()), iv.data(),
                             static_cast<int32_t>(iv.size()), nullptr, 0,
                             reinterpret_cast<unsigned char*>(cipher.data()));
    return cipher;
  };
  EXPECT_NE(encrypt(iv_a), encrypt(iv_b));
}

// An invalid key length must be reported as an error for both directions
TEST(TestAesGcmEncryptUtils, TestInvalidKeyLength) {
  const std::string iv(12, 'i');
  unsigned char out[64];
  EXPECT_THROW(
      gandiva::aes_encrypt_gcm("data", 4, "short", 5, iv.data(), 12, nullptr, 0, out),
      std::runtime_error);
  EXPECT_THROW(gandiva::aes_decrypt_gcm(std::string(32, 'x').data(), 32, "short", 5,
                                        iv.data(), 12, nullptr, 0, out),
               std::runtime_error);
}

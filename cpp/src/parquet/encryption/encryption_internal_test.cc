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

#include "parquet/encryption/encryption_internal.h"

namespace parquet::encryption::test {

static const int kKeyLength = 16;
static const std::string kKey = "1234567890123450";
static const std::string kAad = "abcdefgh";
static const std::string kPlaintext =
    "Apache Parquet is an open source, column-oriented data file format designed for "
    "efficient data storage and retrieval";

class TestAesEncryption : public ::testing::Test {
 protected:
  static void EncryptionRoundTrip(ParquetCipher::type cipher_type, bool write_length) {
    bool metadata = false;

    AesEncryptor encryptor(cipher_type, kKeyLength, metadata, write_length);

    int expected_ciphertext_len =
        static_cast<int>(kPlaintext.size()) + encryptor.CiphertextSizeDelta();
    std::string ciphertext(expected_ciphertext_len, '\0');

    int ciphertext_length = encryptor.Encrypt(
        str2bytes(kPlaintext), static_cast<int>(kPlaintext.size()), str2bytes(kKey),
        static_cast<int>(kKey.size()), str2bytes(kAad), static_cast<int>(kAad.size()),
        reinterpret_cast<uint8_t*>(&ciphertext[0]));

    ASSERT_EQ(ciphertext_length, expected_ciphertext_len);

    AesDecryptor decryptor(cipher_type, kKeyLength, metadata, write_length);

    int expected_plaintext_length = ciphertext_length - decryptor.CiphertextSizeDelta();
    std::string decrypted_text(expected_plaintext_length, '\0');

    int plaintext_length = decryptor.Decrypt(
        str2bytes(ciphertext), ciphertext_length, str2bytes(kKey),
        static_cast<int>(kKey.size()), str2bytes(kAad), static_cast<int>(kAad.size()),
        reinterpret_cast<uint8_t*>(&decrypted_text[0]));

    ASSERT_EQ(plaintext_length, static_cast<int>(kPlaintext.size()));
    ASSERT_EQ(plaintext_length, expected_plaintext_length);
    ASSERT_EQ(decrypted_text, kPlaintext);
  }

  static void DecryptInvalidCiphertext(ParquetCipher::type cipher_type) {
    bool metadata = false;
    bool write_length = true;

    AesDecryptor decryptor(cipher_type, kKeyLength, metadata, write_length);

    // Create ciphertext of all zeros, so the ciphertext length will be read as zero
    const int ciphertext_length = 100;
    std::string ciphertext(ciphertext_length, '\0');

    int expected_plaintext_length = ciphertext_length - decryptor.CiphertextSizeDelta();
    std::string decrypted_text(expected_plaintext_length, '\0');

    EXPECT_THROW(decryptor.Decrypt(str2bytes(ciphertext), 0, str2bytes(kKey),
                                   static_cast<int>(kKey.size()), str2bytes(kAad),
                                   static_cast<int>(kAad.size()),
                                   reinterpret_cast<uint8_t*>(&decrypted_text[0])),
                 ParquetException);
  }

  static void DecryptCiphertextBufferTooSmall(ParquetCipher::type cipher_type) {
    bool metadata = false;
    bool write_length = true;

    AesEncryptor encryptor(cipher_type, kKeyLength, metadata, write_length);

    int expected_ciphertext_len =
        static_cast<int>(kPlaintext.size()) + encryptor.CiphertextSizeDelta();
    std::string ciphertext(expected_ciphertext_len, '\0');

    int ciphertext_length = encryptor.Encrypt(
        str2bytes(kPlaintext), static_cast<int>(kPlaintext.size()), str2bytes(kKey),
        static_cast<int>(kKey.size()), str2bytes(kAad), static_cast<int>(kAad.size()),
        reinterpret_cast<uint8_t*>(&ciphertext[0]));

    AesDecryptor decryptor(cipher_type, kKeyLength, metadata, write_length);

    int expected_plaintext_length = ciphertext_length - decryptor.CiphertextSizeDelta();
    std::string decrypted_text(expected_plaintext_length, '\0');

    EXPECT_THROW(decryptor.Decrypt(str2bytes(ciphertext), ciphertext_length - 1,
                                   str2bytes(kKey), static_cast<int>(kKey.size()),
                                   str2bytes(kAad), static_cast<int>(kAad.size()),
                                   reinterpret_cast<uint8_t*>(&decrypted_text[0])),
                 ParquetException);
  }
};

TEST_F(TestAesEncryption, AesGcmRoundTrip) {
  EncryptionRoundTrip(ParquetCipher::AES_GCM_V1, /*write_length=*/true);
  EncryptionRoundTrip(ParquetCipher::AES_GCM_V1, /*write_length=*/false);
}

TEST_F(TestAesEncryption, AesGcmCtrRoundTrip) {
  EncryptionRoundTrip(ParquetCipher::AES_GCM_CTR_V1, /*write_length=*/true);
  EncryptionRoundTrip(ParquetCipher::AES_GCM_CTR_V1, /*write_length=*/false);
}

TEST_F(TestAesEncryption, AesGcmDecryptInvalidCiphertext) {
  DecryptInvalidCiphertext(ParquetCipher::AES_GCM_V1);
}

TEST_F(TestAesEncryption, AesGcmCtrDecryptInvalidCiphertext) {
  DecryptInvalidCiphertext(ParquetCipher::AES_GCM_CTR_V1);
}

TEST_F(TestAesEncryption, AesGcmDecryptCiphertextBufferTooSmall) {
  DecryptCiphertextBufferTooSmall(ParquetCipher::AES_GCM_V1);
}

TEST_F(TestAesEncryption, AesGcmCtrDecryptCiphertextBufferTooSmall) {
  DecryptCiphertextBufferTooSmall(ParquetCipher::AES_GCM_CTR_V1);
}

}  // namespace parquet::encryption::test

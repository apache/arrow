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

class TestAesEncryption : public ::testing::Test {
 protected:
  static void EncryptionRoundTrip(ParquetCipher::type cipher_type, bool write_length) {
    int key_length = 16;
    std::string key = "1234567890123450";
    std::string aad = "abcdefgh";
    bool metadata = false;
    std::string plaintext =
        "Apache Parquet is an open source, column-oriented data file format designed for "
        "efficient data storage and retrieval";

    AesEncryptor encryptor(cipher_type, key_length, metadata, write_length);

    int expected_ciphertext_len =
        static_cast<int>(plaintext.size()) + encryptor.CiphertextSizeDelta();
    std::string ciphertext(expected_ciphertext_len, '\0');

    int ciphertext_length = encryptor.Encrypt(
        str2bytes(plaintext), static_cast<int>(plaintext.size()), str2bytes(key),
        static_cast<int>(key.size()), str2bytes(aad), static_cast<int>(aad.size()),
        reinterpret_cast<uint8_t*>(&ciphertext[0]));

    ASSERT_EQ(ciphertext_length, expected_ciphertext_len);

    AesDecryptor decryptor(cipher_type, key_length, metadata, write_length);

    int expected_plaintext_length = ciphertext_length - decryptor.CiphertextSizeDelta();
    std::string decrypted_text(expected_plaintext_length, '\0');

    int plaintext_length = decryptor.Decrypt(
        str2bytes(ciphertext), ciphertext_length, str2bytes(key),
        static_cast<int>(key.size()), str2bytes(aad), static_cast<int>(aad.size()),
        reinterpret_cast<uint8_t*>(&decrypted_text[0]));

    ASSERT_EQ(plaintext_length, static_cast<int>(plaintext.size()));
    ASSERT_EQ(plaintext_length, expected_plaintext_length);
    ASSERT_EQ(decrypted_text, plaintext);
  }

  static void DecryptInvalidCiphertext(ParquetCipher::type cipher_type) {
    int key_length = 16;
    std::string key = "1234567890123450";
    std::string aad = "abcdefgh";
    bool metadata = false;
    bool write_length = true;

    AesDecryptor decryptor(cipher_type, key_length, metadata, write_length);

    // Create ciphertext of all zeros, so the ciphertext length will be read as zero
    const int ciphertext_length = 100;
    std::string ciphertext(ciphertext_length, '\0');

    int expected_plaintext_length = ciphertext_length - decryptor.CiphertextSizeDelta();
    std::string decrypted_text(expected_plaintext_length, '\0');

    EXPECT_THROW(decryptor.Decrypt(str2bytes(ciphertext), 0, str2bytes(key),
                                   static_cast<int>(key.size()), str2bytes(aad),
                                   static_cast<int>(aad.size()),
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

}  // namespace parquet::encryption::test

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
  void SetUp() override {
    key_length_ = 16;
    key_ = "1234567890123450";
    aad_ = "abcdefgh";
    plain_text_ =
        "Apache Parquet is an open source, column-oriented data file format designed for "
        "efficient data storage and retrieval";
  }

  void EncryptionRoundTrip(ParquetCipher::type cipher_type, bool write_length) {
    bool metadata = false;

    AesEncryptor encryptor(cipher_type, key_length_, metadata, write_length);

    int expected_ciphertext_len =
        static_cast<int>(plain_text_.size()) + encryptor.CiphertextSizeDelta();
    std::vector<uint8_t> ciphertext(expected_ciphertext_len, '\0');

    int ciphertext_length =
        encryptor.Encrypt(str2bytes(plain_text_), static_cast<int>(plain_text_.size()),
                          str2bytes(key_), static_cast<int>(key_.size()), str2bytes(aad_),
                          static_cast<int>(aad_.size()), ciphertext.data());

    ASSERT_EQ(ciphertext_length, expected_ciphertext_len);

    AesDecryptor decryptor(cipher_type, key_length_, metadata, write_length);

    int expected_plaintext_length = decryptor.PlaintextLength(ciphertext_length);
    std::vector<uint8_t> decrypted_text(expected_plaintext_length, '\0');

    int plaintext_length =
        decryptor.Decrypt(ciphertext, str2span(key_), str2span(aad_), decrypted_text);

    std::string decrypted_text_str(decrypted_text.begin(), decrypted_text.end());

    ASSERT_EQ(plaintext_length, static_cast<int>(plain_text_.size()));
    ASSERT_EQ(plaintext_length, expected_plaintext_length);
    ASSERT_EQ(decrypted_text_str, plain_text_);
  }

  void DecryptInvalidCiphertext(ParquetCipher::type cipher_type) {
    bool metadata = false;
    bool write_length = true;

    AesDecryptor decryptor(cipher_type, key_length_, metadata, write_length);

    // Create ciphertext of all zeros, so the ciphertext length will be read as zero
    const int ciphertext_length = 100;
    std::vector<uint8_t> ciphertext(ciphertext_length, '\0');

    int expected_plaintext_length = decryptor.PlaintextLength(ciphertext_length);
    std::vector<uint8_t> decrypted_text(expected_plaintext_length, '\0');

    EXPECT_THROW(
        decryptor.Decrypt(ciphertext, str2span(key_), str2span(aad_), decrypted_text),
        ParquetException);
  }

  void DecryptCiphertextBufferTooSmall(ParquetCipher::type cipher_type) {
    bool metadata = false;
    bool write_length = true;

    AesEncryptor encryptor(cipher_type, key_length_, metadata, write_length);

    int expected_ciphertext_len =
        static_cast<int>(plain_text_.size()) + encryptor.CiphertextSizeDelta();
    std::vector<uint8_t> ciphertext(expected_ciphertext_len, '\0');

    int ciphertext_length =
        encryptor.Encrypt(str2bytes(plain_text_), static_cast<int>(plain_text_.size()),
                          str2bytes(key_), static_cast<int>(key_.size()), str2bytes(aad_),
                          static_cast<int>(aad_.size()), ciphertext.data());

    AesDecryptor decryptor(cipher_type, key_length_, metadata, write_length);

    int expected_plaintext_length = decryptor.PlaintextLength(ciphertext_length);
    std::vector<uint8_t> decrypted_text(expected_plaintext_length, '\0');

    ::arrow::util::span<uint8_t> truncated_ciphertext(ciphertext.data(),
                                                      ciphertext_length - 1);
    EXPECT_THROW(decryptor.Decrypt(truncated_ciphertext, str2span(key_), str2span(aad_),
                                   decrypted_text),
                 ParquetException);
  }

 private:
  int key_length_ = 0;
  std::string key_;
  std::string aad_;
  std::string plain_text_;
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

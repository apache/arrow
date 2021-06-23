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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "parquet/properties.h"
#include "parquet/types.h"

using parquet::ParquetCipher;

namespace parquet {
namespace encryption {

constexpr int kGcmTagLength = 16;
constexpr int kNonceLength = 12;

// Module types
constexpr int8_t kFooter = 0;
constexpr int8_t kColumnMetaData = 1;
constexpr int8_t kDataPage = 2;
constexpr int8_t kDictionaryPage = 3;
constexpr int8_t kDataPageHeader = 4;
constexpr int8_t kDictionaryPageHeader = 5;
constexpr int8_t kColumnIndex = 6;
constexpr int8_t kOffsetIndex = 7;

/// Performs AES encryption operations with GCM or CTR ciphers.
class AesEncryptor {
 public:
  /// Can serve one key length only. Possible values: 16, 24, 32 bytes.
  explicit AesEncryptor(ParquetCipher::type alg_id, int key_len, bool metadata);

  static AesEncryptor* Make(ParquetCipher::type alg_id, int key_len, bool metadata,
                            std::vector<AesEncryptor*>* all_encryptors);

  ~AesEncryptor();

  /// Size difference between plaintext and ciphertext, for this cipher.
  int CiphertextSizeDelta();

  /// Encrypts plaintext with the key and aad. Key length is passed only for validation.
  /// If different from value in constructor, exception will be thrown.
  int Encrypt(const uint8_t* plaintext, int plaintext_len, const uint8_t* key,
              int key_len, const uint8_t* aad, int aad_len, uint8_t* ciphertext);

  /// Encrypts plaintext footer, in order to compute footer signature (tag).
  int SignedFooterEncrypt(const uint8_t* footer, int footer_len, const uint8_t* key,
                          int key_len, const uint8_t* aad, int aad_len,
                          const uint8_t* nonce, uint8_t* encrypted_footer);

  void WipeOut();

 private:
  // PIMPL Idiom
  class AesEncryptorImpl;
  std::unique_ptr<AesEncryptorImpl> impl_;
};

/// Performs AES decryption operations with GCM or CTR ciphers.
class AesDecryptor {
 public:
  /// Can serve one key length only. Possible values: 16, 24, 32 bytes.
  explicit AesDecryptor(ParquetCipher::type alg_id, int key_len, bool metadata);

  static AesDecryptor* Make(ParquetCipher::type alg_id, int key_len, bool metadata,
                            std::vector<AesDecryptor*>* all_decryptors);

  ~AesDecryptor();
  void WipeOut();

  /// Size difference between plaintext and ciphertext, for this cipher.
  int CiphertextSizeDelta();

  /// Decrypts ciphertext with the key and aad. Key length is passed only for
  /// validation. If different from value in constructor, exception will be thrown.
  int Decrypt(const uint8_t* ciphertext, int ciphertext_len, const uint8_t* key,
              int key_len, const uint8_t* aad, int aad_len, uint8_t* plaintext);

 private:
  // PIMPL Idiom
  class AesDecryptorImpl;
  std::unique_ptr<AesDecryptorImpl> impl_;
};

std::string CreateModuleAad(const std::string& file_aad, int8_t module_type,
                            int16_t row_group_ordinal, int16_t column_ordinal,
                            int16_t page_ordinal);

std::string CreateFooterAad(const std::string& aad_prefix_bytes);

// Update last two bytes of page (or page header) module AAD
void QuickUpdatePageAad(const std::string& AAD, int16_t new_page_ordinal);

// Wraps OpenSSL RAND_bytes function
void RandBytes(unsigned char* buf, int num);

}  // namespace encryption
}  // namespace parquet

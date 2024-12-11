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

#include "arrow/util/span.h"
#include "parquet/properties.h"
#include "parquet/types.h"

using parquet::ParquetCipher;

namespace parquet::encryption {

constexpr int32_t kGcmTagLength = 16;
constexpr int32_t kNonceLength = 12;

// Module types
constexpr int8_t kFooter = 0;
constexpr int8_t kColumnMetaData = 1;
constexpr int8_t kDataPage = 2;
constexpr int8_t kDictionaryPage = 3;
constexpr int8_t kDataPageHeader = 4;
constexpr int8_t kDictionaryPageHeader = 5;
constexpr int8_t kColumnIndex = 6;
constexpr int8_t kOffsetIndex = 7;
constexpr int8_t kBloomFilterHeader = 8;
constexpr int8_t kBloomFilterBitset = 9;

/// Performs AES encryption operations with GCM or CTR ciphers.
class PARQUET_EXPORT AesEncryptor {
 public:
  /// Can serve one key length only. Possible values: 16, 24, 32 bytes.
  /// If write_length is true, prepend ciphertext length to the ciphertext
  explicit AesEncryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                        bool write_length = true);

  static std::unique_ptr<AesEncryptor> Make(ParquetCipher::type alg_id, int32_t key_len,
                                            bool metadata);

  static std::unique_ptr<AesEncryptor> Make(ParquetCipher::type alg_id, int32_t key_len,
                                            bool metadata, bool write_length);

  ~AesEncryptor();

  /// The size of the ciphertext, for this cipher and the specified plaintext length.
  [[nodiscard]] int32_t CiphertextLength(int64_t plaintext_len) const;

  /// Encrypts plaintext with the key and aad. Key length is passed only for validation.
  /// If different from value in constructor, exception will be thrown.
  int32_t Encrypt(::arrow::util::span<const uint8_t> plaintext,
                  ::arrow::util::span<const uint8_t> key,
                  ::arrow::util::span<const uint8_t> aad,
                  ::arrow::util::span<uint8_t> ciphertext);

  /// Encrypts plaintext footer, in order to compute footer signature (tag).
  int32_t SignedFooterEncrypt(::arrow::util::span<const uint8_t> footer,
                              ::arrow::util::span<const uint8_t> key,
                              ::arrow::util::span<const uint8_t> aad,
                              ::arrow::util::span<const uint8_t> nonce,
                              ::arrow::util::span<uint8_t> encrypted_footer);

 private:
  // PIMPL Idiom
  class AesEncryptorImpl;
  std::unique_ptr<AesEncryptorImpl> impl_;
};

/// Performs AES decryption operations with GCM or CTR ciphers.
class PARQUET_EXPORT AesDecryptor {
 public:
  /// Can serve one key length only. Possible values: 16, 24, 32 bytes.
  /// If contains_length is true, expect ciphertext length prepended to the ciphertext
  explicit AesDecryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                        bool contains_length = true);

  /// \brief Factory function to create an AesDecryptor
  ///
  /// \param alg_id the encryption algorithm to use
  /// \param key_len key length. Possible values: 16, 24, 32 bytes.
  /// \param metadata if true then this is a metadata decryptor
  /// out when decryption is finished
  /// \return shared pointer to a new AesDecryptor
  static std::shared_ptr<AesDecryptor> Make(
      ParquetCipher::type alg_id, int32_t key_len, bool metadata);

  ~AesDecryptor();

  /// The size of the plaintext, for this cipher and the specified ciphertext length.
  [[nodiscard]] int32_t PlaintextLength(int32_t ciphertext_len) const;

  /// The size of the ciphertext, for this cipher and the specified plaintext length.
  [[nodiscard]] int32_t CiphertextLength(int32_t plaintext_len) const;

  /// Decrypts ciphertext with the key and aad. Key length is passed only for
  /// validation. If different from value in constructor, exception will be thrown.
  /// The caller is responsible for ensuring that the plaintext buffer is at least as
  /// large as PlaintextLength(ciphertext_len).
  int32_t Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                  ::arrow::util::span<const uint8_t> key,
                  ::arrow::util::span<const uint8_t> aad,
                  ::arrow::util::span<uint8_t> plaintext);

 private:
  // PIMPL Idiom
  class AesDecryptorImpl;
  std::unique_ptr<AesDecryptorImpl> impl_;
};

std::string CreateModuleAad(const std::string& file_aad, int8_t module_type,
                            int16_t row_group_ordinal, int16_t column_ordinal,
                            int32_t page_ordinal);

std::string CreateFooterAad(const std::string& aad_prefix_bytes);

// Update last two bytes of page (or page header) module AAD
void QuickUpdatePageAad(int32_t new_page_ordinal, std::string* AAD);

// Wraps OpenSSL RAND_bytes function
void RandBytes(unsigned char* buf, size_t num);

// Ensure OpenSSL is initialized.
//
// This is only necessary in specific situations since OpenSSL otherwise
// initializes itself automatically. For example, under Valgrind, a memory
// leak will be reported if OpenSSL is initialized for the first time from
// a worker thread; calling this function from the main thread prevents this.
void EnsureBackendInitialized();

}  // namespace parquet::encryption

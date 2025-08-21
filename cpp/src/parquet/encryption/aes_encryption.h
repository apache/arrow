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

#include "arrow/util/span.h"
#include "parquet/types.h"

using parquet::ParquetCipher;

namespace parquet::encryption {

/// Performs AES encryption operations with GCM or CTR ciphers.
class PARQUET_EXPORT AesEncryptor {
 public:
  /// Can serve one key length only. Possible values: 16, 24, 32 bytes.
  /// If write_length is true, prepend ciphertext length to the ciphertext
  explicit AesEncryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                        bool write_length = true);

  static std::unique_ptr<AesEncryptor> Make(ParquetCipher::type alg_id, int32_t key_len,
                                            bool metadata, bool write_length = true);

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
  /// \brief Construct an AesDecryptor
  ///
  /// \param alg_id the encryption algorithm to use
  /// \param key_len key length. Possible values: 16, 24, 32 bytes.
  /// \param metadata if true then this is a metadata decryptor
  /// \param contains_length if true, expect ciphertext length prepended to the ciphertext
  explicit AesDecryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                        bool contains_length = true);

  static std::unique_ptr<AesDecryptor> Make(ParquetCipher::type alg_id, int32_t key_len,
                                            bool metadata);

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

}  // namespace parquet::encryption

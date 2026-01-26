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

#include "parquet/encryption/encoding_properties.h"
#include "parquet/platform.h"

namespace parquet::encryption {

class PARQUET_EXPORT EncryptorInterface {
 public:
  virtual ~EncryptorInterface() = default;

  /// Signal whether the encryptor can calculate a valid ciphertext length before
  /// performing encryption or not. If false, a proper sized buffer cannot be
  /// allocated before calling the Encrypt method, and Arrow must use this
  /// encryptor's EncryptWithManagedBuffer method instead of Encrypt.
  [[nodiscard]] virtual bool CanCalculateCiphertextLength() const = 0;

  /// Calculate the size of the ciphertext for a given plaintext length.
  [[nodiscard]] virtual int32_t CiphertextLength(int64_t plaintext_len) const = 0;

  /// Encrypt the plaintext and leave the results in the ciphertext buffer.
  /// Most implementations will require the key and aad to be provided, but it is
  /// up to each encryptor whether to use them or not.
  virtual int32_t Encrypt(::arrow::util::span<const uint8_t> plaintext,
                          ::arrow::util::span<const uint8_t> key,
                          ::arrow::util::span<const uint8_t> aad,
                          ::arrow::util::span<uint8_t> ciphertext) = 0;

  /// Encrypt the plaintext and leave the results in the ciphertext buffer.
  /// The buffer will be resized to the appropriate size by the encryptor during
  /// encryption. This method is used when the encryptor cannot calculate the
  /// ciphertext length before encryption.
  virtual int32_t EncryptWithManagedBuffer(::arrow::util::span<const uint8_t> plaintext,
                                           ::arrow::ResizableBuffer* ciphertext) = 0;

  /// Some Encryptors may need to understand the page encoding before the encryption
  /// process. This method will be called from ColumnWriter before invoking the
  /// Encrypt method.
  virtual void UpdateEncodingProperties(
      std::unique_ptr<EncodingProperties> encoding_properties) {}

  /// After the column_writer writes a dictionary or a data page, this method will be
  /// called so that each encryptor can provide any encryptor-specific column
  /// metadata that should be stored in the Parquet file. The keys and values are
  /// added to the column metadata, any conflicting key and value pairs are
  /// overwritten. There is no need to clear the metadata after the call.
  virtual std::shared_ptr<KeyValueMetadata> GetKeyValueMetadata(int8_t module_type) {
    return nullptr;
  }

  /// Encrypt footer metadata for signature verification purposes only.
  /// This method is used specifically for footer signature verification in encrypted
  /// Parquet files with plaintext footers. It encrypts the footer metadata using
  /// the provided key, AAD, and nonce to generate an authentication tag that can
  /// be compared against a stored signature.
  virtual int32_t SignedFooterEncrypt(::arrow::util::span<const uint8_t> footer,
                                      ::arrow::util::span<const uint8_t> key,
                                      ::arrow::util::span<const uint8_t> aad,
                                      ::arrow::util::span<const uint8_t> nonce,
                                      ::arrow::util::span<uint8_t> encrypted_footer) = 0;
};

}  // namespace parquet::encryption

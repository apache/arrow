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

#include <openssl/evp.h>
#include <memory>

#include "arrow/util/span.h"
#include "parquet/encryption/decryptor_interface.h"
#include "parquet/encryption/encryptor_interface.h"
#include "parquet/exception.h"
#include "parquet/types.h"

using parquet::ParquetCipher;

namespace parquet::encryption {

class AesCryptoContext {
 public:
  AesCryptoContext(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                   bool include_length);

  virtual ~AesCryptoContext() = default;

 protected:
  static void DeleteCipherContext(EVP_CIPHER_CTX* ctx) { EVP_CIPHER_CTX_free(ctx); }

  using CipherContext = std::unique_ptr<EVP_CIPHER_CTX, decltype(&DeleteCipherContext)>;

  static CipherContext NewCipherContext() {
    auto ctx = CipherContext(EVP_CIPHER_CTX_new(), DeleteCipherContext);
    if (!ctx) {
      throw ParquetException("Couldn't init cipher context");
    }
    return ctx;
  }

  int32_t aes_mode_;
  int32_t key_length_;
  int32_t ciphertext_size_delta_;
  int32_t length_buffer_length_;
};

/// Performs AES encryption operations with GCM or CTR ciphers.
class PARQUET_EXPORT AesEncryptor : public AesCryptoContext, public EncryptorInterface {
 public:
  /// Can serve one key length only. Possible values: 16, 24, 32 bytes.
  /// If write_length is true, prepend ciphertext length to the ciphertext
  explicit AesEncryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                        bool write_length = true);

  static std::unique_ptr<AesEncryptor> Make(ParquetCipher::type alg_id, int32_t key_len,
                                            bool metadata, bool write_length = true);

  ~AesEncryptor() = default;

  /// Start of Encryptor Interface methods.

  /// Signal whether the encryptor can calculate a valid ciphertext length before
  /// performing encryption.
  [[nodiscard]] bool CanCalculateCiphertextLength() const override { return true; }

  /// The size of the ciphertext, for this cipher and the specified plaintext length.
  [[nodiscard]] int32_t CiphertextLength(int64_t plaintext_len) const override;

  /// Encrypts plaintext with the key and aad. Key length is passed only for validation.
  /// If different from value in constructor, exception will be thrown.
  int32_t Encrypt(::arrow::util::span<const uint8_t> plaintext,
                  ::arrow::util::span<const uint8_t> key,
                  ::arrow::util::span<const uint8_t> aad,
                  ::arrow::util::span<uint8_t> ciphertext) override;

  /// Encrypt the plaintext and leave the results in the ciphertext buffer. This method is
  /// not supported as we can calculate the ciphertext length before encryption.
  int32_t EncryptWithManagedBuffer(::arrow::util::span<const uint8_t> plaintext,
                                   ::arrow::ResizableBuffer* ciphertext) override {
    throw ParquetException(
        "EncryptWithManagedBuffer is not supported in AesEncryptor, use Encrypt instead");
  }

  /// Encrypts plaintext footer, in order to compute footer signature (tag).
  int32_t SignedFooterEncrypt(::arrow::util::span<const uint8_t> footer,
                              ::arrow::util::span<const uint8_t> key,
                              ::arrow::util::span<const uint8_t> aad,
                              ::arrow::util::span<const uint8_t> nonce,
                              ::arrow::util::span<uint8_t> encrypted_footer) override;

  /// End of Encryptor Interface methods.

 private:
  [[nodiscard]] CipherContext MakeCipherContext() const;

  int32_t GcmEncrypt(::arrow::util::span<const uint8_t> plaintext,
                     ::arrow::util::span<const uint8_t> key,
                     ::arrow::util::span<const uint8_t> nonce,
                     ::arrow::util::span<const uint8_t> aad,
                     ::arrow::util::span<uint8_t> ciphertext);

  int32_t CtrEncrypt(::arrow::util::span<const uint8_t> plaintext,
                     ::arrow::util::span<const uint8_t> key,
                     ::arrow::util::span<const uint8_t> nonce,
                     ::arrow::util::span<uint8_t> ciphertext);
};

// AesEncryptor supports only three key lengths: 16, 24, 32 bytes, so at most there
// could be up to three types of meta_encryptors and data_encryptors. This factory
// uses a cache to store the encryptors for the different key lengths.
class AesEncryptorFactory {
 public:
  AesEncryptor* GetMetaAesEncryptor(ParquetCipher::type alg_id, size_t key_size);
  AesEncryptor* GetDataAesEncryptor(ParquetCipher::type alg_id, size_t key_size);

 private:
  /// Build a cache key including algorithm id, key length, and metadata flag.
  static uint64_t MakeCacheKey(ParquetCipher::type alg_id, int32_t key_len,
                               bool metadata);

  std::unordered_map<uint64_t, std::unique_ptr<AesEncryptor>> encryptor_cache_;
};

/// Performs AES decryption operations with GCM or CTR ciphers.
class PARQUET_EXPORT AesDecryptor : public AesCryptoContext, public DecryptorInterface {
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

  ~AesDecryptor() = default;

  /// Start of Decryptor Interface methods.

  /// Signal whether the decryptor can calculate a valid plaintext or ciphertext
  /// length before performing decryption or not. If false, a proper sized buffer
  /// cannot be allocated before calling the Decrypt method, and Arrow must use this
  /// decryptor's DecryptWithManagedBuffer method instead of Decrypt.
  [[nodiscard]] bool CanCalculateLengths() const override { return true; }

  /// The size of the plaintext, for this cipher and the specified ciphertext length.
  [[nodiscard]] int32_t PlaintextLength(int32_t ciphertext_len) const override;

  /// The size of the ciphertext, for this cipher and the specified plaintext length.
  [[nodiscard]] int32_t CiphertextLength(int32_t plaintext_len) const override;

  /// Decrypts ciphertext with the key and aad. Key length is passed only for
  /// validation. If different from value in constructor, exception will be thrown.
  /// The caller is responsible for ensuring that the plaintext buffer is at least as
  /// large as PlaintextLength(ciphertext_len).
  int32_t Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                  ::arrow::util::span<const uint8_t> key,
                  ::arrow::util::span<const uint8_t> aad,
                  ::arrow::util::span<uint8_t> plaintext) override;

  /// Decrypt the ciphertext and leave the results in the plaintext buffer. This
  /// method is not supported as we can calculate the plaintext length before
  /// decryption.
  int32_t DecryptWithManagedBuffer(::arrow::util::span<const uint8_t> ciphertext,
                                   ::arrow::ResizableBuffer* plaintext) override {
    throw ParquetException(
        "DecryptWithManagedBuffer is not supported in AesDecryptor, use Decrypt instead");
  }

  /// End of Decryptor Interface methods.

 private:
  [[nodiscard]] CipherContext MakeCipherContext() const;

  /// Get the actual ciphertext length, inclusive of the length buffer length,
  /// and validate that the provided buffer size is large enough.
  [[nodiscard]] int32_t GetCiphertextLength(
      ::arrow::util::span<const uint8_t> ciphertext) const;

  int32_t GcmDecrypt(::arrow::util::span<const uint8_t> ciphertext,
                     ::arrow::util::span<const uint8_t> key,
                     ::arrow::util::span<const uint8_t> aad,
                     ::arrow::util::span<uint8_t> plaintext);

  int32_t CtrDecrypt(::arrow::util::span<const uint8_t> ciphertext,
                     ::arrow::util::span<const uint8_t> key,
                     ::arrow::util::span<uint8_t> plaintext);
};

}  // namespace parquet::encryption

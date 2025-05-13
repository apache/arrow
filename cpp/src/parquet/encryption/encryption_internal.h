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
#include <openssl/evp.h>
#include <string>
#include <vector>

#include "arrow/util/span.h"
#include "parquet/encryption/openssl_internal.h"
#include "parquet/properties.h"
#include "parquet/types.h"

using parquet::ParquetCipher;
using ::arrow::util::span;

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

// AesCryptoContext
constexpr int32_t kGcmMode = 0;
constexpr int32_t kCtrMode = 1;
constexpr int32_t kCtrIvLength = 16;
constexpr int32_t kBufferSizeLength = 4;


class PARQUET_EXPORT EncryptorInterface {
  public:
    virtual ~EncryptorInterface() = default;
    /// The size of the ciphertext, for this cipher and the specified plaintext length.
    [[nodiscard]] virtual int32_t CiphertextLength(int64_t plaintext_len) const = 0;

    /// Encrypts plaintext with the key and aad. Key length is passed only for validation.
    /// If different from value in constructor, exception will be thrown.
    virtual int32_t Encrypt(span<const uint8_t> plaintext, span<const uint8_t> key,
                            span<const uint8_t> aad, span<uint8_t> ciphertext) = 0;

      /// Encrypts plaintext footer, in order to compute footer signature (tag).
    virtual int32_t SignedFooterEncrypt(span<const uint8_t> footer, span<const uint8_t> key,
                                        span<const uint8_t> aad, span<const uint8_t> nonce,
                                        span<uint8_t> encrypted_footer) = 0;
};

class AesCryptoContext {
  public:
   AesCryptoContext(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                    bool include_length) {
     openssl::EnsureInitialized();
 
     length_buffer_length_ = include_length ? kBufferSizeLength : 0;
     ciphertext_size_delta_ = length_buffer_length_ + kNonceLength;
 
     if (ParquetCipher::AES_GCM_V1 != alg_id && ParquetCipher::AES_GCM_CTR_V1 != alg_id) {
       std::stringstream ss;
       ss << "Crypto algorithm " << alg_id << " is not supported";
       throw ParquetException(ss.str());
     }
     if (16 != key_len && 24 != key_len && 32 != key_len) {
       std::stringstream ss;
       ss << "Wrong key length: " << key_len;
       throw ParquetException(ss.str());
     }
 
     if (metadata || (ParquetCipher::AES_GCM_V1 == alg_id)) {
       aes_mode_ = kGcmMode;
       ciphertext_size_delta_ += kGcmTagLength;
     } else {
       aes_mode_ = kCtrMode;
     }
 
     key_length_ = key_len;
   }
 
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

class PARQUET_EXPORT AesEncryptorImpl : public AesCryptoContext, public EncryptorInterface {
  public:
    explicit AesEncryptorImpl(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                              bool write_length);
    
    static std::unique_ptr<AesEncryptorImpl> Make(ParquetCipher::type alg_id, int32_t key_len,
                                                  bool metadata, bool write_length = true);
 
    int32_t Encrypt(span<const uint8_t> plaintext, span<const uint8_t> key,
                    span<const uint8_t> aad, span<uint8_t> ciphertext) override;
 
    int32_t SignedFooterEncrypt(span<const uint8_t> footer, span<const uint8_t> key,
                                span<const uint8_t> aad, span<const uint8_t> nonce,
                                span<uint8_t> encrypted_footer) override;
 
   [[nodiscard]] int32_t CiphertextLength(int64_t plaintext_len) const override {
     if (plaintext_len < 0) {
       std::stringstream ss;
       ss << "Negative plaintext length " << plaintext_len;
       throw ParquetException(ss.str());
     } else if (plaintext_len >
                std::numeric_limits<int32_t>::max() - ciphertext_size_delta_) {
       std::stringstream ss;
       ss << "Plaintext length " << plaintext_len << " plus ciphertext size delta "
          << ciphertext_size_delta_ << " overflows int32";
       throw ParquetException(ss.str());
     }
 
     return static_cast<int32_t>(plaintext_len + ciphertext_size_delta_);
   }
 
  private:
   [[nodiscard]] CipherContext MakeCipherContext() const;
 
   int32_t GcmEncrypt(span<const uint8_t> plaintext, span<const uint8_t> key,
                      span<const uint8_t> nonce, span<const uint8_t> aad,
                      span<uint8_t> ciphertext);
 
   int32_t CtrEncrypt(span<const uint8_t> plaintext, span<const uint8_t> key,
                      span<const uint8_t> nonce, span<uint8_t> ciphertext);
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
  int32_t Decrypt(span<const uint8_t> ciphertext, span<const uint8_t> key,
                  span<const uint8_t> aad, span<uint8_t> plaintext);

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

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

#include "parquet/encryption/encryption_internal.h"

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "parquet/encryption/openssl_internal.h"
#include "parquet/exception.h"

using ::arrow::util::span;
using parquet::ParquetException;

namespace parquet::encryption {

constexpr int kGcmMode = 0;
constexpr int kCtrMode = 1;
constexpr int kCtrIvLength = 16;
constexpr int kBufferSizeLength = 4;

#define ENCRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_EncryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    throw ParquetException("Couldn't init ALG encryption");           \
  }

#define DECRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_DecryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    throw ParquetException("Couldn't init ALG decryption");           \
  }

class AesEncryptor::AesEncryptorImpl {
 public:
  explicit AesEncryptorImpl(ParquetCipher::type alg_id, int key_len, bool metadata,
                            bool write_length);

  ~AesEncryptorImpl() { WipeOut(); }

  int Encrypt(span<const uint8_t> plaintext, span<const uint8_t> key,
              span<const uint8_t> aad, span<uint8_t> ciphertext);

  int SignedFooterEncrypt(span<const uint8_t> footer, span<const uint8_t> key,
                          span<const uint8_t> aad, span<const uint8_t> nonce,
                          span<uint8_t> encrypted_footer);
  void WipeOut() {
    if (nullptr != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
      ctx_ = nullptr;
    }
  }

  [[nodiscard]] int32_t CiphertextLength(int64_t plaintext_len) const {
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
  EVP_CIPHER_CTX* ctx_;
  int aes_mode_;
  int key_length_;
  int ciphertext_size_delta_;
  int length_buffer_length_;

  int GcmEncrypt(span<const uint8_t> plaintext, span<const uint8_t> key,
                 span<const uint8_t> nonce, span<const uint8_t> aad,
                 span<uint8_t> ciphertext);

  int CtrEncrypt(span<const uint8_t> plaintext, span<const uint8_t> key,
                 span<const uint8_t> nonce, span<uint8_t> ciphertext);
};

AesEncryptor::AesEncryptorImpl::AesEncryptorImpl(ParquetCipher::type alg_id, int key_len,
                                                 bool metadata, bool write_length) {
  openssl::EnsureInitialized();

  ctx_ = nullptr;

  length_buffer_length_ = write_length ? kBufferSizeLength : 0;
  ciphertext_size_delta_ = length_buffer_length_ + kNonceLength;
  if (metadata || (ParquetCipher::AES_GCM_V1 == alg_id)) {
    aes_mode_ = kGcmMode;
    ciphertext_size_delta_ += kGcmTagLength;
  } else {
    aes_mode_ = kCtrMode;
  }

  if (16 != key_len && 24 != key_len && 32 != key_len) {
    std::stringstream ss;
    ss << "Wrong key length: " << key_len;
    throw ParquetException(ss.str());
  }

  key_length_ = key_len;

  ctx_ = EVP_CIPHER_CTX_new();
  if (nullptr == ctx_) {
    throw ParquetException("Couldn't init cipher context");
  }

  if (kGcmMode == aes_mode_) {
    // Init AES-GCM with specified key length
    if (16 == key_len) {
      ENCRYPT_INIT(ctx_, EVP_aes_128_gcm());
    } else if (24 == key_len) {
      ENCRYPT_INIT(ctx_, EVP_aes_192_gcm());
    } else if (32 == key_len) {
      ENCRYPT_INIT(ctx_, EVP_aes_256_gcm());
    }
  } else {
    // Init AES-CTR with specified key length
    if (16 == key_len) {
      ENCRYPT_INIT(ctx_, EVP_aes_128_ctr());
    } else if (24 == key_len) {
      ENCRYPT_INIT(ctx_, EVP_aes_192_ctr());
    } else if (32 == key_len) {
      ENCRYPT_INIT(ctx_, EVP_aes_256_ctr());
    }
  }
}

int AesEncryptor::AesEncryptorImpl::SignedFooterEncrypt(span<const uint8_t> footer,
                                                        span<const uint8_t> key,
                                                        span<const uint8_t> aad,
                                                        span<const uint8_t> nonce,
                                                        span<uint8_t> encrypted_footer) {
  if (static_cast<size_t>(key_length_) != key.size()) {
    std::stringstream ss;
    ss << "Wrong key length " << key.size() << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  if (encrypted_footer.size() != footer.size() + ciphertext_size_delta_) {
    std::stringstream ss;
    ss << "Encrypted footer buffer length " << encrypted_footer.size()
       << " does not match expected length " << (footer.size() + ciphertext_size_delta_);
    throw ParquetException(ss.str());
  }

  if (kGcmMode != aes_mode_) {
    throw ParquetException("Must use AES GCM (metadata) encryptor");
  }

  return GcmEncrypt(footer, key, nonce, aad, encrypted_footer);
}

int AesEncryptor::AesEncryptorImpl::Encrypt(span<const uint8_t> plaintext,
                                            span<const uint8_t> key,
                                            span<const uint8_t> aad,
                                            span<uint8_t> ciphertext) {
  if (static_cast<size_t>(key_length_) != key.size()) {
    std::stringstream ss;
    ss << "Wrong key length " << key.size() << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  if (ciphertext.size() != plaintext.size() + ciphertext_size_delta_) {
    std::stringstream ss;
    ss << "Ciphertext buffer length " << ciphertext.size()
       << " does not match expected length "
       << (plaintext.size() + ciphertext_size_delta_);
    throw ParquetException(ss.str());
  }

  std::array<uint8_t, kNonceLength> nonce{};
  // Random nonce
  RAND_bytes(nonce.data(), kNonceLength);

  if (kGcmMode == aes_mode_) {
    return GcmEncrypt(plaintext, key, nonce, aad, ciphertext);
  }

  return CtrEncrypt(plaintext, key, nonce, ciphertext);
}

int AesEncryptor::AesEncryptorImpl::GcmEncrypt(span<const uint8_t> plaintext,
                                               span<const uint8_t> key,
                                               span<const uint8_t> nonce,
                                               span<const uint8_t> aad,
                                               span<uint8_t> ciphertext) {
  int len;
  int ciphertext_len;

  std::array<uint8_t, kGcmTagLength> tag{};

  if (nonce.size() != static_cast<size_t>(kNonceLength)) {
    std::stringstream ss;
    ss << "Invalid nonce size " << nonce.size() << ", expected " << kNonceLength;
    throw ParquetException(ss.str());
  }

  // Setting key and IV (nonce)
  if (1 != EVP_EncryptInit_ex(ctx_, nullptr, nullptr, key.data(), nonce.data())) {
    throw ParquetException("Couldn't set key and nonce");
  }

  // Setting additional authenticated data
  if ((!aad.empty()) && (1 != EVP_EncryptUpdate(ctx_, nullptr, &len, aad.data(),
                                                static_cast<int>(aad.size())))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Encryption
  if (1 !=
      EVP_EncryptUpdate(ctx_, ciphertext.data() + length_buffer_length_ + kNonceLength,
                        &len, plaintext.data(), static_cast<int>(plaintext.size()))) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 !=
      EVP_EncryptFinal_ex(
          ctx_, ciphertext.data() + length_buffer_length_ + kNonceLength + len, &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Getting the tag
  if (1 != EVP_CIPHER_CTX_ctrl(ctx_, EVP_CTRL_GCM_GET_TAG, kGcmTagLength, tag.data())) {
    throw ParquetException("Couldn't get AES-GCM tag");
  }

  // Copying the buffer size, nonce and tag to ciphertext
  int buffer_size = kNonceLength + ciphertext_len + kGcmTagLength;
  if (length_buffer_length_ > 0) {
    ciphertext[3] = static_cast<uint8_t>(0xff & (buffer_size >> 24));
    ciphertext[2] = static_cast<uint8_t>(0xff & (buffer_size >> 16));
    ciphertext[1] = static_cast<uint8_t>(0xff & (buffer_size >> 8));
    ciphertext[0] = static_cast<uint8_t>(0xff & (buffer_size));
  }
  std::copy(nonce.begin(), nonce.begin() + kNonceLength,
            ciphertext.begin() + length_buffer_length_);
  std::copy(tag.begin(), tag.end(),
            ciphertext.begin() + length_buffer_length_ + kNonceLength + ciphertext_len);

  return length_buffer_length_ + buffer_size;
}

int AesEncryptor::AesEncryptorImpl::CtrEncrypt(span<const uint8_t> plaintext,
                                               span<const uint8_t> key,
                                               span<const uint8_t> nonce,
                                               span<uint8_t> ciphertext) {
  int len;
  int ciphertext_len;

  if (nonce.size() != static_cast<size_t>(kNonceLength)) {
    std::stringstream ss;
    ss << "Invalid nonce size " << nonce.size() << ", expected " << kNonceLength;
    throw ParquetException(ss.str());
  }

  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial
  // counter field.
  // The first 31 bits of the initial counter field are set to 0, the last bit
  // is set to 1.
  std::array<uint8_t, kCtrIvLength> iv{};
  std::copy(nonce.begin(), nonce.begin() + kNonceLength, iv.begin());
  iv[kCtrIvLength - 1] = 1;

  // Setting key and IV
  if (1 != EVP_EncryptInit_ex(ctx_, nullptr, nullptr, key.data(), iv.data())) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Encryption
  if (1 !=
      EVP_EncryptUpdate(ctx_, ciphertext.data() + length_buffer_length_ + kNonceLength,
                        &len, plaintext.data(), static_cast<int>(plaintext.size()))) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 !=
      EVP_EncryptFinal_ex(
          ctx_, ciphertext.data() + length_buffer_length_ + kNonceLength + len, &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Copying the buffer size and nonce to ciphertext
  int buffer_size = kNonceLength + ciphertext_len;
  if (length_buffer_length_ > 0) {
    ciphertext[3] = static_cast<uint8_t>(0xff & (buffer_size >> 24));
    ciphertext[2] = static_cast<uint8_t>(0xff & (buffer_size >> 16));
    ciphertext[1] = static_cast<uint8_t>(0xff & (buffer_size >> 8));
    ciphertext[0] = static_cast<uint8_t>(0xff & (buffer_size));
  }
  std::copy(nonce.begin(), nonce.begin() + kNonceLength,
            ciphertext.begin() + length_buffer_length_);

  return length_buffer_length_ + buffer_size;
}

AesEncryptor::~AesEncryptor() {}

int AesEncryptor::SignedFooterEncrypt(span<const uint8_t> footer, span<const uint8_t> key,
                                      span<const uint8_t> aad, span<const uint8_t> nonce,
                                      span<uint8_t> encrypted_footer) {
  return impl_->SignedFooterEncrypt(footer, key, aad, nonce, encrypted_footer);
}

void AesEncryptor::WipeOut() { impl_->WipeOut(); }

int32_t AesEncryptor::CiphertextLength(int64_t plaintext_len) const {
  return impl_->CiphertextLength(plaintext_len);
}

int AesEncryptor::Encrypt(span<const uint8_t> plaintext, span<const uint8_t> key,
                          span<const uint8_t> aad, span<uint8_t> ciphertext) {
  return impl_->Encrypt(plaintext, key, aad, ciphertext);
}

AesEncryptor::AesEncryptor(ParquetCipher::type alg_id, int key_len, bool metadata,
                           bool write_length)
    : impl_{std::unique_ptr<AesEncryptorImpl>(
          new AesEncryptorImpl(alg_id, key_len, metadata, write_length))} {}

class AesDecryptor::AesDecryptorImpl {
 public:
  explicit AesDecryptorImpl(ParquetCipher::type alg_id, int key_len, bool metadata,
                            bool contains_length);

  ~AesDecryptorImpl() { WipeOut(); }

  int Decrypt(span<const uint8_t> ciphertext, span<const uint8_t> key,
              span<const uint8_t> aad, span<uint8_t> plaintext);

  void WipeOut() {
    if (nullptr != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
      ctx_ = nullptr;
    }
  }

  [[nodiscard]] int PlaintextLength(int ciphertext_len) const {
    if (ciphertext_len < ciphertext_size_delta_) {
      std::stringstream ss;
      ss << "Ciphertext length " << ciphertext_len << " is invalid, expected at least "
         << ciphertext_size_delta_;
      throw ParquetException(ss.str());
    }
    return ciphertext_len - ciphertext_size_delta_;
  }

  [[nodiscard]] int CiphertextLength(int plaintext_len) const {
    if (plaintext_len < 0) {
      std::stringstream ss;
      ss << "Negative plaintext length " << plaintext_len;
      throw ParquetException(ss.str());
    } else if (plaintext_len > std::numeric_limits<int>::max() - ciphertext_size_delta_) {
      std::stringstream ss;
      ss << "Plaintext length " << plaintext_len << " plus ciphertext size delta "
         << ciphertext_size_delta_ << " overflows int32";
      throw ParquetException(ss.str());
    }
    return plaintext_len + ciphertext_size_delta_;
  }

 private:
  EVP_CIPHER_CTX* ctx_;
  int aes_mode_;
  int key_length_;
  int ciphertext_size_delta_;
  int length_buffer_length_;

  /// Get the actual ciphertext length, inclusive of the length buffer length,
  /// and validate that the provided buffer size is large enough.
  [[nodiscard]] int GetCiphertextLength(span<const uint8_t> ciphertext) const;

  int GcmDecrypt(span<const uint8_t> ciphertext, span<const uint8_t> key,
                 span<const uint8_t> aad, span<uint8_t> plaintext);

  int CtrDecrypt(span<const uint8_t> ciphertext, span<const uint8_t> key,
                 span<uint8_t> plaintext);
};

int AesDecryptor::Decrypt(span<const uint8_t> ciphertext, span<const uint8_t> key,
                          span<const uint8_t> aad, span<uint8_t> plaintext) {
  return impl_->Decrypt(ciphertext, key, aad, plaintext);
}

void AesDecryptor::WipeOut() { impl_->WipeOut(); }

AesDecryptor::~AesDecryptor() {}

AesDecryptor::AesDecryptorImpl::AesDecryptorImpl(ParquetCipher::type alg_id, int key_len,
                                                 bool metadata, bool contains_length) {
  openssl::EnsureInitialized();

  ctx_ = nullptr;
  length_buffer_length_ = contains_length ? kBufferSizeLength : 0;
  ciphertext_size_delta_ = length_buffer_length_ + kNonceLength;
  if (metadata || (ParquetCipher::AES_GCM_V1 == alg_id)) {
    aes_mode_ = kGcmMode;
    ciphertext_size_delta_ += kGcmTagLength;
  } else {
    aes_mode_ = kCtrMode;
  }

  if (16 != key_len && 24 != key_len && 32 != key_len) {
    std::stringstream ss;
    ss << "Wrong key length: " << key_len;
    throw ParquetException(ss.str());
  }

  key_length_ = key_len;

  ctx_ = EVP_CIPHER_CTX_new();
  if (nullptr == ctx_) {
    throw ParquetException("Couldn't init cipher context");
  }

  if (kGcmMode == aes_mode_) {
    // Init AES-GCM with specified key length
    if (16 == key_len) {
      DECRYPT_INIT(ctx_, EVP_aes_128_gcm());
    } else if (24 == key_len) {
      DECRYPT_INIT(ctx_, EVP_aes_192_gcm());
    } else if (32 == key_len) {
      DECRYPT_INIT(ctx_, EVP_aes_256_gcm());
    }
  } else {
    // Init AES-CTR with specified key length
    if (16 == key_len) {
      DECRYPT_INIT(ctx_, EVP_aes_128_ctr());
    } else if (24 == key_len) {
      DECRYPT_INIT(ctx_, EVP_aes_192_ctr());
    } else if (32 == key_len) {
      DECRYPT_INIT(ctx_, EVP_aes_256_ctr());
    }
  }
}

AesEncryptor* AesEncryptor::Make(ParquetCipher::type alg_id, int key_len, bool metadata,
                                 std::vector<AesEncryptor*>* all_encryptors) {
  return Make(alg_id, key_len, metadata, true /*write_length*/, all_encryptors);
}

AesEncryptor* AesEncryptor::Make(ParquetCipher::type alg_id, int key_len, bool metadata,
                                 bool write_length,
                                 std::vector<AesEncryptor*>* all_encryptors) {
  if (ParquetCipher::AES_GCM_V1 != alg_id && ParquetCipher::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw ParquetException(ss.str());
  }

  AesEncryptor* encryptor = new AesEncryptor(alg_id, key_len, metadata, write_length);
  if (all_encryptors != nullptr) all_encryptors->push_back(encryptor);
  return encryptor;
}

AesDecryptor::AesDecryptor(ParquetCipher::type alg_id, int key_len, bool metadata,
                           bool contains_length)
    : impl_{std::unique_ptr<AesDecryptorImpl>(
          new AesDecryptorImpl(alg_id, key_len, metadata, contains_length))} {}

std::shared_ptr<AesDecryptor> AesDecryptor::Make(
    ParquetCipher::type alg_id, int key_len, bool metadata,
    std::vector<std::weak_ptr<AesDecryptor>>* all_decryptors) {
  if (ParquetCipher::AES_GCM_V1 != alg_id && ParquetCipher::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw ParquetException(ss.str());
  }

  auto decryptor = std::make_shared<AesDecryptor>(alg_id, key_len, metadata);
  if (all_decryptors != nullptr) {
    all_decryptors->push_back(decryptor);
  }
  return decryptor;
}

int AesDecryptor::PlaintextLength(int ciphertext_len) const {
  return impl_->PlaintextLength(ciphertext_len);
}

int AesDecryptor::CiphertextLength(int plaintext_len) const {
  return impl_->CiphertextLength(plaintext_len);
}

int AesDecryptor::AesDecryptorImpl::GetCiphertextLength(
    span<const uint8_t> ciphertext) const {
  if (length_buffer_length_ > 0) {
    // Note: length_buffer_length_ must be either 0 or kBufferSizeLength
    if (ciphertext.size() < static_cast<size_t>(kBufferSizeLength)) {
      std::stringstream ss;
      ss << "Ciphertext buffer length " << ciphertext.size()
         << " is insufficient to read the ciphertext length."
         << " At least " << kBufferSizeLength << " bytes are required.";
      throw ParquetException(ss.str());
    }

    // Extract ciphertext length
    uint32_t written_ciphertext_len = (static_cast<uint32_t>(ciphertext[3]) << 24) |
                                      (static_cast<uint32_t>(ciphertext[2]) << 16) |
                                      (static_cast<uint32_t>(ciphertext[1]) << 8) |
                                      (static_cast<uint32_t>(ciphertext[0]));

    if (written_ciphertext_len >
        static_cast<uint32_t>(std::numeric_limits<int>::max() - length_buffer_length_)) {
      std::stringstream ss;
      ss << "Written ciphertext length " << written_ciphertext_len
         << " plus length buffer length " << length_buffer_length_ << " overflows int";
      throw ParquetException(ss.str());
    } else if (ciphertext.size() <
               static_cast<size_t>(written_ciphertext_len) + length_buffer_length_) {
      std::stringstream ss;
      ss << "Serialized ciphertext length "
         << (written_ciphertext_len + length_buffer_length_)
         << " is greater than the provided ciphertext buffer length "
         << ciphertext.size();
      throw ParquetException(ss.str());
    }

    return static_cast<int>(written_ciphertext_len) + length_buffer_length_;
  } else {
    if (ciphertext.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
      std::stringstream ss;
      ss << "Ciphertext buffer length " << ciphertext.size() << " overflows int";
      throw ParquetException(ss.str());
    }
    return static_cast<int>(ciphertext.size());
  }
}

int AesDecryptor::AesDecryptorImpl::GcmDecrypt(span<const uint8_t> ciphertext,
                                               span<const uint8_t> key,
                                               span<const uint8_t> aad,
                                               span<uint8_t> plaintext) {
  int len;
  int plaintext_len;

  std::array<uint8_t, kGcmTagLength> tag{};
  std::array<uint8_t, kNonceLength> nonce{};

  int ciphertext_len = GetCiphertextLength(ciphertext);

  if (plaintext.size() < static_cast<size_t>(ciphertext_len) - ciphertext_size_delta_) {
    std::stringstream ss;
    ss << "Plaintext buffer length " << plaintext.size() << " is insufficient "
       << "for ciphertext length " << ciphertext_len;
    throw ParquetException(ss.str());
  }

  if (ciphertext_len < length_buffer_length_ + kNonceLength + kGcmTagLength) {
    std::stringstream ss;
    ss << "Invalid ciphertext length " << ciphertext_len << ". Expected at least "
       << length_buffer_length_ + kNonceLength + kGcmTagLength << "\n";
    throw ParquetException(ss.str());
  }

  // Extracting IV and tag
  std::copy(ciphertext.begin() + length_buffer_length_,
            ciphertext.begin() + length_buffer_length_ + kNonceLength, nonce.begin());
  std::copy(ciphertext.begin() + ciphertext_len - kGcmTagLength,
            ciphertext.begin() + ciphertext_len, tag.begin());

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx_, nullptr, nullptr, key.data(), nonce.data())) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Setting additional authenticated data
  if ((!aad.empty()) && (1 != EVP_DecryptUpdate(ctx_, nullptr, &len, aad.data(),
                                                static_cast<int>(aad.size())))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Decryption
  if (!EVP_DecryptUpdate(
          ctx_, plaintext.data(), &len,
          ciphertext.data() + length_buffer_length_ + kNonceLength,
          ciphertext_len - length_buffer_length_ - kNonceLength - kGcmTagLength)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Checking the tag (authentication)
  if (!EVP_CIPHER_CTX_ctrl(ctx_, EVP_CTRL_GCM_SET_TAG, kGcmTagLength, tag.data())) {
    throw ParquetException("Failed authentication");
  }

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx_, plaintext.data() + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int AesDecryptor::AesDecryptorImpl::CtrDecrypt(span<const uint8_t> ciphertext,
                                               span<const uint8_t> key,
                                               span<uint8_t> plaintext) {
  int len;
  int plaintext_len;

  std::array<uint8_t, kCtrIvLength> iv{};

  int ciphertext_len = GetCiphertextLength(ciphertext);

  if (plaintext.size() < static_cast<size_t>(ciphertext_len) - ciphertext_size_delta_) {
    std::stringstream ss;
    ss << "Plaintext buffer length " << plaintext.size() << " is insufficient "
       << "for ciphertext length " << ciphertext_len;
    throw ParquetException(ss.str());
  }

  if (ciphertext_len < length_buffer_length_ + kNonceLength) {
    std::stringstream ss;
    ss << "Invalid ciphertext length " << ciphertext_len << ". Expected at least "
       << length_buffer_length_ + kNonceLength << "\n";
    throw ParquetException(ss.str());
  }

  // Extracting nonce
  std::copy(ciphertext.begin() + length_buffer_length_,
            ciphertext.begin() + length_buffer_length_ + kNonceLength, iv.begin());
  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial
  // counter field.
  // The first 31 bits of the initial counter field are set to 0, the last bit
  // is set to 1.
  iv[kCtrIvLength - 1] = 1;

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx_, nullptr, nullptr, key.data(), iv.data())) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Decryption
  if (!EVP_DecryptUpdate(ctx_, plaintext.data(), &len,
                         ciphertext.data() + length_buffer_length_ + kNonceLength,
                         ciphertext_len - length_buffer_length_ - kNonceLength)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx_, plaintext.data() + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int AesDecryptor::AesDecryptorImpl::Decrypt(span<const uint8_t> ciphertext,
                                            span<const uint8_t> key,
                                            span<const uint8_t> aad,
                                            span<uint8_t> plaintext) {
  if (static_cast<size_t>(key_length_) != key.size()) {
    std::stringstream ss;
    ss << "Wrong key length " << key.size() << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  if (kGcmMode == aes_mode_) {
    return GcmDecrypt(ciphertext, key, aad, plaintext);
  }

  return CtrDecrypt(ciphertext, key, plaintext);
}

static std::string ShortToBytesLe(int16_t input) {
  int8_t output[2];
  memset(output, 0, 2);
  output[1] = static_cast<int8_t>(0xff & (input >> 8));
  output[0] = static_cast<int8_t>(0xff & (input));

  return std::string(reinterpret_cast<char const*>(output), 2);
}

static void CheckPageOrdinal(int32_t page_ordinal) {
  if (ARROW_PREDICT_FALSE(page_ordinal > std::numeric_limits<int16_t>::max())) {
    throw ParquetException("Encrypted Parquet files can't have more than " +
                           std::to_string(std::numeric_limits<int16_t>::max()) +
                           " pages per chunk: got " + std::to_string(page_ordinal));
  }
}

std::string CreateModuleAad(const std::string& file_aad, int8_t module_type,
                            int16_t row_group_ordinal, int16_t column_ordinal,
                            int32_t page_ordinal) {
  CheckPageOrdinal(page_ordinal);
  const int16_t page_ordinal_short = static_cast<int16_t>(page_ordinal);
  int8_t type_ordinal_bytes[1];
  type_ordinal_bytes[0] = module_type;
  std::string type_ordinal_bytes_str(reinterpret_cast<char const*>(type_ordinal_bytes),
                                     1);
  if (kFooter == module_type) {
    std::string result = file_aad + type_ordinal_bytes_str;
    return result;
  }
  std::string row_group_ordinal_bytes = ShortToBytesLe(row_group_ordinal);
  std::string column_ordinal_bytes = ShortToBytesLe(column_ordinal);
  if (kDataPage != module_type && kDataPageHeader != module_type) {
    std::ostringstream out;
    out << file_aad << type_ordinal_bytes_str << row_group_ordinal_bytes
        << column_ordinal_bytes;
    return out.str();
  }
  std::string page_ordinal_bytes = ShortToBytesLe(page_ordinal_short);
  std::ostringstream out;
  out << file_aad << type_ordinal_bytes_str << row_group_ordinal_bytes
      << column_ordinal_bytes << page_ordinal_bytes;
  return out.str();
}

std::string CreateFooterAad(const std::string& aad_prefix_bytes) {
  return CreateModuleAad(aad_prefix_bytes, kFooter, static_cast<int16_t>(-1),
                         static_cast<int16_t>(-1), static_cast<int16_t>(-1));
}

// Update last two bytes with new page ordinal (instead of creating new page AAD
// from scratch)
void QuickUpdatePageAad(int32_t new_page_ordinal, std::string* AAD) {
  CheckPageOrdinal(new_page_ordinal);
  const std::string page_ordinal_bytes =
      ShortToBytesLe(static_cast<int16_t>(new_page_ordinal));
  std::memcpy(AAD->data() + AAD->length() - 2, page_ordinal_bytes.data(), 2);
}

void RandBytes(unsigned char* buf, int num) {
  openssl::EnsureInitialized();
  RAND_bytes(buf, num);
}

void EnsureBackendInitialized() { openssl::EnsureInitialized(); }

}  // namespace parquet::encryption

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

#include "parquet/encryption/aes_encryption.h"

#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>

#include "parquet/encryption/encryption_utils.h"
#include "parquet/encryption/openssl_internal.h"
#include "parquet/exception.h"

using parquet::ParquetException;

namespace parquet::encryption {

constexpr int32_t kGcmMode = 0;
constexpr int32_t kCtrMode = 1;
constexpr int32_t kCtrIvLength = 16;
constexpr int32_t kBufferSizeLength = 4;

#define ENCRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_EncryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    throw ParquetException("Couldn't init ALG encryption");           \
  }

#define DECRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_DecryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    throw ParquetException("Couldn't init ALG decryption");           \
  }

AesCryptoContext::AesCryptoContext(ParquetCipher::type alg_id, int32_t key_len,
                                   bool metadata, bool include_length) {
  openssl::EnsureInitialized();

  length_buffer_length_ = include_length ? kBufferSizeLength : 0;
  ciphertext_size_delta_ = length_buffer_length_ + kNonceLength;

  // Not all encryptors support metadata encryption. When that happens, even if the
  // ParquetCipher is not AES, the metadata is encrypted using AES. This check
  // should pass.
  bool is_aes_algorithm =
      ParquetCipher::AES_GCM_V1 == alg_id || ParquetCipher::AES_GCM_CTR_V1 == alg_id;
  if (!is_aes_algorithm && !metadata) {
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

AesEncryptor::AesEncryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                           bool write_length)
    : AesCryptoContext(alg_id, key_len, metadata, write_length) {}

std::unique_ptr<AesEncryptor> AesEncryptor::Make(ParquetCipher::type alg_id,
                                                 int32_t key_len, bool metadata,
                                                 bool write_length) {
  return std::make_unique<AesEncryptor>(alg_id, key_len, metadata, write_length);
}

int32_t AesEncryptor::CiphertextLength(int64_t plaintext_len) const {
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

AesCryptoContext::CipherContext AesEncryptor::MakeCipherContext() const {
  auto ctx = NewCipherContext();
  if (kGcmMode == aes_mode_) {
    // Init AES-GCM with specified key length
    if (16 == key_length_) {
      ENCRYPT_INIT(ctx.get(), EVP_aes_128_gcm());
    } else if (24 == key_length_) {
      ENCRYPT_INIT(ctx.get(), EVP_aes_192_gcm());
    } else if (32 == key_length_) {
      ENCRYPT_INIT(ctx.get(), EVP_aes_256_gcm());
    }
  } else {
    // Init AES-CTR with specified key length
    if (16 == key_length_) {
      ENCRYPT_INIT(ctx.get(), EVP_aes_128_ctr());
    } else if (24 == key_length_) {
      ENCRYPT_INIT(ctx.get(), EVP_aes_192_ctr());
    } else if (32 == key_length_) {
      ENCRYPT_INIT(ctx.get(), EVP_aes_256_ctr());
    }
  }
  return ctx;
}

int32_t AesEncryptor::SignedFooterEncrypt(::arrow::util::span<const uint8_t> footer,
                                          ::arrow::util::span<const uint8_t> key,
                                          ::arrow::util::span<const uint8_t> aad,
                                          ::arrow::util::span<const uint8_t> nonce,
                                          ::arrow::util::span<uint8_t> encrypted_footer) {
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

int32_t AesEncryptor::Encrypt(::arrow::util::span<const uint8_t> plaintext,
                              ::arrow::util::span<const uint8_t> key,
                              ::arrow::util::span<const uint8_t> aad,
                              ::arrow::util::span<uint8_t> ciphertext) {
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

int32_t AesEncryptor::GcmEncrypt(::arrow::util::span<const uint8_t> plaintext,
                                 ::arrow::util::span<const uint8_t> key,
                                 ::arrow::util::span<const uint8_t> nonce,
                                 ::arrow::util::span<const uint8_t> aad,
                                 ::arrow::util::span<uint8_t> ciphertext) {
  int len;
  int32_t ciphertext_len;

  std::array<uint8_t, kGcmTagLength> tag{};

  if (nonce.size() != static_cast<size_t>(kNonceLength)) {
    std::stringstream ss;
    ss << "Invalid nonce size " << nonce.size() << ", expected " << kNonceLength;
    throw ParquetException(ss.str());
  }

  auto ctx = MakeCipherContext();

  // Setting key and IV (nonce)
  if (1 != EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr, key.data(), nonce.data())) {
    throw ParquetException("Couldn't set key and nonce");
  }

  // Setting additional authenticated data
  if (aad.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    std::stringstream ss;
    ss << "AAD size " << aad.size() << " overflows int";
    throw ParquetException(ss.str());
  }
  if ((!aad.empty()) && (1 != EVP_EncryptUpdate(ctx.get(), nullptr, &len, aad.data(),
                                                static_cast<int>(aad.size())))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Encryption
  if (plaintext.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    std::stringstream ss;
    ss << "Plaintext size " << plaintext.size() << " overflows int";
    throw ParquetException(ss.str());
  }
  if (1 != EVP_EncryptUpdate(
               ctx.get(), ciphertext.data() + length_buffer_length_ + kNonceLength, &len,
               plaintext.data(), static_cast<int>(plaintext.size()))) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 != EVP_EncryptFinal_ex(
               ctx.get(), ciphertext.data() + length_buffer_length_ + kNonceLength + len,
               &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Getting the tag
  if (1 !=
      EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, kGcmTagLength, tag.data())) {
    throw ParquetException("Couldn't get AES-GCM tag");
  }

  // Copying the buffer size, nonce and tag to ciphertext
  int32_t buffer_size = kNonceLength + ciphertext_len + kGcmTagLength;
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

int32_t AesEncryptor::CtrEncrypt(::arrow::util::span<const uint8_t> plaintext,
                                 ::arrow::util::span<const uint8_t> key,
                                 ::arrow::util::span<const uint8_t> nonce,
                                 ::arrow::util::span<uint8_t> ciphertext) {
  int len;
  int32_t ciphertext_len;

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

  auto ctx = MakeCipherContext();

  // Setting key and IV
  if (1 != EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr, key.data(), iv.data())) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Encryption
  if (plaintext.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    std::stringstream ss;
    ss << "Plaintext size " << plaintext.size() << " overflows int";
    throw ParquetException(ss.str());
  }
  if (1 != EVP_EncryptUpdate(
               ctx.get(), ciphertext.data() + length_buffer_length_ + kNonceLength, &len,
               plaintext.data(), static_cast<int>(plaintext.size()))) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 != EVP_EncryptFinal_ex(
               ctx.get(), ciphertext.data() + length_buffer_length_ + kNonceLength + len,
               &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Copying the buffer size and nonce to ciphertext
  int32_t buffer_size = kNonceLength + ciphertext_len;
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

uint64_t AesEncryptorFactory::MakeCacheKey(ParquetCipher::type alg_id, int32_t key_len,
                                           bool metadata) {
  uint64_t key = 0;
  // Set the algorithm id in the most significant 32 bits.
  key |= static_cast<uint64_t>(static_cast<uint32_t>(alg_id)) << 32;
  // Set the key length in the next 8 bits.
  key |= static_cast<uint64_t>(static_cast<uint8_t>(key_len));
  // Set the metadata flag in the next 8 bits.
  key |= static_cast<uint64_t>(metadata ? 1 : 0) << 8;
  return key;
}

AesEncryptor* AesEncryptorFactory::GetMetaAesEncryptor(ParquetCipher::type alg_id,
                                                       size_t key_size) {
  if (key_size > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    throw ParquetException("Invalid key length: exceeds int32_t max");
  }
  auto key_len = static_cast<int32_t>(key_size);
  // Create the cache key using the algorithm id, key length, and metadata flag
  // to avoid collisions for encryptors with the same key length.
  uint64_t cache_key = MakeCacheKey(alg_id, key_len, /*metadata=*/true);

  // If no encryptor exists for this cache key, create one.
  if (encryptor_cache_.find(cache_key) == encryptor_cache_.end()) {
    encryptor_cache_[cache_key] = AesEncryptor::Make(alg_id, key_len, /*metadata=*/true);
  }

  return encryptor_cache_[cache_key].get();
}

AesEncryptor* AesEncryptorFactory::GetDataAesEncryptor(ParquetCipher::type alg_id,
                                                       size_t key_size) {
  if (key_size > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    throw ParquetException("Invalid key length: exceeds int32_t max");
  }
  auto key_len = static_cast<int32_t>(key_size);
  // Create the cache key using the algorithm id, key length, and metadata flag
  // to avoid collisions for encryptors with the same key length.
  uint64_t cache_key = MakeCacheKey(alg_id, key_len, /*metadata=*/false);

  // If no encryptor exists for this cache key, create one.
  if (encryptor_cache_.find(cache_key) == encryptor_cache_.end()) {
    encryptor_cache_[cache_key] = AesEncryptor::Make(alg_id, key_len, /*metadata=*/false);
  }
  return encryptor_cache_[cache_key].get();
}

AesDecryptor::AesDecryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                           bool contains_length)
    : AesCryptoContext(alg_id, key_len, metadata, contains_length) {}

std::unique_ptr<AesDecryptor> AesDecryptor::Make(ParquetCipher::type alg_id,
                                                 int32_t key_len, bool metadata) {
  return std::make_unique<AesDecryptor>(alg_id, key_len, metadata);
}

int32_t AesDecryptor::PlaintextLength(int32_t ciphertext_len) const {
  if (ciphertext_len < ciphertext_size_delta_) {
    std::stringstream ss;
    ss << "Ciphertext length " << ciphertext_len << " is invalid, expected at least "
       << ciphertext_size_delta_;
    throw ParquetException(ss.str());
  }
  return ciphertext_len - ciphertext_size_delta_;
}

int32_t AesDecryptor::CiphertextLength(int32_t plaintext_len) const {
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
  return plaintext_len + ciphertext_size_delta_;
}

int32_t AesDecryptor::Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                              ::arrow::util::span<const uint8_t> key,
                              ::arrow::util::span<const uint8_t> aad,
                              ::arrow::util::span<uint8_t> plaintext) {
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

AesCryptoContext::CipherContext AesDecryptor::MakeCipherContext() const {
  auto ctx = NewCipherContext();
  if (kGcmMode == aes_mode_) {
    // Init AES-GCM with specified key length
    if (16 == key_length_) {
      DECRYPT_INIT(ctx.get(), EVP_aes_128_gcm());
    } else if (24 == key_length_) {
      DECRYPT_INIT(ctx.get(), EVP_aes_192_gcm());
    } else if (32 == key_length_) {
      DECRYPT_INIT(ctx.get(), EVP_aes_256_gcm());
    }
  } else {
    // Init AES-CTR with specified key length
    if (16 == key_length_) {
      DECRYPT_INIT(ctx.get(), EVP_aes_128_ctr());
    } else if (24 == key_length_) {
      DECRYPT_INIT(ctx.get(), EVP_aes_192_ctr());
    } else if (32 == key_length_) {
      DECRYPT_INIT(ctx.get(), EVP_aes_256_ctr());
    }
  }
  return ctx;
}

int32_t AesDecryptor::GetCiphertextLength(
    ::arrow::util::span<const uint8_t> ciphertext) const {
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
        static_cast<uint32_t>(std::numeric_limits<int32_t>::max() -
                              length_buffer_length_)) {
      std::stringstream ss;
      ss << "Written ciphertext length " << written_ciphertext_len
         << " plus length buffer length " << length_buffer_length_ << " overflows int32";
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

    return static_cast<int32_t>(written_ciphertext_len) + length_buffer_length_;
  } else {
    if (ciphertext.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      std::stringstream ss;
      ss << "Ciphertext buffer length " << ciphertext.size() << " overflows int32";
      throw ParquetException(ss.str());
    }
    return static_cast<int32_t>(ciphertext.size());
  }
}

int32_t AesDecryptor::GcmDecrypt(::arrow::util::span<const uint8_t> ciphertext,
                                 ::arrow::util::span<const uint8_t> key,
                                 ::arrow::util::span<const uint8_t> aad,
                                 ::arrow::util::span<uint8_t> plaintext) {
  int len;
  int32_t plaintext_len;

  std::array<uint8_t, kGcmTagLength> tag{};
  std::array<uint8_t, kNonceLength> nonce{};

  int32_t ciphertext_len = GetCiphertextLength(ciphertext);

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

  auto ctx = MakeCipherContext();

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr, key.data(), nonce.data())) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Setting additional authenticated data
  if (aad.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    std::stringstream ss;
    ss << "AAD size " << aad.size() << " overflows int";
    throw ParquetException(ss.str());
  }
  if ((!aad.empty()) && (1 != EVP_DecryptUpdate(ctx.get(), nullptr, &len, aad.data(),
                                                static_cast<int>(aad.size())))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Decryption
  int decryption_length =
      ciphertext_len - length_buffer_length_ - kNonceLength - kGcmTagLength;
  if (!EVP_DecryptUpdate(ctx.get(), plaintext.data(), &len,
                         ciphertext.data() + length_buffer_length_ + kNonceLength,
                         decryption_length)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Checking the tag (authentication)
  if (!EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, kGcmTagLength, tag.data())) {
    throw ParquetException("Failed authentication");
  }

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx.get(), plaintext.data() + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int32_t AesDecryptor::CtrDecrypt(::arrow::util::span<const uint8_t> ciphertext,
                                 ::arrow::util::span<const uint8_t> key,
                                 ::arrow::util::span<uint8_t> plaintext) {
  int len;
  int32_t plaintext_len;

  std::array<uint8_t, kCtrIvLength> iv{};

  int32_t ciphertext_len = GetCiphertextLength(ciphertext);

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

  auto ctx = MakeCipherContext();

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr, key.data(), iv.data())) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Decryption
  int decryption_length = ciphertext_len - length_buffer_length_ - kNonceLength;
  if (!EVP_DecryptUpdate(ctx.get(), plaintext.data(), &len,
                         ciphertext.data() + length_buffer_length_ + kNonceLength,
                         decryption_length)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx.get(), plaintext.data() + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

#undef ENCRYPT_INIT
#undef DECRYPT_INIT

}  // namespace parquet::encryption

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

#include "parquet/encryption_internal.h"
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "parquet/exception.h"

using parquet::ParquetException;

namespace parquet {
namespace encryption {

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
  explicit AesEncryptorImpl(ParquetCipher::type alg_id, int key_len, bool metadata);

  ~AesEncryptorImpl() {
    if (NULLPTR != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
      ctx_ = NULLPTR;
    }
  }

  int Encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len,
              uint8_t* aad, int aad_len, uint8_t* ciphertext);

  int SignedFooterEncrypt(const uint8_t* footer, int footer_len, uint8_t* key,
                          int key_len, uint8_t* aad, int aad_len, uint8_t* nonce,
                          uint8_t* encrypted_footer);
  void WipeOut() {
    if (NULLPTR != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
      ctx_ = NULLPTR;
    }
  }

  int ciphertext_size_delta() { return ciphertext_size_delta_; }

 private:
  EVP_CIPHER_CTX* ctx_;
  int aes_mode_;
  int key_length_;
  int ciphertext_size_delta_;

  int GcmEncrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len,
                 uint8_t* nonce, uint8_t* aad, int aad_len, uint8_t* ciphertext);

  int CtrEncrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len,
                 uint8_t* nonce, uint8_t* ciphertext);
};

AesEncryptor::AesEncryptorImpl::AesEncryptorImpl(ParquetCipher::type alg_id, int key_len,
                                                 bool metadata) {
  ctx_ = nullptr;

  ciphertext_size_delta_ = kBufferSizeLength + kNonceLength;
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

int AesEncryptor::AesEncryptorImpl::SignedFooterEncrypt(const uint8_t* footer,
                                                        int footer_len, uint8_t* key,
                                                        int key_len, uint8_t* aad,
                                                        int aad_len, uint8_t* nonce,
                                                        uint8_t* encrypted_footer) {
  if (key_length_ != key_len) {
    std::stringstream ss;
    ss << "Wrong key length " << key_len << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  if (kGcmMode != aes_mode_) {
    throw ParquetException("Must use AES GCM (metadata) encryptor");
  }

  return GcmEncrypt(footer, footer_len, key, key_len, nonce, aad, aad_len,
                    encrypted_footer);
}

int AesEncryptor::AesEncryptorImpl::Encrypt(const uint8_t* plaintext, int plaintext_len,
                                            uint8_t* key, int key_len, uint8_t* aad,
                                            int aad_len, uint8_t* ciphertext) {
  if (key_length_ != key_len) {
    std::stringstream ss;
    ss << "Wrong key length " << key_len << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  uint8_t nonce[kNonceLength];
  memset(nonce, 0, kNonceLength);
  // Random nonce
  RAND_bytes(nonce, sizeof(nonce));

  if (kGcmMode == aes_mode_) {
    return GcmEncrypt(plaintext, plaintext_len, key, key_len, nonce, aad, aad_len,
                      ciphertext);
  }

  return CtrEncrypt(plaintext, plaintext_len, key, key_len, nonce, ciphertext);
}

int AesEncryptor::AesEncryptorImpl::GcmEncrypt(const uint8_t* plaintext,
                                               int plaintext_len, uint8_t* key,
                                               int key_len, uint8_t* nonce, uint8_t* aad,
                                               int aad_len, uint8_t* ciphertext) {
  int len;
  int ciphertext_len;

  uint8_t tag[kGcmTagLength];
  memset(tag, 0, kGcmTagLength);

  // Setting key and IV (nonce)
  if (1 != EVP_EncryptInit_ex(ctx_, nullptr, nullptr, key, nonce)) {
    throw ParquetException("Couldn't set key and nonce");
  }

  // Setting additional authenticated data
  if ((nullptr != aad) && (1 != EVP_EncryptUpdate(ctx_, nullptr, &len, aad, aad_len))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Encryption
  if (1 != EVP_EncryptUpdate(ctx_, ciphertext + kBufferSizeLength + kNonceLength, &len,
                             plaintext, plaintext_len)) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 != EVP_EncryptFinal_ex(ctx_, ciphertext + kBufferSizeLength + kNonceLength + len,
                               &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Getting the tag
  if (1 != EVP_CIPHER_CTX_ctrl(ctx_, EVP_CTRL_GCM_GET_TAG, kGcmTagLength, tag)) {
    throw ParquetException("Couldn't get AES-GCM tag");
  }

  // Copying the buffer size, nonce and tag to ciphertext
  int buffer_size = kNonceLength + ciphertext_len + kGcmTagLength;
  ciphertext[3] = static_cast<uint8_t>(0xff & (buffer_size >> 24));
  ciphertext[2] = static_cast<uint8_t>(0xff & (buffer_size >> 16));
  ciphertext[1] = static_cast<uint8_t>(0xff & (buffer_size >> 8));
  ciphertext[0] = static_cast<uint8_t>(0xff & (buffer_size));
  std::copy(nonce, nonce + kNonceLength, ciphertext + kBufferSizeLength);
  std::copy(tag, tag + kGcmTagLength,
            ciphertext + kBufferSizeLength + kNonceLength + ciphertext_len);

  return kBufferSizeLength + buffer_size;
}

int AesEncryptor::AesEncryptorImpl::CtrEncrypt(const uint8_t* plaintext,
                                               int plaintext_len, uint8_t* key,
                                               int key_len, uint8_t* nonce,
                                               uint8_t* ciphertext) {
  int len;
  int ciphertext_len;

  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial
  // counter field.
  // The first 31 bits of the initial counter field are set to 0, the last bit
  // is set to 1.
  uint8_t iv[kCtrIvLength];
  memset(iv, 0, kCtrIvLength);
  std::copy(nonce, nonce + kNonceLength, iv);
  iv[kCtrIvLength - 1] = 1;

  // Setting key and IV
  if (1 != EVP_EncryptInit_ex(ctx_, nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Encryption
  if (1 != EVP_EncryptUpdate(ctx_, ciphertext + kBufferSizeLength + kNonceLength, &len,
                             plaintext, plaintext_len)) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 != EVP_EncryptFinal_ex(ctx_, ciphertext + kBufferSizeLength + kNonceLength + len,
                               &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Copying the buffer size and nonce to ciphertext
  int buffer_size = kNonceLength + ciphertext_len;
  ciphertext[3] = static_cast<uint8_t>(0xff & (buffer_size >> 24));
  ciphertext[2] = static_cast<uint8_t>(0xff & (buffer_size >> 16));
  ciphertext[1] = static_cast<uint8_t>(0xff & (buffer_size >> 8));
  ciphertext[0] = static_cast<uint8_t>(0xff & (buffer_size));
  std::copy(nonce, nonce + kNonceLength, ciphertext + kBufferSizeLength);

  return kBufferSizeLength + buffer_size;
}

AesEncryptor::~AesEncryptor() { impl_->~AesEncryptorImpl(); }

int AesEncryptor::SignedFooterEncrypt(const uint8_t* footer, int footer_len, uint8_t* key,
                                      int key_len, uint8_t* aad, int aad_len,
                                      uint8_t* nonce, uint8_t* encrypted_footer) {
  return impl_->SignedFooterEncrypt(footer, footer_len, key, key_len, aad, aad_len, nonce,
                                    encrypted_footer);
}

void AesEncryptor::WipeOut() { impl_->WipeOut(); }

int AesEncryptor::CiphertextSizeDelta() { return impl_->ciphertext_size_delta(); }

int AesEncryptor::Encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key,
                          int key_len, uint8_t* aad, int aad_len, uint8_t* ciphertext) {
  return impl_->Encrypt(plaintext, plaintext_len, key, key_len, aad, aad_len, ciphertext);
}

AesEncryptor::AesEncryptor(ParquetCipher::type alg_id, int key_len, bool metadata)
    : impl_{std::unique_ptr<AesEncryptorImpl>(
          new AesEncryptorImpl(alg_id, key_len, metadata))} {}

class AesDecryptor::AesDecryptorImpl {
 public:
  explicit AesDecryptorImpl(ParquetCipher::type alg_id, int key_len, bool metadata);

  ~AesDecryptorImpl() {
    if (NULLPTR != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
      ctx_ = NULLPTR;
    }
  }

  int Decrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* key, int key_len,
              uint8_t* aad, int aad_len, uint8_t* plaintext);

  void WipeOut() {
    if (NULLPTR != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
      ctx_ = NULLPTR;
    }
  }

  int ciphertext_size_delta() { return ciphertext_size_delta_; }

 private:
  EVP_CIPHER_CTX* ctx_;
  int aes_mode_;
  int key_length_;
  int ciphertext_size_delta_;
  int GcmDecrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* key, int key_len,
                 uint8_t* aad, int aad_len, uint8_t* plaintext);

  int CtrDecrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* key, int key_len,
                 uint8_t* plaintext);
};

int AesDecryptor::Decrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key,
                          int key_len, uint8_t* aad, int aad_len, uint8_t* ciphertext) {
  return impl_->Decrypt(plaintext, plaintext_len, key, key_len, aad, aad_len, ciphertext);
}

void AesDecryptor::WipeOut() { impl_->WipeOut(); }

AesDecryptor::~AesDecryptor() { impl_->~AesDecryptorImpl(); }

AesDecryptor::AesDecryptorImpl::AesDecryptorImpl(ParquetCipher::type alg_id, int key_len,
                                                 bool metadata) {
  ctx_ = nullptr;

  ciphertext_size_delta_ = kBufferSizeLength + kNonceLength;
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

AesEncryptor* AesEncryptor::Make(
    ParquetCipher::type alg_id, int key_len, bool metadata,
    std::shared_ptr<std::vector<AesEncryptor*>> all_encryptors) {
  if (ParquetCipher::AES_GCM_V1 != alg_id && ParquetCipher::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw ParquetException(ss.str());
  }

  AesEncryptor* encryptor = new AesEncryptor(alg_id, key_len, metadata);
  if (all_encryptors != NULLPTR) {
    all_encryptors->push_back(encryptor);
  }
  return encryptor;
}

AesDecryptor::AesDecryptor(ParquetCipher::type alg_id, int key_len, bool metadata)
    : impl_{std::unique_ptr<AesDecryptorImpl>(
          new AesDecryptorImpl(alg_id, key_len, metadata))} {}

AesDecryptor* AesDecryptor::Make(
    ParquetCipher::type alg_id, int key_len, bool metadata,
    std::shared_ptr<std::vector<AesDecryptor*>> all_decryptors) {
  if (ParquetCipher::AES_GCM_V1 != alg_id && ParquetCipher::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw ParquetException(ss.str());
  }

  AesDecryptor* decryptor = new AesDecryptor(alg_id, key_len, metadata);
  if (all_decryptors != NULLPTR) {
    all_decryptors->push_back(decryptor);
  }
  return decryptor;
}

int AesDecryptor::CiphertextSizeDelta() { return impl_->ciphertext_size_delta(); }

int AesDecryptor::AesDecryptorImpl::GcmDecrypt(const uint8_t* ciphertext,
                                               int ciphertext_len, uint8_t* key,
                                               int key_len, uint8_t* aad, int aad_len,
                                               uint8_t* plaintext) {
  int len;
  int plaintext_len;

  uint8_t tag[kGcmTagLength];
  memset(tag, 0, kGcmTagLength);
  uint8_t nonce[kNonceLength];
  memset(nonce, 0, kNonceLength);

  // Extract ciphertext length
  int written_ciphertext_len = ((ciphertext[3] & 0xff) << 24) |
                               ((ciphertext[2] & 0xff) << 16) |
                               ((ciphertext[1] & 0xff) << 8) | ((ciphertext[0] & 0xff));

  if (ciphertext_len > 0 &&
      ciphertext_len != (written_ciphertext_len + kBufferSizeLength)) {
    throw ParquetException("Wrong ciphertext length");
  }
  ciphertext_len = written_ciphertext_len + kBufferSizeLength;

  // Extracting IV and tag
  std::copy(ciphertext + kBufferSizeLength, ciphertext + kBufferSizeLength + kNonceLength,
            nonce);
  std::copy(ciphertext + ciphertext_len - kGcmTagLength, ciphertext + ciphertext_len,
            tag);

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx_, nullptr, nullptr, key, nonce)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Setting additional authenticated data
  if ((nullptr != aad) && (1 != EVP_DecryptUpdate(ctx_, nullptr, &len, aad, aad_len))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Decryption
  if (!EVP_DecryptUpdate(
          ctx_, plaintext, &len, ciphertext + kBufferSizeLength + kNonceLength,
          ciphertext_len - kBufferSizeLength - kNonceLength - kGcmTagLength)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Checking the tag (authentication)
  if (!EVP_CIPHER_CTX_ctrl(ctx_, EVP_CTRL_GCM_SET_TAG, kGcmTagLength, tag)) {
    throw ParquetException("Failed authentication");
  }

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx_, plaintext + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int AesDecryptor::AesDecryptorImpl::CtrDecrypt(const uint8_t* ciphertext,
                                               int ciphertext_len, uint8_t* key,
                                               int key_len, uint8_t* plaintext) {
  int len;
  int plaintext_len;

  uint8_t iv[kCtrIvLength];
  memset(iv, 0, kCtrIvLength);

  // Extract ciphertext length
  int written_ciphertext_len = ((ciphertext[3] & 0xff) << 24) |
                               ((ciphertext[2] & 0xff) << 16) |
                               ((ciphertext[1] & 0xff) << 8) | ((ciphertext[0] & 0xff));

  if (ciphertext_len > 0 &&
      ciphertext_len != (written_ciphertext_len + kBufferSizeLength)) {
    throw ParquetException("Wrong ciphertext length");
  }
  ciphertext_len = written_ciphertext_len;

  // Extracting nonce
  std::copy(ciphertext + kBufferSizeLength, ciphertext + kBufferSizeLength + kNonceLength,
            iv);
  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial
  // counter field.
  // The first 31 bits of the initial counter field are set to 0, the last bit
  // is set to 1.
  iv[kCtrIvLength - 1] = 1;

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx_, nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Decryption
  if (!EVP_DecryptUpdate(ctx_, plaintext, &len,
                         ciphertext + kBufferSizeLength + kNonceLength,
                         ciphertext_len - kNonceLength)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx_, plaintext + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int AesDecryptor::AesDecryptorImpl::Decrypt(const uint8_t* ciphertext, int ciphertext_len,
                                            uint8_t* key, int key_len, uint8_t* aad,
                                            int aad_len, uint8_t* plaintext) {
  if (key_length_ != key_len) {
    std::stringstream ss;
    ss << "Wrong key length " << key_len << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  if (kGcmMode == aes_mode_) {
    return GcmDecrypt(ciphertext, ciphertext_len, key, key_len, aad, aad_len, plaintext);
  }

  return CtrDecrypt(ciphertext, ciphertext_len, key, key_len, plaintext);
}

static std::string ShortToBytesLe(int16_t input) {
  int8_t output[2];
  memset(output, 0, 2);
  output[1] = static_cast<int8_t>(0xff & (input >> 8));
  output[0] = static_cast<int8_t>(0xff & (input));

  return std::string(reinterpret_cast<char const*>(output), 2);
}

std::string CreateModuleAad(const std::string& file_aad, int8_t module_type,
                            int16_t row_group_ordinal, int16_t column_ordinal,
                            int16_t page_ordinal) {
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
  std::string page_ordinal_bytes = ShortToBytesLe(page_ordinal);
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
void QuickUpdatePageAad(const std::string& AAD, int16_t new_page_ordinal) {
  std::string page_ordinal_bytes = ShortToBytesLe(new_page_ordinal);
  int length = static_cast<int>(AAD.size());
  std::memcpy(reinterpret_cast<int16_t*>(const_cast<char*>(AAD.c_str() + length - 2)),
              reinterpret_cast<const int16_t*>(page_ordinal_bytes.c_str()), 2);
}

}  // namespace encryption
}  // namespace parquet

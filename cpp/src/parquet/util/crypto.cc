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

#include "parquet/util/crypto.h"
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <algorithm>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include "parquet/exception.h"

using parquet::ParquetException;

namespace parquet_encryption {

constexpr int aesGcm = 0;
constexpr int aesCtr = 1;
constexpr int encryptType = 0;
constexpr int decryptType = 1;
constexpr int gcmTagLen = 16;
constexpr int nonceLen = 12;
constexpr int ctrIvLen = 16;
constexpr int bufferSizeLen = 4;
constexpr int rndMaxBytes = 32;

#define ENCRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_EncryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    throw ParquetException("Couldn't init ALG encryption");           \
  }

#define DECRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_DecryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    throw ParquetException("Couldn't init ALG decryption");           \
  }

class EvpCipher {
 public:
  explicit EvpCipher(int cipher, int key_len, int type) {
    ctx_ = nullptr;

    if (aesGcm != cipher && aesCtr != cipher) {
      std::stringstream ss;
      ss << "Wrong cipher: " << cipher;
      throw ParquetException(ss.str());
    }

    if (16 != key_len && 24 != key_len && 32 != key_len) {
      std::stringstream ss;
      ss << "Wrong key length: " << key_len;
      throw ParquetException(ss.str());
    }

    if (encryptType != type && decryptType != type) {
      std::stringstream ss;
      ss << "Wrong cipher type: " << type;
      throw ParquetException(ss.str());
    }

    ctx_ = EVP_CIPHER_CTX_new();
    if (nullptr == ctx_) {
      throw ParquetException("Couldn't init cipher context");
    }

    if (aesGcm == cipher) {
      // Init AES-GCM with specified key length
      if (16 == key_len) {
        if (encryptType == type) {
          ENCRYPT_INIT(ctx_, EVP_aes_128_gcm());
        } else {
          DECRYPT_INIT(ctx_, EVP_aes_128_gcm());
        }
      } else if (24 == key_len) {
        if (encryptType == type) {
          ENCRYPT_INIT(ctx_, EVP_aes_192_gcm());
        } else {
          DECRYPT_INIT(ctx_, EVP_aes_192_gcm());
        }
      } else if (32 == key_len) {
        if (encryptType == type) {
          ENCRYPT_INIT(ctx_, EVP_aes_256_gcm());
        } else {
          DECRYPT_INIT(ctx_, EVP_aes_256_gcm());
        }
      }
    } else {
      // Init AES-CTR with specified key length
      if (16 == key_len) {
        if (encryptType == type) {
          ENCRYPT_INIT(ctx_, EVP_aes_128_ctr());
        } else {
          DECRYPT_INIT(ctx_, EVP_aes_128_ctr());
        }
      } else if (24 == key_len) {
        if (encryptType == type) {
          ENCRYPT_INIT(ctx_, EVP_aes_192_ctr());
        } else {
          DECRYPT_INIT(ctx_, EVP_aes_192_ctr());
        }
      } else if (32 == key_len) {
        if (encryptType == type) {
          ENCRYPT_INIT(ctx_, EVP_aes_256_ctr());
        } else {
          DECRYPT_INIT(ctx_, EVP_aes_256_ctr());
        }
      }
    }
  }

  EVP_CIPHER_CTX* get() { return ctx_; }

  ~EvpCipher() {
    if (nullptr != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
    }
  }

 private:
  EVP_CIPHER_CTX* ctx_;
};

int gcm_encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len,
                uint8_t* nonce, int nonce_len, uint8_t* aad, int aad_len, uint8_t* ciphertext) {
  int len;
  int ciphertext_len;

  uint8_t tag[gcmTagLen];
  memset(tag, 0, gcmTagLen);

  // Init cipher context
  EvpCipher cipher(aesGcm, key_len, encryptType);

  // Setting key and IV (nonce)
  if (1 != EVP_EncryptInit_ex(cipher.get(), nullptr, nullptr, key, nonce)) {
    throw ParquetException("Couldn't set key and nonce");
  }

  // Setting additional authenticated data
  if ((nullptr != aad) &&
      (1 != EVP_EncryptUpdate(cipher.get(), nullptr, &len, aad, aad_len))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Encryption
  if (1 != EVP_EncryptUpdate(cipher.get(), ciphertext + bufferSizeLen + nonceLen, 
                             &len, plaintext, plaintext_len)) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 != EVP_EncryptFinal_ex(cipher.get(), ciphertext + bufferSizeLen + nonce_len + len, &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Getting the tag
  if (1 != EVP_CIPHER_CTX_ctrl(cipher.get(), EVP_CTRL_GCM_GET_TAG, gcmTagLen, tag)) {
    throw ParquetException("Couldn't get AES-GCM tag");
  }

  // Copying the buffer size, nonce and tag to ciphertext
  int bufferSize = nonce_len + ciphertext_len + gcmTagLen;
  ciphertext[3] = (uint8_t)(0xff & (bufferSize >> 24));
  ciphertext[2] = (uint8_t)(0xff & (bufferSize >> 16));
  ciphertext[1] = (uint8_t)(0xff & (bufferSize >> 8));
  ciphertext[0] = (uint8_t)(0xff & (bufferSize));
  std::copy(nonce, nonce + nonce_len, ciphertext + bufferSizeLen);
  std::copy(tag, tag + gcmTagLen, ciphertext + bufferSizeLen + nonce_len + ciphertext_len);

  return bufferSizeLen + bufferSize;
}

int ctr_encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len,
                uint8_t* ciphertext) {
  int len;
  int ciphertext_len;
  
  uint8_t nonce[nonceLen];
  memset(nonce, 0, nonceLen);

  // Random nonce
  RAND_load_file("/dev/urandom", rndMaxBytes);
  RAND_bytes(nonce, sizeof(nonce));

  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial counter field. 
  // The first 31 bits of the initial counter field are set to 0, the last bit is set to 1.
  uint8_t iv[ctrIvLen];
  memset(iv, 0, ctrIvLen);
  std::copy(nonce, nonce + nonceLen, iv);
  iv[ctrIvLen - 1] = 1;

  // Init cipher context
  EvpCipher cipher(aesCtr, key_len, encryptType);

  // Setting key and IV
  if (1 != EVP_EncryptInit_ex(cipher.get(), nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Encryption
  if (1 != EVP_EncryptUpdate(cipher.get(), ciphertext + bufferSizeLen + ctrIvLen, &len, plaintext,
                             plaintext_len)) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 != EVP_EncryptFinal_ex(cipher.get(), ciphertext + bufferSizeLen + ctrIvLen + len, &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Copying the buffer size and nonce to ciphertext
  int bufferSize = nonceLen + ciphertext_len;
  ciphertext[3] = (uint8_t)(0xff & (bufferSize >> 24));
  ciphertext[2] = (uint8_t)(0xff & (bufferSize >> 16));
  ciphertext[1] = (uint8_t)(0xff & (bufferSize >> 8));
  ciphertext[0] = (uint8_t)(0xff & (bufferSize));
  std::copy(nonce, nonce + nonceLen, ciphertext + bufferSizeLen);

  return bufferSizeLen + bufferSize;
}

int SignedFooterEncrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, 
                        int key_len, uint8_t* aad, int aad_len, uint8_t* nonce, int nonce_len,
                        uint8_t* ciphertext) {

    return gcm_encrypt(plaintext, plaintext_len, key, key_len, nonce, nonce_len, aad, aad_len, ciphertext);
}

int Encrypt(Encryption::type alg_id, bool metadata, const uint8_t* plaintext,
            int plaintext_len, uint8_t* key, int key_len, uint8_t* aad, int aad_len,
            uint8_t* ciphertext) {
  if (Encryption::AES_GCM_V1 != alg_id && Encryption::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw ParquetException(ss.str());
  }

  if (metadata || (Encryption::AES_GCM_V1 == alg_id)) {
    uint8_t nonce[nonceLen];
    memset(nonce, 0, nonceLen);

    // Random nonce
    RAND_load_file("/dev/urandom", rndMaxBytes);
    RAND_bytes(nonce, sizeof(nonce));
    return gcm_encrypt(plaintext, plaintext_len, key, key_len, nonce, nonceLen, aad, aad_len, ciphertext);
  }

  // Data (page) encryption with AES_GCM_CTR_V1
  return ctr_encrypt(plaintext, plaintext_len, key, key_len, ciphertext);
}

int Encrypt(std::shared_ptr<EncryptionProperties> encryption_props, bool metadata,
            const uint8_t* plaintext, int plaintext_len, uint8_t* ciphertext) {
  return Encrypt(encryption_props->algorithm(), metadata, plaintext, plaintext_len,
                 encryption_props->key_bytes(), encryption_props->key_length(),
                 encryption_props->aad_bytes(), encryption_props->aad_length(),
                 ciphertext);
}

int gcm_decrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* key, int key_len,
                uint8_t* aad, int aad_len, uint8_t* plaintext) {
  int len;
  int plaintext_len;

  uint8_t tag[gcmTagLen];
  memset(tag, 0, gcmTagLen);
  uint8_t nonce[nonceLen];
  memset(nonce, 0, nonceLen);
  
  // Extract ciphertext length
  int written_ciphertext_len = 
        ((ciphertext[3] & 0xff) << 24) |
        ((ciphertext[2] & 0xff) << 16) |
        ((ciphertext[1] & 0xff) <<  8) |
        ((ciphertext[0] & 0xff));
        
  if (ciphertext_len > 0 && ciphertext_len != (written_ciphertext_len + bufferSizeLen)) {
    throw ParquetException("Wrong ciphertext length");
  }
  ciphertext_len = written_ciphertext_len + bufferSizeLen;

  // Extracting IV and tag
  std::copy(ciphertext + bufferSizeLen, ciphertext + bufferSizeLen + nonceLen, nonce);
  std::copy(ciphertext + ciphertext_len - gcmTagLen, ciphertext + ciphertext_len, tag);

  // Init cipher context
  EvpCipher cipher(aesGcm, key_len, decryptType);

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(cipher.get(), nullptr, nullptr, key, nonce)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Setting additional authenticated data
  if ((nullptr != aad) &&
      (1 != EVP_DecryptUpdate(cipher.get(), nullptr, &len, aad, aad_len))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Decryption
  if (!EVP_DecryptUpdate(cipher.get(), plaintext, &len, ciphertext + bufferSizeLen + nonceLen,
                         ciphertext_len - bufferSizeLen - nonceLen - gcmTagLen)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Checking the tag (authentication)
  if (!EVP_CIPHER_CTX_ctrl(cipher.get(), EVP_CTRL_GCM_SET_TAG, gcmTagLen, tag)) {
    throw ParquetException("Failed authentication");
  }

  // Finalization
  if (1 != EVP_DecryptFinal_ex(cipher.get(), plaintext + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int ctr_decrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* key, int key_len,
                uint8_t* plaintext) {
  int len;
  int plaintext_len;

  uint8_t iv[ctrIvLen];
  memset(iv, 0, ctrIvLen);
  
  // Extract ciphertext length
  int written_ciphertext_len = 
        ((ciphertext[3] & 0xff) << 24) |
        ((ciphertext[2] & 0xff) << 16) |
        ((ciphertext[1] & 0xff) <<  8) |
        ((ciphertext[0] & 0xff));
        
  if (ciphertext_len > 0 && ciphertext_len != (written_ciphertext_len + bufferSizeLen)) {
    throw ParquetException("Wrong ciphertext length");
  }
  ciphertext_len = written_ciphertext_len;

  // Extracting nonce
  std::copy(ciphertext + bufferSizeLen, ciphertext + bufferSizeLen + nonceLen, iv);
  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial counter field. 
  // The first 31 bits of the initial counter field are set to 0, the last bit is set to 1.
  iv[ctrIvLen - 1] = 1;

  // Init cipher context
  EvpCipher cipher(aesCtr, key_len, decryptType);

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(cipher.get(), nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Decryption
  if (!EVP_DecryptUpdate(cipher.get(), plaintext, &len, ciphertext + bufferSizeLen + ctrIvLen,
                         ciphertext_len - ctrIvLen)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Finalization
  if (1 != EVP_DecryptFinal_ex(cipher.get(), plaintext + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int Decrypt(Encryption::type alg_id, bool metadata, const uint8_t* ciphertext,
            int ciphertext_len, uint8_t* key, int key_len, uint8_t* aad, int aad_len,
            uint8_t* plaintext) {
  if (Encryption::AES_GCM_V1 != alg_id && Encryption::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw ParquetException(ss.str());
  }

  if (metadata || (Encryption::AES_GCM_V1 == alg_id)) {
    return gcm_decrypt(ciphertext, ciphertext_len, key, key_len, aad, aad_len, plaintext);
  }

  // Data (page) decryption with AES_GCM_CTR_V1
  return ctr_decrypt(ciphertext, ciphertext_len, key, key_len, plaintext);
}

int Decrypt(std::shared_ptr<EncryptionProperties> encryption_props, bool metadata,
            const uint8_t* ciphertext, int ciphertext_len, uint8_t* plaintext) {
  return Decrypt(encryption_props->algorithm(), metadata, ciphertext, ciphertext_len,
                 encryption_props->key_bytes(), encryption_props->key_length(),
                 encryption_props->aad_bytes(), encryption_props->aad_length(),
                 plaintext);
}

}  // namespace parquet_encryption

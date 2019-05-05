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
#include <openssl/rand.h>
#include <algorithm>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include "parquet/exception.h"

using parquet::ParquetException;

namespace parquet_encryption {

constexpr int GCM_MODE = 0;
constexpr int CTR_MODE = 1;
constexpr int CTRIvLength = 16;
constexpr int BufferSizeLength = 4;

#define ENCRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_EncryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    throw ParquetException("Couldn't init ALG encryption");           \
  }

#define DECRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_DecryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    throw ParquetException("Couldn't init ALG decryption");           \
  }

AesEncryptor::AesEncryptor(ParquetCipher::type alg_id, int key_len, bool metadata) {
  
  ctx_ = nullptr;

  if (ParquetCipher::AES_GCM_V1 != alg_id && ParquetCipher::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw ParquetException(ss.str());
  }

  ciphertext_size_delta_ = BufferSizeLength + NonceLength;
  if (metadata || (ParquetCipher::AES_GCM_V1 == alg_id)) {
    aes_mode_ = GCM_MODE;
    ciphertext_size_delta_ += GCMTagLength;
  } else {
    aes_mode_ = CTR_MODE;
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

  if (GCM_MODE == aes_mode_) {
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

int AesEncryptor::CiphertextSizeDelta() {
  return ciphertext_size_delta_;
}

int AesEncryptor::gcm_encrypt(const uint8_t* plaintext, int plaintext_len, 
                              uint8_t* key, int key_len, uint8_t* nonce, uint8_t* aad, 
                              int aad_len, uint8_t* ciphertext) {
  int len;
  int ciphertext_len;

  uint8_t tag[GCMTagLength];
  memset(tag, 0, GCMTagLength);

  // Setting key and IV (nonce)
  if (1 != EVP_EncryptInit_ex(ctx_, nullptr, nullptr, key, nonce)) {
    throw ParquetException("Couldn't set key and nonce");
  }

  // Setting additional authenticated data
  if ((nullptr != aad) &&
      (1 != EVP_EncryptUpdate(ctx_, nullptr, &len, aad, aad_len))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Encryption
  if (1 != EVP_EncryptUpdate(ctx_, ciphertext + BufferSizeLength + NonceLength, 
                             &len, plaintext, plaintext_len)) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 != EVP_EncryptFinal_ex(ctx_, ciphertext + BufferSizeLength + NonceLength + len, &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Getting the tag
  if (1 != EVP_CIPHER_CTX_ctrl(ctx_, EVP_CTRL_GCM_GET_TAG, GCMTagLength, tag)) {
    throw ParquetException("Couldn't get AES-GCM tag");
  }

  // Copying the buffer size, nonce and tag to ciphertext
  int bufferSize = NonceLength + ciphertext_len + GCMTagLength;
  ciphertext[3] = (uint8_t)(0xff & (bufferSize >> 24));
  ciphertext[2] = (uint8_t)(0xff & (bufferSize >> 16));
  ciphertext[1] = (uint8_t)(0xff & (bufferSize >> 8));
  ciphertext[0] = (uint8_t)(0xff & (bufferSize));
  std::copy(nonce, nonce + NonceLength, ciphertext + BufferSizeLength);
  std::copy(tag, tag + GCMTagLength, ciphertext + BufferSizeLength + NonceLength + ciphertext_len);

  return BufferSizeLength + bufferSize;
}


int AesEncryptor::ctr_encrypt(const uint8_t* plaintext, int plaintext_len, 
                              uint8_t* key, int key_len, uint8_t* nonce, 
                              uint8_t* ciphertext) {
  int len;
  int ciphertext_len;

  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial counter field. 
  // The first 31 bits of the initial counter field are set to 0, the last bit is set to 1.
  uint8_t iv[CTRIvLength];
  memset(iv, 0, CTRIvLength);
  std::copy(nonce, nonce + NonceLength, iv);
  iv[CTRIvLength - 1] = 1;

  // Setting key and IV
  if (1 != EVP_EncryptInit_ex(ctx_, nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Encryption
  if (1 != EVP_EncryptUpdate(ctx_, ciphertext + BufferSizeLength + CTRIvLength, &len, plaintext,
                             plaintext_len)) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if (1 != EVP_EncryptFinal_ex(ctx_, ciphertext + BufferSizeLength + CTRIvLength + len, &len)) {
    throw ParquetException("Failed encryption finalization");
  }

  ciphertext_len += len;

  // Copying the buffer size and nonce to ciphertext
  int bufferSize = NonceLength + ciphertext_len;
  ciphertext[3] = (uint8_t)(0xff & (bufferSize >> 24));
  ciphertext[2] = (uint8_t)(0xff & (bufferSize >> 16));
  ciphertext[1] = (uint8_t)(0xff & (bufferSize >> 8));
  ciphertext[0] = (uint8_t)(0xff & (bufferSize));
  std::copy(nonce, nonce + NonceLength, ciphertext + BufferSizeLength);

  return BufferSizeLength + bufferSize;
}

int AesEncryptor::SignedFooterEncrypt(const uint8_t* footer, int footer_len, 
                                       uint8_t* key, int key_len,  uint8_t* aad, int aad_len,
                                       uint8_t* nonce, uint8_t* encrypted_footer) {

  if (key_length_ != key_len) {
    std::stringstream ss;
    ss << "Wrong key length " << key_len << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  if (GCM_MODE != aes_mode_) {
    throw ParquetException("Must use AES GCM (metadata) encryptor");
  }

  return gcm_encrypt(footer, footer_len, key, key_len, nonce, aad, aad_len, encrypted_footer); 
}

int AesEncryptor::Encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len, 
                          uint8_t* aad, int aad_len, uint8_t* ciphertext) {

  if (key_length_ != key_len) {
    std::stringstream ss;
    ss << "Wrong key length " << key_len << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  uint8_t nonce[NonceLength];
  memset(nonce, 0, NonceLength);
  // Random nonce
  RAND_bytes(nonce, sizeof(nonce));

  if (GCM_MODE == aes_mode_) {
    return gcm_encrypt(plaintext, plaintext_len, key, key_len, nonce, 
                       aad, aad_len, ciphertext);
  }

  return ctr_encrypt(plaintext, plaintext_len, key, key_len, nonce, ciphertext);
}

AesDecryptor::AesDecryptor(ParquetCipher::type alg_id, int key_len, bool metadata) {
  
  ctx_ = nullptr;

  if (ParquetCipher::AES_GCM_V1 != alg_id && ParquetCipher::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw ParquetException(ss.str());
  }

  ciphertext_size_delta_ = BufferSizeLength + NonceLength;
  if (metadata || (ParquetCipher::AES_GCM_V1 == alg_id)) {
    aes_mode_ = GCM_MODE;
    ciphertext_size_delta_ += GCMTagLength;
  } else {
   aes_mode_ = CTR_MODE;
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

  if (GCM_MODE == aes_mode_) {
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

int AesDecryptor::CiphertextSizeDelta() {
  return ciphertext_size_delta_;
}

int AesDecryptor::gcm_decrypt(const uint8_t* ciphertext, int ciphertext_len, 
                              uint8_t* key, int key_len, uint8_t* aad, int aad_len, 
                              uint8_t* plaintext) {
  int len;
  int plaintext_len;

  uint8_t tag[GCMTagLength];
  memset(tag, 0, GCMTagLength);
  uint8_t nonce[NonceLength];
  memset(nonce, 0, NonceLength);
  
  // Extract ciphertext length
  int written_ciphertext_len = 
        ((ciphertext[3] & 0xff) << 24) |
        ((ciphertext[2] & 0xff) << 16) |
        ((ciphertext[1] & 0xff) <<  8) |
        ((ciphertext[0] & 0xff));
        
  if (ciphertext_len > 0 && ciphertext_len != (written_ciphertext_len + BufferSizeLength)) {
    throw ParquetException("Wrong ciphertext length");
  }
  ciphertext_len = written_ciphertext_len + BufferSizeLength;

  // Extracting IV and tag
  std::copy(ciphertext + BufferSizeLength, ciphertext + BufferSizeLength + NonceLength, nonce);
  std::copy(ciphertext + ciphertext_len - GCMTagLength, ciphertext + ciphertext_len, tag);


  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx_, nullptr, nullptr, key, nonce)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Setting additional authenticated data
  if ((nullptr != aad) &&
      (1 != EVP_DecryptUpdate(ctx_, nullptr, &len, aad, aad_len))) {
    throw ParquetException("Couldn't set AAD");
  }

  // Decryption
  if (!EVP_DecryptUpdate(ctx_, plaintext, &len, ciphertext + BufferSizeLength + NonceLength,
                         ciphertext_len - BufferSizeLength - NonceLength - GCMTagLength)) {
    throw ParquetException("Failed decryption update");
  }

  plaintext_len = len;

  // Checking the tag (authentication)
  if (!EVP_CIPHER_CTX_ctrl(ctx_, EVP_CTRL_GCM_SET_TAG, GCMTagLength, tag)) {
    throw ParquetException("Failed authentication");
  }

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx_, plaintext + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int AesDecryptor::ctr_decrypt(const uint8_t* ciphertext, int ciphertext_len, 
                              uint8_t* key, int key_len, uint8_t* plaintext) {
  int len;
  int plaintext_len;

  uint8_t iv[CTRIvLength];
  memset(iv, 0, CTRIvLength);
  
  // Extract ciphertext length
  int written_ciphertext_len = 
        ((ciphertext[3] & 0xff) << 24) |
        ((ciphertext[2] & 0xff) << 16) |
        ((ciphertext[1] & 0xff) <<  8) |
        ((ciphertext[0] & 0xff));
        
  if (ciphertext_len > 0 && ciphertext_len != (written_ciphertext_len + BufferSizeLength)) {
    throw ParquetException("Wrong ciphertext length");
  }
  ciphertext_len = written_ciphertext_len;

  // Extracting nonce
  std::copy(ciphertext + BufferSizeLength, ciphertext + BufferSizeLength + NonceLength, iv);
  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial counter field. 
  // The first 31 bits of the initial counter field are set to 0, the last bit is set to 1.
  iv[CTRIvLength - 1] = 1;


  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx_, nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Decryption
  if (!EVP_DecryptUpdate(ctx_, plaintext, &len, ciphertext + BufferSizeLength + CTRIvLength,
                         ciphertext_len - CTRIvLength)) {
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

int AesDecryptor::Decrypt(const uint8_t* ciphertext, int ciphertext_len, 
                          uint8_t* key, int key_len, uint8_t* aad, int aad_len, 
                          uint8_t* plaintext) {

  if (key_length_ != key_len) {
    std::stringstream ss;
    ss << "Wrong key length " << key_len << ". Should be " << key_length_;
    throw ParquetException(ss.str());
  }

  if (GCM_MODE == aes_mode_) {
    return gcm_decrypt(ciphertext, ciphertext_len, key, key_len, aad, aad_len, plaintext);
  }

  return ctr_decrypt(ciphertext, ciphertext_len, key, key_len, plaintext);
}


static std::string shortToBytesLE(int16_t input) {
  int8_t output[2];
  memset(output, 0, 2);
  output[1] = (int8_t)(0xff & (input >> 8));
  output[0] = (int8_t)(0xff & (input));
  std::string output_str(reinterpret_cast<char const*>(output), 2) ;

  return output_str;
}

std::string createModuleAAD(const std::string& fileAAD, int8_t module_type,
			    int16_t row_group_ordinal, int16_t column_ordinal,
			    int16_t page_ordinal) {

  int8_t type_ordinal_bytes[1];
  type_ordinal_bytes[0] = module_type;
  std::string type_ordinal_bytes_str(reinterpret_cast<char const*>(type_ordinal_bytes), 1) ;
  if (Footer == module_type) {
    std::string result = fileAAD + type_ordinal_bytes_str;
    return result;
  }
  std::string row_group_ordinal_bytes = shortToBytesLE(row_group_ordinal);
  std::string column_ordinal_bytes = shortToBytesLE(column_ordinal);
  if (DataPage != module_type && DataPageHeader != module_type) {
    std::string result =  fileAAD + type_ordinal_bytes_str + row_group_ordinal_bytes
      + column_ordinal_bytes;
    return result;
  }
  std::string page_ordinal_bytes = shortToBytesLE(page_ordinal);
  std::string result = fileAAD + type_ordinal_bytes_str + row_group_ordinal_bytes
    + column_ordinal_bytes + page_ordinal_bytes;;
  return result;
}

std::string createFooterAAD(const std::string& aad_prefix_bytes) {
  return createModuleAAD(aad_prefix_bytes, Footer, (int16_t) -1, (int16_t) -1, (int16_t) -1);
}

// Update last two bytes with new page ordinal (instead of creating new page AAD from scratch)
void quickUpdatePageAAD(const std::string &AAD, int16_t new_page_ordinal) {
  std::string page_ordinal_bytes = shortToBytesLE(new_page_ordinal);
  int length = (int)AAD.size();
  std::memcpy((int16_t*)(AAD.c_str()+length-2), (int16_t*)(page_ordinal_bytes.c_str()), 2);
}

}  // namespace parquet_encryption

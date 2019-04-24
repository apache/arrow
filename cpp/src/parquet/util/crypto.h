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

#ifndef PARQUET_UTIL_CRYPTO_H
#define PARQUET_UTIL_CRYPTO_H

#include <memory>

#include "parquet/properties.h"
#include "parquet/types.h"
#include <openssl/evp.h>

using parquet::ParquetCipher;

namespace parquet_encryption {

constexpr int GCMTagLength = 16;
constexpr int NonceLength = 12;

// Module types
const int8_t Footer = 0;
const int8_t ColumnMetaData = 1;
const int8_t DataPage = 2;
const int8_t DictionaryPage = 3;
const int8_t DataPageHeader = 4;
const int8_t DictionaryPageHeader = 5;
const int8_t ColumnIndex = 6;
const int8_t OffsetIndex = 7;


class AesEncryptor {
 public:
  AesEncryptor(ParquetCipher::type alg_id, int key_len, bool metadata);

  int Encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len, 
              uint8_t* aad, int aad_len, uint8_t* ciphertext);

  int SignedFooterEncrypt(const uint8_t* footer, int footer_len, uint8_t* key, int key_len,
                           uint8_t* aad, int aad_len, uint8_t* nonce, uint8_t* encrypted_footer);
  
  ~AesEncryptor() {
    if (nullptr != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
    }
  }

 private:
  EVP_CIPHER_CTX* ctx_;
  int aes_mode_;
  int key_length_;

  int gcm_encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len,
                  uint8_t* nonce, uint8_t* aad, int aad_len, uint8_t* ciphertext);

  int ctr_encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* key, int key_len,
                  uint8_t* nonce, uint8_t* ciphertext);
};

class AesDecryptor {
 public:
  AesDecryptor(ParquetCipher::type alg_id, int key_len, bool metadata);

  int CiphertextSizeDelta();

  int Decrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* key, int key_len, 
              uint8_t* aad, int aad_len, uint8_t* plaintext);
  
  ~AesDecryptor() {
    if (nullptr != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
    }
  }

 private:
  EVP_CIPHER_CTX* ctx_;
  int aes_mode_;
  int key_length_;
  int ciphertext_size_delta_;

  int gcm_decrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* key, int key_len,
                  uint8_t* aad, int aad_len, uint8_t* plaintext);

  int ctr_decrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* key, int key_len,
                  uint8_t* plaintext);
};


std::string createModuleAAD(const std::string& fileAAD, int8_t module_type,
			    int16_t row_group_ordinal, int16_t column_ordinal,
			    int16_t page_ordinal);

std::string createFooterAAD(const std::string& aad_prefix_bytes);

void quickUpdatePageAAD(const std::string &AAD, int16_t new_page_ordinal);

}  // namespace parquet_encryption

#endif  // PARQUET_UTIL_CRYPTO_H

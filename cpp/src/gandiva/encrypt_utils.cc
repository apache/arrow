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

#include "gandiva/encrypt_utils.h"

#include <stdexcept>

namespace gandiva {
GANDIVA_EXPORT
int32_t aes_encrypt(const char* plaintext, int32_t plaintext_len, const char* key,
                    unsigned char* cipher) {
  int32_t cipher_len = 0;
  int32_t len = 0;
  EVP_CIPHER_CTX* en_ctx = EVP_CIPHER_CTX_new();

  if (!en_ctx) {
    throw std::runtime_error("could not create a new evp cipher ctx for encryption");
  }

  if (!EVP_EncryptInit_ex(en_ctx, EVP_aes_128_ecb(), nullptr,
                          reinterpret_cast<const unsigned char*>(key), nullptr)) {
    throw std::runtime_error("could not initialize evp cipher ctx for encryption");
  }

  if (!EVP_EncryptUpdate(en_ctx, cipher, &len,
                         reinterpret_cast<const unsigned char*>(plaintext),
                         plaintext_len)) {
    throw std::runtime_error("could not update evp cipher ctx for encryption");
  }

  cipher_len += len;

  if (!EVP_EncryptFinal_ex(en_ctx, cipher + len, &len)) {
    throw std::runtime_error("could not finish evp cipher ctx for encryption");
  }

  cipher_len += len;

  EVP_CIPHER_CTX_free(en_ctx);
  return cipher_len;
}

GANDIVA_EXPORT
int32_t aes_decrypt(const char* ciphertext, int32_t ciphertext_len, const char* key,
                    unsigned char* plaintext) {
  int32_t plaintext_len = 0;
  int32_t len = 0;
  EVP_CIPHER_CTX* de_ctx = EVP_CIPHER_CTX_new();

  if (!de_ctx) {
    throw std::runtime_error("could not create a new evp cipher ctx for decryption");
  }

  if (!EVP_DecryptInit_ex(de_ctx, EVP_aes_128_ecb(), nullptr,
                          reinterpret_cast<const unsigned char*>(key), nullptr)) {
    throw std::runtime_error("could not initialize evp cipher ctx for decryption");
  }

  if (!EVP_DecryptUpdate(de_ctx, plaintext, &len,
                         reinterpret_cast<const unsigned char*>(ciphertext),
                         ciphertext_len)) {
    throw std::runtime_error("could not update evp cipher ctx for decryption");
  }

  plaintext_len += len;

  if (!EVP_DecryptFinal_ex(de_ctx, plaintext + len, &len)) {
    throw std::runtime_error("could not finish evp cipher ctx for decryption");
  }

  plaintext_len += len;

  EVP_CIPHER_CTX_free(de_ctx);
  return plaintext_len;
}
}  // namespace gandiva

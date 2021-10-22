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

#include "encrypt_utils.h"

#include "gandiva/gdv_function_stubs.h"

namespace gandiva {

int32_t aes_encrypt(int64_t context, unsigned char* plaintext, int32_t plaintext_len,
                    unsigned char* key, unsigned char* cipher) {
  int32_t cipher_len = 0;
  int32_t len = 0;
  EVP_CIPHER_CTX* en_ctx = EVP_CIPHER_CTX_new();

  if (!en_ctx) {
    return 0;
  }

  if (!EVP_EncryptInit_ex(en_ctx, EVP_aes_128_ecb(), nullptr, key, nullptr)) {
    return 0;
  }

  if (!EVP_EncryptUpdate(en_ctx, cipher, &len, plaintext, plaintext_len)) {
    return 0;
  }

  cipher_len += len;

  if (!EVP_EncryptFinal_ex(en_ctx, cipher + len, &len)) {
    return 0;
  }

  cipher_len += len;

  EVP_CIPHER_CTX_free(en_ctx);
  return cipher_len;
}

int32_t aes_decrypt(int64_t context, unsigned char* ciphertext, int32_t ciphertext_len,
                    unsigned char* key, unsigned char* plaintext) {
  int32_t plaintext_len = 0;
  int32_t len = 0;
  EVP_CIPHER_CTX* de_ctx = EVP_CIPHER_CTX_new();

  if (!de_ctx) {
    return 0;
  }

  if (!EVP_DecryptInit_ex(de_ctx, EVP_aes_128_ecb(), nullptr, key, nullptr)) {
    return 0;
  }

  if (!EVP_DecryptUpdate(de_ctx, plaintext, &len, ciphertext, ciphertext_len)) {
    return 0;
  }

  plaintext_len += len;

  if (!EVP_DecryptFinal_ex(de_ctx, plaintext + len, &len)) {
    return 0;
  }

  plaintext_len += len;

  EVP_CIPHER_CTX_free(de_ctx);
  return plaintext_len;
}
}  // namespace gandiva

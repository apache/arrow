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

#include "gandiva/encrypt_utils_gcm.h"
#include <openssl/aes.h>
#include <openssl/err.h>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include "gandiva/encrypt_utils_common.h"

namespace gandiva {

namespace {

const EVP_CIPHER* get_gcm_cipher_algo(int32_t key_length) {
  switch (key_length) {
    case 16:
      return EVP_aes_128_gcm();
    case 24:
      return EVP_aes_192_gcm();
    case 32:
      return EVP_aes_256_gcm();
    default: {
      std::ostringstream oss;
      oss << "Unsupported key length for AES-GCM: " << key_length
          << " bytes. Supported lengths: 16, 24, 32 bytes";
      throw std::runtime_error(oss.str());
    }
  }
}

}  // namespace

GANDIVA_EXPORT
int32_t aes_encrypt_gcm(const char* plaintext, int32_t plaintext_len, const char* key,
                        int32_t key_len, const char* iv, int32_t iv_len, const char* aad,
                        int32_t aad_len, unsigned char* cipher) {
  if (iv_len <= 0) {
    throw std::runtime_error(
        "Invalid IV length for AES-GCM: IV length must be greater than 0");
  }

  int32_t cipher_len = 0;
  int32_t len = 0;
  // Resolve the algorithm first: it throws on an invalid key length and must not leak ctx
  const EVP_CIPHER* cipher_algo = get_gcm_cipher_algo(key_len);
  EVP_CIPHER_CTX* en_ctx = EVP_CIPHER_CTX_new();

  if (!en_ctx) {
    throw std::runtime_error("Could not create EVP cipher context for encryption: " +
                             get_openssl_error_string());
  }

  try {
    if (!EVP_EncryptInit_ex(en_ctx, cipher_algo, nullptr, nullptr, nullptr)) {
      throw std::runtime_error(
          "Could not initialize EVP cipher context for encryption: " +
          get_openssl_error_string());
    }

    // The IV length must be set before the IV itself is supplied; otherwise OpenSSL
    // consumes only the default 12 bytes of the IV and ignores the rest.
    if (!EVP_CIPHER_CTX_ctrl(en_ctx, EVP_CTRL_GCM_SET_IVLEN, iv_len, nullptr)) {
      throw std::runtime_error("Could not set GCM IV length: " +
                               get_openssl_error_string());
    }

    if (!EVP_EncryptInit_ex(en_ctx, nullptr, nullptr,
                            reinterpret_cast<const unsigned char*>(key),
                            reinterpret_cast<const unsigned char*>(iv))) {
      throw std::runtime_error("Could not set key and IV for encryption: " +
                               get_openssl_error_string());
    }

    // Process AAD if provided
    if (aad != nullptr && aad_len > 0) {
      if (!EVP_EncryptUpdate(en_ctx, nullptr, &len,
                             reinterpret_cast<const unsigned char*>(aad), aad_len)) {
        throw std::runtime_error("Could not process AAD for encryption: " +
                                 get_openssl_error_string());
      }
    }

    // Encrypt plaintext
    if (!EVP_EncryptUpdate(en_ctx, cipher, &len,
                           reinterpret_cast<const unsigned char*>(plaintext),
                           plaintext_len)) {
      throw std::runtime_error("Could not update EVP cipher context for encryption: " +
                               get_openssl_error_string());
    }

    cipher_len += len;

    // Finalize encryption
    if (!EVP_EncryptFinal_ex(en_ctx, cipher + len, &len)) {
      throw std::runtime_error("Could not finalize EVP cipher context for encryption: " +
                               get_openssl_error_string());
    }

    cipher_len += len;

    // Get the authentication tag and append it to ciphertext
    if (!EVP_CIPHER_CTX_ctrl(en_ctx, EVP_CTRL_GCM_GET_TAG, GCM_TAG_LENGTH,
                             cipher + cipher_len)) {
      throw std::runtime_error("Could not get GCM authentication tag: " +
                               get_openssl_error_string());
    }
    cipher_len += GCM_TAG_LENGTH;
  } catch (...) {
    EVP_CIPHER_CTX_free(en_ctx);
    throw;
  }

  EVP_CIPHER_CTX_free(en_ctx);
  return cipher_len;
}

GANDIVA_EXPORT
int32_t aes_decrypt_gcm(const char* ciphertext, int32_t ciphertext_len, const char* key,
                        int32_t key_len, const char* iv, int32_t iv_len, const char* aad,
                        int32_t aad_len, unsigned char* plaintext) {
  if (iv_len <= 0) {
    throw std::runtime_error(
        "Invalid IV length for AES-GCM: IV length must be greater than 0");
  }

  if (ciphertext_len < GCM_TAG_LENGTH) {
    throw std::runtime_error(
        "Ciphertext too short for AES-GCM: must be at least 16 bytes for tag");
  }

  int32_t plaintext_len = 0;
  int32_t len = 0;
  // Resolve the algorithm first: it throws on an invalid key length and must not leak ctx
  const EVP_CIPHER* cipher_algo = get_gcm_cipher_algo(key_len);
  EVP_CIPHER_CTX* de_ctx = EVP_CIPHER_CTX_new();

  if (!de_ctx) {
    throw std::runtime_error("Could not create EVP cipher context for decryption: " +
                             get_openssl_error_string());
  }

  try {
    if (!EVP_DecryptInit_ex(de_ctx, cipher_algo, nullptr, nullptr, nullptr)) {
      throw std::runtime_error(
          "Could not initialize EVP cipher context for decryption: " +
          get_openssl_error_string());
    }

    // The IV length must be set before the IV itself is supplied (see encryption).
    if (!EVP_CIPHER_CTX_ctrl(de_ctx, EVP_CTRL_GCM_SET_IVLEN, iv_len, nullptr)) {
      throw std::runtime_error("Could not set GCM IV length: " +
                               get_openssl_error_string());
    }

    if (!EVP_DecryptInit_ex(de_ctx, nullptr, nullptr,
                            reinterpret_cast<const unsigned char*>(key),
                            reinterpret_cast<const unsigned char*>(iv))) {
      throw std::runtime_error("Could not set key and IV for decryption: " +
                               get_openssl_error_string());
    }

    // Process AAD if provided
    if (aad != nullptr && aad_len > 0) {
      if (!EVP_DecryptUpdate(de_ctx, nullptr, &len,
                             reinterpret_cast<const unsigned char*>(aad), aad_len)) {
        throw std::runtime_error("Could not process AAD for decryption: " +
                                 get_openssl_error_string());
      }
    }

    // Extract tag from end of ciphertext
    int32_t actual_ciphertext_len = ciphertext_len - GCM_TAG_LENGTH;
    const unsigned char* tag =
        reinterpret_cast<const unsigned char*>(ciphertext + actual_ciphertext_len);

    // Set the authentication tag
    if (!EVP_CIPHER_CTX_ctrl(de_ctx, EVP_CTRL_GCM_SET_TAG, GCM_TAG_LENGTH,
                             const_cast<unsigned char*>(tag))) {
      throw std::runtime_error("Could not set GCM authentication tag: " +
                               get_openssl_error_string());
    }

    // Decrypt ciphertext
    if (!EVP_DecryptUpdate(de_ctx, plaintext, &len,
                           reinterpret_cast<const unsigned char*>(ciphertext),
                           actual_ciphertext_len)) {
      throw std::runtime_error("Could not update EVP cipher context for decryption: " +
                               get_openssl_error_string());
    }

    plaintext_len += len;

    // Finalize decryption (this verifies the tag)
    if (!EVP_DecryptFinal_ex(de_ctx, plaintext + len, &len)) {
      throw std::runtime_error("GCM tag verification failed or decryption error: " +
                               get_openssl_error_string());
    }
    plaintext_len += len;
  } catch (...) {
    EVP_CIPHER_CTX_free(de_ctx);
    throw;
  }

  EVP_CIPHER_CTX_free(de_ctx);
  return plaintext_len;
}

}  // namespace gandiva

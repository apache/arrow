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

#include <openssl/evp.h>
#include <cstdint>
#include "gandiva/visibility.h"

namespace gandiva {

// GCM mode identifier
constexpr const char* AES_GCM_MODE = "AES-GCM";

// GCM authentication tag length in bytes
constexpr int32_t GCM_TAG_LENGTH = 16;

/**
 * Encrypt data using AES-GCM algorithm
 *
 * @param plaintext The data to encrypt
 * @param plaintext_len Length of plaintext in bytes
 * @param key The encryption key (16, 24, or 32 bytes for 128, 192, 256-bit keys)
 * @param key_len Length of key in bytes
 * @param iv The initialization vector (variable length, typically 12 bytes)
 * @param iv_len Length of IV in bytes
 * @param aad Optional additional authenticated data (can be null)
 * @param aad_len Length of AAD in bytes (0 if aad is null)
 * @param cipher Output buffer for encrypted data (must be at least plaintext_len + 16
 * bytes)
 * @return Length of encrypted data in bytes (plaintext_len + 16 for the tag)
 * @throws std::runtime_error on encryption failure or invalid parameters
 */
GANDIVA_EXPORT
int32_t aes_encrypt_gcm(const char* plaintext, int32_t plaintext_len, const char* key,
                        int32_t key_len, const char* iv, int32_t iv_len, const char* aad,
                        int32_t aad_len, unsigned char* cipher);

/**
 * Decrypt data using AES-GCM algorithm
 *
 * @param ciphertext The data to decrypt (includes 16-byte authentication tag at the end)
 * @param ciphertext_len Length of ciphertext in bytes (includes tag)
 * @param key The decryption key (16, 24, or 32 bytes for 128, 192, 256-bit keys)
 * @param key_len Length of key in bytes
 * @param iv The initialization vector (variable length, typically 12 bytes)
 * @param iv_len Length of IV in bytes
 * @param aad Optional additional authenticated data (can be null)
 * @param aad_len Length of AAD in bytes (0 if aad is null)
 * @param plaintext Output buffer for decrypted data
 * @return Length of decrypted data in bytes (ciphertext_len - 16)
 * @throws std::runtime_error on decryption failure, invalid parameters, or tag
 * verification failure
 */
GANDIVA_EXPORT
int32_t aes_decrypt_gcm(const char* ciphertext, int32_t ciphertext_len, const char* key,
                        int32_t key_len, const char* iv, int32_t iv_len, const char* aad,
                        int32_t aad_len, unsigned char* plaintext);

}  // namespace gandiva

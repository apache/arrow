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

// CBC mode identifiers
constexpr const char* AES_CBC_MODE = "AES-CBC";
constexpr const char* AES_CBC_PKCS7_MODE = "AES-CBC-PKCS7";
constexpr const char* AES_CBC_NONE_MODE = "AES-CBC-NONE";

/**
 * Encrypt data using AES-CBC algorithm with explicit padding mode
 *
 * @param plaintext The data to encrypt
 * @param plaintext_len Length of plaintext in bytes
 * @param key The encryption key (16, 24, or 32 bytes for 128, 192, 256-bit keys)
 * @param key_len Length of key in bytes
 * @param iv The initialization vector (must be exactly 16 bytes)
 * @param iv_len Length of IV in bytes (must be 16)
 * @param use_padding Whether to use PKCS7 padding (true) or no padding (false)
 * @param cipher Output buffer for encrypted data
 * @return Length of encrypted data in bytes
 * @throws std::runtime_error on encryption failure or invalid parameters
 */
GANDIVA_EXPORT
int32_t aes_encrypt_cbc(const char* plaintext, int32_t plaintext_len, const char* key,
                        int32_t key_len, const char* iv, int32_t iv_len, bool use_padding,
                        unsigned char* cipher);

/**
 * Decrypt data using AES-CBC algorithm with explicit padding mode
 *
 * @param ciphertext The data to decrypt
 * @param ciphertext_len Length of ciphertext in bytes
 * @param key The decryption key (16, 24, or 32 bytes for 128, 192, 256-bit keys)
 * @param key_len Length of key in bytes
 * @param iv The initialization vector (must be exactly 16 bytes)
 * @param iv_len Length of IV in bytes (must be 16)
 * @param use_padding Whether to use PKCS7 padding (true) or no padding (false)
 * @param plaintext Output buffer for decrypted data
 * @return Length of decrypted data in bytes
 * @throws std::runtime_error on decryption failure or invalid parameters
 */
GANDIVA_EXPORT
int32_t aes_decrypt_cbc(const char* ciphertext, int32_t ciphertext_len, const char* key,
                        int32_t key_len, const char* iv, int32_t iv_len, bool use_padding,
                        unsigned char* plaintext);

}  // namespace gandiva

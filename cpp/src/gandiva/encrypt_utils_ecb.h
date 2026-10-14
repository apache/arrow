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

// ECB mode identifiers
constexpr const char* AES_ECB_MODE = "AES-ECB";
constexpr const char* AES_ECB_PKCS7_MODE = "AES-ECB-PKCS7";
constexpr const char* AES_ECB_NONE_MODE = "AES-ECB-NONE";

/**
 * Encrypt data using AES-ECB algorithm (legacy, insecure)
 *
 * WARNING: ECB mode is deterministic and should not be used for sensitive data.
 * Use other encryption modes for better security.
 *
 * @param plaintext The data to encrypt
 * @param plaintext_len Length of plaintext in bytes
 * @param key The encryption key (16, 24, or 32 bytes for 128, 192, 256-bit keys)
 * @param key_len Length of key in bytes
 * @param use_padding Whether to use PKCS7 padding (true) or no padding (false)
 * @param cipher Output buffer for encrypted data
 * @return Length of encrypted data in bytes
 * @throws std::runtime_error on encryption failure
 */
GANDIVA_EXPORT
int32_t aes_encrypt_ecb(const char* plaintext, int32_t plaintext_len, const char* key,
                        int32_t key_len, bool use_padding, unsigned char* cipher);

/**
 * Decrypt data using AES-ECB algorithm (legacy, insecure)
 *
 * WARNING: ECB mode is deterministic and should not be used for sensitive data.
 * Use other encryption modes for better security.
 *
 * @param ciphertext The data to decrypt
 * @param ciphertext_len Length of ciphertext in bytes
 * @param key The decryption key (16, 24, or 32 bytes for 128, 192, 256-bit keys)
 * @param key_len Length of key in bytes
 * @param use_padding Whether to use PKCS7 padding (true) or no padding (false)
 * @param plaintext Output buffer for decrypted data
 * @return Length of decrypted data in bytes
 * @throws std::runtime_error on decryption failure
 */
GANDIVA_EXPORT
int32_t aes_decrypt_ecb(const char* ciphertext, int32_t ciphertext_len, const char* key,
                        int32_t key_len, bool use_padding, unsigned char* plaintext);

}  // namespace gandiva

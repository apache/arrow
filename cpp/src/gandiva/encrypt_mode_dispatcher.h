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

#include <cstdint>

namespace gandiva {

/**
 * Dispatcher for AES encryption/decryption based on mode string.
 * Routes calls to appropriate implementation.
 */
class EncryptModeDispatcher {
 public:
  /**
   * Encrypt data using the specified mode
   *
   * @param plaintext The data to encrypt
   * @param plaintext_len Length of plaintext in bytes
   * @param key The encryption key
   * @param key_len Length of key in bytes
   * @param mode Mode string
   * @param mode_len Length of mode string in bytes
   * @param iv The initialization vector (optional, only for modes that support it)
   * @param iv_len Length of the IV in bytes
   * @param aad Additional authenticated data (optional, only used by GCM)
   * @param aad_len Length of aad in bytes
   * @param cipher Output buffer for encrypted data
   * @return Length of encrypted data in bytes
   * @throws std::runtime_error on encryption failure or unsupported mode
   */
  static int32_t encrypt(const char* plaintext, int32_t plaintext_len, const char* key,
                         int32_t key_len, const char* mode, int32_t mode_len,
                         const char* iv, int32_t iv_len, const char* aad, int32_t aad_len,
                         unsigned char* cipher);

  /**
   * Decrypt data using the specified mode
   *
   * @param ciphertext The data to decrypt
   * @param ciphertext_len Length of ciphertext in bytes
   * @param key The decryption key
   * @param key_len Length of key in bytes
   * @param mode Mode string
   * @param mode_len Length of mode string in bytes
   * @param iv The initialization vector (optional, only for modes that support it)
   * @param iv_len Length of the IV in bytes
   * @param aad Additional authenticated data (optional, only used by GCM)
   * @param aad_len Length of aad in bytes
   * @param plaintext Output buffer for decrypted data
   * @return Length of decrypted data in bytes
   * @throws std::runtime_error on decryption failure or unsupported mode
   */
  static int32_t decrypt(const char* ciphertext, int32_t ciphertext_len, const char* key,
                         int32_t key_len, const char* mode, int32_t mode_len,
                         const char* iv, int32_t iv_len, const char* aad, int32_t aad_len,
                         unsigned char* plaintext);
};

}  // namespace gandiva

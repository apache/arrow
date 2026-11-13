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

#include "gandiva/encrypt_mode_dispatcher.h"
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include "arrow/util/string.h"
#include "gandiva/encrypt_utils_cbc.h"
#include "gandiva/encrypt_utils_ecb.h"
#include "gandiva/encrypt_utils_gcm.h"

namespace gandiva {

namespace {

// Supported encryption modes
const std::vector<std::string_view> SUPPORTED_MODES = {
    AES_ECB_MODE,       AES_ECB_PKCS7_MODE, AES_ECB_NONE_MODE, AES_CBC_MODE,
    AES_CBC_PKCS7_MODE, AES_CBC_NONE_MODE,  AES_GCM_MODE};

// Internal enum for dispatching to the right encryption implementation
enum class EncryptionMode {
  ECB,
  ECB_PKCS7,
  ECB_NONE,
  CBC,
  CBC_PKCS7,
  CBC_NONE,
  GCM,
  UNKNOWN
};

EncryptionMode ParseEncryptionMode(std::string_view mode_str) {
  if (mode_str == AES_ECB_MODE) return EncryptionMode::ECB;
  if (mode_str == AES_ECB_PKCS7_MODE) return EncryptionMode::ECB_PKCS7;
  if (mode_str == AES_ECB_NONE_MODE) return EncryptionMode::ECB_NONE;
  if (mode_str == AES_CBC_MODE) return EncryptionMode::CBC;
  if (mode_str == AES_CBC_PKCS7_MODE) return EncryptionMode::CBC_PKCS7;
  if (mode_str == AES_CBC_NONE_MODE) return EncryptionMode::CBC_NONE;
  if (mode_str == AES_GCM_MODE) return EncryptionMode::GCM;
  return EncryptionMode::UNKNOWN;
}

}  // namespace

int32_t EncryptModeDispatcher::encrypt(const char* plaintext, int32_t plaintext_len,
                                       const char* key, int32_t key_len, const char* mode,
                                       int32_t mode_len, const char* iv, int32_t iv_len,
                                       const char* aad, int32_t aad_len,
                                       unsigned char* cipher) {
  std::string mode_str = arrow::internal::AsciiToUpper(std::string_view(mode, mode_len));

  switch (ParseEncryptionMode(mode_str)) {
    case EncryptionMode::ECB:
    case EncryptionMode::ECB_PKCS7:
      // Shorthand AES-ECB and explicit AES-ECB-PKCS7 both use ECB with PKCS7 padding
      return aes_encrypt_ecb(plaintext, plaintext_len, key, key_len, true, cipher);
    case EncryptionMode::ECB_NONE:
      // ECB without padding
      return aes_encrypt_ecb(plaintext, plaintext_len, key, key_len, false, cipher);
    case EncryptionMode::CBC:
    case EncryptionMode::CBC_PKCS7:
      // Shorthand AES-CBC and explicit AES-CBC-PKCS7 both use CBC with PKCS7
      return aes_encrypt_cbc(plaintext, plaintext_len, key, key_len, iv, iv_len, true,
                             cipher);
    case EncryptionMode::CBC_NONE:
      // CBC without padding
      return aes_encrypt_cbc(plaintext, plaintext_len, key, key_len, iv, iv_len, false,
                             cipher);
    case EncryptionMode::GCM:
      return aes_encrypt_gcm(plaintext, plaintext_len, key, key_len, iv, iv_len, aad,
                             aad_len, cipher);
    case EncryptionMode::UNKNOWN:
    default: {
      std::string modes_str = arrow::internal::JoinStrings(SUPPORTED_MODES, ", ");
      std::ostringstream oss;
      oss << "Unsupported encryption mode: " << mode_str
          << ". Supported modes: " << modes_str;
      throw std::runtime_error(oss.str());
    }
  }
}

int32_t EncryptModeDispatcher::decrypt(const char* ciphertext, int32_t ciphertext_len,
                                       const char* key, int32_t key_len, const char* mode,
                                       int32_t mode_len, const char* iv, int32_t iv_len,
                                       const char* aad, int32_t aad_len,
                                       unsigned char* plaintext) {
  std::string mode_str = arrow::internal::AsciiToUpper(std::string_view(mode, mode_len));

  switch (ParseEncryptionMode(mode_str)) {
    case EncryptionMode::ECB:
    case EncryptionMode::ECB_PKCS7:
      // Shorthand AES-ECB and explicit AES-ECB-PKCS7 both use ECB with PKCS7 padding
      return aes_decrypt_ecb(ciphertext, ciphertext_len, key, key_len, true, plaintext);
    case EncryptionMode::ECB_NONE:
      // ECB without padding
      return aes_decrypt_ecb(ciphertext, ciphertext_len, key, key_len, false, plaintext);
    case EncryptionMode::CBC:
    case EncryptionMode::CBC_PKCS7:
      // Shorthand AES-CBC and explicit AES-CBC-PKCS7 both use CBC with PKCS7
      return aes_decrypt_cbc(ciphertext, ciphertext_len, key, key_len, iv, iv_len, true,
                             plaintext);
    case EncryptionMode::CBC_NONE:
      // CBC without padding
      return aes_decrypt_cbc(ciphertext, ciphertext_len, key, key_len, iv, iv_len, false,
                             plaintext);
    case EncryptionMode::GCM:
      return aes_decrypt_gcm(ciphertext, ciphertext_len, key, key_len, iv, iv_len, aad,
                             aad_len, plaintext);
    case EncryptionMode::UNKNOWN:
    default: {
      std::string modes_str = arrow::internal::JoinStrings(SUPPORTED_MODES, ", ");
      std::ostringstream oss;
      oss << "Unsupported decryption mode: " << mode_str
          << ". Supported modes: " << modes_str;
      throw std::runtime_error(oss.str());
    }
  }
}

}  // namespace gandiva

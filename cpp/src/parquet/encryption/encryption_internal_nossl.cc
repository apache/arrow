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

#include "parquet/encryption/encryption_internal.h"
#include "parquet/exception.h"

namespace parquet::encryption {

void ThrowOpenSSLRequiredException() {
  throw ParquetException(
      "Calling encryption method in Arrow/Parquet built without OpenSSL");
}

class AesEncryptor::AesEncryptorImpl {};

AesEncryptor::~AesEncryptor() {}

int AesEncryptor::SignedFooterEncrypt(::arrow::util::span<const uint8_t> footer,
                                      ::arrow::util::span<const uint8_t> key,
                                      ::arrow::util::span<const uint8_t> aad,
                                      ::arrow::util::span<const uint8_t> nonce,
                                      ::arrow::util::span<uint8_t> encrypted_footer) {
  ThrowOpenSSLRequiredException();
  return -1;
}

void AesEncryptor::WipeOut() { ThrowOpenSSLRequiredException(); }

int32_t AesEncryptor::CiphertextLength(int64_t plaintext_len) const {
  ThrowOpenSSLRequiredException();
  return -1;
}

int AesEncryptor::Encrypt(::arrow::util::span<const uint8_t> plaintext,
                          ::arrow::util::span<const uint8_t> key,
                          ::arrow::util::span<const uint8_t> aad,
                          ::arrow::util::span<uint8_t> ciphertext) {
  ThrowOpenSSLRequiredException();
  return -1;
}

AesEncryptor::AesEncryptor(ParquetCipher::type alg_id, int key_len, bool metadata,
                           bool write_length) {
  ThrowOpenSSLRequiredException();
}

class AesDecryptor::AesDecryptorImpl {};

int AesDecryptor::Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                          ::arrow::util::span<const uint8_t> key,
                          ::arrow::util::span<const uint8_t> aad,
                          ::arrow::util::span<uint8_t> plaintext) {
  ThrowOpenSSLRequiredException();
  return -1;
}

void AesDecryptor::WipeOut() { ThrowOpenSSLRequiredException(); }

AesDecryptor::~AesDecryptor() {}

AesEncryptor* AesEncryptor::Make(ParquetCipher::type alg_id, int key_len, bool metadata,
                                 std::vector<AesEncryptor*>* all_encryptors) {
  return NULLPTR;
}

AesEncryptor* AesEncryptor::Make(ParquetCipher::type alg_id, int key_len, bool metadata,
                                 bool write_length,
                                 std::vector<AesEncryptor*>* all_encryptors) {
  return NULLPTR;
}

AesDecryptor::AesDecryptor(ParquetCipher::type alg_id, int key_len, bool metadata,
                           bool contains_length) {
  ThrowOpenSSLRequiredException();
}

std::shared_ptr<AesDecryptor> AesDecryptor::Make(
    ParquetCipher::type alg_id, int key_len, bool metadata,
    std::vector<std::weak_ptr<AesDecryptor>>* all_decryptors) {
  return NULLPTR;
}

int AesDecryptor::PlaintextLength(int ciphertext_len) const {
  ThrowOpenSSLRequiredException();
  return -1;
}

int AesDecryptor::CiphertextLength(int plaintext_len) const {
  ThrowOpenSSLRequiredException();
  return -1;
}

std::string CreateModuleAad(const std::string& file_aad, int8_t module_type,
                            int16_t row_group_ordinal, int16_t column_ordinal,
                            int32_t page_ordinal) {
  ThrowOpenSSLRequiredException();
  return "";
}

std::string CreateFooterAad(const std::string& aad_prefix_bytes) {
  ThrowOpenSSLRequiredException();
  return "";
}

void QuickUpdatePageAad(int32_t new_page_ordinal, std::string* AAD) {
  ThrowOpenSSLRequiredException();
}

void RandBytes(unsigned char* buf, int num) { ThrowOpenSSLRequiredException(); }

void EnsureBackendInitialized() {}

}  // namespace parquet::encryption

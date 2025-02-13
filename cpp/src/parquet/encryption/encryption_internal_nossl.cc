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

int32_t AesEncryptor::SignedFooterEncrypt(::arrow::util::span<const uint8_t> footer,
                                          ::arrow::util::span<const uint8_t> key,
                                          ::arrow::util::span<const uint8_t> aad,
                                          ::arrow::util::span<const uint8_t> nonce,
                                          ::arrow::util::span<uint8_t> encrypted_footer) {
  ThrowOpenSSLRequiredException();
  return -1;
}

int32_t AesEncryptor::CiphertextLength(int64_t plaintext_len) const {
  ThrowOpenSSLRequiredException();
  return -1;
}

int32_t AesEncryptor::Encrypt(::arrow::util::span<const uint8_t> plaintext,
                              ::arrow::util::span<const uint8_t> key,
                              ::arrow::util::span<const uint8_t> aad,
                              ::arrow::util::span<uint8_t> ciphertext) {
  ThrowOpenSSLRequiredException();
  return -1;
}

AesEncryptor::AesEncryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                           bool write_length) {
  ThrowOpenSSLRequiredException();
}

class AesDecryptor::AesDecryptorImpl {};

int32_t AesDecryptor::Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                              ::arrow::util::span<const uint8_t> key,
                              ::arrow::util::span<const uint8_t> aad,
                              ::arrow::util::span<uint8_t> plaintext) {
  ThrowOpenSSLRequiredException();
  return -1;
}

AesDecryptor::~AesDecryptor() {}

std::unique_ptr<AesEncryptor> AesEncryptor::Make(ParquetCipher::type alg_id,
                                                 int32_t key_len, bool metadata,
                                                 bool write_length) {
  ThrowOpenSSLRequiredException();
  return NULLPTR;
}

AesDecryptor::AesDecryptor(ParquetCipher::type alg_id, int32_t key_len, bool metadata,
                           bool contains_length) {
  ThrowOpenSSLRequiredException();
}

std::unique_ptr<AesDecryptor> AesDecryptor::Make(ParquetCipher::type alg_id,
                                                 int32_t key_len, bool metadata) {
  ThrowOpenSSLRequiredException();
  return NULLPTR;
}

int32_t AesDecryptor::PlaintextLength(int32_t ciphertext_len) const {
  ThrowOpenSSLRequiredException();
  return -1;
}

int32_t AesDecryptor::CiphertextLength(int32_t plaintext_len) const {
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

void RandBytes(unsigned char* buf, size_t num) { ThrowOpenSSLRequiredException(); }

void EnsureBackendInitialized() {}

}  // namespace parquet::encryption

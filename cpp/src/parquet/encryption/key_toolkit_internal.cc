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

#include "arrow/util/base64.h"
#include "arrow/util/secure_string.h"

#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/key_toolkit_internal.h"

using arrow::util::SecureString;

namespace parquet::encryption::internal {

// Acceptable key lengths in number of bits, used to validate the data key lengths
// configured by users and the master key lengths fetched from KMS server.
static constexpr const int32_t kAcceptableDataKeyLengths[] = {128, 192, 256};

std::string EncryptKeyLocally(const SecureString& key_bytes,
                              const SecureString& master_key, const std::string& aad) {
  AesEncryptor key_encryptor(ParquetCipher::AES_GCM_V1,
                             static_cast<int>(master_key.size()), false,
                             false /*write_length*/);

  int32_t encrypted_key_len =
      key_encryptor.CiphertextLength(static_cast<int64_t>(key_bytes.size()));
  std::string encrypted_key(encrypted_key_len, '\0');
  ::arrow::util::span<uint8_t> encrypted_key_span(
      reinterpret_cast<uint8_t*>(&encrypted_key[0]), encrypted_key_len);

  encrypted_key_len = key_encryptor.Encrypt(key_bytes.as_span(), master_key.as_span(),
                                            str2span(aad), encrypted_key_span);

  return ::arrow::util::base64_encode(
      ::std::string_view(encrypted_key.data(), encrypted_key_len));
}

SecureString DecryptKeyLocally(const std::string& encoded_encrypted_key,
                               const SecureString& master_key, const std::string& aad) {
  std::string encrypted_key = ::arrow::util::base64_decode(encoded_encrypted_key);

  AesDecryptor key_decryptor(ParquetCipher::AES_GCM_V1,
                             static_cast<int>(master_key.size()), false,
                             false /*contains_length*/);

  int32_t decrypted_key_len =
      key_decryptor.PlaintextLength(static_cast<int>(encrypted_key.size()));
  SecureString decrypted_key(decrypted_key_len, '\0');

  decrypted_key_len = key_decryptor.Decrypt(str2span(encrypted_key), master_key.as_span(),
                                            str2span(aad), decrypted_key.as_span());

  return decrypted_key;
}

bool ValidateKeyLength(int32_t key_length_bits) {
  int32_t* found_key_length = std::find(
      const_cast<int32_t*>(kAcceptableDataKeyLengths),
      const_cast<int32_t*>(std::end(kAcceptableDataKeyLengths)), key_length_bits);
  return found_key_length != std::end(kAcceptableDataKeyLengths);
}

}  // namespace parquet::encryption::internal

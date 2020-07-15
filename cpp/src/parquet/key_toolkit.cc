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

#include "parquet/key_toolkit.h"
#include "parquet/encryption_internal.h"

#include "arrow/util/base64.h"

namespace parquet {

namespace encryption {

std::string KeyToolkit::EncryptKeyLocally(const uint8_t* key_bytes, int key_size,
                                          const uint8_t* master_key_bytes,
                                          int master_key_size, const uint8_t* aad_bytes,
                                          int aad_size) {
  AesEncryptor key_encryptor(ParquetCipher::AES_GCM_V1, master_key_size, false);

  int encrypted_key_len = key_size + key_encryptor.CiphertextSizeDelta();
  std::vector<uint8_t> encrypted_key(encrypted_key_len);
  encrypted_key_len =
      key_encryptor.Encrypt(key_bytes, key_size, master_key_bytes, master_key_size,
                            aad_bytes, aad_size, encrypted_key.data());

  return arrow::util::base64_encode(encrypted_key.data(), encrypted_key_len);
}

std::vector<uint8_t> KeyToolkit::DecryptKeyLocally(
    const std::string& encoded_encrypted_key, const uint8_t* master_key_bytes,
    int master_key_size, const uint8_t* aad_bytes, int aad_size) {
  std::string encrypted_key = arrow::util::base64_decode(encoded_encrypted_key);

  AesDecryptor key_decryptor(ParquetCipher::AES_GCM_V1, master_key_size, false);

  int decrypted_key_len =
      encoded_encrypted_key.size() - key_decryptor.CiphertextSizeDelta();
  std::vector<uint8_t> decrypted_key(decrypted_key_len);
  decrypted_key_len = key_decryptor.Decrypt(
      reinterpret_cast<const uint8_t*>(&encrypted_key[0]), encrypted_key.size(),
      master_key_bytes, master_key_size, aad_bytes, aad_size, decrypted_key.data());

  return decrypted_key;
}

}  // namespace encryption

}  // namespace parquet

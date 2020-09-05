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

std::shared_ptr<KmsClient> KeyToolkit::GetKmsClient(
    std::shared_ptr<KmsClientFactory> kms_client_factory,
    const KmsConnectionConfig& kms_connection_config, bool is_wrap_locally,
    uint64_t cache_entry_lifetime_ms) {
  std::unordered_map<std::string, std::shared_ptr<KmsClient>>&
      kms_client_per_kms_instance_cache =
          kms_client_cache_per_token().GetOrCreateInternalCache(
              kms_connection_config.key_access_token(), cache_entry_lifetime_ms);

  if (kms_client_per_kms_instance_cache.find(kms_connection_config.kms_instance_id) ==
      kms_client_per_kms_instance_cache.end()) {
    std::shared_ptr<KmsClient> kms_client =
        kms_client_factory->CreateKmsClient(kms_connection_config);
    kms_client_per_kms_instance_cache.insert(
        {kms_connection_config.kms_instance_id, kms_client});
  }

  return kms_client_per_kms_instance_cache[kms_connection_config.kms_instance_id];
}

std::string KeyToolkit::EncryptKeyLocally(const std::string& key_bytes,
                                          const std::string& master_key,
                                          const std::string& aad) {
  AesEncryptor key_encryptor(ParquetCipher::AES_GCM_V1,
                             static_cast<int>(master_key.size()), false);

  int encrypted_key_len =
      static_cast<int>(key_bytes.size()) + key_encryptor.CiphertextSizeDelta();
  std::string encrypted_key(encrypted_key_len, '\0');
  encrypted_key_len = key_encryptor.Encrypt(
      reinterpret_cast<const uint8_t*>(key_bytes.data()),
      static_cast<int>(key_bytes.size()),
      reinterpret_cast<const uint8_t*>(master_key.data()),
      static_cast<int>(master_key.size()), reinterpret_cast<const uint8_t*>(aad.data()),
      static_cast<int>(aad.size()), reinterpret_cast<uint8_t*>(&encrypted_key[0]));

  std::string encoded_encrypted_key = arrow::util::base64_encode(
      reinterpret_cast<const uint8_t*>(&encrypted_key[0]), encrypted_key_len);
  return encoded_encrypted_key;
}

std::string KeyToolkit::DecryptKeyLocally(const std::string& encoded_encrypted_key,
                                          const std::string& master_key,
                                          const std::string& aad) {
  std::string encrypted_key = arrow::util::base64_decode(encoded_encrypted_key);

  AesDecryptor key_decryptor(ParquetCipher::AES_GCM_V1,
                             static_cast<int>(master_key.size()), false);

  int decrypted_key_len =
      static_cast<int>(encrypted_key.size()) - key_decryptor.CiphertextSizeDelta();
  std::string decrypted_key(decrypted_key_len, '\0');
  decrypted_key_len = key_decryptor.Decrypt(
      reinterpret_cast<const uint8_t*>(encrypted_key.data()),
      static_cast<int>(encrypted_key.size()),
      reinterpret_cast<const uint8_t*>(master_key.data()),
      static_cast<int>(master_key.size()), reinterpret_cast<const uint8_t*>(aad.data()),
      static_cast<int>(aad.size()), reinterpret_cast<uint8_t*>(&decrypted_key[0]));

  return decrypted_key;
}

// Flush any caches that are tied to the (compromised) access_token
void KeyToolkit::RemoveCacheEntriesForToken(const std::string& access_token) {
  kms_client_cache_per_token().RemoveCacheEntriesForToken(access_token);
  kek_write_cache_per_token().RemoveCacheEntriesForToken(access_token);
  kek_read_cache_per_token().RemoveCacheEntriesForToken(access_token);
}

void KeyToolkit::RemoveCacheEntriesForAllTokens() {
  kms_client_cache_per_token().Clear();
  kek_write_cache_per_token().Clear();
  kek_read_cache_per_token().Clear();
}

}  // namespace encryption
}  // namespace parquet

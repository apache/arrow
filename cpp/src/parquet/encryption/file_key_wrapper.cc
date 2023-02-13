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

#include "parquet/encryption/file_key_wrapper.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/key_material.h"
#include "parquet/encryption/key_metadata.h"
#include "parquet/encryption/key_toolkit_internal.h"
#include "parquet/exception.h"

namespace parquet {
namespace encryption {

FileKeyWrapper::FileKeyWrapper(KeyToolkit* key_toolkit,
                               const KmsConnectionConfig& kms_connection_config,
                               std::shared_ptr<FileKeyMaterialStore> key_material_store,
                               double cache_entry_lifetime_seconds, bool double_wrapping)
    : kms_connection_config_(kms_connection_config),
      key_material_store_(key_material_store),
      cache_entry_lifetime_seconds_(cache_entry_lifetime_seconds),
      double_wrapping_(double_wrapping),
      key_counter_(0) {
  kms_connection_config_.SetDefaultIfEmpty();
  // Check caches upon each file writing (clean once in cache_entry_lifetime_seconds_)
  key_toolkit->kms_client_cache_per_token().CheckCacheForExpiredTokens(
      cache_entry_lifetime_seconds_);
  kms_client_ =
      key_toolkit->GetKmsClient(kms_connection_config, cache_entry_lifetime_seconds_);

  if (double_wrapping) {
    key_toolkit->kek_write_cache_per_token().CheckCacheForExpiredTokens(
        cache_entry_lifetime_seconds_);
    kek_per_master_key_id_ =
        key_toolkit->kek_write_cache_per_token().GetOrCreateInternalCache(
            kms_connection_config.key_access_token(), cache_entry_lifetime_seconds_);
  }
}

std::string FileKeyWrapper::GetEncryptionKeyMetadata(const std::string& data_key,
                                                     const std::string& master_key_id,
                                                     bool is_footer_key,
                                                     std::string key_id_in_file) {
  if (kms_client_ == NULL) {
    throw ParquetException("No KMS client available. See previous errors.");
  }

  std::string encoded_kek_id;
  std::string encoded_wrapped_kek;
  std::string encoded_wrapped_dek;
  if (!double_wrapping_) {
    encoded_wrapped_dek = kms_client_->WrapKey(data_key, master_key_id);
  } else {
    // Find in cache, or generate KEK for Master Key ID
    KeyEncryptionKey key_encryption_key = kek_per_master_key_id_->GetOrInsert(
        master_key_id, [this, master_key_id]() -> KeyEncryptionKey {
          return this->CreateKeyEncryptionKey(master_key_id);
        });
    // Encrypt DEK with KEK
    const std::string& aad = key_encryption_key.kek_id();
    const std::string& kek_bytes = key_encryption_key.kek_bytes();
    encoded_wrapped_dek = internal::EncryptKeyLocally(data_key, kek_bytes, aad);
    encoded_kek_id = key_encryption_key.encoded_kek_id();
    encoded_wrapped_kek = key_encryption_key.encoded_wrapped_kek();
  }

  bool store_key_material_internally = (nullptr == key_material_store_);

  std::string serialized_key_material =
      KeyMaterial::SerializeToJson(is_footer_key, kms_connection_config_.kms_instance_id,
                                   kms_connection_config_.kms_instance_url, master_key_id,
                                   double_wrapping_, encoded_kek_id, encoded_wrapped_kek,
                                   encoded_wrapped_dek, store_key_material_internally);

  // Internal key material storage: key metadata and key material are the same
  if (store_key_material_internally) {
    return serialized_key_material;
  }
  // External key material storage: key metadata is a reference to a key in the material
  // store
  if (key_id_in_file.empty()) {
    // The key id may be specified explicitly to support key rotation, but usually
    // we generate an arbitrary identifier that just needs to be unique across
    // columns and the footer.
    if (is_footer_key) {
      key_id_in_file = KeyMaterial::kFooterKeyIdInFile;
    } else {
      // Generate a new unique identifier using an incrementing counter
      key_id_in_file =
          KeyMaterial::kColumnKeyIdInFilePrefix + std::to_string(key_counter_);
      key_counter_++;
    }
  }
  key_material_store_->AddKeyMaterial(key_id_in_file, serialized_key_material);
  std::string serialized_key_metadata =
      KeyMetadata::CreateSerializedForExternalMaterial(key_id_in_file);
  return serialized_key_metadata;
}

KeyEncryptionKey FileKeyWrapper::CreateKeyEncryptionKey(
    const std::string& master_key_id) {
  std::string kek_bytes(kKeyEncryptionKeyLength, '\0');
  RandBytes(reinterpret_cast<uint8_t*>(&kek_bytes[0]), kKeyEncryptionKeyLength);

  std::string kek_id(kKeyEncryptionKeyIdLength, '\0');
  RandBytes(reinterpret_cast<uint8_t*>(&kek_id[0]), kKeyEncryptionKeyIdLength);

  // Encrypt KEK with Master key
  std::string encoded_wrapped_kek = kms_client_->WrapKey(kek_bytes, master_key_id);

  return KeyEncryptionKey(kek_bytes, kek_id, encoded_wrapped_kek);
}

}  // namespace encryption
}  // namespace parquet

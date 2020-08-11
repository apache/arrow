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

#include "parquet/file_key_wrapper.h"
#include "parquet/encryption_internal.h"
#include "parquet/exception.h"
#include "parquet/key_material.h"
#include "parquet/key_metadata.h"
#include "parquet/key_toolkit.h"

namespace parquet {
namespace encryption {

FileKeyWrapper::FileKeyWrapper(std::shared_ptr<KmsClientFactory> kms_client_factory,
                               const KmsConnectionConfig& kms_connection_config,
                               std::shared_ptr<FileKeyMaterialStore> key_material_store,
                               uint64_t cache_entry_lifetime, bool double_wrapping,
                               bool is_wrap_locally)
    : kms_connection_config_(kms_connection_config),
      key_material_store_(key_material_store),
      cache_entry_lifetime_(cache_entry_lifetime),
      double_wrapping_(double_wrapping),
      key_counter_(0) {
  kms_connection_config_.SetDefaultIfEmpty();
  // Check caches upon each file writing (clean once in cache_entry_lifetime_)
  KeyToolkit::kms_client_cache_per_token().CheckCacheForExpiredTokens(
      cache_entry_lifetime_);
  kms_client_ = KeyToolkit::GetKmsClient(kms_client_factory, kms_connection_config,
                                         is_wrap_locally, cache_entry_lifetime_);

  if (double_wrapping) {
    KeyToolkit::kek_write_cache_per_token().CheckCacheForExpiredTokens(
        cache_entry_lifetime_);
    kek_per_master_key_id_ =
        KeyToolkit::kek_write_cache_per_token().GetOrCreateInternalCache(
            kms_connection_config.key_access_token(), cache_entry_lifetime_);
  }
}

std::string FileKeyWrapper::GetEncryptionKeyMetadata(const std::vector<uint8_t>& data_key,
                                                     const std::string& master_key_id,
                                                     bool is_footer_key) {
  return GetEncryptionKeyMetadata(data_key, master_key_id, is_footer_key, "");
}

std::string FileKeyWrapper::GetEncryptionKeyMetadata(const std::vector<uint8_t>& data_key,
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
    if (kek_per_master_key_id_.find(master_key_id) == kek_per_master_key_id_.end()) {
      kek_per_master_key_id_.insert(
          {master_key_id, CreateKeyEncryptionKey(master_key_id)});
    }
    const KeyEncryptionKey& key_encryption_key = kek_per_master_key_id_[master_key_id];

    // Encrypt DEK with KEK
    const std::vector<uint8_t>& aad = key_encryption_key.kek_id();
    const std::vector<uint8_t>& kek_bytes = key_encryption_key.kek_bytes();
    encoded_wrapped_dek = KeyToolkit::EncryptKeyLocally(data_key, kek_bytes, aad);
    encoded_kek_id = key_encryption_key.encoded_kek_id();
    encoded_wrapped_kek = key_encryption_key.encoded_wrapped_kek();
  }

  bool store_key_material_internally = (NULL == key_material_store_);

  std::string serialized_key_material = KeyMaterial::CreateSerialized(
      is_footer_key, kms_connection_config_.kms_instance_id,
      kms_connection_config_.kms_instance_url, master_key_id, double_wrapping_,
      encoded_kek_id, encoded_wrapped_kek, encoded_wrapped_dek,
      store_key_material_internally);

  // Internal key material storage: key metadata and key material are the same
  if (store_key_material_internally) {
    return serialized_key_material;
  }

  // External key material storage: key metadata is a reference to a key in the material
  // store
  if (key_id_in_file.empty()) {
    if (is_footer_key) {
      key_id_in_file = KeyMaterial::FOOTER_KEY_ID_IN_FILE;
    } else {
      key_id_in_file = KeyMaterial::COLUMN_KEY_ID_IN_FILE_PREFIX + key_counter_;
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
  std::vector<uint8_t> kek_bytes(KEK_LENGTH);
  RandBytes(kek_bytes.data(), KEK_LENGTH);

  std::vector<uint8_t> kek_id(KEK_ID_LENGTH);
  RandBytes(kek_id.data(), KEK_ID_LENGTH);

  // Encrypt KEK with Master key
  std::string encoded_wrapped_kek = kms_client_->WrapKey(kek_bytes, master_key_id);

  return KeyEncryptionKey(kek_bytes, kek_id, encoded_wrapped_kek);
}

}  // namespace encryption
}  // namespace parquet

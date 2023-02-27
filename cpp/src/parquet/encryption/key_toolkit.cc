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

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/result.h"

#include "parquet/encryption/file_key_unwrapper.h"
#include "parquet/encryption/file_key_wrapper.h"
#include "parquet/encryption/key_material.h"
#include "parquet/encryption/key_toolkit.h"

#include "parquet/encryption/file_system_key_material_store.h"
#include "parquet/encryption/key_toolkit_internal.h"

namespace parquet {
namespace encryption {

std::shared_ptr<KmsClient> KeyToolkit::GetKmsClient(
    const KmsConnectionConfig& kms_connection_config, double cache_entry_lifetime_ms) {
  if (kms_client_factory_ == NULL) {
    throw ParquetException("No KmsClientFactory is registered.");
  }
  auto kms_client_per_kms_instance_cache =
      kms_client_cache_per_token().GetOrCreateInternalCache(
          kms_connection_config.key_access_token(), cache_entry_lifetime_ms);

  return kms_client_per_kms_instance_cache->GetOrInsert(
      kms_connection_config.kms_instance_id, [this, kms_connection_config]() {
        return this->kms_client_factory_->CreateKmsClient(kms_connection_config);
      });
}

void KeyToolkit::RotateMasterKeys(
    const KmsConnectionConfig& kms_connection_config,
    const std::string& parquet_file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system, bool double_wrapping,
    double cache_lifetime_seconds) {
  // If process wrote files with double-wrapped keys, clean KEK cache (since master keys
  // are changing). Only once for each key rotation cycle; not for every file.
  const auto now = internal::CurrentTimePoint();
  auto lock = last_cache_clean_for_key_rotation_time_mutex_.Lock();
  if (now > last_cache_clean_for_key_rotation_time_ +
                std::chrono::duration<double>(kCacheCleanPeriodForKeyRotation)) {
    kek_write_cache_per_token().Clear();
    last_cache_clean_for_key_rotation_time_ = now;
  }
  lock.Unlock();

  std::shared_ptr<FileKeyMaterialStore> key_material_store =
      FileSystemKeyMaterialStore::Make(parquet_file_path, file_system, false);

  // Unwrapper for decrypting encrypted keys
  FileKeyUnwrapper file_key_unwrapper(this, kms_connection_config, cache_lifetime_seconds,
                                      key_material_store);

  // Create a temporary store to hold new key material during rotation,
  // and wrapper that will write material to this store when getting key metadata.
  std::shared_ptr<FileKeyMaterialStore> temp_key_material_store =
      FileSystemKeyMaterialStore::Make(parquet_file_path, file_system, true);
  FileKeyWrapper file_key_wrapper(this, kms_connection_config, temp_key_material_store,
                                  cache_lifetime_seconds, double_wrapping);

  std::vector<std::string> file_key_id_set = key_material_store->GetKeyIDSet();

  // When writing new encryption material, we re-use the same key identifiers
  // so that key metadata does not need to change.
  // Start with footer key (to get KMS ID, URL, if needed).
  // We can rely on the footer key using a standardised key identifier.
  std::string footer_key_id_str = std::string(KeyMaterial::kFooterKeyIdInFile);
  std::string key_material_string = key_material_store->GetKeyMaterial(footer_key_id_str);
  KeyWithMasterId key =
      file_key_unwrapper.GetDataEncryptionKey(KeyMaterial::Parse(key_material_string));
  file_key_wrapper.GetEncryptionKeyMetadata(key.data_key(), key.master_id(), true,
                                            std::string(KeyMaterial::kFooterKeyIdInFile));

  // Rotate column keys
  for (auto const& key_id_in_file : file_key_id_set) {
    if (key_id_in_file == std::string(KeyMaterial::kFooterKeyIdInFile)) {
      continue;
    }
    key_material_string = key_material_store->GetKeyMaterial(key_id_in_file);
    KeyWithMasterId column_key =
        file_key_unwrapper.GetDataEncryptionKey(KeyMaterial::Parse(key_material_string));
    file_key_wrapper.GetEncryptionKeyMetadata(
        column_key.data_key(), column_key.master_id(), false, key_id_in_file);
  }

  // Save material to the temporary store then move it to the original store location
  temp_key_material_store->SaveMaterial();
  key_material_store->RemoveMaterial();
  temp_key_material_store->MoveMaterialTo(key_material_store);
}

// Flush any caches that are tied to the (compromised) access_token
void KeyToolkit::RemoveCacheEntriesForToken(const std::string& access_token) {
  kms_client_cache_per_token().Remove(access_token);
  kek_write_cache_per_token().Remove(access_token);
  kek_read_cache_per_token().Remove(access_token);
}

void KeyToolkit::RemoveCacheEntriesForAllTokens() {
  kms_client_cache_per_token().Clear();
  kek_write_cache_per_token().Clear();
  kek_read_cache_per_token().Clear();
}

}  // namespace encryption
}  // namespace parquet

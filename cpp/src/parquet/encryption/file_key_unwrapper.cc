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

#include <iostream>

#include "arrow/util/utf8.h"

#include "parquet/encryption/file_key_unwrapper.h"
#include "parquet/encryption/key_metadata.h"

namespace parquet {
namespace encryption {

using internal::KeyWithMasterId;

FileKeyUnwrapper::FileKeyUnwrapper(KeyToolkit* key_toolkit,
                                   const KmsConnectionConfig& kms_connection_config,
                                   double cache_lifetime_seconds)
    : key_toolkit_(key_toolkit),
      kms_connection_config_(kms_connection_config),
      cache_entry_lifetime_seconds_(cache_lifetime_seconds) {
  kek_per_kek_id_ = key_toolkit_->kek_read_cache_per_token().GetOrCreateInternalCache(
      kms_connection_config.key_access_token(), cache_entry_lifetime_seconds_);
}

std::string FileKeyUnwrapper::GetKey(const std::string& key_metadata_bytes) {
  // key_metadata is expected to be in UTF8 encoding
  ::arrow::util::InitializeUTF8();
  if (!::arrow::util::ValidateUTF8(
          reinterpret_cast<const uint8_t*>(key_metadata_bytes.data()),
          key_metadata_bytes.size())) {
    throw ParquetException("key metadata should be in UTF8 encoding");
  }
  KeyMetadata key_metadata = KeyMetadata::Parse(key_metadata_bytes);

  if (!key_metadata.key_material_stored_internally()) {
    throw ParquetException("External key material store is not supported yet.");
  }

  const KeyMaterial& key_material = key_metadata.key_material();

  return GetDataEncryptionKey(key_material).data_key();
}

KeyWithMasterId FileKeyUnwrapper::GetDataEncryptionKey(const KeyMaterial& key_material) {
  auto kms_client = GetKmsClientFromConfigOrKeyMaterial(key_material);

  bool double_wrapping = key_material.is_double_wrapped();
  const std::string& master_key_id = key_material.master_key_id();
  const std::string& encoded_wrapped_dek = key_material.wrapped_dek();

  std::string data_key;
  if (!double_wrapping) {
    data_key = kms_client->UnwrapKey(encoded_wrapped_dek, master_key_id);
  } else {
    // Get Key Encryption Key
    const std::string& encoded_kek_id = key_material.kek_id();
    const std::string& encoded_wrapped_kek = key_material.wrapped_kek();

    std::string kek_bytes = kek_per_kek_id_->GetOrInsert(
        encoded_kek_id, [kms_client, encoded_wrapped_kek, master_key_id]() {
          return kms_client->UnwrapKey(encoded_wrapped_kek, master_key_id);
        });

    // Decrypt the data key
    std::string aad = ::arrow::util::base64_decode(encoded_kek_id);
    data_key = internal::DecryptKeyLocally(encoded_wrapped_dek, kek_bytes, aad);
  }

  return KeyWithMasterId(data_key, master_key_id);
}

std::shared_ptr<KmsClient> FileKeyUnwrapper::GetKmsClientFromConfigOrKeyMaterial(
    const KeyMaterial& key_material) {
  std::string& kms_instance_id = kms_connection_config_.kms_instance_id;
  if (kms_instance_id.empty()) {
    kms_instance_id = key_material.kms_instance_id();
    if (kms_instance_id.empty()) {
      throw ParquetException(
          "KMS instance ID is missing both in both kms connection configuration and file "
          "key material");
    }
  }

  std::string& kms_instance_url = kms_connection_config_.kms_instance_url;
  if (kms_instance_url.empty()) {
    kms_instance_url = key_material.kms_instance_url();
    if (kms_instance_url.empty()) {
      throw ParquetException(
          "KMS instance ID is missing both in both kms connection configuration and file "
          "key material");
    }
  }

  return key_toolkit_->GetKmsClient(kms_connection_config_,
                                    cache_entry_lifetime_seconds_);
}

}  // namespace encryption
}  // namespace parquet

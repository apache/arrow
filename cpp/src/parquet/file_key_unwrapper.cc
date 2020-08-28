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

#include "parquet/file_key_unwrapper.h"
#include "parquet/key_metadata.h"
#include "parquet/key_toolkit.h"

namespace parquet {
namespace encryption {

FileKeyUnwrapper::FileKeyUnwrapper(std::shared_ptr<KmsClientFactory> kms_client_factory,
                                   const KmsConnectionConfig& kms_connection_config,
                                   uint64_t cache_lifetime_seconds, bool is_wrap_locally)
    : kms_client_factory_(kms_client_factory),
      kms_connection_config_(kms_connection_config),
      cache_entry_lifetime_ms_(1000 * cache_lifetime_seconds),
      is_wrap_locally_(is_wrap_locally) {
  kms_connection_config.refreshable_key_access_token->SetDefaultIfEmpty();
}

std::string FileKeyUnwrapper::GetKey(const std::string& key_metadata_bytes) const {
  KeyMetadata key_metadata = KeyMetadata::Parse(key_metadata_bytes);

  if (!key_metadata.key_material_stored_internally()) {
    throw ParquetException("External key material store is not supported yet.");
  }

  const KeyMaterial& key_material = key_metadata.key_material();

  return GetDataEncryptionKey(key_material).data_key();
}

KeyWithMasterId FileKeyUnwrapper::GetDataEncryptionKey(
    const KeyMaterial& key_material) const {
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

    if (kek_per_kek_id_.find(encoded_kek_id) == kek_per_kek_id_.end()) {
      kek_per_kek_id_.insert(
          {encoded_kek_id, kms_client->UnwrapKey(encoded_wrapped_kek, master_key_id)});
    }

    std::string kek_bytes = kek_per_kek_id_[encoded_kek_id];

    // Decrypt the data key
    std::string aad = arrow::util::base64_decode(encoded_kek_id);
    data_key = KeyToolkit::DecryptKeyLocally(encoded_wrapped_dek, kek_bytes, aad);
  }

  return KeyWithMasterId(data_key, master_key_id);
}

std::shared_ptr<KmsClient> FileKeyUnwrapper::GetKmsClientFromConfigOrKeyMaterial(
    const KeyMaterial& key_material) const {
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

  return KeyToolkit::GetKmsClient(kms_client_factory_, kms_connection_config_,
                                  is_wrap_locally_, cache_entry_lifetime_ms_);
}

}  // namespace encryption
}  // namespace parquet

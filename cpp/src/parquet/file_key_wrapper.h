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

#pragma once

#include <map>
#include <memory>
#include <string>

#include "arrow/buffer.h"

#include "parquet/file_key_material_store.h"
#include "parquet/key_encryption_key.h"
#include "parquet/kms_client.h"
#include "parquet/kms_client_factory.h"

namespace parquet {

namespace encryption {

class FileKeyWrapper {
 public:
  static constexpr int KEK_LENGTH = 16;
  static constexpr int KEK_ID_LENGTH = 16;

  static constexpr int RND_MAX_BYTES = 32;

  FileKeyWrapper(std::shared_ptr<KmsClientFactory> kms_client_factory,
                 const KmsConnectionConfig& kms_connection_config,
                 std::shared_ptr<FileKeyMaterialStore> key_material_store,
                 uint64_t cache_entry_lifetime, bool double_wrapping,
                 bool is_wrap_locally);

  std::string GetEncryptionKeyMetadata(const std::vector<uint8_t>& data_key,
                                       const std::string& master_key_id,
                                       bool is_footer_key);

  std::string GetEncryptionKeyMetadata(const std::vector<uint8_t>& data_key,
                                       const std::string& master_key_id,
                                       bool is_footer_key, std::string key_id_in_file);

 private:
  KeyEncryptionKey CreateKeyEncryptionKey(const std::string& master_key_id);

  // A map of MEK_ID -> KeyEncryptionKey, for the current token
  std::map<std::string, KeyEncryptionKey> kek_per_master_key_id_;

  std::shared_ptr<KmsClient> kms_client_;
  KmsConnectionConfig kms_connection_config_;
  std::shared_ptr<FileKeyMaterialStore> key_material_store_;
  uint64_t cache_entry_lifetime_;
  bool double_wrapping_;
  uint16_t key_counter_;
};

}  // namespace encryption

}  // namespace parquet

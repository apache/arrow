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

#include "parquet/exception.h"
#include "parquet/in_memory_kms.h"
#include "parquet/key_toolkit.h"
#include "parquet/kms_client_factory.h"
#include "parquet/remote_kms_client.h"
#include "parquet/string_util.h"

using parquet::encryption::KeyToolkit;
using parquet::encryption::KmsClient;
using parquet::encryption::KmsClientFactory;
using parquet::encryption::KmsConnectionConfig;
using parquet::encryption::RemoteKmsClient;

namespace parquet {
namespace test {

std::map<std::string, std::vector<uint8_t>> InMemoryKms::master_key_map_;

void InMemoryKms::InitializeMasterKeys(
    const std::map<std::string, std::string>& master_keys_map) {
  master_key_map_ = ParseKeyList(master_keys_map);
}

void InMemoryKms::InitializeInternal() {}

std::string InMemoryKms::WrapKeyInServer(const std::vector<uint8_t>& key_bytes,
                                         const std::string& master_key_identifier) {
  // Always use the latest key version for writing
  if (master_key_map_.find(master_key_identifier) == master_key_map_.end()) {
    throw ParquetException("Key not found: " + master_key_identifier);
  }
  const std::vector<uint8_t>& master_key_bytes =
      master_key_map_.at(master_key_identifier);

  std::shared_ptr<arrow::Buffer> aad = arrow::Buffer::FromString(master_key_identifier);
  std::vector<uint8_t> aad_bytes(master_key_identifier.begin(),
                                 master_key_identifier.end());
  return KeyToolkit::EncryptKeyLocally(key_bytes, master_key_bytes, aad_bytes);
}

std::vector<uint8_t> InMemoryKms::UnwrapKeyInServer(
    const std::string& wrapped_key, const std::string& master_key_identifier) {
  if (master_key_map_.find(master_key_identifier) == master_key_map_.end()) {
    throw ParquetException("Key not found: " + master_key_identifier);
  }
  const std::vector<uint8_t>& master_key_bytes =
      master_key_map_.at(master_key_identifier);

  std::vector<uint8_t> aad_bytes(master_key_identifier.begin(),
                                 master_key_identifier.end());
  return KeyToolkit::DecryptKeyLocally(wrapped_key, master_key_bytes, aad_bytes);
}

std::vector<uint8_t> InMemoryKms::GetMasterKeyFromServer(
    const std::string& master_key_identifier) {
  // Always return the latest key version
  return master_key_map_.at(master_key_identifier);
}

std::map<std::string, std::vector<uint8_t>> InMemoryKms::ParseKeyList(
    const std::map<std::string, std::string>& master_keys_map) {
  std::map<std::string, std::vector<uint8_t>> key_map;

  for (auto it = master_keys_map.begin(); it != master_keys_map.end(); it++) {
    std::string key = it->second;
    std::vector<uint8_t> key_bytes(key.begin(), key.end());
    key_map.insert({it->first, key_bytes});
  }
  return key_map;
}

}  // namespace test
}  // namespace parquet

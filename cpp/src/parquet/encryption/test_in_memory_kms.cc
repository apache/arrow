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

#include "parquet/encryption/key_toolkit_internal.h"
#include "parquet/encryption/test_in_memory_kms.h"
#include "parquet/exception.h"

using arrow::util::SecureString;

namespace parquet::encryption {

std::unordered_map<std::string, SecureString>
    TestOnlyLocalWrapInMemoryKms::master_key_map_;
std::unordered_map<std::string, SecureString>
    TestOnlyInServerWrapKms::unwrapping_master_key_map_;
std::unordered_map<std::string, SecureString>
    TestOnlyInServerWrapKms::wrapping_master_key_map_;

void TestOnlyLocalWrapInMemoryKms::InitializeMasterKeys(
    const std::unordered_map<std::string, SecureString>& master_keys_map) {
  master_key_map_ = master_keys_map;
}

TestOnlyLocalWrapInMemoryKms::TestOnlyLocalWrapInMemoryKms(
    const KmsConnectionConfig& kms_connection_config)
    : LocalWrapKmsClient(kms_connection_config) {}

const SecureString& TestOnlyLocalWrapInMemoryKms::GetMasterKeyFromServer(
    const std::string& master_key_identifier) {
  // Always return the latest key version
  return master_key_map_.at(master_key_identifier);
}

void TestOnlyInServerWrapKms::InitializeMasterKeys(
    const std::unordered_map<std::string, SecureString>& master_keys_map) {
  unwrapping_master_key_map_ = master_keys_map;
  wrapping_master_key_map_ = unwrapping_master_key_map_;
}

void TestOnlyInServerWrapKms::StartKeyRotation(
    const std::unordered_map<std::string, SecureString>& new_master_key_map) {
  if (new_master_key_map.empty()) {
    throw ParquetException("No encryption key list");
  }
  wrapping_master_key_map_ = new_master_key_map;
}

void TestOnlyInServerWrapKms::FinishKeyRotation() {
  unwrapping_master_key_map_ = wrapping_master_key_map_;
}

std::string TestOnlyInServerWrapKms::WrapKey(const SecureString& key_bytes,
                                             const std::string& master_key_identifier) {
  // Always use the latest key version for writing
  if (wrapping_master_key_map_.find(master_key_identifier) ==
      wrapping_master_key_map_.end()) {
    throw ParquetException("Key not found: " + master_key_identifier);
  }
  const SecureString& master_key = wrapping_master_key_map_.at(master_key_identifier);

  std::string aad = master_key_identifier;
  return internal::EncryptKeyLocally(key_bytes, master_key, aad);
}

SecureString TestOnlyInServerWrapKms::UnWrapKey(
    const std::string& wrapped_key, const std::string& master_key_identifier) {
  if (unwrapping_master_key_map_.find(master_key_identifier) ==
      unwrapping_master_key_map_.end()) {
    throw ParquetException("Key not found: " + master_key_identifier);
  }
  const SecureString& master_key = unwrapping_master_key_map_.at(master_key_identifier);

  std::string aad = master_key_identifier;
  return internal::DecryptKeyLocally(wrapped_key, master_key, aad);
}

SecureString TestOnlyInServerWrapKms::GetMasterKeyFromServer(
    const std::string& master_key_identifier) {
  // Always return the latest key version
  return wrapping_master_key_map_.at(master_key_identifier);
}

}  // namespace parquet::encryption

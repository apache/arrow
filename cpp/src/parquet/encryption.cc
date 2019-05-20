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

#include "parquet/encryption.h"

#include <openssl/rand.h>
#include <string.h>
#include <map>
#include <utility>

#include "arrow/util/utf8.h"

namespace parquet {

// integer key retriever
void IntegerKeyIdRetriever::PutKey(uint32_t key_id, const std::string& key) {
  key_map_.insert(std::make_pair(key_id, key));
}

const std::string& IntegerKeyIdRetriever::GetKey(const std::string& key_metadata) {
  uint32_t key_id;
  memcpy(reinterpret_cast<uint8_t*>(&key_id), key_metadata.c_str(), 4);

  return key_map_[key_id];
}

// string key retriever
void StringKeyIdRetriever::PutKey(const std::string& key_id, const std::string& key) {
  key_map_.insert(std::make_pair(key_id, key));
}

const std::string& StringKeyIdRetriever::GetKey(const std::string& key_id) {
  return key_map_[key_id];
}

ColumnEncryptionProperties::Builder* ColumnEncryptionProperties::Builder::key_id(
    const std::string& key_id) {
  // key_id is expected to be in UTF8 encoding
  ::arrow::util::InitializeUTF8();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(key_id.c_str());
  if (!::arrow::util::ValidateUTF8(data, key_id.size())) {
    throw ParquetException("key id should be in UTF8 encoding");
  }

  DCHECK(!key_id.empty());
  this->key_metadata(key_id);
  return this;
}

ColumnEncryptionProperties::ColumnEncryptionProperties(
    bool encrypted, const std::shared_ptr<schema::ColumnPath>& column_path,
    const std::string& key, const std::string& key_metadata)
    : column_path_(column_path) {
  // column encryption properties object (with a column key) can be used for writing only
  // one file.
  // Upon completion of file writing, the encryption keys in the properties will be wiped
  // out (set to 0 in memory).
  utilized_ = false;

  DCHECK(column_path != nullptr);
  if (!encrypted) {
    DCHECK(key.empty() && key_metadata.empty());
  }

  if (!key.empty()) {
    DCHECK(key.length() == 16 || key.length() == 24 || key.length() == 32);
  }

  encrypted_with_footer_key_ = (encrypted && key.empty());
  if (encrypted_with_footer_key_) {
    DCHECK(key_metadata.empty());
  }

  encrypted_ = encrypted;
  key_metadata_ = key_metadata;
  key_ = key;
}

ColumnDecryptionProperties::ColumnDecryptionProperties(
    const std::shared_ptr<schema::ColumnPath>& column_path, const std::string& key)
    : column_path_(column_path) {
  utilized_ = false;
  DCHECK(column_path != nullptr);

  if (!key.empty()) {
    DCHECK(key.length() == 16 || key.length() == 24 || key.length() == 32);
  }

  key_ = key;
}

const std::string& FileDecryptionProperties::column_key(
    const std::shared_ptr<schema::ColumnPath>& column_path) {
  if (column_properties_.find(column_path) != column_properties_.end()) {
    auto column_prop = column_properties_[column_path];
    if (column_prop != nullptr) {
      return column_prop->key();
    }
  }
  return empty_string_;
}

FileDecryptionProperties::FileDecryptionProperties(
    const std::string& footer_key,
    const std::shared_ptr<DecryptionKeyRetriever>& key_retriever,
    bool check_plaintext_footer_integrity, const std::string& aad_prefix,
    std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier,
    const std::map<std::shared_ptr<schema::ColumnPath>,
                   std::shared_ptr<ColumnDecryptionProperties>,
                   schema::ColumnPath::CmpColumnPath>& column_properties,
    bool plaintext_files_allowed) {
  DCHECK(!footer_key.empty() || NULLPTR != key_retriever ||
         0 != column_properties.size());

  if (!footer_key.empty()) {
    DCHECK(footer_key.length() == 16 || footer_key.length() == 24 ||
           footer_key.length() == 32);
  }
  if (footer_key.empty() && check_plaintext_footer_integrity) {
    DCHECK(NULLPTR != key_retriever);
  }
  aad_prefix_verifier_ = aad_prefix_verifier;
  footer_key_ = footer_key;
  check_plaintext_footer_integrity_ = check_plaintext_footer_integrity;
  key_retriever_ = key_retriever;
  aad_prefix_ = aad_prefix;
  column_properties_ = column_properties;
  plaintext_files_allowed_ = plaintext_files_allowed;
  utilized_ = false;
}

FileEncryptionProperties::Builder* FileEncryptionProperties::Builder::footer_key_id(
    const std::string& key_id) {
  // key_id is expected to be in UTF8 encoding
  ::arrow::util::InitializeUTF8();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(key_id.c_str());
  if (!::arrow::util::ValidateUTF8(data, key_id.size())) {
    throw ParquetException("footer key id should be in UTF8 encoding");
  }

  if (key_id.empty()) {
    return this;
  }

  return footer_key_metadata(key_id);
}

std::shared_ptr<ColumnEncryptionProperties> FileEncryptionProperties::column_properties(
    const std::shared_ptr<schema::ColumnPath>& column_path) {
  if (column_properties_.size() == 0) {
    auto builder = std::shared_ptr<ColumnEncryptionProperties::Builder>(
        new ColumnEncryptionProperties::Builder(column_path));
    return builder->build();
  }
  if (column_properties_.find(column_path) != column_properties_.end()) {
    return column_properties_[column_path];
  }

  return NULLPTR;
}

FileEncryptionProperties::FileEncryptionProperties(
    ParquetCipher::type cipher, const std::string& footer_key,
    const std::string& footer_key_metadata, bool encrypted_footer,
    const std::string& aad_prefix, bool store_aad_prefix_in_file,
    const std::map<std::shared_ptr<schema::ColumnPath>,
                   std::shared_ptr<ColumnEncryptionProperties>,
                   schema::ColumnPath::CmpColumnPath>& column_properties)
    : footer_key_(footer_key),
      footer_key_metadata_(footer_key_metadata),
      encrypted_footer_(encrypted_footer),
      column_properties_(column_properties) {
  // file encryption properties object can be used for writing only one file.
  // Upon completion of file writing, the encryption keys in the properties will be wiped
  // out (set to 0 in memory).
  utilized_ = false;

  DCHECK(!footer_key.empty());
  // footer_key must be either 16, 24 or 32 bytes.
  DCHECK(footer_key.length() == 16 || footer_key.length() == 24 ||
         footer_key.length() == 32);

  uint8_t aad_file_unique[AAD_FILE_UNIQUE_LENGTH];
  memset(aad_file_unique, 0, AAD_FILE_UNIQUE_LENGTH);
  RAND_bytes(aad_file_unique, sizeof(AAD_FILE_UNIQUE_LENGTH));
  std::string aad_file_unique_str(reinterpret_cast<char const*>(aad_file_unique),
                                  AAD_FILE_UNIQUE_LENGTH);

  bool supply_aad_prefix = false;
  if (aad_prefix.empty()) {
    file_aad_ = aad_file_unique_str;
  } else {
    file_aad_ = aad_prefix + aad_file_unique_str;
    if (!store_aad_prefix_in_file) supply_aad_prefix = true;
  }
  algorithm_.algorithm = cipher;
  algorithm_.aad.aad_file_unique = aad_file_unique_str;
  algorithm_.aad.supply_aad_prefix = supply_aad_prefix;
  if (!aad_prefix.empty() && store_aad_prefix_in_file) {
    algorithm_.aad.aad_prefix = aad_prefix;
  }
}

}  // namespace parquet

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

#include "parquet/encryption/encryption.h"

#include <string.h>

#include <map>
#include <utility>

#include "arrow/util/logging.h"
#include "arrow/util/utf8.h"
#include "parquet/encryption/encryption_internal.h"

namespace parquet {

// integer key retriever
void IntegerKeyIdRetriever::PutKey(uint32_t key_id, const std::string& key) {
  key_map_.insert({key_id, key});
}

std::string IntegerKeyIdRetriever::GetKey(const std::string& key_metadata) {
  uint32_t key_id;
  memcpy(reinterpret_cast<uint8_t*>(&key_id), key_metadata.c_str(), 4);

  return key_map_.at(key_id);
}

// string key retriever
void StringKeyIdRetriever::PutKey(const std::string& key_id, const std::string& key) {
  key_map_.insert({key_id, key});
}

std::string StringKeyIdRetriever::GetKey(const std::string& key_id) {
  return key_map_.at(key_id);
}

ColumnEncryptionProperties::Builder* ColumnEncryptionProperties::Builder::key(
    std::string column_key) {
  if (column_key.empty()) return this;

  DCHECK(key_.empty());
  key_ = column_key;
  return this;
}

ColumnEncryptionProperties::Builder* ColumnEncryptionProperties::Builder::key_metadata(
    const std::string& key_metadata) {
  DCHECK(!key_metadata.empty());
  DCHECK(key_metadata_.empty());
  key_metadata_ = key_metadata;
  return this;
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

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::column_keys(
    const ColumnPathToDecryptionPropertiesMap& column_decryption_properties) {
  if (column_decryption_properties.size() == 0) return this;

  if (column_decryption_properties_.size() != 0)
    throw ParquetException("Column properties already set");

  for (const auto& element : column_decryption_properties) {
    if (element.second->is_utilized()) {
      throw ParquetException("Column properties utilized in another file");
    }
    element.second->set_utilized();
  }

  column_decryption_properties_ = column_decryption_properties;
  return this;
}

void FileDecryptionProperties::WipeOutDecryptionKeys() {
  footer_key_.clear();

  for (const auto& element : column_decryption_properties_) {
    element.second->WipeOutDecryptionKey();
  }
}

bool FileDecryptionProperties::is_utilized() {
  if (footer_key_.empty() && column_decryption_properties_.size() == 0 &&
      aad_prefix_.empty())
    return false;

  return utilized_;
}

std::shared_ptr<FileDecryptionProperties> FileDecryptionProperties::DeepClone(
    std::string new_aad_prefix) {
  std::string footer_key_copy = footer_key_;
  ColumnPathToDecryptionPropertiesMap column_decryption_properties_map_copy;

  for (const auto& element : column_decryption_properties_) {
    column_decryption_properties_map_copy.insert(
        {element.second->column_path(), element.second->DeepClone()});
  }

  if (new_aad_prefix.empty()) new_aad_prefix = aad_prefix_;
  return std::shared_ptr<FileDecryptionProperties>(new FileDecryptionProperties(
      footer_key_copy, key_retriever_, check_plaintext_footer_integrity_, new_aad_prefix,
      aad_prefix_verifier_, column_decryption_properties_map_copy,
      plaintext_files_allowed_));
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::footer_key(
    const std::string footer_key) {
  if (footer_key.empty()) {
    return this;
  }
  DCHECK(footer_key_.empty());
  footer_key_ = footer_key;
  return this;
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::key_retriever(
    const std::shared_ptr<DecryptionKeyRetriever>& key_retriever) {
  if (key_retriever == nullptr) return this;

  DCHECK(key_retriever_ == nullptr);
  key_retriever_ = key_retriever;
  return this;
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::aad_prefix(
    const std::string& aad_prefix) {
  if (aad_prefix.empty()) {
    return this;
  }
  DCHECK(aad_prefix_.empty());
  aad_prefix_ = aad_prefix;
  return this;
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::aad_prefix_verifier(
    std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier) {
  if (aad_prefix_verifier == nullptr) return this;

  DCHECK(aad_prefix_verifier_ == nullptr);
  aad_prefix_verifier_ = std::move(aad_prefix_verifier);
  return this;
}

ColumnDecryptionProperties::Builder* ColumnDecryptionProperties::Builder::key(
    const std::string& key) {
  if (key.empty()) return this;

  DCHECK(!key.empty());
  key_ = key;
  return this;
}

std::shared_ptr<ColumnDecryptionProperties> ColumnDecryptionProperties::Builder::build() {
  return std::shared_ptr<ColumnDecryptionProperties>(
      new ColumnDecryptionProperties(column_path_, key_));
}

void ColumnDecryptionProperties::WipeOutDecryptionKey() { key_.clear(); }

std::shared_ptr<ColumnDecryptionProperties> ColumnDecryptionProperties::DeepClone() {
  std::string key_copy = key_;
  return std::shared_ptr<ColumnDecryptionProperties>(
      new ColumnDecryptionProperties(column_path_, key_copy));
}

FileEncryptionProperties::Builder* FileEncryptionProperties::Builder::footer_key_metadata(
    const std::string& footer_key_metadata) {
  if (footer_key_metadata.empty()) return this;

  DCHECK(footer_key_metadata_.empty());
  footer_key_metadata_ = footer_key_metadata;
  return this;
}

FileEncryptionProperties::Builder* FileEncryptionProperties::Builder::encrypted_columns(
    const ColumnPathToEncryptionPropertiesMap& encrypted_columns) {
  if (encrypted_columns.size() == 0) return this;

  if (encrypted_columns_.size() != 0)
    throw ParquetException("Column properties already set");

  for (const auto& element : encrypted_columns) {
    if (element.second->is_utilized()) {
      throw ParquetException("Column properties utilized in another file");
    }
    element.second->set_utilized();
  }
  encrypted_columns_ = encrypted_columns;
  return this;
}

void FileEncryptionProperties::WipeOutEncryptionKeys() {
  footer_key_.clear();
  for (const auto& element : encrypted_columns_) {
    element.second->WipeOutEncryptionKey();
  }
}

std::shared_ptr<FileEncryptionProperties> FileEncryptionProperties::DeepClone(
    std::string new_aad_prefix) {
  std::string footer_key_copy = footer_key_;
  ColumnPathToEncryptionPropertiesMap encrypted_columns_map_copy;

  for (const auto& element : encrypted_columns_) {
    encrypted_columns_map_copy.insert(
        {element.second->column_path(), element.second->DeepClone()});
  }

  if (new_aad_prefix.empty()) new_aad_prefix = aad_prefix_;
  return std::shared_ptr<FileEncryptionProperties>(new FileEncryptionProperties(
      algorithm_.algorithm, footer_key_copy, footer_key_metadata_, encrypted_footer_,
      new_aad_prefix, store_aad_prefix_in_file_, encrypted_columns_map_copy));
}

FileEncryptionProperties::Builder* FileEncryptionProperties::Builder::aad_prefix(
    const std::string& aad_prefix) {
  if (aad_prefix.empty()) return this;

  DCHECK(aad_prefix_.empty());
  aad_prefix_ = aad_prefix;
  store_aad_prefix_in_file_ = true;
  return this;
}

FileEncryptionProperties::Builder*
FileEncryptionProperties::Builder::disable_aad_prefix_storage() {
  DCHECK(!aad_prefix_.empty());

  store_aad_prefix_in_file_ = false;
  return this;
}

ColumnEncryptionProperties::ColumnEncryptionProperties(bool encrypted,
                                                       const std::string& column_path,
                                                       const std::string& key,
                                                       const std::string& key_metadata)
    : column_path_(column_path) {
  // column encryption properties object (with a column key) can be used for writing only
  // one file.
  // Upon completion of file writing, the encryption keys in the properties will be wiped
  // out (set to 0 in memory).
  utilized_ = false;

  DCHECK(!column_path.empty());
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

ColumnDecryptionProperties::ColumnDecryptionProperties(const std::string& column_path,
                                                       const std::string& key)
    : column_path_(column_path) {
  utilized_ = false;
  DCHECK(!column_path.empty());

  if (!key.empty()) {
    DCHECK(key.length() == 16 || key.length() == 24 || key.length() == 32);
  }

  key_ = key;
}

std::string FileDecryptionProperties::column_key(const std::string& column_path) const {
  if (column_decryption_properties_.find(column_path) !=
      column_decryption_properties_.end()) {
    auto column_prop = column_decryption_properties_.at(column_path);
    if (column_prop != nullptr) {
      return column_prop->key();
    }
  }
  return empty_string_;
}

FileDecryptionProperties::FileDecryptionProperties(
    const std::string& footer_key, std::shared_ptr<DecryptionKeyRetriever> key_retriever,
    bool check_plaintext_footer_integrity, const std::string& aad_prefix,
    std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier,
    const ColumnPathToDecryptionPropertiesMap& column_decryption_properties,
    bool plaintext_files_allowed) {
  DCHECK(!footer_key.empty() || nullptr != key_retriever ||
         0 != column_decryption_properties.size());

  if (!footer_key.empty()) {
    DCHECK(footer_key.length() == 16 || footer_key.length() == 24 ||
           footer_key.length() == 32);
  }
  if (footer_key.empty() && check_plaintext_footer_integrity) {
    DCHECK(nullptr != key_retriever);
  }
  aad_prefix_verifier_ = std::move(aad_prefix_verifier);
  footer_key_ = footer_key;
  check_plaintext_footer_integrity_ = check_plaintext_footer_integrity;
  key_retriever_ = std::move(key_retriever);
  aad_prefix_ = aad_prefix;
  column_decryption_properties_ = column_decryption_properties;
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

std::shared_ptr<ColumnEncryptionProperties>
FileEncryptionProperties::column_encryption_properties(const std::string& column_path) {
  if (encrypted_columns_.size() == 0) {
    auto builder = std::make_shared<ColumnEncryptionProperties::Builder>(column_path);
    return builder->build();
  }
  if (encrypted_columns_.find(column_path) != encrypted_columns_.end()) {
    return encrypted_columns_[column_path];
  }

  return nullptr;
}

FileEncryptionProperties::FileEncryptionProperties(
    ParquetCipher::type cipher, const std::string& footer_key,
    const std::string& footer_key_metadata, bool encrypted_footer,
    const std::string& aad_prefix, bool store_aad_prefix_in_file,
    const ColumnPathToEncryptionPropertiesMap& encrypted_columns)
    : footer_key_(footer_key),
      footer_key_metadata_(footer_key_metadata),
      encrypted_footer_(encrypted_footer),
      aad_prefix_(aad_prefix),
      store_aad_prefix_in_file_(store_aad_prefix_in_file),
      encrypted_columns_(encrypted_columns) {
  // file encryption properties object can be used for writing only one file.
  // Upon completion of file writing, the encryption keys in the properties will be wiped
  // out (set to 0 in memory).
  utilized_ = false;

  DCHECK(!footer_key.empty());
  // footer_key must be either 16, 24 or 32 bytes.
  DCHECK(footer_key.length() == 16 || footer_key.length() == 24 ||
         footer_key.length() == 32);

  uint8_t aad_file_unique[kAadFileUniqueLength];
  memset(aad_file_unique, 0, kAadFileUniqueLength);
  encryption::RandBytes(aad_file_unique, sizeof(kAadFileUniqueLength));
  std::string aad_file_unique_str(reinterpret_cast<char const*>(aad_file_unique),
                                  kAadFileUniqueLength);

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

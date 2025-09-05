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

#include <map>
#include <utility>

#include "arrow/util/logging_internal.h"
#include "arrow/util/string.h"
#include "arrow/util/utf8.h"
#include "parquet/encryption/encryption_internal.h"

using ::arrow::util::SecureString;

namespace parquet {

// any empty SecureString key is interpreted as if no key is given
// this instance is used when a SecureString reference is returned
static SecureString kNoKey = SecureString();

// integer key retriever
void IntegerKeyIdRetriever::PutKey(uint32_t key_id, SecureString key) {
  key_map_.insert({key_id, std::move(key)});
}

// string key retriever
void StringKeyIdRetriever::PutKey(std::string key_id, SecureString key) {
  key_map_.insert({std::move(key_id), std::move(key)});
}

SecureString StringKeyIdRetriever::GetKey(const std::string& key_id) {
  return key_map_.at(key_id);
}

ColumnEncryptionProperties::Builder* ColumnEncryptionProperties::Builder::key(
    SecureString column_key) {
  if (column_key.empty()) return this;

  DCHECK(key_.empty());
  key_ = std::move(column_key);
  return this;
}

ColumnEncryptionProperties::Builder* ColumnEncryptionProperties::Builder::key_metadata(
    std::string key_metadata) {
  DCHECK(!key_metadata.empty());
  key_metadata_ = std::move(key_metadata);
  return this;
}

ColumnEncryptionProperties::Builder* ColumnEncryptionProperties::Builder::key_id(
    std::string key_id) {
  // key_id is expected to be in UTF8 encoding
  ::arrow::util::InitializeUTF8();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(key_id.c_str());
  if (!::arrow::util::ValidateUTF8(data, key_id.size())) {
    throw ParquetException("key id should be in UTF8 encoding");
  }

  DCHECK(!key_id.empty());
  this->key_metadata(std::move(key_id));
  return this;
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::column_keys(
    ColumnPathToDecryptionPropertiesMap column_decryption_properties) {
  if (column_decryption_properties.size() == 0) return this;

  if (column_decryption_properties_.size() != 0)
    throw ParquetException("Column properties already set");

  column_decryption_properties_ = std::move(column_decryption_properties);
  return this;
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::footer_key(
    SecureString footer_key) {
  if (footer_key.empty()) {
    return this;
  }
  DCHECK(footer_key_.empty());
  footer_key_ = std::move(footer_key);
  return this;
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::key_retriever(
    std::shared_ptr<DecryptionKeyRetriever> key_retriever) {
  if (key_retriever == nullptr) return this;

  DCHECK(key_retriever_ == nullptr);
  key_retriever_ = std::move(key_retriever);
  return this;
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::aad_prefix(
    std::string aad_prefix) {
  if (aad_prefix.empty()) {
    return this;
  }
  DCHECK(aad_prefix_.empty());
  aad_prefix_ = std::move(aad_prefix);
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
    SecureString key) {
  if (key.empty()) return this;

  DCHECK(key_.empty());
  key_ = std::move(key);
  return this;
}

std::shared_ptr<ColumnDecryptionProperties> ColumnDecryptionProperties::Builder::build() {
  return std::shared_ptr<ColumnDecryptionProperties>(
      new ColumnDecryptionProperties(column_path_, key_));
}

FileEncryptionProperties::Builder* FileEncryptionProperties::Builder::footer_key_metadata(
    std::string footer_key_metadata) {
  if (footer_key_metadata.empty()) return this;

  DCHECK(footer_key_metadata_.empty());
  footer_key_metadata_ = std::move(footer_key_metadata);
  return this;
}

FileEncryptionProperties::Builder* FileEncryptionProperties::Builder::encrypted_columns(
    ColumnPathToEncryptionPropertiesMap encrypted_columns) {
  if (encrypted_columns.size() == 0) return this;

  if (encrypted_columns_.size() != 0)
    throw ParquetException("Column properties already set");

  encrypted_columns_ = std::move(encrypted_columns);
  return this;
}

FileEncryptionProperties::Builder* FileEncryptionProperties::Builder::aad_prefix(
    std::string aad_prefix) {
  if (aad_prefix.empty()) return this;

  DCHECK(aad_prefix_.empty());
  aad_prefix_ = std::move(aad_prefix);
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
                                                       std::string column_path,
                                                       SecureString key,
                                                       std::string key_metadata)
    : column_path_(std::move(column_path)),
      encrypted_(encrypted),
      encrypted_with_footer_key_(encrypted && key.empty()),
      key_(std::move(key)),
      key_metadata_(std::move(key_metadata)) {
  DCHECK(!column_path_.empty());
  if (!encrypted) {
    DCHECK(key_.empty() && key_metadata_.empty());
  }
  if (!key_.empty()) {
    DCHECK(key_.length() == 16 || key_.length() == 24 || key_.length() == 32);
  }
  if (encrypted_with_footer_key_) {
    DCHECK(key_metadata_.empty());
  }
}

ColumnDecryptionProperties::ColumnDecryptionProperties(std::string column_path,
                                                       SecureString key)
    : column_path_(std::move(column_path)), key_(std::move(key)) {
  DCHECK(!column_path_.empty());

  if (!key_.empty()) {
    DCHECK(key_.length() == 16 || key_.length() == 24 || key_.length() == 32);
  }
}

const SecureString& FileDecryptionProperties::column_key(
    const std::string& column_path) const {
  if (column_decryption_properties_.find(column_path) !=
      column_decryption_properties_.end()) {
    auto column_prop = column_decryption_properties_.at(column_path);
    if (column_prop != nullptr) {
      return column_prop->key();
    }
  }
  return kNoKey;
}

FileDecryptionProperties::FileDecryptionProperties(
    SecureString footer_key, std::shared_ptr<DecryptionKeyRetriever> key_retriever,
    bool check_plaintext_footer_integrity, std::string aad_prefix,
    std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier,
    ColumnPathToDecryptionPropertiesMap column_decryption_properties,
    bool plaintext_files_allowed)
    : footer_key_(std::move(footer_key)),
      aad_prefix_(std::move(aad_prefix)),
      aad_prefix_verifier_(std::move(aad_prefix_verifier)),
      column_decryption_properties_(std::move(column_decryption_properties)),
      key_retriever_(std::move(key_retriever)),
      check_plaintext_footer_integrity_(check_plaintext_footer_integrity),
      plaintext_files_allowed_(plaintext_files_allowed) {
  DCHECK(!footer_key_.empty() || nullptr != key_retriever_ ||
         0 != column_decryption_properties_.size());
  if (!footer_key_.empty()) {
    DCHECK(footer_key_.length() == 16 || footer_key_.length() == 24 ||
           footer_key_.length() == 32);
  }
  if (footer_key_.empty() && check_plaintext_footer_integrity) {
    DCHECK(nullptr != key_retriever_);
  }
}

FileEncryptionProperties::Builder* FileEncryptionProperties::Builder::footer_key_id(
    std::string key_id) {
  // key_id is expected to be in UTF8 encoding
  ::arrow::util::InitializeUTF8();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(key_id.c_str());
  if (!::arrow::util::ValidateUTF8(data, key_id.size())) {
    throw ParquetException("footer key id should be in UTF8 encoding");
  }

  if (key_id.empty()) {
    return this;
  }

  return footer_key_metadata(std::move(key_id));
}

std::shared_ptr<ColumnEncryptionProperties>
FileEncryptionProperties::column_encryption_properties(const std::string& column_path) {
  if (encrypted_columns_.size() == 0) {
    auto builder = std::make_shared<ColumnEncryptionProperties::Builder>(column_path);
    return builder->build();
  }
  auto it = encrypted_columns_.find(column_path);
  if (it != encrypted_columns_.end()) {
    return it->second;
  }

  // We do not have an exact match of column_path in encrypted_columns_
  // there might be the root parent field in encrypted_columns_.
  auto pos = column_path.find('.');
  if (pos != std::string::npos) {
    std::string root = column_path.substr(0, pos);
    it = encrypted_columns_.find(root);
    if (it != encrypted_columns_.end()) {
      return it->second;
    }
  }

  return nullptr;
}

FileEncryptionProperties::FileEncryptionProperties(
    ParquetCipher::type cipher, SecureString footer_key, std::string footer_key_metadata,
    bool encrypted_footer, std::string aad_prefix, bool store_aad_prefix_in_file,
    ColumnPathToEncryptionPropertiesMap encrypted_columns)
    : footer_key_(std::move(footer_key)),
      footer_key_metadata_(std::move(footer_key_metadata)),
      encrypted_footer_(encrypted_footer),
      aad_prefix_(std::move(aad_prefix)),
      store_aad_prefix_in_file_(store_aad_prefix_in_file),
      encrypted_columns_(std::move(encrypted_columns)) {
  DCHECK(!footer_key_.empty());
  // footer_key must be either 16, 24 or 32 bytes.
  DCHECK(footer_key_.length() == 16 || footer_key_.length() == 24 ||
         footer_key_.length() == 32);

  uint8_t aad_file_unique[kAadFileUniqueLength];
  encryption::RandBytes(aad_file_unique, kAadFileUniqueLength);
  std::string aad_file_unique_str(reinterpret_cast<char const*>(aad_file_unique),
                                  kAadFileUniqueLength);

  bool supply_aad_prefix = false;
  if (aad_prefix_.empty()) {
    file_aad_ = aad_file_unique_str;
  } else {
    file_aad_ = aad_prefix_ + aad_file_unique_str;
    if (!store_aad_prefix_in_file_) supply_aad_prefix = true;
  }
  algorithm_.algorithm = cipher;
  algorithm_.aad.aad_file_unique = aad_file_unique_str;
  algorithm_.aad.supply_aad_prefix = supply_aad_prefix;
  if (!aad_prefix_.empty() && store_aad_prefix_in_file_) {
    algorithm_.aad.aad_prefix = aad_prefix_;
  }
}

}  // namespace parquet

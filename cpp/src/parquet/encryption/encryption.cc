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
#include <optional>
#include <rapidjson/document.h>
#include <string.h>
#include <utility>
#include <iostream>
#include "arrow/util/logging_internal.h"
#include "arrow/util/utf8.h"
#include "parquet/encryption/encryption_internal.h"

namespace parquet {

namespace {
  /// Helper method for validating JSON strings in app_context.
  bool IsValidJson(const std::string& json_str) {
    std::cout << "Validating JSON: " << json_str << std::endl;
    rapidjson::Document doc;
    std::cout << "Parsing JSON" << std::endl;
    doc.Parse(json_str.c_str());
    std::cout << "Parsed JSON" << std::endl;
    std::cout << "IsObject: " << doc.IsObject() << std::endl;
    std::cout << "HasParseError: " << doc.HasParseError() << std::endl;
    return doc.IsObject() && !doc.HasParseError();
  }
}  // Anonymous namespace

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

ColumnEncryptionProperties::Builder* ColumnEncryptionProperties::Builder::parquet_cipher(
    ParquetCipher::type parquet_cipher) {
  this->parquet_cipher_ = parquet_cipher;
  return this;
}

FileDecryptionProperties::Builder* FileDecryptionProperties::Builder::column_keys(
    const ColumnPathToDecryptionPropertiesMap& column_decryption_properties) {
  if (column_decryption_properties.size() == 0) return this;

  if (column_decryption_properties_.size() != 0)
    throw ParquetException("Column properties already set");

  column_decryption_properties_ = column_decryption_properties;
  return this;
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

ExternalFileDecryptionProperties::Builder* ExternalFileDecryptionProperties::Builder::app_context(
    const std::string context) {
      std::cout << "--------------------------------" << std::endl;
      std::cout << "Setting app_context" << std::endl;
      std::cout << "app_context_: [" << context << "]" << std::endl;
  if (!app_context_.empty()) {
    throw ParquetException("App context already set");
  }
  if (context.empty()) {
    std::cout << "App context is empty" << std::endl;
    return this;
  }

  if (!IsValidJson(context)) {
    std::cout << "Invalid JSON" << std::endl;
    throw ParquetException("App context is not a valid JSON string");
  }
  std::cout << "Valid JSON" << std::endl;
  app_context_ = context;
  std::cout << "App context set" << std::endl;
  std::cout << "app_context_: [" << app_context_ << "]" << std::endl;
  std::cout << "--------------------------------" << std::endl;
  return this;
}

ExternalFileDecryptionProperties::Builder* 
ExternalFileDecryptionProperties::Builder::connection_config(
    const std::map<ParquetCipher::type, std::map<std::string, std::string>>& config) {
  std::cout << "Setting connection config" << std::endl;
  if (connection_config_.size() != 0) {
    throw ParquetException("Connection config already set");
  }

  if (config.size() == 0) {
    return this;
  }
  std::cout << "Setting connection config" << std::endl;
  connection_config_ = config;
  return this;
}

std::shared_ptr<ExternalFileDecryptionProperties> 
ExternalFileDecryptionProperties::Builder::build_external() {
  std::cout << "\n\n--------------------------------" << std::endl;
  std::cout << "Building external" << std::endl;
  std::cout << "app_context_: [" << app_context_ << "]" << std::endl;
  return std::shared_ptr<ExternalFileDecryptionProperties>(new ExternalFileDecryptionProperties(
    footer_key_, key_retriever_, check_plaintext_footer_integrity_, aad_prefix_,
    aad_prefix_verifier_, column_decryption_properties_, plaintext_files_allowed_,
    app_context_, connection_config_));
}

ExternalFileDecryptionProperties::ExternalFileDecryptionProperties(
    const std::string& footer_key,
    std::shared_ptr<DecryptionKeyRetriever> key_retriever,
    bool check_plaintext_footer_integrity, const std::string& aad_prefix,
    std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier,
    const ColumnPathToDecryptionPropertiesMap& column_decryption_properties,
    bool plaintext_files_allowed,
    const std::string& app_context,
    const std::map<ParquetCipher::type, std::map<std::string, std::string>>& connection_config)
    : FileDecryptionProperties(footer_key, key_retriever, check_plaintext_footer_integrity,
                               aad_prefix, aad_prefix_verifier, column_decryption_properties,
                               plaintext_files_allowed),
      app_context_(app_context),
      connection_config_(connection_config) {
            // Debug: Check what was actually stored
    std::cout << "Constructor received app_context: [" << app_context << "]" << std::endl;
    std::cout << "Constructor stored app_context_: [" << app_context_ << "]" << std::endl;
    std::cout << "Constructor stored connection_config_ size: " << connection_config_.size() << std::endl;
    
    // Debug: Check memory addresses
    std::cout << "app_context_ memory address: " << (void*)app_context_.data() << std::endl;
    std::cout << "app_context_ size: " << app_context_.size() << std::endl;
      }

ColumnDecryptionProperties::Builder* ColumnDecryptionProperties::Builder::key(
    const std::string& key) {
  if (key.empty()) return this;

  DCHECK(!key.empty());
  key_ = key;
  return this;
}

ColumnDecryptionProperties::Builder* ColumnDecryptionProperties::Builder::parquet_cipher(
    ParquetCipher::type parquet_cipher) {
  parquet_cipher_ = parquet_cipher;
  return this;
}

std::shared_ptr<ColumnDecryptionProperties> ColumnDecryptionProperties::Builder::build() {
  return std::shared_ptr<ColumnDecryptionProperties>(
      new ColumnDecryptionProperties(column_path_, key_, parquet_cipher_));
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

  encrypted_columns_ = encrypted_columns;
  return this;
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

ColumnEncryptionProperties::ColumnEncryptionProperties(
        bool encrypted, const std::string& column_path, const std::string& key,
        const std::string& key_metadata, std::optional<ParquetCipher::type> parquet_cipher)
    : column_path_(column_path), parquet_cipher_(parquet_cipher) {
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

ColumnDecryptionProperties::ColumnDecryptionProperties(
        const std::string& column_path, const std::string& key, 
        std::optional<ParquetCipher::type> parquet_cipher)
    : column_path_(column_path), parquet_cipher_(parquet_cipher) {
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
      std::cout << "FileDecryptionProperties constructor" << std::endl;
      std::cout << "Are you going to crash me here?" << std::endl;
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
  std::cout << "FileDecryptionProperties constructor finished" << std::endl;
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
  DCHECK(!footer_key.empty());
  // footer_key must be either 16, 24 or 32 bytes.
  DCHECK(footer_key.length() == 16 || footer_key.length() == 24 ||
         footer_key.length() == 32);

  uint8_t aad_file_unique[kAadFileUniqueLength];
  encryption::RandBytes(aad_file_unique, kAadFileUniqueLength);
  std::string aad_file_unique_str(reinterpret_cast<char const*>(aad_file_unique),
                                  kAadFileUniqueLength);

  bool supply_aad_prefix = false;
  if (aad_prefix.empty()) {
    file_aad_ = aad_file_unique_str;
  } else {
    file_aad_ = aad_prefix + aad_file_unique_str;
    if (!store_aad_prefix_in_file_) supply_aad_prefix = true;
  }
  algorithm_.algorithm = cipher;
  algorithm_.aad.aad_file_unique = aad_file_unique_str;
  algorithm_.aad.supply_aad_prefix = supply_aad_prefix;
  if (!aad_prefix.empty() && store_aad_prefix_in_file_) {
    algorithm_.aad.aad_prefix = aad_prefix;
  }
}

ExternalFileEncryptionProperties::Builder* ExternalFileEncryptionProperties::Builder::app_context(
    const std::string& context) {
  std::cout << "Setting app_context" << std::endl;
  std::cout << "app_context_: [" << context << "]" << std::endl;
  if (!app_context_.empty()) {
    throw ParquetException("App context already set");
  }

  if (context.empty()) {
    std::cout << "App context is empty" << std::endl;
    return this;
  }

  if (!IsValidJson(context)) {
    std::cout << "App context is not a valid JSON string" << std::endl;
    throw ParquetException("App context is not a valid JSON string");
  }

  app_context_ = context;
  std::cout << "App context set" << std::endl;
  std::cout << "app_context_: [" << app_context_ << "]" << std::endl;
  return this;
}

ExternalFileEncryptionProperties::Builder*
ExternalFileEncryptionProperties::Builder::connection_config(
    const std::map<std::string, std::string>& config) {
  if (connection_config_.size() != 0) {
    throw ParquetException("Connection config already set");
  }
  
  if (config.size() == 0) {
    return this;
  }

  connection_config_ = config;
  return this;
}

std::shared_ptr<ExternalFileEncryptionProperties> 
ExternalFileEncryptionProperties::Builder::build_external() {
  return std::shared_ptr<ExternalFileEncryptionProperties>(new ExternalFileEncryptionProperties(
    parquet_cipher_, footer_key_, footer_key_metadata_, encrypted_footer_, aad_prefix_,
    store_aad_prefix_in_file_, encrypted_columns_, app_context_, connection_config_));
}

ExternalFileEncryptionProperties::ExternalFileEncryptionProperties(
    ParquetCipher::type cipher, const std::string& footer_key,
    const std::string& footer_key_metadata, bool encrypted_footer,
    const std::string& aad_prefix, bool store_aad_prefix_in_file,
    const ColumnPathToEncryptionPropertiesMap& encrypted_columns,
    const std::string& app_context,
    const std::map<std::string, std::string>& connection_config)
    : FileEncryptionProperties(cipher, footer_key, footer_key_metadata, encrypted_footer,
                               aad_prefix, store_aad_prefix_in_file, encrypted_columns),
      app_context_(app_context),
      connection_config_(connection_config) {}

}  // namespace parquet

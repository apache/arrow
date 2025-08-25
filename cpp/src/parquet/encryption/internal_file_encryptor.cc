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

#include "parquet/encryption/internal_file_encryptor.h"

#include "parquet/encryption/aes_encryption.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/encryption_utils.h"

namespace parquet {

// Encryptor
Encryptor::Encryptor(encryption::EncryptorInterface* encryptor_instance, const std::string& key,
                     const std::string& file_aad, const std::string& aad,
                     ::arrow::MemoryPool* pool)
    : encryptor_instance_(encryptor_instance),
      key_(key),
      file_aad_(file_aad),
      aad_(aad),
      pool_(pool) {}

int32_t Encryptor::CiphertextLength(int64_t plaintext_len) const {
  return encryptor_instance_->CiphertextLength(plaintext_len);
}

int32_t Encryptor::Encrypt(::arrow::util::span<const uint8_t> plaintext,
                           ::arrow::util::span<uint8_t> ciphertext) {
  return encryptor_instance_->Encrypt(plaintext, str2span(key_), str2span(aad_), ciphertext);
}

// InternalFileEncryptor
InternalFileEncryptor::InternalFileEncryptor(FileEncryptionProperties* properties,
                                             ::arrow::MemoryPool* pool)
    : properties_(properties), pool_(pool) {}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetFooterEncryptor() {
  if (footer_encryptor_ != nullptr) {
    return footer_encryptor_;
  }

  ParquetCipher::type algorithm = properties_->algorithm().algorithm;
  std::string footer_aad = encryption::CreateFooterAad(properties_->file_aad());
  std::string footer_key = properties_->footer_key();
  auto encryptor_instance = GetMetaEncryptor(algorithm, footer_key.size());
  footer_encryptor_ = std::make_shared<Encryptor>(
    encryptor_instance, footer_key, properties_->file_aad(), footer_aad, pool_);
  return footer_encryptor_;
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetFooterSigningEncryptor() {
  if (footer_signing_encryptor_ != nullptr) {
    return footer_signing_encryptor_;
  }

  ParquetCipher::type algorithm = properties_->algorithm().algorithm;
  std::string footer_aad = encryption::CreateFooterAad(properties_->file_aad());
  std::string footer_signing_key = properties_->footer_key();
  auto encryptor_instance = GetMetaEncryptor(algorithm, footer_signing_key.size());
  footer_signing_encryptor_ = std::make_shared<Encryptor>(
    encryptor_instance, footer_signing_key, properties_->file_aad(), footer_aad, pool_);
  return footer_signing_encryptor_;
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetColumnMetaEncryptor(
    const std::string& column_path) {
  return GetColumnEncryptor(column_path, true);
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetColumnDataEncryptor(
    const std::string& column_path, const ColumnChunkMetaDataBuilder* column_chunk_metadata) {
  return GetColumnEncryptor(column_path, false, column_chunk_metadata);
}

std::shared_ptr<Encryptor>
InternalFileEncryptor::InternalFileEncryptor::GetColumnEncryptor(
    const std::string& column_path, bool metadata,
    const ColumnChunkMetaDataBuilder* column_chunk_metadata) {
  // first look if we already got the encryptor from before
  if (metadata) {
    if (column_metadata_map_.find(column_path) != column_metadata_map_.end()) {
      return column_metadata_map_.at(column_path);
    }
  } else {
    if (column_data_map_.find(column_path) != column_data_map_.end()) {
      return column_data_map_.at(column_path);
    }
  }
  auto column_prop = properties_->column_encryption_properties(column_path);
  if (column_prop == nullptr) {
    return nullptr;
  }

  std::string key;
  if (column_prop->is_encrypted_with_footer_key()) {
    key = properties_->footer_key();
  } else {
    key = column_prop->key();
  }

  ParquetCipher::type algorithm = properties_->algorithm().algorithm;
  if (!metadata) {
    // Column data encryption might specify a different algorithm
    if (column_prop->parquet_cipher().has_value()) {
      algorithm = column_prop->parquet_cipher().value();
    }
  }
  auto encryptor_instance = metadata ? GetMetaEncryptor(algorithm, key.size())
                                      : GetDataEncryptor(algorithm, key.size(),
                                                         column_chunk_metadata);

  std::string file_aad = properties_->file_aad();
  std::shared_ptr<Encryptor> encryptor =
      std::make_shared<Encryptor>(encryptor_instance, key, file_aad, "", pool_);
  if (metadata)
    column_metadata_map_[column_path] = encryptor;
  else
    column_data_map_[column_path] = encryptor;

  return encryptor;
}

encryption::EncryptorInterface* InternalFileEncryptor::GetMetaEncryptor(
    ParquetCipher::type algorithm, size_t key_size) {
  // Metadata is encrypted with AES.
  return aes_encryptor_factory_.GetMetaAesEncryptor(algorithm, key_size);
}

encryption::EncryptorInterface* InternalFileEncryptor::GetDataEncryptor(
    ParquetCipher::type algorithm, size_t key_size,
    const ColumnChunkMetaDataBuilder* column_chunk_metadata) {
  if (algorithm == ParquetCipher::EXTERNAL_DBPA_V1) {
    if (dynamic_cast<ExternalFileEncryptionProperties*>(properties_) == nullptr) {
      throw ParquetException("External DBPA encryption requires ExternalFileEncryptionProperties.");
    }

    return external_dbpa_encryptor_factory_.GetEncryptor(
        algorithm, column_chunk_metadata,
        dynamic_cast<ExternalFileEncryptionProperties*>(properties_));
  }
  return aes_encryptor_factory_.GetDataAesEncryptor(algorithm, key_size);
}

}  // namespace parquet

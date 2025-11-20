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
#include "arrow/util/secure_string.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/encryption_internal.h"

using arrow::util::SecureString;

namespace parquet {

// Encryptor
Encryptor::Encryptor(encryption::AesEncryptor* aes_encryptor, SecureString key,
                     std::string file_aad, std::string aad, ::arrow::MemoryPool* pool)
    : aes_encryptor_(aes_encryptor),
      key_(std::move(key)),
      file_aad_(std::move(file_aad)),
      aad_(std::move(aad)),
      pool_(pool) {}

int32_t Encryptor::CiphertextLength(int64_t plaintext_len) const {
  return aes_encryptor_->CiphertextLength(plaintext_len);
}

int32_t Encryptor::Encrypt(::arrow::util::span<const uint8_t> plaintext,
                           ::arrow::util::span<uint8_t> ciphertext) {
  return aes_encryptor_->Encrypt(plaintext, key_.as_span(), str2span(aad_), ciphertext);
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
  const SecureString& footer_key = properties_->footer_key();
  auto aes_encryptor = GetMetaAesEncryptor(algorithm, footer_key.size());
  footer_encryptor_ = std::make_shared<Encryptor>(
      aes_encryptor, footer_key, properties_->file_aad(), footer_aad, pool_);
  return footer_encryptor_;
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetFooterSigningEncryptor() {
  if (footer_signing_encryptor_ != nullptr) {
    return footer_signing_encryptor_;
  }

  ParquetCipher::type algorithm = properties_->algorithm().algorithm;
  std::string footer_aad = encryption::CreateFooterAad(properties_->file_aad());
  const SecureString& footer_signing_key = properties_->footer_key();
  auto aes_encryptor = GetMetaAesEncryptor(algorithm, footer_signing_key.size());
  footer_signing_encryptor_ = std::make_shared<Encryptor>(
      aes_encryptor, footer_signing_key, properties_->file_aad(), footer_aad, pool_);
  return footer_signing_encryptor_;
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetColumnMetaEncryptor(
    const std::string& column_path) {
  return GetColumnEncryptor(column_path, true);
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetColumnDataEncryptor(
    const std::string& column_path) {
  return GetColumnEncryptor(column_path, false);
}

std::shared_ptr<Encryptor>
InternalFileEncryptor::InternalFileEncryptor::GetColumnEncryptor(
    const std::string& column_path, bool metadata) {
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

  const SecureString& key = column_prop->is_encrypted_with_footer_key()
                                ? properties_->footer_key()
                                : column_prop->key();

  ParquetCipher::type algorithm = properties_->algorithm().algorithm;
  auto aes_encryptor = metadata ? GetMetaAesEncryptor(algorithm, key.size())
                                : GetDataAesEncryptor(algorithm, key.size());

  std::string file_aad = properties_->file_aad();
  std::shared_ptr<Encryptor> encryptor =
      std::make_shared<Encryptor>(aes_encryptor, key, file_aad, "", pool_);
  if (metadata)
    column_metadata_map_[column_path] = encryptor;
  else
    column_data_map_[column_path] = encryptor;

  return encryptor;
}

int InternalFileEncryptor::MapKeyLenToEncryptorArrayIndex(int32_t key_len) const {
  if (key_len == 16)
    return 0;
  else if (key_len == 24)
    return 1;
  else if (key_len == 32)
    return 2;
  throw ParquetException("encryption key must be 16, 24 or 32 bytes in length");
}

encryption::AesEncryptor* InternalFileEncryptor::GetMetaAesEncryptor(
    ParquetCipher::type algorithm, size_t key_size) {
  auto key_len = static_cast<int32_t>(key_size);
  int index = MapKeyLenToEncryptorArrayIndex(key_len);
  if (meta_encryptor_[index] == nullptr) {
    meta_encryptor_[index] = encryption::AesEncryptor::Make(algorithm, key_len, true);
  }
  return meta_encryptor_[index].get();
}

encryption::AesEncryptor* InternalFileEncryptor::GetDataAesEncryptor(
    ParquetCipher::type algorithm, size_t key_size) {
  auto key_len = static_cast<int32_t>(key_size);
  int index = MapKeyLenToEncryptorArrayIndex(key_len);
  if (data_encryptor_[index] == nullptr) {
    data_encryptor_[index] = encryption::AesEncryptor::Make(algorithm, key_len, false);
  }
  return data_encryptor_[index].get();
}

}  // namespace parquet

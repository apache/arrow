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

#include "parquet/internal_file_encryptor.h"
#include "parquet/encryption.h"
#include "parquet/util/crypto.h"

namespace parquet {

static inline uint8_t* str2bytes(const std::string& str) {
  if (str.empty()) return NULLPTR;

  char* cbytes = const_cast<char*>(str.c_str());
  return reinterpret_cast<uint8_t*>(cbytes);
}

// Encryptor
Encryptor::Encryptor(parquet_encryption::AesEncryptor* aes_encryptor,
                     const std::string& key, const std::string& file_aad,
                     const std::string& aad)
    : aes_encryptor_(aes_encryptor), key_(key), file_aad_(file_aad), aad_(aad) {}

int Encryptor::CiphertextSizeDelta() { return aes_encryptor_->CiphertextSizeDelta(); }

int Encryptor::Encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* ciphertext) {
  return aes_encryptor_->Encrypt(plaintext, plaintext_len, str2bytes(key_),
                                 static_cast<int>(key_.size()), str2bytes(aad_),
                                 static_cast<int>(aad_.size()), ciphertext);
}

// InternalFileEncryptor
InternalFileEncryptor::InternalFileEncryptor(FileEncryptionProperties* properties)
    : properties_(properties) {
  all_encryptors_ = std::shared_ptr<std::list<parquet_encryption::AesEncryptor*>>(
      new std::list<parquet_encryption::AesEncryptor*>);

  column_data_map_ = std::shared_ptr<
      std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Encryptor>,
               parquet::schema::ColumnPath::CmpColumnPath>>(
      new std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Encryptor>,
                   schema::ColumnPath::CmpColumnPath>());

  column_metadata_map_ = std::shared_ptr<
      std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Encryptor>,
               parquet::schema::ColumnPath::CmpColumnPath>>(
      new std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Encryptor>,
                   schema::ColumnPath::CmpColumnPath>());
}

void InternalFileEncryptor::wipeout_encryption_keys() {
  properties_->wipeout_encryption_keys();

  for (auto const& i : *all_encryptors_) {
    i->WipeOut();
  }
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetFooterEncryptor() {
  if (footer_encryptor_ != NULLPTR) {
    return footer_encryptor_;
  }

  ParquetCipher::type algorithm = properties_->algorithm().algorithm;
  std::string footer_aad = parquet_encryption::createFooterAAD(properties_->file_aad());
  std::string footer_key = properties_->footer_key();
  auto aes_encryptor = GetMetaAesEncryptor(algorithm, footer_key.size());
  std::shared_ptr<Encryptor> encryptor = std::make_shared<Encryptor>(
      aes_encryptor, footer_key, properties_->file_aad(), footer_aad);
  footer_encryptor_ = encryptor;
  return encryptor;
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetFooterSigningEncryptor() {
  if (footer_signing_encryptor_ != NULLPTR) {
    return footer_signing_encryptor_;
  }

  ParquetCipher::type algorithm = properties_->algorithm().algorithm;
  std::string footer_aad = parquet_encryption::createFooterAAD(properties_->file_aad());
  std::string footer_signing_key = properties_->footer_key();
  auto aes_encryptor = GetMetaAesEncryptor(algorithm, footer_signing_key.size());
  std::shared_ptr<Encryptor> encryptor = std::make_shared<Encryptor>(
      aes_encryptor, footer_signing_key, properties_->file_aad(), footer_aad);
  footer_signing_encryptor_ = encryptor;
  return encryptor;
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetColumnMetaEncryptor(
    const std::shared_ptr<schema::ColumnPath>& column_path) {
  return GetColumnEncryptor(column_path, true);
}

std::shared_ptr<Encryptor> InternalFileEncryptor::GetColumnDataEncryptor(
    const std::shared_ptr<schema::ColumnPath>& column_path) {
  return GetColumnEncryptor(column_path, false);
}

std::shared_ptr<Encryptor>
InternalFileEncryptor::InternalFileEncryptor::GetColumnEncryptor(
    const std::shared_ptr<schema::ColumnPath>& column_path, bool metadata) {
  // first look if we already got the encryptor from before
  if (metadata) {
    if (column_metadata_map_->find(column_path) != column_metadata_map_->end()) {
      return column_metadata_map_->at(column_path);
    }
  } else {
    if (column_data_map_->find(column_path) != column_data_map_->end()) {
      return column_data_map_->at(column_path);
    }
  }
  auto column_prop = properties_->column_properties(column_path);
  if (column_prop == NULLPTR) {
    return NULLPTR;
  }

  std::string key;
  if (column_prop->is_encrypted_with_footer_key()) {
    key = properties_->footer_key();
  } else {
    key = column_prop->key();
  }

  ParquetCipher::type algorithm = properties_->algorithm().algorithm;
  auto aes_encryptor = metadata ? GetMetaAesEncryptor(algorithm, key.size())
                                : GetDataAesEncryptor(algorithm, key.size());

  std::string file_aad = properties_->file_aad();
  std::shared_ptr<Encryptor> encryptor =
      std::make_shared<Encryptor>(aes_encryptor, key, file_aad, "");
  if (metadata)
    (*column_metadata_map_)[column_path] = encryptor;
  else
    (*column_data_map_)[column_path] = encryptor;

  return encryptor;
}

parquet_encryption::AesEncryptor* InternalFileEncryptor::GetMetaAesEncryptor(
    ParquetCipher::type algorithm, size_t key_size) {
  int key_len = static_cast<int>(key_size);
  if (key_len == 16) {
    if (meta_encryptor_128_ == NULLPTR) {
      meta_encryptor_128_.reset(new parquet_encryption::AesEncryptor(
          algorithm, key_len, true, all_encryptors_));
    }
    return meta_encryptor_128_.get();
  } else if (key_len == 24) {
    if (meta_encryptor_196_ == NULLPTR) {
      meta_encryptor_196_.reset(new parquet_encryption::AesEncryptor(
          algorithm, key_len, true, all_encryptors_));
    }
    return meta_encryptor_196_.get();
  } else if (key_len == 32) {
    if (meta_encryptor_256_ == NULLPTR) {
      meta_encryptor_256_.reset(new parquet_encryption::AesEncryptor(
          algorithm, key_len, true, all_encryptors_));
    }
    return meta_encryptor_256_.get();
  }
  throw ParquetException("encryption key must be 16, 24 or 32 bytes in length");
}

parquet_encryption::AesEncryptor* InternalFileEncryptor::GetDataAesEncryptor(
    ParquetCipher::type algorithm, size_t key_size) {
  int key_len = static_cast<int>(key_size);
  if (key_len == 16) {
    if (data_encryptor_128_ == NULLPTR) {
      data_encryptor_128_.reset(new parquet_encryption::AesEncryptor(
          algorithm, key_len, false, all_encryptors_));
    }
    return data_encryptor_128_.get();
  } else if (key_len == 24) {
    if (data_encryptor_196_ == NULLPTR) {
      data_encryptor_196_.reset(new parquet_encryption::AesEncryptor(
          algorithm, key_len, false, all_encryptors_));
    }
    return data_encryptor_196_.get();
  } else if (key_len == 32) {
    if (data_encryptor_256_ == NULLPTR) {
      data_encryptor_256_.reset(new parquet_encryption::AesEncryptor(
          algorithm, key_len, false, all_encryptors_));
    }
    return data_encryptor_256_.get();
  }
  throw ParquetException("encryption key must be 16, 24 or 32 bytes in length");
}

}  // namespace parquet

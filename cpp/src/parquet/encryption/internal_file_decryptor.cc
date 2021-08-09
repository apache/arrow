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

#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/encryption_internal.h"

namespace parquet {

// Decryptor
Decryptor::Decryptor(encryption::AesDecryptor* aes_decryptor, const std::string& key,
                     const std::string& file_aad, const std::string& aad,
                     ::arrow::MemoryPool* pool)
    : aes_decryptor_(aes_decryptor),
      key_(key),
      file_aad_(file_aad),
      aad_(aad),
      pool_(pool) {}

int Decryptor::CiphertextSizeDelta() { return aes_decryptor_->CiphertextSizeDelta(); }

int Decryptor::Decrypt(const uint8_t* ciphertext, int ciphertext_len,
                       uint8_t* plaintext) {
  return aes_decryptor_->Decrypt(ciphertext, ciphertext_len, str2bytes(key_),
                                 static_cast<int>(key_.size()), str2bytes(aad_),
                                 static_cast<int>(aad_.size()), plaintext);
}

// InternalFileDecryptor
InternalFileDecryptor::InternalFileDecryptor(FileDecryptionProperties* properties,
                                             const std::string& file_aad,
                                             ParquetCipher::type algorithm,
                                             const std::string& footer_key_metadata,
                                             ::arrow::MemoryPool* pool)
    : properties_(properties),
      file_aad_(file_aad),
      algorithm_(algorithm),
      footer_key_metadata_(footer_key_metadata),
      pool_(pool) {
  if (properties_->is_utilized()) {
    throw ParquetException(
        "Re-using decryption properties with explicit keys for another file");
  }
  properties_->set_utilized();
}

void InternalFileDecryptor::WipeOutDecryptionKeys() {
  properties_->WipeOutDecryptionKeys();
  for (auto const& i : all_decryptors_) {
    i->WipeOut();
  }
}

std::string InternalFileDecryptor::GetFooterKey() {
  std::string footer_key = properties_->footer_key();
  // ignore footer key metadata if footer key is explicitly set via API
  if (footer_key.empty()) {
    if (footer_key_metadata_.empty())
      throw ParquetException("No footer key or key metadata");
    if (properties_->key_retriever() == nullptr)
      throw ParquetException("No footer key or key retriever");
    try {
      footer_key = properties_->key_retriever()->GetKey(footer_key_metadata_);
    } catch (KeyAccessDeniedException& e) {
      std::stringstream ss;
      ss << "Footer key: access denied " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
  }
  if (footer_key.empty()) {
    throw ParquetException(
        "Footer key unavailable. Could not verify "
        "plaintext footer metadata");
  }
  return footer_key;
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptor() {
  std::string aad = encryption::CreateFooterAad(file_aad_);
  return GetFooterDecryptor(aad, true);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptorForColumnMeta(
    const std::string& aad) {
  return GetFooterDecryptor(aad, true);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptorForColumnData(
    const std::string& aad) {
  return GetFooterDecryptor(aad, false);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptor(
    const std::string& aad, bool metadata) {
  if (metadata) {
    if (footer_metadata_decryptor_ != nullptr) return footer_metadata_decryptor_;
  } else {
    if (footer_data_decryptor_ != nullptr) return footer_data_decryptor_;
  }

  std::string footer_key = properties_->footer_key();
  if (footer_key.empty()) {
    if (footer_key_metadata_.empty())
      throw ParquetException("No footer key or key metadata");
    if (properties_->key_retriever() == nullptr)
      throw ParquetException("No footer key or key retriever");
    try {
      footer_key = properties_->key_retriever()->GetKey(footer_key_metadata_);
    } catch (KeyAccessDeniedException& e) {
      std::stringstream ss;
      ss << "Footer key: access denied " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
  }
  if (footer_key.empty()) {
    throw ParquetException(
        "Invalid footer encryption key. "
        "Could not parse footer metadata");
  }

  // Create both data and metadata decryptors to avoid redundant retrieval of key
  // from the key_retriever.
  auto aes_metadata_decryptor = GetMetaAesDecryptor(footer_key.size());
  auto aes_data_decryptor = GetDataAesDecryptor(footer_key.size());

  footer_metadata_decryptor_ = std::make_shared<Decryptor>(
      aes_metadata_decryptor, footer_key, file_aad_, aad, pool_);
  footer_data_decryptor_ =
      std::make_shared<Decryptor>(aes_data_decryptor, footer_key, file_aad_, aad, pool_);

  if (metadata) return footer_metadata_decryptor_;
  return footer_data_decryptor_;
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnMetaDecryptor(
    const std::string& column_path, const std::string& column_key_metadata,
    const std::string& aad) {
  return GetColumnDecryptor(column_path, column_key_metadata, aad, true);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnDataDecryptor(
    const std::string& column_path, const std::string& column_key_metadata,
    const std::string& aad) {
  return GetColumnDecryptor(column_path, column_key_metadata, aad, false);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnDecryptor(
    const std::string& column_path, const std::string& column_key_metadata,
    const std::string& aad, bool metadata) {
  std::string column_key;
  // first look if we already got the decryptor from before
  if (metadata) {
    if (column_metadata_map_.find(column_path) != column_metadata_map_.end()) {
      auto res(column_metadata_map_.at(column_path));
      res->UpdateAad(aad);
      return res;
    }
  } else {
    if (column_data_map_.find(column_path) != column_data_map_.end()) {
      auto res(column_data_map_.at(column_path));
      res->UpdateAad(aad);
      return res;
    }
  }

  column_key = properties_->column_key(column_path);
  // No explicit column key given via API. Retrieve via key metadata.
  if (column_key.empty() && !column_key_metadata.empty() &&
      properties_->key_retriever() != nullptr) {
    try {
      column_key = properties_->key_retriever()->GetKey(column_key_metadata);
    } catch (KeyAccessDeniedException& e) {
      std::stringstream ss;
      ss << "HiddenColumnException, path=" + column_path + " " << e.what() << "\n";
      throw HiddenColumnException(ss.str());
    }
  }
  if (column_key.empty()) {
    throw HiddenColumnException("HiddenColumnException, path=" + column_path);
  }

  // Create both data and metadata decryptors to avoid redundant retrieval of key
  // using the key_retriever.
  auto aes_metadata_decryptor = GetMetaAesDecryptor(column_key.size());
  auto aes_data_decryptor = GetDataAesDecryptor(column_key.size());

  column_metadata_map_[column_path] = std::make_shared<Decryptor>(
      aes_metadata_decryptor, column_key, file_aad_, aad, pool_);
  column_data_map_[column_path] =
      std::make_shared<Decryptor>(aes_data_decryptor, column_key, file_aad_, aad, pool_);

  if (metadata) return column_metadata_map_[column_path];
  return column_data_map_[column_path];
}

int InternalFileDecryptor::MapKeyLenToDecryptorArrayIndex(int key_len) {
  if (key_len == 16)
    return 0;
  else if (key_len == 24)
    return 1;
  else if (key_len == 32)
    return 2;
  throw ParquetException("decryption key must be 16, 24 or 32 bytes in length");
}

encryption::AesDecryptor* InternalFileDecryptor::GetMetaAesDecryptor(size_t key_size) {
  int key_len = static_cast<int>(key_size);
  int index = MapKeyLenToDecryptorArrayIndex(key_len);
  if (meta_decryptor_[index] == nullptr) {
    meta_decryptor_[index].reset(
        encryption::AesDecryptor::Make(algorithm_, key_len, true, &all_decryptors_));
  }
  return meta_decryptor_[index].get();
}

encryption::AesDecryptor* InternalFileDecryptor::GetDataAesDecryptor(size_t key_size) {
  int key_len = static_cast<int>(key_size);
  int index = MapKeyLenToDecryptorArrayIndex(key_len);
  if (data_decryptor_[index] == nullptr) {
    data_decryptor_[index].reset(
        encryption::AesDecryptor::Make(algorithm_, key_len, false, &all_decryptors_));
  }
  return data_decryptor_[index].get();
}

}  // namespace parquet

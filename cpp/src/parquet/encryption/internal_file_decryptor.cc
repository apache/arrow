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
#include "arrow/util/logging.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/metadata.h"

namespace parquet {

// Decryptor
Decryptor::Decryptor(std::shared_ptr<encryption::AesDecryptor> aes_decryptor,
                     const std::string& key, const std::string& file_aad,
                     const std::string& aad, ::arrow::MemoryPool* pool)
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
  std::lock_guard<std::mutex> lock(mutex_);
  properties_->WipeOutDecryptionKeys();
  for (auto const& i : all_decryptors_) {
    if (auto aes_decryptor = i.lock()) {
      aes_decryptor->WipeOut();
    }
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
  int key_len = static_cast<int>(footer_key.size());
  std::shared_ptr<encryption::AesDecryptor> aes_metadata_decryptor;
  std::shared_ptr<encryption::AesDecryptor> aes_data_decryptor;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    aes_metadata_decryptor = encryption::AesDecryptor::Make(
        algorithm_, key_len, /*metadata=*/true, &all_decryptors_);
    aes_data_decryptor = encryption::AesDecryptor::Make(
        algorithm_, key_len, /*metadata=*/false, &all_decryptors_);
  }

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
  std::string column_key = properties_->column_key(column_path);

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

  int key_len = static_cast<int>(column_key.size());
  std::lock_guard<std::mutex> lock(mutex_);
  auto aes_decryptor =
      encryption::AesDecryptor::Make(algorithm_, key_len, metadata, &all_decryptors_);
  return std::make_shared<Decryptor>(std::move(aes_decryptor), column_key, file_aad_, aad,
                                     pool_);
}

namespace {

std::shared_ptr<Decryptor> GetColumnDecryptor(
    const ColumnCryptoMetaData* crypto_metadata, InternalFileDecryptor* file_decryptor,
    const std::function<std::shared_ptr<Decryptor>(
        InternalFileDecryptor* file_decryptor, const std::string& column_path,
        const std::string& column_key_metadata, const std::string& aad)>& func,
    bool metadata) {
  if (crypto_metadata == nullptr) {
    return nullptr;
  }

  if (file_decryptor == nullptr) {
    throw ParquetException("RowGroup is noted as encrypted but no file decryptor");
  }

  if (crypto_metadata->encrypted_with_footer_key()) {
    return metadata ? file_decryptor->GetFooterDecryptorForColumnMeta()
                    : file_decryptor->GetFooterDecryptorForColumnData();
  }

  // The column is encrypted with its own key
  const std::string& column_key_metadata = crypto_metadata->key_metadata();
  const std::string column_path = crypto_metadata->path_in_schema()->ToDotString();
  return func(file_decryptor, column_path, column_key_metadata, /*aad=*/"");
}

}  // namespace

std::shared_ptr<Decryptor> GetColumnMetaDecryptor(
    const ColumnCryptoMetaData* crypto_metadata, InternalFileDecryptor* file_decryptor) {
  return GetColumnDecryptor(crypto_metadata, file_decryptor,
                            &InternalFileDecryptor::GetColumnMetaDecryptor,
                            /*metadata=*/true);
}

std::shared_ptr<Decryptor> GetColumnDataDecryptor(
    const ColumnCryptoMetaData* crypto_metadata, InternalFileDecryptor* file_decryptor) {
  return GetColumnDecryptor(crypto_metadata, file_decryptor,
                            &InternalFileDecryptor::GetColumnDataDecryptor,
                            /*metadata=*/false);
}

void UpdateDecryptor(const std::shared_ptr<Decryptor>& decryptor,
                     int16_t row_group_ordinal, int16_t column_ordinal,
                     int8_t module_type) {
  ARROW_DCHECK(!decryptor->file_aad().empty());
  const std::string aad =
      encryption::CreateModuleAad(decryptor->file_aad(), module_type, row_group_ordinal,
                                  column_ordinal, kNonPageOrdinal);
  decryptor->UpdateAad(aad);
}

}  // namespace parquet

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
Decryptor::Decryptor(std::unique_ptr<encryption::AesDecryptor> aes_decryptor,
                     const std::string& key, const std::string& file_aad,
                     const std::string& aad, ::arrow::MemoryPool* pool)
    : aes_decryptor_(std::move(aes_decryptor)),
      key_(key),
      file_aad_(file_aad),
      aad_(aad),
      pool_(pool) {}

Decryptor::~Decryptor() = default;

int32_t Decryptor::PlaintextLength(int32_t ciphertext_len) const {
  return aes_decryptor_->PlaintextLength(ciphertext_len);
}

int32_t Decryptor::CiphertextLength(int32_t plaintext_len) const {
  return aes_decryptor_->CiphertextLength(plaintext_len);
}

int32_t Decryptor::Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                           ::arrow::util::span<uint8_t> plaintext) {
  return aes_decryptor_->Decrypt(ciphertext, str2span(key_), str2span(aad_), plaintext);
}

// InternalFileDecryptor
InternalFileDecryptor::InternalFileDecryptor(
    std::shared_ptr<FileDecryptionProperties> properties, const std::string& file_aad,
    ParquetCipher::type algorithm, const std::string& footer_key_metadata,
    ::arrow::MemoryPool* pool)
    : properties_(std::move(properties)),
      file_aad_(file_aad),
      algorithm_(algorithm),
      footer_key_metadata_(footer_key_metadata),
      pool_(pool) {}

std::string InternalFileDecryptor::GetFooterKey() {
  std::unique_lock lock(mutex_);
  if (!footer_key_.empty()) {
    return footer_key_;
  }

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

  // cache footer key to avoid repeated retrieval of key from the key_retriever
  footer_key_ = footer_key;
  return footer_key;
}

std::unique_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptor() {
  std::string aad = encryption::CreateFooterAad(file_aad_);
  return GetFooterDecryptor(aad, true);
}

std::unique_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptor(
    const std::string& aad, bool metadata) {
  std::string footer_key = GetFooterKey();

  auto key_len = static_cast<int32_t>(footer_key.size());
  auto aes_decryptor = encryption::AesDecryptor::Make(algorithm_, key_len, metadata);
  return std::make_unique<Decryptor>(std::move(aes_decryptor), footer_key, file_aad_, aad,
                                     pool_);
}

std::string InternalFileDecryptor::GetColumnKey(const std::string& column_path,
                                                const std::string& column_key_metadata) {
  std::string column_key = properties_->column_key(column_path);

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
  return column_key;
}

std::unique_ptr<Decryptor> InternalFileDecryptor::GetColumnDecryptor(
    const std::string& column_path, const std::string& column_key_metadata,
    const std::string& aad, bool metadata) {
  std::string column_key = GetColumnKey(column_path, column_key_metadata);
  auto key_len = static_cast<int32_t>(column_key.size());
  auto aes_decryptor = encryption::AesDecryptor::Make(algorithm_, key_len, metadata);
  return std::make_unique<Decryptor>(std::move(aes_decryptor), column_key, file_aad_, aad,
                                     pool_);
}

std::function<std::unique_ptr<Decryptor>()>
InternalFileDecryptor::GetColumnDecryptorFactory(
    const ColumnCryptoMetaData* crypto_metadata, const std::string& aad, bool metadata) {
  if (crypto_metadata->encrypted_with_footer_key()) {
    return [this, aad, metadata]() { return GetFooterDecryptor(aad, metadata); };
  }

  // The column is encrypted with its own key
  const std::string& column_key_metadata = crypto_metadata->key_metadata();
  const std::string column_path = crypto_metadata->path_in_schema()->ToDotString();
  std::string column_key = GetColumnKey(column_path, column_key_metadata);

  return [this, aad, metadata, column_key = std::move(column_key)]() {
    auto key_len = static_cast<int32_t>(column_key.size());
    auto aes_decryptor = encryption::AesDecryptor::Make(algorithm_, key_len, metadata);
    return std::make_unique<Decryptor>(std::move(aes_decryptor), column_key, file_aad_,
                                       aad, pool_);
  };
}

std::function<std::unique_ptr<Decryptor>()>
InternalFileDecryptor::GetColumnMetaDecryptorFactory(
    InternalFileDecryptor* file_descryptor, const ColumnCryptoMetaData* crypto_metadata,
    const std::string& aad) {
  if (crypto_metadata == nullptr) {
    // Column is not encrypted
    return [] { return nullptr; };
  }
  if (file_descryptor == nullptr) {
    throw ParquetException("Column is noted as encrypted but no file decryptor");
  }
  return file_descryptor->GetColumnDecryptorFactory(crypto_metadata, aad,
                                                    /*metadata=*/true);
}

std::function<std::unique_ptr<Decryptor>()>
InternalFileDecryptor::GetColumnDataDecryptorFactory(
    InternalFileDecryptor* file_descryptor, const ColumnCryptoMetaData* crypto_metadata,
    const std::string& aad) {
  if (crypto_metadata == nullptr) {
    // Column is not encrypted
    return [] { return nullptr; };
  }
  if (file_descryptor == nullptr) {
    throw ParquetException("Column is noted as encrypted but no file decryptor");
  }
  return file_descryptor->GetColumnDecryptorFactory(crypto_metadata, aad,
                                                    /*metadata=*/false);
}

void UpdateDecryptor(Decryptor* decryptor, int16_t row_group_ordinal,
                     int16_t column_ordinal, int8_t module_type) {
  ARROW_DCHECK(!decryptor->file_aad().empty());
  const std::string aad =
      encryption::CreateModuleAad(decryptor->file_aad(), module_type, row_group_ordinal,
                                  column_ordinal, kNonPageOrdinal);
  decryptor->UpdateAad(aad);
}

}  // namespace parquet

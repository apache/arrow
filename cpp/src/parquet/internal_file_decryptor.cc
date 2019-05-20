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

#include "parquet/internal_file_decryptor.h"
#include "parquet/encryption.h"
#include "parquet/util/crypto.h"

namespace parquet {

// FooterSigningEncryptor
static inline uint8_t* str2bytes(const std::string& str) {
  if (str.empty()) return NULLPTR;

  char* cbytes = const_cast<char*>(str.c_str());
  return reinterpret_cast<uint8_t*>(cbytes);
}

FooterSigningEncryptor::FooterSigningEncryptor(ParquetCipher::type algorithm,
                                               const std::string& key,
                                               const std::string& file_aad,
                                               const std::string& aad)
    : algorithm_(algorithm), key_(key), file_aad_(file_aad), aad_(aad) {
  aes_encryptor_.reset(new parquet_encryption::AesEncryptor(
      algorithm, static_cast<int>(key_.size()), true, NULLPTR));
}

int FooterSigningEncryptor::CiphertextSizeDelta() {
  return aes_encryptor_->CiphertextSizeDelta();
}

int FooterSigningEncryptor::SignedFooterEncrypt(const uint8_t* footer, int footer_len,
                                                uint8_t* nonce,
                                                uint8_t* encrypted_footer) {
  return aes_encryptor_->SignedFooterEncrypt(
      footer, footer_len, str2bytes(key_), static_cast<int>(key_.size()), str2bytes(aad_),
      static_cast<int>(aad_.size()), nonce, encrypted_footer);
}

// Decryptor
Decryptor::Decryptor(parquet_encryption::AesDecryptor* aes_decryptor,
                     const std::string& key, const std::string& file_aad,
                     const std::string& aad)
    : aes_decryptor_(aes_decryptor), key_(key), file_aad_(file_aad), aad_(aad) {}

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
                                             const std::string& footer_key_metadata)
    : properties_(properties),
      file_aad_(file_aad),
      algorithm_(algorithm),
      footer_key_metadata_(footer_key_metadata) {
  if (properties_->is_utilized()) {
    throw ParquetException(
        "Re-using decryption properties with explicit keys for another file");
  }
  properties_->set_utilized();

  all_decryptors_ = std::shared_ptr<std::list<parquet_encryption::AesDecryptor*>>(
      new std::list<parquet_encryption::AesDecryptor*>);
  column_data_map_ = std::shared_ptr<
      std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Decryptor>,
               parquet::schema::ColumnPath::CmpColumnPath>>(
      new std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Decryptor>,
                   schema::ColumnPath::CmpColumnPath>());

  column_metadata_map_ = std::shared_ptr<
      std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Decryptor>,
               parquet::schema::ColumnPath::CmpColumnPath>>(
      new std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Decryptor>,
                   schema::ColumnPath::CmpColumnPath>());
}

void InternalFileDecryptor::wipeout_decryption_keys() {
  properties_->wipeout_decryption_keys();
  for (auto const& i : *all_decryptors_) {
    i->WipeOut();
  }
}

std::shared_ptr<FooterSigningEncryptor>
InternalFileDecryptor::GetFooterSigningEncryptor() {
  if (footer_signing_encryptor_ != NULLPTR) return footer_signing_encryptor_;
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

  std::string aad = parquet_encryption::createFooterAAD(file_aad_);

  footer_signing_encryptor_ =
      std::make_shared<FooterSigningEncryptor>(algorithm_, footer_key, file_aad_, aad);
  return footer_signing_encryptor_;
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptor() {
  std::string aad = parquet_encryption::createFooterAAD(file_aad_);
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
    if (footer_metadata_decryptor_ != NULLPTR) return footer_metadata_decryptor_;
  } else {
    if (footer_data_decryptor_ != NULLPTR) return footer_data_decryptor_;
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

  std::shared_ptr<Decryptor> footer_metadata_decryptor =
      std::make_shared<Decryptor>(aes_metadata_decryptor, footer_key, file_aad_, aad);
  std::shared_ptr<Decryptor> footer_data_decryptor =
      std::make_shared<Decryptor>(aes_data_decryptor, footer_key, file_aad_, aad);

  footer_metadata_decryptor_ = footer_metadata_decryptor;
  footer_data_decryptor_ = footer_data_decryptor;

  if (metadata) return footer_metadata_decryptor;
  return footer_data_decryptor;
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnMetaDecryptor(
    std::shared_ptr<schema::ColumnPath> column_path,
    const std::string& column_key_metadata, const std::string& aad) {
  return GetColumnDecryptor(column_path, column_key_metadata, aad, true);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnDataDecryptor(
    std::shared_ptr<schema::ColumnPath> column_path,
    const std::string& column_key_metadata, const std::string& aad) {
  return GetColumnDecryptor(column_path, column_key_metadata, aad, false);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnDecryptor(
    std::shared_ptr<schema::ColumnPath> column_path,
    const std::string& column_key_metadata, const std::string& aad, bool metadata) {
  std::string column_key;
  // first look if we already got the decryptor from before
  if (metadata) {
    if (column_metadata_map_->find(column_path) != column_metadata_map_->end()) {
      return column_metadata_map_->at(column_path);
    }
  } else {
    if (column_data_map_->find(column_path) != column_data_map_->end()) {
      return column_data_map_->at(column_path);
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
      ss << "HiddenColumnException, path=" + column_path->ToDotString() + " " << e.what()
         << "\n";
      throw HiddenColumnException(ss.str());
    }
  }
  if (column_key.empty()) {
    throw HiddenColumnException("HiddenColumnException, path=" +
                                column_path->ToDotString());
  }

  // Create both data and metadata decryptors to avoid redundant retrieval of key
  // using the key_retriever.
  auto aes_metadata_decryptor = GetMetaAesDecryptor(column_key.size());
  auto aes_data_decryptor = GetDataAesDecryptor(column_key.size());

  std::shared_ptr<Decryptor> metadata_decryptor =
      std::make_shared<Decryptor>(aes_metadata_decryptor, column_key, file_aad_, aad);
  std::shared_ptr<Decryptor> data_decryptor =
      std::make_shared<Decryptor>(aes_data_decryptor, column_key, file_aad_, aad);

  (*column_metadata_map_)[column_path] = metadata_decryptor;
  (*column_data_map_)[column_path] = data_decryptor;

  if (metadata) return metadata_decryptor;
  return data_decryptor;
}

parquet_encryption::AesDecryptor* InternalFileDecryptor::GetMetaAesDecryptor(
    size_t key_size) {
  int key_len = static_cast<int>(key_size);
  if (key_len == 16) {
    if (meta_decryptor_128_ == NULLPTR) {
      meta_decryptor_128_.reset(new parquet_encryption::AesDecryptor(
          algorithm_, key_len, true, all_decryptors_));
    }
    return meta_decryptor_128_.get();
  } else if (key_len == 24) {
    if (meta_decryptor_196_ == NULLPTR) {
      meta_decryptor_196_.reset(new parquet_encryption::AesDecryptor(
          algorithm_, key_len, true, all_decryptors_));
    }
    return meta_decryptor_196_.get();
  } else if (key_len == 32) {
    if (meta_decryptor_256_ == NULLPTR) {
      meta_decryptor_256_.reset(new parquet_encryption::AesDecryptor(
          algorithm_, key_len, true, all_decryptors_));
    }
    return meta_decryptor_256_.get();
  }
  throw ParquetException("encryption key must be 16, 24 or 32 bytes in length");
}

parquet_encryption::AesDecryptor* InternalFileDecryptor::GetDataAesDecryptor(
    size_t key_size) {
  int key_len = static_cast<int>(key_size);
  if (key_len == 16) {
    if (data_decryptor_128_ == NULLPTR) {
      data_decryptor_128_.reset(new parquet_encryption::AesDecryptor(
          algorithm_, key_len, false, all_decryptors_));
    }
    return data_decryptor_128_.get();
  } else if (key_len == 24) {
    if (data_decryptor_196_ == NULLPTR) {
      data_decryptor_196_.reset(new parquet_encryption::AesDecryptor(
          algorithm_, key_len, false, all_decryptors_));
    }
    return data_decryptor_196_.get();
  } else if (key_len == 32) {
    if (data_decryptor_256_ == NULLPTR) {
      data_decryptor_256_.reset(new parquet_encryption::AesDecryptor(
          algorithm_, key_len, false, all_decryptors_));
    }
    return data_decryptor_256_.get();
  }
  throw ParquetException("encryption key must be 16, 24 or 32 bytes in length");
}

}  // namespace parquet

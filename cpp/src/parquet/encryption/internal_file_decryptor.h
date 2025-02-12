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

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "parquet/schema.h"

namespace parquet {

namespace encryption {
class AesDecryptor;
class AesEncryptor;
}  // namespace encryption

class ColumnCryptoMetaData;
class FileDecryptionProperties;

class PARQUET_EXPORT Decryptor {
 public:
  Decryptor(std::shared_ptr<encryption::AesDecryptor> decryptor, const std::string& key,
            const std::string& file_aad, const std::string& aad,
            ::arrow::MemoryPool* pool);

  const std::string& file_aad() const { return file_aad_; }
  void UpdateAad(const std::string& aad) { aad_ = aad; }
  ::arrow::MemoryPool* pool() { return pool_; }

  [[nodiscard]] int32_t PlaintextLength(int32_t ciphertext_len) const;
  [[nodiscard]] int32_t CiphertextLength(int32_t plaintext_len) const;
  int32_t Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                  ::arrow::util::span<uint8_t> plaintext);

 private:
  std::shared_ptr<encryption::AesDecryptor> aes_decryptor_;
  std::string key_;
  std::string file_aad_;
  std::string aad_;
  ::arrow::MemoryPool* pool_;
};

class InternalFileDecryptor {
 public:
  explicit InternalFileDecryptor(FileDecryptionProperties* properties,
                                 const std::string& file_aad,
                                 ParquetCipher::type algorithm,
                                 const std::string& footer_key_metadata,
                                 ::arrow::MemoryPool* pool);
  ~InternalFileDecryptor();

  const std::string& file_aad() const { return file_aad_; }

  std::string GetFooterKey();

  ParquetCipher::type algorithm() const { return algorithm_; }

  const std::string& footer_key_metadata() const { return footer_key_metadata_; }

  const FileDecryptionProperties* properties() const { return properties_; }

  void WipeOutDecryptionKeys();

  ::arrow::MemoryPool* pool() const { return pool_; }

  std::shared_ptr<Decryptor> GetFooterDecryptor();

  std::shared_ptr<Decryptor> GetColumnMetaDecryptor(
      const std::string& column_path, const std::string& column_key_metadata,
      const std::string& aad = "") {
    return GetColumnDecryptor(column_path, column_key_metadata, aad, /*metadata=*/true);
  }

  std::shared_ptr<Decryptor> GetColumnDataDecryptor(
      const std::string& column_path, const std::string& column_key_metadata,
      const std::string& aad = "") {
    return GetColumnDecryptor(column_path, column_key_metadata, aad, /*metadata=*/false);
  }

  // These functions are static as they may be called with a null InternalFileDecryptor*.
  static std::function<std::shared_ptr<Decryptor>()> GetColumnMetaDecryptorFactory(
      InternalFileDecryptor*, const ColumnCryptoMetaData* crypto_metadata,
      const std::string& aad = "");
  static std::function<std::shared_ptr<Decryptor>()> GetColumnDataDecryptorFactory(
      InternalFileDecryptor*, const ColumnCryptoMetaData* crypto_metadata,
      const std::string& aad = "");

 private:
  FileDecryptionProperties* properties_;
  // Concatenation of aad_prefix (if exists) and aad_file_unique
  std::string file_aad_;
  ParquetCipher::type algorithm_;
  std::string footer_key_metadata_;
  ::arrow::MemoryPool* pool_;

  // Protects footer_key_ updates
  std::mutex mutex_;
  std::string footer_key_;

  std::string GetColumnKey(const std::string& column_path,
                           const std::string& column_key_metadata);

  std::shared_ptr<Decryptor> GetFooterDecryptor(const std::string& aad, bool metadata);

  std::shared_ptr<Decryptor> GetColumnDecryptor(const std::string& column_path,
                                                const std::string& column_key_metadata,
                                                const std::string& aad, bool metadata);

  std::function<std::shared_ptr<Decryptor>()> GetColumnDecryptorFactory(
      const ColumnCryptoMetaData* crypto_metadata, const std::string& aad, bool metadata);
};

void UpdateDecryptor(const std::shared_ptr<Decryptor>& decryptor,
                     int16_t row_group_ordinal, int16_t column_ordinal,
                     int8_t module_type);

}  // namespace parquet

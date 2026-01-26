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

#include <map>
#include <memory>
#include <string>

#include "parquet/encryption/aes_encryption.h"
#include "parquet/encryption/encoding_properties.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/encryptor_interface.h"
#include "parquet/encryption/external_dbpa_encryption.h"
#include "parquet/metadata.h"

namespace parquet {

using ::parquet::encryption::EncodingProperties;

class FileEncryptionProperties;
class ColumnEncryptionProperties;

class PARQUET_EXPORT Encryptor {
 public:
  Encryptor(encryption::EncryptorInterface* encryptor_interface,
            ::arrow::util::SecureString key, std::string file_aad, std::string aad,
            ::arrow::MemoryPool* pool);
  const std::string& file_aad() { return file_aad_; }
  void UpdateAad(const std::string& aad) { aad_ = aad; }
  ::arrow::MemoryPool* pool() { return pool_; }

  [[nodiscard]] bool CanCalculateCiphertextLength() const;
  [[nodiscard]] int32_t CiphertextLength(int64_t plaintext_len) const;

  int32_t Encrypt(::arrow::util::span<const uint8_t> plaintext,
                  ::arrow::util::span<uint8_t> ciphertext);

  int32_t EncryptWithManagedBuffer(::arrow::util::span<const uint8_t> plaintext,
                                   ::arrow::ResizableBuffer* ciphertext);

  void UpdateEncodingProperties(std::unique_ptr<EncodingProperties> encoding_properties);

  /// After the column_writer writes a dictionary or a data page, this method will
  /// be called so that each encryptor can provide any encryptor-specific column
  /// metadata that should be stored in the Parquet file. The keys and values are
  /// added to the column metadata, any conflicting key and value pairs are
  /// overwritten. There is no need to clear the metadata after the call.
  std::shared_ptr<KeyValueMetadata> GetKeyValueMetadata(int8_t module_type);

  bool EncryptColumnMetaData(
      bool encrypted_footer,
      const std::shared_ptr<ColumnEncryptionProperties>& column_encryption_properties) {
    // if column is not encrypted then do not encrypt the column metadata
    if (!column_encryption_properties || !column_encryption_properties->is_encrypted())
      return false;
    // if plaintext footer then encrypt the column metadata
    if (!encrypted_footer) return true;
    // if column is not encrypted with footer key then encrypt the column metadata
    return !column_encryption_properties->is_encrypted_with_footer_key();
  }

 private:
  encryption::EncryptorInterface* encryptor_instance_;
  ::arrow::util::SecureString key_;
  std::string file_aad_;
  std::string aad_;
  ::arrow::MemoryPool* pool_;
};

class InternalFileEncryptor {
 public:
  explicit InternalFileEncryptor(FileEncryptionProperties* properties,
                                 ::arrow::MemoryPool* pool);

  std::shared_ptr<Encryptor> GetFooterEncryptor();
  std::shared_ptr<Encryptor> GetFooterSigningEncryptor();
  std::shared_ptr<Encryptor> GetColumnMetaEncryptor(const std::string& column_path);
  std::shared_ptr<Encryptor> GetColumnDataEncryptor(
      const std::string& column_path,
      const ColumnChunkMetaDataBuilder* column_chunk_metadata = nullptr);

 private:
  FileEncryptionProperties* properties_;

  std::map<std::string, std::shared_ptr<Encryptor>> column_data_map_;
  std::map<std::string, std::shared_ptr<Encryptor>> column_metadata_map_;

  std::shared_ptr<Encryptor> footer_signing_encryptor_;
  std::shared_ptr<Encryptor> footer_encryptor_;

  ::arrow::MemoryPool* pool_;
  encryption::AesEncryptorFactory aes_encryptor_factory_;
  encryption::ExternalDBPAEncryptorAdapterFactory external_dbpa_encryptor_factory_;

  std::shared_ptr<Encryptor> GetColumnEncryptor(
      const std::string& column_path, bool metadata,
      const ColumnChunkMetaDataBuilder* column_chunk_metadata = nullptr);

  encryption::EncryptorInterface* GetMetaEncryptor(ParquetCipher::type algorithm,
                                                   size_t key_len);

  encryption::EncryptorInterface* GetDataEncryptor(
      ParquetCipher::type algorithm, size_t key_len,
      const ColumnChunkMetaDataBuilder* column_chunk_metadata = nullptr);
};

}  // namespace parquet

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
#include <vector>

#include "parquet/encryption/encryption.h"
#include "parquet/schema.h"

namespace parquet {

namespace encryption {
class AesEncryptor;
}  // namespace encryption

class FileEncryptionProperties;
class ColumnEncryptionProperties;

class PARQUET_EXPORT Encryptor {
 public:
  Encryptor(encryption::AesEncryptor* aes_encryptor, const std::string& key,
            const std::string& file_aad, const std::string& aad,
            ::arrow::MemoryPool* pool);
  const std::string& file_aad() { return file_aad_; }
  void UpdateAad(const std::string& aad) { aad_ = aad; }
  ::arrow::MemoryPool* pool() { return pool_; }

  int CiphertextSizeDelta();
  int Encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* ciphertext);

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
  encryption::AesEncryptor* aes_encryptor_;
  std::string key_;
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
  std::shared_ptr<Encryptor> GetColumnDataEncryptor(const std::string& column_path);
  void WipeOutEncryptionKeys();

 private:
  FileEncryptionProperties* properties_;

  std::map<std::string, std::shared_ptr<Encryptor>> column_data_map_;
  std::map<std::string, std::shared_ptr<Encryptor>> column_metadata_map_;

  std::shared_ptr<Encryptor> footer_signing_encryptor_;
  std::shared_ptr<Encryptor> footer_encryptor_;

  std::vector<encryption::AesEncryptor*> all_encryptors_;

  // Key must be 16, 24 or 32 bytes in length. Thus there could be up to three
  // types of meta_encryptors and data_encryptors.
  std::unique_ptr<encryption::AesEncryptor> meta_encryptor_[3];
  std::unique_ptr<encryption::AesEncryptor> data_encryptor_[3];

  ::arrow::MemoryPool* pool_;

  std::shared_ptr<Encryptor> GetColumnEncryptor(const std::string& column_path,
                                                bool metadata);

  encryption::AesEncryptor* GetMetaAesEncryptor(ParquetCipher::type algorithm,
                                                size_t key_len);
  encryption::AesEncryptor* GetDataAesEncryptor(ParquetCipher::type algorithm,
                                                size_t key_len);

  int MapKeyLenToEncryptorArrayIndex(int key_len);
};

}  // namespace parquet

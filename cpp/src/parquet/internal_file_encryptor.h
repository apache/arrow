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

#ifndef INTERNAL_FILE_ENCRYPTOR_H
#define INTERNAL_FILE_ENCRYPTOR_H

#include <list>
#include <map>
#include <memory>
#include <string>

#include "parquet/encryption.h"
#include "parquet/schema.h"

namespace parquet_encryption {
class AesEncryptor;
}

namespace parquet {

class FileEncryptionProperties;
class ColumnEncryptionProperties;

class Encryptor {
 public:
  Encryptor(parquet_encryption::AesEncryptor* aes_encryptor, const std::string& key,
            const std::string& file_aad, const std::string& aad);
  const std::string& file_aad() { return file_aad_; }
  void update_aad(const std::string& aad) { aad_ = aad; }

  int CiphertextSizeDelta();
  int Encrypt(const uint8_t* plaintext, int plaintext_len, uint8_t* ciphertext);

  bool encryptColumnMetaData(
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
  parquet_encryption::AesEncryptor* aes_encryptor_;
  std::string key_;
  std::string file_aad_;
  std::string aad_;
};

class InternalFileEncryptor {
 public:
  explicit InternalFileEncryptor(FileEncryptionProperties* propperties);

  std::shared_ptr<Encryptor> GetFooterEncryptor();
  std::shared_ptr<Encryptor> GetFooterSigningEncryptor();
  std::shared_ptr<Encryptor> GetColumnMetaEncryptor(
      const std::shared_ptr<schema::ColumnPath>& column_path);
  std::shared_ptr<Encryptor> GetColumnDataEncryptor(
      const std::shared_ptr<schema::ColumnPath>& column_path);
  void wipeout_encryption_keys();

 private:
  FileEncryptionProperties* properties_;

  std::shared_ptr<
      std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Encryptor>,
               parquet::schema::ColumnPath::CmpColumnPath>>
      column_data_map_;
  std::shared_ptr<
      std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Encryptor>,
               parquet::schema::ColumnPath::CmpColumnPath>>
      column_metadata_map_;

  std::shared_ptr<Encryptor> footer_signing_encryptor_;
  std::shared_ptr<Encryptor> footer_encryptor_;

  std::shared_ptr<std::list<parquet_encryption::AesEncryptor*>> all_encryptors_;

  std::unique_ptr<parquet_encryption::AesEncryptor> meta_encryptor_128_;
  std::unique_ptr<parquet_encryption::AesEncryptor> meta_encryptor_196_;
  std::unique_ptr<parquet_encryption::AesEncryptor> meta_encryptor_256_;
  std::unique_ptr<parquet_encryption::AesEncryptor> data_encryptor_128_;
  std::unique_ptr<parquet_encryption::AesEncryptor> data_encryptor_196_;
  std::unique_ptr<parquet_encryption::AesEncryptor> data_encryptor_256_;

  std::shared_ptr<Encryptor> GetColumnEncryptor(
      const std::shared_ptr<schema::ColumnPath>& column_path, bool metadata);

  parquet_encryption::AesEncryptor* GetMetaAesEncryptor(ParquetCipher::type algorithm,
                                                        size_t key_len);
  parquet_encryption::AesEncryptor* GetDataAesEncryptor(ParquetCipher::type algorithm,
                                                        size_t key_len);
};

}  // namespace parquet

#endif  // INTERNAL_FILE_ENCRYPTORS_H

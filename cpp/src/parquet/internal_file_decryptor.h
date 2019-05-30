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

#ifndef INTERNAL_FILE_DECRYPTOR_H
#define INTERNAL_FILE_DECRYPTOR_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "parquet/schema.h"

namespace parquet {

namespace encryption {
class AesDecryptor;
class AesEncryptor;
}  // namespace encryption

class FileDecryptionProperties;

class FooterSigningEncryptor {
 public:
  FooterSigningEncryptor(ParquetCipher::type algorithm, const std::string& key,
                         const std::string& file_aad, const std::string& aad);
  int CiphertextSizeDelta();
  int SignedFooterEncrypt(const uint8_t* footer, int footer_len, uint8_t* nonce,
                          uint8_t* encrypted_footer);

 private:
  std::string key_;
  std::string file_aad_;
  std::string aad_;

  std::shared_ptr<encryption::AesEncryptor> aes_encryptor_;
};

class Decryptor {
 public:
  Decryptor(encryption::AesDecryptor* decryptor, const std::string& key,
            const std::string& file_aad, const std::string& aad);

  const std::string& file_aad() const { return file_aad_; }
  void update_aad(const std::string& aad) { aad_ = aad; }

  int CiphertextSizeDelta();
  int Decrypt(const uint8_t* ciphertext, int ciphertext_len, uint8_t* plaintext);

 private:
  encryption::AesDecryptor* aes_decryptor_;
  std::string key_;
  std::string file_aad_;
  std::string aad_;
};

class InternalFileDecryptor {
 public:
  explicit InternalFileDecryptor(FileDecryptionProperties* properties,
                                 const std::string& file_aad,
                                 ParquetCipher::type algorithm,
                                 const std::string& footer_key_metadata);

  std::string& file_aad() { return file_aad_; }

  ParquetCipher::type algorithm() { return algorithm_; }

  std::string& footer_key_metadata() { return footer_key_metadata_; }

  std::shared_ptr<FooterSigningEncryptor> GetFooterSigningEncryptor();

  FileDecryptionProperties* properties() { return properties_; }

  void wipeout_decryption_keys();

  std::shared_ptr<Decryptor> GetFooterDecryptor();
  std::shared_ptr<Decryptor> GetFooterDecryptorForColumnMeta(const std::string& aad = "");
  std::shared_ptr<Decryptor> GetFooterDecryptorForColumnData(const std::string& aad = "");
  std::shared_ptr<Decryptor> GetColumnMetaDecryptor(
      std::shared_ptr<schema::ColumnPath> column_path,
      const std::string& column_key_metadata, const std::string& aad = "");
  std::shared_ptr<Decryptor> GetColumnDataDecryptor(
      std::shared_ptr<schema::ColumnPath> column_path,
      const std::string& column_key_metadata, const std::string& aad = "");

 private:
  FileDecryptionProperties* properties_;
  // Concatenation of aad_prefix (if exists) and aad_file_unique
  std::string file_aad_;
  std::shared_ptr<
      std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Decryptor>,
               parquet::schema::ColumnPath::CmpColumnPath>>
      column_data_map_;
  std::shared_ptr<
      std::map<std::shared_ptr<schema::ColumnPath>, std::shared_ptr<Decryptor>,
               parquet::schema::ColumnPath::CmpColumnPath>>
      column_metadata_map_;

  std::shared_ptr<Decryptor> footer_metadata_decryptor_;
  std::shared_ptr<Decryptor> footer_data_decryptor_;
  ParquetCipher::type algorithm_;
  std::string footer_key_metadata_;
  std::shared_ptr<FooterSigningEncryptor> footer_signing_encryptor_;
  std::shared_ptr<std::vector<encryption::AesDecryptor*>> all_decryptors_;

  std::unique_ptr<encryption::AesDecryptor> meta_decryptor_128_;
  std::unique_ptr<encryption::AesDecryptor> meta_decryptor_196_;
  std::unique_ptr<encryption::AesDecryptor> meta_decryptor_256_;
  std::unique_ptr<encryption::AesDecryptor> data_decryptor_128_;
  std::unique_ptr<encryption::AesDecryptor> data_decryptor_196_;
  std::unique_ptr<encryption::AesDecryptor> data_decryptor_256_;

  std::shared_ptr<Decryptor> GetFooterDecryptor(const std::string& aad, bool metadata);
  std::shared_ptr<Decryptor> GetColumnDecryptor(
      std::shared_ptr<schema::ColumnPath> column_path,
      const std::string& column_key_metadata, const std::string& aad,
      bool metadata = false);

  encryption::AesDecryptor* GetMetaAesDecryptor(size_t key_size);
  encryption::AesDecryptor* GetDataAesDecryptor(size_t key_size);
};

}  // namespace parquet

#endif  // INTERNAL_FILE_ENCRYPTORS_H

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
//
// Self-contained tests that generate and read encrypted files in a local
// temporary directory, without relying on parquet-testing data files.

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/util/config.h"

#include "parquet/encryption/encryption.h"
#include "parquet/encryption/external/test_utils.h"
#include "parquet/encryption/test_encryption_util.h"

namespace parquet::encryption::test {

class PerColumnEncryption : public ::testing::Test {
 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;
  FileEncryptor encryptor_;
  FileDecryptor decryptor_;

  void SetUp() override {
#ifndef ARROW_WITH_SNAPPY
    GTEST_SKIP() << "Test requires Snappy compression";
#endif
    temp_dir_ = temp_data_dir().ValueOrDie();
  }
};

TEST_F(PerColumnEncryption, CTR_WriteRead) {
  // Build encryption properties: two columns and the footer with different keys,
  // using AES_GCM_CTR_V1.
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols;
  parquet::ColumnEncryptionProperties::Builder col_builder_double(kDoubleFieldName);
  parquet::ColumnEncryptionProperties::Builder col_builder_float(kFloatFieldName);
  col_builder_double.key(kColumnEncryptionKey1)->key_id("kc1");
  col_builder_float.key(kColumnEncryptionKey2)->key_id("kc2");
  encryption_cols[kDoubleFieldName] = col_builder_double.build();
  encryption_cols[kFloatFieldName] = col_builder_float.build();

  parquet::FileEncryptionProperties::Builder file_encryption_builder(
      kFooterEncryptionKey);
  const std::string file_path = temp_dir_->path().ToString() +
                                "rw_encrypt_columns_and_footer_ctr.parquet.encrypted";
  // Write the encrypted file to the temporary location .
  // It uses the built-in synthetic dataset from FileEncryptor (not an external file).
  // The test calls FileEncryptor::EncryptFile(...), which writes fixed data with a
  // predefined schema and values.
  encryptor_.EncryptFile(file_path,
                         file_encryption_builder.footer_key_metadata("kf")
                             ->encrypted_columns(encryption_cols)
                             ->algorithm(parquet::ParquetCipher::AES_GCM_CTR_V1)
                             ->build());

  // Decrypt using key retriever
  auto retriever_1 = std::make_shared<parquet::StringKeyIdRetriever>();
  retriever_1->PutKey("kf", kFooterEncryptionKey);
  retriever_1->PutKey("kc1", kColumnEncryptionKey1);
  retriever_1->PutKey("kc2", kColumnEncryptionKey2);
  parquet::FileDecryptionProperties::Builder dec_builder_1;
  auto dec_props_1 = dec_builder_1.key_retriever(retriever_1)->build();
  EXPECT_NO_THROW(decryptor_.DecryptFile(file_path, dec_props_1));
  EXPECT_NO_THROW(decryptor_.DecryptPageIndex(file_path, dec_props_1));

  // Decrypt using explicit keys
  std::map<std::string, std::shared_ptr<parquet::ColumnDecryptionProperties>>
      decryption_cols;
  parquet::ColumnDecryptionProperties::Builder decryption_double(kDoubleFieldName);
  parquet::ColumnDecryptionProperties::Builder decryption_float(kFloatFieldName);
  decryption_cols[kDoubleFieldName] =
      decryption_double.key(kColumnEncryptionKey1)->build();
  decryption_cols[kFloatFieldName] = decryption_float.key(kColumnEncryptionKey2)->build();
  parquet::FileDecryptionProperties::Builder dec_builder_3;
  auto dec_props_3 = dec_builder_3.footer_key(kFooterEncryptionKey)
                         ->column_keys(decryption_cols)
                         ->build();
  EXPECT_NO_THROW(decryptor_.DecryptFile(file_path, dec_props_3));
  EXPECT_NO_THROW(decryptor_.DecryptPageIndex(file_path, dec_props_3));
}

TEST_F(PerColumnEncryption, PerColumnExternal_WriteRead) {
  // Build encryption properties: mix of file-level AES_GCM_V1 and per-column
  // EXTERNAL_DBPA_V1.
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols;
  parquet::ColumnEncryptionProperties::Builder col_builder_double(kDoubleFieldName);
  parquet::ColumnEncryptionProperties::Builder col_builder_float(kFloatFieldName);
  col_builder_double.key(kColumnEncryptionKey1)->key_id("kc1");
  col_builder_double.parquet_cipher(parquet::ParquetCipher::AES_GCM_V1);
  col_builder_float.key(kColumnEncryptionKey2)->key_id("kc2");
  col_builder_float.parquet_cipher(parquet::ParquetCipher::EXTERNAL_DBPA_V1);
  encryption_cols[kDoubleFieldName] = col_builder_double.build();
  encryption_cols[kFloatFieldName] = col_builder_float.build();

  std::string library_path =
      parquet::encryption::external::test::TestUtils::GetTestLibraryPath();

  parquet::ExternalFileEncryptionProperties::Builder file_encryption_builder(
      kFooterEncryptionKey);
  const std::string file_path = temp_dir_->path().ToString() +
                                "rw_encrypt_with_per_column_encryption.parquet.encrypted";
  // It uses the built-in synthetic dataset from FileEncryptor (not an external file).
  // The test calls FileEncryptor::EncryptFile(...), which writes fixed data with a
  // predefined schema and values.
  encryptor_.EncryptFile(file_path, file_encryption_builder.footer_key_metadata("kf")
                                        ->encrypted_columns(encryption_cols)
                                        ->algorithm(parquet::ParquetCipher::AES_GCM_V1)
                                        ->configuration_properties(
                                            {{parquet::ParquetCipher::EXTERNAL_DBPA_V1,
                                              {{"agent_library_path", library_path},
                                               {"file_path", "/tmp/test"},
                                               {"other_config", "value"}}}})
                                        ->build_external());

  // Decrypt using external configuration (decryption config 5 equivalent).
  auto retriever = std::make_shared<parquet::StringKeyIdRetriever>();
  retriever->PutKey("kf", kFooterEncryptionKey);
  retriever->PutKey("kc1", kColumnEncryptionKey1);
  retriever->PutKey("kc2", kColumnEncryptionKey2);
  parquet::ExternalFileDecryptionProperties::Builder file_decryption_builder;
  file_decryption_builder.key_retriever(retriever);
  file_decryption_builder.configuration_properties(
      {{parquet::ParquetCipher::EXTERNAL_DBPA_V1,
        {{"agent_library_path", library_path},
         {"file_path", "/tmp/test"},
         {"other_config", "value"}}}});
  auto dec_props = file_decryption_builder.build_external();

  EXPECT_NO_THROW(decryptor_.DecryptFile(file_path, dec_props));
  EXPECT_NO_THROW(decryptor_.DecryptPageIndex(file_path, dec_props));
}

}  // namespace parquet::encryption::test

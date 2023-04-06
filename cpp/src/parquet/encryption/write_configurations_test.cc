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

#include <gtest/gtest.h>

#include <stdio.h>

#include <arrow/io/file.h>

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/test_util.h"

/*
 * This file contains unit-tests for writing encrypted Parquet files with
 * different encryption configurations.
 * The files are saved in temporary folder and will be deleted after reading
 * them in encryption-read-configurations-test.cc test.
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * Each unit-test creates a single parquet file with eight columns using one of the
 * following encryption configurations:
 *
 *  - Encryption configuration 1:   Encrypt all columns and the footer with the same key.
 *                                  (uniform encryption)
 *  - Encryption configuration 2:   Encrypt two columns and the footer, with different
 *                                  keys.
 *  - Encryption configuration 3:   Encrypt two columns, with different keys.
 *                                  Don’t encrypt footer (to enable legacy readers)
 *                                  - plaintext footer mode.
 *  - Encryption configuration 4:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix for file identity
 *                                  verification.
 *  - Encryption configuration 5:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix, and call
 *                                  disable_aad_prefix_storage to prevent file
 *                                  identity storage in file metadata.
 *  - Encryption configuration 6:   Encrypt two columns and the footer, with different
 *                                  keys. Use the alternative (AES_GCM_CTR_V1) algorithm.
 */

namespace parquet {
namespace encryption {
namespace test {

using FileClass = ::arrow::io::FileOutputStream;

std::unique_ptr<TemporaryDir> temp_dir;

class TestEncryptionConfiguration : public ::testing::Test {
 public:
  static void SetUpTestCase();

 protected:
  FileEncryptor encryptor_;

  std::string path_to_double_field_ = kDoubleFieldName;
  std::string path_to_float_field_ = kFloatFieldName;
  std::string file_name_;
  std::string kFooterEncryptionKey_ = std::string(kFooterEncryptionKey);
  std::string kColumnEncryptionKey1_ = std::string(kColumnEncryptionKey1);
  std::string kColumnEncryptionKey2_ = std::string(kColumnEncryptionKey2);
  std::string kFileName_ = std::string(kFileName);

  void EncryptFile(
      std::shared_ptr<parquet::FileEncryptionProperties> encryption_configurations,
      std::string file_name) {
    std::string file = temp_dir->path().ToString() + file_name;
    encryptor_.EncryptFile(file, encryption_configurations);
  }
};

// Encryption configuration 1: Encrypt all columns and the footer with the same key.
// (uniform encryption)
TEST_F(TestEncryptionConfiguration, UniformEncryption) {
  parquet::FileEncryptionProperties::Builder file_encryption_builder_1(
      kFooterEncryptionKey_);

  this->EncryptFile(file_encryption_builder_1.footer_key_metadata("kf")->build(),
                    "tmp_uniform_encryption.parquet.encrypted");
}

// Encryption configuration 2: Encrypt two columns and the footer, with different keys.
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsAndTheFooter) {
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols2;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_20(
      path_to_double_field_);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_21(
      path_to_float_field_);
  encryption_col_builder_20.key(kColumnEncryptionKey1_)->key_id("kc1");
  encryption_col_builder_21.key(kColumnEncryptionKey2_)->key_id("kc2");

  encryption_cols2[path_to_double_field_] = encryption_col_builder_20.build();
  encryption_cols2[path_to_float_field_] = encryption_col_builder_21.build();

  parquet::FileEncryptionProperties::Builder file_encryption_builder_2(
      kFooterEncryptionKey_);

  this->EncryptFile(file_encryption_builder_2.footer_key_metadata("kf")
                        ->encrypted_columns(encryption_cols2)
                        ->build(),
                    "tmp_encrypt_columns_and_footer.parquet.encrypted");
}

// Encryption configuration 3: Encrypt two columns, with different keys.
// Don’t encrypt footer.
// (plaintext footer mode, readable by legacy readers)
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsWithPlaintextFooter) {
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols3;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_30(
      path_to_double_field_);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_31(
      path_to_float_field_);
  encryption_col_builder_30.key(kColumnEncryptionKey1_)->key_id("kc1");
  encryption_col_builder_31.key(kColumnEncryptionKey2_)->key_id("kc2");

  encryption_cols3[path_to_double_field_] = encryption_col_builder_30.build();
  encryption_cols3[path_to_float_field_] = encryption_col_builder_31.build();
  parquet::FileEncryptionProperties::Builder file_encryption_builder_3(
      kFooterEncryptionKey_);

  this->EncryptFile(file_encryption_builder_3.footer_key_metadata("kf")
                        ->encrypted_columns(encryption_cols3)
                        ->set_plaintext_footer()
                        ->build(),
                    "tmp_encrypt_columns_plaintext_footer.parquet.encrypted");
}

// Encryption configuration 4: Encrypt two columns and the footer, with different keys.
// Use aad_prefix.
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsAndFooterWithAadPrefix) {
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols4;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_40(
      path_to_double_field_);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_41(
      path_to_float_field_);
  encryption_col_builder_40.key(kColumnEncryptionKey1_)->key_id("kc1");
  encryption_col_builder_41.key(kColumnEncryptionKey2_)->key_id("kc2");

  encryption_cols4[path_to_double_field_] = encryption_col_builder_40.build();
  encryption_cols4[path_to_float_field_] = encryption_col_builder_41.build();
  parquet::FileEncryptionProperties::Builder file_encryption_builder_4(
      kFooterEncryptionKey_);

  this->EncryptFile(file_encryption_builder_4.footer_key_metadata("kf")
                        ->encrypted_columns(encryption_cols4)
                        ->aad_prefix(kFileName_)
                        ->build(),
                    "tmp_encrypt_columns_and_footer_aad.parquet.encrypted");
}

// Encryption configuration 5: Encrypt two columns and the footer, with different keys.
// Use aad_prefix and disable_aad_prefix_storage.
TEST_F(TestEncryptionConfiguration,
       EncryptTwoColumnsAndFooterWithAadPrefixDisable_aad_prefix_storage) {
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols5;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_50(
      path_to_double_field_);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_51(
      path_to_float_field_);
  encryption_col_builder_50.key(kColumnEncryptionKey1_)->key_id("kc1");
  encryption_col_builder_51.key(kColumnEncryptionKey2_)->key_id("kc2");

  encryption_cols5[path_to_double_field_] = encryption_col_builder_50.build();
  encryption_cols5[path_to_float_field_] = encryption_col_builder_51.build();
  parquet::FileEncryptionProperties::Builder file_encryption_builder_5(
      kFooterEncryptionKey_);

  this->EncryptFile(
      file_encryption_builder_5.encrypted_columns(encryption_cols5)
          ->footer_key_metadata("kf")
          ->aad_prefix(kFileName_)
          ->disable_aad_prefix_storage()
          ->build(),
      "tmp_encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted");
}

// Encryption configuration 6: Encrypt two columns and the footer, with different keys.
// Use AES_GCM_CTR_V1 algorithm.
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsAndFooterUseAES_GCM_CTR) {
  std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>>
      encryption_cols6;
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_60(
      path_to_double_field_);
  parquet::ColumnEncryptionProperties::Builder encryption_col_builder_61(
      path_to_float_field_);
  encryption_col_builder_60.key(kColumnEncryptionKey1_)->key_id("kc1");
  encryption_col_builder_61.key(kColumnEncryptionKey2_)->key_id("kc2");

  encryption_cols6[path_to_double_field_] = encryption_col_builder_60.build();
  encryption_cols6[path_to_float_field_] = encryption_col_builder_61.build();
  parquet::FileEncryptionProperties::Builder file_encryption_builder_6(
      kFooterEncryptionKey_);

  EXPECT_NO_THROW(
      this->EncryptFile(file_encryption_builder_6.footer_key_metadata("kf")
                            ->encrypted_columns(encryption_cols6)
                            ->algorithm(parquet::ParquetCipher::AES_GCM_CTR_V1)
                            ->build(),
                        "tmp_encrypt_columns_and_footer_ctr.parquet.encrypted"));
}

// Set temp_dir before running the write/read tests. The encrypted files will
// be written/read from this directory.
void TestEncryptionConfiguration::SetUpTestCase() {
  temp_dir = temp_data_dir().ValueOrDie();
}

}  // namespace test
}  // namespace encryption
}  // namespace parquet

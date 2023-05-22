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

#include <fstream>

#include "arrow/io/file.h"
#include "arrow/testing/gtest_compat.h"

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/file_reader.h"
#include "parquet/test_util.h"

/*
 * This file contains a unit-test for reading encrypted Parquet files with
 * different decryption configurations.
 *
 * The unit-test is called multiple times, each time to decrypt parquet files using
 * different decryption configuration as described below.
 * In each call two encrypted files are read: one temporary file that was generated using
 * encryption-write-configurations-test.cc test and will be deleted upon
 * reading it, while the second resides in
 * parquet-testing/data repository. Those two encrypted files were encrypted using the
 * same encryption configuration.
 * The encrypted parquet file names are passed as parameter to the unit-test.
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * The following decryption configurations are used to decrypt each parquet file:
 *
 *  - Decryption configuration 1:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key.
 *  - Decryption configuration 2:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key. Supplies
 *                                  aad_prefix to verify file identity.
 *  - Decryption configuration 3:   Decrypt using explicit column and footer keys
 *                                  (instead of key retrieval callback).
 *  - Decryption Configuration 4:   PlainText Footer mode - test legacy reads,
 *                                  read the footer + all non-encrypted columns.
 *                                  (pairs with encryption configuration 3)
 *
 * The encrypted parquet files that is read was encrypted using one of the configurations
 * below:
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

using parquet::test::ParquetTestException;

class TestDecryptionConfiguration
    : public testing::TestWithParam<std::tuple<int, const char*>> {
 public:
  void SetUp() {
#ifndef ARROW_WITH_SNAPPY
    GTEST_SKIP() << "Test requires Snappy compression";
#endif
    CreateDecryptionConfigurations();
  }

 protected:
  FileDecryptor decryptor_;
  std::string path_to_double_field_ = kDoubleFieldName;
  std::string path_to_float_field_ = kFloatFieldName;
  // This vector will hold various decryption configurations.
  std::vector<std::shared_ptr<parquet::FileDecryptionProperties>>
      vector_of_decryption_configurations_;
  std::string kFooterEncryptionKey_ = std::string(kFooterEncryptionKey);
  std::string kColumnEncryptionKey1_ = std::string(kColumnEncryptionKey1);
  std::string kColumnEncryptionKey2_ = std::string(kColumnEncryptionKey2);
  std::string kFileName_ = std::string(kFileName);

  void CreateDecryptionConfigurations() {
    /**********************************************************************************
                           Creating a number of Decryption configurations
     **********************************************************************************/

    // Decryption configuration 1: Decrypt using key retriever callback that holds the
    // keys of two encrypted columns and the footer key.
    std::shared_ptr<parquet::StringKeyIdRetriever> string_kr1 =
        std::make_shared<parquet::StringKeyIdRetriever>();
    string_kr1->PutKey("kf", kFooterEncryptionKey_);
    string_kr1->PutKey("kc1", kColumnEncryptionKey1_);
    string_kr1->PutKey("kc2", kColumnEncryptionKey2_);
    std::shared_ptr<parquet::DecryptionKeyRetriever> kr1 =
        std::static_pointer_cast<parquet::StringKeyIdRetriever>(string_kr1);

    parquet::FileDecryptionProperties::Builder file_decryption_builder_1;
    vector_of_decryption_configurations_.push_back(
        file_decryption_builder_1.key_retriever(kr1)->build());

    // Decryption configuration 2: Decrypt using key retriever callback that holds the
    // keys of two encrypted columns and the footer key. Supply aad_prefix.
    std::shared_ptr<parquet::StringKeyIdRetriever> string_kr2 =
        std::make_shared<parquet::StringKeyIdRetriever>();
    string_kr2->PutKey("kf", kFooterEncryptionKey_);
    string_kr2->PutKey("kc1", kColumnEncryptionKey1_);
    string_kr2->PutKey("kc2", kColumnEncryptionKey2_);
    std::shared_ptr<parquet::DecryptionKeyRetriever> kr2 =
        std::static_pointer_cast<parquet::StringKeyIdRetriever>(string_kr2);

    parquet::FileDecryptionProperties::Builder file_decryption_builder_2;
    vector_of_decryption_configurations_.push_back(
        file_decryption_builder_2.key_retriever(kr2)->aad_prefix(kFileName_)->build());

    // Decryption configuration 3: Decrypt using explicit column and footer keys. Supply
    // aad_prefix.
    std::string path_float_ptr = kFloatFieldName;
    std::string path_double_ptr = kDoubleFieldName;
    std::map<std::string, std::shared_ptr<parquet::ColumnDecryptionProperties>>
        decryption_cols;
    parquet::ColumnDecryptionProperties::Builder decryption_col_builder31(
        path_double_ptr);
    parquet::ColumnDecryptionProperties::Builder decryption_col_builder32(path_float_ptr);

    decryption_cols[path_double_ptr] =
        decryption_col_builder31.key(kColumnEncryptionKey1_)->build();
    decryption_cols[path_float_ptr] =
        decryption_col_builder32.key(kColumnEncryptionKey2_)->build();

    parquet::FileDecryptionProperties::Builder file_decryption_builder_3;
    vector_of_decryption_configurations_.push_back(
        file_decryption_builder_3.footer_key(kFooterEncryptionKey_)
            ->column_keys(decryption_cols)
            ->build());

    // Decryption Configuration 4: use plaintext footer mode, read only footer + plaintext
    // columns.
    vector_of_decryption_configurations_.push_back(NULL);
  }

  void DecryptFile(std::string file, int decryption_config_num) {
    std::string exception_msg;
    std::shared_ptr<FileDecryptionProperties> file_decryption_properties;
    // if we get decryption_config_num = x then it means the actual number is x+1
    // and since we want decryption_config_num=4 we set the condition to 3
    if (decryption_config_num != 3) {
      file_decryption_properties =
          vector_of_decryption_configurations_[decryption_config_num]->DeepClone();
    }

    decryptor_.DecryptFile(file, file_decryption_properties);
  }

  // Check that the decryption result is as expected.
  void CheckResults(const std::string file_name, unsigned decryption_config_num,
                    unsigned encryption_config_num) {
    // Encryption_configuration number five contains aad_prefix and
    // disable_aad_prefix_storage.
    // An exception is expected to be thrown if the file is not decrypted with aad_prefix.
    if (encryption_config_num == 5) {
      if (decryption_config_num == 1 || decryption_config_num == 3) {
        EXPECT_THROW(DecryptFile(file_name, decryption_config_num - 1), ParquetException);
        return;
      }
    }
    // Decryption configuration number two contains aad_prefix. An exception is expected
    // to be thrown if the file was not encrypted with the same aad_prefix.
    if (decryption_config_num == 2) {
      if (encryption_config_num != 5 && encryption_config_num != 4) {
        EXPECT_THROW(DecryptFile(file_name, decryption_config_num - 1), ParquetException);
        return;
      }
    }

    // decryption config 4 can only work when the encryption configuration is 3
    if (decryption_config_num == 4 && encryption_config_num != 3) {
      return;
    }
    EXPECT_NO_THROW(DecryptFile(file_name, decryption_config_num - 1));
  }

  // Returns true if file exists. Otherwise returns false.
  bool fexists(const std::string& filename) {
    std::ifstream ifile(filename.c_str());
    return ifile.good();
  }
};

// Read encrypted parquet file.
// The test reads two parquet files that were encrypted using the same encryption
// configuration:
// one was generated in encryption-write-configurations-test.cc tests and is deleted
// once the file is read and the second exists in parquet-testing/data folder.
// The name of the files are passed as parameters to the unit-test.
TEST_P(TestDecryptionConfiguration, TestDecryption) {
  int encryption_config_num = std::get<0>(GetParam());
  const char* param_file_name = std::get<1>(GetParam());
  // Decrypt parquet file that was generated in encryption-write-configurations-test.cc
  // test.
  std::string tmp_file_name = "tmp_" + std::string(param_file_name);
  std::string file_name = temp_dir->path().ToString() + tmp_file_name;
  if (!fexists(file_name)) {
    std::stringstream ss;
    ss << "File " << file_name << " is missing from temporary dir.";
    throw ParquetTestException(ss.str());
  }

  // Iterate over the decryption configurations and use each one to read the encrypted
  // parquet file.
  for (unsigned index = 0; index < vector_of_decryption_configurations_.size(); ++index) {
    unsigned decryption_config_num = index + 1;
    CheckResults(file_name, decryption_config_num, encryption_config_num);
  }
  // Delete temporary test file.
  ASSERT_EQ(std::remove(file_name.c_str()), 0);

  // Decrypt parquet file that resides in parquet-testing/data directory.
  file_name = data_file(param_file_name);

  if (!fexists(file_name)) {
    std::stringstream ss;
    ss << "File " << file_name << " is missing from parquet-testing repo.";
    throw ParquetTestException(ss.str());
  }

  // Iterate over the decryption configurations and use each one to read the encrypted
  // parquet file.
  for (unsigned index = 0; index < vector_of_decryption_configurations_.size(); ++index) {
    unsigned decryption_config_num = index + 1;
    CheckResults(file_name, decryption_config_num, encryption_config_num);
  }
}

INSTANTIATE_TEST_SUITE_P(
    DecryptionTests, TestDecryptionConfiguration,
    ::testing::Values(
        std::make_tuple(1, "uniform_encryption.parquet.encrypted"),
        std::make_tuple(2, "encrypt_columns_and_footer.parquet.encrypted"),
        std::make_tuple(3, "encrypt_columns_plaintext_footer.parquet.encrypted"),
        std::make_tuple(4, "encrypt_columns_and_footer_aad.parquet.encrypted"),
        std::make_tuple(
            5, "encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted"),
        std::make_tuple(6, "encrypt_columns_and_footer_ctr.parquet.encrypted")));

}  // namespace test
}  // namespace encryption
}  // namespace parquet

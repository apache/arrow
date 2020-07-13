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
#include "parquet/file_reader.h"
#include "parquet/test_encryption_util.h"
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
namespace test {

class TestDecryptionConfiguration
    : public testing::TestWithParam<std::tuple<int, const char*>> {
 public:
  void SetUp() { CreateDecryptionConfigurations(); }

 protected:
  std::string path_to_double_field_ = "double_field";
  std::string path_to_float_field_ = "float_field";
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
    std::string path_float_ptr = "float_field";
    std::string path_double_ptr = "double_field";
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
    parquet::ReaderProperties reader_properties = parquet::default_reader_properties();
    // if we get decryption_config_num = x then it means the actual number is x+1
    // and since we want decryption_config_num=4 we set the condition to 3
    if (decryption_config_num != 3) {
      reader_properties.file_decryption_properties(
          vector_of_decryption_configurations_[decryption_config_num]->DeepClone());
    }

    auto file_reader =
        parquet::ParquetFileReader::OpenFile(file, false, reader_properties);

    // Get the File MetaData
    std::shared_ptr<parquet::FileMetaData> file_metadata = file_reader->metadata();

    // Get the number of RowGroups
    int num_row_groups = file_metadata->num_row_groups();

    // Get the number of Columns
    int num_columns = file_metadata->num_columns();
    ASSERT_EQ(num_columns, 8);

    // Iterate over all the RowGroups in the file
    for (int r = 0; r < num_row_groups; ++r) {
      // Get the RowGroup Reader
      std::shared_ptr<parquet::RowGroupReader> row_group_reader =
          file_reader->RowGroup(r);

      // Get the RowGroupMetaData
      std::unique_ptr<RowGroupMetaData> rg_metadata = file_metadata->RowGroup(r);

      int64_t values_read = 0;
      int64_t rows_read = 0;
      int16_t definition_level;
      int16_t repetition_level;
      int i;
      std::shared_ptr<parquet::ColumnReader> column_reader;

      // Get the Column Reader for the boolean column
      column_reader = row_group_reader->Column(0);
      parquet::BoolReader* bool_reader =
          static_cast<parquet::BoolReader*>(column_reader.get());

      // Get the ColumnChunkMetaData for the boolean column
      std::unique_ptr<ColumnChunkMetaData> boolean_md = rg_metadata->ColumnChunk(0);

      // Read all the rows in the column
      i = 0;
      while (bool_reader->HasNext()) {
        bool value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = bool_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        ASSERT_EQ(rows_read, 1);
        // There are no NULL values in the rows written
        ASSERT_EQ(values_read, 1);
        // Verify the value written
        bool expected_value = ((i % 2) == 0) ? true : false;
        ASSERT_EQ(value, expected_value);
        i++;
      }
      // make sure we got the same number of values the metadata says
      ASSERT_EQ(boolean_md->num_values(), i);

      // Get the Column Reader for the Int32 column
      column_reader = row_group_reader->Column(1);
      parquet::Int32Reader* int32_reader =
          static_cast<parquet::Int32Reader*>(column_reader.get());

      // Get the ColumnChunkMetaData for the Int32 column
      std::unique_ptr<ColumnChunkMetaData> int32_md = rg_metadata->ColumnChunk(1);

      // Read all the rows in the column
      i = 0;
      while (int32_reader->HasNext()) {
        int32_t value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = int32_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        ASSERT_EQ(rows_read, 1);
        // There are no NULL values in the rows written
        ASSERT_EQ(values_read, 1);
        // Verify the value written
        ASSERT_EQ(value, i);
        i++;
      }
      // make sure we got the same number of values the metadata says
      ASSERT_EQ(int32_md->num_values(), i);

      // Get the Column Reader for the Int64 column
      column_reader = row_group_reader->Column(2);
      parquet::Int64Reader* int64_reader =
          static_cast<parquet::Int64Reader*>(column_reader.get());

      // Get the ColumnChunkMetaData for the Int64 column
      std::unique_ptr<ColumnChunkMetaData> int64_md = rg_metadata->ColumnChunk(2);

      // Read all the rows in the column
      i = 0;
      while (int64_reader->HasNext()) {
        int64_t value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level,
                                            &value, &values_read);
        // Ensure only one value is read
        ASSERT_EQ(rows_read, 1);
        // There are no NULL values in the rows written
        ASSERT_EQ(values_read, 1);
        // Verify the value written
        int64_t expected_value = i * 1000 * 1000;
        expected_value *= 1000 * 1000;
        ASSERT_EQ(value, expected_value);
        if ((i % 2) == 0) {
          ASSERT_EQ(repetition_level, 1);
        } else {
          ASSERT_EQ(repetition_level, 0);
        }
        i++;
      }
      // make sure we got the same number of values the metadata says
      ASSERT_EQ(int64_md->num_values(), i);

      // Get the Column Reader for the Int96 column
      column_reader = row_group_reader->Column(3);
      parquet::Int96Reader* int96_reader =
          static_cast<parquet::Int96Reader*>(column_reader.get());

      // Get the ColumnChunkMetaData for the Int96 column
      std::unique_ptr<ColumnChunkMetaData> int96_md = rg_metadata->ColumnChunk(3);

      // Read all the rows in the column
      i = 0;
      while (int96_reader->HasNext()) {
        parquet::Int96 value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = int96_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        ASSERT_EQ(rows_read, 1);
        // There are no NULL values in the rows written
        ASSERT_EQ(values_read, 1);
        // Verify the value written
        parquet::Int96 expected_value;
        expected_value.value[0] = i;
        expected_value.value[1] = i + 1;
        expected_value.value[2] = i + 2;
        for (int j = 0; j < 3; j++) {
          ASSERT_EQ(value.value[j], expected_value.value[j]);
        }
        i++;
      }
      // make sure we got the same number of values the metadata says
      ASSERT_EQ(int96_md->num_values(), i);

      if (decryption_config_num != 3) {
        // Get the Column Reader for the Float column
        column_reader = row_group_reader->Column(4);
        parquet::FloatReader* float_reader =
            static_cast<parquet::FloatReader*>(column_reader.get());

        // Get the ColumnChunkMetaData for the Float column
        std::unique_ptr<ColumnChunkMetaData> float_md = rg_metadata->ColumnChunk(4);

        // Read all the rows in the column
        i = 0;
        while (float_reader->HasNext()) {
          float value;
          // Read one value at a time. The number of rows read is returned. values_read
          // contains the number of non-null rows
          rows_read = float_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          // Ensure only one value is read
          ASSERT_EQ(rows_read, 1);
          // There are no NULL values in the rows written
          ASSERT_EQ(values_read, 1);
          // Verify the value written
          float expected_value = static_cast<float>(i) * 1.1f;
          ASSERT_EQ(value, expected_value);
          i++;
        }
        // make sure we got the same number of values the metadata says
        ASSERT_EQ(float_md->num_values(), i);

        // Get the Column Reader for the Double column
        column_reader = row_group_reader->Column(5);
        parquet::DoubleReader* double_reader =
            static_cast<parquet::DoubleReader*>(column_reader.get());

        // Get the ColumnChunkMetaData for the Double column
        std::unique_ptr<ColumnChunkMetaData> double_md = rg_metadata->ColumnChunk(5);

        // Read all the rows in the column
        i = 0;
        while (double_reader->HasNext()) {
          double value;
          // Read one value at a time. The number of rows read is returned. values_read
          // contains the number of non-null rows
          rows_read = double_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          // Ensure only one value is read
          ASSERT_EQ(rows_read, 1);
          // There are no NULL values in the rows written
          ASSERT_EQ(values_read, 1);
          // Verify the value written
          double expected_value = i * 1.1111111;
          ASSERT_EQ(value, expected_value);
          i++;
        }
        // make sure we got the same number of values the metadata says
        ASSERT_EQ(double_md->num_values(), i);
      }

      // Get the Column Reader for the ByteArray column
      column_reader = row_group_reader->Column(6);
      parquet::ByteArrayReader* ba_reader =
          static_cast<parquet::ByteArrayReader*>(column_reader.get());

      // Get the ColumnChunkMetaData for the ByteArray column
      std::unique_ptr<ColumnChunkMetaData> ba_md = rg_metadata->ColumnChunk(6);

      // Read all the rows in the column
      i = 0;
      while (ba_reader->HasNext()) {
        parquet::ByteArray value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read =
            ba_reader->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
        // Ensure only one value is read
        ASSERT_EQ(rows_read, 1);
        // Verify the value written
        char expected_value[kFixedLength] = "parquet";
        expected_value[7] = static_cast<char>('0' + i / 100);
        expected_value[8] = static_cast<char>('0' + (i / 10) % 10);
        expected_value[9] = static_cast<char>('0' + i % 10);
        if (i % 2 == 0) {  // only alternate values exist
          // There are no NULL values in the rows written
          ASSERT_EQ(values_read, 1);
          ASSERT_EQ(value.len, kFixedLength);
          ASSERT_EQ(memcmp(value.ptr, &expected_value[0], kFixedLength), 0);
          ASSERT_EQ(definition_level, 1);
        } else {
          // There are NULL values in the rows written
          ASSERT_EQ(values_read, 0);
          ASSERT_EQ(definition_level, 0);
        }
        i++;
      }
      // make sure we got the same number of values the metadata says
      ASSERT_EQ(ba_md->num_values(), i);

      // Get the Column Reader for the FixedLengthByteArray column
      column_reader = row_group_reader->Column(7);
      parquet::FixedLenByteArrayReader* flba_reader =
          static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());

      // Get the ColumnChunkMetaData for the FixedLengthByteArray column
      std::unique_ptr<ColumnChunkMetaData> flba_md = rg_metadata->ColumnChunk(7);

      // Read all the rows in the column
      i = 0;
      while (flba_reader->HasNext()) {
        parquet::FixedLenByteArray value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = flba_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        ASSERT_EQ(rows_read, 1);
        // There are no NULL values in the rows written
        ASSERT_EQ(values_read, 1);
        // Verify the value written
        char v = static_cast<char>(i);
        char expected_value[kFixedLength] = {v, v, v, v, v, v, v, v, v, v};
        ASSERT_EQ(memcmp(value.ptr, &expected_value[0], kFixedLength), 0);
        i++;
      }
      // make sure we got the same number of values the metadata says
      ASSERT_EQ(flba_md->num_values(), i);
    }
    file_reader->Close();
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
  std::string file_name = data_file(tmp_file_name.c_str());
  if (!fexists(file_name)) {
    std::stringstream ss;
    ss << "File " << file_name << " is missing from parquet-testing repo.";
    throw ParquetTestException(ss.str());
  }

  // Iterate over the decryption configurations and use each one to read the encrypted
  // parqeut file.
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
  // parqeut file.
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
}  // namespace parquet

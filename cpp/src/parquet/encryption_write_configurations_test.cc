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
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/test_encryption_util.h"
#include "parquet/test_util.h"

/*
 * This file contains unit-tests for writing encrypted Parquet files with
 * different encryption configurations.
 * The files are saved in parquet-testing/data folder and will be deleted after reading
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
namespace test {

using FileClass = ::arrow::io::FileOutputStream;

class TestEncryptionConfiguration : public ::testing::Test {
 public:
  void SetUp() {
    // Setup the parquet schema
    schema_ = SetupEncryptionSchema();
  }

 protected:
  std::string path_to_double_field_ = "double_field";
  std::string path_to_float_field_ = "float_field";
  std::string file_name_;
  int num_rgs = 5;
  int rows_per_rowgroup_ = 50;
  std::shared_ptr<GroupNode> schema_;
  std::string kFooterEncryptionKey_ = std::string(kFooterEncryptionKey);
  std::string kColumnEncryptionKey1_ = std::string(kColumnEncryptionKey1);
  std::string kColumnEncryptionKey2_ = std::string(kColumnEncryptionKey2);
  std::string kFileName_ = std::string(kFileName);

  void EncryptFile(
      std::shared_ptr<parquet::FileEncryptionProperties> encryption_configurations,
      std::string file_name) {
    std::string file = data_file(file_name.c_str());

    WriterProperties::Builder prop_builder;
    prop_builder.compression(parquet::Compression::SNAPPY);
    prop_builder.encryption(encryption_configurations);
    std::shared_ptr<WriterProperties> writer_properties = prop_builder.build();

    PARQUET_ASSIGN_OR_THROW(auto out_file, FileClass::Open(file));
    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema_, writer_properties);

    for (int r = 0; r < num_rgs; r++) {
      bool buffered_mode = r % 2 == 0;
      auto row_group_writer = buffered_mode ? file_writer->AppendBufferedRowGroup()
                                            : file_writer->AppendRowGroup();

      int column_index = 0;
      // Captures i by reference; increments it by one
      auto get_next_column = [&]() {
        return buffered_mode ? row_group_writer->column(column_index++)
                             : row_group_writer->NextColumn();
      };

      // Write the Bool column
      parquet::BoolWriter* bool_writer =
          static_cast<parquet::BoolWriter*>(get_next_column());
      for (int i = 0; i < rows_per_rowgroup_; i++) {
        bool value = ((i % 2) == 0) ? true : false;
        bool_writer->WriteBatch(1, nullptr, nullptr, &value);
      }

      // Write the Int32 column
      parquet::Int32Writer* int32_writer =
          static_cast<parquet::Int32Writer*>(get_next_column());
      for (int i = 0; i < rows_per_rowgroup_; i++) {
        int32_t value = i;
        int32_writer->WriteBatch(1, nullptr, nullptr, &value);
      }

      // Write the Int64 column. Each row has repeats twice.
      parquet::Int64Writer* int64_writer =
          static_cast<parquet::Int64Writer*>(get_next_column());
      for (int i = 0; i < 2 * rows_per_rowgroup_; i++) {
        int64_t value = i * 1000 * 1000;
        value *= 1000 * 1000;
        int16_t definition_level = 1;
        int16_t repetition_level = 0;
        if ((i % 2) == 0) {
          repetition_level = 1;  // start of a new record
        }
        int64_writer->WriteBatch(1, &definition_level, &repetition_level, &value);
      }

      // Write the INT96 column.
      parquet::Int96Writer* int96_writer =
          static_cast<parquet::Int96Writer*>(get_next_column());
      for (int i = 0; i < rows_per_rowgroup_; i++) {
        parquet::Int96 value;
        value.value[0] = i;
        value.value[1] = i + 1;
        value.value[2] = i + 2;
        int96_writer->WriteBatch(1, nullptr, nullptr, &value);
      }

      // Write the Float column
      parquet::FloatWriter* float_writer =
          static_cast<parquet::FloatWriter*>(get_next_column());
      for (int i = 0; i < rows_per_rowgroup_; i++) {
        float value = static_cast<float>(i) * 1.1f;
        float_writer->WriteBatch(1, nullptr, nullptr, &value);
      }

      // Write the Double column
      parquet::DoubleWriter* double_writer =
          static_cast<parquet::DoubleWriter*>(get_next_column());
      for (int i = 0; i < rows_per_rowgroup_; i++) {
        double value = i * 1.1111111;
        double_writer->WriteBatch(1, nullptr, nullptr, &value);
      }

      // Write the ByteArray column. Make every alternate values NULL
      parquet::ByteArrayWriter* ba_writer =
          static_cast<parquet::ByteArrayWriter*>(get_next_column());
      for (int i = 0; i < rows_per_rowgroup_; i++) {
        parquet::ByteArray value;
        char hello[kFixedLength] = "parquet";
        hello[7] = static_cast<char>(static_cast<int>('0') + i / 100);
        hello[8] = static_cast<char>(static_cast<int>('0') + (i / 10) % 10);
        hello[9] = static_cast<char>(static_cast<int>('0') + i % 10);
        if (i % 2 == 0) {
          int16_t definition_level = 1;
          value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
          value.len = kFixedLength;
          ba_writer->WriteBatch(1, &definition_level, nullptr, &value);
        } else {
          int16_t definition_level = 0;
          ba_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        }
      }

      // Write the FixedLengthByteArray column
      parquet::FixedLenByteArrayWriter* flba_writer =
          static_cast<parquet::FixedLenByteArrayWriter*>(get_next_column());
      for (int i = 0; i < rows_per_rowgroup_; i++) {
        parquet::FixedLenByteArray value;
        char v = static_cast<char>(i);
        char flba[kFixedLength] = {v, v, v, v, v, v, v, v, v, v};
        value.ptr = reinterpret_cast<const uint8_t*>(&flba[0]);
        flba_writer->WriteBatch(1, nullptr, nullptr, &value);
      }
    }

    // Close the ParquetFileWriter
    file_writer->Close();

    return;
  }

  std::shared_ptr<GroupNode> SetupEncryptionSchema() {
    parquet::schema::NodeVector fields;
    // Create a primitive node named 'boolean_field' with type:BOOLEAN,
    // repetition:REQUIRED
    fields.push_back(PrimitiveNode::Make("boolean_field", Repetition::REQUIRED,
                                         Type::BOOLEAN, ConvertedType::NONE));

    // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED,
    // logical type:TIME_MILLIS
    fields.push_back(PrimitiveNode::Make("int32_field", Repetition::REQUIRED, Type::INT32,
                                         ConvertedType::TIME_MILLIS));

    // Create a primitive node named 'int64_field' with type:INT64, repetition:REPEATED
    fields.push_back(PrimitiveNode::Make("int64_field", Repetition::REPEATED, Type::INT64,
                                         ConvertedType::NONE));

    fields.push_back(PrimitiveNode::Make("int96_field", Repetition::REQUIRED, Type::INT96,
                                         ConvertedType::NONE));

    fields.push_back(PrimitiveNode::Make("float_field", Repetition::REQUIRED, Type::FLOAT,
                                         ConvertedType::NONE));

    fields.push_back(PrimitiveNode::Make("double_field", Repetition::REQUIRED,
                                         Type::DOUBLE, ConvertedType::NONE));

    // Create a primitive node named 'ba_field' with type:BYTE_ARRAY, repetition:OPTIONAL
    fields.push_back(PrimitiveNode::Make("ba_field", Repetition::OPTIONAL,
                                         Type::BYTE_ARRAY, ConvertedType::NONE));

    // Create a primitive node named 'flba_field' with type:FIXED_LEN_BYTE_ARRAY,
    // repetition:REQUIRED, field_length = kFixedLength
    fields.push_back(PrimitiveNode::Make("flba_field", Repetition::REQUIRED,
                                         Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE,
                                         kFixedLength));

    // Create a GroupNode named 'schema' using the primitive nodes defined above
    // This GroupNode is the root node of the schema tree
    return std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED, fields));
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

}  // namespace test
}  // namespace parquet

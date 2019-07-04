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
#include "parquet/test-util.h"

/*
 * This file contains unit-test for writing and reading encrypted Parquet file with
 * different encryption and decryption configuration.
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * The unit-test creates a single parquet file with eight columns using the
 * following encryption configuration:
 *
 *  - Encryption configuration :   Encrypt two columns, with different keys.
 *                                 Don’t encrypt footer (to enable legacy readers)
 *                                 - plaintext footer mode.
 *
 * The written parquet file produced above is read by each of the following decryption
 * configurations:
 *
 *  - Decryption configuration :   Decrypt using key retriever that holds the keys of
 *                                 two encrypted columns and the footer key.
 */

namespace parquet {
namespace test {
std::string data_file(const char* file) {
  std::string dir_string(test::get_data_dir());
  std::stringstream ss;
  ss << dir_string << "/" << file;
  return ss.str();
}

using FileClass = ::arrow::io::FileOutputStream;

using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

constexpr int kFixedLength = 10;

const char kFooterEncryptionKey[] = "0123456789012345";  // 128bit/16
const char kColumnEncryptionKey1[] = "1234567890123450";
const char kColumnEncryptionKey2[] = "1234567890123451";
const char kFileName[] = "tester";

class TestEncryptionConfiguration : public ::testing::Test {
 public:
  void SetUp() {
    createDecryptionConfigurations();
    // Setup the parquet schema
    schema_ = SetupEncryptionSchema();
    std::string res = "test.parquet.encrypted";
    file_name_ = data_file(res.c_str());
  }

  void TearDown() {
    // delete test file.
    ASSERT_EQ(std::remove(file_name_.c_str()), 0);
  }

 protected:
  std::shared_ptr<parquet::schema::ColumnPath> path_to_double_field_ =
      parquet::schema::ColumnPath::FromDotString("double_field");
  std::shared_ptr<parquet::schema::ColumnPath> path_to_float_field_ =
      parquet::schema::ColumnPath::FromDotString("float_field");
  std::string file_name_;
  int rows_per_rowgroup_ = 50;
  std::shared_ptr<GroupNode> schema_;
  // This vector will hold various decryption configurations.
  std::vector<std::shared_ptr<parquet::FileDecryptionProperties>>
      vector_of_decryption_configurations_;
  std::string kFooterEncryptionKey_ = std::string(kFooterEncryptionKey);
  std::string kColumnEncryptionKey1_ = std::string(kColumnEncryptionKey1);
  std::string kColumnEncryptionKey2_ = std::string(kColumnEncryptionKey2);
  std::string kFileName_ = std::string(kFileName);

  void createDecryptionConfigurations() {
    /**********************************************************************************
                           Creating Decryption configuration
     **********************************************************************************/

    // Decryption configuration: Decrypt using key retriever callback that holds the
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

  }

  void EncryptFile(
      std::shared_ptr<parquet::FileEncryptionProperties> encryption_configurations,
      std::string file) {
    std::shared_ptr<FileClass> out_file;

    WriterProperties::Builder prop_builder;
    prop_builder.compression(parquet::Compression::SNAPPY);
    prop_builder.encryption(encryption_configurations);
    std::shared_ptr<WriterProperties> writer_properties = prop_builder.build();

    PARQUET_THROW_NOT_OK(FileClass::Open(file, &out_file));
    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema_, writer_properties);

    RowGroupWriter* row_group_writer;
    row_group_writer = file_writer->AppendRowGroup();

    // Write the Bool column
    parquet::BoolWriter* bool_writer =
        static_cast<parquet::BoolWriter*>(row_group_writer->NextColumn());
    for (int i = 0; i < rows_per_rowgroup_; i++) {
      bool value = ((i % 2) == 0) ? true : false;
      bool_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Int32 column
    parquet::Int32Writer* int32_writer =
        static_cast<parquet::Int32Writer*>(row_group_writer->NextColumn());
    for (int i = 0; i < rows_per_rowgroup_; i++) {
      int32_t value = i;
      int32_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Int64 column. Each row has repeats twice.
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(row_group_writer->NextColumn());
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
        static_cast<parquet::Int96Writer*>(row_group_writer->NextColumn());
    for (int i = 0; i < rows_per_rowgroup_; i++) {
      parquet::Int96 value;
      value.value[0] = i;
      value.value[1] = i + 1;
      value.value[2] = i + 2;
      int96_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Float column
    parquet::FloatWriter* float_writer =
        static_cast<parquet::FloatWriter*>(row_group_writer->NextColumn());
    for (int i = 0; i < rows_per_rowgroup_; i++) {
      float value = static_cast<float>(i) * 1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Double column
    parquet::DoubleWriter* double_writer =
        static_cast<parquet::DoubleWriter*>(row_group_writer->NextColumn());
    for (int i = 0; i < rows_per_rowgroup_; i++) {
      double value = i * 1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the ByteArray column. Make every alternate values NULL
    parquet::ByteArrayWriter* ba_writer =
        static_cast<parquet::ByteArrayWriter*>(row_group_writer->NextColumn());
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
        static_cast<parquet::FixedLenByteArrayWriter*>(row_group_writer->NextColumn());
    for (int i = 0; i < rows_per_rowgroup_; i++) {
      parquet::FixedLenByteArray value;
      char v = static_cast<char>(i);
      char flba[kFixedLength] = {v, v, v, v, v, v, v, v, v, v};
      value.ptr = reinterpret_cast<const uint8_t*>(&flba[0]);

      flba_writer->WriteBatch(1, nullptr, nullptr, &value);
    }
    // Close the ParquetFileWriter
    file_writer->Close();

    return;
  }

  void DecryptFile(std::string file, int example_id, int encryption_configuration) {
    std::string exception_msg;
    try {
      parquet::ReaderProperties reader_properties = parquet::default_reader_properties();
      reader_properties.file_decryption_properties(
          vector_of_decryption_configurations_[example_id]->DeepClone());

      auto file_reader =
          parquet::ParquetFileReader::OpenFile(file, false, reader_properties);

      // Get the File MetaData
      std::shared_ptr<parquet::FileMetaData> file_metadata = file_reader->metadata();

      // Get the number of RowGroups
      int num_row_groups = file_metadata->num_row_groups();
      ASSERT_EQ(num_row_groups, 1);

      // Get the number of Columns
      int num_columns = file_metadata->num_columns();
      ASSERT_EQ(num_columns, 8);

      // Iterate over all the RowGroups in the file
      for (int r = 0; r < num_row_groups; ++r) {
        // Get the RowGroup Reader
        std::shared_ptr<parquet::RowGroupReader> row_group_reader =
            file_reader->RowGroup(r);

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
        // Get the Column Reader for the Int32 column
        column_reader = row_group_reader->Column(1);
        parquet::Int32Reader* int32_reader =
            static_cast<parquet::Int32Reader*>(column_reader.get());
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
        // Get the Column Reader for the Int64 column
        column_reader = row_group_reader->Column(2);
        parquet::Int64Reader* int64_reader =
            static_cast<parquet::Int64Reader*>(column_reader.get());
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

        // Get the Column Reader for the Int96 column
        column_reader = row_group_reader->Column(3);
        parquet::Int96Reader* int96_reader =
            static_cast<parquet::Int96Reader*>(column_reader.get());
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

        // Get the Column Reader for the Float column
        column_reader = row_group_reader->Column(4);
        parquet::FloatReader* float_reader =
            static_cast<parquet::FloatReader*>(column_reader.get());
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
        // Get the Column Reader for the Double column
        column_reader = row_group_reader->Column(5);
        parquet::DoubleReader* double_reader =
            static_cast<parquet::DoubleReader*>(column_reader.get());
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
        // Get the Column Reader for the ByteArray column
        column_reader = row_group_reader->Column(6);
        parquet::ByteArrayReader* ba_reader =
            static_cast<parquet::ByteArrayReader*>(column_reader.get());
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
        // Get the Column Reader for the FixedLengthByteArray column
        column_reader = row_group_reader->Column(7);
        parquet::FixedLenByteArrayReader* flba_reader =
            static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
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
        file_reader->Close();
      }
    } catch (const std::exception& e) {
      exception_msg = e.what();
    }
    CheckResult(encryption_configuration, example_id, exception_msg);
  }

  // Check that the decryption result is as expected.
  void CheckResult(int encryption_configuration_number, int example_id,
                   std::string exception_msg) {
    if (!exception_msg.empty()) {
      ASSERT_EQ(1, 0);
    }
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

// Encryption configuration: Encrypt two columns, with different keys.
// Don’t encrypt footer.
// (plaintext footer mode, readable by legacy readers)
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsWithPlaintextFooter) {
  std::map<std::shared_ptr<parquet::schema::ColumnPath>,
           std::shared_ptr<parquet::ColumnEncryptionProperties>,
           parquet::schema::ColumnPath::CmpColumnPath>
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
                        ->column_properties(encryption_cols3)
                        ->set_plaintext_footer()
                        ->build(),
                    file_name_);

  // Iterate over the decryption configurations and use each one to read the encrypted
  // parqeut file.
  for (unsigned example_id = 0; example_id < vector_of_decryption_configurations_.size();
       ++example_id) {
    DecryptFile(file_name_, example_id, 3 /* encryption_configuration_number */);
  }
}

}  // namespace test
}  // namespace parquet

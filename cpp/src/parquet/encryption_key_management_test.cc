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

#include <iostream>
#include <string>

#include <arrow/io/file.h>
#include "arrow/testing/util.h"

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encoding.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/in_memory_kms.h"
#include "parquet/key_toolkit.h"
#include "parquet/platform.h"
#include "parquet/properties_driven_crypto_factory.h"
#include "parquet/schema.h"
#include "parquet/test_encryption_util.h"
#include "parquet/test_util.h"

using namespace parquet::encryption;

namespace parquet {

namespace test {

using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

using FileClass = ::arrow::io::FileOutputStream;

constexpr int COLUMN_KEY_SIZE = 6;

const char FOOTER_MASTER_KEY[] = "0123456789112345";
const char* const COLUMN_MASTER_KEYS[] = {"1234567890123450", "1234567890123451",
                                          "1234567890123452", "1234567890123453",
                                          "1234567890123454", "1234567890123455"};
const char* const COLUMN_MASTER_KEY_IDS[] = {"kc1", "kc2", "kc3", "kc4", "kc5", "kc6"};
const char FOOTER_MASTER_KEY_ID[] = "kf";

const char NEW_FOOTER_MASTER_KEY[] = "9123456789012345";
const char* const NEW_COLUMN_MASTER_KEYS[] = {"9234567890123450", "9234567890123451",
                                              "9234567890123452", "9234567890123453",
                                              "9234567890123454", "9234567890123455"};

std::string BuildKeyList(const char* const* ids, const char* const* keys, int size) {
  std::ostringstream stream;
  for (int i = 0; i < size; i++) {
    stream << ids[i] << ":" << keys[i];
    if (i < size - 1) {
      stream << ",";
    }
  }
  return stream.str();
}

std::vector<std::string> BuildKeyList2(const char* const* column_ids,
                                       const char* const* column_keys, int size,
                                       const char* footer_id, const char* footer_key) {
  std::vector<std::string> key_list;
  for (int i = 0; i < size; i++) {
    std::ostringstream stream;
    stream << column_ids[i] << ":" << column_keys[i];
    key_list.push_back(stream.str());
  }
  std::ostringstream stream2;
  stream2 << footer_id << ":" << footer_key;
  key_list.push_back(stream2.str());
  return key_list;
}

const std::string KEY_LIST =
    BuildKeyList(COLUMN_MASTER_KEY_IDS, COLUMN_MASTER_KEYS, COLUMN_KEY_SIZE);

const std::vector<std::string> KEY_LIST2 =
    BuildKeyList2(COLUMN_MASTER_KEY_IDS, COLUMN_MASTER_KEYS, COLUMN_KEY_SIZE,
                  FOOTER_MASTER_KEY_ID, FOOTER_MASTER_KEY);

const std::string NEW_KEY_LIST =
    BuildKeyList(COLUMN_MASTER_KEY_IDS, NEW_COLUMN_MASTER_KEYS, COLUMN_KEY_SIZE);

std::string BuildColumnKeyMapping() {
  std::ostringstream stream;
  stream << COLUMN_MASTER_KEY_IDS[0] << ":"
         << "double_field"
         << ";" << COLUMN_MASTER_KEY_IDS[1] << ":"
         << "float_field"
         << ";" << COLUMN_MASTER_KEY_IDS[2] << ":"
         << "boolean_field"
         << ";" << COLUMN_MASTER_KEY_IDS[3] << ":"
         << "int32_field"
         << ";" << COLUMN_MASTER_KEY_IDS[4] << ":"
         << "ba_field"
         << ";" << COLUMN_MASTER_KEY_IDS[5] << ":"
         << "flba_field"
         << ";";
  return stream.str();
}

const std::string COLUMN_KEY_MAPPING = BuildColumnKeyMapping();

class EncryptionCrypto {
 public:
  EncryptionCrypto() { schema_ = SetupEncryptionSchema(); }
  int num_rgs = 5;
  int rows_per_rowgroup_ = 50;
  std::shared_ptr<GroupNode> schema_;

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

  void DecryptFile(std::string file_name,
                   std::shared_ptr<FileDecryptionProperties> file_decryption_properties) {
    std::string file = data_file(file_name.c_str());
    std::string exception_msg;
    parquet::ReaderProperties reader_properties = parquet::default_reader_properties();
    reader_properties.file_decryption_properties(file_decryption_properties);

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

      // if (decryption_config_num != 3) {
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
      // }

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
};

class TestEncrytionKeyManagement : public ::testing::Test {
 protected:
  EncryptionCrypto encryption_crypto_;

  void SetupCryptoFactory(PropertiesDrivenCryptoFactory& crypto_factory) {
    std::shared_ptr<KmsClientFactory> kms_client_factory =
        std::make_shared<InMemoryKmsClientFactory>(KEY_LIST2);
    crypto_factory.kms_client_factory(kms_client_factory);
  }

  std::vector<std::shared_ptr<EncryptionConfiguration>> GetEncryptionConfigurations(
      bool double_wrapping, bool wrap_locally) {
    std::vector<std::shared_ptr<EncryptionConfiguration>> configs;
    std::vector<EncryptionConfiguration::Builder*> config_builders;

    // encrypt some columns and footer, different keys
    EncryptionConfiguration::Builder builder1(FOOTER_MASTER_KEY_ID);
    builder1.column_keys(COLUMN_KEY_MAPPING);
    config_builders.push_back(&builder1);

    // encrypt columns, plaintext footer, different keys
    EncryptionConfiguration::Builder builder2(FOOTER_MASTER_KEY_ID);
    builder2.column_keys(COLUMN_KEY_MAPPING)->plaintext_footer(true);
    config_builders.push_back(&builder2);

    // encrypt some columns and footer, same key
    EncryptionConfiguration::Builder builder3(FOOTER_MASTER_KEY_ID);
    builder3.uniform_encryption();
    config_builders.push_back(&builder3);

    // Encrypt two columns and the footer, with different keys.
    // Use AES_GCM_CTR_V1 algorithm.
    EncryptionConfiguration::Builder builder4(FOOTER_MASTER_KEY_ID);
    builder4.column_keys(COLUMN_KEY_MAPPING)
        ->encryption_algorithm(ParquetCipher::AES_GCM_CTR_V1);
    config_builders.push_back(&builder4);

    for (EncryptionConfiguration::Builder* builder : config_builders) {
      auto config =
          builder->double_wrapping(double_wrapping)->wrap_locally(wrap_locally)->build();
      configs.push_back(config);
    }

    // non encryption
    configs.push_back(NULL);

    return configs;
  }

  std::shared_ptr<DecryptionConfiguration> GetDecryptionConfiguration(bool wrap_locally) {
    DecryptionConfiguration::Builder builder;
    return builder.wrap_locally(wrap_locally)->build();
  }

  void WriteEncryptedParquetFile(
      const KmsConnectionConfig& kms_connection_config,
      std::shared_ptr<EncryptionConfiguration> encryption_config,
      const std::string& file_name) {
    PropertiesDrivenCryptoFactory crypto_factory;

    SetupCryptoFactory(crypto_factory);

    std::shared_ptr<FileEncryptionProperties> file_encryption_properties =
        crypto_factory.GetFileEncryptionProperties(kms_connection_config,
                                                   encryption_config);
    encryption_crypto_.EncryptFile(file_encryption_properties, file_name);
  }

  void ReadEncryptedParquetFile(
      const KmsConnectionConfig& kms_connection_config,
      std::shared_ptr<DecryptionConfiguration> decryption_config,
      const std::string& file_name) {
    PropertiesDrivenCryptoFactory crypto_factory;

    std::shared_ptr<KmsClientFactory> kms_client_factory =
        std::make_shared<InMemoryKmsClientFactory>(KEY_LIST2);
    crypto_factory.kms_client_factory(kms_client_factory);

    std::shared_ptr<FileDecryptionProperties> file_decryption_properties =
        crypto_factory.GetFileDecryptionProperties(kms_connection_config,
                                                   decryption_config);

    encryption_crypto_.DecryptFile(file_name, file_decryption_properties);
  }

  std::string GetFileName(std::shared_ptr<EncryptionConfiguration> encryption_config) {
    std::string suffix = encryption_config == NULL ? ".parquet" : ".parquet.encrypted";
    return "demo" + suffix;  // TODO
  }
};

TEST_F(TestEncrytionKeyManagement, TestWriteReadEncryptedParquetFiles) {
  const std::vector<bool> bool_flags = {true, false};

  KmsConnectionConfig kms_connection_config;
  kms_connection_config.refreshable_key_access_token = std::make_shared<KeyAccessToken>();
  kms_connection_config.refreshable_key_access_token->Refresh(
      KmsClient::KEY_ACCESS_TOKEN_DEFAULT);

  KeyToolkit::RemoveCacheEntriesForAllTokens();
  for (int i = 0; i < 2; i++) {
    bool wrap_locally = (i == 0);
    auto decryption_config = this->GetDecryptionConfiguration(wrap_locally);
    for (int j = 0; j < 2; j++) {
      bool double_wrapping = (j == 0);
      auto encryption_configs =
          this->GetEncryptionConfigurations(double_wrapping, wrap_locally);
      for (auto encryption_config : encryption_configs) {
        std::cout << "double_wrapping:" << double_wrapping
                  << " wrap_locally:" << wrap_locally << std::endl;
        std::string file_name = GetFileName(encryption_config);
        this->WriteEncryptedParquetFile(kms_connection_config, encryption_config,
                                        file_name);
        this->ReadEncryptedParquetFile(kms_connection_config, decryption_config,
                                       file_name);
      }
    }
  }
}

}  // namespace test

}  // namespace parquet

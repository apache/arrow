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
#include "arrow/testing/util.h"

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"

#include <string>
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encoding.h"
#include "parquet/platform.h"

#include "parquet/in_memory_kms.h"
#include "parquet/key_toolkit.h"
#include "parquet/properties_driven_crypto_factory.h"
#include "parquet/test_encryption_util.h"

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

const char FOOTER_MASTER_KEY[] = "0123456789012345";
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

std::vector<std::string> BuildKeyList2(const char* const* ids, const char* const* keys,
                                       int size) {
  std::vector<std::string> key_list;
  for (int i = 0; i < size; i++) {
    std::ostringstream stream;
    stream << ids[i] << ":" << keys[i];
    key_list.push_back(stream.str());
  }
  return key_list;
}

const std::string KEY_LIST =
    BuildKeyList(COLUMN_MASTER_KEY_IDS, COLUMN_MASTER_KEYS, COLUMN_KEY_SIZE);

const std::vector<std::string> KEY_LIST2 =
    BuildKeyList2(COLUMN_MASTER_KEY_IDS, COLUMN_MASTER_KEYS, COLUMN_KEY_SIZE);

const std::string NEW_KEY_LIST =
    BuildKeyList(COLUMN_MASTER_KEY_IDS, NEW_COLUMN_MASTER_KEYS, COLUMN_KEY_SIZE);

std::string BuildColumnKeyMapping() {
  std::ostringstream stream;
  for (int i = 0; i < COLUMN_KEY_SIZE; i++) {
    stream << COLUMN_MASTER_KEY_IDS[i] << ":"
           << "SingleRow.DOUBLE_FIELD_NAME";  // TODO
    if (i < COLUMN_KEY_SIZE - 1) {
      stream << ",";
    }
  }
  return stream.str();
}

// private static final String COLUMN_KEY_MAPPING = new StringBuilder()
//   << COLUMN_MASTER_KEY_IDS[0] << ": " << SingleRow.DOUBLE_FIELD_NAME << "; ")
//   << COLUMN_MASTER_KEY_IDS[1] << ": " << SingleRow.FLOAT_FIELD_NAME << "; ")
//   << COLUMN_MASTER_KEY_IDS[2] << ": " << SingleRow.BOOLEAN_FIELD_NAME << "; ")
//   << COLUMN_MASTER_KEY_IDS[3] << ": " << SingleRow.INT32_FIELD_NAME << "; ")
//   << COLUMN_MASTER_KEY_IDS[4] << ": " << SingleRow.BINARY_FIELD_NAME << "; ")
//   << COLUMN_MASTER_KEY_IDS[5] << ": " << SingleRow.FIXED_LENGTH_BINARY_FIELD_NAME)
//   .toString();

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

  void WriteEncryptedParquetFile(
      const KmsConnectionConfig& kms_connection_config,
      std::shared_ptr<EncryptionConfiguration> encryption_config,
      const std::string& file_name) {
    std::shared_ptr<FileEncryptionProperties> file_encryption_properties = NULL;
    try {
      std::shared_ptr<KmsClientFactory> kms_client_factory =
          std::make_shared<InMemoryKmsClientFactory>(KEY_LIST2);
      PropertiesDrivenCryptoFactory crypto_factory;
      crypto_factory.kms_client_factory(kms_client_factory);
      file_encryption_properties = crypto_factory.GetFileEncryptionProperties(
          kms_connection_config, encryption_config);
      this->EncryptFile(file_encryption_properties, file_name);

    } catch (const std::exception& e) {
      std::cout << "Failed writing " << file_name << ": " << e.what();  // TODO
      // addErrorToErrorCollectorAndLog("Failed writing " + file.toString(), e,
      //                                encryptionConfiguration, null);
    }
  }
};

TEST_F(TestEncryptionConfiguration, TestWriteReadEncryptedParquetFiles) {
  // Path rootPath = new Path(temporaryFolder.getRoot().getPath());
  // std::cout << "======== TestWriteReadEncryptedParquetFiles {} ========",
  // rootPath.toString()); LOG.info("Run: isKeyMaterialInternalStorage={}
  // isDoubleWrapping={} isWrapLocally={}", isKeyMaterialInternalStorage,
  // isDoubleWrapping, isWrapLocally);
  EncryptionConfiguration::Builder builder("footer_key");
  builder.uniform_encryption();

  KeyToolkit::RemoveCacheEntriesForAllTokens();
  this->WriteEncryptedParquetFile(KmsConnectionConfig(), builder.build(),
                                  "demo.parquet.encrypted");

  // Write using various encryption configurations.
  // TestWriteEncryptedParquetFiles(root_path);
  // Read using various decryption configurations.
  // TestReadEncryptedParquetFiles(rootPath, DATA, threadPool);

  // TODO: REMOVE
  // PropertiesDrivenCryptoFactory crypto_factory;
  //  crypto_factory.GetFileEncryptionProperties(
  //      kms_connection_config,
  //      encryption_config,
  //      hdfs_connection_config,
  //      temp_file_path);
}

}  // namespace test

}  // namespace parquet

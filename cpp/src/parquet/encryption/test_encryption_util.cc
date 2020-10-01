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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#include <arrow/io/file.h>

#include "parquet/encryption/test_encryption_util.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/properties.h"

using FileClass = ::arrow::io::FileOutputStream;

namespace parquet {
namespace encryption {
namespace test {

std::string data_file(const char* file) {
  std::string dir_string(parquet::test::get_data_dir());
  std::stringstream ss;
  ss << dir_string << "/" << file;
  return ss.str();
}

FileEncryptor::FileEncryptor() { schema_ = SetupEncryptionSchema(); }

std::shared_ptr<GroupNode> FileEncryptor::SetupEncryptionSchema() {
  parquet::schema::NodeVector fields;
  // Create a primitive node named 'boolean_field' with type:BOOLEAN,
  // repetition:REQUIRED
  fields.push_back(PrimitiveNode::Make(kBooleanFieldName, Repetition::REQUIRED,
                                       Type::BOOLEAN, ConvertedType::NONE));

  // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED,
  // logical type:TIME_MILLIS
  fields.push_back(PrimitiveNode::Make(kInt32FieldName, Repetition::REQUIRED, Type::INT32,
                                       ConvertedType::TIME_MILLIS));

  // Create a primitive node named 'int64_field' with type:INT64, repetition:REPEATED
  fields.push_back(PrimitiveNode::Make(kInt64FieldName, Repetition::REPEATED, Type::INT64,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kInt96FieldName, Repetition::REQUIRED, Type::INT96,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kFloatFieldName, Repetition::REQUIRED, Type::FLOAT,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kDoubleFieldName, Repetition::REQUIRED,
                                       Type::DOUBLE, ConvertedType::NONE));

  // Create a primitive node named 'ba_field' with type:BYTE_ARRAY, repetition:OPTIONAL
  fields.push_back(PrimitiveNode::Make(kByteArrayFieldName, Repetition::OPTIONAL,
                                       Type::BYTE_ARRAY, ConvertedType::NONE));

  // Create a primitive node named 'flba_field' with type:FIXED_LEN_BYTE_ARRAY,
  // repetition:REQUIRED, field_length = kFixedLength
  fields.push_back(PrimitiveNode::Make(kFixedLenByteArrayFieldName, Repetition::REQUIRED,
                                       Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE,
                                       kFixedLength));

  // Create a GroupNode named 'schema' using the primitive nodes defined above
  // This GroupNode is the root node of the schema tree
  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

void FileEncryptor::EncryptFile(
    std::string file,
    std::shared_ptr<parquet::FileEncryptionProperties> encryption_configurations) {
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

void FileDecryptor::DecryptFile(
    std::string file,
    std::shared_ptr<FileDecryptionProperties> file_decryption_properties) {
  std::string exception_msg;
  parquet::ReaderProperties reader_properties = parquet::default_reader_properties();
  if (file_decryption_properties) {
    reader_properties.file_decryption_properties(file_decryption_properties->DeepClone());
  }

  auto file_reader = parquet::ParquetFileReader::OpenFile(file, false, reader_properties);

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
    std::shared_ptr<parquet::RowGroupReader> row_group_reader = file_reader->RowGroup(r);

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
      rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level, &value,
                                          &values_read);
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

    if (file_decryption_properties) {
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

}  // namespace test
}  // namespace encryption
}  // namespace parquet

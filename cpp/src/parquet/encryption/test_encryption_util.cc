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

#include <sstream>

#include <arrow/io/file.h>

#include "arrow/testing/future_util.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/test_util.h"

using ::arrow::io::FileOutputStream;

using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

namespace parquet {
namespace encryption {
namespace test {

std::string data_file(const char* file) {
  std::string dir_string(parquet::test::get_data_dir());
  std::stringstream ss;
  ss << dir_string << "/" << file;
  return ss.str();
}

std::unordered_map<std::string, std::string> BuildKeyMap(const char* const* column_ids,
                                                         const char* const* column_keys,
                                                         const char* footer_id,
                                                         const char* footer_key) {
  std::unordered_map<std::string, std::string> key_map;
  // add column keys
  for (int i = 0; i < 6; i++) {
    key_map.insert({column_ids[i], column_keys[i]});
  }
  // add footer key
  key_map.insert({footer_id, footer_key});

  return key_map;
}

std::string BuildColumnKeyMapping() {
  std::ostringstream stream;
  stream << kColumnMasterKeyIds[0] << ":" << kDoubleFieldName << ";"
         << kColumnMasterKeyIds[1] << ":" << kFloatFieldName << ";"
         << kColumnMasterKeyIds[2] << ":" << kBooleanFieldName << ";"
         << kColumnMasterKeyIds[3] << ":" << kInt32FieldName << ";"
         << kColumnMasterKeyIds[4] << ":" << kByteArrayFieldName << ";"
         << kColumnMasterKeyIds[5] << ":" << kFixedLenByteArrayFieldName << ";";
  return stream.str();
}

template <typename DType>
struct ColumnData {
  typedef typename DType::c_type T;

  std::vector<T> values;
  std::vector<int16_t> definition_levels;
  std::vector<int16_t> repetition_levels;

  int64_t rows() const { return values.size(); }
  const T* raw_values() const { return values.data(); }
  const int16_t* raw_definition_levels() const {
    return definition_levels.size() == 0 ? nullptr : definition_levels.data();
  }
  const int16_t* raw_repetition_levels() const {
    return repetition_levels.size() == 0 ? nullptr : repetition_levels.data();
  }
};

template <typename DType>
ColumnData<DType> GenerateSampleData(int rows) {
  return ColumnData<DType>();
}

template <>
ColumnData<Int32Type> GenerateSampleData<Int32Type>(int rows) {
  ColumnData<Int32Type> int32_col;
  // Int32 column
  for (int i = 0; i < rows; i++) {
    int32_col.values.push_back(i);
  }
  return int32_col;
}

template <>
ColumnData<Int64Type> GenerateSampleData<Int64Type>(int rows) {
  ColumnData<Int64Type> int64_col;
  // The Int64 column. Each row has repeats twice.
  for (int i = 0; i < 2 * rows; i++) {
    int64_t value = i * 1000 * 1000;
    value *= 1000 * 1000;
    int16_t definition_level = 1;
    int16_t repetition_level = 0;
    if ((i % 2) == 0) {
      repetition_level = 1;  // start of a new record
    }
    int64_col.values.push_back(value);
    int64_col.definition_levels.push_back(definition_level);
    int64_col.repetition_levels.push_back(repetition_level);
  }
  return int64_col;
}

template <>
ColumnData<Int96Type> GenerateSampleData<Int96Type>(int rows) {
  ColumnData<Int96Type> int96_col;
  for (int i = 0; i < rows; i++) {
    parquet::Int96 value;
    value.value[0] = i;
    value.value[1] = i + 1;
    value.value[2] = i + 2;
    int96_col.values.push_back(value);
  }
  return int96_col;
}

template <>
ColumnData<FloatType> GenerateSampleData<FloatType>(int rows) {
  ColumnData<FloatType> float_col;
  for (int i = 0; i < rows; i++) {
    float value = static_cast<float>(i) * 1.1f;
    float_col.values.push_back(value);
  }
  return float_col;
}

template <>
ColumnData<DoubleType> GenerateSampleData<DoubleType>(int rows) {
  ColumnData<DoubleType> double_col;
  for (int i = 0; i < rows; i++) {
    double value = i * 1.1111111;
    double_col.values.push_back(value);
  }
  return double_col;
}

template <typename DType, typename NextFunc>
void WriteBatch(int rows, const NextFunc get_next_column) {
  ColumnData<DType> column = GenerateSampleData<DType>(rows);
  TypedColumnWriter<DType>* writer =
      static_cast<TypedColumnWriter<DType>*>(get_next_column());
  writer->WriteBatch(column.rows(), column.raw_definition_levels(),
                     column.raw_repetition_levels(), column.raw_values());
}

FileEncryptor::FileEncryptor() { schema_ = SetupEncryptionSchema(); }

std::shared_ptr<GroupNode> FileEncryptor::SetupEncryptionSchema() {
  parquet::schema::NodeVector fields;

  fields.push_back(PrimitiveNode::Make(kBooleanFieldName, Repetition::REQUIRED,
                                       Type::BOOLEAN, ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kInt32FieldName, Repetition::REQUIRED, Type::INT32,
                                       ConvertedType::TIME_MILLIS));

  fields.push_back(PrimitiveNode::Make(kInt64FieldName, Repetition::REPEATED, Type::INT64,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kInt96FieldName, Repetition::REQUIRED, Type::INT96,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kFloatFieldName, Repetition::REQUIRED, Type::FLOAT,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kDoubleFieldName, Repetition::REQUIRED,
                                       Type::DOUBLE, ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kByteArrayFieldName, Repetition::OPTIONAL,
                                       Type::BYTE_ARRAY, ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make(kFixedLenByteArrayFieldName, Repetition::REQUIRED,
                                       Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE,
                                       kFixedLength));

  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

void FileEncryptor::EncryptFile(
    std::string file,
    std::shared_ptr<parquet::FileEncryptionProperties> encryption_configurations) {
  WriterProperties::Builder prop_builder;
  prop_builder.compression(parquet::Compression::UNCOMPRESSED);
  prop_builder.encryption(encryption_configurations);
  std::shared_ptr<WriterProperties> writer_properties = prop_builder.build();

  PARQUET_ASSIGN_OR_THROW(auto out_file, FileOutputStream::Open(file));
  // Create a ParquetFileWriter instance
  std::shared_ptr<parquet::ParquetFileWriter> file_writer =
      parquet::ParquetFileWriter::Open(out_file, schema_, writer_properties);

  for (int r = 0; r < num_rowgroups_; r++) {
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
    WriteBatch<Int32Type>(rows_per_rowgroup_, get_next_column);

    // Write the Int64 column.
    WriteBatch<Int64Type>(rows_per_rowgroup_, get_next_column);

    // Write the INT96 column.
    WriteBatch<Int96Type>(rows_per_rowgroup_, get_next_column);

    // Write the Float column
    WriteBatch<FloatType>(rows_per_rowgroup_, get_next_column);

    // Write the Double column
    WriteBatch<DoubleType>(rows_per_rowgroup_, get_next_column);

    // Write the ByteArray column. Make every alternate values NULL
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
  PARQUET_THROW_NOT_OK(out_file->Close());

  return;
}  // namespace test

template <typename DType, typename RowGroupReader, typename RowGroupMetadata>
void ReadAndVerifyColumn(RowGroupReader* rg_reader, RowGroupMetadata* rg_md,
                         int column_index, int rows) {
  ColumnData<DType> expected_column_data = GenerateSampleData<DType>(rows);
  std::shared_ptr<parquet::ColumnReader> column_reader = rg_reader->Column(column_index);
  TypedColumnReader<DType>* reader =
      static_cast<TypedColumnReader<DType>*>(column_reader.get());

  std::unique_ptr<ColumnChunkMetaData> col_md = rg_md->ColumnChunk(column_index);

  int64_t rows_should_read = expected_column_data.values.size();

  // Read all the rows in the column
  ColumnData<DType> read_col_data;
  read_col_data.values.resize(rows_should_read);
  int64_t values_read;
  int64_t rows_read;
  if (expected_column_data.definition_levels.size() > 0 &&
      expected_column_data.repetition_levels.size() > 0) {
    std::vector<int16_t> definition_levels(rows_should_read);
    std::vector<int16_t> repetition_levels(rows_should_read);
    rows_read = reader->ReadBatch(rows_should_read, definition_levels.data(),
                                  repetition_levels.data(), read_col_data.values.data(),
                                  &values_read);
    ASSERT_EQ(definition_levels, expected_column_data.definition_levels);
    ASSERT_EQ(repetition_levels, expected_column_data.repetition_levels);
  } else {
    rows_read = reader->ReadBatch(rows_should_read, nullptr, nullptr,
                                  read_col_data.values.data(), &values_read);
  }
  ASSERT_EQ(rows_read, rows_should_read);
  ASSERT_EQ(values_read, rows_should_read);
  // make sure we got the same number of values the metadata says
  ASSERT_EQ(col_md->num_values(), rows_read);
  // GH-35571: need to use approximate floating-point comparison because of
  // precision issues on MinGW32 (the values generated in the C++ test code
  // may not exactly match those from the parquet-testing data files).
  if constexpr (std::is_floating_point_v<typename DType::c_type>) {
    ASSERT_EQ(read_col_data.rows(), expected_column_data.rows());
    for (int i = 0; i < read_col_data.rows(); ++i) {
      if constexpr (std::is_same_v<float, typename DType::c_type>) {
        EXPECT_FLOAT_EQ(expected_column_data.values[i], read_col_data.values[i]);
      } else {
        EXPECT_DOUBLE_EQ(expected_column_data.values[i], read_col_data.values[i]);
      }
    }
  } else {
    ASSERT_EQ(expected_column_data.values, read_col_data.values);
  }
}

void FileDecryptor::DecryptFile(
    std::string file,
    std::shared_ptr<FileDecryptionProperties> file_decryption_properties) {
  std::string exception_msg;
  parquet::ReaderProperties reader_properties = parquet::default_reader_properties();
  if (file_decryption_properties) {
    reader_properties.file_decryption_properties(file_decryption_properties->DeepClone());
  }

  std::shared_ptr<::arrow::io::RandomAccessFile> source;
  PARQUET_ASSIGN_OR_THROW(
      source, ::arrow::io::ReadableFile::Open(file, reader_properties.memory_pool()));

  auto file_reader = parquet::ParquetFileReader::Open(source, reader_properties);
  CheckFile(file_reader.get(), file_decryption_properties.get());

  if (file_decryption_properties) {
    reader_properties.file_decryption_properties(file_decryption_properties->DeepClone());
  }
  auto fut = parquet::ParquetFileReader::OpenAsync(source, reader_properties);
  ASSERT_FINISHES_OK(fut);
  ASSERT_OK_AND_ASSIGN(file_reader, fut.MoveResult());
  CheckFile(file_reader.get(), file_decryption_properties.get());

  file_reader->Close();
  PARQUET_THROW_NOT_OK(source->Close());
}

void FileDecryptor::CheckFile(parquet::ParquetFileReader* file_reader,
                              FileDecryptionProperties* file_decryption_properties) {
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

    int rows_per_rowgroup = static_cast<int>(rg_metadata->num_rows());

    int64_t values_read = 0;
    int64_t rows_read = 0;
    int16_t definition_level;
    // int16_t repetition_level;
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

    ReadAndVerifyColumn<Int32Type>(row_group_reader.get(), rg_metadata.get(), 1,
                                   rows_per_rowgroup);

    ReadAndVerifyColumn<Int64Type>(row_group_reader.get(), rg_metadata.get(), 2,
                                   rows_per_rowgroup);

    ReadAndVerifyColumn<Int96Type>(row_group_reader.get(), rg_metadata.get(), 3,
                                   rows_per_rowgroup);

    if (file_decryption_properties) {
      ReadAndVerifyColumn<FloatType>(row_group_reader.get(), rg_metadata.get(), 4,
                                     rows_per_rowgroup);

      ReadAndVerifyColumn<DoubleType>(row_group_reader.get(), rg_metadata.get(), 5,
                                      rows_per_rowgroup);
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
}

}  // namespace test
}  // namespace encryption
}  // namespace parquet

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cassert>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

/*
 * This example describes writing and reading Parquet Files in C++ and serves as a
 * reference to the API.
 * The file contains all the physical data types supported by Parquet.
 **/

/* Parquet is a structured columnar file format
 * Parquet File = "Parquet data" + "Parquet Metadata"
 * "Parquet data" is simply a vector of RowGroups. Each RowGroup is a batch of rows in a
 * columnar layout
 * "Parquet Metadata" contains the "file schema" and attributes of the RowGroups and their
 * Columns
 * "file schema" is a tree where each node is either a primitive type (leaf nodes) or a
 * complex (nested) type (internal nodes)
 * For specific details, please refer the format here:
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 **/

constexpr int NUM_ROWS_PER_ROW_GROUP = 500;
constexpr int FIXED_LENGTH = 10;
const char PARQUET_FILENAME[] = "parquet_cpp_example.parquet";

using parquet::Repetition;
using parquet::Type;
using parquet::LogicalType;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;

static std::shared_ptr<GroupNode> SetupSchema() {
  parquet::schema::NodeVector fields;
  // Create a primitive node named 'boolean_field' with type:BOOLEAN,
  // repetition:REQUIRED
  fields.push_back(PrimitiveNode::Make("boolean_field", Repetition::REQUIRED,
                                       Type::BOOLEAN, LogicalType::NONE));

  // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED,
  // logical type:TIME_MILLIS
  fields.push_back(PrimitiveNode::Make("int32_field", Repetition::REQUIRED, Type::INT32,
                                       LogicalType::TIME_MILLIS));

  // Create a primitive node named 'int64_field' with type:INT64, repetition:REPEATED
  fields.push_back(PrimitiveNode::Make("int64_field", Repetition::REPEATED, Type::INT64,
                                       LogicalType::NONE));

  fields.push_back(PrimitiveNode::Make("int96_field", Repetition::REQUIRED, Type::INT96,
                                       LogicalType::NONE));

  fields.push_back(PrimitiveNode::Make("float_field", Repetition::REQUIRED, Type::FLOAT,
                                       LogicalType::NONE));

  fields.push_back(PrimitiveNode::Make("double_field", Repetition::REQUIRED, Type::DOUBLE,
                                       LogicalType::NONE));

  // Create a primitive node named 'ba_field' with type:BYTE_ARRAY, repetition:OPTIONAL
  fields.push_back(PrimitiveNode::Make("ba_field", Repetition::OPTIONAL, Type::BYTE_ARRAY,
                                       LogicalType::NONE));

  // Create a primitive node named 'flba_field' with type:FIXED_LEN_BYTE_ARRAY,
  // repetition:REQUIRED, field_length = FIXED_LENGTH
  fields.push_back(PrimitiveNode::Make("flba_field", Repetition::REQUIRED,
                                       Type::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE,
                                       FIXED_LENGTH));

  // Create a GroupNode named 'schema' using the primitive nodes defined above
  // This GroupNode is the root node of the schema tree
  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

int main(int argc, char** argv) {
  /**********************************************************************************
                             PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  try {
    // Create a local file output stream instance.
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    PARQUET_THROW_NOT_OK(FileClass::Open(PARQUET_FILENAME, &out_file));

    // Setup the parquet schema
    std::shared_ptr<GroupNode> schema = SetupSchema();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rg_writer =
        file_writer->AppendRowGroup(NUM_ROWS_PER_ROW_GROUP);

    // Write the Bool column
    parquet::BoolWriter* bool_writer =
        static_cast<parquet::BoolWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      bool value = ((i % 2) == 0) ? true : false;
      bool_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Int32 column
    parquet::Int32Writer* int32_writer =
        static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int32_t value = i;
      int32_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Int64 column. Each row has repeats twice.
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
    for (int i = 0; i < 2 * NUM_ROWS_PER_ROW_GROUP; i++) {
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
        static_cast<parquet::Int96Writer*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::Int96 value;
      value.value[0] = i;
      value.value[1] = i + 1;
      value.value[2] = i + 2;
      int96_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Float column
    parquet::FloatWriter* float_writer =
        static_cast<parquet::FloatWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      float value = i * 1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Double column
    parquet::DoubleWriter* double_writer =
        static_cast<parquet::DoubleWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      double value = i * 1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the ByteArray column. Make every alternate values NULL
    parquet::ByteArrayWriter* ba_writer =
        static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::ByteArray value;
      char hello[FIXED_LENGTH] = "parquet";
      hello[7] = '0' + i / 100;
      hello[8] = '0' + (i / 10) % 10;
      hello[9] = '0' + i % 10;
      if (i % 2 == 0) {
        int16_t definition_level = 1;
        value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
        value.len = FIXED_LENGTH;
        ba_writer->WriteBatch(1, &definition_level, nullptr, &value);
      } else {
        int16_t definition_level = 0;
        ba_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
      }
    }

    // Write the FixedLengthByteArray column
    parquet::FixedLenByteArrayWriter* flba_writer =
        static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::FixedLenByteArray value;
      char v = static_cast<char>(i);
      char flba[FIXED_LENGTH] = {v, v, v, v, v, v, v, v, v, v};
      value.ptr = reinterpret_cast<const uint8_t*>(&flba[0]);

      flba_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Close the ParquetFileWriter
    file_writer->Close();

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << std::endl;
    return -1;
  }

  /**********************************************************************************
                             PARQUET READER EXAMPLE
  **********************************************************************************/

  try {
    // Create a ParquetReader instance
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
        parquet::ParquetFileReader::OpenFile(PARQUET_FILENAME, false);

    // Get the File MetaData
    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

    // Get the number of RowGroups
    int num_row_groups = file_metadata->num_row_groups();
    assert(num_row_groups == 1);

    // Get the number of Columns
    int num_columns = file_metadata->num_columns();
    assert(num_columns == 8);

    // Iterate over all the RowGroups in the file
    for (int r = 0; r < num_row_groups; ++r) {
      // Get the RowGroup Reader
      std::shared_ptr<parquet::RowGroupReader> row_group_reader =
          parquet_reader->RowGroup(r);

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
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        bool expected_value = ((i % 2) == 0) ? true : false;
        assert(value == expected_value);
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
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        assert(value == i);
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
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        int64_t expected_value = i * 1000 * 1000;
        expected_value *= 1000 * 1000;
        assert(value == expected_value);
        if ((i % 2) == 0) {
          assert(repetition_level == 1);
        } else {
          assert(repetition_level == 0);
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
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        parquet::Int96 expected_value;
        expected_value.value[0] = i;
        expected_value.value[1] = i + 1;
        expected_value.value[2] = i + 2;
        for (int j = 0; j < 3; j++) {
          assert(value.value[j] == expected_value.value[j]);
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
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        float expected_value = i * 1.1f;
        assert(value == expected_value);
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
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        double expected_value = i * 1.1111111;
        assert(value == expected_value);
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
        assert(rows_read == 1);
        // Verify the value written
        char expected_value[FIXED_LENGTH] = "parquet";
        expected_value[7] = '0' + i / 100;
        expected_value[8] = '0' + (i / 10) % 10;
        expected_value[9] = '0' + i % 10;
        if (i % 2 == 0) {  // only alternate values exist
          // There are no NULL values in the rows written
          assert(values_read == 1);
          assert(value.len == FIXED_LENGTH);
          assert(memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0);
          assert(definition_level == 1);
        } else {
          // There are NULL values in the rows written
          assert(values_read == 0);
          assert(definition_level == 0);
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
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        char v = static_cast<char>(i);
        char expected_value[FIXED_LENGTH] = {v, v, v, v, v, v, v, v, v, v};
        assert(memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0);
        i++;
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "Parquet read error: " << e.what() << std::endl;
    return -1;
  }

  std::cout << "Parquet Writing and Reading Complete" << std::endl;

  return 0;
}

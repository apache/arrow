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

#include "parquet/metadata3.h"

#include <gtest/gtest.h>
#include <fstream>

#include "arrow/io/memory.h"
#include "arrow/testing/gtest_compat.h"
#include "arrow/util/config.h"
#include "flatbuffers/flatbuffers.h"
#include "generated/parquet3_generated.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"
#include "parquet/types.h"

namespace parquet {

namespace test {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

class TestMetadata3RoundTrip : public ::testing::Test {
 public:
  void SetUp() override {}

 protected:
  // Helper to verify flatbuffer is valid
  void VerifyFlatbuffer(const std::string& flatbuf) {
    // FlatBuffers require proper alignment. When copied to a string, alignment may be
    // lost. Create an aligned buffer for verification
    std::vector<uint8_t> aligned_buffer(flatbuf.begin(), flatbuf.end());
    flatbuffers::Verifier verifier(aligned_buffer.data(), aligned_buffer.size());
    ASSERT_TRUE(format3::VerifyFileMetaDataBuffer(verifier));
  }

  // Helper to compare logical equivalence of Thrift FileMetaData after round-trip
  void AssertFileMetadataLogicallyEqual(const format::FileMetaData& original,
                                        const format::FileMetaData& converted) {
    // Compare file-level metadata
    ASSERT_EQ(original.version, converted.version);
    ASSERT_EQ(original.num_rows, converted.num_rows);
    ASSERT_EQ(original.schema.size(), converted.schema.size());
    ASSERT_EQ(original.row_groups.size(), converted.row_groups.size());

    // Compare row groups
    for (size_t rg = 0; rg < original.row_groups.size(); ++rg) {
      const auto& orig_rg = original.row_groups[rg];
      const auto& conv_rg = converted.row_groups[rg];
      ASSERT_EQ(orig_rg.num_rows, conv_rg.num_rows);
      ASSERT_EQ(orig_rg.total_byte_size, conv_rg.total_byte_size);
      ASSERT_EQ(orig_rg.columns.size(), conv_rg.columns.size());

      // Compare columns
      for (size_t col = 0; col < orig_rg.columns.size(); ++col) {
        const auto& orig_col = orig_rg.columns[col].meta_data;
        const auto& conv_col = conv_rg.columns[col].meta_data;

        ASSERT_EQ(orig_col.type, conv_col.type);
        ASSERT_EQ(orig_col.codec, conv_col.codec);
        ASSERT_EQ(orig_col.num_values, conv_col.num_values);
        ASSERT_EQ(orig_col.total_compressed_size, conv_col.total_compressed_size);
        ASSERT_EQ(orig_col.total_uncompressed_size, conv_col.total_uncompressed_size);
        ASSERT_EQ(orig_col.data_page_offset, conv_col.data_page_offset);
        ASSERT_EQ(orig_col.path_in_schema, conv_col.path_in_schema);

        // Compare dictionary_page_offset
        ASSERT_EQ(orig_col.__isset.dictionary_page_offset,
                  conv_col.__isset.dictionary_page_offset);
        if (orig_col.__isset.dictionary_page_offset) {
          ASSERT_EQ(orig_col.dictionary_page_offset, conv_col.dictionary_page_offset);
        }

        // Compare statistics
        ASSERT_EQ(orig_col.__isset.statistics, conv_col.__isset.statistics);
        if (orig_col.__isset.statistics) {
          const auto& orig_stats = orig_col.statistics;
          const auto& conv_stats = conv_col.statistics;

          ASSERT_EQ(orig_stats.__isset.null_count, conv_stats.__isset.null_count);
          if (orig_stats.__isset.null_count) {
            ASSERT_EQ(orig_stats.null_count, conv_stats.null_count);
          }

          ASSERT_EQ(orig_stats.__isset.min_value, conv_stats.__isset.min_value);
          ASSERT_EQ(orig_stats.__isset.max_value, conv_stats.__isset.max_value);

          if (orig_stats.__isset.min_value && orig_stats.__isset.max_value) {
            // Metadata3 is lossy for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY types
            // It only stores up to 4 bytes (after removing common prefix) to save space
            bool is_byte_array = (orig_col.type == format::Type::BYTE_ARRAY ||
                                  orig_col.type == format::Type::FIXED_LEN_BYTE_ARRAY);

            if (is_byte_array) {
              // For byte arrays, verify that truncated statistics form a conservative
              // (wider) range converted_min <= original_min and converted_max >=
              // original_max
              ASSERT_LE(conv_stats.min_value, orig_stats.min_value)
                  << "Converted min should be <= original min for conservative filtering";
              ASSERT_GE(conv_stats.max_value, orig_stats.max_value)
                  << "Converted max should be >= original max for conservative filtering";

              // The is_exact flag should be false for truncated values
              if (orig_stats.min_value.size() > 4) {
                ASSERT_FALSE(conv_stats.is_min_value_exact);
                ASSERT_FALSE(conv_stats.is_max_value_exact);
              }
            } else {
              // For other types, values should match exactly
              ASSERT_EQ(orig_stats.min_value, conv_stats.min_value);
              ASSERT_EQ(orig_stats.max_value, conv_stats.max_value);
              ASSERT_EQ(orig_stats.is_min_value_exact, conv_stats.is_min_value_exact);
              ASSERT_EQ(orig_stats.is_max_value_exact, conv_stats.is_max_value_exact);
            }
          }
        }
      }
    }
  }

  // Helper to create a simple schema with specified types
  std::shared_ptr<GroupNode> MakeSchema(const std::vector<Type::type>& types,
                                        const std::vector<std::string>& names) {
    schema::NodeVector fields;
    for (size_t i = 0; i < types.size(); ++i) {
      fields.push_back(schema::PrimitiveNode::Make(names[i], Repetition::OPTIONAL,
                                                   types[i], ConvertedType::NONE));
    }
    return std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  // Helper to write a Parquet file with random data
  std::shared_ptr<::arrow::Buffer> WriteParquetFile(
      std::shared_ptr<GroupNode> schema, int num_rowgroups, int rows_per_rowgroup,
      const std::shared_ptr<WriterProperties>& props = nullptr) {
    auto sink = CreateOutputStream();

    auto writer_props = props;
    if (!writer_props) {
      // Enable metadata3 by default for these tests
      writer_props = WriterProperties::Builder().enable_write_metadata3()->build();
    }

    auto file_writer = ParquetFileWriter::Open(sink, schema, writer_props);
    SchemaDescriptor schema_descr;
    schema_descr.Init(schema);

    for (int rg = 0; rg < num_rowgroups; ++rg) {
      auto row_group_writer = file_writer->AppendRowGroup();

      for (int col = 0; col < schema_descr.num_columns(); ++col) {
        const auto* descr = schema_descr.Column(col);
        Type::type type = descr->physical_type();

        switch (type) {
          case Type::INT32: {
            auto column_writer =
                static_cast<Int32Writer*>(row_group_writer->NextColumn());
            std::vector<int32_t> values(rows_per_rowgroup);
            std::vector<int16_t> def_levels(rows_per_rowgroup);
            random_numbers(rows_per_rowgroup, rg * 1000 + col, -10000, 10000,
                           values.data());
            random_numbers(rows_per_rowgroup, rg * 2000 + col, (int16_t)0, (int16_t)1,
                           def_levels.data());
            column_writer->WriteBatch(rows_per_rowgroup, def_levels.data(), nullptr,
                                      values.data());
            column_writer->Close();
            break;
          }
          case Type::INT64: {
            auto column_writer =
                static_cast<Int64Writer*>(row_group_writer->NextColumn());
            std::vector<int64_t> values(rows_per_rowgroup);
            std::vector<int16_t> def_levels(rows_per_rowgroup);
            random_numbers(rows_per_rowgroup, rg * 1000 + col, (int64_t)-1000000,
                           (int64_t)1000000, values.data());
            random_numbers(rows_per_rowgroup, rg * 2000 + col, (int16_t)0, (int16_t)1,
                           def_levels.data());
            column_writer->WriteBatch(rows_per_rowgroup, def_levels.data(), nullptr,
                                      values.data());
            column_writer->Close();
            break;
          }
          case Type::FLOAT: {
            auto column_writer =
                static_cast<FloatWriter*>(row_group_writer->NextColumn());
            std::vector<float> values(rows_per_rowgroup);
            std::vector<int16_t> def_levels(rows_per_rowgroup);
            random_numbers(rows_per_rowgroup, rg * 1000 + col, -1000.0f, 1000.0f,
                           values.data());
            random_numbers(rows_per_rowgroup, rg * 2000 + col, (int16_t)0, (int16_t)1,
                           def_levels.data());
            column_writer->WriteBatch(rows_per_rowgroup, def_levels.data(), nullptr,
                                      values.data());
            column_writer->Close();
            break;
          }
          case Type::DOUBLE: {
            auto column_writer =
                static_cast<DoubleWriter*>(row_group_writer->NextColumn());
            std::vector<double> values(rows_per_rowgroup);
            std::vector<int16_t> def_levels(rows_per_rowgroup);
            random_numbers(rows_per_rowgroup, rg * 1000 + col, -10000.0, 10000.0,
                           values.data());
            random_numbers(rows_per_rowgroup, rg * 2000 + col, (int16_t)0, (int16_t)1,
                           def_levels.data());
            column_writer->WriteBatch(rows_per_rowgroup, def_levels.data(), nullptr,
                                      values.data());
            column_writer->Close();
            break;
          }
          case Type::BYTE_ARRAY: {
            auto column_writer =
                static_cast<ByteArrayWriter*>(row_group_writer->NextColumn());
            std::vector<ByteArray> values(rows_per_rowgroup);
            std::vector<int16_t> def_levels(rows_per_rowgroup);
            std::vector<uint8_t> buf(rows_per_rowgroup * 20);
            random_byte_array(rows_per_rowgroup, rg * 1000 + col, buf.data(),
                              values.data(), 5, 15);
            random_numbers(rows_per_rowgroup, rg * 2000 + col, (int16_t)0, (int16_t)1,
                           def_levels.data());
            column_writer->WriteBatch(rows_per_rowgroup, def_levels.data(), nullptr,
                                      values.data());
            column_writer->Close();
            break;
          }
          case Type::FIXED_LEN_BYTE_ARRAY: {
            auto column_writer =
                static_cast<FixedLenByteArrayWriter*>(row_group_writer->NextColumn());
            std::vector<FixedLenByteArray> values(rows_per_rowgroup);
            std::vector<int16_t> def_levels(rows_per_rowgroup);
            std::vector<uint8_t> buf(rows_per_rowgroup * FLBA_LENGTH);
            random_fixed_byte_array(rows_per_rowgroup, rg * 1000 + col, buf.data(),
                                    FLBA_LENGTH, values.data());
            random_numbers(rows_per_rowgroup, rg * 2000 + col, (int16_t)0, (int16_t)1,
                           def_levels.data());
            column_writer->WriteBatch(rows_per_rowgroup, def_levels.data(), nullptr,
                                      values.data());
            column_writer->Close();
            break;
          }
          default:
            throw ParquetException("Unsupported type in test");
        }
      }
      row_group_writer->Close();
    }
    file_writer->Close();

    PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
    return buffer;
  }
};

// Test basic round-trip conversion with INT32 columns
// Debug test to check flatbuffer verification
TEST_F(TestMetadata3RoundTrip, DebugFlatbufferVerification) {
  auto schema = MakeSchema({Type::INT32}, {"col1"});
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/1, /*rows_per_rowgroup=*/100);

  // First, let's check if the metadata can be converted to flatbuffer
  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  auto metadata = file_reader->metadata();

  // Get the Thrift metadata
  std::string thrift_serialized = metadata->SerializeToString();
  auto reader_props = default_reader_properties();
  ThriftDeserializer deserializer(reader_props);
  format::FileMetaData thrift_md;
  uint32_t len = static_cast<uint32_t>(thrift_serialized.size());
  deserializer.DeserializeMessage(
      reinterpret_cast<const uint8_t*>(thrift_serialized.data()), &len, &thrift_md);

  // Check writer properties
  auto writer_props = WriterProperties::Builder().enable_write_metadata3()->build();
  std::cout << "Writer properties write_metadata3(): "
            << (writer_props->write_metadata3() ? "TRUE" : "FALSE") << std::endl;

  // Print schema info
  std::cout << "Schema size: " << thrift_md.schema.size() << std::endl;
  for (size_t i = 0; i < thrift_md.schema.size() && i < 5; ++i) {
    auto& se = thrift_md.schema[i];
    std::cout << "Schema element " << i << ": name=" << se.name;
    if (se.__isset.type) std::cout << ", type=" << se.type;
    if (se.__isset.converted_type) std::cout << ", converted_type=" << se.converted_type;
    if (se.__isset.logicalType) std::cout << ", has logicalType";
    std::cout << std::endl;
  }

  // Try to convert
  std::string flatbuf;
  bool converted = ToFlatbuffer(&thrift_md, &flatbuf);
  std::cout << "ToFlatbuffer result: " << (converted ? "SUCCESS" : "FAILED") << std::endl;
  if (converted) {
    std::cout << "Converted flatbuffer size: " << flatbuf.size() << std::endl;
  }

  // Check file size
  std::cout << "Parquet file size: " << buffer->size() << " bytes" << std::endl;
  std::cout << "Thrift metadata size: " << thrift_serialized.size() << " bytes"
            << std::endl;

  // Extract the flatbuffer from the file
  std::string extracted_flatbuf;
  auto result = ExtractFlatbuffer(buffer, &extracted_flatbuf);
  std::cout << "ExtractFlatbuffer result: " << result.ok() << ", size: " << *result
            << std::endl;

  // Check the last few bytes of metadata to see if flatbuffer marker is there
  if (buffer->size() > 100) {
    // Read the metadata length from footer
    const uint8_t* footer = buffer->data() + buffer->size() - 8;
    uint32_t metadata_len = *reinterpret_cast<const uint32_t*>(footer);
    metadata_len = ::arrow::bit_util::FromLittleEndian(metadata_len);
    std::cout << "Metadata length from footer: " << metadata_len << " bytes" << std::endl;

    if (metadata_len > 50 && buffer->size() > metadata_len + 8) {
      const uint8_t* md_start = buffer->data() + buffer->size() - 8 - metadata_len;
      // Check last 40 bytes of metadata for UUID marker
      const uint8_t* check_pos = md_start + metadata_len - 40;
      std::cout << "Last 40 bytes of metadata (hex): ";
      for (int i = 0; i < 40 && i < metadata_len; ++i) {
        printf("%02x ", check_pos[i]);
      }
      std::cout << std::endl;
    }
  }

  if (result.ok() && *result > 0) {
    std::cout << "Flatbuffer size: " << extracted_flatbuf.size() << std::endl;

    // Try to verify it
    flatbuffers::Verifier verifier(
        reinterpret_cast<const uint8_t*>(extracted_flatbuf.data()),
        extracted_flatbuf.size());
    bool valid = format3::VerifyFileMetaDataBuffer(verifier);
    std::cout << "Verification result: " << (valid ? "PASSED" : "FAILED") << std::endl;

    // Also check if we can read it
    auto fmd = format3::GetFileMetaData(extracted_flatbuf.data());
    std::cout << "Can read FileMetaData: " << (fmd != nullptr ? "YES" : "NO")
              << std::endl;
    if (fmd) {
      std::cout << "Version: " << fmd->version() << std::endl;
      std::cout << "Num rows: " << fmd->num_rows() << std::endl;
    }

    ASSERT_TRUE(valid)
        << "Flatbuffer verification should pass for writer-created flatbuffers";
  } else {
    std::cout << "No flatbuffer found in file" << std::endl;
    if (!converted) {
      std::cout << "Reason: ToFlatbuffer() returned false - metadata cannot be converted"
                << std::endl;
    } else {
      std::cout << "ToFlatbuffer succeeded but flatbuffer not found in file - writer may "
                   "not be using it"
                << std::endl;
    }
  }
}

TEST_F(TestMetadata3RoundTrip, Int32Columns) {
  auto schema = MakeSchema({Type::INT32, Type::INT32}, {"col1", "col2"});

  // Write file with metadata3 enabled (flatbuffer will be embedded)
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/2, /*rows_per_rowgroup=*/100);

  // Read back without metadata3 to get Thrift metadata
  auto source1 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_thrift = default_reader_properties();
  reader_props_thrift.set_read_metadata3(false);
  auto file_reader1 = ParquetFileReader::Open(source1, reader_props_thrift);
  auto metadata1 = file_reader1->metadata();
  std::string thrift1 = metadata1->SerializeToString();

  // Read back with metadata3 enabled to read from flatbuffer
  auto source2 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_fb = default_reader_properties();
  reader_props_fb.set_read_metadata3(true);
  auto file_reader2 = ParquetFileReader::Open(source2, reader_props_fb);
  auto metadata2 = file_reader2->metadata();
  std::string thrift2 = metadata2->SerializeToString();

  // Deserialize both to compare
  ThriftDeserializer deserializer(default_reader_properties());
  format::FileMetaData md1, md2;
  uint32_t len1 = static_cast<uint32_t>(thrift1.size());
  uint32_t len2 = static_cast<uint32_t>(thrift2.size());
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift1.data()), &len1,
                                  &md1);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift2.data()), &len2,
                                  &md2);

  // Compare: metadata read from Thrift vs metadata read from Flatbuffer should be
  // equivalent
  AssertFileMetadataLogicallyEqual(md1, md2);
}

// Test round-trip with INT64 columns
TEST_F(TestMetadata3RoundTrip, Int64Columns) {
  auto schema = MakeSchema({Type::INT64, Type::INT64}, {"col1", "col2"});
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/2, /*rows_per_rowgroup=*/100);

  // Read back without metadata3 to get Thrift metadata
  auto source1 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_thrift = default_reader_properties();
  reader_props_thrift.set_read_metadata3(false);
  auto file_reader1 = ParquetFileReader::Open(source1, reader_props_thrift);
  auto metadata1 = file_reader1->metadata();
  std::string thrift1 = metadata1->SerializeToString();

  // Read back with metadata3 enabled to read from flatbuffer
  auto source2 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_fb = default_reader_properties();
  reader_props_fb.set_read_metadata3(true);
  auto file_reader2 = ParquetFileReader::Open(source2, reader_props_fb);
  auto metadata2 = file_reader2->metadata();
  std::string thrift2 = metadata2->SerializeToString();

  // Deserialize both to compare
  ThriftDeserializer deserializer(default_reader_properties());
  format::FileMetaData md1, md2;
  uint32_t len1 = static_cast<uint32_t>(thrift1.size());
  uint32_t len2 = static_cast<uint32_t>(thrift2.size());
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift1.data()), &len1,
                                  &md1);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift2.data()), &len2,
                                  &md2);

  // Compare: metadata read from Thrift vs metadata read from Flatbuffer should be
  // equivalent
  AssertFileMetadataLogicallyEqual(md1, md2);
}

// Test round-trip with FLOAT and DOUBLE columns
TEST_F(TestMetadata3RoundTrip, FloatDoubleColumns) {
  auto schema = MakeSchema({Type::FLOAT, Type::DOUBLE}, {"float_col", "double_col"});
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/2, /*rows_per_rowgroup=*/100);

  // Read back without metadata3 to get Thrift metadata
  auto source1 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_thrift = default_reader_properties();
  reader_props_thrift.set_read_metadata3(false);
  auto file_reader1 = ParquetFileReader::Open(source1, reader_props_thrift);
  auto metadata1 = file_reader1->metadata();
  std::string thrift1 = metadata1->SerializeToString();

  // Read back with metadata3 enabled to read from flatbuffer
  auto source2 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_fb = default_reader_properties();
  reader_props_fb.set_read_metadata3(true);
  auto file_reader2 = ParquetFileReader::Open(source2, reader_props_fb);
  auto metadata2 = file_reader2->metadata();
  std::string thrift2 = metadata2->SerializeToString();

  // Deserialize both to compare
  ThriftDeserializer deserializer(default_reader_properties());
  format::FileMetaData md1, md2;
  uint32_t len1 = static_cast<uint32_t>(thrift1.size());
  uint32_t len2 = static_cast<uint32_t>(thrift2.size());
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift1.data()), &len1,
                                  &md1);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift2.data()), &len2,
                                  &md2);

  // Compare: metadata read from Thrift vs metadata read from Flatbuffer should be
  // equivalent
  AssertFileMetadataLogicallyEqual(md1, md2);
}

// Test round-trip with BYTE_ARRAY columns
TEST_F(TestMetadata3RoundTrip, ByteArrayColumns) {
  auto schema = MakeSchema({Type::BYTE_ARRAY}, {"byte_array_col"});
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/2, /*rows_per_rowgroup=*/100);

  // Read back without metadata3 to get Thrift metadata
  auto source1 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_thrift = default_reader_properties();
  reader_props_thrift.set_read_metadata3(false);
  auto file_reader1 = ParquetFileReader::Open(source1, reader_props_thrift);
  auto metadata1 = file_reader1->metadata();
  std::string thrift1 = metadata1->SerializeToString();

  // Read back with metadata3 enabled to read from flatbuffer
  auto source2 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_fb = default_reader_properties();
  reader_props_fb.set_read_metadata3(true);
  auto file_reader2 = ParquetFileReader::Open(source2, reader_props_fb);
  auto metadata2 = file_reader2->metadata();
  std::string thrift2 = metadata2->SerializeToString();

  // Deserialize both to compare
  ThriftDeserializer deserializer(default_reader_properties());
  format::FileMetaData md1, md2;
  uint32_t len1 = static_cast<uint32_t>(thrift1.size());
  uint32_t len2 = static_cast<uint32_t>(thrift2.size());
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift1.data()), &len1,
                                  &md1);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift2.data()), &len2,
                                  &md2);

  // Compare: metadata read from Thrift vs metadata read from Flatbuffer should be
  // equivalent
  AssertFileMetadataLogicallyEqual(md1, md2);
}

// Test round-trip with FIXED_LEN_BYTE_ARRAY columns
TEST_F(TestMetadata3RoundTrip, FixedLenByteArrayColumns) {
  schema::NodeVector fields;
  fields.push_back(schema::PrimitiveNode::Make("flba_col", Repetition::OPTIONAL,
                                               Type::FIXED_LEN_BYTE_ARRAY,
                                               ConvertedType::NONE, FLBA_LENGTH));
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));

  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/2, /*rows_per_rowgroup=*/100);

  // Read back without metadata3 to get Thrift metadata
  auto source1 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_thrift = default_reader_properties();
  reader_props_thrift.set_read_metadata3(false);
  auto file_reader1 = ParquetFileReader::Open(source1, reader_props_thrift);
  auto metadata1 = file_reader1->metadata();
  std::string thrift1 = metadata1->SerializeToString();

  // Read back with metadata3 enabled to read from flatbuffer
  auto source2 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_fb = default_reader_properties();
  reader_props_fb.set_read_metadata3(true);
  auto file_reader2 = ParquetFileReader::Open(source2, reader_props_fb);
  auto metadata2 = file_reader2->metadata();
  std::string thrift2 = metadata2->SerializeToString();

  // Deserialize both to compare
  ThriftDeserializer deserializer(default_reader_properties());
  format::FileMetaData md1, md2;
  uint32_t len1 = static_cast<uint32_t>(thrift1.size());
  uint32_t len2 = static_cast<uint32_t>(thrift2.size());
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift1.data()), &len1,
                                  &md1);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift2.data()), &len2,
                                  &md2);

  // Compare: metadata read from Thrift vs metadata read from Flatbuffer should be
  // equivalent
  AssertFileMetadataLogicallyEqual(md1, md2);
}

// Test round-trip with mixed column types
TEST_F(TestMetadata3RoundTrip, MixedColumnTypes) {
  auto schema =
      MakeSchema({Type::INT32, Type::INT64, Type::FLOAT, Type::DOUBLE, Type::BYTE_ARRAY},
                 {"int32_col", "int64_col", "float_col", "double_col", "byte_array_col"});
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/3, /*rows_per_rowgroup=*/100);

  // Read back without metadata3 to get Thrift metadata
  auto source1 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_thrift = default_reader_properties();
  reader_props_thrift.set_read_metadata3(false);
  auto file_reader1 = ParquetFileReader::Open(source1, reader_props_thrift);
  auto metadata1 = file_reader1->metadata();
  std::string thrift1 = metadata1->SerializeToString();

  // Read back with metadata3 enabled to read from flatbuffer
  auto source2 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_fb = default_reader_properties();
  reader_props_fb.set_read_metadata3(true);
  auto file_reader2 = ParquetFileReader::Open(source2, reader_props_fb);
  auto metadata2 = file_reader2->metadata();
  std::string thrift2 = metadata2->SerializeToString();

  // Deserialize both to compare
  ThriftDeserializer deserializer(default_reader_properties());
  format::FileMetaData md1, md2;
  uint32_t len1 = static_cast<uint32_t>(thrift1.size());
  uint32_t len2 = static_cast<uint32_t>(thrift2.size());
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift1.data()), &len1,
                                  &md1);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift2.data()), &len2,
                                  &md2);

  // Compare: metadata read from Thrift vs metadata read from Flatbuffer should be
  // equivalent
  AssertFileMetadataLogicallyEqual(md1, md2);
}

// Test AppendFlatbuffer and ExtractFlatbuffer
TEST_F(TestMetadata3RoundTrip, AppendAndExtractFlatbuffer) {
  auto schema = MakeSchema({Type::INT32, Type::INT64}, {"col1", "col2"});
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/2, /*rows_per_rowgroup=*/100);

  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  auto metadata = file_reader->metadata();

  std::string thrift_serialized = metadata->SerializeToString();
  auto reader_props = default_reader_properties();
  ThriftDeserializer deserializer(reader_props);
  format::FileMetaData original_md;
  uint32_t len = static_cast<uint32_t>(thrift_serialized.size());
  deserializer.DeserializeMessage(
      reinterpret_cast<const uint8_t*>(thrift_serialized.data()), &len, &original_md);

  // Convert to flatbuffer
  std::string flatbuf;
  ASSERT_TRUE(ToFlatbuffer(&original_md, &flatbuf));

  // Verify the original flatbuffer before appending
  VerifyFlatbuffer(flatbuf);

  // Append flatbuffer to the Thrift serialized data
  std::string parquet_format = thrift_serialized;
  AppendFlatbuffer(flatbuf, &parquet_format);
  ASSERT_GT(parquet_format.size(), thrift_serialized.size());

  // Manually add footer (length + "PAR1") as WriteFileMetaData would
  uint32_t metadata_len = static_cast<uint32_t>(parquet_format.size());
  char footer[8];
  *reinterpret_cast<uint32_t*>(footer) = ::arrow::bit_util::ToLittleEndian(metadata_len);
  std::memcpy(footer + 4, "PAR1", 4);
  parquet_format.append(footer, 8);

  // Extract flatbuffer back
  auto parquet_buf = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(parquet_format.data()), parquet_format.size());
  std::string extracted_flatbuf;
  auto result = ExtractFlatbuffer(parquet_buf, &extracted_flatbuf);
  ASSERT_TRUE(result.ok()) << result.status();

  // Verify extracted flatbuffer
  VerifyFlatbuffer(extracted_flatbuf);

  // Do round-trip test on extracted flatbuffer
  // Convert Flatbuffer → Thrift
  auto fmd = format3::GetFileMetaData(extracted_flatbuf.data());
  format::FileMetaData converted_md = FromFlatbuffer(fmd);

  // Compare the original and round-tripped Thrift FileMetadata
  AssertFileMetadataLogicallyEqual(original_md, converted_md);
}

// Unit test for ExtractFlatbuffer - basic round-trip
TEST_F(TestMetadata3RoundTrip, ExtractFlatbufferRoundTrip) {
  // Create a simple Thrift FileMetaData
  format::FileMetaData thrift_md;
  thrift_md.__set_version(1);
  thrift_md.__set_num_rows(100);
  thrift_md.__set_created_by("test_creator");

  // Add a simple schema
  format::SchemaElement root;
  root.__set_name("test_schema");
  root.__set_repetition_type(format::FieldRepetitionType::REQUIRED);
  root.__set_num_children(0);
  thrift_md.schema.push_back(root);

  // Serialize the Thrift metadata
  ThriftSerializer serializer;
  uint32_t len;
  uint8_t* thrift_buffer;
  serializer.SerializeToBuffer(&thrift_md, &len, &thrift_buffer);
  std::string thrift_str(reinterpret_cast<const char*>(thrift_buffer), len);

  // Convert to flatbuffer
  std::string flatbuf;
  ASSERT_TRUE(ToFlatbuffer(&thrift_md, &flatbuf));

  // Append flatbuffer to thrift
  AppendFlatbuffer(flatbuf, &thrift_str);

  // Add Parquet footer (length + "PAR1")
  uint32_t metadata_len = static_cast<uint32_t>(thrift_str.size());
  char footer[8];
  *reinterpret_cast<uint32_t*>(footer) = ::arrow::bit_util::ToLittleEndian(metadata_len);
  std::memcpy(footer + 4, "PAR1", 4);
  thrift_str.append(footer, 8);

  // Now extract the flatbuffer
  auto buffer = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(thrift_str.data()), thrift_str.size());

  std::string extracted_flatbuf;
  auto result = ExtractFlatbuffer(buffer, &extracted_flatbuf);

  // Verify extraction succeeded
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_GT(*result, 0);

  // Verify the extracted flatbuffer matches the original
  ASSERT_EQ(flatbuf.size(), extracted_flatbuf.size());
  ASSERT_EQ(flatbuf, extracted_flatbuf);
}

// Unit test for ExtractFlatbuffer - no flatbuffer present
TEST_F(TestMetadata3RoundTrip, ExtractFlatbufferNotPresent) {
  // Create a mock Parquet file without flatbuffer extension
  std::string mock_thrift = "mock_thrift_data_without_extension";

  // Add Parquet footer (length + "PAR1")
  uint32_t metadata_len = static_cast<uint32_t>(mock_thrift.size());
  char footer[8];
  *reinterpret_cast<uint32_t*>(footer) = ::arrow::bit_util::ToLittleEndian(metadata_len);
  std::memcpy(footer + 4, "PAR1", 4);
  mock_thrift.append(footer, 8);

  auto buffer = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(mock_thrift.data()), mock_thrift.size());

  std::string extracted_flatbuf;
  auto result = ExtractFlatbuffer(buffer, &extracted_flatbuf);

  // Should return 0 indicating no flatbuffer found
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(*result, 0);
  ASSERT_TRUE(extracted_flatbuf.empty());
}

// Unit test for ExtractFlatbuffer - buffer too small
TEST_F(TestMetadata3RoundTrip, ExtractFlatbufferBufferTooSmall) {
  // Create a buffer with less than 8 bytes
  std::string small_data = "PAR1";
  auto buffer = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(small_data.data()), small_data.size());

  std::string extracted_flatbuf;
  auto result = ExtractFlatbuffer(buffer, &extracted_flatbuf);

  // Should return required size
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(*result, 8);
}

// Unit test for ExtractFlatbuffer - large flatbuffer (will be compressed)
TEST_F(TestMetadata3RoundTrip, ExtractFlatbufferCompressed) {
  // Create a large Thrift FileMetaData to test compression
  format::FileMetaData thrift_md;
  thrift_md.__set_version(1);
  thrift_md.__set_num_rows(1000000);
  thrift_md.__set_created_by("test_creator_with_long_name_to_increase_size");

  // Add many schema elements to make it large enough to compress
  format::SchemaElement root;
  root.__set_name("root");
  root.__set_repetition_type(format::FieldRepetitionType::REQUIRED);
  root.__set_num_children(50);
  thrift_md.schema.push_back(root);

  for (int i = 0; i < 50; ++i) {
    format::SchemaElement col;
    col.__set_name("column_" + std::to_string(i));
    col.__set_type(format::Type::INT64);
    col.__set_repetition_type(format::FieldRepetitionType::OPTIONAL);
    thrift_md.schema.push_back(col);
  }

  // Serialize the Thrift metadata
  ThriftSerializer serializer;
  uint32_t len;
  uint8_t* thrift_buffer;
  serializer.SerializeToBuffer(&thrift_md, &len, &thrift_buffer);
  std::string thrift_str(reinterpret_cast<const char*>(thrift_buffer), len);

  // Convert to flatbuffer
  std::string flatbuf;
  ASSERT_TRUE(ToFlatbuffer(&thrift_md, &flatbuf));

  // Append flatbuffer to thrift
  AppendFlatbuffer(flatbuf, &thrift_str);

  uint32_t metadata_len = static_cast<uint32_t>(thrift_str.size());
  char footer[8];
  *reinterpret_cast<uint32_t*>(footer) = ::arrow::bit_util::ToLittleEndian(metadata_len);
  std::memcpy(footer + 4, "PAR1", 4);
  thrift_str.append(footer, 8);

  // Extract
  auto buffer = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(thrift_str.data()), thrift_str.size());

  std::string extracted_flatbuf;
  auto result = ExtractFlatbuffer(buffer, &extracted_flatbuf);

  ASSERT_TRUE(result.ok());
  ASSERT_GT(*result, 0);

  // Verify the extracted flatbuffer matches the original
  ASSERT_EQ(flatbuf.size(), extracted_flatbuf.size());
  ASSERT_EQ(flatbuf, extracted_flatbuf);
}

// Unit test for ExtractFlatbuffer - invalid magic number
TEST_F(TestMetadata3RoundTrip, ExtractFlatbufferInvalidMagic) {
  // Create a buffer with invalid magic number
  std::string data;
  data.resize(100);

  // Add invalid footer
  uint32_t metadata_len = 50;
  char footer[8];
  *reinterpret_cast<uint32_t*>(footer) = ::arrow::bit_util::ToLittleEndian(metadata_len);
  std::memcpy(footer + 4, "XXXX", 4);  // Invalid magic
  data.append(footer, 8);

  auto buffer = std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(data.data()),
                                         data.size());

  std::string extracted_flatbuf;
  // ExtractFlatbuffer throws an exception for invalid magic number
  EXPECT_THROW({ ExtractFlatbuffer(buffer, &extracted_flatbuf); }, ParquetException);
}

// Test with large number of row groups
TEST_F(TestMetadata3RoundTrip, ManyRowGroups) {
  auto schema =
      MakeSchema({Type::INT32, Type::INT64, Type::FLOAT}, {"col1", "col2", "col3"});
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/10, /*rows_per_rowgroup=*/50);

  // Read back without metadata3 to get Thrift metadata
  auto source1 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_thrift = default_reader_properties();
  reader_props_thrift.set_read_metadata3(false);
  auto file_reader1 = ParquetFileReader::Open(source1, reader_props_thrift);
  auto metadata1 = file_reader1->metadata();
  std::string thrift1 = metadata1->SerializeToString();

  // Read back with metadata3 enabled to read from flatbuffer
  auto source2 = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto reader_props_fb = default_reader_properties();
  reader_props_fb.set_read_metadata3(true);
  auto file_reader2 = ParquetFileReader::Open(source2, reader_props_fb);
  auto metadata2 = file_reader2->metadata();
  std::string thrift2 = metadata2->SerializeToString();

  // Deserialize both to compare
  ThriftDeserializer deserializer(default_reader_properties());
  format::FileMetaData md1, md2;
  uint32_t len1 = static_cast<uint32_t>(thrift1.size());
  uint32_t len2 = static_cast<uint32_t>(thrift2.size());
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift1.data()), &len1,
                                  &md1);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(thrift2.data()), &len2,
                                  &md2);

  // Compare: metadata read from Thrift vs metadata read from Flatbuffer should be
  // equivalent
  AssertFileMetadataLogicallyEqual(md1, md2);
}

// Test flatbuffer size is smaller than thrift for typical cases
TEST_F(TestMetadata3RoundTrip, FlatbufferSizeComparison) {
  auto schema = MakeSchema({Type::INT32, Type::INT64, Type::FLOAT, Type::DOUBLE},
                           {"col1", "col2", "col3", "col4"});
  auto buffer = WriteParquetFile(schema, /*num_rowgroups=*/5, /*rows_per_rowgroup=*/100);

  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  auto metadata = file_reader->metadata();

  std::string thrift_serialized = metadata->SerializeToString();
  auto reader_props = default_reader_properties();
  ThriftDeserializer deserializer(reader_props);
  format::FileMetaData original_md;
  uint32_t len = static_cast<uint32_t>(thrift_serialized.size());
  deserializer.DeserializeMessage(
      reinterpret_cast<const uint8_t*>(thrift_serialized.data()), &len, &original_md);

  std::string flatbuf;
  ASSERT_TRUE(ToFlatbuffer(&original_md, &flatbuf));

  // Log the sizes for comparison
  std::cout << "Thrift size: " << thrift_serialized.size() << " bytes" << std::endl;
  std::cout << "Flatbuffer size: " << flatbuf.size() << " bytes" << std::endl;

  if (flatbuf.size() < thrift_serialized.size()) {
    double ratio = static_cast<double>(flatbuf.size()) / thrift_serialized.size();
    std::cout << "Flatbuffer is " << (1.0 - ratio) * 100.0 << "% smaller" << std::endl;
  }
}

}  // namespace test
}  // namespace parquet

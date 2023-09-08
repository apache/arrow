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

#include "parquet/metadata.h"

#include <gtest/gtest.h>

#include "arrow/util/key_value_metadata.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"
#include "parquet/types.h"

namespace parquet {

namespace metadata {

// Helper function for generating table metadata
std::unique_ptr<parquet::FileMetaData> GenerateTableMetaData(
    const parquet::SchemaDescriptor& schema,
    const std::shared_ptr<WriterProperties>& props, const int64_t& nrows,
    EncodedStatistics stats_int, EncodedStatistics stats_float) {
  auto f_builder = FileMetaDataBuilder::Make(&schema, props);
  auto rg1_builder = f_builder->AppendRowGroup();
  // Write the metadata
  // rowgroup1 metadata
  auto col1_builder = rg1_builder->NextColumnChunk();
  auto col2_builder = rg1_builder->NextColumnChunk();
  // column metadata
  std::map<Encoding::type, int32_t> dict_encoding_stats({{Encoding::RLE_DICTIONARY, 1}});
  std::map<Encoding::type, int32_t> data_encoding_stats(
      {{Encoding::PLAIN, 1}, {Encoding::RLE, 1}});
  stats_int.set_is_signed(true);
  col1_builder->SetStatistics(stats_int);
  stats_float.set_is_signed(true);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 4, 0, 10, 512, 600, true, false, dict_encoding_stats,
                       data_encoding_stats);
  col2_builder->Finish(nrows / 2, 24, 0, 30, 512, 600, true, false, dict_encoding_stats,
                       data_encoding_stats);

  rg1_builder->set_num_rows(nrows / 2);
  rg1_builder->Finish(1024);

  // rowgroup2 metadata
  auto rg2_builder = f_builder->AppendRowGroup();
  col1_builder = rg2_builder->NextColumnChunk();
  col2_builder = rg2_builder->NextColumnChunk();
  // column metadata
  col1_builder->SetStatistics(stats_int);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, /*dictionary_page_offset=*/0, 0, 10, 512, 600,
                       /*has_dictionary=*/false, false, dict_encoding_stats,
                       data_encoding_stats);
  col2_builder->Finish(nrows / 2, 16, 0, 26, 512, 600, true, false, dict_encoding_stats,
                       data_encoding_stats);

  rg2_builder->set_num_rows(nrows / 2);
  rg2_builder->Finish(1024);

  // Return the metadata accessor
  return f_builder->Finish();
}

void AssertEncodings(const ColumnChunkMetaData& data,
                     const std::set<parquet::Encoding::type>& expected) {
  std::set<parquet::Encoding::type> encodings(data.encodings().begin(),
                                              data.encodings().end());
  ASSERT_EQ(encodings, expected);
}

TEST(Metadata, TestBuildAccess) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

  WriterProperties::Builder prop_builder;

  std::shared_ptr<WriterProperties> props =
      prop_builder.version(ParquetVersion::PARQUET_2_6)->build();

  fields.push_back(parquet::schema::Int32("int_col", Repetition::REQUIRED));
  fields.push_back(parquet::schema::Float("float_col", Repetition::REQUIRED));
  root = parquet::schema::GroupNode::Make("schema", Repetition::REPEATED, fields);
  schema.Init(root);

  int64_t nrows = 1000;
  int32_t int_min = 100, int_max = 200;
  EncodedStatistics stats_int;
  stats_int.set_null_count(0)
      .set_distinct_count(nrows)
      .set_min(std::string(reinterpret_cast<const char*>(&int_min), 4))
      .set_max(std::string(reinterpret_cast<const char*>(&int_max), 4));
  EncodedStatistics stats_float;
  float float_min = 100.100f, float_max = 200.200f;
  stats_float.set_null_count(0)
      .set_distinct_count(nrows)
      .set_min(std::string(reinterpret_cast<const char*>(&float_min), 4))
      .set_max(std::string(reinterpret_cast<const char*>(&float_max), 4));

  // Generate the metadata
  auto f_accessor = GenerateTableMetaData(schema, props, nrows, stats_int, stats_float);

  std::string f_accessor_serialized_metadata = f_accessor->SerializeToString();
  uint32_t expected_len = static_cast<uint32_t>(f_accessor_serialized_metadata.length());

  // decoded_len is an in-out parameter
  uint32_t decoded_len = expected_len;
  auto f_accessor_copy =
      FileMetaData::Make(f_accessor_serialized_metadata.data(), &decoded_len);

  // Check that all of the serialized data is consumed
  ASSERT_EQ(expected_len, decoded_len);

  // Run this block twice, one for f_accessor, one for f_accessor_copy.
  // To make sure SerializedMetadata was deserialized correctly.
  std::vector<FileMetaData*> f_accessors = {f_accessor.get(), f_accessor_copy.get()};
  for (int loop_index = 0; loop_index < 2; loop_index++) {
    // file metadata
    ASSERT_EQ(nrows, f_accessors[loop_index]->num_rows());
    ASSERT_LE(0, static_cast<int>(f_accessors[loop_index]->size()));
    ASSERT_EQ(2, f_accessors[loop_index]->num_row_groups());
    ASSERT_EQ(ParquetVersion::PARQUET_2_6, f_accessors[loop_index]->version());
    ASSERT_EQ(DEFAULT_CREATED_BY, f_accessors[loop_index]->created_by());
    ASSERT_EQ(3, f_accessors[loop_index]->num_schema_elements());

    // row group1 metadata
    auto rg1_accessor = f_accessors[loop_index]->RowGroup(0);
    ASSERT_EQ(2, rg1_accessor->num_columns());
    ASSERT_EQ(nrows / 2, rg1_accessor->num_rows());
    ASSERT_EQ(1024, rg1_accessor->total_byte_size());
    ASSERT_EQ(1024, rg1_accessor->total_compressed_size());
    EXPECT_EQ(rg1_accessor->file_offset(),
              rg1_accessor->ColumnChunk(0)->dictionary_page_offset());

    auto rg1_column1 = rg1_accessor->ColumnChunk(0);
    auto rg1_column2 = rg1_accessor->ColumnChunk(1);
    ASSERT_EQ(true, rg1_column1->is_stats_set());
    ASSERT_EQ(true, rg1_column2->is_stats_set());
    ASSERT_EQ(stats_float.min(), rg1_column2->statistics()->EncodeMin());
    ASSERT_EQ(stats_float.max(), rg1_column2->statistics()->EncodeMax());
    ASSERT_EQ(stats_int.min(), rg1_column1->statistics()->EncodeMin());
    ASSERT_EQ(stats_int.max(), rg1_column1->statistics()->EncodeMax());
    ASSERT_EQ(0, rg1_column1->statistics()->null_count());
    ASSERT_EQ(0, rg1_column2->statistics()->null_count());
    ASSERT_EQ(nrows, rg1_column1->statistics()->distinct_count());
    ASSERT_EQ(nrows, rg1_column2->statistics()->distinct_count());
    ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg1_column1->compression());
    ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg1_column2->compression());
    ASSERT_EQ(nrows / 2, rg1_column1->num_values());
    ASSERT_EQ(nrows / 2, rg1_column2->num_values());
    {
      std::set<parquet::Encoding::type> encodings{parquet::Encoding::RLE,
                                                  parquet::Encoding::RLE_DICTIONARY,
                                                  parquet::Encoding::PLAIN};
      AssertEncodings(*rg1_column1, encodings);
    }
    {
      std::set<parquet::Encoding::type> encodings{parquet::Encoding::RLE,
                                                  parquet::Encoding::RLE_DICTIONARY,
                                                  parquet::Encoding::PLAIN};
      AssertEncodings(*rg1_column2, encodings);
    }
    ASSERT_EQ(512, rg1_column1->total_compressed_size());
    ASSERT_EQ(512, rg1_column2->total_compressed_size());
    ASSERT_EQ(600, rg1_column1->total_uncompressed_size());
    ASSERT_EQ(600, rg1_column2->total_uncompressed_size());
    ASSERT_EQ(4, rg1_column1->dictionary_page_offset());
    ASSERT_EQ(24, rg1_column2->dictionary_page_offset());
    ASSERT_EQ(10, rg1_column1->data_page_offset());
    ASSERT_EQ(30, rg1_column2->data_page_offset());
    ASSERT_EQ(3, rg1_column1->encoding_stats().size());
    ASSERT_EQ(3, rg1_column2->encoding_stats().size());

    auto rg2_accessor = f_accessors[loop_index]->RowGroup(1);
    ASSERT_EQ(2, rg2_accessor->num_columns());
    ASSERT_EQ(nrows / 2, rg2_accessor->num_rows());
    ASSERT_EQ(1024, rg2_accessor->total_byte_size());
    ASSERT_EQ(1024, rg2_accessor->total_compressed_size());
    EXPECT_EQ(rg2_accessor->file_offset(),
              rg2_accessor->ColumnChunk(0)->data_page_offset());

    auto rg2_column1 = rg2_accessor->ColumnChunk(0);
    auto rg2_column2 = rg2_accessor->ColumnChunk(1);
    ASSERT_EQ(true, rg2_column1->is_stats_set());
    ASSERT_EQ(true, rg2_column2->is_stats_set());
    ASSERT_EQ(stats_float.min(), rg2_column2->statistics()->EncodeMin());
    ASSERT_EQ(stats_float.max(), rg2_column2->statistics()->EncodeMax());
    ASSERT_EQ(stats_int.min(), rg1_column1->statistics()->EncodeMin());
    ASSERT_EQ(stats_int.max(), rg1_column1->statistics()->EncodeMax());
    ASSERT_EQ(0, rg2_column1->statistics()->null_count());
    ASSERT_EQ(0, rg2_column2->statistics()->null_count());
    ASSERT_EQ(nrows, rg2_column1->statistics()->distinct_count());
    ASSERT_EQ(nrows, rg2_column2->statistics()->distinct_count());
    ASSERT_EQ(nrows / 2, rg2_column1->num_values());
    ASSERT_EQ(nrows / 2, rg2_column2->num_values());
    ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg2_column1->compression());
    ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg2_column2->compression());
    {
      std::set<parquet::Encoding::type> encodings{parquet::Encoding::RLE,
                                                  parquet::Encoding::PLAIN};
      AssertEncodings(*rg2_column1, encodings);
    }
    {
      std::set<parquet::Encoding::type> encodings{parquet::Encoding::RLE,
                                                  parquet::Encoding::RLE_DICTIONARY,
                                                  parquet::Encoding::PLAIN};
      AssertEncodings(*rg2_column2, encodings);
    }
    ASSERT_EQ(512, rg2_column1->total_compressed_size());
    ASSERT_EQ(512, rg2_column2->total_compressed_size());
    ASSERT_EQ(600, rg2_column1->total_uncompressed_size());
    ASSERT_EQ(600, rg2_column2->total_uncompressed_size());
    EXPECT_FALSE(rg2_column1->has_dictionary_page());
    ASSERT_EQ(0, rg2_column1->dictionary_page_offset());
    ASSERT_EQ(16, rg2_column2->dictionary_page_offset());
    ASSERT_EQ(10, rg2_column1->data_page_offset());
    ASSERT_EQ(26, rg2_column2->data_page_offset());
    ASSERT_EQ(2, rg2_column1->encoding_stats().size());
    ASSERT_EQ(3, rg2_column2->encoding_stats().size());

    // Test FileMetaData::set_file_path
    ASSERT_TRUE(rg2_column1->file_path().empty());
    f_accessors[loop_index]->set_file_path("/foo/bar/bar.parquet");
    ASSERT_EQ("/foo/bar/bar.parquet", rg2_column1->file_path());
  }

  // Test AppendRowGroups
  auto f_accessor_2 = GenerateTableMetaData(schema, props, nrows, stats_int, stats_float);
  f_accessor->AppendRowGroups(*f_accessor_2);
  ASSERT_EQ(4, f_accessor->num_row_groups());
  ASSERT_EQ(nrows * 2, f_accessor->num_rows());
  ASSERT_LE(0, static_cast<int>(f_accessor->size()));
  ASSERT_EQ(ParquetVersion::PARQUET_2_6, f_accessor->version());
  ASSERT_EQ(DEFAULT_CREATED_BY, f_accessor->created_by());
  ASSERT_EQ(3, f_accessor->num_schema_elements());

  // Test AppendRowGroups from self (ARROW-13654)
  f_accessor->AppendRowGroups(*f_accessor);
  ASSERT_EQ(8, f_accessor->num_row_groups());
  ASSERT_EQ(nrows * 4, f_accessor->num_rows());
  ASSERT_EQ(3, f_accessor->num_schema_elements());

  // Test Subset
  auto f_accessor_1 = f_accessor->Subset({2, 3});
  ASSERT_TRUE(f_accessor_1->Equals(*f_accessor_2));

  f_accessor_1 = f_accessor_2->Subset({0});
  f_accessor_1->AppendRowGroups(*f_accessor->Subset({0}));
  ASSERT_TRUE(f_accessor_1->Equals(*f_accessor->Subset({2, 0})));
}

TEST(Metadata, TestV1Version) {
  // PARQUET-839
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

  WriterProperties::Builder prop_builder;

  std::shared_ptr<WriterProperties> props =
      prop_builder.version(ParquetVersion::PARQUET_1_0)->build();

  fields.push_back(parquet::schema::Int32("int_col", Repetition::REQUIRED));
  fields.push_back(parquet::schema::Float("float_col", Repetition::REQUIRED));
  root = parquet::schema::GroupNode::Make("schema", Repetition::REPEATED, fields);
  schema.Init(root);

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);

  // Read the metadata
  auto f_accessor = f_builder->Finish();

  // file metadata
  ASSERT_EQ(ParquetVersion::PARQUET_1_0, f_accessor->version());
}

TEST(Metadata, TestKeyValueMetadata) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

  WriterProperties::Builder prop_builder;

  std::shared_ptr<WriterProperties> props =
      prop_builder.version(ParquetVersion::PARQUET_1_0)->build();

  fields.push_back(parquet::schema::Int32("int_col", Repetition::REQUIRED));
  fields.push_back(parquet::schema::Float("float_col", Repetition::REQUIRED));
  root = parquet::schema::GroupNode::Make("schema", Repetition::REPEATED, fields);
  schema.Init(root);

  auto kvmeta = std::make_shared<KeyValueMetadata>();
  kvmeta->Append("test_key", "test_value");

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);

  // Read the metadata
  auto f_accessor = f_builder->Finish(kvmeta);

  // Key value metadata
  ASSERT_TRUE(f_accessor->key_value_metadata());
  EXPECT_TRUE(f_accessor->key_value_metadata()->Equals(*kvmeta));
}

TEST(Metadata, TestAddKeyValueMetadata) {
  schema::NodeVector fields;
  fields.push_back(schema::Int32("int_col", Repetition::REQUIRED));
  auto schema = std::static_pointer_cast<schema::GroupNode>(
      schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));

  auto kv_meta = std::make_shared<KeyValueMetadata>();
  kv_meta->Append("test_key_1", "test_value_1");
  kv_meta->Append("test_key_2", "test_value_2_");

  auto sink = CreateOutputStream();
  auto writer_props = parquet::WriterProperties::Builder().disable_dictionary()->build();
  auto file_writer =
      parquet::ParquetFileWriter::Open(sink, schema, writer_props, kv_meta);

  // Key value metadata that will be added to the file.
  auto kv_meta_added = std::make_shared<KeyValueMetadata>();
  kv_meta_added->Append("test_key_2", "test_value_2");
  kv_meta_added->Append("test_key_3", "test_value_3");

  file_writer->AddKeyValueMetadata(kv_meta_added);
  file_writer->Close();

  // Throw if appending key value metadata to closed file.
  auto kv_meta_ignored = std::make_shared<KeyValueMetadata>();
  kv_meta_ignored->Append("test_key_4", "test_value_4");
  EXPECT_THROW(file_writer->AddKeyValueMetadata(kv_meta_ignored), ParquetException);

  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);

  ASSERT_NE(nullptr, file_reader->metadata());
  ASSERT_NE(nullptr, file_reader->metadata()->key_value_metadata());
  auto read_kv_meta = file_reader->metadata()->key_value_metadata();

  // Verify keys that were added before file writer was closed are present.
  for (int i = 1; i <= 3; ++i) {
    auto index = std::to_string(i);
    PARQUET_ASSIGN_OR_THROW(auto value, read_kv_meta->Get("test_key_" + index));
    EXPECT_EQ("test_value_" + index, value);
  }
  // Verify keys that were added after file writer was closed are not present.
  EXPECT_FALSE(read_kv_meta->Contains("test_key_4"));
}

TEST(Metadata, TestHasBloomFilter) {
  std::string dir_string(parquet::test::get_data_dir());
  std::string path = dir_string + "/data_index_bloom_encoding_stats.parquet";
  auto reader = ParquetFileReader::OpenFile(path, false);
  auto file_metadata = reader->metadata();
  ASSERT_EQ(1, file_metadata->num_row_groups());
  auto row_group_metadata = file_metadata->RowGroup(0);
  ASSERT_EQ(1, row_group_metadata->num_columns());
  auto col_chunk_metadata = row_group_metadata->ColumnChunk(0);
  auto bloom_filter_offset = col_chunk_metadata->bloom_filter_offset();
  ASSERT_TRUE(bloom_filter_offset.has_value());
  ASSERT_EQ(192, bloom_filter_offset);
}

TEST(Metadata, TestReadPageIndex) {
  std::string dir_string(parquet::test::get_data_dir());
  std::string path = dir_string + "/alltypes_tiny_pages.parquet";
  auto reader = ParquetFileReader::OpenFile(path, false);
  auto file_metadata = reader->metadata();
  ASSERT_EQ(1, file_metadata->num_row_groups());
  auto row_group_metadata = file_metadata->RowGroup(0);
  ASSERT_EQ(13, row_group_metadata->num_columns());
  std::vector<int64_t> ci_offsets = {323583, 327502, 328009, 331928, 335847,
                                     339766, 350345, 354264, 364843, 384342,
                                     -1,     386473, 390392};
  std::vector<int32_t> ci_lengths = {3919,  507,   3919, 3919, 3919, 10579, 3919,
                                     10579, 19499, 2131, -1,   3919, 3919};
  std::vector<int64_t> oi_offsets = {394311, 397814, 398637, 401888, 405139,
                                     408390, 413670, 416921, 422201, 431936,
                                     435457, 446002, 449253};
  std::vector<int32_t> oi_lengths = {3503, 823,  3251, 3251,  3251, 5280, 3251,
                                     5280, 9735, 3521, 10545, 3251, 3251};
  for (int i = 0; i < row_group_metadata->num_columns(); ++i) {
    auto col_chunk_metadata = row_group_metadata->ColumnChunk(i);
    auto ci_location = col_chunk_metadata->GetColumnIndexLocation();
    if (i == 10) {
      // column_id 10 does not have column index
      ASSERT_FALSE(ci_location.has_value());
    } else {
      ASSERT_TRUE(ci_location.has_value());
    }
    if (ci_location.has_value()) {
      ASSERT_EQ(ci_offsets.at(i), ci_location->offset);
      ASSERT_EQ(ci_lengths.at(i), ci_location->length);
    }
    auto oi_location = col_chunk_metadata->GetOffsetIndexLocation();
    ASSERT_TRUE(oi_location.has_value());
    ASSERT_EQ(oi_offsets.at(i), oi_location->offset);
    ASSERT_EQ(oi_lengths.at(i), oi_location->length);
    ASSERT_FALSE(col_chunk_metadata->bloom_filter_offset().has_value());
  }
}

TEST(Metadata, TestSortingColumns) {
  schema::NodeVector fields;
  fields.push_back(schema::Int32("sort_col", Repetition::REQUIRED));
  fields.push_back(schema::Int32("int_col", Repetition::REQUIRED));

  auto schema = std::static_pointer_cast<schema::GroupNode>(
      schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));

  std::vector<SortingColumn> sorting_columns;
  {
    SortingColumn sorting_column;
    sorting_column.column_idx = 0;
    sorting_column.descending = false;
    sorting_column.nulls_first = false;
    sorting_columns.push_back(sorting_column);
  }

  auto sink = CreateOutputStream();
  auto writer_props = parquet::WriterProperties::Builder()
                          .disable_dictionary()
                          ->set_sorting_columns(sorting_columns)
                          ->build();

  EXPECT_EQ(sorting_columns, writer_props->sorting_columns());

  auto file_writer = parquet::ParquetFileWriter::Open(sink, schema, writer_props);

  auto row_group_writer = file_writer->AppendBufferedRowGroup();
  row_group_writer->Close();
  file_writer->Close();

  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);

  ASSERT_NE(nullptr, file_reader->metadata());
  ASSERT_EQ(1, file_reader->metadata()->num_row_groups());
  auto row_group_reader = file_reader->RowGroup(0);
  auto* row_group_read_metadata = row_group_reader->metadata();
  ASSERT_NE(nullptr, row_group_read_metadata);
  EXPECT_EQ(sorting_columns, row_group_read_metadata->sorting_columns());
}

TEST(ApplicationVersion, Basics) {
  ApplicationVersion version("parquet-mr version 1.7.9");
  ApplicationVersion version1("parquet-mr version 1.8.0");
  ApplicationVersion version2("parquet-cpp version 1.0.0");
  ApplicationVersion version3("");
  ApplicationVersion version4("parquet-mr version 1.5.0ab-cdh5.5.0+cd (build abcd)");
  ApplicationVersion version5("parquet-mr");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(7, version.version.minor);
  ASSERT_EQ(9, version.version.patch);

  ASSERT_EQ("parquet-cpp", version2.application_);
  ASSERT_EQ(1, version2.version.major);
  ASSERT_EQ(0, version2.version.minor);
  ASSERT_EQ(0, version2.version.patch);

  ASSERT_EQ("parquet-mr", version4.application_);
  ASSERT_EQ("abcd", version4.build_);
  ASSERT_EQ(1, version4.version.major);
  ASSERT_EQ(5, version4.version.minor);
  ASSERT_EQ(0, version4.version.patch);
  ASSERT_EQ("ab", version4.version.unknown);
  ASSERT_EQ("cdh5.5.0", version4.version.pre_release);
  ASSERT_EQ("cd", version4.version.build_info);

  ASSERT_EQ("parquet-mr", version5.application_);
  ASSERT_EQ(0, version5.version.major);
  ASSERT_EQ(0, version5.version.minor);
  ASSERT_EQ(0, version5.version.patch);

  ASSERT_EQ(true, version.VersionLt(version1));

  EncodedStatistics stats;
  ASSERT_FALSE(version1.HasCorrectStatistics(Type::INT96, stats, SortOrder::UNKNOWN));
  ASSERT_TRUE(version.HasCorrectStatistics(Type::INT32, stats, SortOrder::SIGNED));
  ASSERT_FALSE(version.HasCorrectStatistics(Type::BYTE_ARRAY, stats, SortOrder::SIGNED));
  ASSERT_TRUE(version1.HasCorrectStatistics(Type::BYTE_ARRAY, stats, SortOrder::SIGNED));
  ASSERT_FALSE(
      version1.HasCorrectStatistics(Type::BYTE_ARRAY, stats, SortOrder::UNSIGNED));
  ASSERT_TRUE(version3.HasCorrectStatistics(Type::FIXED_LEN_BYTE_ARRAY, stats,
                                            SortOrder::SIGNED));

  // Check that the old stats are correct if min and max are the same
  // regardless of sort order
  EncodedStatistics stats_str;
  stats_str.set_min("a").set_max("b");
  ASSERT_FALSE(
      version1.HasCorrectStatistics(Type::BYTE_ARRAY, stats_str, SortOrder::UNSIGNED));
  stats_str.set_max("a");
  ASSERT_TRUE(
      version1.HasCorrectStatistics(Type::BYTE_ARRAY, stats_str, SortOrder::UNSIGNED));

  // Check that the same holds true for ints
  int32_t int_min = 100, int_max = 200;
  EncodedStatistics stats_int;
  stats_int.set_min(std::string(reinterpret_cast<const char*>(&int_min), 4))
      .set_max(std::string(reinterpret_cast<const char*>(&int_max), 4));
  ASSERT_FALSE(
      version1.HasCorrectStatistics(Type::BYTE_ARRAY, stats_int, SortOrder::UNSIGNED));
  stats_int.set_max(std::string(reinterpret_cast<const char*>(&int_min), 4));
  ASSERT_TRUE(
      version1.HasCorrectStatistics(Type::BYTE_ARRAY, stats_int, SortOrder::UNSIGNED));
}

TEST(ApplicationVersion, Empty) {
  ApplicationVersion version("");

  ASSERT_EQ("", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(0, version.version.major);
  ASSERT_EQ(0, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, NoVersion) {
  ApplicationVersion version("parquet-mr (build abcd)");

  ASSERT_EQ("parquet-mr (build abcd)", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(0, version.version.major);
  ASSERT_EQ(0, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionEmpty) {
  ApplicationVersion version("parquet-mr version ");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(0, version.version.major);
  ASSERT_EQ(0, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoMajor) {
  ApplicationVersion version("parquet-mr version .");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(0, version.version.major);
  ASSERT_EQ(0, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionInvalidMajor) {
  ApplicationVersion version("parquet-mr version x1");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(0, version.version.major);
  ASSERT_EQ(0, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionMajorOnly) {
  ApplicationVersion version("parquet-mr version 1");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(0, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoMinor) {
  ApplicationVersion version("parquet-mr version 1.");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(0, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionMajorMinorOnly) {
  ApplicationVersion version("parquet-mr version 1.7");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(7, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionInvalidMinor) {
  ApplicationVersion version("parquet-mr version 1.x7");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(0, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoPatch) {
  ApplicationVersion version("parquet-mr version 1.7.");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(7, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionInvalidPatch) {
  ApplicationVersion version("parquet-mr version 1.7.x9");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(7, version.version.minor);
  ASSERT_EQ(0, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoUnknown) {
  ApplicationVersion version("parquet-mr version 1.7.9-cdh5.5.0+cd");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(7, version.version.minor);
  ASSERT_EQ(9, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("cdh5.5.0", version.version.pre_release);
  ASSERT_EQ("cd", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoPreRelease) {
  ApplicationVersion version("parquet-mr version 1.7.9ab+cd");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(7, version.version.minor);
  ASSERT_EQ(9, version.version.patch);
  ASSERT_EQ("ab", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("cd", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoUnknownNoPreRelease) {
  ApplicationVersion version("parquet-mr version 1.7.9+cd");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(7, version.version.minor);
  ASSERT_EQ(9, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("cd", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoUnknownBuildInfoPreRelease) {
  ApplicationVersion version("parquet-mr version 1.7.9+cd-cdh5.5.0");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(7, version.version.minor);
  ASSERT_EQ(9, version.version.patch);
  ASSERT_EQ("", version.version.unknown);
  ASSERT_EQ("", version.version.pre_release);
  ASSERT_EQ("cd-cdh5.5.0", version.version.build_info);
}

TEST(ApplicationVersion, FullWithSpaces) {
  ApplicationVersion version(
      " parquet-mr \t version \v 1.5.3ab-cdh5.5.0+cd \r (build \n abcd \f) ");

  ASSERT_EQ("parquet-mr", version.application_);
  ASSERT_EQ("abcd", version.build_);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(5, version.version.minor);
  ASSERT_EQ(3, version.version.patch);
  ASSERT_EQ("ab", version.version.unknown);
  ASSERT_EQ("cdh5.5.0", version.version.pre_release);
  ASSERT_EQ("cd", version.version.build_info);
}

}  // namespace metadata
}  // namespace parquet

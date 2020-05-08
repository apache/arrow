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

#include "parquet/schema.h"
#include "parquet/statistics.h"
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
  col1_builder->Finish(nrows / 2, 6, 0, 10, 512, 600, true, false, dict_encoding_stats,
                       data_encoding_stats);
  col2_builder->Finish(nrows / 2, 16, 0, 26, 512, 600, true, false, dict_encoding_stats,
                       data_encoding_stats);

  rg2_builder->set_num_rows(nrows / 2);
  rg2_builder->Finish(1024);

  // Return the metadata accessor
  return f_builder->Finish();
}

TEST(Metadata, TestBuildAccess) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

  WriterProperties::Builder prop_builder;

  std::shared_ptr<WriterProperties> props =
      prop_builder.version(ParquetVersion::PARQUET_2_0)->build();

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
    ASSERT_EQ(ParquetVersion::PARQUET_2_0, f_accessors[loop_index]->version());
    ASSERT_EQ(DEFAULT_CREATED_BY, f_accessors[loop_index]->created_by());
    ASSERT_EQ(3, f_accessors[loop_index]->num_schema_elements());

    // row group1 metadata
    auto rg1_accessor = f_accessors[loop_index]->RowGroup(0);
    ASSERT_EQ(2, rg1_accessor->num_columns());
    ASSERT_EQ(nrows / 2, rg1_accessor->num_rows());
    ASSERT_EQ(1024, rg1_accessor->total_byte_size());

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
    ASSERT_EQ(3, rg1_column1->encodings().size());
    ASSERT_EQ(3, rg1_column2->encodings().size());
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
    ASSERT_EQ(3, rg2_column1->encodings().size());
    ASSERT_EQ(3, rg2_column2->encodings().size());
    ASSERT_EQ(512, rg2_column1->total_compressed_size());
    ASSERT_EQ(512, rg2_column2->total_compressed_size());
    ASSERT_EQ(600, rg2_column1->total_uncompressed_size());
    ASSERT_EQ(600, rg2_column2->total_uncompressed_size());
    ASSERT_EQ(6, rg2_column1->dictionary_page_offset());
    ASSERT_EQ(16, rg2_column2->dictionary_page_offset());
    ASSERT_EQ(10, rg2_column1->data_page_offset());
    ASSERT_EQ(26, rg2_column2->data_page_offset());
    ASSERT_EQ(3, rg2_column1->encoding_stats().size());
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
  ASSERT_EQ(ParquetVersion::PARQUET_2_0, f_accessor->version());
  ASSERT_EQ(DEFAULT_CREATED_BY, f_accessor->created_by());
  ASSERT_EQ(3, f_accessor->num_schema_elements());
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

TEST(ApplicationVersion, Basics) {
  ApplicationVersion version("parquet-mr version 1.7.9");
  ApplicationVersion version1("parquet-mr version 1.8.0");
  ApplicationVersion version2("parquet-cpp version 1.0.0");
  ApplicationVersion version3("");
  // ApplicationVersion version4("parquet-mr version 1.5.0ab-cdh5.5.0+cd (build abcd)");
  ApplicationVersion version4("parquet-mr version 1.5.0-cdh5.5.0+cd (build abcd)");
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
  // ASSERT_EQ("ab", version4.version.unknown);
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

TEST(ApplicationVersion, ParquetMr) {
  auto v1 = ApplicationVersion("parquet-mr version 1.7.9");
  ASSERT_EQ(v1.application_, "parquet-mr");
  ASSERT_EQ(v1.version.major, 1);
  ASSERT_EQ(v1.version.minor, 7);
  ASSERT_EQ(v1.version.patch, 9);
  ASSERT_EQ(v1.build_, "");

  auto v2 = ApplicationVersion("parquet-mr version 1.6.0 (build abcd)");
  ASSERT_EQ(v2.application_, "parquet-mr");
  ASSERT_EQ(v2.version.major, 1);
  ASSERT_EQ(v2.version.minor, 6);
  ASSERT_EQ(v2.version.patch, 0);
  ASSERT_EQ(v2.build_, "abcd");

  auto v3 = ApplicationVersion("parquet-mr version 1.6.22rc99-SNAPSHOT (build abcd)");
  ASSERT_EQ(v3.application_, "parquet-mr");
  ASSERT_EQ(v3.version.major, 1);
  ASSERT_EQ(v3.version.minor, 6);
  ASSERT_EQ(v3.version.patch, 22);
  ASSERT_EQ(v3.version.unknown, "rc99");
  // TODO(kszucs): perhaps to lowercase should be removed?
  ASSERT_EQ(v3.version.pre_release, "snapshot");
  ASSERT_EQ(v3.build_, "abcd");

  // TODO(kszucs): parquet-mr raises in the following case
  auto v4 = ApplicationVersion("unparseable string");
  ASSERT_EQ(v4.application_, "unparseable string");
  ASSERT_EQ(v4.version.major, 0);
  ASSERT_EQ(v4.version.minor, 0);
  ASSERT_EQ(v4.version.patch, 0);
  ASSERT_EQ(v4.version.unknown, "");
  ASSERT_EQ(v4.version.pre_release, "");
  ASSERT_EQ(v4.build_, "");

  // missing semver
  auto v5 = ApplicationVersion("parquet-mr version (build abcd)");
  ASSERT_EQ(v5.application_, "parquet-mr");
  ASSERT_EQ(v5.version.major, 0);
  ASSERT_EQ(v5.version.minor, 0);
  ASSERT_EQ(v5.version.patch, 0);
  ASSERT_EQ(v5.version.unknown, "");
  ASSERT_EQ(v5.version.pre_release, "");
  ASSERT_EQ(v5.build_, "abcd");

  auto v6 = ApplicationVersion("parquet-mr version  (build abcd)");
  ASSERT_EQ(v6.application_, "parquet-mr");
  ASSERT_EQ(v6.version.major, 0);
  ASSERT_EQ(v6.version.minor, 0);
  ASSERT_EQ(v6.version.patch, 0);
  ASSERT_EQ(v6.version.unknown, "");
  ASSERT_EQ(v6.version.pre_release, "");
  ASSERT_EQ(v6.build_, "abcd");

  // missing build hash
  auto v7 = ApplicationVersion("parquet-mr version 1.6.0 (build )");
  ASSERT_EQ(v7.application_, "parquet-mr");
  ASSERT_EQ(v7.version.major, 1);
  ASSERT_EQ(v7.version.minor, 6);
  ASSERT_EQ(v7.version.patch, 0);
  ASSERT_EQ(v7.version.unknown, "");
  ASSERT_EQ(v7.version.pre_release, "");
  ASSERT_EQ(v7.build_, "");

  ApplicationVersion v8;
  auto cases8 = {"parquet-mr version 1.6.0 (build)",
                 "parquet-mr     version 1.6.0 (    build)",
                 "parquet-mr version 1.6.0 (  build )"};
  for (auto const& s : cases8) {
    auto v8 = ApplicationVersion(s);
    ASSERT_EQ(v8.application_, "parquet-mr");
    ASSERT_EQ(v8.version.major, 1);
    ASSERT_EQ(v8.version.minor, 6);
    ASSERT_EQ(v8.version.patch, 0);
    ASSERT_EQ(v8.version.unknown, "");
    ASSERT_EQ(v8.version.pre_release, "");
    ASSERT_EQ(v8.build_, "");
  }

  ApplicationVersion v9;
  auto cases9 = {"parquet-mr version (build )", "parquet-mr      version"};
  for (auto const& s : cases9) {
    auto v9 = ApplicationVersion(s);
    ASSERT_EQ(v9.application_, "parquet-mr");
    ASSERT_EQ(v9.version.major, 0);
    ASSERT_EQ(v9.version.minor, 0);
    ASSERT_EQ(v9.version.patch, 0);
    ASSERT_EQ(v9.version.unknown, "");
    ASSERT_EQ(v9.version.pre_release, "");
    ASSERT_EQ(v9.build_, "");
  }

  auto v10 = ApplicationVersion("");
  ASSERT_EQ(v10.application_, "unknown");
  ASSERT_EQ(v10.version.major, 0);
  ASSERT_EQ(v10.version.minor, 0);
  ASSERT_EQ(v10.version.patch, 0);
  ASSERT_EQ(v10.version.unknown, "");
  ASSERT_EQ(v10.version.pre_release, "");
  ASSERT_EQ(v10.build_, "");

  // missing entire build section
  ApplicationVersion v11;
  auto cases11 = {"parquet-mr version 1.6.0", "parquet-mr     version    1.6.0"};
  for (auto const& s : cases11) {
    auto v11 = ApplicationVersion(s);
    ASSERT_EQ(v11.application_, "parquet-mr");
    ASSERT_EQ(v11.version.major, 1);
    ASSERT_EQ(v11.version.minor, 6);
    ASSERT_EQ(v11.version.patch, 0);
    ASSERT_EQ(v11.version.unknown, "");
    ASSERT_EQ(v11.version.pre_release, "");
    ASSERT_EQ(v11.build_, "");
  }

  ApplicationVersion v12;
  auto cases12 = {"parquet-mr version 1.8.0rc4", "parquet-mr     version    1.8.0rc4"};
  for (auto const& s : cases12) {
    auto v12 = ApplicationVersion(s);
    ASSERT_EQ(v12.application_, "parquet-mr");
    ASSERT_EQ(v12.version.major, 1);
    ASSERT_EQ(v12.version.minor, 8);
    ASSERT_EQ(v12.version.patch, 0);
    ASSERT_EQ(v12.version.unknown, "rc4");
    ASSERT_EQ(v12.version.pre_release, "");
    ASSERT_EQ(v12.build_, "");
  }

  ApplicationVersion v13;
  auto cases13 = {"parquet-mr version 1.8.0rc4-SNAPSHOT",
                  "parquet-mr      version    1.8.0rc4-SNAPSHOT  "};
  for (auto const& s : cases13) {
    auto v13 = ApplicationVersion(s);
    ASSERT_EQ(v13.application_, "parquet-mr");
    ASSERT_EQ(v13.version.major, 1);
    ASSERT_EQ(v13.version.minor, 8);
    ASSERT_EQ(v13.version.patch, 0);
    ASSERT_EQ(v13.version.unknown, "rc4");
    ASSERT_EQ(v13.version.pre_release, "snapshot");
    ASSERT_EQ(v13.build_, "");
  }

  ApplicationVersion v14;
  auto cases14 = {"parquet-mr version", "parquet-mr     version (    build)",
                  "parquet-mr    version    (build    )"};
  for (auto const& s : cases14) {
    v14 = ApplicationVersion(s);
    ASSERT_EQ(v14.application_, "parquet-mr");
    ASSERT_EQ(v14.version.major, 0);
    ASSERT_EQ(v14.version.minor, 0);
    ASSERT_EQ(v14.version.patch, 0);
    ASSERT_EQ(v14.version.unknown, "");
    ASSERT_EQ(v14.version.pre_release, "");
    ASSERT_EQ(v14.build_, "");
  }
}

}  // namespace metadata
}  // namespace parquet

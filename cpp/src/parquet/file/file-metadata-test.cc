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

#include "parquet/column/statistics.h"
#include "parquet/file/metadata.h"
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/types.h"
#include <gtest/gtest.h>

namespace parquet {

namespace metadata {

TEST(Metadata, TestBuildAccess) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

  std::shared_ptr<WriterProperties> props = WriterProperties::Builder().build();

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
  float float_min = 100.100, float_max = 200.200;
  stats_float.set_null_count(0)
      .set_distinct_count(nrows)
      .set_min(std::string(reinterpret_cast<const char*>(&float_min), 4))
      .set_max(std::string(reinterpret_cast<const char*>(&float_max), 4));

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);
  auto rg1_builder = f_builder->AppendRowGroup(nrows / 2);
  auto rg2_builder = f_builder->AppendRowGroup(nrows / 2);

  // Write the metadata
  // rowgroup1 metadata
  auto col1_builder = rg1_builder->NextColumnChunk();
  auto col2_builder = rg1_builder->NextColumnChunk();
  // column metadata
  col1_builder->SetStatistics(stats_int);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 4, 0, 10, 512, 600, true, false);
  col2_builder->Finish(nrows / 2, 24, 0, 30, 512, 600, true, false);
  rg1_builder->Finish(1024);

  // rowgroup2 metadata
  col1_builder = rg2_builder->NextColumnChunk();
  col2_builder = rg2_builder->NextColumnChunk();
  // column metadata
  col1_builder->SetStatistics(stats_int);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 6, 0, 10, 512, 600, true, false);
  col2_builder->Finish(nrows / 2, 16, 0, 26, 512, 600, true, false);
  rg2_builder->Finish(1024);

  // Read the metadata
  auto f_accessor = f_builder->Finish();

  // file metadata
  ASSERT_EQ(nrows, f_accessor->num_rows());
  ASSERT_LE(0, f_accessor->size());
  ASSERT_EQ(2, f_accessor->num_row_groups());
  ASSERT_EQ(DEFAULT_WRITER_VERSION, f_accessor->version());
  ASSERT_EQ(DEFAULT_CREATED_BY, f_accessor->created_by());
  ASSERT_EQ(3, f_accessor->num_schema_elements());

  // row group1 metadata
  auto rg1_accessor = f_accessor->RowGroup(0);
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
  ASSERT_EQ(2, rg1_column1->encodings().size());
  ASSERT_EQ(2, rg1_column2->encodings().size());
  ASSERT_EQ(512, rg1_column1->total_compressed_size());
  ASSERT_EQ(512, rg1_column2->total_compressed_size());
  ASSERT_EQ(600, rg1_column1->total_uncompressed_size());
  ASSERT_EQ(600, rg1_column2->total_uncompressed_size());
  ASSERT_EQ(4, rg1_column1->dictionary_page_offset());
  ASSERT_EQ(24, rg1_column2->dictionary_page_offset());
  ASSERT_EQ(10, rg1_column1->data_page_offset());
  ASSERT_EQ(30, rg1_column2->data_page_offset());

  auto rg2_accessor = f_accessor->RowGroup(1);
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
  ASSERT_EQ(2, rg2_column1->encodings().size());
  ASSERT_EQ(2, rg2_column2->encodings().size());
  ASSERT_EQ(512, rg2_column1->total_compressed_size());
  ASSERT_EQ(512, rg2_column2->total_compressed_size());
  ASSERT_EQ(600, rg2_column1->total_uncompressed_size());
  ASSERT_EQ(600, rg2_column2->total_uncompressed_size());
  ASSERT_EQ(6, rg2_column1->dictionary_page_offset());
  ASSERT_EQ(16, rg2_column2->dictionary_page_offset());
  ASSERT_EQ(10, rg2_column1->data_page_offset());
  ASSERT_EQ(26, rg2_column2->data_page_offset());
}

TEST(FileVersion, Basics) {
  FileMetaData::Version version("parquet-mr version 1.2.8");

  ASSERT_EQ("parquet-mr", version.application);
  ASSERT_EQ(1, version.version.major);
  ASSERT_EQ(2, version.version.minor);
  ASSERT_EQ(8, version.version.patch);
}

}  // namespace metadata
}  // namespace parquet

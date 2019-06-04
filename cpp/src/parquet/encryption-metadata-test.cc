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

#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"

namespace parquet {

namespace metadata {

const char kFooterEncryptionKey[] = "0123456789012345";  // 128bit/16
const char kColumnEncryptionKey1[] = "1234567890123450";
// const char kColumnEncryptionKey2[] = "1234567890123451";

TEST(Metadata, UniformEncryption) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

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

  FileEncryptionProperties::Builder encryption_prop_builder(kFooterEncryptionKey);
  encryption_prop_builder.footer_key_metadata("kf");

  WriterProperties::Builder writer_prop_builder;
  writer_prop_builder.version(ParquetVersion::PARQUET_2_0);
  writer_prop_builder.encryption(encryption_prop_builder.build());
  auto props = writer_prop_builder.build();

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);
  auto rg1_builder = f_builder->AppendRowGroup();

  // Write the metadata
  // rowgroup1 metadata
  auto col1_builder = rg1_builder->NextColumnChunk();
  auto col2_builder = rg1_builder->NextColumnChunk();
  // column metadata
  stats_int.set_is_signed(true);
  col1_builder->SetStatistics(stats_int);
  stats_float.set_is_signed(true);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 4, 0, 10, 512, 600, true, false);
  col2_builder->Finish(nrows / 2, 24, 0, 30, 512, 600, true, false);

  rg1_builder->set_num_rows(nrows / 2);
  rg1_builder->Finish(1024);

  // rowgroup2 metadata
  auto rg2_builder = f_builder->AppendRowGroup();
  col1_builder = rg2_builder->NextColumnChunk();
  col2_builder = rg2_builder->NextColumnChunk();
  // column metadata
  col1_builder->SetStatistics(stats_int);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 6, 0, 10, 512, 600, true, false);
  col2_builder->Finish(nrows / 2, 16, 0, 26, 512, 600, true, false);

  rg2_builder->set_num_rows(nrows / 2);
  rg2_builder->Finish(1024);

  // Read the metadata
  auto f_accessor = f_builder->Finish();

  ASSERT_EQ(false, f_accessor->is_encryption_algorithm_set());

  auto file_crypto_metadata = f_builder->GetCryptoMetaData();
  ASSERT_EQ(true, file_crypto_metadata != NULLPTR);

  // file metadata
  ASSERT_EQ(nrows, f_accessor->num_rows());
  ASSERT_LE(0, static_cast<int>(f_accessor->size()));
  ASSERT_EQ(2, f_accessor->num_row_groups());
  ASSERT_EQ(ParquetVersion::PARQUET_2_0, f_accessor->version());
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
}

TEST(Metadata, EncryptFooterAndOneColumn) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

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

  std::shared_ptr<parquet::schema::ColumnPath> int_col_path =
      parquet::schema::ColumnPath::FromDotString("int_col");
  ColumnEncryptionProperties::Builder int_col_builder(int_col_path);
  int_col_builder.key(kColumnEncryptionKey1);
  int_col_builder.key_id("kc1");

  std::map<std::shared_ptr<schema::ColumnPath>,
           std::shared_ptr<ColumnEncryptionProperties>, schema::ColumnPath::CmpColumnPath>
      encryption_col_props;
  encryption_col_props[int_col_path] = int_col_builder.build();

  FileEncryptionProperties::Builder encryption_prop_builder(kFooterEncryptionKey);
  encryption_prop_builder.footer_key_metadata("kf");
  encryption_prop_builder.column_properties(encryption_col_props);

  WriterProperties::Builder writer_prop_builder;
  writer_prop_builder.version(ParquetVersion::PARQUET_2_0);
  writer_prop_builder.encryption(encryption_prop_builder.build());
  auto props = writer_prop_builder.build();

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);
  auto rg1_builder = f_builder->AppendRowGroup();

  // Write the metadata
  // rowgroup1 metadata
  auto col1_builder = rg1_builder->NextColumnChunk();
  auto col2_builder = rg1_builder->NextColumnChunk();
  // column metadata
  stats_int.set_is_signed(true);
  col1_builder->SetStatistics(stats_int);
  stats_float.set_is_signed(true);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 4, 0, 10, 512, 600, true, false);
  col2_builder->Finish(nrows / 2, 24, 0, 30, 512, 600, true, false);

  rg1_builder->set_num_rows(nrows / 2);
  rg1_builder->Finish(1024);

  // rowgroup2 metadata
  auto rg2_builder = f_builder->AppendRowGroup();
  col1_builder = rg2_builder->NextColumnChunk();
  col2_builder = rg2_builder->NextColumnChunk();
  // column metadata
  col1_builder->SetStatistics(stats_int);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 6, 0, 10, 512, 600, true, false);
  col2_builder->Finish(nrows / 2, 16, 0, 26, 512, 600, true, false);

  rg2_builder->set_num_rows(nrows / 2);
  rg2_builder->Finish(1024);

  // Read the metadata
  auto f_accessor = f_builder->Finish();

  ASSERT_EQ(false, f_accessor->is_encryption_algorithm_set());

  auto file_crypto_metadata = f_builder->GetCryptoMetaData();
  ASSERT_EQ(true, file_crypto_metadata != NULLPTR);

  // file metadata
  ASSERT_EQ(nrows, f_accessor->num_rows());
  ASSERT_LE(0, static_cast<int>(f_accessor->size()));
  ASSERT_EQ(2, f_accessor->num_row_groups());
  ASSERT_EQ(ParquetVersion::PARQUET_2_0, f_accessor->version());
  ASSERT_EQ(DEFAULT_CREATED_BY, f_accessor->created_by());
  ASSERT_EQ(3, f_accessor->num_schema_elements());

  // row group1 metadata
  auto rg1_accessor = f_accessor->RowGroup(0);
  ASSERT_EQ(2, rg1_accessor->num_columns());
  ASSERT_EQ(nrows / 2, rg1_accessor->num_rows());
  ASSERT_EQ(1024, rg1_accessor->total_byte_size());

  auto rg1_column1 = rg1_accessor->ColumnChunk(0);
  auto rg1_column2 = rg1_accessor->ColumnChunk(1);
  ASSERT_EQ(false, rg1_column1->is_metadata_set());
  ASSERT_THROW(rg1_column1->is_stats_set(), ParquetException);
  ASSERT_THROW(rg1_column1->statistics(), ParquetException);
  ASSERT_THROW(rg1_column1->compression(), ParquetException);
  ASSERT_THROW(rg1_column1->num_values(), ParquetException);
  ASSERT_THROW(rg1_column1->encodings(), ParquetException);
  ASSERT_THROW(rg1_column1->total_compressed_size(), ParquetException);
  ASSERT_THROW(rg1_column1->total_uncompressed_size(), ParquetException);
  ASSERT_THROW(rg1_column1->dictionary_page_offset(), ParquetException);
  ASSERT_THROW(rg1_column1->data_page_offset(), ParquetException);

  ASSERT_EQ(true, rg1_column2->is_stats_set());
  ASSERT_EQ(stats_float.min(), rg1_column2->statistics()->EncodeMin());
  ASSERT_EQ(stats_float.max(), rg1_column2->statistics()->EncodeMax());
  ASSERT_EQ(0, rg1_column2->statistics()->null_count());
  ASSERT_EQ(nrows, rg1_column2->statistics()->distinct_count());
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg1_column2->compression());
  ASSERT_EQ(nrows / 2, rg1_column2->num_values());
  ASSERT_EQ(3, rg1_column2->encodings().size());
  ASSERT_EQ(512, rg1_column2->total_compressed_size());
  ASSERT_EQ(600, rg1_column2->total_uncompressed_size());
  ASSERT_EQ(24, rg1_column2->dictionary_page_offset());
  ASSERT_EQ(30, rg1_column2->data_page_offset());

  auto rg2_accessor = f_accessor->RowGroup(1);
  ASSERT_EQ(2, rg2_accessor->num_columns());
  ASSERT_EQ(nrows / 2, rg2_accessor->num_rows());
  ASSERT_EQ(1024, rg2_accessor->total_byte_size());

  auto rg2_column1 = rg2_accessor->ColumnChunk(0);
  auto rg2_column2 = rg2_accessor->ColumnChunk(1);
  ASSERT_EQ(false, rg1_column1->is_metadata_set());
  ASSERT_THROW(rg2_column1->is_stats_set(), ParquetException);
  ASSERT_THROW(rg2_column1->statistics(), ParquetException);
  ASSERT_THROW(rg2_column1->compression(), ParquetException);
  ASSERT_THROW(rg2_column1->num_values(), ParquetException);
  ASSERT_THROW(rg2_column1->encodings(), ParquetException);
  ASSERT_THROW(rg2_column1->total_compressed_size(), ParquetException);
  ASSERT_THROW(rg2_column1->total_uncompressed_size(), ParquetException);
  ASSERT_THROW(rg2_column1->dictionary_page_offset(), ParquetException);
  ASSERT_THROW(rg2_column1->data_page_offset(), ParquetException);

  ASSERT_EQ(true, rg2_column2->is_stats_set());
  ASSERT_EQ(stats_float.min(), rg2_column2->statistics()->EncodeMin());
  ASSERT_EQ(stats_float.max(), rg2_column2->statistics()->EncodeMax());
  ASSERT_EQ(0, rg2_column2->statistics()->null_count());
  ASSERT_EQ(nrows, rg2_column2->statistics()->distinct_count());
  ASSERT_EQ(nrows / 2, rg2_column2->num_values());
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg2_column2->compression());
  ASSERT_EQ(3, rg2_column2->encodings().size());
  ASSERT_EQ(512, rg2_column2->total_compressed_size());
  ASSERT_EQ(600, rg2_column2->total_uncompressed_size());
  ASSERT_EQ(16, rg2_column2->dictionary_page_offset());
  ASSERT_EQ(26, rg2_column2->data_page_offset());
}

TEST(Metadata, PlaintextFooter) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

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

  std::shared_ptr<parquet::schema::ColumnPath> int_col_path =
      parquet::schema::ColumnPath::FromDotString("int_col");
  ColumnEncryptionProperties::Builder int_col_builder(int_col_path);
  int_col_builder.key(kColumnEncryptionKey1);
  int_col_builder.key_id("kc1");

  std::map<std::shared_ptr<schema::ColumnPath>,
           std::shared_ptr<ColumnEncryptionProperties>, schema::ColumnPath::CmpColumnPath>
      encryption_col_props;
  encryption_col_props[int_col_path] = int_col_builder.build();

  FileEncryptionProperties::Builder encryption_prop_builder(kFooterEncryptionKey);
  encryption_prop_builder.footer_key_metadata("kf");
  encryption_prop_builder.set_plaintext_footer();
  encryption_prop_builder.column_properties(encryption_col_props);

  WriterProperties::Builder writer_prop_builder;
  writer_prop_builder.version(ParquetVersion::PARQUET_2_0);
  writer_prop_builder.encryption(encryption_prop_builder.build());
  auto props = writer_prop_builder.build();

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);
  auto rg1_builder = f_builder->AppendRowGroup();

  // Write the metadata
  // rowgroup1 metadata
  auto col1_builder = rg1_builder->NextColumnChunk();
  auto col2_builder = rg1_builder->NextColumnChunk();
  // column metadata
  stats_int.set_is_signed(true);
  col1_builder->SetStatistics(stats_int);
  stats_float.set_is_signed(true);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 4, 0, 10, 512, 600, true, false);
  col2_builder->Finish(nrows / 2, 24, 0, 30, 512, 600, true, false);

  rg1_builder->set_num_rows(nrows / 2);
  rg1_builder->Finish(1024);

  // rowgroup2 metadata
  auto rg2_builder = f_builder->AppendRowGroup();
  col1_builder = rg2_builder->NextColumnChunk();
  col2_builder = rg2_builder->NextColumnChunk();
  // column metadata
  col1_builder->SetStatistics(stats_int);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 6, 0, 10, 512, 600, true, false);
  col2_builder->Finish(nrows / 2, 16, 0, 26, 512, 600, true, false);

  rg2_builder->set_num_rows(nrows / 2);
  rg2_builder->Finish(1024);

  // Read the metadata
  auto f_accessor = f_builder->Finish();

  ASSERT_EQ(true, f_accessor->is_encryption_algorithm_set());

  auto file_crypto_metadata = f_builder->GetCryptoMetaData();
  ASSERT_EQ(NULLPTR, file_crypto_metadata);

  // file metadata
  ASSERT_EQ(nrows, f_accessor->num_rows());
  ASSERT_LE(0, static_cast<int>(f_accessor->size()));
  ASSERT_EQ(2, f_accessor->num_row_groups());
  ASSERT_EQ(ParquetVersion::PARQUET_2_0, f_accessor->version());
  ASSERT_EQ(DEFAULT_CREATED_BY, f_accessor->created_by());
  ASSERT_EQ(3, f_accessor->num_schema_elements());

  // row group1 metadata
  auto rg1_accessor = f_accessor->RowGroup(0);
  ASSERT_EQ(2, rg1_accessor->num_columns());
  ASSERT_EQ(nrows / 2, rg1_accessor->num_rows());
  ASSERT_EQ(1024, rg1_accessor->total_byte_size());

  auto rg1_column1 = rg1_accessor->ColumnChunk(0);
  auto rg1_column2 = rg1_accessor->ColumnChunk(1);
  ASSERT_EQ(true, rg1_column1->is_metadata_set());
  ASSERT_EQ(false, rg1_column1->is_stats_set());
  ASSERT_EQ(NULLPTR, rg1_column1->statistics());
  // ASSERT_THROW(rg1_column1->encodings(), ParquetException);

  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg1_column1->compression());
  ASSERT_EQ(nrows / 2, rg1_column1->num_values());
  ASSERT_EQ(3, rg1_column1->encodings().size());
  ASSERT_EQ(512, rg1_column1->total_compressed_size());
  ASSERT_EQ(600, rg1_column1->total_uncompressed_size());
  ASSERT_EQ(4, rg1_column1->dictionary_page_offset());
  ASSERT_EQ(10, rg1_column1->data_page_offset());

  ASSERT_EQ(true, rg1_column2->is_stats_set());
  ASSERT_EQ(stats_float.min(), rg1_column2->statistics()->EncodeMin());
  ASSERT_EQ(stats_float.max(), rg1_column2->statistics()->EncodeMax());
  ASSERT_EQ(0, rg1_column2->statistics()->null_count());
  ASSERT_EQ(nrows, rg1_column2->statistics()->distinct_count());
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg1_column2->compression());
  ASSERT_EQ(nrows / 2, rg1_column2->num_values());
  ASSERT_EQ(3, rg1_column2->encodings().size());
  ASSERT_EQ(512, rg1_column2->total_compressed_size());
  ASSERT_EQ(600, rg1_column2->total_uncompressed_size());
  ASSERT_EQ(24, rg1_column2->dictionary_page_offset());
  ASSERT_EQ(30, rg1_column2->data_page_offset());

  auto rg2_accessor = f_accessor->RowGroup(1);
  ASSERT_EQ(2, rg2_accessor->num_columns());
  ASSERT_EQ(nrows / 2, rg2_accessor->num_rows());
  ASSERT_EQ(1024, rg2_accessor->total_byte_size());

  auto rg2_column1 = rg2_accessor->ColumnChunk(0);
  auto rg2_column2 = rg2_accessor->ColumnChunk(1);
  ASSERT_EQ(true, rg2_column1->is_metadata_set());
  ASSERT_EQ(false, rg2_column1->is_stats_set());
  ASSERT_EQ(NULLPTR, rg2_column1->statistics());
  // ASSERT_THROW(rg2_column1->encodings(), ParquetException);

  ASSERT_EQ(nrows / 2, rg2_column1->num_values());
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg2_column1->compression());
  ASSERT_EQ(3, rg2_column1->encodings().size());
  ASSERT_EQ(512, rg2_column1->total_compressed_size());
  ASSERT_EQ(600, rg2_column1->total_uncompressed_size());
  ASSERT_EQ(6, rg2_column1->dictionary_page_offset());
  ASSERT_EQ(10, rg2_column1->data_page_offset());

  ASSERT_EQ(true, rg2_column2->is_stats_set());
  ASSERT_EQ(stats_float.min(), rg2_column2->statistics()->EncodeMin());
  ASSERT_EQ(stats_float.max(), rg2_column2->statistics()->EncodeMax());
  ASSERT_EQ(0, rg2_column2->statistics()->null_count());
  ASSERT_EQ(nrows, rg2_column2->statistics()->distinct_count());
  ASSERT_EQ(nrows / 2, rg2_column2->num_values());
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg2_column2->compression());
  ASSERT_EQ(3, rg2_column2->encodings().size());
  ASSERT_EQ(512, rg2_column2->total_compressed_size());
  ASSERT_EQ(600, rg2_column2->total_uncompressed_size());
  ASSERT_EQ(16, rg2_column2->dictionary_page_offset());
  ASSERT_EQ(26, rg2_column2->data_page_offset());
}

}  // namespace metadata
}  // namespace parquet

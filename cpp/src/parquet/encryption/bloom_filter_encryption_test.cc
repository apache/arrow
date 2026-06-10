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

#include <memory>
#include <string>

#include "arrow/io/file.h"
#include "arrow/io/memory.h"

#include "parquet/bloom_filter.h"
#include "parquet/bloom_filter_reader.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/properties.h"
#include "parquet/schema.h"

namespace parquet::encryption::test {
namespace {

std::shared_ptr<parquet::FileDecryptionProperties> BuildDecryptionProperties() {
  // Map test key ids to fixed test keys for decrypting the file footer and columns.
  std::shared_ptr<parquet::StringKeyIdRetriever> kr =
      std::make_shared<parquet::StringKeyIdRetriever>();
  kr->PutKey(kFooterMasterKeyId, kFooterEncryptionKey);
  kr->PutKey(kColumnMasterKeyIds[0], kColumnEncryptionKey1);
  kr->PutKey(kColumnMasterKeyIds[1], kColumnEncryptionKey2);

  parquet::FileDecryptionProperties::Builder builder;
  return builder
      .key_retriever(std::static_pointer_cast<parquet::DecryptionKeyRetriever>(kr))
      ->build();
}

}  // namespace

// Read Bloom filters from an encrypted parquet-testing file.
// The test data enables Bloom filters for double_field and float_field only.
TEST(EncryptedBloomFilterReader, ReadEncryptedBloomFilter) {
  const std::string file_path =
      data_file("encrypt_columns_and_footer_bloom_filter.parquet.encrypted");

  parquet::ReaderProperties reader_properties = parquet::default_reader_properties();
  reader_properties.file_decryption_properties(BuildDecryptionProperties());

  PARQUET_ASSIGN_OR_THROW(auto source, ::arrow::io::ReadableFile::Open(
                                           file_path, reader_properties.memory_pool()));
  auto file_reader = parquet::ParquetFileReader::Open(source, reader_properties);
  auto file_metadata = file_reader->metadata();

  ASSERT_EQ(file_metadata->num_columns(), 4);
  ASSERT_GE(file_metadata->num_row_groups(), 1);

  auto& bloom_filter_reader = file_reader->GetBloomFilterReader();
  auto row_group_0 = bloom_filter_reader.RowGroup(0);
  ASSERT_NE(nullptr, row_group_0);

  auto double_filter = row_group_0->GetColumnBloomFilter(0);
  auto float_filter = row_group_0->GetColumnBloomFilter(1);
  auto int32_filter = row_group_0->GetColumnBloomFilter(2);
  auto name_filter = row_group_0->GetColumnBloomFilter(3);

  // double_field and float_field have Bloom filters; the others do not.
  ASSERT_NE(nullptr, double_filter);
  ASSERT_NE(nullptr, float_filter);
  ASSERT_EQ(nullptr, int32_filter);
  ASSERT_EQ(nullptr, name_filter);

  // Values follow a simple pattern in the test data.
  for (int i : {0, 1, 7, 42}) {
    const double value = static_cast<double>(i) + 0.5;
    EXPECT_TRUE(double_filter->FindHash(double_filter->Hash(value)));
  }

  for (int i : {0, 2, 5, 10}) {
    const float value = static_cast<float>(i) + 0.25f;
    EXPECT_TRUE(float_filter->FindHash(float_filter->Hash(value)));
  }
}

namespace {

std::shared_ptr<schema::GroupNode> SingleInt64Schema(const std::string& field_name) {
  auto field = schema::PrimitiveNode::Make(field_name, Repetition::REQUIRED, Type::INT64,
                                           ConvertedType::NONE);
  return std::static_pointer_cast<schema::GroupNode>(
      schema::GroupNode::Make("schema", Repetition::REQUIRED, {field}));
}

std::shared_ptr<FileEncryptionProperties> BuildEncryptionProperties(
    const std::string& /*field_name*/) {
  FileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  return builder.build();
}

std::shared_ptr<FileDecryptionProperties> BuildDecryptionPropertiesWithExplicitKeys(
    const std::string& /*field_name*/) {
  FileDecryptionProperties::Builder builder;
  return builder.footer_key(kFooterEncryptionKey)->build();
}

}  // namespace

// Round trip, write a small encrypted file with a Bloom filter on the encrypted
// column, then read it back and verify the Bloom filter contains the inserted
// values and rejects values that were never inserted.
TEST(EncryptedBloomFilterWriter, RoundTripEncryptedBloomFilter) {
  const std::string field_name = "id";
  constexpr int kNumValues = 64;

  auto schema = SingleInt64Schema(field_name);

  WriterProperties::Builder prop_builder;
  prop_builder.compression(Compression::UNCOMPRESSED);
  prop_builder.enable_bloom_filter(field_name, {});
  prop_builder.encryption(BuildEncryptionProperties(field_name));
  auto writer_properties = prop_builder.build();

  PARQUET_ASSIGN_OR_THROW(auto sink, ::arrow::io::BufferOutputStream::Create());
  auto file_writer = ParquetFileWriter::Open(sink, schema, writer_properties);
  auto* row_group_writer = file_writer->AppendRowGroup();
  auto* int64_writer = static_cast<Int64Writer*>(row_group_writer->NextColumn());
  std::vector<int64_t> values(kNumValues);
  for (int i = 0; i < kNumValues; ++i) {
    values[i] = static_cast<int64_t>(i) * 7 + 13;
  }
  int64_writer->WriteBatch(static_cast<int64_t>(values.size()), nullptr, nullptr,
                           values.data());
  file_writer->Close();
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());

  ReaderProperties reader_properties = default_reader_properties();
  reader_properties.file_decryption_properties(
      BuildDecryptionPropertiesWithExplicitKeys(field_name));

  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source, reader_properties);
  auto& bloom_filter_reader = file_reader->GetBloomFilterReader();
  auto row_group_0 = bloom_filter_reader.RowGroup(0);
  ASSERT_NE(nullptr, row_group_0);
  auto filter = row_group_0->GetColumnBloomFilter(0);
  ASSERT_NE(nullptr, filter);

  for (int64_t value : values) {
    EXPECT_TRUE(filter->FindHash(filter->Hash(value)))
        << "missing inserted value " << value;
  }

  for (int64_t miss : {int64_t{-1}, int64_t{1'000'000}, int64_t{1'000'001}}) {
    EXPECT_FALSE(filter->FindHash(filter->Hash(miss)))
        << "unexpected hit for non-inserted value " << miss;
  }
}

TEST(EncryptedBloomFilterWriter, ColumnKeyEncryptedBloomFilterIsNotYetImplemented) {
  const std::string field_name = "id";
  auto schema = SingleInt64Schema(field_name);

  auto col_props =
      ColumnEncryptionProperties::Builder().key(kColumnEncryptionKey1)->build();
  ColumnPathToEncryptionPropertiesMap encrypted_columns{{field_name, col_props}};
  FileEncryptionProperties::Builder enc_builder(kFooterEncryptionKey);
  auto file_encryption_properties =
      enc_builder.encrypted_columns(std::move(encrypted_columns))->build();

  WriterProperties::Builder prop_builder;
  prop_builder.compression(Compression::UNCOMPRESSED);
  prop_builder.enable_bloom_filter(field_name, {});
  prop_builder.encryption(file_encryption_properties);
  auto writer_properties = prop_builder.build();

  PARQUET_ASSIGN_OR_THROW(auto sink, ::arrow::io::BufferOutputStream::Create());
  auto file_writer = ParquetFileWriter::Open(sink, schema, writer_properties);
  auto* row_group_writer = file_writer->AppendRowGroup();
  auto* int64_writer = static_cast<Int64Writer*>(row_group_writer->NextColumn());
  int64_t value = 42;
  int64_writer->WriteBatch(1, nullptr, nullptr, &value);
  EXPECT_THROW(file_writer->Close(), ParquetException);
}

}  // namespace parquet::encryption::test

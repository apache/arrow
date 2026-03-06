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

#include "parquet/bloom_filter.h"
#include "parquet/bloom_filter_reader.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/file_reader.h"
#include "parquet/properties.h"

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

}  // namespace parquet::encryption::test

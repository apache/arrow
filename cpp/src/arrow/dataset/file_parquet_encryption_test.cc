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

#include <string_view>

#include "gtest/gtest.h"

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include "arrow/array.h"
#include "arrow/dataset/parquet_encryption_config.h"
#include "arrow/dataset/partition.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/io/api.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "parquet/arrow/reader.h"
#include "parquet/encryption/crypto_factory.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/kms_client.h"
#include "parquet/encryption/test_in_memory_kms.h"

constexpr std::string_view kFooterKeyMasterKey = "0123456789012345";
constexpr std::string_view kFooterKeyMasterKeyId = "footer_key";
constexpr std::string_view kFooterKeyName = "footer_key";
constexpr std::string_view kColumnMasterKey = "1234567890123450";
constexpr std::string_view kColumnMasterKeyId = "col_key";
constexpr std::string_view kColumnKeyMapping = "col_key: a";
constexpr std::string_view kBaseDir = "";

using arrow::internal::checked_pointer_cast;

namespace arrow {
namespace dataset {

class DatasetEncryptionTest : public ::testing::Test {
 public:
  // This function creates a mock file system using the current time point, creates a
  // directory with the given base directory path, and writes a dataset to it using
  // provided Parquet file write options. The dataset is partitioned using a Hive
  // partitioning scheme. The function also checks if the written files exist in the file
  // system.
  static void SetUpTestSuite() {
#ifdef ARROW_VALGRIND
    // Not necessary otherwise, but prevents a Valgrind leak by making sure
    // OpenSSL initialization is done from the main thread
    // (see GH-38304 for analysis).
    ::parquet::encryption::EnsureBackendInitialized();
#endif

    // Creates a mock file system using the current time point.
    EXPECT_OK_AND_ASSIGN(file_system_, fs::internal::MockFileSystem::Make(
                                           std::chrono::system_clock::now(), {}));
    ASSERT_OK(file_system_->CreateDir(std::string(kBaseDir)));

    // Prepare table data.
    auto table_schema = schema({field("a", int64()), field("c", int64()),
                                field("e", int64()), field("part", utf8())});
    table_ = TableFromJSON(table_schema, {R"([
                          [ 0, 9, 1, "a" ],
                          [ 1, 8, 2, "a" ],
                          [ 2, 7, 1, "c" ],
                          [ 3, 6, 2, "c" ],
                          [ 4, 5, 1, "e" ],
                          [ 5, 4, 2, "e" ],
                          [ 6, 3, 1, "g" ],
                          [ 7, 2, 2, "g" ],
                          [ 8, 1, 1, "i" ],
                          [ 9, 0, 2, "i" ]
                        ])"});

    // Use a Hive-style partitioning scheme.
    partitioning_ = std::make_shared<HivePartitioning>(schema({field("part", utf8())}));

    // Prepare encryption properties.
    std::unordered_map<std::string, std::string> key_map;
    key_map.emplace(kColumnMasterKeyId, kColumnMasterKey);
    key_map.emplace(kFooterKeyMasterKeyId, kFooterKeyMasterKey);

    crypto_factory_ = std::make_shared<parquet::encryption::CryptoFactory>();
    auto kms_client_factory =
        std::make_shared<parquet::encryption::TestOnlyInMemoryKmsClientFactory>(
            /*wrap_locally=*/true, key_map);
    crypto_factory_->RegisterKmsClientFactory(std::move(kms_client_factory));
    kms_connection_config_ = std::make_shared<parquet::encryption::KmsConnectionConfig>();

    // Set write options with encryption configuration.
    auto encryption_config =
        std::make_shared<parquet::encryption::EncryptionConfiguration>(
            std::string(kFooterKeyName));
    encryption_config->column_keys = kColumnKeyMapping;
    auto parquet_encryption_config = std::make_shared<ParquetEncryptionConfig>();
    // Directly assign shared_ptr objects to ParquetEncryptionConfig members
    parquet_encryption_config->crypto_factory = crypto_factory_;
    parquet_encryption_config->kms_connection_config = kms_connection_config_;
    parquet_encryption_config->encryption_config = std::move(encryption_config);

    auto file_format = std::make_shared<ParquetFileFormat>();
    auto parquet_file_write_options =
        checked_pointer_cast<ParquetFileWriteOptions>(file_format->DefaultWriteOptions());
    parquet_file_write_options->parquet_encryption_config =
        std::move(parquet_encryption_config);

    // Write dataset.
    auto dataset = std::make_shared<InMemoryDataset>(table_);
    EXPECT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());
    EXPECT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

    FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = parquet_file_write_options;
    write_options.filesystem = file_system_;
    write_options.base_dir = kBaseDir;
    write_options.partitioning = partitioning_;
    write_options.basename_template = "part{i}.parquet";
    ASSERT_OK(FileSystemDataset::Write(write_options, std::move(scanner)));
  }

 protected:
  inline static std::shared_ptr<fs::FileSystem> file_system_;
  inline static std::shared_ptr<Table> table_;
  inline static std::shared_ptr<HivePartitioning> partitioning_;
  inline static std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory_;
  inline static std::shared_ptr<parquet::encryption::KmsConnectionConfig>
      kms_connection_config_;
};

// This test demonstrates the process of writing a partitioned Parquet file with the same
// encryption properties applied to each file within the dataset. The encryption
// properties are determined based on the selected columns. After writing the dataset, the
// test reads the data back and verifies that it can be successfully decrypted and
// scanned.
TEST_F(DatasetEncryptionTest, WriteReadDatasetWithEncryption) {
  // Create decryption properties.
  auto decryption_config =
      std::make_shared<parquet::encryption::DecryptionConfiguration>();
  auto parquet_decryption_config = std::make_shared<ParquetDecryptionConfig>();
  parquet_decryption_config->crypto_factory = crypto_factory_;
  parquet_decryption_config->kms_connection_config = kms_connection_config_;
  parquet_decryption_config->decryption_config = std::move(decryption_config);

  // Set scan options.
  auto parquet_scan_options = std::make_shared<ParquetFragmentScanOptions>();
  parquet_scan_options->parquet_decryption_config = std::move(parquet_decryption_config);

  auto file_format = std::make_shared<ParquetFileFormat>();
  file_format->default_fragment_scan_options = std::move(parquet_scan_options);

  // Get FileInfo objects for all files under the base directory
  fs::FileSelector selector;
  selector.base_dir = kBaseDir;
  selector.recursive = true;

  FileSystemFactoryOptions factory_options;
  factory_options.partitioning = partitioning_;
  factory_options.partition_base_dir = kBaseDir;
  ASSERT_OK_AND_ASSIGN(auto dataset_factory,
                       FileSystemDatasetFactory::Make(file_system_, selector, file_format,
                                                      factory_options));

  // Read dataset into table
  ASSERT_OK_AND_ASSIGN(auto dataset, dataset_factory->Finish());
  ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());
  ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());
  ASSERT_OK_AND_ASSIGN(auto read_table, scanner->ToTable());

  // Verify the data was read correctly
  ASSERT_OK_AND_ASSIGN(auto combined_table, read_table->CombineChunks());
  // Validate the table
  ASSERT_OK(combined_table->ValidateFull());
  AssertTablesEqual(*combined_table, *table_);
}

// Read a single parquet file with and without decryption properties.
TEST_F(DatasetEncryptionTest, ReadSingleFile) {
  // Open the Parquet file.
  ASSERT_OK_AND_ASSIGN(auto input, file_system_->OpenInputFile("part=a/part0.parquet"));

  // Try to read metadata without providing decryption properties
  // when the footer is encrypted.
  ASSERT_THROW(parquet::ReadMetaData(input), parquet::ParquetException);

  // Create the ReaderProperties object using the FileDecryptionProperties object
  auto decryption_config =
      std::make_shared<parquet::encryption::DecryptionConfiguration>();
  auto file_decryption_properties = crypto_factory_->GetFileDecryptionProperties(
      *kms_connection_config_, *decryption_config);
  auto reader_properties = parquet::default_reader_properties();
  reader_properties.file_decryption_properties(file_decryption_properties);

  // Read entire file as a single Arrow table
  parquet::arrow::FileReaderBuilder reader_builder;
  ASSERT_OK(reader_builder.Open(input, reader_properties));
  ASSERT_OK_AND_ASSIGN(auto arrow_reader, reader_builder.Build());
  std::shared_ptr<Table> table;
  ASSERT_OK(arrow_reader->ReadTable(&table));

  // Check the contents of the table
  ASSERT_EQ(table->num_rows(), 2);
  ASSERT_EQ(table->num_columns(), 3);
  ASSERT_EQ(checked_pointer_cast<Int64Array>(table->column(0)->chunk(0))->GetView(0), 0);
  ASSERT_EQ(checked_pointer_cast<Int64Array>(table->column(1)->chunk(0))->GetView(0), 9);
  ASSERT_EQ(checked_pointer_cast<Int64Array>(table->column(2)->chunk(0))->GetView(0), 1);
}

}  // namespace dataset
}  // namespace arrow

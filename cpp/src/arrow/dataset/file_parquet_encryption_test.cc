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

#include "arrow/array.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/parquet_encryption_config.h"
#include "arrow/dataset/partition.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/io/api.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"
#include "parquet/arrow/reader.h"
#include "parquet/encryption/crypto_factory.h"
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

// Tests come in these variations
enum EncryptionParam {
  COLUMN_KEY,
  UNIFORM,
};

// Base class to test writing and reading encrypted dataset.
class DatasetEncryptionTestBase : public testing::TestWithParam<EncryptionParam> {
 public:
  // This function creates a mock file system using the current time point, creates a
  // directory with the given base directory path, and writes a dataset to it using
  // provided Parquet file write options. The function also checks if the written files
  // exist in the file system.
  void SetUp() override {
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

    // Init dataset and partitioning.
    ASSERT_NO_FATAL_FAILURE(PrepareTableAndPartitioning());

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

    if (GetParam() == COLUMN_KEY) {
      encryption_config->column_keys = kColumnKeyMapping;
    } else if (GetParam() == UNIFORM) {
      encryption_config->uniform_encryption = true;
    } else {
      FAIL() << "Unsupported encryption type " << GetParam();
    }

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

  virtual void PrepareTableAndPartitioning() = 0;

  void TestScanDataset(bool concurrently = false) {
    // Create decryption properties.
    auto decryption_config =
        std::make_shared<parquet::encryption::DecryptionConfiguration>();
    auto parquet_decryption_config = std::make_shared<ParquetDecryptionConfig>();
    parquet_decryption_config->crypto_factory = crypto_factory_;
    parquet_decryption_config->kms_connection_config = kms_connection_config_;
    parquet_decryption_config->decryption_config = std::move(decryption_config);

    // Set scan options.
    auto parquet_scan_options = std::make_shared<ParquetFragmentScanOptions>();
    parquet_scan_options->parquet_decryption_config =
        std::move(parquet_decryption_config);

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
                         FileSystemDatasetFactory::Make(file_system_, selector,
                                                        file_format, factory_options));

    // Create the dataset
    ASSERT_OK_AND_ASSIGN(auto dataset, dataset_factory->Finish());

    if (concurrently) {
      // start with a single thread so we are more likely to build up a queue of jobs
      ASSERT_OK_AND_ASSIGN(auto pool, arrow::internal::ThreadPool::Make(1));
      std::vector<Future<std::shared_ptr<Table>>> threads;

      // Read dataset above multiple times concurrently to see that is thread-safe.
      for (size_t i = 0; i < 100; ++i) {
        threads.push_back(
            DeferNotOk(pool->Submit(DatasetEncryptionTestBase::read, dataset)));
      }

      // ramp up parallelism
      ASSERT_OK(pool->SetCapacity(16));
      // ensure there are sufficient jobs to see concurrent processing
      ASSERT_GT(pool->GetNumTasks(), 16);
      printf("%d", pool->GetNumTasks());

      // wait for all jobs to finish
      pool->WaitForIdle();

      // assert correctness of jobs
      for (auto& thread : threads) {
        ASSERT_OK_AND_ASSIGN(auto read_table, thread.result());
        AssertTablesEqual(*read_table, *table_);
      }
    } else {
      // Reuse the dataset above to scan it twice to make sure decryption works correctly.
      for (size_t i = 0; i < 2; ++i) {
        ASSERT_OK_AND_ASSIGN(auto read_table, read(dataset));
        AssertTablesEqual(*read_table, *table_);
      }
    }
  }

  static Result<std::shared_ptr<Table>> read(const std::shared_ptr<Dataset>& dataset) {
    // Read dataset into table
    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto read_table, scanner->ToTable());

    // Verify the data was read correctly
    ARROW_ASSIGN_OR_RAISE(auto combined_table, read_table->CombineChunks());
    // Validate the table
    RETURN_NOT_OK(combined_table->ValidateFull());
    return combined_table;
  }

 protected:
  std::shared_ptr<fs::FileSystem> file_system_;
  std::shared_ptr<Table> table_;
  std::shared_ptr<Partitioning> partitioning_;
  std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory_;
  std::shared_ptr<parquet::encryption::KmsConnectionConfig> kms_connection_config_;
};

class DatasetEncryptionTest : public DatasetEncryptionTestBase {
 public:
  // The dataset is partitioned using a Hive partitioning scheme.
  void PrepareTableAndPartitioning() override {
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
  }
};

// This test demonstrates the process of writing a partitioned Parquet file with the same
// encryption properties applied to each file within the dataset. The encryption
// properties are determined based on the selected columns. After writing the dataset, the
// test reads the data back and verifies that it can be successfully decrypted and
// scanned.
TEST_P(DatasetEncryptionTest, WriteReadDatasetWithEncryption) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
}

TEST_P(DatasetEncryptionTest, WriteReadDatasetWithEncryptionThreaded) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset(true));
}

// Read a single parquet file with and without decryption properties.
TEST_P(DatasetEncryptionTest, ReadSingleFile) {
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

INSTANTIATE_TEST_SUITE_P(DatasetColumnKeyEncryptionTest, DatasetEncryptionTest,
                         ::testing::Values(COLUMN_KEY));
INSTANTIATE_TEST_SUITE_P(DatasetUniformEncryptionTest, DatasetEncryptionTest,
                         ::testing::Values(UNIFORM));

// GH-39444: This test covers the case where parquet dataset scanner crashes when
// processing encrypted datasets over 2^15 rows in multi-threaded mode.
class LargeRowEncryptionTest : public DatasetEncryptionTestBase {
 public:
  // The dataset is partitioned using a Hive partitioning scheme.
  void PrepareTableAndPartitioning() override {
    // Specifically chosen to be greater than batch size for triggering prefetch.
    constexpr int kRowCount = 32769;

    // Create a random floating-point array with large number of rows.
    arrow::random::RandomArrayGenerator rand_gen(0);
    auto array = rand_gen.Float32(kRowCount, 0.0, 1.0, false);
    auto table_schema = schema({field("a", float32())});

    // Prepare table and partitioning.
    table_ = arrow::Table::Make(table_schema, {array});
    partitioning_ = std::make_shared<dataset::DirectoryPartitioning>(arrow::schema({}));
  }
};

// Test for writing and reading encrypted dataset with large row count.
TEST_P(LargeRowEncryptionTest, ReadEncryptLargeRows) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
}

INSTANTIATE_TEST_SUITE_P(LargeRowColumnKeyEncryptionTest, LargeRowEncryptionTest,
                         ::testing::Values(COLUMN_KEY));
INSTANTIATE_TEST_SUITE_P(LargeRowUniformEncryptionTest, LargeRowEncryptionTest,
                         ::testing::Values(UNIFORM));

}  // namespace dataset
}  // namespace arrow

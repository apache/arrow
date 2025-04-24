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
#include "arrow/compute/api_vector.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/parquet_encryption_config.h"
#include "arrow/dataset/partition.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
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

struct EncryptionTestParam {
  bool uniform_encryption;  // false is using per-column keys
  bool concurrently;
  bool use_crypto_factory;
};

std::ostream& operator<<(std::ostream& os, const EncryptionTestParam& param) {
  os << (param.uniform_encryption ? "UniformEncryption" : "ColumnKeys") << " ";
  os << (param.concurrently ? "Threaded" : "Serial") << " ";
  os << (param.use_crypto_factory ? "CryptoFactory" : "PropertyKeys");
  return os;
}

const auto kAllParamValues = ::testing::Values(
    // non-uniform encryption not supported for property keys by test
    EncryptionTestParam{true, false, false}, EncryptionTestParam{true, true, false},
    EncryptionTestParam{false, false, true}, EncryptionTestParam{true, false, true},
    EncryptionTestParam{false, true, true}, EncryptionTestParam{true, true, true});

// Base class to test writing and reading encrypted dataset.
class DatasetEncryptionTestBase : public testing::TestWithParam<EncryptionTestParam> {
 public:
#ifdef ARROW_VALGRIND
  static constexpr int kConcurrentIterations = 4;
#else
  static constexpr int kConcurrentIterations = 20;
#endif

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
    ASSERT_OK_AND_ASSIGN(expected_table_, table_->CombineChunks());
    ASSERT_OK_AND_ASSIGN(expected_table_, SortTable(expected_table_));

    // Prepare encryption properties.
    std::unordered_map<std::string, std::string> key_map;
    key_map.emplace(kColumnMasterKeyId, kColumnMasterKey);
    key_map.emplace(kFooterKeyMasterKeyId, kFooterKeyMasterKey);

    auto file_format = std::make_shared<ParquetFileFormat>();
    auto parquet_file_write_options =
        checked_pointer_cast<ParquetFileWriteOptions>(file_format->DefaultWriteOptions());

    if (GetParam().use_crypto_factory) {
      // Configure encryption keys via crypto factory.
      crypto_factory_ = std::make_shared<parquet::encryption::CryptoFactory>();
      auto kms_client_factory =
          std::make_shared<parquet::encryption::TestOnlyInMemoryKmsClientFactory>(
              /*wrap_locally=*/true, key_map);
      crypto_factory_->RegisterKmsClientFactory(std::move(kms_client_factory));
      kms_connection_config_ =
          std::make_shared<parquet::encryption::KmsConnectionConfig>();

      // Set write options with encryption configuration.
      auto encryption_config =
          std::make_shared<parquet::encryption::EncryptionConfiguration>(
              std::string(kFooterKeyName));
      encryption_config->uniform_encryption = GetParam().uniform_encryption;
      if (!GetParam().uniform_encryption) {
        encryption_config->column_keys = kColumnKeyMapping;
      }

      auto parquet_encryption_config = std::make_shared<ParquetEncryptionConfig>();
      // Directly assign shared_ptr objects to ParquetEncryptionConfig members
      parquet_encryption_config->crypto_factory = crypto_factory_;
      parquet_encryption_config->kms_connection_config = kms_connection_config_;
      parquet_encryption_config->encryption_config = std::move(encryption_config);
      parquet_file_write_options->parquet_encryption_config =
          std::move(parquet_encryption_config);
    } else {
      // Configure encryption keys via writer options / file encryption properties.
      // non-uniform encryption not support by test
      ASSERT_TRUE(GetParam().uniform_encryption);
      auto file_encryption_properties =
          std::make_unique<parquet::FileEncryptionProperties::Builder>(
              std::string(kFooterKeyMasterKey))
              ->build();
      auto writer_properties = std::make_unique<parquet::WriterProperties::Builder>()
                                   ->encryption(file_encryption_properties)
                                   ->build();
      parquet_file_write_options->writer_properties = writer_properties;
    }

    // Write dataset.
    auto dataset = std::make_shared<InMemoryDataset>(table_);
    EXPECT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());
    ARROW_EXPECT_OK(scanner_builder->UseThreads(GetParam().concurrently));
    EXPECT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

    if (GetParam().concurrently) {
      // Have a notable number of threads to exhibit multi-threading issues
      ASSERT_OK_AND_ASSIGN(auto pool, arrow::internal::ThreadPool::Make(16));
      std::vector<Future<>> futures;

      // Write dataset above multiple times concurrently to see that is thread-safe.
      for (int i = 1; i <= kConcurrentIterations; ++i) {
        FileSystemDatasetWriteOptions write_options;
        write_options.file_write_options = parquet_file_write_options;
        write_options.filesystem = file_system_;
        write_options.base_dir = "thread-" + std::to_string(i);
        write_options.partitioning = partitioning_;
        write_options.basename_template = "part{i}.parquet";
        futures.push_back(
            DeferNotOk(pool->Submit(FileSystemDataset::Write, write_options, scanner)));
      }

      // Assert all jobs succeeded
      for (auto& future : futures) {
        ASSERT_FINISHES_OK(future);
      }
    } else {
      FileSystemDatasetWriteOptions write_options;
      write_options.file_write_options = parquet_file_write_options;
      write_options.filesystem = file_system_;
      write_options.base_dir = kBaseDir;
      write_options.partitioning = partitioning_;
      write_options.basename_template = "part{i}.parquet";
      ASSERT_OK(FileSystemDataset::Write(write_options, std::move(scanner)));
    }
  }

  virtual void PrepareTableAndPartitioning() = 0;

  Result<std::shared_ptr<Dataset>> OpenDataset(
      std::string_view base_dir, const std::shared_ptr<ParquetFileFormat>& file_format) {
    // Get FileInfo objects for all files under the base directory
    fs::FileSelector selector;
    selector.base_dir = base_dir;
    selector.recursive = true;

    FileSystemFactoryOptions factory_options;
    factory_options.partitioning = partitioning_;
    factory_options.partition_base_dir = base_dir;
    ARROW_ASSIGN_OR_RAISE(auto dataset_factory,
                          FileSystemDatasetFactory::Make(file_system_, selector,
                                                         file_format, factory_options));

    // Create the dataset
    return dataset_factory->Finish();
  }

  void TestScanDataset() {
    // Set scan options.
    auto parquet_scan_options = std::make_shared<ParquetFragmentScanOptions>();

    if (GetParam().use_crypto_factory) {
      // Configure decryption keys via crypto factory.
      auto decryption_config =
          std::make_shared<parquet::encryption::DecryptionConfiguration>();
      auto parquet_decryption_config = std::make_shared<ParquetDecryptionConfig>();
      parquet_decryption_config->crypto_factory = crypto_factory_;
      parquet_decryption_config->kms_connection_config = kms_connection_config_;
      parquet_decryption_config->decryption_config = std::move(decryption_config);

      parquet_scan_options->parquet_decryption_config =
          std::move(parquet_decryption_config);
    } else {
      // Configure decryption keys via reader properties / file decryption properties.
      auto file_decryption_properties =
          std::make_unique<parquet::FileDecryptionProperties::Builder>()
              ->footer_key(std::string(kFooterKeyMasterKey))
              ->build();
      parquet_scan_options->reader_properties->file_decryption_properties(
          file_decryption_properties);
    }

    auto file_format = std::make_shared<ParquetFileFormat>();
    file_format->default_fragment_scan_options = std::move(parquet_scan_options);

    if (GetParam().concurrently) {
      // Create the dataset
      ASSERT_OK_AND_ASSIGN(auto dataset, OpenDataset("thread-1", file_format));

      // Have a notable number of threads to exhibit multi-threading issues
      ASSERT_OK_AND_ASSIGN(auto pool, arrow::internal::ThreadPool::Make(16));
      std::vector<Future<std::shared_ptr<Table>>> futures;

      // Read dataset above multiple times concurrently to see that is thread-safe.
      for (int i = 0; i < kConcurrentIterations; ++i) {
        futures.push_back(DeferNotOk(pool->Submit(ReadDataset, dataset)));
      }

      // Assert correctness of jobs
      for (auto& future : futures) {
        ASSERT_OK_AND_ASSIGN(auto read_table, future.result());
        CheckDatasetResults(read_table);
      }

      // Finally check datasets written by all other threads are as expected
      for (int i = 2; i <= kConcurrentIterations; ++i) {
        ASSERT_OK_AND_ASSIGN(dataset,
                             OpenDataset("thread-" + std::to_string(i), file_format));
        ASSERT_OK_AND_ASSIGN(auto read_table, ReadDataset(dataset));
        CheckDatasetResults(read_table);
      }
    } else {
      // Create the dataset
      ASSERT_OK_AND_ASSIGN(auto dataset, OpenDataset(kBaseDir, file_format));

      // Reuse the dataset above to scan it twice to make sure decryption works correctly.
      for (int i = 0; i < 2; ++i) {
        ASSERT_OK_AND_ASSIGN(auto read_table, ReadDataset(dataset));
        CheckDatasetResults(read_table);
      }
    }
  }

  static Result<std::shared_ptr<Table>> ReadDataset(
      const std::shared_ptr<Dataset>& dataset) {
    // Read dataset into table
    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_EXPECT_OK(scanner_builder->UseThreads(GetParam().concurrently));
    return scanner->ToTable();
  }

  void CheckDatasetResults(const std::shared_ptr<Table>& table) {
    ASSERT_OK(table->ValidateFull());
    // Make results comparable despite ordering and chunking differences
    ASSERT_OK_AND_ASSIGN(auto combined_table, table->CombineChunks());
    ASSERT_OK_AND_ASSIGN(auto sorted_table, SortTable(combined_table));
    AssertTablesEqual(*sorted_table, *expected_table_);
  }

  // Sort table for comparability of dataset read results, which may be unordered.
  // This relies on column "a" having statistically unique values.
  Result<std::shared_ptr<Table>> SortTable(const std::shared_ptr<Table>& table) {
    compute::SortOptions options({compute::SortKey("a")});
    ARROW_ASSIGN_OR_RAISE(auto indices, compute::SortIndices(table, options));
    ARROW_ASSIGN_OR_RAISE(auto sorted, compute::Take(table, indices));
    EXPECT_EQ(sorted.kind(), Datum::TABLE);
    return sorted.table();
  }

 protected:
  std::string base_dir_ = GetParam().concurrently ? "thread-1" : std::string(kBaseDir);
  std::shared_ptr<fs::FileSystem> file_system_;
  std::shared_ptr<Table> table_, expected_table_;
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

// Read a single parquet file with and without decryption properties.
TEST_P(DatasetEncryptionTest, ReadSingleFile) {
  // Open the Parquet file.
  ASSERT_OK_AND_ASSIGN(auto input,
                       file_system_->OpenInputFile(base_dir_ + "/part=a/part0.parquet"));

  // Try to read metadata without providing decryption properties
  // when the footer is encrypted.
  ASSERT_THROW(parquet::ReadMetaData(input), parquet::ParquetException);

  // Create the ReaderProperties object using the FileDecryptionProperties object
  auto decryption_config =
      std::make_shared<parquet::encryption::DecryptionConfiguration>();
  std::shared_ptr<parquet::FileDecryptionProperties> file_decryption_properties;
  if (GetParam().use_crypto_factory) {
    // Configure decryption keys via file decryption properties with crypto factory key
    // retriever.
    file_decryption_properties = crypto_factory_->GetFileDecryptionProperties(
        *kms_connection_config_, *decryption_config);
  } else {
    // Configure decryption keys via file decryption properties with static footer key.
    file_decryption_properties =
        std::make_unique<parquet::FileDecryptionProperties::Builder>()
            ->footer_key(std::string(kFooterKeyMasterKey))
            ->build();
  }
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

INSTANTIATE_TEST_SUITE_P(DatasetEncryptionTest, DatasetEncryptionTest, kAllParamValues);

// GH-39444: This test covers the case where parquet dataset scanner crashes when
// processing encrypted datasets over 2^15 rows in multi-threaded mode.
class LargeRowCountEncryptionTest : public DatasetEncryptionTestBase {
 public:
  // The dataset is partitioned using a Hive partitioning scheme.
  void PrepareTableAndPartitioning() override {
    // Specifically chosen to be greater than batch size for triggering prefetch.
    constexpr int kRowCount = 32769;
    // Number of batches
    constexpr int kBatchCount = 5;

    // Create multiple random floating-point arrays with large number of rows.
    arrow::random::RandomArrayGenerator rand_gen(0);
    auto arrays = std::vector<std::shared_ptr<arrow::Array>>();
    for (int i = 0; i < kBatchCount; i++) {
      arrays.push_back(rand_gen.Float32(kRowCount, 0.0, 1.0, false));
    }
    ASSERT_OK_AND_ASSIGN(auto column, ChunkedArray::Make(arrays, float32()));
    auto table_schema = schema({field("a", float32())});

    // Prepare table and partitioning.
    table_ = arrow::Table::Make(table_schema, {column});
    partitioning_ = std::make_shared<dataset::DirectoryPartitioning>(arrow::schema({}));
  }
};

// Test for writing and reading encrypted dataset with large row count.
TEST_P(LargeRowCountEncryptionTest, ReadEncryptLargeRowCount) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
}

INSTANTIATE_TEST_SUITE_P(LargeRowCountEncryptionTest, LargeRowCountEncryptionTest,
                         kAllParamValues);

}  // namespace dataset
}  // namespace arrow

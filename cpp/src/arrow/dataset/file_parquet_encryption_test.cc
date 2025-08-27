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
#include "arrow/util/secure_string.h"
#include "arrow/util/thread_pool.h"
#include "parquet/arrow/reader.h"
#include "parquet/encryption/crypto_factory.h"
#include "parquet/encryption/kms_client.h"
#include "parquet/encryption/test_in_memory_kms.h"

using arrow::util::SecureString;

const SecureString kFooterKeyMasterKey("0123456789012345");
constexpr std::string_view kFooterKeyMasterKeyId = "footer_key";
constexpr std::string_view kFooterKeyName = "footer_key";

const SecureString kColumnMasterKey("1234567890123450");
constexpr std::string_view kColumnMasterKeyId = "col_key";
constexpr std::string_view kColumnName = "a";

constexpr std::string_view kBaseDir = "";

using arrow::internal::checked_pointer_cast;

namespace arrow {
namespace dataset {

struct EncryptionTestParam {
  bool uniform_encryption;  // false is using per-column keys
  bool concurrently;
  bool use_crypto_factory;

  EncryptionTestParam(bool uniform_encryption, bool concurrently, bool use_crypto_factory)
      : uniform_encryption(uniform_encryption),
        concurrently(concurrently),
        use_crypto_factory(use_crypto_factory) {}
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
template <typename T, typename = typename std::enable_if<
                          std::is_base_of<EncryptionTestParam, T>::value>::type>
class DatasetEncryptionTestBase : public testing::TestWithParam<T> {
 public:
#ifdef ARROW_VALGRIND
  static constexpr int kConcurrentIterations = 4;
#else
  static constexpr int kConcurrentIterations = 20;
#endif

  static const EncryptionTestParam& GetParam() {
    return testing::WithParamInterface<T>::GetParam();
  }

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
    EXPECT_OK_AND_ASSIGN(
        file_system_,
        fs::internal::MockFileSystem::Make(
            std::chrono::system_clock::now(),
            {fs::FileInfo(std::string(kBaseDir), fs::FileType::Directory)}));

    // Init dataset and partitioning.
    ASSERT_NO_FATAL_FAILURE(PrepareTableAndPartitioning());
    ASSERT_OK_AND_ASSIGN(expected_table_, table_->CombineChunks());

    // Prepare encryption properties.
    key_map_.emplace(kColumnMasterKeyId, kColumnMasterKey);
    key_map_.emplace(kFooterKeyMasterKeyId, kFooterKeyMasterKey);

    auto file_format = std::make_shared<ParquetFileFormat>();
    auto parquet_file_write_options =
        checked_pointer_cast<ParquetFileWriteOptions>(file_format->DefaultWriteOptions());

    if (GetParam().use_crypto_factory) {
      // Configure encryption keys via crypto factory.
      crypto_factory_ = std::make_shared<parquet::encryption::CryptoFactory>();
      auto kms_client_factory =
          std::make_shared<parquet::encryption::TestOnlyInMemoryKmsClientFactory>(
              /*wrap_locally=*/true, key_map_);
      crypto_factory_->RegisterKmsClientFactory(std::move(kms_client_factory));
      kms_connection_config_ =
          std::make_shared<parquet::encryption::KmsConnectionConfig>();

      // Set write options with encryption configuration.
      auto encryption_config =
          std::make_shared<parquet::encryption::EncryptionConfiguration>(
              std::string(kFooterKeyName));
      encryption_config->uniform_encryption = GetParam().uniform_encryption;
      if (!GetParam().uniform_encryption) {
        std::stringstream column_key;
        column_key << kColumnMasterKeyId << ": " << ColumnName();
        encryption_config->column_keys = column_key.str();
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
              kFooterKeyMasterKey)
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
        write_options.preserve_order = true;
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
  virtual std::string_view ColumnName() { return kColumnName; }

  Result<std::shared_ptr<Dataset>> OpenDataset(
      std::string_view base_dir,
      const std::shared_ptr<parquet::encryption::CryptoFactory>& crypto_factory,
      const std::unordered_map<std::string, SecureString>& key_map) const {
    // make sure these keys are served by the KMS
    parquet::encryption::TestOnlyLocalWrapInMemoryKms::InitializeMasterKeys(key_map);
    parquet::encryption::TestOnlyInServerWrapKms::InitializeMasterKeys(key_map);

    // Set scan options.
    auto parquet_scan_options = std::make_shared<ParquetFragmentScanOptions>();

    if (GetParam().use_crypto_factory) {
      // Configure decryption keys via crypto factory.
      auto decryption_config =
          std::make_shared<parquet::encryption::DecryptionConfiguration>();
      auto parquet_decryption_config = std::make_shared<ParquetDecryptionConfig>();
      parquet_decryption_config->crypto_factory = crypto_factory;
      parquet_decryption_config->kms_connection_config = kms_connection_config_;
      parquet_decryption_config->decryption_config = std::move(decryption_config);

      parquet_scan_options->parquet_decryption_config =
          std::move(parquet_decryption_config);
    } else {
      // Configure decryption keys via reader properties / file decryption properties.
      auto file_decryption_properties =
          std::make_unique<parquet::FileDecryptionProperties::Builder>()
              ->footer_key(kFooterKeyMasterKey)
              ->build();
      parquet_scan_options->reader_properties->file_decryption_properties(
          file_decryption_properties);
    }

    auto file_format = std::make_shared<ParquetFileFormat>();
    file_format->default_fragment_scan_options = std::move(parquet_scan_options);

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
    if (GetParam().concurrently) {
      // Create the dataset
      ASSERT_OK_AND_ASSIGN(auto dataset,
                           OpenDataset("thread-1", crypto_factory_, key_map_));

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
        ASSERT_OK_AND_ASSIGN(dataset, OpenDataset("thread-" + std::to_string(i),
                                                  crypto_factory_, key_map_));
        ASSERT_OK_AND_ASSIGN(auto read_table, ReadDataset(dataset));
        CheckDatasetResults(read_table);
      }
    } else {
      // Create the dataset
      ASSERT_OK_AND_ASSIGN(auto dataset,
                           OpenDataset(kBaseDir, crypto_factory_, key_map_));

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
    // Make results comparable despite chunking differences
    ASSERT_OK_AND_ASSIGN(auto combined_table, table->CombineChunks());
    AssertTablesEqual(*combined_table, *expected_table_);
  }

 protected:
  std::string base_dir_ = GetParam().concurrently ? "thread-1" : std::string(kBaseDir);
  std::shared_ptr<fs::FileSystem> file_system_;
  std::shared_ptr<Table> table_, expected_table_;
  std::shared_ptr<Partitioning> partitioning_;
  std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory_;
  std::shared_ptr<parquet::encryption::KmsConnectionConfig> kms_connection_config_;
  std::unordered_map<std::string, SecureString> key_map_;
};

class DatasetEncryptionTest : public DatasetEncryptionTestBase<EncryptionTestParam> {
 public:
  // The dataset is partitioned using a Hive partitioning scheme.
  void PrepareTableAndPartitioning() override {
    // Prepare table data.
    auto table_schema =
        schema({field(std::string(kColumnName), int64()), field("c", int64()),
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
            ->footer_key(kFooterKeyMasterKey)
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

struct NestedFieldsEncryptionTestParam : EncryptionTestParam {
  std::string column_name;

  explicit NestedFieldsEncryptionTestParam(std::string column_name)
      : EncryptionTestParam(false, false, true), column_name(std::move(column_name)) {}
};

std::ostream& operator<<(std::ostream& os, const NestedFieldsEncryptionTestParam& param) {
  os << param.column_name;
  return os;
}

class NestedFieldsEncryptionTest
    : public DatasetEncryptionTestBase<NestedFieldsEncryptionTestParam> {
 public:
  NestedFieldsEncryptionTest() : id_data_(ArrayFromJSON(int8(), "[1,2,3]")) {}

  void SetUp() override {
    DatasetEncryptionTestBase::SetUp();

    // deliberately mis-configured column key
    key_map_with_wrong_column_key_.emplace(kColumnMasterKeyId, kFooterKeyMasterKey);
    key_map_with_wrong_column_key_.emplace(kFooterKeyMasterKeyId, kFooterKeyMasterKey);

    crypto_factory_with_wrong_column_key_ =
        std::make_shared<parquet::encryption::CryptoFactory>();
    auto kms_client_factory =
        std::make_shared<parquet::encryption::TestOnlyInMemoryKmsClientFactory>(
            /*wrap_locally=*/true, key_map_with_wrong_column_key_);
    crypto_factory_with_wrong_column_key_->RegisterKmsClientFactory(
        std::move(kms_client_factory));
  }

  // The dataset is partitioned using a directory partitioning scheme.
  void PrepareTableAndPartitioning() override {
    // Prepare table and partitioning.
    auto table_schema = schema({field("id", int8()), field("a", column_type_)});
    table_ = arrow::Table::Make(table_schema, {id_data_, column_data_});
    partitioning_ = std::make_shared<dataset::DirectoryPartitioning>(arrow::schema({}));
  }

  static const NestedFieldsEncryptionTestParam& GetParam() {
    return WithParamInterface::GetParam();
  }

  std::string_view ColumnName() override { return GetParam().column_name; }

  void TestDatasetColumnEncryption() {
    // Read the dataset with wrong column key
    ASSERT_OK_AND_ASSIGN(auto dataset,
                         OpenDataset(kBaseDir, crypto_factory_with_wrong_column_key_,
                                     key_map_with_wrong_column_key_));

    // Reuse the dataset above to scan it twice to make sure decryption works correctly.
    for (size_t i = 0; i < 2; ++i) {
      // Read the non-encrypted id column into table
      ASSERT_OK_AND_ASSIGN(auto id_scanner_builder, dataset->NewScan());
      ASSERT_OK(id_scanner_builder->Project({"id"}));
      ASSERT_OK_AND_ASSIGN(auto id_scanner, id_scanner_builder->Finish());
      ASSERT_OK_AND_ASSIGN(auto read_table, id_scanner->ToTable());

      // Verify the column was read correctly
      ASSERT_OK_AND_ASSIGN(auto combined_table, read_table->CombineChunks());
      ASSERT_OK(combined_table->ValidateFull());
      ASSERT_OK_AND_ASSIGN(auto expected_table, table_->RemoveColumn(1));
      AssertTablesEqual(*combined_table, *expected_table);

      // Read the encrypted column 'a' into table
      ASSERT_OK_AND_ASSIGN(auto a_scanner_builder, dataset->NewScan());
      ASSERT_OK(a_scanner_builder->Project({"a"}));
      ASSERT_OK_AND_ASSIGN(auto a_scanner, a_scanner_builder->Finish());
      ASSERT_RAISES_WITH_MESSAGE(IOError, "IOError: Failed decryption finalization",
                                 a_scanner->ToTable());
    }
  }

 protected:
  std::shared_ptr<DataType> column_type_;
  std::shared_ptr<Array> id_data_;
  std::shared_ptr<Array> column_data_;
  std::shared_ptr<parquet::encryption::CryptoFactory>
      crypto_factory_with_wrong_column_key_;
  std::unordered_map<std::string, SecureString> key_map_with_wrong_column_key_;
};

class ListFieldEncryptionTest : public NestedFieldsEncryptionTest {
 public:
  ListFieldEncryptionTest() {
    column_type_ = list(int32());
    column_data_ = ArrayFromJSON(column_type_, "[[1, 2, 3], [4, 5], [6]]");
  }
};

class MapFieldEncryptionTest : public NestedFieldsEncryptionTest {
 public:
  MapFieldEncryptionTest() {
    column_type_ = map(utf8(), int32());
    column_data_ =
        ArrayFromJSON(column_type_, R"([[["one", 1]], [["two", 2]], [["three", 3]]])");
  }
};

class StructFieldEncryptionTest : public NestedFieldsEncryptionTest {
 public:
  StructFieldEncryptionTest() {
    column_type_ = struct_({field("f1", int32()), field("f2", utf8())});
    column_data_ = ArrayFromJSON(
        column_type_,
        R"([{"f1":1, "f2":"one"}, {"f1":2, "f2":"two"}, {"f1":3, "f2":"three"}])");
  }
};

class DeepNestedFieldEncryptionTest : public NestedFieldsEncryptionTest {
 public:
  DeepNestedFieldEncryptionTest() {
    auto struct_type = struct_({field("f1", int32()), field("f2", utf8())});
    auto map_type = map(int32(), struct_type);
    auto list_type = list(map_type);

    column_type_ = list_type;
    column_data_ = ArrayFromJSON(column_type_, R"([
      [
        [[1, {"f1":1, "f2":"one"}], [2, {"f1":2, "f2":"two"}]],
        [[3, {"f1":3, "f2":"three"}]]
      ],
      [
        [[4, {"f1":4, "f2":"fur"}]]
      ],
      []
    ])");
  }
};

const auto kListFieldEncryptionTestParamValues =
    ::testing::Values(NestedFieldsEncryptionTestParam("a"),
                      NestedFieldsEncryptionTestParam("a.list.element"));

const auto kMapFieldEncryptionTestParamValues =
    ::testing::Values(NestedFieldsEncryptionTestParam("a"),
                      NestedFieldsEncryptionTestParam("a.key_value.key"),
                      NestedFieldsEncryptionTestParam("a.key_value.value"));

const auto kStructFieldEncryptionTestParamValues = ::testing::Values(
    NestedFieldsEncryptionTestParam("a"), NestedFieldsEncryptionTestParam("a.f1"),
    NestedFieldsEncryptionTestParam("a.f2"));

const auto kDeepNestedFieldEncryptionTestParamValues = ::testing::Values(
    NestedFieldsEncryptionTestParam("a"),
    NestedFieldsEncryptionTestParam("a.list.element.key_value.key"),
    NestedFieldsEncryptionTestParam("a.list.element.key_value.value.f1"),
    NestedFieldsEncryptionTestParam("a.list.element.key_value.value.f2"));

TEST_P(ListFieldEncryptionTest, ColumnKeys) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
  ASSERT_NO_FATAL_FAILURE(TestDatasetColumnEncryption());
}

TEST_P(MapFieldEncryptionTest, ColumnKeys) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
  ASSERT_NO_FATAL_FAILURE(TestDatasetColumnEncryption());
}

TEST_P(StructFieldEncryptionTest, ColumnKeys) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
  ASSERT_NO_FATAL_FAILURE(TestDatasetColumnEncryption());
}

TEST_P(DeepNestedFieldEncryptionTest, ColumnKeys) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
  ASSERT_NO_FATAL_FAILURE(TestDatasetColumnEncryption());
}

// Test writing and reading encrypted nested fields
INSTANTIATE_TEST_SUITE_P(List, ListFieldEncryptionTest,
                         kListFieldEncryptionTestParamValues);
INSTANTIATE_TEST_SUITE_P(Map, MapFieldEncryptionTest, kMapFieldEncryptionTestParamValues);
INSTANTIATE_TEST_SUITE_P(Struct, StructFieldEncryptionTest,
                         kStructFieldEncryptionTestParamValues);
INSTANTIATE_TEST_SUITE_P(DeepNested, DeepNestedFieldEncryptionTest,
                         kDeepNestedFieldEncryptionTestParamValues);

// GH-39444: This test covers the case where parquet dataset scanner crashes when
// processing encrypted datasets over 2^15 rows in multi-threaded mode.
class LargeRowCountEncryptionTest
    : public DatasetEncryptionTestBase<EncryptionTestParam> {
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

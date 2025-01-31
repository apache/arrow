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

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/util/logging.h>
#include <boost/container/container_fwd.hpp>
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
constexpr std::string_view kColumnName = "a";
constexpr std::string_view kBaseDir = "";

using arrow::internal::checked_pointer_cast;

namespace arrow {
namespace dataset {

// Base class to test writing and reading encrypted dataset.
class DatasetEncryptionTestBase : public ::testing::Test {
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
    std::stringstream column_key;
    column_key << kColumnMasterKeyId << ": " << ColumnKey();
    encryption_config->column_keys = column_key.str();
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
  virtual std::string_view ColumnKey() { return kColumnName; }

  void TestScanDataset() {
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

    // Reuse the dataset above to scan it twice to make sure decryption works correctly.
    for (size_t i = 0; i < 2; ++i) {
      // Read dataset into table
      ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());
      ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());
      ASSERT_OK_AND_ASSIGN(auto read_table, scanner->ToTable());

      // Verify the data was read correctly
      ASSERT_OK_AND_ASSIGN(auto combined_table, read_table->CombineChunks());
      // Validate the table
      ASSERT_OK(combined_table->ValidateFull());
      AssertTablesEqual(*combined_table, *table_);
    }
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
TEST_F(DatasetEncryptionTest, WriteReadDatasetWithEncryption) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
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

class NestedFieldsEncryptionTest : public DatasetEncryptionTestBase,
                                   public ::testing::WithParamInterface<std::string> {
 public:
  NestedFieldsEncryptionTest() : rand_gen(0) {}

  // The dataset is partitioned using a Hive partitioning scheme.
  void PrepareTableAndPartitioning() override {
    // Prepare table and partitioning.
    auto table_schema = schema({field("a", std::move(column_type_))});
    table_ = arrow::Table::Make(table_schema, {column_data_});
    partitioning_ = std::make_shared<dataset::DirectoryPartitioning>(arrow::schema({}));
  }

  std::string_view ColumnKey() override { return GetParam(); }

 protected:
  std::shared_ptr<DataType> column_type_;
  std::shared_ptr<Array> column_data_;
  arrow::random::RandomArrayGenerator rand_gen;
};

class ListFieldEncryptionTest : public NestedFieldsEncryptionTest {
 public:
  ListFieldEncryptionTest() {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto value_builder = std::make_shared<arrow::Int32Builder>(pool);
    arrow::ListBuilder list_builder = arrow::ListBuilder(pool, value_builder);
    ARROW_CHECK_OK(list_builder.Append());
    ARROW_CHECK_OK(value_builder->Append(1));
    ARROW_CHECK_OK(value_builder->Append(2));
    ARROW_CHECK_OK(value_builder->Append(3));
    ARROW_CHECK_OK(list_builder.Append());
    ARROW_CHECK_OK(value_builder->Append(4));
    ARROW_CHECK_OK(value_builder->Append(5));
    ARROW_CHECK_OK(list_builder.Append());
    ARROW_CHECK_OK(value_builder->Append(6));

    std::shared_ptr<arrow::Array> list_array;
    arrow::Status status = list_builder.Finish(&list_array);

    column_type_ = list(int32());
    column_data_ = list_array;
  }
};

class MapFieldEncryptionTest : public NestedFieldsEncryptionTest {
 public:
  MapFieldEncryptionTest() : NestedFieldsEncryptionTest() {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto map_type = map(utf8(), int32());
    auto key_builder = std::make_shared<arrow::StringBuilder>(pool);
    auto item_builder = std::make_shared<arrow::Int32Builder>(pool);
    auto map_builder =
        std::make_shared<arrow::MapBuilder>(pool, key_builder, item_builder, map_type);
    ARROW_CHECK_OK(map_builder->Append());
    ARROW_CHECK_OK(key_builder->Append("one"));
    ARROW_CHECK_OK(item_builder->Append(1));
    ARROW_CHECK_OK(map_builder->Append());
    ARROW_CHECK_OK(key_builder->Append("two"));
    ARROW_CHECK_OK(item_builder->Append(2));
    ARROW_CHECK_OK(map_builder->Append());
    ARROW_CHECK_OK(key_builder->Append("three"));
    ARROW_CHECK_OK(item_builder->Append(3));

    std::shared_ptr<arrow::Array> map_array;
    ARROW_CHECK_OK(map_builder->Finish(&map_array));

    column_type_ = map_type;
    column_data_ = map_array;
  }
};

class StructFieldEncryptionTest : public NestedFieldsEncryptionTest {
 public:
  StructFieldEncryptionTest() : NestedFieldsEncryptionTest() {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto struct_type = struct_({field("f1", int32()), field("f2", utf8())});
    auto f1_builder = std::make_shared<arrow::Int32Builder>(pool);
    auto f2_builder = std::make_shared<arrow::StringBuilder>(pool);
    std::vector<std::shared_ptr<ArrayBuilder>> value_builders = {f1_builder, f2_builder};
    auto struct_builder = std::make_shared<arrow::StructBuilder>(std::move(struct_type),
                                                                 pool, value_builders);
    ARROW_CHECK_OK(struct_builder->Append());
    ARROW_CHECK_OK(f1_builder->Append(1));
    ARROW_CHECK_OK(f2_builder->Append("one"));
    ARROW_CHECK_OK(struct_builder->Append());
    ARROW_CHECK_OK(f1_builder->Append(2));
    ARROW_CHECK_OK(f2_builder->Append("two"));
    ARROW_CHECK_OK(struct_builder->Append());
    ARROW_CHECK_OK(f1_builder->Append(3));
    ARROW_CHECK_OK(f2_builder->Append("three"));

    std::shared_ptr<arrow::Array> struct_array;
    ARROW_CHECK_OK(struct_builder->Finish(&struct_array));

    column_type_ = struct_type;
    column_data_ = struct_array;
  }
};

class DeepNestedFieldEncryptionTest : public NestedFieldsEncryptionTest {
 public:
  DeepNestedFieldEncryptionTest() : NestedFieldsEncryptionTest() {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    auto struct_type = struct_({field("f1", int32()), field("f2", utf8())});
    auto f1_builder = std::make_shared<arrow::Int32Builder>(pool);
    auto f2_builder = std::make_shared<arrow::StringBuilder>(pool);
    std::vector<std::shared_ptr<ArrayBuilder>> value_builders = {f1_builder, f2_builder};
    auto struct_builder = std::make_shared<arrow::StructBuilder>(std::move(struct_type),
                                                                 pool, value_builders);

    auto map_type = map(int32(), struct_type);
    auto key_builder = std::make_shared<arrow::Int32Builder>(pool);
    auto item_builder = struct_builder;
    auto map_builder =
        std::make_shared<arrow::MapBuilder>(pool, key_builder, item_builder, map_type);

    auto list_type = list(map_type);
    auto value_builder = map_builder;
    arrow::ListBuilder list_builder = arrow::ListBuilder(pool, value_builder);

    ARROW_CHECK_OK(list_builder.Append());
    ARROW_CHECK_OK(value_builder->Append());

    ARROW_CHECK_OK(key_builder->Append(1));
    ARROW_CHECK_OK(item_builder->Append());
    ARROW_CHECK_OK(f1_builder->Append(1));
    ARROW_CHECK_OK(f2_builder->Append("one"));

    ARROW_CHECK_OK(key_builder->Append(1));
    ARROW_CHECK_OK(item_builder->Append());
    ARROW_CHECK_OK(f1_builder->Append(2));
    ARROW_CHECK_OK(f2_builder->Append("two"));

    ARROW_CHECK_OK(value_builder->Append());

    ARROW_CHECK_OK(key_builder->Append(3));
    ARROW_CHECK_OK(item_builder->Append());
    ARROW_CHECK_OK(f1_builder->Append(3));
    ARROW_CHECK_OK(f2_builder->Append("three"));

    ARROW_CHECK_OK(list_builder.Append());
    ARROW_CHECK_OK(value_builder->Append());

    ARROW_CHECK_OK(key_builder->Append(4));
    ARROW_CHECK_OK(item_builder->Append());
    ARROW_CHECK_OK(f1_builder->Append(4));
    ARROW_CHECK_OK(f2_builder->Append("four"));

    std::shared_ptr<arrow::Array> list_array;
    arrow::Status status = list_builder.Finish(&list_array);

    column_type_ = list_type;
    column_data_ = list_array;
  }
};

// Test writing and reading encrypted nested fields
INSTANTIATE_TEST_SUITE_P(List, ListFieldEncryptionTest,
                         ::testing::Values("a", "a.list.element"));
INSTANTIATE_TEST_SUITE_P(Map, MapFieldEncryptionTest,
                         ::testing::Values("a", "a.key", "a.value", "a.key_value.key",
                                           "a.key_value.value"));
INSTANTIATE_TEST_SUITE_P(Struct, StructFieldEncryptionTest,
                         ::testing::Values("a", "a.f1", "a.f2"));
INSTANTIATE_TEST_SUITE_P(DeepNested, DeepNestedFieldEncryptionTest,
                         ::testing::Values("a", "a.list.element",
                                           "a.list.element.key_value.key",
                                           "a.list.element.key_value.value",
                                           "a.list.element.key_value.value.f1",
                                           "a.list.element.key_value.value.f2"));

TEST_P(ListFieldEncryptionTest, ColumnKeys) { TestScanDataset(); }

TEST_P(MapFieldEncryptionTest, ColumnKeys) { TestScanDataset(); }

TEST_P(StructFieldEncryptionTest, ColumnKeys) { TestScanDataset(); }

TEST_P(DeepNestedFieldEncryptionTest, ColumnKeys) { TestScanDataset(); }

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
TEST_F(LargeRowEncryptionTest, ReadEncryptLargeRows) {
  ASSERT_NO_FATAL_FAILURE(TestScanDataset());
}

}  // namespace dataset
}  // namespace arrow

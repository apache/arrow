// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/api.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/parquet_encryption_config.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/util/secure_string.h"
#include "parquet/encryption/crypto_factory.h"
#include "parquet/encryption/test_in_memory_kms.h"

#include <iostream>

namespace fs = arrow::fs;

namespace ds = arrow::dataset;

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  auto int_builder = arrow::Int32Builder();

  std::shared_ptr<arrow::Array> arr_i;
  ARROW_RETURN_NOT_OK(int_builder.AppendValues({1, 3, 5, 7, 1}));
  ARROW_RETURN_NOT_OK(int_builder.Finish(&arr_i));

  auto struct_type = arrow::struct_({{"a", arrow::int32()}, {"b", arrow::int64()}});
  auto pool = arrow::default_memory_pool();
  auto a_builder = std::make_shared<arrow::Int32Builder>();
  auto b_builder = std::make_shared<arrow::Int64Builder>();
  auto struct_builder = arrow::StructBuilder(struct_type, pool, {a_builder, b_builder});

  std::shared_ptr<arrow::Array> arr_struct;
  ARROW_RETURN_NOT_OK(struct_builder.Append());
  ARROW_RETURN_NOT_OK(a_builder->Append(2));
  ARROW_RETURN_NOT_OK(b_builder->Append(20));
  ARROW_RETURN_NOT_OK(struct_builder.Append());
  ARROW_RETURN_NOT_OK(a_builder->Append(4));
  ARROW_RETURN_NOT_OK(b_builder->Append(40));
  ARROW_RETURN_NOT_OK(struct_builder.Append());
  ARROW_RETURN_NOT_OK(a_builder->Append(6));
  ARROW_RETURN_NOT_OK(b_builder->Append(60));
  ARROW_RETURN_NOT_OK(struct_builder.Append());
  ARROW_RETURN_NOT_OK(a_builder->Append(8));
  ARROW_RETURN_NOT_OK(b_builder->Append(80));
  ARROW_RETURN_NOT_OK(struct_builder.Append());
  ARROW_RETURN_NOT_OK(a_builder->Append(10));
  ARROW_RETURN_NOT_OK(b_builder->Append(100));
  ARROW_RETURN_NOT_OK(struct_builder.Finish(&arr_struct));

  auto map_type = arrow::map(arrow::int32(), arrow::utf8());
  auto key_builder = std::make_shared<arrow::Int32Builder>();
  auto item_builder = std::make_shared<arrow::StringBuilder>();
  auto map_builder = arrow::MapBuilder(pool, key_builder, item_builder, map_type);

  std::shared_ptr<arrow::Array> arr_map;
  ARROW_RETURN_NOT_OK(map_builder.Append());
  ARROW_RETURN_NOT_OK(key_builder->AppendValues({2, 4}));
  ARROW_RETURN_NOT_OK(item_builder->AppendValues({"2", "4"}));
  ARROW_RETURN_NOT_OK(map_builder.Append());
  ARROW_RETURN_NOT_OK(key_builder->AppendValues({6}));
  ARROW_RETURN_NOT_OK(item_builder->AppendValues({"6"}));
  ARROW_RETURN_NOT_OK(map_builder.Append());
  ARROW_RETURN_NOT_OK(map_builder.Append());
  ARROW_RETURN_NOT_OK(key_builder->AppendValues({8, 10}));
  ARROW_RETURN_NOT_OK(item_builder->AppendValues({"8", "10"}));
  ARROW_RETURN_NOT_OK(map_builder.Append());
  ARROW_RETURN_NOT_OK(map_builder.Finish(&arr_map));

  auto list_type = arrow::list(arrow::int32());
  auto value_builder = std::make_shared<arrow::Int32Builder>();
  auto list_builder = arrow::ListBuilder(pool, value_builder, list_type);

  std::shared_ptr<arrow::Array> arr_list;
  ARROW_RETURN_NOT_OK(list_builder.Append());
  ARROW_RETURN_NOT_OK(value_builder->AppendValues({1, 2, 3}));
  ARROW_RETURN_NOT_OK(list_builder.Append());
  ARROW_RETURN_NOT_OK(value_builder->AppendValues({4, 5, 6}));
  ARROW_RETURN_NOT_OK(list_builder.Append());
  ARROW_RETURN_NOT_OK(value_builder->AppendValues({7}));
  ARROW_RETURN_NOT_OK(list_builder.Append());
  ARROW_RETURN_NOT_OK(value_builder->AppendValues({8}));
  ARROW_RETURN_NOT_OK(list_builder.Append());
  ARROW_RETURN_NOT_OK(list_builder.Finish(&arr_list));

  auto schema = arrow::schema({
      arrow::field("i", arrow::int32()),
      arrow::field("s", struct_type),
      arrow::field("m", map_type),
      arrow::field("l", list_type),
  });

  return arrow::Table::Make(schema, {arr_i, arr_struct, arr_map, arr_list});
}

std::shared_ptr<parquet::encryption::CryptoFactory> GetCryptoFactory() {
  // Configure KMS.
  std::unordered_map<std::string, arrow::util::SecureString> key_map;
  key_map.emplace("footerKeyId", arrow::util::SecureString("0123456789012345"));
  key_map.emplace("columnKeyId", arrow::util::SecureString("1234567890123456"));

  auto crypto_factory = std::make_shared<parquet::encryption::CryptoFactory>();
  auto kms_client_factory =
      // for testing only, do not use it as an example of KmsClientFactory implementation
      std::make_shared<parquet::encryption::TestOnlyInMemoryKmsClientFactory>(
          /*wrap_locally=*/true, key_map);
  crypto_factory->RegisterKmsClientFactory(std::move(kms_client_factory));
  return crypto_factory;
}

arrow::Status WriteEncryptedFile(const std::string& path_to_file) {
  using arrow::internal::checked_pointer_cast;

  // Get a configured crypto factory and kms connection conf.
  auto crypto_factory = GetCryptoFactory();
  auto kms_connection_config =
      std::make_shared<parquet::encryption::KmsConnectionConfig>();

  // Set write options with encryption configuration.
  auto encryption_config = std::make_shared<parquet::encryption::EncryptionConfiguration>(
      std::string("footerKeyId"));
  encryption_config->column_keys =
      "columnKeyId: i, s.a, s.b, m.key_value.key, m.key_value.value, l.list.element";

  auto parquet_encryption_config = std::make_shared<ds::ParquetEncryptionConfig>();
  // Directly assign shared_ptr objects to ParquetEncryptionConfig members.
  parquet_encryption_config->crypto_factory = crypto_factory;
  parquet_encryption_config->kms_connection_config = kms_connection_config;
  parquet_encryption_config->encryption_config = std::move(encryption_config);

  auto file_format = std::make_shared<ds::ParquetFileFormat>();
  auto parquet_file_write_options = checked_pointer_cast<ds::ParquetFileWriteOptions>(
      file_format->DefaultWriteOptions());
  parquet_file_write_options->parquet_encryption_config =
      std::move(parquet_encryption_config);

  // Write dataset.
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, GetTable());
  printf("%s", table->ToString().c_str());
  auto dataset = std::make_shared<ds::InMemoryDataset>(table);
  ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
  ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());

  auto file_system = std::make_shared<fs::LocalFileSystem>();
  auto partitioning = std::make_shared<ds::HivePartitioning>(
      arrow::schema({arrow::field("part", arrow::utf8())}));

  ds::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = parquet_file_write_options;
  write_options.filesystem = file_system;
  write_options.base_dir = path_to_file;
  write_options.partitioning = partitioning;
  write_options.basename_template = "part{i}.parquet";
  return ds::FileSystemDataset::Write(write_options, std::move(scanner));
}

arrow::Status ReadEncryptedFile(const std::string& path_to_file) {
  // Get a configured crypto factory and kms connection conf
  auto crypto_factory = GetCryptoFactory();
  auto kms_connection_config =
      std::make_shared<parquet::encryption::KmsConnectionConfig>();

  // Create decryption properties.
  auto decryption_config =
      std::make_shared<parquet::encryption::DecryptionConfiguration>();
  auto parquet_decryption_config = std::make_shared<ds::ParquetDecryptionConfig>();
  parquet_decryption_config->crypto_factory = crypto_factory;
  parquet_decryption_config->kms_connection_config = kms_connection_config;
  parquet_decryption_config->decryption_config = std::move(decryption_config);

  // Set scan options.
  auto parquet_scan_options = std::make_shared<ds::ParquetFragmentScanOptions>();
  parquet_scan_options->parquet_decryption_config = std::move(parquet_decryption_config);

  // Get configured Parquet file format
  auto file_format = std::make_shared<ds::ParquetFileFormat>();
  file_format->default_fragment_scan_options = std::move(parquet_scan_options);

  // Get the FileSystem.
  auto file_system = std::make_shared<fs::LocalFileSystem>();

  // Get FileInfo objects for all files under the base directory
  fs::FileSelector selector;
  selector.base_dir = path_to_file;
  selector.recursive = true;

  // Create the dataset
  ds::FileSystemFactoryOptions factory_options;
  ARROW_ASSIGN_OR_RAISE(auto dataset_factory,
                        ds::FileSystemDatasetFactory::Make(file_system, selector,
                                                           file_format, factory_options));
  ARROW_ASSIGN_OR_RAISE(auto dataset, dataset_factory->Finish());
  ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
  ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
  ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());
  std::cout << "Table size: " << table->num_rows() << "\n";
  return arrow::Status::OK();
}

arrow::Status RunExamples(const std::string& path_to_file) {
  ARROW_RETURN_NOT_OK(WriteEncryptedFile(path_to_file));
  ARROW_RETURN_NOT_OK(ReadEncryptedFile(path_to_file));
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  std::string path_to_file = argv[1];
  arrow::Status status = RunExamples(path_to_file);

  if (!status.ok()) {
    std::cerr << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

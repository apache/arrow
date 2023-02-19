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

#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>

#include <arrow/io/file.h>
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/logging.h"

#include "parquet/encryption/crypto_factory.h"
#include "parquet/encryption/key_toolkit.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/encryption/test_in_memory_kms.h"
#include "parquet/file_reader.h"
#include "parquet/test_util.h"

namespace parquet {
namespace encryption {
namespace test {

class TestEncryptionKeyManagement : public ::testing::Test {
 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;
  FileEncryptor encryptor_;
  FileDecryptor decryptor_;

  std::unordered_map<std::string, std::string> key_list_;
  std::unordered_map<std::string, std::string> new_key_list_;
  std::string column_key_mapping_;
  KmsConnectionConfig kms_connection_config_;
  CryptoFactory crypto_factory_;
  bool wrap_locally_;

  void SetUp() {
#ifndef ARROW_WITH_SNAPPY
    GTEST_SKIP() << "Test requires Snappy compression";
#endif
    key_list_ = BuildKeyMap(kColumnMasterKeyIds, kColumnMasterKeys, kFooterMasterKeyId,
                            kFooterMasterKey);
    new_key_list_ = BuildKeyMap(kColumnMasterKeyIds, kNewColumnMasterKeys,
                                kFooterMasterKeyId, kNewFooterMasterKey);
    column_key_mapping_ = BuildColumnKeyMapping();
    temp_dir_ = temp_data_dir().ValueOrDie();
  }

  void SetupCryptoFactory(bool wrap_locally) {
    wrap_locally_ = wrap_locally;
    std::shared_ptr<KmsClientFactory> kms_client_factory =
        std::make_shared<TestOnlyInMemoryKmsClientFactory>(wrap_locally, key_list_);
    crypto_factory_.RegisterKmsClientFactory(kms_client_factory);
  }

  std::string GetFileName(bool double_wrapping, bool wrap_locally,
                          bool internal_key_material, int encryption_no) {
    std::string file_name;
    file_name += double_wrapping ? "double_wrapping" : "no_double_wrapping";
    file_name += wrap_locally ? "-wrap_locally" : "-wrap_on_server";
    file_name +=
        internal_key_material ? "-internal_key_material" : "-external_key_material";
    switch (encryption_no) {
      case 0:
        file_name += "-encrypt_columns_and_footer_diff_keys";
        break;
      case 1:
        file_name += "-encrypt_columns_not_footer";
        break;
      case 2:
        file_name += "-encrypt_columns_and_footer_same_keys";
        break;
      case 3:
        file_name += "-encrypt_columns_and_footer_ctr";
        break;
      default:
        file_name += "-no_encrypt";
        break;
    }
    file_name += encryption_no == 4 ? ".parquet" : ".parquet.encrypted";
    return file_name;
  }

  EncryptionConfiguration GetEncryptionConfiguration(bool double_wrapping,
                                                     bool internal_key_material,
                                                     int encryption_no) {
    EncryptionConfiguration encryption(kFooterMasterKeyId);
    encryption.double_wrapping = double_wrapping;

    switch (encryption_no) {
      case 0:
        // encrypt some columns and footer, different keys
        encryption.column_keys = column_key_mapping_;
        break;
      case 1:
        // encrypt columns, plaintext footer, different keys
        encryption.column_keys = column_key_mapping_;
        encryption.plaintext_footer = true;
        break;
      case 2:
        // encrypt some columns and footer, same key
        encryption.uniform_encryption = true;
        break;
      case 3:
        // Encrypt two columns and the footer, with different keys.
        // Use AES_GCM_CTR_V1 algorithm.
        encryption.column_keys = column_key_mapping_;
        encryption.encryption_algorithm = ParquetCipher::AES_GCM_CTR_V1;
        break;
      default:
        // no encryption
        ARROW_LOG(FATAL) << "Invalid encryption_no";
    }

    encryption.internal_key_material = internal_key_material;
    return encryption;
  }

  DecryptionConfiguration GetDecryptionConfiguration() {
    return DecryptionConfiguration();
  }

  void WriteEncryptedParquetFile(bool double_wrapping, bool internal_key_material,
                                 int encryption_no) {
    std::string file_name =
        GetFileName(double_wrapping, wrap_locally_, internal_key_material, encryption_no);

    auto encryption_config =
        GetEncryptionConfiguration(double_wrapping, internal_key_material, encryption_no);

    std::string file_path = temp_dir_->path().ToString() + file_name;
    if (internal_key_material) {
      auto file_encryption_properties = crypto_factory_.GetFileEncryptionProperties(
          kms_connection_config_, encryption_config);
      encryptor_.EncryptFile(file_path, file_encryption_properties);
    } else {
      auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
      auto file_encryption_properties = crypto_factory_.GetFileEncryptionProperties(
          kms_connection_config_, encryption_config, file_path, file_system);
      encryptor_.EncryptFile(file_path, file_encryption_properties);
    }
  }

  void ReadEncryptedParquetFile(bool double_wrapping, bool internal_key_material,
                                int encryption_no) {
    auto decryption_config = GetDecryptionConfiguration();
    std::string file_name =
        GetFileName(double_wrapping, wrap_locally_, internal_key_material, encryption_no);
    std::string file_path = temp_dir_->path().ToString() + file_name;
    if (internal_key_material) {
      auto file_decryption_properties = crypto_factory_.GetFileDecryptionProperties(
          kms_connection_config_, decryption_config);

      decryptor_.DecryptFile(file_path, file_decryption_properties);
    } else {
      auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
      auto file_decryption_properties = crypto_factory_.GetFileDecryptionProperties(
          kms_connection_config_, decryption_config, file_path, file_system);

      decryptor_.DecryptFile(file_path, file_decryption_properties);
    }
  }

  void RotateKeys(bool double_wrapping, int encryption_no) {
    auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
    std::string file_name =
        GetFileName(double_wrapping, wrap_locally_, false, encryption_no);
    std::string file = temp_dir_->path().ToString() + file_name;

    crypto_factory_.RemoveCacheEntriesForAllTokens();
    TestOnlyInServerWrapKms::StartKeyRotation(new_key_list_);
    crypto_factory_.RotateMasterKeys(kms_connection_config_, file, file_system,
                                     double_wrapping);
    TestOnlyInServerWrapKms::FinishKeyRotation();
    crypto_factory_.RemoveCacheEntriesForAllTokens();
  }
};

class TestEncryptionKeyManagementMultiThread : public TestEncryptionKeyManagement {
 protected:
  void WriteEncryptedParquetFiles() {
    std::vector<std::thread> write_threads;
    for (const bool internal_key_material : {false, true}) {
      for (const bool double_wrapping : {false, true}) {
        for (int encryption_no = 0; encryption_no < 4; encryption_no++) {
          write_threads.push_back(std::thread(
              [this, double_wrapping, internal_key_material, encryption_no]() {
                this->WriteEncryptedParquetFile(double_wrapping, internal_key_material,
                                                encryption_no);
              }));
        }
      }
    }
    for (auto& th : write_threads) {
      th.join();
    }
  }

  void ReadEncryptedParquetFiles() {
    std::vector<std::thread> read_threads;
    for (const bool internal_key_material : {false, true}) {
      for (const bool double_wrapping : {false, true}) {
        for (int encryption_no = 0; encryption_no < 4; encryption_no++) {
          read_threads.push_back(std::thread(
              [this, double_wrapping, internal_key_material, encryption_no]() {
                this->ReadEncryptedParquetFile(double_wrapping, internal_key_material,
                                               encryption_no);
              }));
        }
      }
    }
    for (auto& th : read_threads) {
      th.join();
    }
  }
};

TEST_F(TestEncryptionKeyManagement, WrapLocally) {
  this->SetupCryptoFactory(true);

  for (const bool internal_key_material : {false, true}) {
    for (const bool double_wrapping : {false, true}) {
      for (int encryption_no = 0; encryption_no < 4; encryption_no++) {
        this->WriteEncryptedParquetFile(double_wrapping, internal_key_material,
                                        encryption_no);
        this->ReadEncryptedParquetFile(double_wrapping, internal_key_material,
                                       encryption_no);
      }
    }
  }
}

TEST_F(TestEncryptionKeyManagement, WrapOnServer) {
  this->SetupCryptoFactory(false);

  for (const bool internal_key_material : {false, true}) {
    for (const bool double_wrapping : {false, true}) {
      for (int encryption_no = 0; encryption_no < 4; encryption_no++) {
        this->WriteEncryptedParquetFile(double_wrapping, internal_key_material,
                                        encryption_no);
        this->ReadEncryptedParquetFile(double_wrapping, internal_key_material,
                                       encryption_no);
      }
    }
  }
}

TEST_F(TestEncryptionKeyManagement, CheckExternalKeyStoreWithNullFilePath) {
  bool internal_key_material = false;
  bool double_wrapping = true;
  int encryption_no = 0;
  this->SetupCryptoFactory(false);

  auto encryption_config =
      GetEncryptionConfiguration(double_wrapping, internal_key_material, encryption_no);

  EXPECT_THROW(crypto_factory_.GetFileEncryptionProperties(kms_connection_config_,
                                                           encryption_config),
               ParquetException);
}

TEST_F(TestEncryptionKeyManagement, CheckKeyRotationDoubleWrapping) {
  bool internal_key_material =
      false;  // Key rotation is not supported for internal key material
  bool double_wrapping = true;
  this->SetupCryptoFactory(
      false);  // key rotation is not supported with local key wrapping

  for (int encryption_no = 0; encryption_no < 4; encryption_no++) {
    TestOnlyInServerWrapKms::InitializeMasterKeys(key_list_);
    this->WriteEncryptedParquetFile(double_wrapping, internal_key_material,
                                    encryption_no);
    this->ReadEncryptedParquetFile(double_wrapping, internal_key_material, encryption_no);
    this->RotateKeys(double_wrapping, encryption_no);
    this->ReadEncryptedParquetFile(double_wrapping, internal_key_material, encryption_no);
  }
}

TEST_F(TestEncryptionKeyManagement, CheckKeyRotationSingleWrapping) {
  bool internal_key_material =
      false;  // Key rotation is not supported for internal key material
  bool double_wrapping = false;
  this->SetupCryptoFactory(
      false);  // key rotation is not supported with local key wrapping

  for (int encryption_no = 0; encryption_no < 4; encryption_no++) {
    TestOnlyInServerWrapKms::InitializeMasterKeys(key_list_);
    this->WriteEncryptedParquetFile(double_wrapping, internal_key_material,
                                    encryption_no);
    this->ReadEncryptedParquetFile(double_wrapping, internal_key_material, encryption_no);
    this->RotateKeys(double_wrapping, encryption_no);
    this->ReadEncryptedParquetFile(double_wrapping, internal_key_material, encryption_no);
  }
}

TEST_F(TestEncryptionKeyManagement, KeyRotationWithInternalMaterial) {
  bool internal_key_material = true;
  bool double_wrapping = false;
  this->SetupCryptoFactory(false);
  int encryption_no = 0;

  TestOnlyInServerWrapKms::InitializeMasterKeys(key_list_);
  this->WriteEncryptedParquetFile(double_wrapping, internal_key_material, encryption_no);
  // Key rotation requires external key material so this should throw an exception
  EXPECT_THROW(this->RotateKeys(double_wrapping, encryption_no), ParquetException);
}

TEST_F(TestEncryptionKeyManagementMultiThread, WrapLocally) {
  this->SetupCryptoFactory(true);

  this->WriteEncryptedParquetFiles();
  this->ReadEncryptedParquetFiles();
}

TEST_F(TestEncryptionKeyManagementMultiThread, WrapOnServer) {
  this->SetupCryptoFactory(false);

  this->WriteEncryptedParquetFiles();
  this->ReadEncryptedParquetFiles();
}

// Test reading a file written with parquet-mr using external key material
TEST_F(TestEncryptionKeyManagement, ReadParquetMRExternalKeyMaterialFile) {
  // This test file was created using the same key identifiers used in the test KMS,
  // so we don't need to modify the setup.
  this->SetupCryptoFactory(false);
  auto file_path = data_file("external_key_material_java.parquet.encrypted");
  auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
  auto decryption_config = GetDecryptionConfiguration();
  auto file_decryption_properties = crypto_factory_.GetFileDecryptionProperties(
      kms_connection_config_, decryption_config, file_path, file_system);

  parquet::ReaderProperties reader_properties = parquet::default_reader_properties();
  reader_properties.file_decryption_properties(file_decryption_properties->DeepClone());

  std::shared_ptr<::arrow::io::RandomAccessFile> source;
  PARQUET_ASSIGN_OR_THROW(source, ::arrow::io::ReadableFile::Open(
                                      file_path, reader_properties.memory_pool()));
  auto file_reader = parquet::ParquetFileReader::Open(source, reader_properties);
  auto file_metadata = file_reader->metadata();
  ASSERT_EQ(file_metadata->num_row_groups(), 1);
  ASSERT_EQ(file_metadata->num_columns(), 2);
  auto row_group = file_reader->RowGroup(0);
  auto num_rows = row_group->metadata()->num_rows();
  ASSERT_EQ(num_rows, 100);
  std::vector<int> int_values(num_rows);
  std::vector<parquet::ByteArray> string_values(num_rows);
  int64_t values_read;

  auto int_reader = std::dynamic_pointer_cast<parquet::Int32Reader>(row_group->Column(0));
  int_reader->ReadBatch(num_rows, nullptr, nullptr, int_values.data(), &values_read);
  ASSERT_EQ(values_read, num_rows);

  auto string_reader =
      std::dynamic_pointer_cast<parquet::ByteArrayReader>(row_group->Column(1));
  string_reader->ReadBatch(num_rows, nullptr, nullptr, string_values.data(),
                           &values_read);
  ASSERT_EQ(values_read, num_rows);

  std::vector<std::string> prefixes = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
  for (int64_t row = 0; row < num_rows; ++row) {
    ASSERT_EQ(int_values[row], row);
    std::string expected_string = prefixes[row % prefixes.size()] + std::to_string(row);
    std::string_view read_string(reinterpret_cast<const char*>(string_values[row].ptr),
                                 string_values[row].len);
    ASSERT_EQ(read_string, expected_string);
  }
}

}  // namespace test
}  // namespace encryption
}  // namespace parquet

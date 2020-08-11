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

#include "arrow/testing/util.h"

#include "parquet/in_memory_kms.h"
#include "parquet/key_toolkit.h"
#include "parquet/properties_driven_crypto_factory.h"
#include "parquet/test_encryption_util.h"
#include "parquet/test_util.h"

namespace parquet {

namespace test {

using encryption::DecryptionConfiguration;
using encryption::EncryptionConfiguration;
using encryption::KeyAccessToken;
using encryption::PropertiesDrivenCryptoFactory;

const char FOOTER_MASTER_KEY[] = "0123456789112345";
const char* const COLUMN_MASTER_KEYS[] = {"1234567890123450", "1234567890123451",
                                          "1234567890123452", "1234567890123453",
                                          "1234567890123454", "1234567890123455"};
const char* const COLUMN_MASTER_KEY_IDS[] = {"kc1", "kc2", "kc3", "kc4", "kc5", "kc6"};
const char FOOTER_MASTER_KEY_ID[] = "kf";

std::map<std::string, std::string> BuildKeyMap(const char* const* column_ids,
                                               const char* const* column_keys,
                                               const char* footer_id,
                                               const char* footer_key) {
  std::map<std::string, std::string> key_map;
  // add column keys
  for (int i = 0; i < 6; i++) {
    key_map.insert({column_ids[i], column_keys[i]});
  }
  // add footer key
  key_map.insert({footer_id, footer_key});

  return key_map;
}

std::string BuildColumnKeyMapping() {
  std::ostringstream stream;
  stream << COLUMN_MASTER_KEY_IDS[0] << ":" << DOUBLE_FIELD_NAME << ";"
         << COLUMN_MASTER_KEY_IDS[1] << ":" << FLOAT_FIELD_NAME << ";"
         << COLUMN_MASTER_KEY_IDS[2] << ":" << BOOLEAN_FIELD_NAME << ";"
         << COLUMN_MASTER_KEY_IDS[3] << ":" << INT32_FIELD_NAME << ";"
         << COLUMN_MASTER_KEY_IDS[4] << ":" << BA_FIELD_NAME << ";"
         << COLUMN_MASTER_KEY_IDS[5] << ":" << FLBA_FIELD_NAME << ";";
  return stream.str();
}

class TestEncrytionKeyManagement : public ::testing::Test {
 public:
  void SetUp() {
    key_list_ = BuildKeyMap(COLUMN_MASTER_KEY_IDS, COLUMN_MASTER_KEYS,
                            FOOTER_MASTER_KEY_ID, FOOTER_MASTER_KEY);
    column_key_mapping_ = BuildColumnKeyMapping();

    kms_connection_config_.refreshable_key_access_token =
        std::make_shared<KeyAccessToken>();
  }

 protected:
  FileEncryptor encryptor_;
  FileDecryptor decryptor_;

  std::map<std::string, std::string> key_list_;
  std::string column_key_mapping_;
  KmsConnectionConfig kms_connection_config_;

  void SetupCryptoFactory(PropertiesDrivenCryptoFactory& crypto_factory,
                          bool wrap_locally) {
    KeyToolkit::RemoveCacheEntriesForAllTokens();
    std::shared_ptr<KmsClientFactory> kms_client_factory =
        std::make_shared<InMemoryKmsClientFactory>(wrap_locally, key_list_);
    crypto_factory.kms_client_factory(kms_client_factory);
  }

  std::string GetFileName(bool double_wrapping, bool wrap_locally, int encryption_no) {
    std::string file_name;
    file_name += double_wrapping ? "double_wrapping" : "no_double_wrapping";
    file_name += wrap_locally ? "-wrap_locally" : "-no_wrap_locally";
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
      default:  // case 4:
        file_name += "-no_encrypt";
        break;
    }
    file_name += encryption_no == 4 ? ".parquet" : ".parquet.encrypted";
    return file_name;
  }

  std::vector<std::shared_ptr<EncryptionConfiguration>> GetEncryptionConfigurations(
      bool double_wrapping, bool wrap_locally) {
    std::vector<std::shared_ptr<EncryptionConfiguration>> configs;
    std::vector<EncryptionConfiguration::Builder*> config_builders;

    // encrypt some columns and footer, different keys
    EncryptionConfiguration::Builder builder1(FOOTER_MASTER_KEY_ID);
    builder1.column_keys(column_key_mapping_);
    config_builders.push_back(&builder1);

    // encrypt columns, plaintext footer, different keys
    EncryptionConfiguration::Builder builder2(FOOTER_MASTER_KEY_ID);
    builder2.column_keys(column_key_mapping_)->plaintext_footer(true);
    config_builders.push_back(&builder2);

    // encrypt some columns and footer, same key
    EncryptionConfiguration::Builder builder3(FOOTER_MASTER_KEY_ID);
    builder3.uniform_encryption();
    config_builders.push_back(&builder3);

    // Encrypt two columns and the footer, with different keys.
    // Use AES_GCM_CTR_V1 algorithm.
    EncryptionConfiguration::Builder builder4(FOOTER_MASTER_KEY_ID);
    builder4.column_keys(column_key_mapping_)
        ->encryption_algorithm(ParquetCipher::AES_GCM_CTR_V1);
    config_builders.push_back(&builder4);

    for (EncryptionConfiguration::Builder* builder : config_builders) {
      auto config =
          builder->double_wrapping(double_wrapping)->wrap_locally(wrap_locally)->build();
      configs.push_back(config);
    }

    // non encryption
    configs.push_back(NULL);

    return configs;
  }

  std::shared_ptr<DecryptionConfiguration> GetDecryptionConfiguration(bool wrap_locally) {
    DecryptionConfiguration::Builder builder;
    return builder.wrap_locally(wrap_locally)->build();
  }

  void WriteEncryptedParquetFiles() {
    for (int i = 0; i < 2; i++) {
      bool wrap_locally = (i == 0);

      PropertiesDrivenCryptoFactory crypto_factory;
      SetupCryptoFactory(crypto_factory, wrap_locally);

      for (int j = 0; j < 2; j++) {
        bool double_wrapping = (j == 0);
        auto encryption_configs =
            this->GetEncryptionConfigurations(double_wrapping, wrap_locally);
        for (size_t encryption_no = 0; encryption_no < encryption_configs.size();
             encryption_no++) {
          std::string file_name =
              GetFileName(double_wrapping, wrap_locally, encryption_no);
          std::cout << "Writing file: " << file_name << std::endl;

          auto encryption_config = encryption_configs[encryption_no];
          std::shared_ptr<FileEncryptionProperties> file_encryption_properties =
              crypto_factory.GetFileEncryptionProperties(kms_connection_config_,
                                                         encryption_config);

          std::string file = data_file(file_name.c_str());
          encryptor_.EncryptFile(file, file_encryption_properties);
        }
      }
    }
  }

  void ReadEncryptedParquetFiles() {
    for (int i = 0; i < 2; i++) {
      bool wrap_locally = (i == 0);

      PropertiesDrivenCryptoFactory crypto_factory;
      SetupCryptoFactory(crypto_factory, wrap_locally);

      auto decryption_config = this->GetDecryptionConfiguration(wrap_locally);
      for (int j = 0; j < 2; j++) {
        bool double_wrapping = (j == 0);
        for (size_t encryption_no = 0; encryption_no < 5; encryption_no++) {
          std::string file_name =
              GetFileName(double_wrapping, wrap_locally, encryption_no);
          std::cout << "Reading file: " << file_name << std::endl;

          std::shared_ptr<FileDecryptionProperties> file_decryption_properties =
              crypto_factory.GetFileDecryptionProperties(kms_connection_config_,
                                                         decryption_config);

          std::string file = data_file(file_name.c_str());
          decryptor_.DecryptFile(file, file_decryption_properties);
        }
      }
    }
  }
};

TEST_F(TestEncrytionKeyManagement, TestWriteReadEncryptedParquetFiles) {
  KeyToolkit::RemoveCacheEntriesForAllTokens();
  this->WriteEncryptedParquetFiles();
  this->ReadEncryptedParquetFiles();
}

}  // namespace test

}  // namespace parquet

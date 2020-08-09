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

std::vector<std::string> BuildKeyList(const char* const* column_ids,
                                      const char* const* column_keys,
                                      const char* footer_id, const char* footer_key) {
  std::vector<std::string> key_list;
  // add column keys
  for (int i = 0; i < 6; i++) {
    std::ostringstream stream;
    stream << column_ids[i] << ":" << column_keys[i];
    key_list.push_back(stream.str());
  }
  // add footer key
  std::ostringstream stream2;
  stream2 << footer_id << ":" << footer_key;
  key_list.push_back(stream2.str());

  return key_list;
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
    key_list_ = BuildKeyList(COLUMN_MASTER_KEY_IDS, COLUMN_MASTER_KEYS,
                             FOOTER_MASTER_KEY_ID, FOOTER_MASTER_KEY);
    column_key_mapping_ = BuildColumnKeyMapping();
  }

 protected:
  FileEncryptor encryptor_;
  FileDecryptor decryptor_;

  std::vector<std::string> key_list_;
  std::string column_key_mapping_;

  void SetupCryptoFactory(PropertiesDrivenCryptoFactory& crypto_factory) {
    std::shared_ptr<KmsClientFactory> kms_client_factory =
        std::make_shared<InMemoryKmsClientFactory>(key_list_);
    crypto_factory.kms_client_factory(kms_client_factory);
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

  void WriteEncryptedParquetFile(
      const KmsConnectionConfig& kms_connection_config,
      std::shared_ptr<EncryptionConfiguration> encryption_config,
      const std::string& file_name) {
    PropertiesDrivenCryptoFactory crypto_factory;

    SetupCryptoFactory(crypto_factory);

    std::shared_ptr<FileEncryptionProperties> file_encryption_properties =
        crypto_factory.GetFileEncryptionProperties(kms_connection_config,
                                                   encryption_config);
    std::string file = data_file(file_name.c_str());
    encryptor_.EncryptFile(file, file_encryption_properties);
  }

  void ReadEncryptedParquetFile(
      const KmsConnectionConfig& kms_connection_config,
      std::shared_ptr<DecryptionConfiguration> decryption_config,
      const std::string& file_name) {
    PropertiesDrivenCryptoFactory crypto_factory;

    std::shared_ptr<KmsClientFactory> kms_client_factory =
        std::make_shared<InMemoryKmsClientFactory>(key_list_);
    crypto_factory.kms_client_factory(kms_client_factory);

    std::shared_ptr<FileDecryptionProperties> file_decryption_properties =
        crypto_factory.GetFileDecryptionProperties(kms_connection_config,
                                                   decryption_config);

    std::string file = data_file(file_name.c_str());
    decryptor_.DecryptFile(file, file_decryption_properties);
  }

  std::string GetFileName(std::shared_ptr<EncryptionConfiguration> encryption_config) {
    std::string suffix = encryption_config == NULL ? ".parquet" : ".parquet.encrypted";
    return "demo" + suffix;  // TODO
  }
};

TEST_F(TestEncrytionKeyManagement, TestWriteReadEncryptedParquetFiles) {
  const std::vector<bool> bool_flags = {true, false};

  KmsConnectionConfig kms_connection_config;
  kms_connection_config.refreshable_key_access_token = std::make_shared<KeyAccessToken>();
  kms_connection_config.refreshable_key_access_token->Refresh(
      KmsClient::KEY_ACCESS_TOKEN_DEFAULT);

  KeyToolkit::RemoveCacheEntriesForAllTokens();
  for (int i = 0; i < 2; i++) {
    bool wrap_locally = (i == 0);
    auto decryption_config = this->GetDecryptionConfiguration(wrap_locally);
    for (int j = 0; j < 2; j++) {
      bool double_wrapping = (j == 0);
      auto encryption_configs =
          this->GetEncryptionConfigurations(double_wrapping, wrap_locally);
      for (auto encryption_config : encryption_configs) {
        std::cout << "double_wrapping:" << double_wrapping
                  << " wrap_locally:" << wrap_locally << std::endl;
        std::string file_name = GetFileName(encryption_config);
        this->WriteEncryptedParquetFile(kms_connection_config, encryption_config,
                                        file_name);
        this->ReadEncryptedParquetFile(kms_connection_config, decryption_config,
                                       file_name);
      }
    }
  }
}

}  // namespace test

}  // namespace parquet

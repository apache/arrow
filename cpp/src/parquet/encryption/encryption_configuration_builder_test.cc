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

#include "parquet/encryption/properties_driven_crypto_factory.h"
#include "parquet/encryption/test_encryption_util.h"

namespace parquet {
namespace encryption {
namespace test {

class TestEncryptionConfiguration : public ::testing::Test {
 public:
  void SetUp() { column_keys_ = BuildColumnKeyMapping(); }

 protected:
  std::string column_keys_;
};

TEST_F(TestEncryptionConfiguration, DefaultConfigurationWithColumnKeys) {
  EncryptionConfiguration::Builder builder(kFooterMasterKeyId);
  builder.column_keys(column_keys_);
  auto encryption_configuration = builder.build();

  ASSERT_EQ(encryption_configuration->footer_key(), kFooterMasterKeyId);
  ASSERT_EQ(encryption_configuration->column_keys(), column_keys_);
  ASSERT_EQ(encryption_configuration->encryption_algorithm(),
            kDefaultEncryptionAlgorithm);
  ASSERT_EQ(encryption_configuration->plaintext_footer(), kDefaultPlaintextFooter);
  ASSERT_EQ(encryption_configuration->double_wrapping(), kDefaultDoubleWrapping);
  ASSERT_EQ(encryption_configuration->cache_lifetime_seconds(),
            kDefaultCacheLifetimeSeconds);
  ASSERT_EQ(encryption_configuration->internal_key_material(),
            kDefaultInternalKeyMaterial);
  ASSERT_EQ(encryption_configuration->uniform_encryption(), kDefaultUniformEncryption);
  ASSERT_EQ(encryption_configuration->data_key_length_bits(), kDefaultDataKeyLengthBits);
}

TEST_F(TestEncryptionConfiguration, NonDefaultConfiguration) {
  EncryptionConfiguration::Builder builder(kFooterMasterKeyId);
  builder.column_keys(column_keys_)
      ->encryption_algorithm(ParquetCipher::AES_GCM_CTR_V1)
      ->plaintext_footer(true)
      ->double_wrapping(false)
      ->cache_lifetime_seconds(300)
      ->internal_key_material(false)
      ->data_key_length_bits(256);

  auto encryption_configuration = builder.build();

  ASSERT_EQ(encryption_configuration->footer_key(), kFooterMasterKeyId);
  ASSERT_EQ(encryption_configuration->column_keys(), column_keys_);
  ASSERT_EQ(encryption_configuration->encryption_algorithm(),
            ParquetCipher::AES_GCM_CTR_V1);
  ASSERT_EQ(encryption_configuration->plaintext_footer(), true);
  ASSERT_EQ(encryption_configuration->double_wrapping(), false);
  ASSERT_EQ(encryption_configuration->cache_lifetime_seconds(), 300);
  ASSERT_EQ(encryption_configuration->internal_key_material(), false);
  ASSERT_EQ(encryption_configuration->uniform_encryption(), false);
  ASSERT_EQ(encryption_configuration->data_key_length_bits(), 256);
}

TEST_F(TestEncryptionConfiguration, EitherColumnKeysOrUniformEncryption) {
  // none of column_keys() or uniform_encryption() is called
  {
    EncryptionConfiguration::Builder builder(kFooterMasterKeyId);
    EXPECT_THROW(builder.build(), ParquetException);
  }
  // both of column_keys() and uniform_encryption() are called
  {
    EncryptionConfiguration::Builder builder(kFooterMasterKeyId);
    builder.column_keys(column_keys_);
    EXPECT_THROW(builder.uniform_encryption(), ParquetException);
  }
  {
    EncryptionConfiguration::Builder builder(kFooterMasterKeyId);
    builder.uniform_encryption();
    EXPECT_THROW(builder.column_keys(column_keys_), ParquetException);
  }
}

TEST_F(TestEncryptionConfiguration, EmptyColumnKeys) {
  EncryptionConfiguration::Builder builder(kFooterMasterKeyId);
  EXPECT_THROW(builder.column_keys(""), ParquetException);
}

TEST_F(TestEncryptionConfiguration, WrongDataKeyLength) {
  std::vector<int> wrong_data_key_length_bits = {0, 32, 1000};
  for (size_t i = 0; i < wrong_data_key_length_bits.size(); i++) {
    int data_key_length = wrong_data_key_length_bits[i];
    EncryptionConfiguration::Builder builder(kFooterMasterKeyId);
    auto encryption_configuration =
        builder.column_keys(column_keys_)->data_key_length_bits(data_key_length)->build();

    PropertiesDrivenCryptoFactory factory;
    EXPECT_THROW(factory.GetFileEncryptionProperties(KmsConnectionConfig(),
                                                     encryption_configuration),
                 ParquetException);
  }
}

TEST(TestDecryptionConfiguration, DefaultConfiguration) {
  DecryptionConfiguration::Builder builder;
  auto decryption_configuration = builder.build();

  ASSERT_EQ(decryption_configuration->cache_lifetime_seconds(),
            kDefaultCacheLifetimeSeconds);
}

TEST(TestDecryptionConfiguration, NonDefaultConfiguration) {
  DecryptionConfiguration::Builder builder;
  auto decryption_configuration = builder.cache_lifetime_seconds(300)->build();

  ASSERT_EQ(decryption_configuration->cache_lifetime_seconds(), 300);
}

}  // namespace test
}  // namespace encryption
}  // namespace parquet

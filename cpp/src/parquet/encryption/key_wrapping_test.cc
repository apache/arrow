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

#include "parquet/encryption/file_key_unwrapper.h"
#include "parquet/encryption/file_key_wrapper.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/encryption/test_in_memory_kms.h"

namespace parquet {
namespace encryption {
namespace test {

class KeyWrappingTest : public ::testing::Test {
 public:
  void SetUp() {
    key_list_ = BuildKeyMap(kColumnMasterKeyIds, kColumnMasterKeys, kFooterMasterKeyId,
                            kFooterMasterKey);
  }

 protected:
  void WrapThenUnwrap(std::shared_ptr<FileKeyMaterialStore> key_material_store,
                      bool double_wrapping, bool is_wrap_locally) {
    double cache_entry_lifetime_seconds = 600;

    KeyToolkit key_toolkit;
    key_toolkit.RegisterKmsClientFactory(
        std::make_shared<TestOnlyInMemoryKmsClientFactory>(is_wrap_locally, key_list_));

    FileKeyWrapper wrapper(&key_toolkit, kms_connection_config_, key_material_store,
                           cache_entry_lifetime_seconds, double_wrapping);

    std::string key_metadata_json_footer =
        wrapper.GetEncryptionKeyMetadata(kFooterEncryptionKey, kFooterMasterKeyId, true);
    std::string key_metadata_json_column = wrapper.GetEncryptionKeyMetadata(
        kColumnEncryptionKey1, kColumnMasterKeyIds[0], false);

    FileKeyUnwrapper unwrapper(&key_toolkit, kms_connection_config_,
                               cache_entry_lifetime_seconds);
    std::string footer_key = unwrapper.GetKey(key_metadata_json_footer);
    ASSERT_EQ(footer_key, kFooterEncryptionKey);

    std::string column_key = unwrapper.GetKey(key_metadata_json_column);
    ASSERT_EQ(column_key, kColumnEncryptionKey1);
  }

  // TODO: this method will be removed when material external storage is supported
  void WrapThenUnwrapWithUnsupportedExternalStorage(bool double_wrapping,
                                                    bool is_wrap_locally) {
    double cache_entry_lifetime_seconds = 600;

    KeyToolkit key_toolkit;
    key_toolkit.RegisterKmsClientFactory(
        std::make_shared<TestOnlyInMemoryKmsClientFactory>(is_wrap_locally, key_list_));

    std::shared_ptr<FileKeyMaterialStore> unsupported_material_store =
        std::make_shared<FileKeyMaterialStore>();

    FileKeyWrapper wrapper(&key_toolkit, kms_connection_config_,
                           unsupported_material_store, cache_entry_lifetime_seconds,
                           double_wrapping);

    EXPECT_THROW(
        wrapper.GetEncryptionKeyMetadata(kFooterEncryptionKey, kFooterMasterKeyId, true),
        ParquetException);
  }

  std::unordered_map<std::string, std::string> key_list_;
  KmsConnectionConfig kms_connection_config_;
};

TEST_F(KeyWrappingTest, InternalMaterialStorage) {
  // key_material_store = NULL indicates that "key material" is stored inside parquet
  // file.
  this->WrapThenUnwrap(NULL, true, true);
  this->WrapThenUnwrap(NULL, true, false);
  this->WrapThenUnwrap(NULL, false, true);
  this->WrapThenUnwrap(NULL, false, false);
}

// TODO: this test should be updated when material external storage is supported
TEST_F(KeyWrappingTest, ExternalMaterialStorage) {
  this->WrapThenUnwrapWithUnsupportedExternalStorage(true, true);
}

}  // namespace test
}  // namespace encryption
}  // namespace parquet

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

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/status.h"

#include "parquet/encryption/file_key_unwrapper.h"
#include "parquet/encryption/file_key_wrapper.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/encryption/test_in_memory_kms.h"

namespace parquet {
namespace encryption {
namespace test {

class KeyWrappingTest : public ::testing::Test {
 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;

  void SetUp() {
    key_list_ = BuildKeyMap(kColumnMasterKeyIds, kColumnMasterKeys, kFooterMasterKeyId,
                            kFooterMasterKey);
    temp_dir_ = temp_data_dir().ValueOrDie();
  }

  void WrapThenUnwrap(bool internal_key_material, bool double_wrapping,
                      bool is_wrap_locally) {
    double cache_entry_lifetime_seconds = 600;
    std::shared_ptr<FileKeyMaterialStore> key_material_store;
    std::shared_ptr<::arrow::fs::FileSystem> file_system = nullptr;
    std::string file_name;
    if (internal_key_material) {
      key_material_store = nullptr;
    } else {
      file_name += double_wrapping ? "double_wrapping" : "no_double_wrapping";
      file_name += is_wrap_locally ? "-wrap_locally" : "-wrap_on_server";
      file_name +=
          internal_key_material ? "-internal_key_material" : "-external_key_material";

      file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
      std::string writeable_file_path(temp_dir_->path().ToString() + file_name);
      try {
        key_material_store =
            FileSystemKeyMaterialStore::Make(writeable_file_path, file_system, false);
      } catch (ParquetException& e) {
        std::stringstream ss;
        ss << "Failed to get key material store" << e.what() << "\n";
        throw ParquetException(ss.str());
      }
    }

    KeyToolkit key_toolkit;
    key_toolkit.RegisterKmsClientFactory(
        std::make_shared<TestOnlyInMemoryKmsClientFactory>(is_wrap_locally, key_list_));

    FileKeyWrapper wrapper(&key_toolkit, kms_connection_config_, key_material_store,
                           cache_entry_lifetime_seconds, double_wrapping);

    std::string key_metadata_json_footer =
        wrapper.GetEncryptionKeyMetadata(kFooterEncryptionKey, kFooterMasterKeyId, true);
    std::string key_metadata_json_column = wrapper.GetEncryptionKeyMetadata(
        kColumnEncryptionKey1, kColumnMasterKeyIds[0], false);

    if (key_material_store != nullptr) key_material_store->SaveMaterial();

    std::string readable_file_path;
    if (!internal_key_material) {
      readable_file_path = temp_dir_->path().ToString() + file_name;
    }

    FileKeyUnwrapper unwrapper(&key_toolkit, kms_connection_config_,
                               cache_entry_lifetime_seconds, readable_file_path,
                               file_system);
    std::string footer_key = unwrapper.GetKey(key_metadata_json_footer);
    ASSERT_EQ(footer_key, kFooterEncryptionKey);

    std::string column_key = unwrapper.GetKey(key_metadata_json_column);
    ASSERT_EQ(column_key, kColumnEncryptionKey1);
  }

  std::unordered_map<std::string, std::string> key_list_;
  KmsConnectionConfig kms_connection_config_;
};

TEST_F(KeyWrappingTest, InternalMaterialStorage) {
  this->WrapThenUnwrap(true, true, true);
  this->WrapThenUnwrap(true, true, false);
  this->WrapThenUnwrap(true, false, true);
  this->WrapThenUnwrap(true, false, false);
}

TEST_F(KeyWrappingTest, ExternalMaterialStorage) {
  this->WrapThenUnwrap(false, true, true);
  this->WrapThenUnwrap(false, true, false);
  this->WrapThenUnwrap(false, false, true);
  this->WrapThenUnwrap(false, false, false);
}

}  // namespace test
}  // namespace encryption
}  // namespace parquet

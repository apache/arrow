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

#pragma once

#include "arrow/util/base64.h"

#include "parquet/key_toolkit.h"
#include "parquet/kms_client_factory.h"
#include "parquet/remote_kms_client.h"
#include "parquet/string_util.h"

using parquet::encryption::KeyToolkit;
using parquet::encryption::KmsClient;
using parquet::encryption::KmsClientFactory;
using parquet::encryption::KmsConnectionConfig;
using parquet::encryption::RemoteKmsClient;

namespace parquet {
namespace test {

using parquet::encryption::RemoteKmsClient;

// This is a mock class, built for testing only. Don't use it as an example of KmsClient
// implementation. (VaultClient is the sample implementation).
class InMemoryKms : public RemoteKmsClient {
 public:
  void InitializeMasterKey(const std::vector<std::string>& master_keys);

 protected:
  void InitializeInternal() override;

  std::string WrapKeyInServer(const std::vector<uint8_t>& key_bytes,
                              const std::string& master_key_identifier) override;

  std::vector<uint8_t> UnwrapKeyInServer(
      const std::string& wrapped_key, const std::string& master_key_identifier) override;

  std::vector<uint8_t> GetMasterKeyFromServer(
      const std::string& master_key_identifier) override;

 private:
  static std::map<std::string, std::vector<uint8_t>> ParseKeyList(
      const std::vector<std::string>& master_keys);

  static std::map<std::string, std::vector<uint8_t>> master_key_map_;
};

class InMemoryKmsClientFactory : public KmsClientFactory {
 public:
  explicit InMemoryKmsClientFactory(const std::vector<std::string>& master_keys)
      : master_keys_(master_keys) {}

  std::shared_ptr<KmsClient> CreateKmsClient(
      const KmsConnectionConfig& kms_connection_config, bool is_wrap_locally) {
    std::shared_ptr<InMemoryKms> in_memory_kms(new InMemoryKms);
    in_memory_kms->Initialize(kms_connection_config, is_wrap_locally);
    in_memory_kms->InitializeMasterKey(master_keys_);
    return in_memory_kms;
  }

 private:
  std::vector<std::string> master_keys_;
};

}  // namespace test
}  // namespace parquet

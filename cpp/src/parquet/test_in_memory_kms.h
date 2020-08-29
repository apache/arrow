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
#include "parquet/platform.h"
#include "parquet/remote_kms_client.h"

namespace parquet {
namespace encryption {

// This is a mock class, built for testing only. Don't use it as an example of KmsClient
// implementation.
class PARQUET_EXPORT TestOnlyInMemoryKms : public RemoteKmsClient {
 public:
  static void InitializeMasterKeys(
      const std::map<std::string, std::string>& master_keys_map);

 protected:
  void InitializeInternal() override;

  std::string WrapKeyInServer(const std::string& key_bytes,
                              const std::string& master_key_identifier) override;

  std::string UnwrapKeyInServer(const std::string& wrapped_key,
                                const std::string& master_key_identifier) override;

  std::string GetMasterKeyFromServer(const std::string& master_key_identifier) override;

 private:
  static std::map<std::string, std::string> master_key_map_;
};

class PARQUET_EXPORT TestOnlyInMemoryKmsClientFactory : public KmsClientFactory {
 public:
  TestOnlyInMemoryKmsClientFactory(
      bool wrap_locally, const std::map<std::string, std::string>& master_keys_map)
      : KmsClientFactory(wrap_locally) {
    TestOnlyInMemoryKms::InitializeMasterKeys(master_keys_map);
  }

  std::shared_ptr<KmsClient> CreateKmsClient(
      const KmsConnectionConfig& kms_connection_config) {
    std::shared_ptr<TestOnlyInMemoryKms> in_memory_kms =
        std::make_shared<TestOnlyInMemoryKms>();
    in_memory_kms->Initialize(kms_connection_config, wrap_locally_);
    return in_memory_kms;
  }
};

}  // namespace encryption
}  // namespace parquet

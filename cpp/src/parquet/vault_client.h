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

#include <map>
#include <string>

#include <boost/asio.hpp>

#include "parquet/kms_client_factory.h"
#include "parquet/remote_kms_client.h"

using boost::asio::streambuf;

namespace parquet {
namespace encryption {

// An example of KmsClient implementation. Not for production use!
// Vault server is set up with
// 1) the Transit secrets engine enabled,
// 2) two master keys in the Transit engine, named “k1” and “k2” (not exportable keys)
// 3) a token, and a policy allowing the token bearer to use these keys
//
// kms_connection_config.refreshable_key_access_token =
//     std::make_shared<KeyAccessToken>("<vault token>");
// kms_connection_config.kms_instance_url = "<vault server url>";

class VaultClient : public RemoteKmsClient {
 protected:
  void InitializeInternal() override;

  std::string WrapKeyInServer(const std::string& key_bytes,
                              const std::string& master_key_identifier) override;

  std::string UnwrapKeyInServer(const std::string& wrapped_key,
                                const std::string& master_key_identifier) override;

 protected:
  std::string GetMasterKeyFromServer(const std::string& master_key_identifier) override;

 private:
  std::string GetContentFromTransitEngine(const std::string& sub_path,
                                          const std::string& json_payload,
                                          const std::string& master_key_identifier);
  std::string BuildPayload(const std::map<std::string, std::string>& param_map);
  std::string ExecuteAndGetResponse(const std::string& end_point,
                                    boost::asio::streambuf& request);
  static std::string ParseReturn(const std::string& response,
                                 const std::string& search_key);

  std::string end_point_prefix_;
  std::string host_;
  int32_t port_;
};

class VaultClientFactory : public KmsClientFactory {
 public:
  // Vault supports in-server wrapping and unwrapping, not wrapping locally.
  VaultClientFactory() : KmsClientFactory(false) {}

  std::shared_ptr<KmsClient> CreateKmsClient(
      const KmsConnectionConfig& kms_connection_config) {
    std::shared_ptr<VaultClient> vault_client(new VaultClient);
    vault_client->Initialize(kms_connection_config, wrap_locally_);
    return vault_client;
  }
};

}  // namespace encryption
}  // namespace parquet

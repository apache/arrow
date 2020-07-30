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

#include <map>
#include <vector>

#include "parquet/kms_client.h"

namespace parquet {

namespace encryption {

class RemoteKmsClient : public KmsClient {
 public:
  static constexpr char LOCAL_WRAP_NO_KEY_VERSION[] = "NO_VERSION";

  void Initialize(const KmsConnectionConfig& kms_connection_config,
                  bool is_wrap_locally) override;

  std::string WrapKey(std::shared_ptr<arrow::Buffer> key_bytes,
                      const std::string& master_key_identifier) override;

  std::vector<uint8_t> UnwrapKey(const std::string& wrapped_key,
                                 const std::string& master_key_identifier) override;

 protected:
  virtual std::string WrapKeyInServer(std::shared_ptr<arrow::Buffer> key_bytes,
                                      const std::string& master_key_identifier) = 0;

  virtual std::vector<uint8_t> UnwrapKeyInServer(
      const std::string& wrapped_key, const std::string& master_key_identifier) = 0;

  virtual std::vector<uint8_t> GetMasterKeyFromServer(
      const std::string& master_key_identifier) = 0;

  virtual void InitializeInternal() = 0;

 private:
  class LocalKeyWrap {
   public:
    static constexpr char LOCAL_WRAP_KEY_VERSION_FIELD[] = "masterKeyVersion";
    static constexpr char LOCAL_WRAP_ENCRYPTED_KEY_FIELD[] = "encryptedKey";

    LocalKeyWrap(const std::string& master_key_version,
                 const std::string& encrypted_encoded_key);

    static std::string CreateSerialized(const std::string& encrypted_encoded_key);

    static LocalKeyWrap Parse(const std::string& wrapped_key);

    const std::string& master_key_version() const { return master_key_version_; }

    const std::string& encrypted_encoded_key() const { return encrypted_encoded_key_; }

   private:
    std::string encrypted_encoded_key_;
    std::string master_key_version_;
  };

  void RefreshToken();
  std::vector<uint8_t> GetKeyFromServer(const std::string& key_identifier);

  KmsConnectionConfig kms_connection_config_;
  bool is_wrap_locally_;
  bool is_default_token_;
  std::map<std::string, std::vector<uint8_t>> master_key_cache_;
};

}  // namespace encryption

}  // namespace parquet

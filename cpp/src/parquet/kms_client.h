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

#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/buffer.h"

#include "parquet/exception.h"

namespace parquet {

namespace encryption {

class KeyAccessToken {
 public:
  void Refresh(const std::string& new_value) { value_ = new_value; }
  const std::string& value() const { return value_; }

 private:
  std::string value_;
};

struct KmsConnectionConfig {
  std::string kms_instance_id;
  std::string kms_instance_url;
  std::shared_ptr<KeyAccessToken> refreshable_key_access_token;
  std::unordered_map<std::string, std::string> custom_kms_conf;

  const std::string& key_access_token() const {
    if (refreshable_key_access_token == NULL ||
        refreshable_key_access_token->value().empty()) {
      throw ParquetException("key access token is not set!");
    }
    return refreshable_key_access_token->value();
  }
};

class KmsClient {
 public:
  static constexpr char KMS_INSTANCE_ID_DEFAULT[] = "DEFAULT";
  static constexpr char KMS_INSTANCE_URL_DEFAULT[] = "DEFAULT";
  static constexpr char KEY_ACCESS_TOKEN_DEFAULT[] = "DEFAULT";

  virtual void Initialize(const KmsConnectionConfig& kms_connection_config,
                          bool is_wrap_locally) = 0;

  virtual std::string WrapKey(const std::vector<uint8_t>& key_bytes,
                              const std::string& master_key_identifier) = 0;

  virtual std::vector<uint8_t> UnwrapKey(const std::string& wrapped_key,
                                const std::string& master_key_identifier) = 0;
};

}  // namespace encryption

}  // namespace parquet

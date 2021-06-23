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

#include "parquet/encryption/kms_client.h"

namespace parquet {
namespace encryption {

constexpr const char KmsClient::kKmsInstanceIdDefault[];
constexpr const char KmsClient::kKmsInstanceUrlDefault[];
constexpr const char KmsClient::kKeyAccessTokenDefault[];

KmsConnectionConfig::KmsConnectionConfig()
    : refreshable_key_access_token(
          std::make_shared<KeyAccessToken>(KmsClient::kKeyAccessTokenDefault)) {}

void KmsConnectionConfig::SetDefaultIfEmpty() {
  if (kms_instance_id.empty()) {
    kms_instance_id = KmsClient::kKmsInstanceIdDefault;
  }
  if (kms_instance_url.empty()) {
    kms_instance_url = KmsClient::kKmsInstanceUrlDefault;
  }
  if (refreshable_key_access_token == NULL) {
    refreshable_key_access_token = std::make_shared<KeyAccessToken>();
  }
}

}  // namespace encryption
}  // namespace parquet

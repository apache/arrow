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

#include "parquet/encryption/key_toolkit.h"

namespace parquet {
namespace encryption {

std::shared_ptr<KmsClient> KeyToolkit::GetKmsClient(
    const KmsConnectionConfig& kms_connection_config, double cache_entry_lifetime_ms) {
  if (kms_client_factory_ == NULL) {
    throw ParquetException("No KmsClientFactory is registered.");
  }
  auto kms_client_per_kms_instance_cache =
      kms_client_cache_per_token().GetOrCreateInternalCache(
          kms_connection_config.key_access_token(), cache_entry_lifetime_ms);

  return kms_client_per_kms_instance_cache->GetOrInsert(
      kms_connection_config.kms_instance_id, [this, kms_connection_config]() {
        return this->kms_client_factory_->CreateKmsClient(kms_connection_config);
      });
}

// Flush any caches that are tied to the (compromised) access_token
void KeyToolkit::RemoveCacheEntriesForToken(const std::string& access_token) {
  kms_client_cache_per_token().Remove(access_token);
  kek_write_cache_per_token().Remove(access_token);
  kek_read_cache_per_token().Remove(access_token);
}

void KeyToolkit::RemoveCacheEntriesForAllTokens() {
  kms_client_cache_per_token().Clear();
  kek_write_cache_per_token().Clear();
  kek_read_cache_per_token().Clear();
}

}  // namespace encryption
}  // namespace parquet

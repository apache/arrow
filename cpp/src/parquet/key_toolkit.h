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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "parquet/key_encryption_key.h"
#include "parquet/kms_client.h"
#include "parquet/kms_client_factory.h"
#include "parquet/two_level_cache_with_expiration.h"

namespace parquet {

namespace encryption {

class KeyToolkit {
 public:
  class KmsClientCache {
   public:
    static KmsClientCache& GetInstance() {
      static KmsClientCache instance;
      return instance;
    }
    TwoLevelCacheWithExpiration<std::shared_ptr<KmsClient>>& cache() { return cache_; }

   private:
    TwoLevelCacheWithExpiration<std::shared_ptr<KmsClient>> cache_;
  };

  class KEKWriteCache {
   public:
    static KEKWriteCache& GetInstance() {
      static KEKWriteCache instance;
      return instance;
    }
    TwoLevelCacheWithExpiration<KeyEncryptionKey>& cache() { return cache_; }

   private:
    TwoLevelCacheWithExpiration<KeyEncryptionKey> cache_;
  };

  static TwoLevelCacheWithExpiration<std::shared_ptr<KmsClient>>&
  kms_client_cache_per_token() {
    return KmsClientCache::GetInstance().cache();
  }

  static TwoLevelCacheWithExpiration<KeyEncryptionKey>& kek_write_cache_per_token() {
    return KEKWriteCache::GetInstance().cache();
  }

  static std::shared_ptr<KmsClient> GetKmsClient(
      std::shared_ptr<KmsClientFactory> kms_client_factory,
      const KmsConnectionConfig& kms_connection_config, bool is_wrap_locally,
      uint64_t cache_entry_lifetime);

  static std::string EncryptKeyLocally(const uint8_t* key_bytes, int key_size,
                                       const uint8_t* master_key_bytes,
                                       int master_key_size, const uint8_t* aad_bytes,
                                       int aad_size);

  static std::vector<uint8_t> DecryptKeyLocally(const std::string& encoded_encrypted_key,
                                                const uint8_t* master_key_bytes,
                                                int master_key_size,
                                                const uint8_t* aad_bytes, int aad_size);
};

}  // namespace encryption

}  // namespace parquet

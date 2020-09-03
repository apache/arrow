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
#include "parquet/platform.h"
#include "parquet/two_level_cache_with_expiration.h"

namespace parquet {
namespace encryption {

// "data encryption key" and "master key identifier" are paired together as the input to
// generate "key metadata" and as an output when parsing from "key material"
class KeyWithMasterId {
 public:
  KeyWithMasterId(const std::string& key_bytes, const std::string& master_id)
      : key_bytes_(key_bytes), master_id_(master_id) {}

  const std::string& data_key() const { return key_bytes_; }
  const std::string& master_id() const { return master_id_; }

 private:
  const std::string key_bytes_;
  const std::string master_id_;
};

// KeyToolkit is a utility that keeps various tools for key management (such as key
// rotation, kms client instantiation, cache control, etc), plus a number of auxiliary
// classes for internal use.
class PARQUET_EXPORT KeyToolkit {
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

  class KeyEncryptionKeyWriteCache {
   public:
    static KeyEncryptionKeyWriteCache& GetInstance() {
      static KeyEncryptionKeyWriteCache instance;
      return instance;
    }
    TwoLevelCacheWithExpiration<KeyEncryptionKey>& cache() { return cache_; }

   private:
    TwoLevelCacheWithExpiration<KeyEncryptionKey> cache_;
  };

  class KeyEncryptionKeyReadCache {
   public:
    static KeyEncryptionKeyReadCache& GetInstance() {
      static KeyEncryptionKeyReadCache instance;
      return instance;
    }
    TwoLevelCacheWithExpiration<std::string>& cache() { return cache_; }

   private:
    TwoLevelCacheWithExpiration<std::string> cache_;
  };

  /// KMS client two level cache: token -> KMSInstanceId -> KmsClient
  static TwoLevelCacheWithExpiration<std::shared_ptr<KmsClient>>&
  kms_client_cache_per_token() {
    return KmsClientCache::GetInstance().cache();
  }

  /// Key encryption key two level cache for wrapping: token -> MasterEncryptionKeyId ->
  /// KeyEncryptionKey
  static TwoLevelCacheWithExpiration<KeyEncryptionKey>& kek_write_cache_per_token() {
    return KeyEncryptionKeyWriteCache::GetInstance().cache();
  }

  /// Key encryption key two level cache for unwrapping: token -> KeyEncryptionKeyId ->
  /// KeyEncryptionKeyBytes
  static TwoLevelCacheWithExpiration<std::string>& kek_read_cache_per_token() {
    return KeyEncryptionKeyReadCache::GetInstance().cache();
  }

  static std::shared_ptr<KmsClient> GetKmsClient(
      std::shared_ptr<KmsClientFactory> kms_client_factory,
      const KmsConnectionConfig& kms_connection_config, bool is_wrap_locally,
      uint64_t cache_entry_lifetime_ms);

  /// Encrypts "key" with "master_key", using AES-GCM and the "aad"
  static std::string EncryptKeyLocally(const std::string& key,
                                       const std::string& master_key,
                                       const std::string& aad);

  /// Decrypts encrypted key with "master_key", using AES-GCM and the "aad"
  static std::string DecryptKeyLocally(const std::string& encoded_encrypted_key,
                                       const std::string& master_key,
                                       const std::string& aad);

  /// Flush any caches that are tied to the (compromised) access_token
  static void RemoveCacheEntriesForToken(const std::string& access_token);

  static void RemoveCacheEntriesForAllTokens();
};

}  // namespace encryption
}  // namespace parquet

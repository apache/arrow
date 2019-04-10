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

#ifndef PARQUET_WRAPPED_KEY_MANAGER_H
#define PARQUET_WRAPPED_KEY_MANAGER_H

#include "parquet/crypto_keytools/kms_client.h"
#include "parquet/crypto_keytools/parquet_key.h"
#include "parquet/crypto_keytools/wrapped_key_store.h"

#include <string>
#include <memory>

#include "parquet/encryption.h"
#include "parquet/util/visibility.h"

namespace parquet {

class DecryptionKeyRetriever;

class PARQUET_EXPORT WrappedKeyManager {
 public:  
  class WrappedKeyRetriever : public DecryptionKeyRetriever {
   public:
    WrappedKeyRetriever(std::shared_ptr<KmsClient> kms_client, bool unwrap_locally,
   	                std::shared_ptr<WrappedKeyStore> key_store, std::string file_id);
    const std::string& GetKey(const std::string& key_metadata);
   
   private:
    std::shared_ptr<KmsClient> const kms_client_;
    const bool unwrap_locally_;
    std::shared_ptr<WrappedKeyStore> const key_store_; //TODO: implement key store
    const std::string file_id_;
    std::string data_key_;
  };
 
  WrappedKeyManager(std::shared_ptr<KmsClient> kms_client);
  WrappedKeyManager(std::shared_ptr<KmsClient> kms_client, bool wrap_locally,
		    std::shared_ptr<WrappedKeyStore> wrapped_key_store, std::string file_id);
  std::unique_ptr<ParquetKey> generateKey(std::string &master_key_id);
  std::shared_ptr<DecryptionKeyRetriever> getKeyRetriever();
 
  private:
   std::shared_ptr<KmsClient> const kms_client_;
   const bool wrap_locally_;
   std::shared_ptr<WrappedKeyStore> const wrapped_key_store_; //TODO: implement key store
   const std::string file_id_; //Used for WrappedKeyStore
};
 
} // namespace parquet

#endif  // PARQUET_WRAPPED_KEY_MANAGER_H

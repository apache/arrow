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
#include "parquet/platform.h"

namespace parquet {
namespace encryption {

// KMS systems wrap keys by encrypting them by master keys, and attaching additional
// information (such as the version number of the masker key) to the result of encryption.
// The master key version is required in  key rotation. Currently, the local wrapping mode
// does not support key rotation (because not all KMS systems allow to fetch a master key
// by its ID and version number). Still, the local wrapping mode adds a placeholder for
// the master key version, that will enable support for key rotation in this mode in the
// future, with appropriate KMS systems. This will also enable backward compatibility,
// where future readers will be able to extract master key version in the files written by
// the current code.
//
// LocalKeyWrap class writes (and reads) the "key wrap" as a flat json with the following
// fields:
// 1. "masterKeyVersion" - a String, with the master key version. In the current version,
// only one value is allowed - "NO_VERSION".
// 2. "encryptedKey" - a String, with the key encrypted by the master key
// (base64-encoded).
class PARQUET_EXPORT RemoteKmsClient : public KmsClient {
 public:
  static constexpr char LOCAL_WRAP_NO_KEY_VERSION[] = "NO_VERSION";

  void Initialize(const KmsConnectionConfig& kms_connection_config,
                  bool is_wrap_locally) override;

  std::string WrapKey(const std::vector<uint8_t>& key_bytes,
                      const std::string& master_key_identifier) override;

  std::vector<uint8_t> UnwrapKey(const std::string& wrapped_key,
                                 const std::string& master_key_identifier) override;

 protected:
  // Wrap a key with the master key in the remote KMS server.
  virtual std::string WrapKeyInServer(const std::vector<uint8_t>& key_bytes,
                                      const std::string& master_key_identifier) = 0;

  // Unwrap a key with the master key in the remote KMS server.
  virtual std::vector<uint8_t> UnwrapKeyInServer(
      const std::string& wrapped_key, const std::string& master_key_identifier) = 0;

  // Get master key from the remote KMS server.
  // Required only for local wrapping. No need to implement if KMS supports in-server
  // wrapping/unwrapping.
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

  std::vector<uint8_t> GetKeyFromServer(const std::string& key_identifier);

  KmsConnectionConfig kms_connection_config_;
  bool is_wrap_locally_;
  bool is_default_token_;
  std::map<std::string, std::vector<uint8_t>> master_key_cache_;
};

}  // namespace encryption
}  // namespace parquet

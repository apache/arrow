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

#include "arrow/json/object_parser.h"
#include "arrow/json/object_writer.h"

#include "parquet/encryption/key_toolkit_internal.h"
#include "parquet/encryption/remote_kms_client.h"
#include "parquet/exception.h"

using arrow::json::ObjectParser;
using arrow::json::ObjectWriter;

namespace parquet {
namespace encryption {

constexpr const char RemoteKmsClient::kLocalWrapNoKeyVersion[];

constexpr const char RemoteKmsClient::LocalKeyWrap::kLocalWrapKeyVersionField[];
constexpr const char RemoteKmsClient::LocalKeyWrap::kLocalWrapEncryptedKeyField[];

RemoteKmsClient::LocalKeyWrap::LocalKeyWrap(const std::string& master_key_version,
                                            const std::string& encrypted_encoded_key)
    : encrypted_encoded_key_(encrypted_encoded_key),
      master_key_version_(master_key_version) {}

std::string RemoteKmsClient::LocalKeyWrap::CreateSerialized(
    const std::string& encrypted_encoded_key) {
  ObjectWriter json_writer;

  json_writer.SetString(kLocalWrapKeyVersionField, kLocalWrapNoKeyVersion);
  json_writer.SetString(kLocalWrapEncryptedKeyField, encrypted_encoded_key);

  return json_writer.Serialize();
}

RemoteKmsClient::LocalKeyWrap RemoteKmsClient::LocalKeyWrap::Parse(
    const std::string& wrapped_key) {
  ObjectParser json_parser;
  if (!json_parser.Parse(wrapped_key)) {
    throw ParquetException("Failed to parse local key wrap json " + wrapped_key);
  }
  std::string master_key_version;
  PARQUET_ASSIGN_OR_THROW(master_key_version,
                          json_parser.GetString(kLocalWrapKeyVersionField));

  std::string encrypted_encoded_key;
  PARQUET_ASSIGN_OR_THROW(encrypted_encoded_key,
                          json_parser.GetString(kLocalWrapEncryptedKeyField));

  return RemoteKmsClient::LocalKeyWrap(master_key_version, encrypted_encoded_key);
}

void RemoteKmsClient::Initialize(const KmsConnectionConfig& kms_connection_config,
                                 bool is_wrap_locally) {
  kms_connection_config_ = kms_connection_config;
  is_wrap_locally_ = is_wrap_locally;
  if (is_wrap_locally_) {
    master_key_cache_.Clear();
  }

  is_default_token_ =
      kms_connection_config_.key_access_token() == KmsClient::kKeyAccessTokenDefault;

  InitializeInternal();
}

std::string RemoteKmsClient::WrapKey(const std::string& key_bytes,
                                     const std::string& master_key_identifier) {
  if (is_wrap_locally_) {
    std::string master_key = master_key_cache_.GetOrAssignIfNotExist(
        master_key_identifier, [this, master_key_identifier]() -> std::string {
          return this->GetKeyFromServer(master_key_identifier);
        });
    std::string aad = master_key_identifier;

    std::string encrypted_encoded_key =
        internal::EncryptKeyLocally(key_bytes, master_key, aad);
    return LocalKeyWrap::CreateSerialized(encrypted_encoded_key);
  } else {
    return WrapKeyInServer(key_bytes, master_key_identifier);
  }
}

std::string RemoteKmsClient::UnwrapKey(const std::string& wrapped_key,
                                       const std::string& master_key_identifier) {
  if (is_wrap_locally_) {
    LocalKeyWrap key_wrap = LocalKeyWrap::Parse(wrapped_key);
    const std::string& master_key_version = key_wrap.master_key_version();
    if (kLocalWrapNoKeyVersion != master_key_version) {
      throw ParquetException(
          "Master key versions are not supported for local wrapping: " +
          master_key_version);
    }
    const std::string& encrypted_encoded_key = key_wrap.encrypted_encoded_key();
    std::string master_key = master_key_cache_.GetOrAssignIfNotExist(
        master_key_identifier, [this, master_key_identifier]() -> std::string {
          return this->GetKeyFromServer(master_key_identifier);
        });
    std::string aad = master_key_identifier;

    return internal::DecryptKeyLocally(encrypted_encoded_key, master_key, aad);
  } else {
    return UnwrapKeyInServer(wrapped_key, master_key_identifier);
  }
}

std::string RemoteKmsClient::GetKeyFromServer(const std::string& key_identifier) {
  return GetMasterKeyFromServer(key_identifier);
}

}  // namespace encryption
}  // namespace parquet

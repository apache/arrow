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

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "parquet/exception.h"
#include "parquet/key_toolkit.h"
#include "parquet/remote_kms_client.h"

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
  rapidjson::Document d;
  auto& allocator = d.GetAllocator();
  rapidjson::Value root(rapidjson::kObjectType);

  root.AddMember(kLocalWrapKeyVersionField, kLocalWrapNoKeyVersion, allocator);

  rapidjson::Value value(rapidjson::kStringType);
  value.SetString(encrypted_encoded_key.c_str(), allocator);
  root.AddMember(kLocalWrapEncryptedKeyField, value, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  root.Accept(writer);

  return buffer.GetString();
}

RemoteKmsClient::LocalKeyWrap RemoteKmsClient::LocalKeyWrap::Parse(
    const std::string& wrapped_key) {
  rapidjson::Document key_wrap_document;
  key_wrap_document.Parse(wrapped_key.c_str());
  if (key_wrap_document.HasParseError() || !key_wrap_document.IsObject()) {
    throw ParquetException("Failed to parse local key wrap json " + wrapped_key);
  }
  std::string master_key_version =
      key_wrap_document[kLocalWrapKeyVersionField].GetString();
  std::string encrypted_encoded_key =
      key_wrap_document[kLocalWrapEncryptedKeyField].GetString();

  return RemoteKmsClient::LocalKeyWrap(master_key_version, encrypted_encoded_key);
}

void RemoteKmsClient::Initialize(const KmsConnectionConfig& kms_connection_config,
                                 bool is_wrap_locally) {
  kms_connection_config_ = kms_connection_config;
  is_wrap_locally_ = is_wrap_locally;
  if (is_wrap_locally_) {
    master_key_cache_.clear();
  }

  is_default_token_ =
      kms_connection_config_.key_access_token() == KmsClient::kKeyAccessTokenDefault;

  InitializeInternal();
}

std::string RemoteKmsClient::WrapKey(const std::string& key_bytes,
                                     const std::string& master_key_identifier) {
  if (is_wrap_locally_) {
    if (master_key_cache_.find(master_key_identifier) == master_key_cache_.end()) {
      master_key_cache_[master_key_identifier] = GetKeyFromServer(master_key_identifier);
    }
    const std::string& master_key = master_key_cache_[master_key_identifier];
    std::string aad = master_key_identifier;

    std::string encrypted_encoded_key =
        KeyToolkit::EncryptKeyLocally(key_bytes, master_key, aad);
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
    if (master_key_cache_.find(master_key_identifier) == master_key_cache_.end()) {
      master_key_cache_[master_key_identifier] = GetKeyFromServer(master_key_identifier);
    }
    const std::string& master_key = master_key_cache_[master_key_identifier];
    std::string aad = master_key_identifier;

    return KeyToolkit::DecryptKeyLocally(encrypted_encoded_key, master_key, aad);
  } else {
    return UnwrapKeyInServer(wrapped_key, master_key_identifier);
  }
}

std::string RemoteKmsClient::GetKeyFromServer(const std::string& key_identifier) {
  return GetMasterKeyFromServer(key_identifier);
}

}  // namespace encryption
}  // namespace parquet

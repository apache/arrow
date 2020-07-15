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

#include "parquet/remote_kms_client.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "parquet/exception.h"

#include "parquet/key_toolkit.h"

namespace parquet {

namespace encryption {

RemoteKmsClient::LocalKeyWrap::LocalKeyWrap(const std::string& master_key_version,
                                            const std::string& encrypted_encoded_key)
    : encrypted_encoded_key_(encrypted_encoded_key),
      master_key_version_(master_key_version) {}

std::string RemoteKmsClient::LocalKeyWrap::CreateSerialized(
    const std::string& encrypted_encoded_key) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

  writer.Key(LOCAL_WRAP_KEY_VERSION_FIELD);
  writer.String(LOCAL_WRAP_NO_KEY_VERSION, sizeof(LOCAL_WRAP_NO_KEY_VERSION));

  writer.Key(LOCAL_WRAP_KEY_VERSION_FIELD);
  writer.String(encrypted_encoded_key.data(), encrypted_encoded_key.size());

  return buffer.GetString();
}

RemoteKmsClient::LocalKeyWrap RemoteKmsClient::LocalKeyWrap::Parse(
    const std::string& wrapped_key) {
  rapidjson::Document key_wrap_document;
  key_wrap_document.Parse(wrapped_key.c_str());
  if (key_wrap_document.HasParseError() || !key_wrap_document.IsObject()) {
    throw new ParquetException("Failed to parse local key wrap json " + wrapped_key);
  }
  std::string master_key_version =
      key_wrap_document[LOCAL_WRAP_KEY_VERSION_FIELD].GetString();
  std::string encrypted_encoded_key =
      key_wrap_document[LOCAL_WRAP_ENCRYPTED_KEY_FIELD].GetString();

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
      kms_connection_config_.key_access_token == KmsClient::KEY_ACCESS_TOKEN_DEFAULT;

  InitializeInternal();
}

std::string RemoteKmsClient::WrapKey(const std::vector<uint8_t>& key_bytes,
                                     const std::string& master_key_identifier) {
  if (is_wrap_locally_) {
    if (master_key_cache_.find(master_key_identifier) == master_key_cache_.end()) {
      master_key_cache_[master_key_identifier] = GetKeyFromServer(master_key_identifier);
    }
    const std::vector<uint8_t>& master_key = master_key_cache_[master_key_identifier];
    const uint8_t* aad_bytes = reinterpret_cast<const uint8_t*>(master_key_identifier[0]);
    int aad_size = master_key_identifier.size();
    std::string encrypted_encoded_key = KeyToolkit::EncryptKeyLocally(
        key_bytes.data(), key_bytes.size(), master_key.data(), master_key.size(),
        aad_bytes, aad_size);
    return LocalKeyWrap::CreateSerialized(encrypted_encoded_key);
  } else {
    RefreshToken();
    return WrapKeyInServer(key_bytes, master_key_identifier);
  }
}

std::vector<uint8_t> RemoteKmsClient::UnwrapKey(
    const std::string& wrapped_key, const std::string& master_key_identifier) {
  if (is_wrap_locally_) {
    LocalKeyWrap key_wrap = LocalKeyWrap::Parse(wrapped_key);
    const std::string& master_key_version = key_wrap.master_key_version();
    if (LOCAL_WRAP_NO_KEY_VERSION != master_key_version) {
      throw new ParquetException(
          "Master key versions are not supported for local wrapping: " +
          master_key_version);
    }
    const std::string& encrypted_encoded_key = key_wrap.encrypted_encoded_key();
    if (master_key_cache_.find(master_key_identifier) == master_key_cache_.end()) {
      master_key_cache_[master_key_identifier] = GetKeyFromServer(master_key_identifier);
    }
    const std::vector<uint8_t>& master_key = master_key_cache_[master_key_identifier];

    const uint8_t* aad_bytes = reinterpret_cast<const uint8_t*>(master_key_identifier[0]);
    int aad_size = master_key_identifier.size();
    return KeyToolkit::DecryptKeyLocally(encrypted_encoded_key, master_key.data(),
                                         master_key.size(), aad_bytes, aad_size);
  } else {
    RefreshToken();
    return UnwrapKeyInServer(wrapped_key, master_key_identifier);
  }
}

void RemoteKmsClient::RefreshToken() {
  if (is_default_token_) {
    return;
  }
  // TODO
}

std::vector<uint8_t> RemoteKmsClient::GetKeyFromServer(
    const std::string& key_identifier) {
  RefreshToken();
  return GetMasterKeyFromServer(key_identifier);
}

}  // namespace encryption

}  // namespace parquet

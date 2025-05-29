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
#include "arrow/util/secure_string.h"

#include "parquet/encryption/key_toolkit_internal.h"
#include "parquet/encryption/local_wrap_kms_client.h"
#include "parquet/exception.h"

using ::arrow::json::internal::ObjectParser;
using ::arrow::json::internal::ObjectWriter;
using ::arrow::util::SecureString;

namespace parquet::encryption {

constexpr const char LocalWrapKmsClient::kLocalWrapNoKeyVersion[];

constexpr const char LocalWrapKmsClient::LocalKeyWrap::kLocalWrapKeyVersionField[];
constexpr const char LocalWrapKmsClient::LocalKeyWrap::kLocalWrapEncryptedKeyField[];

LocalWrapKmsClient::LocalKeyWrap::LocalKeyWrap(std::string master_key_version,
                                               std::string encrypted_encoded_key)
    : encrypted_encoded_key_(std::move(encrypted_encoded_key)),
      master_key_version_(std::move(master_key_version)) {}

std::string LocalWrapKmsClient::LocalKeyWrap::CreateSerialized(
    const std::string& encrypted_encoded_key) {
  ObjectWriter json_writer;

  json_writer.SetString(kLocalWrapKeyVersionField, kLocalWrapNoKeyVersion);
  json_writer.SetString(kLocalWrapEncryptedKeyField, encrypted_encoded_key);

  return json_writer.Serialize();
}

LocalWrapKmsClient::LocalKeyWrap LocalWrapKmsClient::LocalKeyWrap::Parse(
    const std::string& wrapped_key) {
  ObjectParser json_parser;
  auto status = json_parser.Parse(wrapped_key);
  if (!status.ok()) {
    throw ParquetException("Failed to parse local key wrap json " + wrapped_key);
  }
  PARQUET_ASSIGN_OR_THROW(const auto master_key_version,
                          json_parser.GetString(kLocalWrapKeyVersionField));

  PARQUET_ASSIGN_OR_THROW(const auto encrypted_encoded_key,
                          json_parser.GetString(kLocalWrapEncryptedKeyField));

  return LocalWrapKmsClient::LocalKeyWrap(std::move(master_key_version),
                                          std::move(encrypted_encoded_key));
}

LocalWrapKmsClient::LocalWrapKmsClient(const KmsConnectionConfig& kms_connection_config)
    : kms_connection_config_(kms_connection_config) {
  master_key_cache_.Clear();
}

std::string LocalWrapKmsClient::WrapKey(const SecureString& key_bytes,
                                        const std::string& master_key_identifier) {
  const auto master_key = master_key_cache_.GetOrInsert(
      master_key_identifier, [this, master_key_identifier]() -> SecureString {
        return this->GetKeyFromServer(master_key_identifier);
      });
  const auto& aad = master_key_identifier;

  const auto encrypted_encoded_key =
      internal::EncryptKeyLocally(key_bytes, master_key, aad);
  return LocalKeyWrap::CreateSerialized(encrypted_encoded_key);
}

SecureString LocalWrapKmsClient::UnwrapKey(const std::string& wrapped_key,
                                           const std::string& master_key_identifier) {
  LocalKeyWrap key_wrap = LocalKeyWrap::Parse(wrapped_key);
  const std::string& master_key_version = key_wrap.master_key_version();
  if (kLocalWrapNoKeyVersion != master_key_version) {
    throw ParquetException("Master key versions are not supported for local wrapping: " +
                           master_key_version);
  }
  const std::string& encrypted_encoded_key = key_wrap.encrypted_encoded_key();
  const SecureString& master_key = master_key_cache_.GetOrInsert(
      master_key_identifier, [this, master_key_identifier]() -> const SecureString& {
        return this->GetKeyFromServer(master_key_identifier);
      });
  const std::string& aad = master_key_identifier;

  return internal::DecryptKeyLocally(encrypted_encoded_key, master_key, aad);
}

const SecureString& LocalWrapKmsClient::GetKeyFromServer(
    const std::string& key_identifier) {
  const SecureString& master_key = GetMasterKeyFromServer(key_identifier);
  int32_t key_length_bits = static_cast<int32_t>(master_key.size() * 8);
  if (!internal::ValidateKeyLength(key_length_bits)) {
    std::ostringstream ss;
    ss << "Wrong master key length : " << key_length_bits;
    throw ParquetException(ss.str());
  }
  return master_key;
}

}  // namespace parquet::encryption

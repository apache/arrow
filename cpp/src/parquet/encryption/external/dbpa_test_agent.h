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

#include <memory>
#include <string>

#include <dbpa_interface.h>

template <typename T>
using span = tcb::span<T>;

using dbps::external::DataBatchProtectionAgentInterface;
using dbps::external::EncryptionResult;
using dbps::external::DecryptionResult;
using dbps::external::Type;
using dbps::external::CompressionCodec;

namespace parquet::encryption::external {

// Implementation of the DataBatchProtectionAgentInterface for testing purposes.
// It is used to test library wrapper/loading code.
// Will never be used in production.
class DBPATestAgent : public DataBatchProtectionAgentInterface {
 public:
  explicit DBPATestAgent();

  void init(
      std::string column_name,
      std::map<std::string, std::string> connection_config,
      std::string app_context,
      std::string column_key_id,
      Type::type data_type,
      std::optional<int> datatype_length,
      CompressionCodec::type compression_type,
      std::optional<std::map<std::string, std::string>> column_encryption_metadata) override {

    if (column_key_id.empty()) {
      throw std::invalid_argument("column_key_id cannot be empty");
    }
    // Store the key id so we can use it for simple test XOR encryption/decryption
    key_ = std::move(column_key_id);
    connection_config_ = std::move(connection_config);
  }

  std::unique_ptr<EncryptionResult> Encrypt(
      span<const uint8_t> plaintext,
      std::map<std::string, std::string> encoding_attributes) override;

  std::unique_ptr<DecryptionResult> Decrypt(
      span<const uint8_t> ciphertext,
      std::map<std::string, std::string> encoding_attributes) override;

  ~DBPATestAgent();

 private:
  // Used as a simple XOR key for test encryption/decryption
  std::string key_;
  // Stored connection configuration from init(); used to toggle test behaviors
  std::map<std::string, std::string> connection_config_;
  // Count Encrypt() calls to allow staged behavior in tests
  size_t encrypt_calls_ = 0;
};

}  // namespace parquet::encryption::external 

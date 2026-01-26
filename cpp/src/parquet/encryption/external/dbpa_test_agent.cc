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

#include <cstring>
#include <iostream>
#include <map>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <dbpa_interface.h>
#include "parquet/encryption/external/dbpa_test_agent.h"
#include "parquet/exception.h"

template <typename T>
using span = tcb::span<T>;
using dbps::external::DecryptionResult;
using dbps::external::EncryptionResult;

namespace parquet::encryption::external {

// Concrete implementation of EncryptionResult for testing
class TestEncryptionResult : public EncryptionResult {
 public:
  TestEncryptionResult(
      std::vector<uint8_t> data, bool success = true, std::string error_msg = "",
      std::map<std::string, std::string> error_fields = {},
      std::optional<std::map<std::string, std::string>> metadata = std::nullopt)
      : ciphertext_data_(std::move(data)),
        success_(success),
        error_message_(std::move(error_msg)),
        error_fields_(std::move(error_fields)),
        metadata_(std::move(metadata)) {}

  span<const uint8_t> ciphertext() const override {
    return span<const uint8_t>(ciphertext_data_.data(), ciphertext_data_.size());
  }

  std::size_t size() const override { return ciphertext_data_.size(); }
  bool success() const override { return success_; }
  const std::string& error_message() const override { return error_message_; }
  const std::map<std::string, std::string>& error_fields() const override {
    return error_fields_;
  }
  const std::optional<std::map<std::string, std::string>> encryption_metadata()
      const override {
    if (metadata_.has_value()) {
      return metadata_;
    }
    return std::map<std::string, std::string>{{"test_key1", "test_value1"},
                                              {"test_key2", "test_value2"}};
  }

 private:
  std::vector<uint8_t> ciphertext_data_;
  bool success_;
  std::string error_message_;
  std::map<std::string, std::string> error_fields_;
  std::optional<std::map<std::string, std::string>> metadata_;
};

// Concrete implementation of DecryptionResult for testing
class TestDecryptionResult : public DecryptionResult {
 public:
  TestDecryptionResult(std::vector<uint8_t> data, bool success = true,
                       std::string error_msg = "",
                       std::map<std::string, std::string> error_fields = {})
      : plaintext_data_(std::move(data)),
        success_(success),
        error_message_(std::move(error_msg)),
        error_fields_(std::move(error_fields)) {}

  span<const uint8_t> plaintext() const override {
    return span<const uint8_t>(plaintext_data_.data(), plaintext_data_.size());
  }

  std::size_t size() const override { return plaintext_data_.size(); }
  bool success() const override { return success_; }
  const std::string& error_message() const override { return error_message_; }
  const std::map<std::string, std::string>& error_fields() const override {
    return error_fields_;
  }

 private:
  std::vector<uint8_t> plaintext_data_;
  bool success_;
  std::string error_message_;
  std::map<std::string, std::string> error_fields_;
};

DBPATestAgent::DBPATestAgent() {}

std::unique_ptr<EncryptionResult> DBPATestAgent::Encrypt(
    span<const uint8_t> plaintext, std::map<std::string, std::string>) {
  // Simple XOR encryption for testing purposes
  // In a real implementation, this would use proper encryption
  std::vector<uint8_t> ciphertext_data(plaintext.size());

  const size_t key_len = key_.size();
  for (size_t i = 0; i < plaintext.size(); ++i) {
    ciphertext_data[i] = plaintext[i] ^ static_cast<uint8_t>(key_[i % key_len]);
  }

  // For tests, optionally force a conflicting metadata value on subsequent calls
  auto it = configuration_properties_.find("dbpa_test_force_conflicting_metadata");
  bool force_conflict = (it != configuration_properties_.end() && it->second == "1");
  encrypt_calls_++;
  if (force_conflict && encrypt_calls_ >= 2) {
    // Return a different value for test_key1 to trigger conflict in writer
    std::map<std::string, std::string> md{{"test_key1", "test_value1_conflict"},
                                          {"test_key2", "test_value2"}};
    return std::make_unique<TestEncryptionResult>(
        std::move(ciphertext_data), true, "", std::map<std::string, std::string>{}, md);
  }

  return std::make_unique<TestEncryptionResult>(std::move(ciphertext_data));
}

std::unique_ptr<DecryptionResult> DBPATestAgent::Decrypt(
    span<const uint8_t> ciphertext, std::map<std::string, std::string>) {
  // Simple XOR decryption for testing purposes
  // In a real implementation, this would perform actual decryption
  std::vector<uint8_t> plaintext_data(ciphertext.size());

  const size_t key_len = key_.size();
  for (size_t i = 0; i < ciphertext.size(); ++i) {
    plaintext_data[i] = ciphertext[i] ^ static_cast<uint8_t>(key_[i % key_len]);
  }

  return std::make_unique<TestDecryptionResult>(std::move(plaintext_data));
}

DBPATestAgent::~DBPATestAgent() {}

// Export function for creating new instances from shared library
extern "C" {
DataBatchProtectionAgentInterface* create_new_instance() {
  return new parquet::encryption::external::DBPATestAgent();
}
}

}  // namespace parquet::encryption::external

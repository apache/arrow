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

#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include <dbpa_interface.h>

template <typename T>
using span = tcb::span<T>;

namespace parquet::encryption::external {

using dbps::external::CompressionCodec;
using dbps::external::DataBatchProtectionAgentInterface;
using dbps::external::DecryptionResult;
using dbps::external::EncryptionResult;
using dbps::external::Type;

class DBPAExecutorTimeoutException;

/**
 * DBPAExecutor - A decorator for DataBatchProtectionAgentInterface with timeout support
 * Original exceptions from wrapped agents are preserved and re-thrown unchanged.
 */
class DBPAExecutor : public DataBatchProtectionAgentInterface {
 public:
  /**
   * Constructor that takes ownership of the wrapped agent with configurable timeouts
   * @param agent The DataBatchProtectionAgentInterface instance to wrap
   * @param init_timeout Timeout for init operations in milliseconds (default: 10000)
   * @param encrypt_timeout Timeout for encrypt operations in milliseconds
   *                        (default: 30000)
   * @param decrypt_timeout Timeout for decrypt operations in milliseconds
   * (default: 30000)
   */
  explicit DBPAExecutor(std::unique_ptr<DataBatchProtectionAgentInterface> agent,
                        int64_t init_timeout = 10000, int64_t encrypt_timeout = 30000,
                        int64_t decrypt_timeout = 30000);

  /**
   * Destructor
   */
  ~DBPAExecutor() override = default;

  /**
   * Initialize the agent with configuration parameters
   * Executes with timeout - original exceptions preserved
   * @throws DBPAExecutorTimeoutException if operation times out
   * @throws Original exceptions from wrapped agent (unchanged!)
   */
  void init(std::string column_name,
            std::map<std::string, std::string> configuration_properties,
            std::string app_context, std::string column_key_id, Type::type data_type,
            std::optional<int> datatype_length, CompressionCodec::type compression_type,
            std::optional<std::map<std::string, std::string>> column_encryption_metadata)
      override;

  /**
   * Encrypt the provided plaintext
   * Executes with timeout - original exceptions preserved
   * @param plaintext The data to encrypt
   * @param encoding_attributes The encoding attributes to use for encryption
   * @return Unique pointer to EncryptionResult
   * @throws DBPAExecutorTimeoutException if operation times out
   * @throws Original exceptions from wrapped agent (unchanged!)
   */
  std::unique_ptr<EncryptionResult> Encrypt(
      span<const uint8_t> plaintext,
      std::map<std::string, std::string> encoding_attributes) override;

  /**
   * Decrypt the provided ciphertext
   * Executes with timeout - original exceptions preserved
   * @param ciphertext The data to decrypt
   * @param encoding_attributes The encoding attributes to use for decryption
   * @return Unique pointer to DecryptionResult
   * @throws DBPAExecutorTimeoutException if operation times out
   * @throws Original exceptions from wrapped agent (unchanged!)
   */
  std::unique_ptr<DecryptionResult> Decrypt(
      span<const uint8_t> ciphertext,
      std::map<std::string, std::string> encoding_attributes) override;

 private:
  std::unique_ptr<DataBatchProtectionAgentInterface> wrapped_agent_;
  int64_t init_timeout_milliseconds_;
  int64_t encrypt_timeout_milliseconds_;
  int64_t decrypt_timeout_milliseconds_;
};  // class DBPAExecutor

/**
 * Exception thrown when a DBPA operation times out
 */
class DBPAExecutorTimeoutException : public std::runtime_error {
 public:
  explicit DBPAExecutorTimeoutException(const std::string& operation,
                                        int64_t timeout_milliseconds)
      : std::runtime_error("DBPAExecutor: " + operation + " operation timed out after " +
                           std::to_string(timeout_milliseconds) + " milliseconds") {}
};

}  // namespace parquet::encryption::external

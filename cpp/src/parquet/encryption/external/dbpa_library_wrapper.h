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
#include <functional>
#include <optional>
#include <map>

#include <dbpa_interface.h>

template <typename T>
using span = tcb::span<T>;

namespace parquet::encryption::external {

using dbps::external::DataBatchProtectionAgentInterface;
using dbps::external::EncryptionResult;
using dbps::external::DecryptionResult;
using dbps::external::Type;
using dbps::external::CompressionCodec;  

// Default implementation for shared library closing function
// This is passed into the constructor of DBPALibraryWrapper, 
// and is used as the default function to close the shared library.
void DefaultSharedLibraryClosingFn(void* library_handle);

// Decorator/Wrapper class for the DataBatchProtectionAgentInterface
// Its main purpose is to close the shared library when Arrow is about to destroy 
// an instance of an DBPAgent
//
// In the constructor we allow to pass a function that will be used to close the shared library.
// This simplifies testing, as we can use a mock function to avoid actually closing the shared library.
class DBPALibraryWrapper : public DataBatchProtectionAgentInterface {
 private:
  std::unique_ptr<DataBatchProtectionAgentInterface> wrapped_agent_;
  void* library_handle_;
  std::function<void(void*)> handle_closing_fn_;

 public:
  // Constructor that takes ownership of the wrapped agent
  explicit DBPALibraryWrapper(
      std::unique_ptr<DataBatchProtectionAgentInterface> agent,
      void* library_handle,
      std::function<void(void*)> handle_closing_fn = &DefaultSharedLibraryClosingFn);

  // Destructor
  // This is the main reason for the decorator/wrapper.
  // This will (a) destroy the wrapped agent, and (b) close the shared library.
  // While the wrapped_agent_ would automatically be destroyed when this object is destroyed
  // we need to explicitly destroy **before** we are able to close the shared library.
  // Doing it in a different order, may cause issues, as by unloading the library may cause the class
  // definition to be unloaded before the destructor completes, and that is likely to cause issues 
  // (such as a segfault).
  ~DBPALibraryWrapper();

  // Decorator implementation of init method
  inline void init(
      std::string column_name,
      std::map<std::string, std::string> connection_config,
      std::string app_context,
      std::string column_key_id,
      Type::type data_type,
      std::optional<int> datatype_length,
      CompressionCodec::type compression_type,
      std::optional<std::map<std::string, std::string>> column_encryption_metadata) override {
    wrapped_agent_->init(std::move(column_name), std::move(connection_config),
                        std::move(app_context), std::move(column_key_id),
                        data_type, datatype_length, compression_type, std::move(column_encryption_metadata));
  }

  // Decorator implementation of Encrypt method - inlined for performance
  inline std::unique_ptr<EncryptionResult> Encrypt(
      span<const uint8_t> plaintext,
      std::map<std::string, std::string> encoding_attributes) override {
    return wrapped_agent_->Encrypt(plaintext, std::move(encoding_attributes));
  }

  // Decorator implementation of Decrypt method - inlined for performance
  inline std::unique_ptr<DecryptionResult> Decrypt(
      span<const uint8_t> ciphertext,
      std::map<std::string, std::string> encoding_attributes) override {
    return wrapped_agent_->Decrypt(ciphertext, std::move(encoding_attributes));
  }
};

}  // namespace parquet::encryption::external

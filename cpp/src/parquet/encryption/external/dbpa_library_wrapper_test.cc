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

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "parquet/encryption/external/dbpa_library_wrapper.h"

#include "parquet/test_util.h"

template <typename T>
using span = tcb::span<T>;

namespace parquet::encryption::external::test {

using dbps::external::CompressionCodec;
using dbps::external::DecryptionResult;
using dbps::external::EncryptionResult;
using dbps::external::Type;

// Simple implementation of EncryptionResult for testing
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
  const std::optional<std::map<std::string, std::string>> encryption_metadata()
      const override {
    return metadata_;
  }
  const std::string& error_message() const override { return error_message_; }
  const std::map<std::string, std::string>& error_fields() const override {
    return error_fields_;
  }

 private:
  std::vector<uint8_t> ciphertext_data_;
  bool success_;
  std::string error_message_;
  std::map<std::string, std::string> error_fields_;
  std::optional<std::map<std::string, std::string>> metadata_;
};

// Simple implementation of DecryptionResult for testing
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

// Companion object to track the order of destruction events
class DestructionOrderTracker {
 public:
  DestructionOrderTracker() : sequence_counter_(0) {}

  // Record an event with a sequence number
  void RecordEvent(const std::string& event_name) {
    events_.emplace_back(event_name, ++sequence_counter_);
  }

  // Get the sequence number for a specific event
  int GetEventSequence(const std::string& event_name) const {
    for (const auto& event : events_) {
      if (event.first == event_name) {
        return event.second;
      }
    }
    return -1;  // Event not found
  }

  // Verify that first_event occurred before second_event
  bool VerifyOrder(const std::string& first_event,
                   const std::string& second_event) const {
    int first_seq = GetEventSequence(first_event);
    int second_seq = GetEventSequence(second_event);

    if (first_seq == -1 || second_seq == -1) {
      return false;  // One or both events not recorded
    }

    return first_seq < second_seq;
  }

  // Get all recorded events in order
  const std::vector<std::pair<std::string, int>>& GetEvents() const { return events_; }

  // Clear all recorded events
  void Clear() {
    events_.clear();
    sequence_counter_ = 0;
  }

  // Check if an event was recorded
  bool WasEventRecorded(const std::string& event_name) const {
    return GetEventSequence(event_name) != -1;
  }

 private:
  std::vector<std::pair<std::string, int>> events_;
  int sequence_counter_;
};  // DestructionOrderTracker

// Companion object to hold mock state that persists after mock instance destruction
class MockCompanionDBPA {
 public:
  explicit MockCompanionDBPA(
      std::shared_ptr<DestructionOrderTracker> order_tracker = nullptr)
      : encrypt_called_(false),
        decrypt_called_(false),
        init_called_(false),
        destructor_called_(false),
        encrypt_count_(0),
        decrypt_count_(0),
        init_count_(0),
        order_tracker_(order_tracker ? order_tracker
                                     : std::make_shared<DestructionOrderTracker>()) {}

  // Test helper methods
  bool WasEncryptCalled() const { return encrypt_called_; }
  bool WasDecryptCalled() const { return decrypt_called_; }
  bool WasInitCalled() const { return init_called_; }
  bool WasDestructorCalled() const { return destructor_called_; }
  int GetEncryptCount() const { return encrypt_count_; }
  int GetDecryptCount() const { return decrypt_count_; }
  int GetInitCount() const { return init_count_; }
  const std::vector<uint8_t>& GetEncryptPlaintext() const { return encrypt_plaintext_; }
  const std::vector<uint8_t>& GetDecryptCiphertext() const { return decrypt_ciphertext_; }
  size_t GetEncryptCiphertextSize() const { return encrypt_ciphertext_size_; }
  std::shared_ptr<DestructionOrderTracker> GetOrderTracker() const {
    return order_tracker_;
  }
  const std::map<std::string, std::string>& GetLastEncryptEncodingAttrs() const {
    return last_encrypt_encoding_attrs_;
  }
  const std::map<std::string, std::string>& GetLastDecryptEncodingAttrs() const {
    return last_decrypt_encoding_attrs_;
  }
  void SetNextEncryptResultMetadata(
      std::optional<std::map<std::string, std::string>> metadata) {
    next_encrypt_result_metadata_ = std::move(metadata);
  }
  std::optional<std::map<std::string, std::string>> ConsumeNextEncryptResultMetadata() {
    auto tmp = std::move(next_encrypt_result_metadata_);
    next_encrypt_result_metadata_.reset();
    return tmp;
  }

  // Init tracking methods
  const std::string& GetInitColumnName() const { return init_column_name_; }
  const std::map<std::string, std::string>& GetInitConfigurationProperties() const {
    return init_configuration_properties_;
  }
  const std::string& GetInitAppContext() const { return init_app_context_; }
  const std::string& GetInitColumnKeyId() const { return init_column_key_id_; }
  Type::type GetInitDataType() const { return init_data_type_; }
  CompressionCodec::type GetInitCompressionType() const { return init_compression_type_; }
  std::optional<int> GetInitDatatypeLength() const { return init_datatype_length_; }
  const std::optional<std::map<std::string, std::string>>&
  GetInitColumnEncryptionMetadata() const {
    return init_column_encryption_metadata_;
  }

  // State update methods (called by the mock instance)
  void SetEncryptCalled(bool called) { encrypt_called_ = called; }
  void SetDecryptCalled(bool called) { decrypt_called_ = called; }
  void SetInitCalled(bool called) { init_called_ = called; }
  void SetDestructorCalled(bool called) {
    destructor_called_ = called;
    if (called) {
      order_tracker_->RecordEvent("agent_destructor");
    }
  }
  void IncrementEncryptCount() { encrypt_count_++; }
  void IncrementDecryptCount() { decrypt_count_++; }
  void IncrementInitCount() { init_count_++; }
  void SetEncryptPlaintext(const std::vector<uint8_t>& plaintext) {
    encrypt_plaintext_ = plaintext;
  }
  void SetDecryptCiphertext(const std::vector<uint8_t>& ciphertext) {
    decrypt_ciphertext_ = ciphertext;
  }
  void SetEncryptCiphertextSize(size_t size) { encrypt_ciphertext_size_ = size; }
  void SetLastEncryptEncodingAttrs(std::map<std::string, std::string> attrs) {
    last_encrypt_encoding_attrs_ = std::move(attrs);
  }
  void SetLastDecryptEncodingAttrs(std::map<std::string, std::string> attrs) {
    last_decrypt_encoding_attrs_ = std::move(attrs);
  }

  // Init parameter tracking
  void SetInitParameters(std::string column_name,
                         std::map<std::string, std::string> configuration_properties,
                         std::string app_context, std::string column_key_id,
                         Type::type data_type, CompressionCodec::type compression_type,
                         std::optional<int> datatype_length = std::nullopt,
                         std::optional<std::map<std::string, std::string>>
                             column_encryption_metadata = std::nullopt) {
    init_column_name_ = std::move(column_name);
    init_configuration_properties_ = std::move(configuration_properties);
    init_app_context_ = std::move(app_context);
    init_column_key_id_ = std::move(column_key_id);
    init_data_type_ = data_type;
    init_compression_type_ = compression_type;
    init_datatype_length_ = datatype_length;
    init_column_encryption_metadata_ = std::move(column_encryption_metadata);
  }

 private:
  bool encrypt_called_;
  bool decrypt_called_;
  bool init_called_;
  bool destructor_called_;
  int encrypt_count_;
  int decrypt_count_;
  int init_count_;
  std::vector<uint8_t> encrypt_plaintext_;
  std::vector<uint8_t> decrypt_ciphertext_;
  size_t encrypt_ciphertext_size_;
  std::shared_ptr<DestructionOrderTracker> order_tracker_;

  // Init parameters
  std::string init_column_name_;
  std::map<std::string, std::string> init_configuration_properties_;
  std::string init_app_context_;
  std::string init_column_key_id_;
  Type::type init_data_type_;
  CompressionCodec::type init_compression_type_;
  std::optional<int> init_datatype_length_;
  std::optional<std::map<std::string, std::string>> init_column_encryption_metadata_;
  std::map<std::string, std::string> last_encrypt_encoding_attrs_;
  std::map<std::string, std::string> last_decrypt_encoding_attrs_;
  std::optional<std::map<std::string, std::string>> next_encrypt_result_metadata_;
};  // MockCompanionDBPA

// Companion object to track shared library handle management operations
class SharedLibHandleManagementCompanion {
 public:
  SharedLibHandleManagementCompanion(
      std::shared_ptr<DestructionOrderTracker> order_tracker = nullptr)
      : handle_close_called_(false),
        handle_close_count_(0),
        last_closed_handle_(nullptr),
        order_tracker_(order_tracker ? order_tracker
                                     : std::make_shared<DestructionOrderTracker>()) {}

  // Test helper methods
  bool WasHandleCloseCalled() const { return handle_close_called_; }
  int GetHandleCloseCount() const { return handle_close_count_; }
  void* GetLastClosedHandle() const { return last_closed_handle_; }
  std::shared_ptr<DestructionOrderTracker> GetOrderTracker() const {
    return order_tracker_;
  }

  // State update methods
  void SetHandleCloseCalled(bool called) { handle_close_called_ = called; }
  void IncrementHandleCloseCount() { handle_close_count_++; }
  void SetLastClosedHandle(void* handle) { last_closed_handle_ = handle; }

  // Create a closure that captures this companion object
  // and returns a function that can be used to close the shared library handle
  std::function<void(void*)> CreateHandleClosingFunction() {
    return [this](void* library_handle) {
      this->SetHandleCloseCalled(true);
      this->IncrementHandleCloseCount();
      this->SetLastClosedHandle(library_handle);
      this->order_tracker_->RecordEvent("handle_close");
    };
  }

 private:
  bool handle_close_called_;
  int handle_close_count_;
  void* last_closed_handle_;
  std::shared_ptr<DestructionOrderTracker> order_tracker_;
};  // SharedLibHandleManagementCompanion

// Mock implementation of DataBatchProtectionAgentInterface for testing delegation
class MockDataBatchProtectionAgent : public DataBatchProtectionAgentInterface {
 public:
  explicit MockDataBatchProtectionAgent(
      std::shared_ptr<MockCompanionDBPA> companion = nullptr)
      : companion_(companion ? companion : std::make_shared<MockCompanionDBPA>()) {}

  ~MockDataBatchProtectionAgent() override { companion_->SetDestructorCalled(true); }

  void init(std::string column_name, std::map<std::string, std::string> connection_config,
            std::string app_context, std::string column_key_id, Type::type data_type,
            std::optional<int> datatype_length, CompressionCodec::type compression_type,
            std::optional<std::map<std::string, std::string>> column_encryption_metadata)
      override {
    companion_->SetInitCalled(true);
    companion_->IncrementInitCount();
    companion_->SetInitParameters(std::move(column_name), std::move(connection_config),
                                  std::move(app_context), std::move(column_key_id),
                                  data_type, compression_type, datatype_length,
                                  std::move(column_encryption_metadata));
  }

  std::unique_ptr<EncryptionResult> Encrypt(
      span<const uint8_t> plaintext,
      std::map<std::string, std::string> encoding_attributes) override {
    companion_->SetEncryptCalled(true);
    companion_->IncrementEncryptCount();
    companion_->SetEncryptPlaintext(
        std::vector<uint8_t>(plaintext.begin(), plaintext.end()));
    companion_->SetEncryptCiphertextSize(plaintext.size());
    companion_->SetLastEncryptEncodingAttrs(std::move(encoding_attributes));

    // Create a simple mock encryption result
    std::vector<uint8_t> ciphertext_data(plaintext.begin(), plaintext.end());
    auto result_metadata = companion_->ConsumeNextEncryptResultMetadata();
    return std::make_unique<TestEncryptionResult>(std::move(ciphertext_data), true, "",
                                                  std::map<std::string, std::string>{},
                                                  std::move(result_metadata));
  }

  std::unique_ptr<DecryptionResult> Decrypt(
      span<const uint8_t> ciphertext,
      std::map<std::string, std::string> encoding_attributes) override {
    companion_->SetDecryptCalled(true);
    companion_->IncrementDecryptCount();
    companion_->SetDecryptCiphertext(
        std::vector<uint8_t>(ciphertext.begin(), ciphertext.end()));
    companion_->SetLastDecryptEncodingAttrs(std::move(encoding_attributes));

    // Create a simple mock decryption result
    std::vector<uint8_t> plaintext_data(ciphertext.begin(), ciphertext.end());
    return std::make_unique<TestDecryptionResult>(std::move(plaintext_data));
  }

  // Getter for the companion object
  std::shared_ptr<MockCompanionDBPA> GetCompanion() const { return companion_; }

 private:
  std::shared_ptr<MockCompanionDBPA> companion_;
};

// Test fixture for DBPALibraryWrapper tests
class DBPALibraryWrapperTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create test data
    test_plaintext_ = "Hello, World!";
    test_ciphertext_.resize(test_plaintext_.size());

    // Create shared destruction order tracker
    destruction_order_tracker_ = std::make_shared<DestructionOrderTracker>();

    // Create companion objects with shared order tracker
    mock_companion_ = std::make_shared<MockCompanionDBPA>(destruction_order_tracker_);
    handle_companion_ =
        std::make_shared<SharedLibHandleManagementCompanion>(destruction_order_tracker_);

    // Create mock agent
    mock_agent_ = std::make_unique<MockDataBatchProtectionAgent>(mock_companion_);
    mock_agent_ptr_ = mock_agent_.get();
  }

  void TearDown() override {
    // mock_companion_ and handle_companion_ remain valid for assertions even after
    // mock_agent_ is destroyed
    mock_agent_.reset();
  }

  // Helper method to create a wrapper with mock agent and handle management tracking
  std::unique_ptr<DBPALibraryWrapper> CreateWrapper() {
    return CreateWrapperWithAgent(std::move(mock_agent_));
  }

  // Helper method to create a wrapper with custom agent and handle management tracking
  std::unique_ptr<DBPALibraryWrapper> CreateWrapperWithAgent(
      std::unique_ptr<MockDataBatchProtectionAgent> agent) {
    void* dummy_handle = reinterpret_cast<void*>(0x12345678);

    // Use the existing handle companion from the test fixture
    return std::make_unique<DBPALibraryWrapper>(
        std::move(agent), dummy_handle, handle_companion_->CreateHandleClosingFunction());
  }

  // Helper method to create wrapper with custom handle closing function
  std::unique_ptr<DBPALibraryWrapper> CreateWrapperWithCustomClosing(
      std::function<void(void*)> handle_closing_fn) {
    void* dummy_handle = reinterpret_cast<void*>(0x12345678);

    return std::make_unique<DBPALibraryWrapper>(std::move(mock_agent_), dummy_handle,
                                                handle_closing_fn);
  }

  std::string test_plaintext_;
  std::vector<uint8_t> test_ciphertext_;
  std::shared_ptr<DestructionOrderTracker> destruction_order_tracker_;
  std::shared_ptr<MockCompanionDBPA> mock_companion_;
  std::shared_ptr<SharedLibHandleManagementCompanion> handle_companion_;
  std::unique_ptr<MockDataBatchProtectionAgent> mock_agent_;
  MockDataBatchProtectionAgent* mock_agent_ptr_;
};

// ============================================================================
// CONSTRUCTOR TESTS
// ============================================================================

TEST_F(DBPALibraryWrapperTest, ConstructorValidParameters) {
  auto mock_agent = std::make_unique<MockDataBatchProtectionAgent>();
  void* dummy_handle = reinterpret_cast<void*>(0x12345678);

  EXPECT_NO_THROW({
    DBPALibraryWrapper wrapper(std::move(mock_agent), dummy_handle,
                               handle_companion_->CreateHandleClosingFunction());
  });
}

TEST_F(DBPALibraryWrapperTest, ConstructorValidParametersWithDefaultClosing) {
  auto mock_agent = std::make_unique<MockDataBatchProtectionAgent>();
  void* dummy_handle = reinterpret_cast<void*>(0x12345678);

  EXPECT_NO_THROW({
    DBPALibraryWrapper wrapper(std::move(mock_agent), dummy_handle,
                               handle_companion_->CreateHandleClosingFunction());
  });
}

TEST_F(DBPALibraryWrapperTest, ConstructorNullAgent) {
  void* dummy_handle = reinterpret_cast<void*>(0x12345678);

  // Test with custom function
  EXPECT_THROW(
      {
        DBPALibraryWrapper wrapper(nullptr, dummy_handle,
                                   handle_companion_->CreateHandleClosingFunction());
      },
      std::invalid_argument);
}

TEST_F(DBPALibraryWrapperTest, ConstructorNullLibraryHandle) {
  auto mock_agent = std::make_unique<MockDataBatchProtectionAgent>();
  void* dummy_handle = reinterpret_cast<void*>(0x12345678);

  // Test with custom function
  EXPECT_THROW(
      { DBPALibraryWrapper wrapper(std::move(mock_agent), dummy_handle, nullptr); },
      std::invalid_argument);
}

// ============================================================================
// HANDLE CLOSING FUNCTION TESTS
// ============================================================================

TEST_F(DBPALibraryWrapperTest, HandleClosingFunctionCalled) {
  void* dummy_handle = reinterpret_cast<void*>(0x12345678);

  // Create wrapper in a scope to trigger destructor
  {
    auto wrapper = CreateWrapper();

    // Verify handle closing hasn't been called yet
    EXPECT_FALSE(handle_companion_->WasHandleCloseCalled());
    EXPECT_EQ(handle_companion_->GetHandleCloseCount(), 0);
  }

  // After wrapper destruction, handle closing should have been called
  EXPECT_TRUE(handle_companion_->WasHandleCloseCalled());
  EXPECT_EQ(handle_companion_->GetHandleCloseCount(), 1);
  EXPECT_EQ(handle_companion_->GetLastClosedHandle(), dummy_handle);
}

TEST_F(DBPALibraryWrapperTest, CustomHandleClosingFunction) {
  bool custom_function_called = false;
  void* custom_last_handle = nullptr;

  auto custom_closing_fn = [&custom_function_called, &custom_last_handle](void* handle) {
    custom_function_called = true;
    custom_last_handle = handle;
  };

  void* dummy_handle = reinterpret_cast<void*>(0x87654321);

  // Create wrapper with custom closing function
  {
    auto mock_agent = std::make_unique<MockDataBatchProtectionAgent>();
    DBPALibraryWrapper wrapper(std::move(mock_agent), dummy_handle, custom_closing_fn);
  }

  // Verify custom function was called
  EXPECT_TRUE(custom_function_called);
  EXPECT_EQ(custom_last_handle, dummy_handle);

  // Verify our handle companion wasn't called
  EXPECT_FALSE(handle_companion_->WasHandleCloseCalled());
  EXPECT_EQ(handle_companion_->GetHandleCloseCount(), 0);
}

// ============================================================================
// DELEGATION FUNCTIONALITY TESTS
// ============================================================================

TEST_F(DBPALibraryWrapperTest, InitDelegation) {
  auto wrapper = CreateWrapper();

  // Test data for init parameters
  std::string column_name = "test_column";
  std::map<std::string, std::string> connection_config = {
      {"host", "localhost"}, {"port", "5432"}, {"database", "testdb"}};
  std::string app_context = "test_app_context";
  std::string column_key_id = "test_key_id";
  Type::type data_type = Type::INT32;
  CompressionCodec::type compression_type = CompressionCodec::SNAPPY;
  std::optional<std::map<std::string, std::string>> column_encryption_metadata =
      std::map<std::string, std::string>{{"metaKey", "metaValue"}};

  // Call init through wrapper
  wrapper->init(column_name, connection_config, app_context, column_key_id, data_type,
                std::nullopt, compression_type, column_encryption_metadata);

  // Verify the mock agent was called
  EXPECT_TRUE(mock_companion_->WasInitCalled());
  EXPECT_EQ(mock_companion_->GetInitCount(), 1);

  // Verify the correct parameters were passed to the mock
  EXPECT_EQ(mock_companion_->GetInitColumnName(), column_name);
  EXPECT_EQ(mock_companion_->GetInitConfigurationProperties(), connection_config);
  EXPECT_EQ(mock_companion_->GetInitAppContext(), app_context);
  EXPECT_EQ(mock_companion_->GetInitColumnKeyId(), column_key_id);
  EXPECT_EQ(mock_companion_->GetInitDataType(), data_type);
  EXPECT_EQ(mock_companion_->GetInitCompressionType(), compression_type);
  ASSERT_TRUE(mock_companion_->GetInitColumnEncryptionMetadata().has_value());
  EXPECT_EQ(mock_companion_->GetInitColumnEncryptionMetadata().value().at("metaKey"),
            "metaValue");
}

TEST_F(DBPALibraryWrapperTest, InitDelegationWithDatatypeLength) {
  auto wrapper = CreateWrapper();

  std::string column_name = "fixed_len_col";
  std::map<std::string, std::string> connection_config = {};
  std::string app_context = "ctx";
  std::string column_key_id = "kid";
  Type::type data_type = Type::FIXED_LEN_BYTE_ARRAY;
  CompressionCodec::type compression_type = CompressionCodec::UNCOMPRESSED;

  wrapper->init(column_name, connection_config, app_context, column_key_id, data_type, 16,
                compression_type, std::nullopt);

  EXPECT_TRUE(mock_companion_->WasInitCalled());
  ASSERT_TRUE(mock_companion_->GetInitDatatypeLength().has_value());
  EXPECT_EQ(mock_companion_->GetInitDatatypeLength().value(), 16);
  EXPECT_FALSE(mock_companion_->GetInitColumnEncryptionMetadata().has_value());
}

TEST_F(DBPALibraryWrapperTest, InitDelegationWithEmptyParameters) {
  auto wrapper = CreateWrapper();

  // Test init with empty parameters
  std::string empty_column_name = "";
  std::map<std::string, std::string> empty_connection_config = {};
  std::string empty_app_context = "";
  std::string empty_column_key_id = "";
  Type::type data_type = Type::BYTE_ARRAY;
  CompressionCodec::type compression_type = CompressionCodec::UNCOMPRESSED;

  // Call init through wrapper
  wrapper->init(empty_column_name, empty_connection_config, empty_app_context,
                empty_column_key_id, data_type, std::nullopt, compression_type,
                std::nullopt);

  // Verify the mock agent was called
  EXPECT_TRUE(mock_companion_->WasInitCalled());
  EXPECT_EQ(mock_companion_->GetInitCount(), 1);

  // Verify the empty parameters were passed correctly
  EXPECT_EQ(mock_companion_->GetInitColumnName(), empty_column_name);
  EXPECT_EQ(mock_companion_->GetInitConfigurationProperties(), empty_connection_config);
  EXPECT_EQ(mock_companion_->GetInitAppContext(), empty_app_context);
  EXPECT_EQ(mock_companion_->GetInitColumnKeyId(), empty_column_key_id);
  EXPECT_EQ(mock_companion_->GetInitDataType(), data_type);
  EXPECT_EQ(mock_companion_->GetInitCompressionType(), compression_type);
  EXPECT_FALSE(mock_companion_->GetInitColumnEncryptionMetadata().has_value());
}

TEST_F(DBPALibraryWrapperTest, EncryptDelegation) {
  auto wrapper = CreateWrapper();

  // Convert test data to spans
  span<const uint8_t> plaintext_span(
      reinterpret_cast<const uint8_t*>(test_plaintext_.data()), test_plaintext_.size());
  span<uint8_t> ciphertext_span(test_ciphertext_.data(), test_ciphertext_.size());

  // Call encrypt through wrapper
  auto result = wrapper->Encrypt(plaintext_span, {});

  // Verify the mock agent was called
  EXPECT_TRUE(mock_companion_->WasEncryptCalled());
  EXPECT_EQ(mock_companion_->GetEncryptCount(), 1);

  // Verify the correct plaintext was passed to the mock
  auto mock_plaintext = mock_companion_->GetEncryptPlaintext();
  std::string mock_plaintext_str(mock_plaintext.begin(), mock_plaintext.end());
  EXPECT_EQ(mock_plaintext_str, test_plaintext_);

  // Verify the correct ciphertext size was passed
  EXPECT_EQ(mock_companion_->GetEncryptCiphertextSize(), test_ciphertext_.size());

  // Verify result is not null
  EXPECT_NE(result, nullptr);
}

TEST_F(DBPALibraryWrapperTest, EncryptDecryptEncodingAttributesDelegation) {
  auto wrapper = CreateWrapper();

  // Encrypt
  std::map<std::string, std::string> expected_result_metadata = {
      {"encryption_algorithm_version", "1"}, {"test_kid", "kid_123"}};
  mock_companion_->SetNextEncryptResultMetadata(expected_result_metadata);
  std::map<std::string, std::string> enc_attrs = {{"format", "plain"}, {"scale", "0"}};
  span<const uint8_t> plaintext_span(
      reinterpret_cast<const uint8_t*>(test_plaintext_.data()), test_plaintext_.size());

  auto enc_result = wrapper->Encrypt(plaintext_span, enc_attrs);
  EXPECT_NE(enc_result, nullptr);
  EXPECT_EQ(mock_companion_->GetLastEncryptEncodingAttrs(), enc_attrs);
  ASSERT_TRUE(enc_result->encryption_metadata().has_value());
  EXPECT_EQ(enc_result->encryption_metadata().value(), expected_result_metadata);

  // Decrypt
  auto ct_span = enc_result->ciphertext();
  std::map<std::string, std::string> dec_attrs = {{"format", "plain"}};
  auto dec_result = wrapper->Decrypt(ct_span, dec_attrs);
  EXPECT_NE(dec_result, nullptr);
  EXPECT_EQ(mock_companion_->GetLastDecryptEncodingAttrs(), dec_attrs);
}

TEST_F(DBPALibraryWrapperTest, DecryptDelegation) {
  auto wrapper = CreateWrapper();

  // Convert test data to spans
  span<const uint8_t> ciphertext_span(
      reinterpret_cast<const uint8_t*>(test_plaintext_.data()), test_plaintext_.size());

  // Call decrypt through wrapper
  auto result = wrapper->Decrypt(ciphertext_span, {});

  // Verify the mock agent was called
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());
  EXPECT_EQ(mock_companion_->GetDecryptCount(), 1);

  // Verify the correct ciphertext was passed to the mock
  auto mock_ciphertext = mock_companion_->GetDecryptCiphertext();
  std::string mock_ciphertext_str(mock_ciphertext.begin(), mock_ciphertext.end());
  EXPECT_EQ(mock_ciphertext_str, test_plaintext_);

  // Verify result is not null
  EXPECT_NE(result, nullptr);
}

TEST_F(DBPALibraryWrapperTest, MultipleEncryptDelegations) {
  auto wrapper = CreateWrapper();

  // Perform multiple encrypt operations
  for (int i = 0; i < 5; ++i) {
    std::string plaintext = "Test " + std::to_string(i);
    std::vector<uint8_t> ciphertext(plaintext.size());

    span<const uint8_t> plaintext_span(reinterpret_cast<const uint8_t*>(plaintext.data()),
                                       plaintext.size());

    auto result = wrapper->Encrypt(plaintext_span, {});
    EXPECT_NE(result, nullptr);
  }

  // Verify the mock agent was called the correct number of times
  EXPECT_TRUE(mock_companion_->WasEncryptCalled());
  EXPECT_EQ(mock_companion_->GetEncryptCount(), 5);
}

TEST_F(DBPALibraryWrapperTest, MultipleDecryptDelegations) {
  auto wrapper = CreateWrapper();

  // Perform multiple decrypt operations
  for (int i = 0; i < 3; ++i) {
    std::string ciphertext = "Test " + std::to_string(i);

    span<const uint8_t> ciphertext_span(
        reinterpret_cast<const uint8_t*>(ciphertext.data()), ciphertext.size());

    auto result = wrapper->Decrypt(ciphertext_span, {});
    EXPECT_NE(result, nullptr);
  }

  // Verify the mock agent was called the correct number of times
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());
  EXPECT_EQ(mock_companion_->GetDecryptCount(), 3);
}

TEST_F(DBPALibraryWrapperTest, MixedOperationsDelegation) {
  auto wrapper = CreateWrapper();

  // Perform mixed encrypt and decrypt operations
  std::vector<std::string> test_data = {"Hello", "World", "Test", "Data"};
  auto call_count = static_cast<int>(test_data.size());

  for (const auto& data : test_data) {
    // Encrypt
    span<const uint8_t> plaintext_span(reinterpret_cast<const uint8_t*>(data.data()),
                                       data.size());

    auto encrypt_result = wrapper->Encrypt(plaintext_span, {});
    EXPECT_NE(encrypt_result, nullptr);

    // Decrypt using the ciphertext from the encryption result
    auto ciphertext_span = encrypt_result->ciphertext();
    auto decrypt_result = wrapper->Decrypt(ciphertext_span, {});
    EXPECT_NE(decrypt_result, nullptr);
  }

  // Verify both operations were called
  EXPECT_TRUE(mock_companion_->WasEncryptCalled());
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());
  EXPECT_EQ(mock_companion_->GetEncryptCount(), call_count);
  EXPECT_EQ(mock_companion_->GetDecryptCount(), call_count);
}

TEST_F(DBPALibraryWrapperTest, InitWithEncryptDecryptOperations) {
  auto wrapper = CreateWrapper();

  // First, initialize the wrapper
  std::string column_name = "test_column";
  std::map<std::string, std::string> connection_config = {{"host", "localhost"},
                                                          {"port", "5432"}};
  std::string app_context = "test_app";
  std::string column_key_id = "test_key";
  Type::type data_type = Type::INT32;
  CompressionCodec::type compression_type = CompressionCodec::SNAPPY;

  wrapper->init(column_name, connection_config, app_context, column_key_id, data_type,
                std::nullopt, compression_type, std::nullopt);

  // Verify init was called
  EXPECT_TRUE(mock_companion_->WasInitCalled());
  EXPECT_EQ(mock_companion_->GetInitCount(), 1);

  // Then perform encrypt/decrypt operations
  std::string test_data = "Test data after init";
  span<const uint8_t> plaintext_span(reinterpret_cast<const uint8_t*>(test_data.data()),
                                     test_data.size());

  auto encrypt_result = wrapper->Encrypt(plaintext_span, {});
  EXPECT_NE(encrypt_result, nullptr);

  auto ciphertext_span = encrypt_result->ciphertext();
  auto decrypt_result = wrapper->Decrypt(ciphertext_span, {});
  EXPECT_NE(decrypt_result, nullptr);

  // Verify all operations were called
  EXPECT_TRUE(mock_companion_->WasEncryptCalled());
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());
  EXPECT_EQ(mock_companion_->GetEncryptCount(), 1);
  EXPECT_EQ(mock_companion_->GetDecryptCount(), 1);

  // Verify init parameters are still accessible
  EXPECT_EQ(mock_companion_->GetInitColumnName(), column_name);
  EXPECT_EQ(mock_companion_->GetInitConfigurationProperties(), connection_config);
  EXPECT_EQ(mock_companion_->GetInitAppContext(), app_context);
  EXPECT_EQ(mock_companion_->GetInitColumnKeyId(), column_key_id);
  EXPECT_EQ(mock_companion_->GetInitDataType(), data_type);
  EXPECT_EQ(mock_companion_->GetInitCompressionType(), compression_type);
  EXPECT_FALSE(mock_companion_->GetInitColumnEncryptionMetadata().has_value());
}

TEST_F(DBPALibraryWrapperTest, DelegationWithEmptyData) {
  auto wrapper = CreateWrapper();

  // Test encryption with empty data
  std::vector<uint8_t> empty_plaintext;

  span<const uint8_t> plaintext_span(empty_plaintext);

  auto encrypt_result = wrapper->Encrypt(plaintext_span, {});
  EXPECT_NE(encrypt_result, nullptr);
  EXPECT_TRUE(mock_companion_->WasEncryptCalled());

  // Test decryption with empty data
  auto ciphertext_span = encrypt_result->ciphertext();
  auto decrypt_result = wrapper->Decrypt(ciphertext_span, {});
  EXPECT_NE(decrypt_result, nullptr);
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());
}

TEST_F(DBPALibraryWrapperTest, DelegationWithNullData) {
  auto wrapper = CreateWrapper();

  // Test encryption with null data pointers but valid spans
  // This tests that the wrapper properly delegates even with null data
  span<const uint8_t> null_plaintext_span(nullptr, size_t{0});

  auto encrypt_result = wrapper->Encrypt(null_plaintext_span, {});
  EXPECT_NE(encrypt_result, nullptr);
  EXPECT_TRUE(mock_companion_->WasEncryptCalled());

  // Test decryption with null data pointer but valid span
  span<const uint8_t> null_decrypt_span(nullptr, size_t{0});
  auto decrypt_result = wrapper->Decrypt(null_decrypt_span, {});
  EXPECT_NE(decrypt_result, nullptr);
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());

  // Verify the mock agent received the correct data (empty vectors)
  auto mock_plaintext = mock_companion_->GetEncryptPlaintext();
  auto mock_ciphertext = mock_companion_->GetDecryptCiphertext();
  EXPECT_EQ(mock_plaintext.size(), 0);
  EXPECT_EQ(mock_ciphertext.size(), 0);
}

// ============================================================================
// DESTRUCTOR FUNCTIONALITY TESTS
// ============================================================================

TEST_F(DBPALibraryWrapperTest, DestructorBasicBehavior) {
  void* dummy_handle = reinterpret_cast<void*>(0x12345678);

  // Create wrapper in a scope to test destructor
  {
    auto wrapper = CreateWrapper();

    // Perform some operations to ensure the wrapper is used
    std::vector<uint8_t> plaintext = {1, 2, 3, 4, 5};

    span<const uint8_t> plaintext_span(plaintext.data(), plaintext.size());

    auto result = wrapper->Encrypt(plaintext_span, {});
    EXPECT_NE(result, nullptr);

    // Verify handle closing hasn't been called yet
    EXPECT_FALSE(handle_companion_->WasHandleCloseCalled());
    EXPECT_EQ(handle_companion_->GetHandleCloseCount(), 0);
  }

  // At this point, the wrapper should have been destroyed and handle closed
  EXPECT_TRUE(handle_companion_->WasHandleCloseCalled());
  EXPECT_EQ(handle_companion_->GetHandleCloseCount(), 1);
  EXPECT_EQ(handle_companion_->GetLastClosedHandle(), dummy_handle);
}

TEST_F(DBPALibraryWrapperTest, DestructorWithMultipleOperations) {
  void* dummy_handle = reinterpret_cast<void*>(0x12345678);

  // Create wrapper in a scope to test destructor
  {
    auto wrapper = CreateWrapper();

    // Perform multiple operations
    for (int i = 0; i < 10; ++i) {
      std::string plaintext = "Test " + std::to_string(i);
      std::vector<uint8_t> ciphertext(plaintext.size());

      span<const uint8_t> plaintext_span(
          reinterpret_cast<const uint8_t*>(plaintext.data()), plaintext.size());

      auto encrypt_result = wrapper->Encrypt(plaintext_span, {});
      EXPECT_NE(encrypt_result, nullptr);

      auto ciphertext_span = encrypt_result->ciphertext();
      auto decrypt_result = wrapper->Decrypt(ciphertext_span, {});
      EXPECT_NE(decrypt_result, nullptr);
    }

    // Verify operations completed but handle not closed yet
    EXPECT_FALSE(handle_companion_->WasHandleCloseCalled());
  }

  // Verify the wrapper was destroyed properly and handle was closed
  EXPECT_TRUE(handle_companion_->WasHandleCloseCalled());
  EXPECT_EQ(handle_companion_->GetHandleCloseCount(), 1);
  EXPECT_EQ(handle_companion_->GetLastClosedHandle(), dummy_handle);
}

TEST_F(DBPALibraryWrapperTest, DestructorOrderVerification) {
  // Clear any previous events from the shared order tracker
  destruction_order_tracker_->Clear();

  // Create a custom mock agent that tracks destruction order
  auto custom_companion = std::make_shared<MockCompanionDBPA>(destruction_order_tracker_);
  auto custom_agent = std::make_unique<MockDataBatchProtectionAgent>(custom_companion);

  void* dummy_handle = reinterpret_cast<void*>(0x12345678);

  // Create wrapper in a scope
  {
    auto wrapper = CreateWrapperWithAgent(std::move(custom_agent));

    // Perform some operations
    std::vector<uint8_t> plaintext = {1, 2, 3};

    span<const uint8_t> plaintext_span(plaintext.data(), plaintext.size());

    auto result = wrapper->Encrypt(plaintext_span, {});
    EXPECT_NE(result, nullptr);

    // Verify neither destructor nor handle closing has been called yet
    EXPECT_FALSE(custom_companion->WasDestructorCalled());
    EXPECT_FALSE(handle_companion_->WasHandleCloseCalled());
    EXPECT_FALSE(destruction_order_tracker_->WasEventRecorded("handle_close"));
    EXPECT_FALSE(destruction_order_tracker_->WasEventRecorded("agent_destructor"));
  }

  // Verify both the custom agent was destroyed and handle was closed
  EXPECT_TRUE(custom_companion->WasDestructorCalled());
  EXPECT_TRUE(handle_companion_->WasHandleCloseCalled());
  EXPECT_EQ(handle_companion_->GetHandleCloseCount(), 1);
  EXPECT_EQ(handle_companion_->GetLastClosedHandle(), dummy_handle);

  // Verify the order of destruction: handle_close should be called BEFORE
  // agent_destructor
  EXPECT_TRUE(destruction_order_tracker_->WasEventRecorded("agent_destructor"));
  EXPECT_TRUE(destruction_order_tracker_->WasEventRecorded("handle_close"));
  EXPECT_TRUE(
      destruction_order_tracker_->VerifyOrder("agent_destructor", "handle_close"));
}

TEST_F(DBPALibraryWrapperTest, DestructionOrderTrackerFunctionality) {
  // Test the destruction order tracker functionality independently
  auto tracker = std::make_shared<DestructionOrderTracker>();

  // Record events in a specific order
  tracker->RecordEvent("first");
  tracker->RecordEvent("second");
  tracker->RecordEvent("third");

  // Verify order tracking
  EXPECT_TRUE(tracker->VerifyOrder("first", "second"));
  EXPECT_TRUE(tracker->VerifyOrder("second", "third"));
  EXPECT_TRUE(tracker->VerifyOrder("first", "third"));

  // Verify reverse order is false
  EXPECT_FALSE(tracker->VerifyOrder("second", "first"));
  EXPECT_FALSE(tracker->VerifyOrder("third", "second"));
  EXPECT_FALSE(tracker->VerifyOrder("third", "first"));

  // Verify sequence numbers
  EXPECT_EQ(tracker->GetEventSequence("first"), 1);
  EXPECT_EQ(tracker->GetEventSequence("second"), 2);
  EXPECT_EQ(tracker->GetEventSequence("third"), 3);

  // Verify event recording
  EXPECT_TRUE(tracker->WasEventRecorded("first"));
  EXPECT_TRUE(tracker->WasEventRecorded("second"));
  EXPECT_TRUE(tracker->WasEventRecorded("third"));
  EXPECT_FALSE(tracker->WasEventRecorded("nonexistent"));
}

// ============================================================================
// INTERFACE COMPLIANCE TESTS
// ============================================================================

TEST_F(DBPALibraryWrapperTest, InterfaceCompliancePolymorphic) {
  auto wrapper = CreateWrapper();

  // Verify the wrapper can be used polymorphically
  DataBatchProtectionAgentInterface* interface_ptr = wrapper.get();
  EXPECT_NE(interface_ptr, nullptr);

  // Test polymorphic init call
  std::string column_name = "polymorphic_column";
  std::map<std::string, std::string> connection_config = {{"test", "value"}};
  std::string app_context = "polymorphic_context";
  std::string column_key_id = "polymorphic_key";
  Type::type data_type = Type::INT64;
  CompressionCodec::type compression_type = CompressionCodec::GZIP;

  interface_ptr->init(column_name, connection_config, app_context, column_key_id,
                      data_type, std::nullopt, compression_type, std::nullopt);

  // Verify init was called through the interface
  EXPECT_TRUE(mock_companion_->WasInitCalled());
  EXPECT_EQ(mock_companion_->GetInitCount(), 1);
  EXPECT_EQ(mock_companion_->GetInitColumnName(), column_name);
  EXPECT_EQ(mock_companion_->GetInitDataType(), data_type);
  EXPECT_EQ(mock_companion_->GetInitCompressionType(), compression_type);
  EXPECT_FALSE(mock_companion_->GetInitColumnEncryptionMetadata().has_value());

  // Test polymorphic encrypt/decrypt calls
  std::vector<uint8_t> plaintext = {1, 2, 3};

  span<const uint8_t> plaintext_span(plaintext.data(), plaintext.size());

  auto encrypt_result = interface_ptr->Encrypt(plaintext_span, {});
  EXPECT_NE(encrypt_result, nullptr);

  auto ciphertext_span = encrypt_result->ciphertext();
  auto decrypt_result = interface_ptr->Decrypt(ciphertext_span, {});
  EXPECT_NE(decrypt_result, nullptr);

  // Verify the mock agent was called through the interface
  EXPECT_TRUE(mock_companion_->WasEncryptCalled());
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

TEST_F(DBPALibraryWrapperTest, EdgeCaseZeroSizeSpans) {
  auto wrapper = CreateWrapper();

  // Test with zero-size spans
  std::vector<uint8_t> empty_data;

  span<const uint8_t> empty_plaintext_span(empty_data);

  auto encrypt_result = wrapper->Encrypt(empty_plaintext_span, {});
  EXPECT_NE(encrypt_result, nullptr);

  auto ciphertext_span = encrypt_result->ciphertext();
  auto decrypt_result = wrapper->Decrypt(ciphertext_span, {});
  EXPECT_NE(decrypt_result, nullptr);

  EXPECT_TRUE(mock_companion_->WasEncryptCalled());
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());
}

TEST_F(DBPALibraryWrapperTest, EdgeCaseSingleByteData) {
  auto wrapper = CreateWrapper();

  // Test with single byte data
  std::vector<uint8_t> single_byte = {0x42};

  span<const uint8_t> plaintext_span(single_byte.data(), single_byte.size());

  auto encrypt_result = wrapper->Encrypt(plaintext_span, {});
  EXPECT_NE(encrypt_result, nullptr);

  auto ciphertext_span = encrypt_result->ciphertext();
  auto decrypt_result = wrapper->Decrypt(ciphertext_span, {});
  EXPECT_NE(decrypt_result, nullptr);

  EXPECT_TRUE(mock_companion_->WasEncryptCalled());
  EXPECT_TRUE(mock_companion_->WasDecryptCalled());
}

}  // namespace parquet::encryption::external::test

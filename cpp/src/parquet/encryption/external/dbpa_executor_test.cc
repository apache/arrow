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

#include "parquet/encryption/external/dbpa_executor.h"

#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <map>
#include <optional>
#include <string>

namespace parquet::encryption::external {

// Concrete implementations for testing
class TestEncryptionResult : public EncryptionResult {
public:
    TestEncryptionResult(std::vector<uint8_t> data, bool success = true, 
                        std::string error_msg = "", 
                        std::map<std::string, std::string> error_fields = {},
                        std::optional<std::map<std::string, std::string>> metadata = std::nullopt)
        : ciphertext_data_(std::move(data)), success_(success), 
          error_message_(std::move(error_msg)), error_fields_(std::move(error_fields)),
          metadata_(std::move(metadata)) {}

    span<const uint8_t> ciphertext() const override {
        return span<const uint8_t>(ciphertext_data_.data(), ciphertext_data_.size());
    }

    std::size_t size() const override { return ciphertext_data_.size(); }
    bool success() const override { return success_; }
    const std::optional<std::map<std::string, std::string>> encryption_metadata() const override {
        return metadata_;
    }
    const std::string& error_message() const override { return error_message_; }
    const std::map<std::string, std::string>& error_fields() const override { return error_fields_; }

private:
    std::vector<uint8_t> ciphertext_data_;
    bool success_;
    std::string error_message_;
    std::map<std::string, std::string> error_fields_;
    std::optional<std::map<std::string, std::string>> metadata_;
};

class TestDecryptionResult : public DecryptionResult {
public:
    TestDecryptionResult(std::vector<uint8_t> data, bool success = true, 
                        std::string error_msg = "", 
                        std::map<std::string, std::string> error_fields = {})
        : plaintext_data_(std::move(data)), success_(success), 
          error_message_(std::move(error_msg)), error_fields_(std::move(error_fields)) {}

    span<const uint8_t> plaintext() const override {
        return span<const uint8_t>(plaintext_data_.data(), plaintext_data_.size());
    }

    std::size_t size() const override { return plaintext_data_.size(); }
    bool success() const override { return success_; }
    const std::string& error_message() const override { return error_message_; }
    const std::map<std::string, std::string>& error_fields() const override { return error_fields_; }

private:
    std::vector<uint8_t> plaintext_data_;
    bool success_;
    std::string error_message_;
    std::map<std::string, std::string> error_fields_;
};

// Mock agent that tracks method calls and parameters for verification
class MockDBPAAgent : public DataBatchProtectionAgentInterface {
public:
    // Track call counts
    int init_call_count_ = 0;
    int encrypt_call_count_ = 0;
    int decrypt_call_count_ = 0;
    
    // Track parameters from last calls
    std::string last_init_column_name_;
    std::map<std::string, std::string> last_init_connection_config_;
    std::string last_init_app_context_;
    std::string last_init_column_key_id_;
    Type::type last_init_data_type_;
    CompressionCodec::type last_init_compression_type_;
    std::optional<int> last_init_datatype_length_;
    std::optional<std::map<std::string, std::string>> last_init_column_encryption_metadata_;
    std::map<std::string, std::string> last_encrypt_encoding_attrs_;
    std::map<std::string, std::string> last_decrypt_encoding_attrs_;
    
    std::vector<uint8_t> last_encrypt_plaintext_;
    std::vector<uint8_t> last_decrypt_ciphertext_;
    
    // Mock results
    std::unique_ptr<TestEncryptionResult> mock_encrypt_result_;
    std::unique_ptr<TestDecryptionResult> mock_decrypt_result_;
    
    // Control behavior
    bool should_throw_on_init_ = false;
    bool should_throw_on_encrypt_ = false;
    bool should_throw_on_decrypt_ = false;
    std::string throw_message_ = "Mock agent error";
    
    void init(std::string column_name,
              std::map<std::string, std::string> connection_config,
              std::string app_context,
              std::string column_key_id,
              Type::type data_type,
              std::optional<int> datatype_length,
              CompressionCodec::type compression_type,
              std::optional<std::map<std::string, std::string>> column_encryption_metadata) override {
        init_call_count_++;
        last_init_column_name_ = column_name;
        last_init_connection_config_ = connection_config;
        last_init_app_context_ = app_context;
        last_init_column_key_id_ = column_key_id;
        last_init_data_type_ = data_type;
        last_init_compression_type_ = compression_type;
        last_init_datatype_length_ = datatype_length;
        last_init_column_encryption_metadata_ = std::move(column_encryption_metadata);
        
        if (should_throw_on_init_) {
            throw std::runtime_error(throw_message_);
        }
    }
    
    std::unique_ptr<EncryptionResult> Encrypt(span<const uint8_t> plaintext,
                                              std::map<std::string, std::string> encoding_attributes) override {
        encrypt_call_count_++;
        last_encrypt_plaintext_.assign(plaintext.begin(), plaintext.end());
        last_encrypt_encoding_attrs_ = std::move(encoding_attributes);
        
        if (should_throw_on_encrypt_) {
            throw std::runtime_error(throw_message_);
        }
        
        // Return a copy of the mock result if available
        if (mock_encrypt_result_) {
            // Create a new result with the same data
            return std::make_unique<TestEncryptionResult>(*mock_encrypt_result_);
        }
        return nullptr;
    }
    
    std::unique_ptr<DecryptionResult> Decrypt(span<const uint8_t> ciphertext,
                                              std::map<std::string, std::string> encoding_attributes) override {
        decrypt_call_count_++;
        last_decrypt_ciphertext_.assign(ciphertext.begin(), ciphertext.end());
        last_decrypt_encoding_attrs_ = std::move(encoding_attributes);
        
        if (should_throw_on_decrypt_) {
            throw std::runtime_error(throw_message_);
        }
        
        // Return a copy of the mock result if available
        if (mock_decrypt_result_) {
            // Create a new result with the same data
            return std::make_unique<TestDecryptionResult>(*mock_decrypt_result_);
        }
        return nullptr;
    }
    
    // Helper methods for test setup
    void ResetCallCounts() {
        init_call_count_ = 0;
        encrypt_call_count_ = 0;
        decrypt_call_count_ = 0;
    }
    
    void SetMockEncryptResult(std::unique_ptr<TestEncryptionResult> result) {
        mock_encrypt_result_ = std::move(result);
    }
    
    void SetMockDecryptResult(std::unique_ptr<TestDecryptionResult> result) {
        mock_decrypt_result_ = std::move(result);
    }
};

class DBPAExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a mock agent to wrap
    mock_agent_ = std::make_unique<MockDBPAAgent>();
    mock_agent_ptr_ = mock_agent_.get(); // Keep raw pointer for verification
    
    // Create the executor that wraps the mock agent with custom timeouts
    executor_ = std::make_unique<DBPAExecutor>(std::move(mock_agent_), 
                                              1000,  // init timeout: 1 second
                                              2000,  // encrypt timeout: 2 seconds  
                                              2000); // decrypt timeout: 2 seconds
  }

  std::unique_ptr<DBPAExecutor> executor_;
  std::unique_ptr<MockDBPAAgent> mock_agent_;
  MockDBPAAgent* mock_agent_ptr_; // Raw pointer for verification
};

TEST_F(DBPAExecutorTest, ConstructorWithNullAgentThrows) {
  EXPECT_THROW(DBPAExecutor(nullptr), std::invalid_argument);
}

TEST_F(DBPAExecutorTest, ConstructorWithInvalidTimeoutsThrows) {
  auto test_agent = std::make_unique<MockDBPAAgent>();
  
  // Test negative timeout
  EXPECT_THROW(DBPAExecutor(std::move(test_agent), -1, 1000, 1000), std::invalid_argument);
  
  // Test zero timeout
  test_agent = std::make_unique<MockDBPAAgent>();
  EXPECT_THROW(DBPAExecutor(std::move(test_agent), 0, 1000, 1000), std::invalid_argument);
}

TEST_F(DBPAExecutorTest, InitForwardsToWrappedAgent) {
  std::string column_name = "test_column";
  std::map<std::string, std::string> connection_config = {{"key", "value"}, {"server", "localhost"}};
  std::string app_context = "test_context";
  std::string column_key_id = "test_key_id";
  Type::type data_type = Type::type::INT32;
  CompressionCodec::type compression_type = CompressionCodec::type::UNCOMPRESSED;

  // Reset call count before test
  mock_agent_ptr_->ResetCallCounts();
  
  // Call init through executor
  std::optional<std::map<std::string, std::string>> column_encryption_metadata = std::map<std::string, std::string>{{"metaKey", "metaValue"}};
  EXPECT_NO_THROW(executor_->init(column_name, connection_config, app_context,
                                 column_key_id, data_type, std::nullopt, compression_type, column_encryption_metadata));
  
  // Verify the mock agent was called exactly once
  EXPECT_EQ(mock_agent_ptr_->init_call_count_, 1);
  
  // Verify all parameters were forwarded correctly
  EXPECT_EQ(mock_agent_ptr_->last_init_column_name_, column_name);
  EXPECT_EQ(mock_agent_ptr_->last_init_connection_config_, connection_config);
  EXPECT_EQ(mock_agent_ptr_->last_init_app_context_, app_context);
  EXPECT_EQ(mock_agent_ptr_->last_init_column_key_id_, column_key_id);
  EXPECT_EQ(mock_agent_ptr_->last_init_data_type_, data_type);
  EXPECT_EQ(mock_agent_ptr_->last_init_compression_type_, compression_type);
  ASSERT_TRUE(mock_agent_ptr_->last_init_column_encryption_metadata_.has_value());
  EXPECT_EQ(mock_agent_ptr_->last_init_column_encryption_metadata_.value().at("metaKey"), "metaValue");
}

TEST_F(DBPAExecutorTest, EncryptForwardsToWrappedAgent) {
  // Initialize the executor first
  executor_->init("test_column", {}, "test_context", "test_key_id",
                 Type::type::INT32, std::nullopt, CompressionCodec::type::UNCOMPRESSED, std::nullopt);

  // Create test data
  std::vector<uint8_t> plaintext = {1, 2, 3, 4, 5};
  span<const uint8_t> plaintext_span(plaintext);

  // Reset call count before test
  mock_agent_ptr_->ResetCallCounts();
  
  // Encrypt should not throw and should return a result
  std::map<std::string, std::string> attrs = {{"format", "plain"}};
  auto result = executor_->Encrypt(plaintext_span, attrs);
  
  // Verify the mock agent was called exactly once
  EXPECT_EQ(mock_agent_ptr_->encrypt_call_count_, 1);
  
  // Verify the plaintext was forwarded correctly
  EXPECT_EQ(mock_agent_ptr_->last_encrypt_plaintext_, plaintext);
  EXPECT_EQ(mock_agent_ptr_->last_encrypt_encoding_attrs_, attrs);
  
  // Note: result might be nullptr since we didn't set up a mock result
  // The important thing is that the call was forwarded
}

TEST_F(DBPAExecutorTest, DecryptForwardsToWrappedAgent) {
  // Initialize the executor first
  executor_->init("test_column", {}, "test_context", "test_key_id",
                 Type::type::INT32, std::nullopt, CompressionCodec::type::UNCOMPRESSED, std::nullopt);

  // Create test data
  std::vector<uint8_t> ciphertext = {5, 4, 3, 2, 1};
  span<const uint8_t> ciphertext_span(ciphertext);

  // Reset call count before test
  mock_agent_ptr_->ResetCallCounts();
  
  // Decrypt should not throw and should return a result
  std::map<std::string, std::string> attrs = {{"format", "plain"}};
  auto result = executor_->Decrypt(ciphertext_span, attrs);
  
  // Verify the mock agent was called exactly once
  EXPECT_EQ(mock_agent_ptr_->decrypt_call_count_, 1);
  
  // Verify the ciphertext was forwarded correctly
  EXPECT_EQ(mock_agent_ptr_->last_decrypt_ciphertext_, ciphertext);
  EXPECT_EQ(mock_agent_ptr_->last_decrypt_encoding_attrs_, attrs);
  
  // Note: result might be nullptr since we didn't set up a mock result
  // The important thing is that the call was forwarded
}

TEST_F(DBPAExecutorTest, InitForwardsDatatypeLength) {
  mock_agent_ptr_->ResetCallCounts();
  EXPECT_NO_THROW(executor_->init("col", {}, "ctx", "kid",
                                  Type::type::INT32, 16, CompressionCodec::type::UNCOMPRESSED, std::nullopt));
  EXPECT_EQ(mock_agent_ptr_->init_call_count_, 1);
  ASSERT_TRUE(mock_agent_ptr_->last_init_datatype_length_.has_value());
  EXPECT_EQ(mock_agent_ptr_->last_init_datatype_length_.value(), 16);
}

// Test that multiple calls are properly forwarded
TEST_F(DBPAExecutorTest, MultipleCallsAreProperlyForwarded) {
  // Reset call counts
  mock_agent_ptr_->ResetCallCounts();
  
  // Make multiple init calls with different parameters
  executor_->init("column1", {{"key1", "value1"}}, "context1", "key1", 
                 Type::type::INT32, std::nullopt, CompressionCodec::type::UNCOMPRESSED, std::nullopt);
  
  // Verify both calls were made
  EXPECT_EQ(mock_agent_ptr_->init_call_count_, 1);
  
  // Verify the last call parameters (second call)
  EXPECT_EQ(mock_agent_ptr_->last_init_column_name_, "column1");
  EXPECT_EQ(mock_agent_ptr_->last_init_connection_config_["key1"], "value1");
  EXPECT_EQ(mock_agent_ptr_->last_init_app_context_, "context1");
  EXPECT_EQ(mock_agent_ptr_->last_init_column_key_id_, "key1");
  EXPECT_EQ(mock_agent_ptr_->last_init_data_type_, Type::type::INT32);
  EXPECT_EQ(mock_agent_ptr_->last_init_compression_type_, CompressionCodec::type::UNCOMPRESSED);
  
  // Make multiple encrypt calls
  std::vector<uint8_t> data1 = {1, 2, 3};
  std::vector<uint8_t> data2 = {4, 5, 6, 7};
  span<const uint8_t> span1(data1);
  span<const uint8_t> span2(data2);
  
  executor_->Encrypt(span1, {});
  executor_->Encrypt(span2, {});
  
  // Verify both encrypt calls were made
  EXPECT_EQ(mock_agent_ptr_->encrypt_call_count_, 2);
  
  // Verify the last encrypt call parameters
  EXPECT_EQ(mock_agent_ptr_->last_encrypt_plaintext_, data2);
  
  // Make multiple decrypt calls
  std::vector<uint8_t> cipher1 = {8, 9, 10};
  std::vector<uint8_t> cipher2 = {11, 12, 13, 14, 15};
  span<const uint8_t> cipher_span1(cipher1);
  span<const uint8_t> cipher_span2(cipher2);
  
  executor_->Decrypt(cipher_span1, {});
  executor_->Decrypt(cipher_span2, {});
  
  // Verify both decrypt calls were made
  EXPECT_EQ(mock_agent_ptr_->decrypt_call_count_, 2);
  
  // Verify the last decrypt call parameters
  EXPECT_EQ(mock_agent_ptr_->last_decrypt_ciphertext_, cipher2);
}

// Test timeout functionality
TEST_F(DBPAExecutorTest, TimeoutExceptionThrownOnSlowOperation) {
  // Create a mock agent that simulates slow operations
  class SlowMockAgent : public DataBatchProtectionAgentInterface {
  public:
      void init(std::string, std::map<std::string, std::string>, std::string, 
                std::string, Type::type, std::optional<int>, CompressionCodec::type,
                std::optional<std::map<std::string, std::string>>) override {
          // Simulate slow operation that takes 200ms
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      
      std::unique_ptr<EncryptionResult> Encrypt(span<const uint8_t>, std::map<std::string, std::string>) override {
          // Simulate slow operation that takes 150ms
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          return nullptr;
      }
      
      std::unique_ptr<DecryptionResult> Decrypt(span<const uint8_t>, std::map<std::string, std::string>) override {
          // Simulate slow operation that takes 120ms
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          return nullptr;
      }
  };
  
  // Create an executor with very short timeouts
  auto slow_agent = std::make_unique<SlowMockAgent>();
  auto timeout_executor = std::make_unique<DBPAExecutor>(std::move(slow_agent), 
                                                         50,  // init timeout: 50ms (will timeout)
                                                         55,  // encrypt timeout: 50ms (will timeout)
                                                         60); // decrypt timeout: 50ms (will timeout)

  // Test init timeout - should throw DBPAExecutorTimeoutException
  try {
    timeout_executor->init("test_column", {}, "test_context", "test_key_id",
                          Type::type::INT32, std::nullopt, CompressionCodec::type::UNCOMPRESSED, std::nullopt);
    FAIL() << "Expected DBPAExecutorTimeoutException to be thrown";
  } catch (const DBPAExecutorTimeoutException& e) {
    // Verify the timeout exception contains expected information
    std::string error_msg = e.what();
    EXPECT_TRUE(error_msg.find("init") != std::string::npos);
    EXPECT_TRUE(error_msg.find("50") != std::string::npos);
    EXPECT_TRUE(error_msg.find("milliseconds") != std::string::npos);
  } catch (...) {
    FAIL() << "Expected DBPAExecutorTimeoutException, but got different exception type";
  }
  
  // Test encrypt timeout - should throw DBPAExecutorTimeoutException
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  span<const uint8_t> data_span(data);
  
  try {
    timeout_executor->Encrypt(data_span, {});
    FAIL() << "Expected DBPAExecutorTimeoutException to be thrown";
  } catch (const DBPAExecutorTimeoutException& e) {
    // Verify the timeout exception contains expected information
    std::string error_msg = e.what();
    EXPECT_TRUE(error_msg.find("encrypt") != std::string::npos);
    EXPECT_TRUE(error_msg.find("55") != std::string::npos);
    EXPECT_TRUE(error_msg.find("milliseconds") != std::string::npos);
  } catch (...) {
    FAIL() << "Expected DBPAExecutorTimeoutException, but got different exception type";
  }
  
  // Test decrypt timeout - should throw DBPAExecutorTimeoutException
  try {
    timeout_executor->Decrypt(data_span, {});
    FAIL() << "Expected DBPAExecutorTimeoutException to be thrown";
  } catch (const DBPAExecutorTimeoutException& e) {
    // Verify the timeout exception contains expected information
    std::string error_msg = e.what();
    EXPECT_TRUE(error_msg.find("decrypt") != std::string::npos);
    EXPECT_TRUE(error_msg.find("60") != std::string::npos);
    EXPECT_TRUE(error_msg.find("milliseconds") != std::string::npos);
  } catch (...) {
    FAIL() << "Expected DBPAExecutorTimeoutException, but got different exception type";
  }
}

// Test that original exceptions are preserved (not wrapped)
TEST_F(DBPAExecutorTest, OriginalExceptionsArePreserved) {
  // Configure mock agent to throw exceptions
  mock_agent_ptr_->should_throw_on_init_ = true;
  mock_agent_ptr_->throw_message_ = "Mock init error";
  
  // Test that init exception is preserved
  try {
    executor_->init("test_column", {}, "test_context", "test_key_id",
                   Type::type::INT32, std::nullopt, CompressionCodec::type::UNCOMPRESSED, std::nullopt);
    FAIL() << "Expected std::runtime_error to be thrown";
  } catch (const std::runtime_error& e) {
    EXPECT_STREQ(e.what(), "Mock init error");
  } catch (...) {
    FAIL() << "Unexpected exception type";
  }
  
  // Reset and test encrypt exception
  mock_agent_ptr_->should_throw_on_init_ = false;
  mock_agent_ptr_->should_throw_on_encrypt_ = true;
  mock_agent_ptr_->throw_message_ = "Mock encrypt error";
  
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  span<const uint8_t> data_span(data);
  
  try {
    executor_->Encrypt(data_span, {});
    FAIL() << "Expected std::runtime_error to be thrown";
  } catch (const std::runtime_error& e) {
    EXPECT_STREQ(e.what(), "Mock encrypt error");
  } catch (...) {
    FAIL() << "Unexpected exception type";
  }
  
  // Reset and test decrypt exception
  mock_agent_ptr_->should_throw_on_encrypt_ = false;
  mock_agent_ptr_->should_throw_on_decrypt_ = true;
  mock_agent_ptr_->throw_message_ = "Mock decrypt error";
  
  try {
    executor_->Decrypt(data_span, {});
    FAIL() << "Expected std::runtime_error to be thrown";
  } catch (const std::runtime_error& e) {
    EXPECT_STREQ(e.what(), "Mock decrypt error");
  } catch (...) {
    FAIL() << "Unexpected exception type";
  }
}

}  // namespace parquet::encryption::external

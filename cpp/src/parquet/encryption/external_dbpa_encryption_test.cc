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

#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <string>
#include <map>
#include <optional>

#include "arrow/util/key_value_metadata.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/external_dbpa_encryption.h"
#include "parquet/encryption/external/test_utils.h"
#include "parquet/encryption/encoding_properties.h"
#include "parquet/encryption/encryption_utils.h"

namespace parquet::encryption::test {

class ExternalDBPAEncryptorAdapterTest : public ::testing::Test {
 protected:
  void SetUp() override {

    // this library will use heuristics to load "libDBPATestAgent.so", needed for tests here.
    std::string library_path = parquet::encryption::external::test::TestUtils::GetTestLibraryPath();

    app_context_ = 
      "{\"user_id\": \"abc123\", \"location\": {\"lat\": 9.7489, \"lon\": -83.7534}}";
    connection_config_ = {
      {"config_path", "path/to/file"}, 
      {"agent_library_path", library_path},
      {"agent_init_timeout_ms", "1000"},
      {"agent_encrypt_timeout_ms", "2000"},
      {"agent_decrypt_timeout_ms", "3000"}
    };
    key_value_metadata_ = KeyValueMetadata::Make({"key1", "key2"}, {"value1", "value2"});
  }

  std::unique_ptr<ExternalDBPAEncryptorAdapter> CreateEncryptor(
    ParquetCipher::type algorithm, std::string column_name, std::string key_id, 
    Type::type data_type, Compression::type compression_type) {
    return ExternalDBPAEncryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, 
      compression_type, app_context_, connection_config_, std::nullopt);
  }

  std::unique_ptr<ExternalDBPADecryptorAdapter> CreateDecryptor(
    ParquetCipher::type algorithm, std::string column_name, std::string key_id, 
    Type::type data_type, Compression::type compression_type) {
    return ExternalDBPADecryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, compression_type, app_context_, 
      connection_config_, std::nullopt, key_value_metadata_);
  }

  void RoundtripEncryption(
      ParquetCipher::type algorithm, std::string column_name, std::string key_id, 
      Type::type data_type, Compression::type compression_type, Encoding::type encoding_type,
      std::string plaintext) {
    std::unique_ptr<ExternalDBPAEncryptorAdapter> encryptor = ExternalDBPAEncryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, compression_type, app_context_, 
      connection_config_, std::nullopt);

    // Create a simple EncodingProperties for testing using the builder pattern
    EncodingPropertiesBuilder builder;
    builder.ColumnPath("test_column")
            .PhysicalType(data_type)
            .CompressionCodec(compression_type)
            .PageType(parquet::PageType::DATA_PAGE_V2)
            .PageV2DefinitionLevelsByteLength(10)
            .PageV2RepetitionLevelsByteLength(10)
            .PageV2NumNulls(10)
            .PageV2IsCompressed(true)
            .DataPageMaxDefinitionLevel(10)
            .DataPageMaxRepetitionLevel(1)
            .PageEncoding(encoding_type)
            .DataPageNumValues(100) 
            .Build();

    encryptor->UpdateEncodingProperties(builder.Build());

    ASSERT_LE(plaintext.size(),
              static_cast<size_t>(std::numeric_limits<int32_t>::max()));
    int32_t expected_ciphertext_length = static_cast<int32_t>(plaintext.size());

    std::shared_ptr<ResizableBuffer> ciphertext_buffer = AllocateBuffer(
      ::arrow::default_memory_pool(), expected_ciphertext_length);
    int32_t encryption_length = encryptor->EncryptWithManagedBuffer(
      str2span(plaintext), ciphertext_buffer.get());  
    ASSERT_EQ(expected_ciphertext_length, encryption_length);

    std::string ciphertext_str(
      ciphertext_buffer->data(), ciphertext_buffer->data() + encryption_length);

    // We know this uses XOR encryption. Therefore, the ciphertext is the same as the plaintext.
    // XOR encrytion encrypts each byte of the plaintext with 0xAA.
    // See external/dbpa_test_agent.cc for the implementation.

    // Assert that plaintext and ciphertext have the same length
    ASSERT_EQ(plaintext.size(), ciphertext_str.size());

    std::unique_ptr<ExternalDBPADecryptorAdapter> decryptor = ExternalDBPADecryptorAdapter::Make(
      algorithm, column_name, key_id, data_type,
      compression_type, app_context_,
      connection_config_, std::nullopt, key_value_metadata_);

    decryptor->UpdateEncodingProperties(builder.Build());

    ASSERT_LE(ciphertext_str.size(),
              static_cast<size_t>(std::numeric_limits<int32_t>::max()));
    int32_t expected_plaintext_length = static_cast<int32_t>(ciphertext_str.size());
    std::shared_ptr<ResizableBuffer> plaintext_buffer = AllocateBuffer(
      ::arrow::default_memory_pool(), expected_plaintext_length);
    int32_t decryption_length = decryptor->DecryptWithManagedBuffer(
      str2span(ciphertext_str), plaintext_buffer.get());
    ASSERT_EQ(expected_plaintext_length, decryption_length);

    std::string plaintext_str(
      plaintext_buffer->data(), plaintext_buffer->data() + decryption_length);

    // Assert that the decrypted plaintext matches the original plaintext
    ASSERT_EQ(plaintext, plaintext_str);
  }

protected:
 std::string empty_string = "";
 std::string app_context_;
 std::map<std::string, std::string> connection_config_;
 std::shared_ptr<const KeyValueMetadata> key_value_metadata_;
};

TEST_F(ExternalDBPAEncryptorAdapterTest, RoundtripEncryptionSucceeds) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;
  Encoding::type encoding_type = Encoding::PLAIN;
  std::string plaintext = "Jean-Luc Picard";

  RoundtripEncryption(
    algorithm, column_name, key_id, data_type, compression_type, encoding_type, plaintext);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, RoundtripEncryption_EmptyPlaintextDoesNotCrash) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;
  Encoding::type encoding_type = Encoding::PLAIN;
  std::string plaintext = "";

  RoundtripEncryption(
    algorithm, column_name, key_id, data_type, compression_type, encoding_type, plaintext);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, GetKeyValueMetadataReturnsNullWhenEmpty) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  auto encryptor = ExternalDBPAEncryptorAdapter::Make(
    algorithm, column_name, key_id, data_type, compression_type,
    app_context_, connection_config_, std::nullopt);

  // No encryption performed yet; metadata should be empty for any module
  auto md_dict = encryptor->GetKeyValueMetadata(parquet::encryption::kDictionaryPage);
  auto md_data = encryptor->GetKeyValueMetadata(parquet::encryption::kDataPage);
  ASSERT_EQ(md_dict, nullptr);
  ASSERT_EQ(md_data, nullptr);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, SignedFooterEncryptionThrowsException) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;
  std::unique_ptr<ExternalDBPAEncryptorAdapter> encryptor = CreateEncryptor(
    algorithm, column_name, key_id, data_type, compression_type);
  std::vector<uint8_t> encrypted_footer(10, '\0');
  EXPECT_THROW(encryptor->SignedFooterEncrypt(
    str2span(/*footer*/""), str2span(/*key*/""), str2span(/*aad*/""), str2span(/*nonce*/""),
    encrypted_footer), ParquetException);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, EncryptWithoutUpdateEncodingPropertiesThrows) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  auto encryptor = ExternalDBPAEncryptorAdapter::Make(
    algorithm, column_name, key_id, data_type, compression_type,
    app_context_, connection_config_, std::nullopt);

  std::string plaintext = "abc";
  std::shared_ptr<ResizableBuffer> ciphertext_buffer = AllocateBuffer(
    ::arrow::default_memory_pool(), 0);
  EXPECT_THROW(
    encryptor->EncryptWithManagedBuffer(
      str2span(plaintext), ciphertext_buffer.get()),
    ParquetException);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, DecryptWithoutUpdateEncodingPropertiesThrows) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  auto decryptor = ExternalDBPADecryptorAdapter::Make(
    algorithm, column_name, key_id, data_type, compression_type,
    app_context_, connection_config_, std::nullopt, key_value_metadata_);

  std::string ciphertext = "xyz";
  std::shared_ptr<ResizableBuffer> plaintext_buffer = AllocateBuffer(
    ::arrow::default_memory_pool(), 0);
  EXPECT_THROW(
    decryptor->DecryptWithManagedBuffer(
      str2span(ciphertext), plaintext_buffer.get()),
    ParquetException);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, EncryptorUnsupportedAlgorithmThrows) {
  // Use AES_GCM_V1 (unsupported) to verify the adapter rejects algorithms other 
  // than EXTERNAL_DBPA_V1
  ParquetCipher::type unsupported_algo = ParquetCipher::AES_GCM_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  EXPECT_THROW(
    CreateEncryptor(
      unsupported_algo, column_name, key_id, data_type, compression_type),
    ParquetException);

  // Also test AES_GCM_CTR_V1
  unsupported_algo = ParquetCipher::AES_GCM_CTR_V1;
  EXPECT_THROW(
    CreateEncryptor(
      unsupported_algo, column_name, key_id, data_type, compression_type),
    ParquetException);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, DecryptorUnsupportedAlgorithmThrows) {
  // Use AES_GCM_V1 (unsupported) to verify the adapter rejects algorithms other
  // than EXTERNAL_DBPA_V1
  ParquetCipher::type unsupported_algo = ParquetCipher::AES_GCM_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  EXPECT_THROW(
    ExternalDBPADecryptorAdapter::Make(
      unsupported_algo, column_name, key_id, data_type, compression_type,
      app_context_, connection_config_, std::nullopt, key_value_metadata_),
    ParquetException);

  // Also test AES_GCM_CTR_V1
  unsupported_algo = ParquetCipher::AES_GCM_CTR_V1;
  EXPECT_THROW(
    ExternalDBPADecryptorAdapter::Make(
      unsupported_algo, column_name, key_id, data_type, compression_type,
      app_context_, connection_config_, std::nullopt, key_value_metadata_),
    std::exception);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, EncryptorMissingLibraryPathThrows) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  std::map<std::string, std::string> bad_config = { {"config_path", "path/to/file"} };
  std::string app_context = "{}";

  EXPECT_THROW(
    ExternalDBPAEncryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, compression_type,
      app_context, bad_config, std::nullopt),
  std::exception);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, EncryptorInvalidLibraryPathThrows) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  std::map<std::string, std::string> bad_config = {
    {"agent_library_path", "/definitely/not/a/real/libDBPA.so"}
  };
  std::string app_context = "{}";

  EXPECT_THROW(
    ExternalDBPAEncryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, compression_type,
      app_context, bad_config, std::nullopt),
    std::exception);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, EncryptorInvalidTimeoutValuesThrows) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  std::string library_path = parquet::encryption::external::test::TestUtils::GetTestLibraryPath();
  std::map<std::string, std::string> bad_config = {
    {"config_path", "path/to/file"}, 
    {"agent_library_path", library_path},
    {"agent_init_timeout_ms", "nope"},
  };
  std::string app_context = "{}";

  EXPECT_THROW(
    ExternalDBPAEncryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, compression_type,
      app_context, bad_config, std::nullopt),
    std::exception);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, DecryptorMissingLibraryPathThrows) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  std::map<std::string, std::string> bad_config = { {"config_path", "path/to/file"} };
  std::string app_context = "{}";

  EXPECT_THROW(
    ExternalDBPADecryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, compression_type,
      app_context, bad_config, std::nullopt, key_value_metadata_),
    std::exception);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, DecryptorInvalidLibraryPathThrows) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  std::map<std::string, std::string> bad_config = {
    {"agent_library_path", "/definitely/not/a/real/libDBPA.so"}
  };
  std::string app_context = "{}";

  EXPECT_THROW(
    ExternalDBPADecryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, compression_type,
      app_context, bad_config, std::nullopt, key_value_metadata_),
    std::exception);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, DecryptorInvalidTimeoutValuesThrows) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;

  std::string library_path = parquet::encryption::external::test::TestUtils::GetTestLibraryPath();
  std::map<std::string, std::string> bad_config = {
    {"config_path", "path/to/file"}, 
    {"agent_library_path", library_path},
    {"agent_init_timeout_ms", "nope"},
  };
  std::string app_context = "{}";

  EXPECT_THROW(
    ExternalDBPADecryptorAdapter::Make(
      algorithm, column_name, key_id, data_type, compression_type,
      app_context, bad_config, std::nullopt, key_value_metadata_),
    std::exception);
}

// Helper stub EncryptionResult for testing metadata accumulation
class StubEncryptionResult : public dbps::external::EncryptionResult {
public:
  explicit StubEncryptionResult(std::map<std::string, std::string> metadata)
    : metadata_(std::move(metadata)) {}

  span<const uint8_t> ciphertext() const override { return {}; }
  std::size_t size() const override { return 0; }
  bool success() const override { return true; }
  const std::optional<std::map<std::string, std::string>> encryption_metadata() const override {
    return metadata_;
  }
  const std::string& error_message() const override { return empty_; }
  const std::map<std::string, std::string>& error_fields() const override { return empty_fields_; }

private:
  std::optional<std::map<std::string, std::string>> metadata_;
  mutable std::string empty_;
  mutable std::map<std::string, std::string> empty_fields_;
};

TEST_F(ExternalDBPAEncryptorAdapterTest, UpdateEncryptorMetadataAccumulatesByModuleType) {
  // Build EncodingProperties for dictionary page and data page V2
  EncodingPropertiesBuilder dict_builder;
  dict_builder
    .ColumnPath("col")
    .PhysicalType(Type::BYTE_ARRAY)
    .CompressionCodec(Compression::UNCOMPRESSED)
    .PageType(parquet::PageType::DICTIONARY_PAGE)
    .PageEncoding(Encoding::PLAIN);
  auto dict_props = dict_builder.Build();

  EncodingPropertiesBuilder data_builder;
  data_builder
    .ColumnPath("col")
    .PhysicalType(Type::BYTE_ARRAY)
    .CompressionCodec(Compression::UNCOMPRESSED)
    .PageType(parquet::PageType::DATA_PAGE_V2)
    .PageEncoding(Encoding::PLAIN)
    .DataPageNumValues(100)
    .DataPageMaxDefinitionLevel(1)
    .DataPageMaxRepetitionLevel(0)
    .PageV2DefinitionLevelsByteLength(4)
    .PageV2RepetitionLevelsByteLength(4)
    .PageV2NumNulls(0)
    .PageV2IsCompressed(true);
  auto data_props = data_builder.Build();

  // Prepare result metadata
  std::map<std::string, std::string> meta1 = {{"m1", "v1"}, {"m2", "v2"}};
  std::map<std::string, std::string> meta2 = {{"m2", "v2_override"}, {"m3", "v3"}};

  StubEncryptionResult r1(meta1);
  StubEncryptionResult r2(meta2);

  std::map<int8_t, std::map<std::string, std::string>> accum;

  // Call helper under test
  UpdateEncryptorMetadata(accum, *dict_props, r1);
  UpdateEncryptorMetadata(accum, *data_props, r2);

  // Verify dictionary page accumulation
  ASSERT_NE(accum.find(parquet::encryption::kDictionaryPage), accum.end());
  const auto& dict_map = accum.at(parquet::encryption::kDictionaryPage);
  ASSERT_EQ(dict_map.at("m1"), "v1");
  ASSERT_EQ(dict_map.at("m2"), "v2");

  // Verify data page accumulation with overwrite behavior
  ASSERT_NE(accum.find(parquet::encryption::kDataPage), accum.end());
  const auto& data_map = accum.at(parquet::encryption::kDataPage);
  ASSERT_EQ(data_map.at("m2"), "v2_override");
  ASSERT_EQ(data_map.at("m3"), "v3");
}

TEST_F(ExternalDBPAEncryptorAdapterTest, DecryptWithWrongKeyIdFails) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string correct_key_id = "employee_name_key";
  std::string wrong_key_id = "wrong_key_id";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;
  Encoding::type encoding_type = Encoding::PLAIN;
  std::string app_context = "{}";
  std::map<std::string, std::string> config = {
    {"agent_library_path", parquet::encryption::external::test::TestUtils::GetTestLibraryPath()}
  };

  auto encryptor = ExternalDBPAEncryptorAdapter::Make(
    algorithm, column_name, correct_key_id, data_type, compression_type,
    app_context, config, std::nullopt);

  // Build encoding properties
  EncodingPropertiesBuilder builder;
  builder.ColumnPath("test_column")
         .PhysicalType(data_type)
         .CompressionCodec(compression_type)
         .PageType(parquet::PageType::DATA_PAGE_V2)
         .PageV2DefinitionLevelsByteLength(10)
         .PageV2RepetitionLevelsByteLength(10)
         .PageV2NumNulls(10)
         .PageV2IsCompressed(true)
         .DataPageMaxDefinitionLevel(10)
         .DataPageMaxRepetitionLevel(1)
         .PageEncoding(encoding_type)
         .DataPageNumValues(100)
         .Build();

  encryptor->UpdateEncodingProperties(builder.Build());

  std::string plaintext = "Sensitive Data";
  std::shared_ptr<ResizableBuffer> ciphertext_buffer = AllocateBuffer(
    ::arrow::default_memory_pool(), 0);

  std::string empty;
  int32_t enc_len = encryptor->EncryptWithManagedBuffer(
    str2span(plaintext), ciphertext_buffer.get());

  std::string ciphertext_str(ciphertext_buffer->data(), ciphertext_buffer->data() + enc_len);

  auto decryptor = ExternalDBPADecryptorAdapter::Make(
    algorithm, column_name, wrong_key_id, data_type, compression_type,
    app_context, config, std::nullopt, key_value_metadata_);

  decryptor->UpdateEncodingProperties(builder.Build());

  std::shared_ptr<ResizableBuffer> plaintext_buffer = AllocateBuffer(
    ::arrow::default_memory_pool(), 0);

  bool threw = false;
  int32_t dec_len = 0;
  try {
    dec_len = decryptor->DecryptWithManagedBuffer(
      str2span(ciphertext_str), plaintext_buffer.get());
  } catch (const ParquetException&) {
    threw = true;
  }

  if (!threw) {
    std::string decrypted(plaintext_buffer->data(), plaintext_buffer->data() + dec_len);
    ASSERT_NE(plaintext, decrypted);
  }
}

TEST_F(ExternalDBPAEncryptorAdapterTest, EncryptCallShouldFail) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;
  std::string plaintext = "Jean-Luc Picard";
  std::vector<uint8_t> ciphertext_buffer(plaintext.size(), '\0');

  std::unique_ptr<ExternalDBPAEncryptorAdapter> encryptor = CreateEncryptor(
    algorithm, column_name, key_id, data_type, compression_type);
  ASSERT_FALSE(encryptor->CanCalculateCiphertextLength());
  ASSERT_LE(plaintext.size(),
            static_cast<size_t>(std::numeric_limits<int32_t>::max()));
  EXPECT_THROW(
    (void) encryptor->CiphertextLength(static_cast<int32_t>(plaintext.size())),
    ParquetException);
  EXPECT_THROW(
    encryptor->Encrypt(
      str2span(plaintext), str2span(/*key*/""), str2span(/*aad*/""), ciphertext_buffer),
    ParquetException);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, DecryptCallShouldFail) {
  ParquetCipher::type algorithm = ParquetCipher::EXTERNAL_DBPA_V1;
  std::string column_name = "employee_name";
  std::string key_id = "employee_name_key";
  Type::type data_type = Type::BYTE_ARRAY;
  Compression::type compression_type = Compression::UNCOMPRESSED;
  std::string ciphertext = "Jean-Luc Picard";
  std::vector<uint8_t> plaintext_buffer(ciphertext.size(), '\0');

  std::unique_ptr<ExternalDBPADecryptorAdapter> decryptor = CreateDecryptor(
    algorithm, column_name, key_id, data_type, compression_type);
  ASSERT_FALSE(decryptor->CanCalculateLengths());
  ASSERT_LE(ciphertext.size(),
            static_cast<size_t>(std::numeric_limits<int32_t>::max()));
  EXPECT_THROW(
    (void) decryptor->CiphertextLength(static_cast<int32_t>(ciphertext.size())),
    ParquetException);
  EXPECT_THROW(
    (void) decryptor->PlaintextLength(static_cast<int32_t>(ciphertext.size())),
    ParquetException);
  EXPECT_THROW(
    decryptor->Decrypt(
      str2span(ciphertext), str2span(/*key*/""), str2span(/*aad*/""), plaintext_buffer),
    ParquetException);
}

TEST_F(ExternalDBPAEncryptorAdapterTest, KeyValueMetadataToStringMap_Nullptr) {
  std::shared_ptr<const KeyValueMetadata> md = nullptr;
  auto result = ExternalDBPAUtils::KeyValueMetadataToStringMap(md);
  ASSERT_FALSE(result.has_value());
}

TEST_F(ExternalDBPAEncryptorAdapterTest, KeyValueMetadataToStringMap_Empty) {
  auto md = ::arrow::key_value_metadata(std::vector<std::string>{}, std::vector<std::string>{});
  auto result = ExternalDBPAUtils::KeyValueMetadataToStringMap(md);
  ASSERT_FALSE(result.has_value());
}

TEST_F(ExternalDBPAEncryptorAdapterTest, KeyValueMetadataToStringMap_Basic) {
  auto md = KeyValueMetadata::Make({"k1", "k2"}, {"v1", "v2"});
  auto result = ExternalDBPAUtils::KeyValueMetadataToStringMap(md);
  ASSERT_TRUE(result.has_value());
  const auto& m = result.value();
  ASSERT_EQ(m.size(), 2u);
  ASSERT_EQ(m.at("k1"), std::string("v1"));
  ASSERT_EQ(m.at("k2"), std::string("v2"));
}

TEST_F(ExternalDBPAEncryptorAdapterTest, KeyValueMetadataToStringMap_MismatchedLengths) {
  auto md_more_keys = KeyValueMetadata::Make({"a", "b", "c"}, {"1", "2"});
  auto res1 = ExternalDBPAUtils::KeyValueMetadataToStringMap(md_more_keys);
  ASSERT_TRUE(res1.has_value());
  const auto& m1 = res1.value();
  ASSERT_EQ(m1.size(), 2u);
  ASSERT_EQ(m1.at("a"), std::string("1"));
  ASSERT_EQ(m1.at("b"), std::string("2"));
  ASSERT_TRUE(m1.find("c") == m1.end());

  auto md_more_vals = KeyValueMetadata::Make({"only"}, {"v1", "v2"});
  auto res2 = ExternalDBPAUtils::KeyValueMetadataToStringMap(md_more_vals);
  ASSERT_TRUE(res2.has_value());
  const auto& m2 = res2.value();
  ASSERT_EQ(m2.size(), 1u);
  ASSERT_EQ(m2.at("only"), std::string("v1"));
}

}  // namespace parquet::encryption::test

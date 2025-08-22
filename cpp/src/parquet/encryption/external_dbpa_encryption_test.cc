// What license shall we use for this file?

#include <gtest/gtest.h>

#include "parquet/encryption/encryption.h"
#include "parquet/encryption/external_dbpa_encryption_adapter.h"

/// TODO(sbrenes): Add proper testing. Right now we are just going to test that the
/// encryptor and decryptor are created and that the plaintext is returned as the ciphertext.

namespace parquet::encryption::test {

class ExternalDBPAEncryptionAdapterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    app_context_ = 
      "{\"user_id\": \"abc123\", \"location\": {\"lat\": 9.7489, \"lon\": -83.7534}}";
    connection_config_ = {
      {"lib_name", "dbpa_lib.so"},
      {"config_path", "path/to/file"}
    };
  }

  void RoundtripEncryption(
      ParquetCipher::type algorithm, std::string column_name, std::string key_id, 
      Type::type data_type, Compression::type compression_type, Encoding::type encoding_type,
      std::string plaintext) {
    ExternalDBPAEncryptorAdapter encryptor(
        algorithm, column_name, key_id, data_type, compression_type, encoding_type,
        app_context_, connection_config_);

    int32_t expected_ciphertext_length = plaintext.size();
    int32_t actual_ciphertext_length = encryptor.CiphertextLength(plaintext.size());
    ASSERT_EQ(expected_ciphertext_length, actual_ciphertext_length);

    std::vector<uint8_t> ciphertext_buffer(expected_ciphertext_length, '\0');
    int32_t encryption_length = encryptor.Encrypt(
      str2span(plaintext), str2span(empty_string), str2span(empty_string), ciphertext_buffer);
    ASSERT_EQ(expected_ciphertext_length, encryption_length);

    std::string ciphertext_str(ciphertext_buffer.begin(), ciphertext_buffer.end());
    ASSERT_EQ(plaintext, ciphertext_str);

    ExternalDBPADecryptorAdapter decryptor(algorithm, column_name, key_id, data_type,
                                           compression_type, encoding_type, app_context_,
                                           connection_config_);

    int32_t expected_plaintext_length = ciphertext_str.size();
    int32_t actual_plaintext_length = decryptor.PlaintextLength(ciphertext_str.size());
    ASSERT_EQ(expected_plaintext_length, actual_plaintext_length);

    std::vector<uint8_t> plaintext_buffer(expected_plaintext_length, '\0');
    int32_t decryption_length = decryptor.Decrypt(
      str2span(ciphertext_str), str2span(empty_string), str2span(empty_string), plaintext_buffer);
    ASSERT_EQ(expected_plaintext_length, decryption_length);

    std::string plaintext_str(plaintext_buffer.begin(), plaintext_buffer.end());
    ASSERT_EQ(plaintext, plaintext_str);
  }
  
private:
 std::string empty_string = "";
 std::string app_context_;
 std::map<std::string, std::string> connection_config_;
};

TEST_F(ExternalDBPAEncryptionAdapterTest, RoundtripEncryptionSucceeds) {
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

}  // namespace parquet::encryption::test

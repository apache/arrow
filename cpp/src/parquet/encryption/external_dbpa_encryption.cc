// What license shall we use for this file?

#include <iostream>

#include "parquet/encryption/external_dbpa_encryption_adapter.h"

/// TODO(sbrenes): Add proper implementation. Right now we are just going to return
/// the plaintext as the ciphertext.

namespace parquet::encryption {

ExternalDBPAEncryptorAdapter::ExternalDBPAEncryptorAdapter(
    ParquetCipher::type algorithm, std::string column_name, std::string key_id,
    Type::type data_type, Compression::type compression_type, Encoding::type encoding_type,
    std::string app_context, std::map<std::string, std::string> connection_config)
    : algorithm_(algorithm), column_name_(column_name), key_id_(key_id),
      data_type_(data_type), compression_type_(compression_type),
      encoding_type_(encoding_type), app_context_(app_context),
      connection_config_(connection_config) {}
  
std::unique_ptr<ExternalDBPAEncryptorAdapter> ExternalDBPAEncryptorAdapter::Make(
    ParquetCipher::type algorithm, std::string column_name, std::string key_id,
    Type::type data_type, Compression::type compression_type, Encoding::type encoding_type,
    std::string app_context, std::map<std::string, std::string> connection_config) {
  return std::make_unique<ExternalDBPAEncryptorAdapter>(
      algorithm, column_name, key_id, data_type, compression_type, encoding_type,
      app_context, connection_config);
}

int32_t ExternalDBPAEncryptorAdapter::CiphertextLength(int64_t plaintext_len) const {
  return plaintext_len;
}
  
int32_t ExternalDBPAEncryptorAdapter::Encrypt(
    ::arrow::util::span<const uint8_t> plaintext, ::arrow::util::span<const uint8_t> key,
    ::arrow::util::span<const uint8_t> aad, ::arrow::util::span<uint8_t> ciphertext) {
  return CallExternalDBPA(plaintext, ciphertext);
}

int32_t ExternalDBPAEncryptorAdapter::SignedFooterEncrypt(
    ::arrow::util::span<const uint8_t> footer, ::arrow::util::span<const uint8_t> key,
    ::arrow::util::span<const uint8_t> aad, ::arrow::util::span<const uint8_t> nonce,
    ::arrow::util::span<uint8_t> encrypted_footer) {
  return CallExternalDBPA(footer, encrypted_footer);
}

int32_t ExternalDBPAEncryptorAdapter::CallExternalDBPA(
    ::arrow::util::span<const uint8_t> plaintext, ::arrow::util::span<uint8_t> ciphertext) {
  std::cout << "\n*-*-*- START: ExternalDBPAEncryptor::Encrypt Hello World! *-*-*-" << std::endl;
  std::cout << "Encryption Algorithm: [" << algorithm_ << "]" << std::endl;
  std::cout << "Column Name: [" << column_name_ << "]" << std::endl;
  std::cout << "Key ID: [" << key_id_ << "]" << std::endl;
  std::cout << "Data Type: [" << data_type_ << "]" << std::endl;
  std::cout << "Compression Type: [" << compression_type_ << "]" << std::endl;
  std::cout << "Encoding Type: [" << encoding_type_ << "]" << std::endl;
  std::cout << "App Context: [" << app_context_ << "]" << std::endl;
  std::cout << "Connection Config:" << std::endl;
  for (const auto& [key, value] : connection_config_) {
    std::cout << "  [" << key << "]: [" << value << "]" << std::endl;
  }

  std::copy(plaintext.begin(), plaintext.end(), ciphertext.begin());

  std::string plaintext_str(plaintext.begin(), plaintext.end());
  std::string ciphertext_str(ciphertext.begin(), ciphertext.end());
  std::cout << "Plaintext: [" << plaintext_str << "]" << std::endl;
  std::cout << "Ciphertext: [" << ciphertext_str << "]" << std::endl;
  std::cout << "*-*-*- END: ExternalDBPAEncryptor::Encrypt Hello World! *-*-*-\n" << std::endl;

  return ciphertext.size();
}

ExternalDBPADecryptorAdapter::ExternalDBPADecryptorAdapter(
    ParquetCipher::type algorithm, std::string column_name, std::string key_id,
    Type::type data_type, Compression::type compression_type, Encoding::type encoding_type,
    std::string app_context, std::map<std::string, std::string> connection_config)
    : algorithm_(algorithm), column_name_(column_name), key_id_(key_id),
      data_type_(data_type), compression_type_(compression_type),
      encoding_type_(encoding_type), app_context_(app_context),
      connection_config_(connection_config) {}

std::unique_ptr<ExternalDBPADecryptorAdapter> ExternalDBPADecryptorAdapter::Make(
    ParquetCipher::type algorithm, std::string column_name, std::string key_id,
    Type::type data_type, Compression::type compression_type, Encoding::type encoding_type,
    std::string app_context, std::map<std::string, std::string> connection_config) {
  return std::make_unique<ExternalDBPADecryptorAdapter>(
      algorithm, column_name, key_id, data_type, compression_type, encoding_type,
      app_context, connection_config);
}

int32_t ExternalDBPADecryptorAdapter::PlaintextLength(int32_t ciphertext_len) const {
  return ciphertext_len;
}

int32_t ExternalDBPADecryptorAdapter::CiphertextLength(int32_t plaintext_len) const {
  return plaintext_len;
}

int32_t ExternalDBPADecryptorAdapter::Decrypt(
    ::arrow::util::span<const uint8_t> ciphertext, ::arrow::util::span<const uint8_t> key,
    ::arrow::util::span<const uint8_t> aad, ::arrow::util::span<uint8_t> plaintext) {
  return CallExternalDBPA(ciphertext, plaintext);
}

int32_t ExternalDBPADecryptorAdapter::CallExternalDBPA(
    ::arrow::util::span<const uint8_t> ciphertext, ::arrow::util::span<uint8_t> plaintext) {
  std::cout << "\n*-*-*- START: ExternalDBPADecryptor::Decrypt Hello World! *-*-*-" << std::endl;
  std::cout << "Decryption Algorithm: [" << algorithm_ << "]" << std::endl;
  std::cout << "Column Name: [" << column_name_ << "]" << std::endl;
  std::cout << "Key ID: [" << key_id_ << "]" << std::endl;
  std::cout << "Data Type: [" << data_type_ << "]" << std::endl;
  std::cout << "Compression Type: [" << compression_type_ << "]" << std::endl;
  std::cout << "Encoding Type: [" << encoding_type_ << "]" << std::endl;
  std::cout << "App Context: [" << app_context_ << "]" << std::endl;
  std::cout << "Connection Config:" << std::endl;
  for (const auto& [key, value] : connection_config_) {
    std::cout << "  [" << key << "]: [" << value << "]" << std::endl;
  }

  std::copy(ciphertext.begin(), ciphertext.end(), plaintext.begin());

  std::string plaintext_str(plaintext.begin(), plaintext.end());
  std::string ciphertext_str(ciphertext.begin(), ciphertext.end());
  std::cout << "Plaintext: [" << plaintext_str << "]" << std::endl;
  std::cout << "Ciphertext: [" << ciphertext_str << "]" << std::endl;
  std::cout << "*-*-*- END: ExternalDBPADecryptor::Decrypt Hello World! *-*-*-\n" << std::endl;

  return plaintext.size();
}

}  // namespace parquet::encryption

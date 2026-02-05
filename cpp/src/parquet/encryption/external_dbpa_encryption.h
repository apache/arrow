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

#include <map>
#include <memory>
#include <optional>
#include <vector>

#include <dbpa_interface.h>

#include "parquet/encryption/decryptor_interface.h"
#include "parquet/encryption/encoding_properties.h"
#include "parquet/encryption/encryptor_interface.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/types.h"

using dbps::external::DataBatchProtectionAgentInterface;

namespace parquet::encryption {

/// Call an external Data Batch Protection Agent (DBPA) to encrypt data.
class PARQUET_EXPORT ExternalDBPAEncryptorAdapter : public EncryptorInterface {
 public:
  static std::unique_ptr<ExternalDBPAEncryptorAdapter> Make(
      ParquetCipher::type algorithm, std::string column_name, std::string key_id,
      Type::type data_type, Compression::type compression_type, std::string app_context,
      std::map<std::string, std::string> configuration_properties,
      std::optional<int> datatype_length);

  ~ExternalDBPAEncryptorAdapter() = default;

  /// Signal whether the encryptor can calculate a valid ciphertext length before
  /// performing encryption.
  [[nodiscard]] bool CanCalculateCiphertextLength() const override { return false; }

  /// The size of the ciphertext, for this cipher and the specified plaintext length.
  [[nodiscard]] int32_t CiphertextLength(int64_t plaintext_len) const override;

  /// Encryption not supported as we cannot calculate the ciphertext before encryption.
  int32_t Encrypt(::arrow::util::span<const uint8_t> plaintext,
                  ::arrow::util::span<const uint8_t> key,
                  ::arrow::util::span<const uint8_t> aad,
                  ::arrow::util::span<uint8_t> ciphertext) override {
    std::stringstream ss;
    ss << "Encrypt is not supported in ExternalDBPAEncryptorAdapter, ";
    ss << "use EncryptWithManagedBuffer instead";
    throw ParquetException(ss.str());
  }

  /// Encrypt the plaintext and leave the results in the ciphertext buffer.
  /// The buffer will be resized to the appropriate size by the agent during encryption.
  int32_t EncryptWithManagedBuffer(::arrow::util::span<const uint8_t> plaintext,
                                   ::arrow::ResizableBuffer* ciphertext) override;

  /// Encrypts plaintext footer, in order to compute footer signature (tag).
  int32_t SignedFooterEncrypt(::arrow::util::span<const uint8_t> footer,
                              ::arrow::util::span<const uint8_t> key,
                              ::arrow::util::span<const uint8_t> aad,
                              ::arrow::util::span<const uint8_t> nonce,
                              ::arrow::util::span<uint8_t> encrypted_footer) override;

  void UpdateEncodingProperties(
      std::unique_ptr<EncodingProperties> encoding_properties) override;

  std::shared_ptr<KeyValueMetadata> GetKeyValueMetadata(int8_t module_type) override;

 private:
  // agent_instance is assumed to be initialized at the time of construction.
  // No initialization nor checks to verify that it is initialized are performed.

  ExternalDBPAEncryptorAdapter(
      ParquetCipher::type algorithm, std::string column_name, std::string key_id,
      Type::type data_type, Compression::type compression_type,
      std::optional<int> datatype_length, std::string app_context,
      std::map<std::string, std::string> configuration_properties,
      std::unique_ptr<DataBatchProtectionAgentInterface> agent_instance);

  int32_t InvokeExternalEncrypt(::arrow::util::span<const uint8_t> plaintext,
                                ::arrow::ResizableBuffer* ciphertext,
                                std::map<std::string, std::string> encoding_attrs);

  ParquetCipher::type algorithm_;
  std::string column_name_;
  std::string key_id_;
  Type::type data_type_;
  Compression::type compression_type_;
  std::optional<int> datatype_length_;
  std::string app_context_;
  std::map<std::string, std::string> configuration_properties_;

  std::unique_ptr<dbps::external::DataBatchProtectionAgentInterface> agent_instance_;

  std::unique_ptr<EncodingProperties> encoding_properties_;
  bool encoding_properties_updated_ = false;

  // Accumulated column encryption metadata per module type (e.g., data page,
  // dictionary page) to be used later by GetKeyValueMetadata.
  std::map<int8_t, std::map<std::string, std::string>> column_encryption_metadata_;
};

// Utilities for External DBPA adapters
class PARQUET_EXPORT ExternalDBPAUtils {
 public:
  // Convert Arrow KeyValueMetadata to a std::map<string, string>.
  // Returns std::nullopt if the input is null or contains no pairs.
  static std::optional<std::map<std::string, std::string>> KeyValueMetadataToStringMap(
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata);
};

// Update encryptor-level metadata accumulator based on encoding attributes and
// EncryptionResult-provided metadata. If no metadata is available or page_type is
// unsupported/absent, function performs no-op.
PARQUET_EXPORT
void UpdateEncryptorMetadata(
    std::map<int8_t, std::map<std::string, std::string>>& metadata_by_module,
    const EncodingProperties& encoding_properties,
    const dbps::external::EncryptionResult& result);

/// Factory for ExternalDBPAEncryptorAdapter instances. The cache exists while the write
/// operation is open, and is used to guarantee the lifetime of the encryptor.
class PARQUET_EXPORT ExternalDBPAEncryptorAdapterFactory {
 public:
  ExternalDBPAEncryptorAdapter* GetEncryptor(
      ParquetCipher::type algorithm,
      const ColumnChunkMetaDataBuilder* column_chunk_metadata,
      ExternalFileEncryptionProperties* external_file_encryption_properties);

 private:
  std::map<std::string, std::unique_ptr<ExternalDBPAEncryptorAdapter>> encryptor_cache_;
};

/// Call an external Data Batch Protection Agent (DBPA) to decrypt data.
/// connection configuration provided.
class PARQUET_EXPORT ExternalDBPADecryptorAdapter : public DecryptorInterface {
 public:
  static std::unique_ptr<ExternalDBPADecryptorAdapter> Make(
      ParquetCipher::type algorithm, std::string column_name, std::string key_id,
      Type::type data_type, Compression::type compression_type, std::string app_context,
      std::map<std::string, std::string> configuration_properties,
      std::optional<int> datatype_length,
      std::shared_ptr<const KeyValueMetadata> key_value_metadata);

  ~ExternalDBPADecryptorAdapter() = default;

  /// Signal whether the decryptor can calculate a valid plaintext or ciphertext
  /// length before performing decryption or not. If false, a proper sized buffer
  /// cannot be allocated before calling the Decrypt method, and Arrow must use this
  /// decryptor's DecryptWithManagedBuffer method instead of Decrypt.
  [[nodiscard]] bool CanCalculateLengths() const override { return false; }

  /// The size of the plaintext, for this cipher and the specified ciphertext length.
  [[nodiscard]] int32_t PlaintextLength(int32_t ciphertext_len) const override;

  /// The size of the ciphertext, for this cipher and the specified plaintext length.
  [[nodiscard]] int32_t CiphertextLength(int32_t plaintext_len) const override;

  /// Decrypt is not supported as we cannot calculate the plaintext length before
  /// decryption.
  int32_t Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                  ::arrow::util::span<const uint8_t> key,
                  ::arrow::util::span<const uint8_t> aad,
                  ::arrow::util::span<uint8_t> plaintext) override {
    std::stringstream ss;
    ss << "Decrypt is not supported in ExternalDBPADecryptorAdapter, ";
    ss << "use DecryptWithManagedBuffer instead";
    throw ParquetException(ss.str());
  }

  /// Decrypt the ciphertext and leave the results in the plaintext buffer.
  /// The buffer will be resized to the correct size during decryption. This method
  /// is used when the decryptor cannot calculate the plaintext length before
  /// decryption.
  int32_t DecryptWithManagedBuffer(::arrow::util::span<const uint8_t> ciphertext,
                                   ::arrow::ResizableBuffer* plaintext) override;

  void UpdateEncodingProperties(
      std::unique_ptr<EncodingProperties> encoding_properties) override;

 private:
  // agent_instance is assumed to be initialized at the time of construction.
  // No initialization nor checks to verify that it is initialized are performed.
  ExternalDBPADecryptorAdapter(
      ParquetCipher::type algorithm, std::string column_name, std::string key_id,
      Type::type data_type, Compression::type compression_type,
      std::optional<int> datatype_length, std::string app_context,
      std::map<std::string, std::string> configuration_properties,
      std::unique_ptr<DataBatchProtectionAgentInterface> agent_instance,
      std::shared_ptr<const KeyValueMetadata> key_value_metadata);

  int32_t InvokeExternalDecrypt(::arrow::util::span<const uint8_t> ciphertext,
                                ::arrow::ResizableBuffer* plaintext,
                                std::map<std::string, std::string> encoding_attrs);

  ParquetCipher::type algorithm_;
  std::string column_name_;
  std::string key_id_;
  Type::type data_type_;
  Compression::type compression_type_;
  std::optional<int> datatype_length_;
  std::string app_context_;
  std::map<std::string, std::string> configuration_properties_;

  std::unique_ptr<dbps::external::DataBatchProtectionAgentInterface> agent_instance_;

  std::unique_ptr<EncodingProperties> encoding_properties_;
  bool encoding_properties_updated_ = false;

  // Store the key value metadata from the column chunk metadata.
  std::shared_ptr<KeyValueMetadata> key_value_metadata_;
};

/// Factory for ExternalDBPADecryptorAdapter instances. No cache exists for decryptors.
class PARQUET_EXPORT ExternalDBPADecryptorAdapterFactory {
 public:
  std::unique_ptr<DecryptorInterface> GetDecryptor(
      ParquetCipher::type algorithm, const ColumnCryptoMetaData* crypto_metadata,
      const ColumnChunkMetaData* column_chunk_metadata,
      ExternalFileDecryptionProperties* external_file_decryption_properties);
};

}  // namespace parquet::encryption

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

#include <string_view>

#include "arrow/util/logging.h"
#include "arrow/util/secure_string.h"
#include "arrow/util/string.h"

#include "parquet/encryption/crypto_factory.h"
#include "parquet/encryption/encryption_utils.h"
#include "parquet/encryption/file_key_unwrapper.h"
#include "parquet/encryption/file_system_key_material_store.h"
#include "parquet/encryption/key_toolkit_internal.h"

using arrow::util::SecureString;

namespace parquet::encryption {

/// Extracting functionality common to both GetFileEncryptionProperties and
/// GetExternalFileEncryptionProperties here for reuse.
namespace {

// Struct to simplify the returned objects in GetFileKeyUtils.
struct FileKeyUtils {
  std::shared_ptr<FileKeyMaterialStore> key_material_store;
  FileKeyWrapper key_wrapper;
};

FileKeyUtils GetFileKeyUtils(
    const std::shared_ptr<KeyToolkit>& key_toolkit,
    const KmsConnectionConfig& kms_connection_config,
    const EncryptionConfiguration& encryption_config, const std::string& file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system) {
  std::shared_ptr<FileKeyMaterialStore> key_material_store = nullptr;
  if (!encryption_config.internal_key_material) {
    try {
      key_material_store =
          FileSystemKeyMaterialStore::Make(file_path, file_system, false);
    } catch (ParquetException& e) {
      std::stringstream ss;
      ss << "Failed to get key material store.\n" << e.what() << "\n";
      throw ParquetException(ss.str());
    }
  }

  FileKeyWrapper key_wrapper(key_toolkit.get(), kms_connection_config, key_material_store,
                             encryption_config.cache_lifetime_seconds,
                             encryption_config.double_wrapping);
  return {key_material_store, std::move(key_wrapper)};
}

int ValidateAndGetKeyLength(int32_t dek_length_bits) {
  if (!internal::ValidateKeyLength(dek_length_bits)) {
    std::ostringstream ss;
    ss << "Wrong data key length : " << dek_length_bits;
    throw ParquetException(ss.str());
  }
  return dek_length_bits / 8;
}

std::map<ParquetCipher::type, std::map<std::string, std::string>>
ConvertConfigurationProperties(
    const std::unordered_map<ParquetCipher::type,
                             std::unordered_map<std::string, std::string>>&
        configuration_properties) {
  std::map<ParquetCipher::type, std::map<std::string, std::string>> converted_config;

  for (const auto& [cipher_type, inner_config] : configuration_properties) {
    if (!IsParquetCipherSupported(cipher_type)) {
      throw ParquetException("Invalid ParquetCipher type: " +
                             std::to_string(static_cast<int>(cipher_type)));
    }

    std::map<std::string, std::string> converted_inner;
    for (const auto& [key, value] : inner_config) {
      if (key.empty()) {
        throw ParquetException("Empty key in configuration properties");
      }
      if (value.empty()) {
        throw ParquetException("Empty value for key '" + key +
                               "' in configuration properties");
      }
      converted_inner[key] = value;
    }
    converted_config[cipher_type] = converted_inner;
  }

  return converted_config;
}

}  // Anonymous namespace

void CryptoFactory::RegisterKmsClientFactory(
    std::shared_ptr<KmsClientFactory> kms_client_factory) {
  key_toolkit_->RegisterKmsClientFactory(std::move(kms_client_factory));
}

std::shared_ptr<FileEncryptionProperties> CryptoFactory::GetFileEncryptionProperties(
    const KmsConnectionConfig& kms_connection_config,
    const EncryptionConfiguration& encryption_config, const std::string& file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system) {
  if (encryption_config.encryption_algorithm == ParquetCipher::EXTERNAL_DBPA_V1) {
    throw ParquetException(
        "EXTERNAL_DBPA_V1 algorithm is not supported for file level encryption");
  }
  if (!encryption_config.uniform_encryption && encryption_config.column_keys.empty()) {
    throw ParquetException("Either column_keys or uniform_encryption must be set");
  } else if (encryption_config.uniform_encryption &&
             !encryption_config.column_keys.empty()) {
    throw ParquetException("Cannot set both column_keys and uniform_encryption");
  }
  const std::string& footer_key_id = encryption_config.footer_key;
  const std::string& column_key_str = encryption_config.column_keys;

  auto [key_material_store, key_wrapper] = GetFileKeyUtils(
      key_toolkit_, kms_connection_config, encryption_config, file_path, file_system);
  int dek_length = ValidateAndGetKeyLength(encryption_config.data_key_length_bits);

  SecureString footer_key(dek_length, '\0');
  RandBytes(footer_key.as_span().data(), footer_key.size());

  std::string footer_key_metadata =
      key_wrapper.GetEncryptionKeyMetadata(footer_key, footer_key_id, true);

  FileEncryptionProperties::Builder properties_builder =
      FileEncryptionProperties::Builder(footer_key);
  properties_builder.footer_key_metadata(std::move(footer_key_metadata));
  properties_builder.algorithm(encryption_config.encryption_algorithm);

  if (!encryption_config.uniform_encryption) {
    ColumnPathToEncryptionPropertiesMap encrypted_columns =
        GetColumnEncryptionProperties(dek_length, column_key_str, &key_wrapper);
    properties_builder.encrypted_columns(std::move(encrypted_columns));

    if (encryption_config.plaintext_footer) {
      properties_builder.set_plaintext_footer();
    }
  }

  if (key_material_store != nullptr) {
    key_material_store->SaveMaterial();
  }
  return properties_builder.build();
}

std::shared_ptr<ExternalFileEncryptionProperties>
CryptoFactory::GetExternalFileEncryptionProperties(
    const KmsConnectionConfig& kms_connection_config,
    const ExternalEncryptionConfiguration& external_encryption_config,
    const std::string& file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system) {
  // Validate the same rules as FileEncryptionProperties but considering
  // per_column_encryption too. If uniform_encryption is not set then either
  // column_keys or per_column_encryption must have values. If uniform_encryption
  // is set, then both column_keys and per_column_encryption must be empty.
  if (external_encryption_config.encryption_algorithm ==
      ParquetCipher::EXTERNAL_DBPA_V1) {
    throw ParquetException(
        "EXTERNAL_DBPA_V1 algorithm is not supported for file level encryption");
  }
  bool no_columns_encrypted = external_encryption_config.column_keys.empty() &&
                              external_encryption_config.per_column_encryption.empty();
  if (!external_encryption_config.uniform_encryption && no_columns_encrypted) {
    throw ParquetException(
        "Either uniform_encryption must be set or column encryption must be specified"
        " in either column_keys or per_column_encryption");
  } else if (external_encryption_config.uniform_encryption && !no_columns_encrypted) {
    throw ParquetException("Cannot set both column encryption and uniform_encryption");
  }

  auto [key_material_store, key_wrapper] =
      GetFileKeyUtils(key_toolkit_, kms_connection_config, external_encryption_config,
                      file_path, file_system);

  int dek_length =
      ValidateAndGetKeyLength(external_encryption_config.data_key_length_bits);

  SecureString footer_key(dek_length, '\0');
  RandBytes(reinterpret_cast<uint8_t*>(footer_key.as_span().data()), footer_key.size());

  std::string footer_key_metadata = key_wrapper.GetEncryptionKeyMetadata(
      footer_key, external_encryption_config.footer_key, true);

  ExternalFileEncryptionProperties::Builder external_properties_builder =
      ExternalFileEncryptionProperties::Builder(footer_key);
  external_properties_builder.footer_key_metadata(footer_key_metadata);
  external_properties_builder.algorithm(external_encryption_config.encryption_algorithm);

  if (!external_encryption_config.uniform_encryption &&
      external_encryption_config.plaintext_footer) {
    external_properties_builder.set_plaintext_footer();
  }

  ColumnPathToEncryptionPropertiesMap encrypted_columns;
  if (!external_encryption_config.column_keys.empty()) {
    encrypted_columns = GetColumnEncryptionProperties(
        dek_length, external_encryption_config.column_keys, &key_wrapper);
  }
  if (!external_encryption_config.per_column_encryption.empty()) {
    for (const auto& pair : external_encryption_config.per_column_encryption) {
      const std::string& column_name = pair.first;
      const ColumnEncryptionAttributes& attributes = pair.second;

      // Validate column names are not in both column_keys and per_column_encryption maps.
      if (encrypted_columns.find(column_name) != encrypted_columns.end()) {
        std::stringstream string_stream;
        string_stream << "Multiple keys defined for column [" << column_name << "]. ";
        string_stream << "Keys found in column_keys and in per_column_encryption.";
        throw ParquetException(string_stream.str());
      }

      SecureString column_key(dek_length, '\0');
      RandBytes(reinterpret_cast<uint8_t*>(column_key.as_span().data()),
                column_key.size());
      std::string column_key_metadata =
          key_wrapper.GetEncryptionKeyMetadata(column_key, attributes.key_id, false);

      std::shared_ptr<ColumnEncryptionProperties> column_properties =
          ColumnEncryptionProperties::Builder(column_name)
              .key(column_key)
              ->key_metadata(column_key_metadata)
              ->parquet_cipher(attributes.parquet_cipher)
              ->build();

      encrypted_columns.insert({column_name, column_properties});
    }
  }
  if (!encrypted_columns.empty()) {
    external_properties_builder.encrypted_columns(encrypted_columns);
  }

  if (!external_encryption_config.app_context.empty()) {
    external_properties_builder.app_context(external_encryption_config.app_context);
  }

  if (!external_encryption_config.configuration_properties.empty()) {
    external_properties_builder.configuration_properties(ConvertConfigurationProperties(
        external_encryption_config.configuration_properties));
  }

  if (key_material_store != nullptr) {
    key_material_store->SaveMaterial();
  }

  return external_properties_builder.build_external();
}

ColumnPathToEncryptionPropertiesMap CryptoFactory::GetColumnEncryptionProperties(
    int dek_length, const std::string& column_keys, FileKeyWrapper* key_wrapper) {
  ColumnPathToEncryptionPropertiesMap encrypted_columns;

  std::vector<::std::string_view> key_to_columns =
      ::arrow::internal::SplitString(column_keys, ';');
  for (size_t i = 0; i < key_to_columns.size(); ++i) {
    std::string cur_key_to_columns =
        ::arrow::internal::TrimString(std::string(key_to_columns[i]));
    if (cur_key_to_columns.empty()) {
      continue;
    }

    std::vector<::std::string_view> parts =
        ::arrow::internal::SplitString(cur_key_to_columns, ':');
    if (parts.size() != 2) {
      std::ostringstream message;
      message << "Incorrect key to columns mapping in column keys property"
              << ": [" << cur_key_to_columns << "]";
      throw ParquetException(message.str());
    }

    std::string column_key_id = ::arrow::internal::TrimString(std::string(parts[0]));
    if (column_key_id.empty()) {
      throw ParquetException("Empty key name in column keys property.");
    }

    std::string column_names_str = ::arrow::internal::TrimString(std::string(parts[1]));
    std::vector<::std::string_view> column_names =
        ::arrow::internal::SplitString(column_names_str, ',');
    if (0 == column_names.size()) {
      throw ParquetException("No columns to encrypt defined for key: " + column_key_id);
    }

    for (size_t j = 0; j < column_names.size(); ++j) {
      std::string column_name =
          ::arrow::internal::TrimString(std::string(column_names[j]));
      if (column_name.empty()) {
        std::ostringstream message;
        message << "Empty column name in column keys property for key: " << column_key_id;
        throw ParquetException(message.str());
      }

      if (encrypted_columns.find(column_name) != encrypted_columns.end()) {
        throw ParquetException("Multiple keys defined for the same column: " +
                               column_name);
      }

      SecureString column_key(dek_length, '\0');
      RandBytes(column_key.as_span().data(), column_key.size());

      std::string column_key_key_metadata =
          key_wrapper->GetEncryptionKeyMetadata(column_key, column_key_id, false);

      std::shared_ptr<ColumnEncryptionProperties> cmd =
          ColumnEncryptionProperties::Builder(column_name)
              .key(column_key)
              ->key_metadata(column_key_key_metadata)
              ->build();
      encrypted_columns.insert({column_name, cmd});
    }
  }
  if (encrypted_columns.empty()) {
    throw ParquetException("No column keys configured in column keys property.");
  }

  return encrypted_columns;
}

std::shared_ptr<FileDecryptionProperties> CryptoFactory::GetFileDecryptionProperties(
    const KmsConnectionConfig& kms_connection_config,
    const DecryptionConfiguration& decryption_config, const std::string& file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system) {
  auto key_retriever = std::make_shared<FileKeyUnwrapper>(
      key_toolkit_, kms_connection_config, decryption_config.cache_lifetime_seconds,
      file_path, file_system);

  return FileDecryptionProperties::Builder()
      .key_retriever(std::move(key_retriever))
      ->plaintext_files_allowed()
      ->build();
}

std::shared_ptr<ExternalFileDecryptionProperties>
CryptoFactory::GetExternalFileDecryptionProperties(
    const KmsConnectionConfig& kms_connection_config,
    const ExternalDecryptionConfiguration& external_decryption_config,
    const std::string& file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system) {
  // Use the same FileKeyUnwrapper as in the FileDecryptionProperties.
  auto key_retriever = std::make_shared<FileKeyUnwrapper>(
      key_toolkit_, kms_connection_config,
      external_decryption_config.cache_lifetime_seconds, file_path, file_system);

  ExternalFileDecryptionProperties::Builder builder;
  builder.key_retriever(key_retriever);
  builder.plaintext_files_allowed();

  if (!external_decryption_config.app_context.empty()) {
    builder.app_context(external_decryption_config.app_context);
  }

  if (!external_decryption_config.configuration_properties.empty()) {
    builder.configuration_properties(ConvertConfigurationProperties(
        external_decryption_config.configuration_properties));
  }

  return builder.build_external();
}

void CryptoFactory::RotateMasterKeys(
    const KmsConnectionConfig& kms_connection_config,
    const std::string& parquet_file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system, bool double_wrapping,
    double cache_lifetime_seconds) {
  key_toolkit_->RotateMasterKeys(kms_connection_config, parquet_file_path, file_system,
                                 double_wrapping, cache_lifetime_seconds);
}

}  // namespace parquet::encryption

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

#include <iostream>
#include <map>
#include <limits>
#include <unordered_map>
#include <optional>
#include <memory>
#include <vector>

#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "parquet/encryption/external_dbpa_encryption.h"
#include "parquet/encryption/key_metadata.h"
#include "parquet/encryption/encryption_utils.h"
#include "parquet/encryption/external/loadable_encryptor_utils.h"
#include "parquet/encryption/external/dbpa_enum_utils.h"
#include "parquet/encryption/external/dbpa_executor.h"
#include "parquet/encryption/external_dbpa_encryption_utils.h"
#include "parquet/exception.h"
#include "parquet/types.h"

using parquet::encryption::external::LoadableEncryptorUtils;
using parquet::encryption::external::DBPAEnumUtils;
using parquet::encryption::external::DBPAExecutor;

using dbps::external::EncryptionResult;
using dbps::external::DecryptionResult;

namespace parquet::encryption {

namespace {
// Utility function to load and initialize a DataBatchProtectionAgentInterface instance
// Shared between the encryptor and decryptor.
std::unique_ptr<dbps::external::DataBatchProtectionAgentInterface> LoadAndInitializeAgent(
    const std::string& column_name,
    const std::map<std::string, std::string>& configuration_properties,
    const std::string& app_context,
    const std::string& key_id,
    Type::type data_type,
    Compression::type compression_type,
    std::optional<int> datatype_length,
    std::shared_ptr<const KeyValueMetadata> key_value_metadata) {

  // Load a new DataBatchProtectionAgentInterface instance from the shared library
  const std::string SHARED_LIBRARY_PATH_KEY = "agent_library_path";
  const std::string INIT_TIMEOUT_KEY = "agent_init_timeout_ms";
  const std::string ENCRYPT_TIMEOUT_KEY = "agent_encrypt_timeout_ms";
  const std::string DECRYPT_TIMEOUT_KEY = "agent_decrypt_timeout_ms";

  // Step 1: Get path to the shared library  
  auto it = configuration_properties.find(SHARED_LIBRARY_PATH_KEY);
  if (it == configuration_properties.end()) {
    auto const msg = "Required configuration key '" + SHARED_LIBRARY_PATH_KEY + "' not found in configuration_properties";
    ARROW_LOG(ERROR) << msg;
    throw ParquetException(msg);
  }
  auto library_path = it->second;
  ARROW_LOG(DEBUG) << "Loading agent from library: library_path = " << library_path;

  // Step 2: Load an instance of the DataBatchProtectionAgentInterface
  auto agent_instance = LoadableEncryptorUtils::LoadFromLibrary(library_path);
  if (!agent_instance) {
    ARROW_LOG(ERROR) << "Failed to create instance of DataBatchProtectionAgentInterface";
    throw ParquetException("Failed to create instance of DataBatchProtectionAgentInterface");
  }        

  //Step 3: Wrap the agent in a DBPAExecutor.
  //operations will timeout, exceptions will be re-thrown.

  ARROW_LOG(DEBUG) << "Wrapping Agent in DBPAExecutor";

  // Assign default values to the timeouts.
  int64_t init_timeout_ms    = 10*1000; //10 seconds
  int64_t encrypt_timeout_ms = 30*1000; //30 seconds
  int64_t decrypt_timeout_ms = 30*1000; //30 seconds.
  // Override the default values if they are present in the configuration_properties.
  try {
    if (configuration_properties.find(INIT_TIMEOUT_KEY) != configuration_properties.end()) {
      init_timeout_ms = std::stoi(configuration_properties.at(INIT_TIMEOUT_KEY));
    }
    if (configuration_properties.find(ENCRYPT_TIMEOUT_KEY) != configuration_properties.end()) {
      encrypt_timeout_ms = std::stoi(configuration_properties.at(ENCRYPT_TIMEOUT_KEY));
    }
    if (configuration_properties.find(DECRYPT_TIMEOUT_KEY) != configuration_properties.end()) {
        decrypt_timeout_ms = std::stoi(configuration_properties.at(DECRYPT_TIMEOUT_KEY));
    }
  } catch (const std::exception& e) {
    ARROW_LOG(ERROR) << "Failed to parse timeout values from configuration_properties: " << e.what();
    throw ParquetException("Failed to parse timeout values from configuration_properties");
  }

  ARROW_LOG(DEBUG) << "init_timeout_ms    = " << init_timeout_ms;
  ARROW_LOG(DEBUG) << "encrypt_timeout_ms = " << encrypt_timeout_ms;
  ARROW_LOG(DEBUG) << "decrypt_timeout_ms = " << decrypt_timeout_ms;

  auto executor_wrapped_agent = std::make_unique<DBPAExecutor>(
    /*agent*/ std::move(agent_instance), 
    /*init_timeout_ms*/ init_timeout_ms, 
    /*encrypt_timeout_ms*/  encrypt_timeout_ms, 
    /*decrypt_timeout_ms*/ decrypt_timeout_ms
  );

  // Step 4: Initialize the agent.
  ARROW_LOG(DEBUG) << "Initializing agent instance";

  // Convert KeyValueMetadata (only provided by the decryptor) into a std::map<string,string>
  std::optional<std::map<std::string, std::string>> column_encryption_metadata =
      ExternalDBPAUtils::KeyValueMetadataToStringMap(key_value_metadata);

  executor_wrapped_agent->init(
    /*column_name*/ column_name,
    /*configuration_properties*/ configuration_properties,
    /*app_context*/ app_context,
    /*column_key_id*/ key_id,
    /*data_type*/ DBPAEnumUtils::ParquetTypeToDBPA(data_type), 
    /*datatype_length*/ datatype_length,
    /*compression_type*/ DBPAEnumUtils::ArrowCompressionToDBPA(compression_type),
    /*column_encryption_metadata*/ std::move(column_encryption_metadata)
  ); 

  ARROW_LOG(DEBUG) << "Successfully initialized agent instance";

  return executor_wrapped_agent;
} //LoadAndInitializeAgent()

// Local helper to map encoding properties' page_type to encryption module type
// Returns std::nullopt if page_type is unsupported
std::optional<int8_t> GetModuleTypeFromEncodingProperties(
  const EncodingProperties& encoding_properties) {
  auto page_type = encoding_properties.GetPageType();
  if (page_type == parquet::PageType::DICTIONARY_PAGE) {
    return encryption::kDictionaryPage;
  }
  if (page_type == parquet::PageType::DATA_PAGE || page_type == parquet::PageType::DATA_PAGE_V2) {
    return encryption::kDataPage;
  }
  return std::nullopt;
} //GetModuleTypeFromEncodingProperties()

} // namespace

// Update the encryptor-level metadata accumulator based on encoding attributes and
// EncryptionResult-provided metadata. If no metadata is available or page_type is
// unsupported/absent, function performs no-op.
void UpdateEncryptorMetadata(
  std::map<int8_t, std::map<std::string, std::string>>& metadata_by_module,
  const EncodingProperties& encoding_properties,
  const dbps::external::EncryptionResult& result) {
  try {
    auto module_type_opt = GetModuleTypeFromEncodingProperties(encoding_properties);
    if (!module_type_opt.has_value()) {
      return;
    }
    auto column_encryption_metadata_opt = result.encryption_metadata();
    if (!column_encryption_metadata_opt.has_value()) {
      return;
    }
    auto& module_metadata = metadata_by_module[module_type_opt.value()];
    for (const auto& kv : column_encryption_metadata_opt.value()) {
      module_metadata[kv.first] = kv.second;
    }
  } catch (const std::exception& e) {
    throw ParquetException("UpdateEncryptorMetadata failed: " + std::string(e.what()));
  }
} //UpdateEncryptorMetadata()

std::optional<std::map<std::string, std::string>> ExternalDBPAUtils::KeyValueMetadataToStringMap(
  const std::shared_ptr<const KeyValueMetadata>& key_value_metadata) {
if (key_value_metadata == nullptr) {
  return std::nullopt;
}
std::map<std::string, std::string> metadata_map;
const auto& keys = key_value_metadata->keys();
const auto& values = key_value_metadata->values();
const auto count = std::min(keys.size(), values.size());
for (size_t i = 0; i < count; ++i) {
  metadata_map.emplace(keys[i], values[i]);
}
if (metadata_map.empty()) {
  return std::nullopt;
}
return metadata_map;
}

//this is a private constructor, invoked from Make()
//at this point, the agent_instance is assumed to be initialized.
ExternalDBPAEncryptorAdapter::ExternalDBPAEncryptorAdapter(
  ParquetCipher::type algorithm, std::string column_name, std::string key_id,
  Type::type data_type, Compression::type compression_type, std::optional<int> datatype_length,
  std::string app_context, std::map<std::string, std::string> configuration_properties,
  std::unique_ptr<DataBatchProtectionAgentInterface> agent_instance)
  : algorithm_(algorithm), column_name_(column_name), key_id_(key_id),
    data_type_(data_type), compression_type_(compression_type),
    datatype_length_(datatype_length), app_context_(app_context),
    configuration_properties_(configuration_properties),
    agent_instance_(std::move(agent_instance)) {

      if (algorithm != ParquetCipher::EXTERNAL_DBPA_V1) {
        throw ParquetException("ExternalDBPAEncryptorAdapter -- Only algorithm ExternalDBPA_V1 is supported");
      }
}

std::unique_ptr<ExternalDBPAEncryptorAdapter> ExternalDBPAEncryptorAdapter::Make(
    ParquetCipher::type algorithm, std::string column_name, std::string key_id,
    Type::type data_type, Compression::type compression_type, std::string app_context,
    std::map<std::string, std::string> configuration_properties,
    std::optional<int> datatype_length) {

        // Ensure DBPA logging threshold is configured before any logs here
        EnsureDbpaLoggingConfigured();


        if (::arrow::util::ArrowLog::IsLevelEnabled(::arrow::util::ArrowLogLevel::ARROW_DEBUG)) {
          ARROW_LOG(DEBUG) << "ExternalDBPAEncryptorAdapter::Make() -- Make()";
          ARROW_LOG(DEBUG) << "  algorithm = " << algorithm;
          ARROW_LOG(DEBUG) << "  column_name = " << column_name;
          ARROW_LOG(DEBUG) << "  key_id = " << key_id;
          ARROW_LOG(DEBUG) << "  data_type = " << data_type;
          ARROW_LOG(DEBUG) << "  compression_type = " << compression_type;
          ARROW_LOG(DEBUG) << "  app_context = " << app_context;
          ARROW_LOG(DEBUG) << "  configuration_properties:";
          for (const auto& [key, value] : configuration_properties) {
            ARROW_LOG(DEBUG) << "    " << key << " = " << value;
          }
        }

        if (algorithm != ParquetCipher::EXTERNAL_DBPA_V1) {
          throw ParquetException("ExternalDBPAEncryptorAdapter::Make() -- Only algorithm ExternalDBPA_V1 is supported");
        }

        ARROW_LOG(DEBUG) << "ExternalDBPAEncryptorAdapter::ExternalDBPAEncryptorAdapter() -- loading and initializing agent";
        // Load and initialize the agent using the utility function
        auto agent_instance = LoadAndInitializeAgent(
          column_name, configuration_properties, app_context, key_id, data_type, compression_type, datatype_length,
          /*key_value_metadata*/ nullptr);

        //if we got to this point, the agent was initialized successfully
        ARROW_LOG(DEBUG) << "ExternalDBPAEncryptorAdapter::ExternalDBPAEncryptorAdapter() -- creating ExternalDBPAEncryptorAdapter";

        // create the instance of the ExternalDBPAEncryptorAdapter
        auto result = std::unique_ptr<ExternalDBPAEncryptorAdapter>(
          new ExternalDBPAEncryptorAdapter(
            /*algorithm*/ algorithm,
            /*column_name*/ column_name,
            /*key_id*/ key_id,
            /*data_type*/ data_type,
            /*compression_type*/ compression_type,
            /*datatype_length*/ datatype_length,
            /*app_context*/ app_context,
            /*configuration_properties*/ configuration_properties,
            /*agent_instance*/ std::move(agent_instance))
        );

        ARROW_LOG(DEBUG) << "ExternalDBPAEncryptorAdapter created successfully";

        return result;
  }

int32_t ExternalDBPAEncryptorAdapter::CiphertextLength(int64_t plaintext_len) const {
  throw ParquetException("ExternalDBPAEncryptorAdapter::CiphertextLength is not supported");
}

void ExternalDBPAEncryptorAdapter::UpdateEncodingProperties(std::unique_ptr<EncodingProperties> encoding_properties) {
  ARROW_LOG(DEBUG) << "ExternalDBPAEncryptorAdapter::UpdateEncodingProperties";

  //fill-in values from the decryptor constructor.
  encoding_properties->set_column_path(column_name_);
  encoding_properties->set_physical_type(data_type_, datatype_length_);
  encoding_properties->set_compression_codec(compression_type_);

  encoding_properties->validate();
  encoding_properties_ = std::move(encoding_properties);
  encoding_properties_updated_ = true;
}

std::shared_ptr<KeyValueMetadata> ExternalDBPAEncryptorAdapter::GetKeyValueMetadata(
  int8_t module_type) {
  auto it = column_encryption_metadata_.find(module_type);
  if (it == column_encryption_metadata_.end() || it->second.empty()) {
    return nullptr;
  }

  const auto& metadata_map = it->second;
  std::unordered_map<std::string, std::string> unordered_map(metadata_map.begin(), metadata_map.end());
  return ::arrow::key_value_metadata(unordered_map);
} //GetKeyValueMetadata()

int32_t ExternalDBPAEncryptorAdapter::EncryptWithManagedBuffer(
    ::arrow::util::span<const uint8_t> plaintext, ::arrow::ResizableBuffer* ciphertext) {

  if (!encoding_properties_updated_) {
    ARROW_LOG(ERROR) << "ExternalDBPAEncryptorAdapter:: EncryptionParams not updated";
    throw ParquetException("ExternalDBPAEncryptorAdapter:: EncryptionParams not updated");
  }

  encoding_properties_updated_ = false;

  return InvokeExternalEncrypt(plaintext, ciphertext, encoding_properties_->ToPropertiesMap());
}

int32_t ExternalDBPAEncryptorAdapter::SignedFooterEncrypt(
    ::arrow::util::span<const uint8_t> footer, ::arrow::util::span<const uint8_t> key,
    ::arrow::util::span<const uint8_t> aad, ::arrow::util::span<const uint8_t> nonce,
    ::arrow::util::span<uint8_t> encrypted_footer) {
      throw ParquetException("ExternalDBPAEncryptorAdapter::SignedFooterEncrypt is not supported");
}

int32_t ExternalDBPAEncryptorAdapter::InvokeExternalEncrypt(
    ::arrow::util::span<const uint8_t> plaintext, 
    ::arrow::ResizableBuffer* ciphertext,
    std::map<std::string, std::string> encoding_attrs) {

      if (::arrow::util::ArrowLog::IsLevelEnabled(::arrow::util::ArrowLogLevel::ARROW_DEBUG)) {
        ARROW_LOG(DEBUG) << "*-*-*- START: ExternalDBPAEncryptor::Encrypt *-*-*-";
        ARROW_LOG(DEBUG) << "Encryption Algorithm: [" << algorithm_ << "]";
        ARROW_LOG(DEBUG) << "Column Name: [" << column_name_ << "]";
        ARROW_LOG(DEBUG) << "Key ID: [" << key_id_ << "]";
        ARROW_LOG(DEBUG) << "Data Type: [" << data_type_ << "]";
        ARROW_LOG(DEBUG) << "Compression Type: [" << compression_type_ << "]";
        ARROW_LOG(DEBUG) << "App Context: [" << app_context_ << "]";
        ARROW_LOG(DEBUG) << "Configuration Properties:";
        for (const auto& [cfg_key, cfg_value] : configuration_properties_) {
          ARROW_LOG(DEBUG) << "  [" << cfg_key << "]: [" << cfg_value << "]";
        }
      }

      ARROW_LOG(DEBUG) << "Calling agent_instance_->Encrypt...";
      std::unique_ptr<EncryptionResult> result = agent_instance_->Encrypt(plaintext, std::move(encoding_attrs));

      if (!result->success()) {
        ARROW_LOG(ERROR) << "Encryption failed: " << result->error_message();
        throw ParquetException(result->error_message());
      }

      ARROW_LOG(DEBUG) << "Encryption successful";
      ARROW_LOG(DEBUG) << "  result size: " << result->size() << " bytes";
      ARROW_LOG(DEBUG) << "  result ciphertext size: " << result->ciphertext().size() << " bytes";

      const auto ciphertext_size64 = result->ciphertext().size();
      if (ciphertext_size64 > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ParquetException("Ciphertext size exceeds int32_t max");
      }
      const int32_t ciphertext_size = static_cast<int32_t>(ciphertext_size64);
      auto status = ciphertext->Resize(ciphertext_size, false);
      if (!status.ok()) {
        ARROW_LOG(ERROR) << "Ciphertext buffer resize failed: " << status.ToString();
        throw ParquetException("Ciphertext buffer resize failed");
      }

      ARROW_LOG(DEBUG) << "Copying result to ciphertext buffer...";
      if (ciphertext_size > 0) {
        std::memcpy(ciphertext->mutable_data(), result->ciphertext().data(), ciphertext_size);
      }
      ARROW_LOG(DEBUG) << "Encryption completed successfully";

      // Accumulate any column_encryption_metadata returned by the result per module type
      UpdateEncryptorMetadata(
        /*metadata_by_module*/ column_encryption_metadata_,
        /*encoding_properties*/ *encoding_properties_,
        /*result*/ *result);

      return static_cast<int32_t>(result->size());
  }

ExternalDBPAEncryptorAdapter* ExternalDBPAEncryptorAdapterFactory::GetEncryptor(
    ParquetCipher::type algorithm, const ColumnChunkMetaDataBuilder* column_chunk_metadata,
    ExternalFileEncryptionProperties* external_file_encryption_properties) {
  if (column_chunk_metadata == nullptr) {
    throw ParquetException("External DBPA encryption requires column chunk metadata");
  }
  auto column_path = column_chunk_metadata->descr()->path();
  if (encryptor_cache_.find(column_path->ToDotString()) == encryptor_cache_.end()) {
    auto configuration_properties = external_file_encryption_properties->configuration_properties();
    if (configuration_properties.find(algorithm) == configuration_properties.end()) {
      throw ParquetException("External DBPA encryption requires its configuration properties");
    }

    auto column_encryption_properties = external_file_encryption_properties
        ->column_encryption_properties(column_path->ToDotString());
    if (column_encryption_properties == nullptr) {
      std::stringstream ss;
      ss << "External DBPA encryption requires column encryption properties for column ["
         << column_path->ToDotString() << "]";
      throw ParquetException(ss.str());
    }

    auto data_type = column_chunk_metadata->descr()->physical_type();
    std::optional<int> datatype_length;
    if (data_type == Type::FIXED_LEN_BYTE_ARRAY) {
      datatype_length = column_chunk_metadata->descr()->type_length();
    }
    auto compression_type = column_chunk_metadata->properties()->compression(column_path);
    auto app_context = external_file_encryption_properties->app_context();
    auto connection_config_for_algorithm = configuration_properties.at(algorithm);

    std::string key_id;
    try {
      auto key_metadata = KeyMetadata::Parse(column_encryption_properties->key_metadata());
      key_id = key_metadata.key_material().master_key_id();
    } catch (const ParquetException& e) {
      // It is possible for the key metadata to only contain the key id itself, so if
      // it cannot be parsed as valid JSON, send the key id as string for the ExternalDBPA
      // to process.
      key_id = column_encryption_properties->key_metadata();
    }

    encryptor_cache_[column_path->ToDotString()] = ExternalDBPAEncryptorAdapter::Make(
        algorithm, column_path->ToDotString(), key_id, data_type, compression_type,
        app_context, connection_config_for_algorithm, datatype_length);
  }

  return encryptor_cache_[column_path->ToDotString()].get();
}

//private constructor, invoked from Make()
//at this point, the agent_instance is assumed to be initialized.
//TODO: consider cleaning up the signature of this private constructor. 
//      Most of the arguments are only needed by agent_instance, which is 
//      instantiated before this constructor is invoked.
ExternalDBPADecryptorAdapter::ExternalDBPADecryptorAdapter(
  ParquetCipher::type algorithm, std::string column_name, std::string key_id,
  Type::type data_type, Compression::type compression_type,
  std::optional<int> datatype_length, std::string app_context,
  std::map<std::string, std::string> configuration_properties,
  std::unique_ptr<DataBatchProtectionAgentInterface> agent_instance,
  std::shared_ptr<const KeyValueMetadata> key_value_metadata)
  : algorithm_(algorithm), column_name_(column_name), key_id_(key_id),
    data_type_(data_type), compression_type_(compression_type),
    datatype_length_(datatype_length), app_context_(app_context),
    configuration_properties_(configuration_properties),
    agent_instance_(std::move(agent_instance)) {

    if (algorithm != ParquetCipher::EXTERNAL_DBPA_V1) {
      throw ParquetException("ExternalDBPADecryptorAdapter -- Only algorithm ExternalDBPA_V1 is supported");
    }
    if (key_value_metadata != nullptr) {
      key_value_metadata_ = key_value_metadata->Copy();
    }
}

std::unique_ptr<ExternalDBPADecryptorAdapter> ExternalDBPADecryptorAdapter::Make(
    ParquetCipher::type algorithm, std::string column_name, std::string key_id,
    Type::type data_type, Compression::type compression_type,
    std::string app_context, std::map<std::string, std::string> configuration_properties,
    std::optional<int> datatype_length,
    std::shared_ptr<const KeyValueMetadata> key_value_metadata) {

        if (algorithm != ParquetCipher::EXTERNAL_DBPA_V1) {
          throw ParquetException("ExternalDBPADecryptorAdapter::Make() -- Only algorithm ExternalDBPA_V1 is supported");
        }

        // Ensure DBPA logging threshold is configured before any logs here
        EnsureDbpaLoggingConfigured();

        ARROW_LOG(DEBUG) << "ExternalDBPADecryptorAdapter::Make() -- Make()";
        ARROW_LOG(DEBUG) << "  algorithm = " << algorithm;
        ARROW_LOG(DEBUG) << "  column_name = " << column_name;
        ARROW_LOG(DEBUG) << "  key_id = " << key_id;
        ARROW_LOG(DEBUG) << "  data_type = " << data_type;
        ARROW_LOG(DEBUG) << "  compression_type = " << compression_type;
        ARROW_LOG(DEBUG) << "  app_context = " << app_context;
        ARROW_LOG(DEBUG) << "  configuration_properties:";
        for (const auto& [key, value] : configuration_properties) {
          ARROW_LOG(DEBUG) << "    " << key << " = " << value;
        }
        ARROW_LOG(DEBUG) << "  key_value_metadata:";
        if (key_value_metadata == nullptr) {
          ARROW_LOG(DEBUG) << "  key_value_metadata: not provided";
        } else {
          ARROW_LOG(DEBUG) << "  key_value_metadata: " << key_value_metadata->ToString();
        }

        ARROW_LOG(DEBUG) << "ExternalDBPADecryptorAdapter::ExternalDBPADecryptorAdapter() -- loading and initializing agent";
        // Load and initialize the agent using the utility function
        auto agent_instance = LoadAndInitializeAgent(
            column_name, configuration_properties, app_context, key_id, data_type, compression_type, datatype_length,
            /*key_value_metadata*/ key_value_metadata);

        //if we got to this point, the agent was initialized successfully

        // create the instance of the ExternalDBPADecryptorAdapter
        auto result = std::unique_ptr<ExternalDBPADecryptorAdapter>(
          new ExternalDBPADecryptorAdapter(
            /*algorithm*/ algorithm,
            /*column_name*/ column_name,
            /*key_id*/ key_id,
            /*data_type*/ data_type,
            /*compression_type*/ compression_type,
            /*datatype_length*/ datatype_length,
            /*app_context*/ app_context,
            /*configuration_properties*/ configuration_properties,
            /*agent_instance*/ std::move(agent_instance),
            /*key_value_metadata*/ key_value_metadata)
        );
        ARROW_LOG(DEBUG) << "ExternalDBPADecryptorAdapter created successfully";

        return result;
  }

int32_t ExternalDBPADecryptorAdapter::PlaintextLength(int32_t ciphertext_len) const {
  throw ParquetException("ExternalDBPADecryptorAdapter::PlaintextLength is not supported");
}

int32_t ExternalDBPADecryptorAdapter::CiphertextLength(int32_t plaintext_len) const {
  throw ParquetException("ExternalDBPADecryptorAdapter::CiphertextLength is not supported");
}

void ExternalDBPADecryptorAdapter::UpdateEncodingProperties(std::unique_ptr<EncodingProperties> encoding_properties) {
  ARROW_LOG(DEBUG) << "ExternalDBPADecryptorAdapter::UpdateEncodingProperties";

  //fill-in values from the decryptor constructor.
  encoding_properties->set_column_path(column_name_);
  encoding_properties->set_physical_type(data_type_, datatype_length_);
  encoding_properties->set_compression_codec(compression_type_);

  encoding_properties->validate();
  encoding_properties_ = std::move(encoding_properties);
  encoding_properties_updated_ = true;
}

int32_t ExternalDBPADecryptorAdapter::DecryptWithManagedBuffer(
    ::arrow::util::span<const uint8_t> ciphertext, ::arrow::ResizableBuffer* plaintext) {

      if (!encoding_properties_updated_) {
        ARROW_LOG(ERROR) << "ExternalDBPADecryptorAdapter:: DecryptionParams not updated";
        throw ParquetException("ExternalDBPADecryptorAdapter:: DecryptionParams not updated");
      }

      encoding_properties_updated_ = false;

      return InvokeExternalDecrypt(ciphertext, plaintext, encoding_properties_->ToPropertiesMap());
}

int32_t ExternalDBPADecryptorAdapter::InvokeExternalDecrypt(
    ::arrow::util::span<const uint8_t> ciphertext, 
    ::arrow::ResizableBuffer* plaintext,
    std::map<std::string, std::string> encoding_attrs) {

      if (::arrow::util::ArrowLog::IsLevelEnabled(::arrow::util::ArrowLogLevel::ARROW_DEBUG)) {
        ARROW_LOG(DEBUG) << "*-*-*- START: ExternalDBPADecryptor::Decrypt *-*-*-";
        ARROW_LOG(DEBUG) << "Decryption Algorithm: [" << algorithm_ << "]";
        ARROW_LOG(DEBUG) << "Column Name: [" << column_name_ << "]";
        ARROW_LOG(DEBUG) << "Key ID: [" << key_id_ << "]";
        ARROW_LOG(DEBUG) << "Data Type: [" << data_type_ << "]";
        ARROW_LOG(DEBUG) << "Compression Type: [" << compression_type_ << "]";
        ARROW_LOG(DEBUG) << "App Context: [" << app_context_ << "]";
        ARROW_LOG(DEBUG) << "Configuration Properties:";
        for (const auto& [key, value] : configuration_properties_) {
          ARROW_LOG(DEBUG) << "  [" << key << "]: [" << value << "]";
        }
      }

      ARROW_LOG(DEBUG) << "Calling agent_instance_->Decrypt...";
      std::unique_ptr<DecryptionResult> result = agent_instance_->Decrypt(ciphertext, std::move(encoding_attrs));

      if (!result->success()) {
        ARROW_LOG(ERROR) << "Decryption failed: " << result->error_message();
        throw ParquetException(result->error_message());
      }

      ARROW_LOG(DEBUG) << "Decryption successful";
      ARROW_LOG(DEBUG) << "  result size: " << result->size() << " bytes";
      ARROW_LOG(DEBUG) << "  result plaintext size: " << result->plaintext().size() << " bytes";

      const auto plaintext_size64 = result->plaintext().size();
      if (plaintext_size64 > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ParquetException("Plaintext size exceeds int32_t max");
      }
      const int32_t plaintext_size = static_cast<int32_t>(plaintext_size64);
      auto status = plaintext->Resize(plaintext_size, false);
      if (!status.ok()) {
        ARROW_LOG(ERROR) << "Plaintext buffer resize failed: " << status.ToString();
        throw ParquetException("Plaintext buffer resize failed");
      }

      ARROW_LOG(DEBUG) << "Copying result to plaintext buffer...";
      if (plaintext_size > 0) {
        std::memcpy(plaintext->mutable_data(), result->plaintext().data(), plaintext_size);
      }
      ARROW_LOG(DEBUG) << "Decryption completed successfully";

      const auto total_size64 = result->size();
      if (total_size64 > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ParquetException("Result size exceeds int32_t max");
      }
      return static_cast<int32_t>(total_size64);
  }

std::unique_ptr<DecryptorInterface> ExternalDBPADecryptorAdapterFactory::GetDecryptor(
  ParquetCipher::type algorithm, const ColumnCryptoMetaData* crypto_metadata,
  const ColumnChunkMetaData* column_chunk_metadata,
  ExternalFileDecryptionProperties* external_file_decryption_properties) {
    if (column_chunk_metadata == nullptr || crypto_metadata == nullptr) {
      throw ParquetException("External DBPA decryption requires column chunk and crypto metadata");
    }
    auto configuration_properties = external_file_decryption_properties->configuration_properties();
    if (configuration_properties.find(algorithm) == configuration_properties.end()) {
      throw ParquetException("External DBPA decryption requires its configuration properties");
    }
    auto column_path = column_chunk_metadata->descr()->path();
    auto data_type = column_chunk_metadata->descr()->physical_type();
    std::optional<int> datatype_length;
    if (data_type == Type::FIXED_LEN_BYTE_ARRAY) {
      datatype_length = column_chunk_metadata->descr()->type_length();
    }
    auto compression_type = column_chunk_metadata->compression();
    auto app_context = external_file_decryption_properties->app_context();
    auto connection_config_for_algorithm = configuration_properties.at(algorithm);
    auto key_value_metadata = column_chunk_metadata->key_value_metadata();

    std::string key_id;
    try {
      auto key_metadata = KeyMetadata::Parse(crypto_metadata->key_metadata());
      key_id = key_metadata.key_material().master_key_id();
    } catch (const ParquetException& e) {
      // It is possible for the key metadata to only contain the key id itself, so if
      // it cannot be parsed as valid JSON, send the key id as string for the ExternalDBPA
      // to process.
      key_id = crypto_metadata->key_metadata();
    }

    return ExternalDBPADecryptorAdapter::Make(
        algorithm, column_path->ToDotString(), key_id, data_type, compression_type, app_context,
        connection_config_for_algorithm, datatype_length, key_value_metadata);
 }

}  // namespace parquet::encryption
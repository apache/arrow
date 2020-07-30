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

#include "arrow/io/hdfs.h"
#include "parquet/encryption.h"
#include "parquet/file_key_wrapper.h"
#include "parquet/kms_client_factory.h"

namespace parquet {

namespace encryption {

static constexpr ParquetCipher::type kDefaultEncryptionAlgorithm =
    ParquetCipher::AES_GCM_V1;
static constexpr bool kDefaultPlaintextFooter = false;
static constexpr bool kDefaultDoubleWrapping = true;
static constexpr bool kDefaultWrapLocally = false;
static constexpr uint64_t kDefaultCacheLifetimeSeconds = 600;  // 10 minutes
static constexpr bool kDefaultInternalKeyMaterial = true;
static constexpr bool kDefaultUniformEncryption = false;
static constexpr int32_t kDefaultDataKeyLengthBits = 128;

class PARQUET_EXPORT EncryptionConfiguration {
 public:
  class PARQUET_EXPORT Builder {
   public:
    explicit Builder(const std::string& footer_key)
        : footer_key_(footer_key),
          encryption_algorithm_(kDefaultEncryptionAlgorithm),
          plaintext_footer_(kDefaultPlaintextFooter),
          double_wrapping_(kDefaultDoubleWrapping),
          wrap_locally_(kDefaultWrapLocally),
          cache_lifetime_seconds_(kDefaultCacheLifetimeSeconds),
          internal_key_material_(kDefaultInternalKeyMaterial),
          uniform_encryption_(kDefaultUniformEncryption),
          data_key_length_bits_(kDefaultDataKeyLengthBits) {}

    // Either
    // ::column_keys(const std::string&)
    // or
    // ::uniform_encryption()
    // must be called. If none are called, or if both are called, an exception will be
    // thrown.
    Builder* column_keys(const std::string& column_keys);
    Builder* uniform_encryption();

    Builder* encryption_algorithm(ParquetCipher::type algo);
    Builder* plaintext_footer(bool plaintext_footer);
    Builder* double_wrapping(bool double_wrapping);
    Builder* wrap_locally(bool wrap_locally);
    Builder* cache_lifetime_seconds(uint64_t cache_lifetime_seconds);
    Builder* internal_key_material(bool internal_key_material);
    Builder* data_key_length_bits(int32_t data_key_length_bits);

    std::shared_ptr<EncryptionConfiguration> build();

   private:
    std::string footer_key_;
    std::string column_keys_;
    ParquetCipher::type encryption_algorithm_;
    bool plaintext_footer_;
    bool double_wrapping_;
    bool wrap_locally_;
    uint64_t cache_lifetime_seconds_;
    bool internal_key_material_;
    bool uniform_encryption_;
    int32_t data_key_length_bits_;
  };

  const std::string& footer_key() const { return footer_key_; }
  const std::string& column_keys() const { return column_keys_; }
  ParquetCipher::type encryption_algorithm() const { return encryption_algorithm_; }
  bool plaintext_footer() const { return plaintext_footer_; }
  bool double_wrapping() const { return double_wrapping_; }
  bool wrap_locally() const { return wrap_locally_; }
  uint64_t cache_lifetime_seconds() const { return cache_lifetime_seconds_; }
  bool internal_key_material() const { return internal_key_material_; }
  bool uniform_encryption() const { return uniform_encryption_; }
  int32_t data_key_length_bits() const { return data_key_length_bits_; }

  EncryptionConfiguration(const std::string& footer_key, const std::string& column_keys,
                          ParquetCipher::type encryption_algorithm, bool plaintext_footer,
                          bool double_wrapping, bool wrap_locally,
                          uint64_t cache_lifetime_seconds, bool internal_key_material,
                          bool uniform_encryption, int32_t data_key_length_bits)
      : footer_key_(footer_key),
        column_keys_(column_keys),
        encryption_algorithm_(encryption_algorithm),
        plaintext_footer_(plaintext_footer),
        double_wrapping_(double_wrapping),
        wrap_locally_(wrap_locally),
        cache_lifetime_seconds_(cache_lifetime_seconds),
        internal_key_material_(internal_key_material),
        uniform_encryption_(uniform_encryption),
        data_key_length_bits_(data_key_length_bits) {}

 private:
  std::string footer_key_;
  std::string column_keys_;
  ParquetCipher::type encryption_algorithm_;
  bool plaintext_footer_;
  bool double_wrapping_;
  bool wrap_locally_;
  uint64_t cache_lifetime_seconds_;
  bool internal_key_material_;
  bool uniform_encryption_;
  int32_t data_key_length_bits_;
};

class PARQUET_EXPORT PropertiesDrivenCryptoFactory {
 public:
  static constexpr char COLUMN_KEYS_PROPERTY_NAME[] = "parquet.encryption.column.keys";

  void kms_client_factory(std::shared_ptr<KmsClientFactory> kms_client_factory);

  std::shared_ptr<FileEncryptionProperties> GetFileEncryptionProperties(
      const KmsConnectionConfig& kms_connection_config,
      std::shared_ptr<EncryptionConfiguration> encryption_config,
      const arrow::io::HdfsConnectionConfig& hdfs_connection_config,
      const std::string& temp_file_path);

 private:
  static int32_t constexpr ACCEPTABLE_DATA_KEY_LENGTHS[] = {128, 192, 256};

  ColumnPathToEncryptionPropertiesMap GetColumnEncryptionProperties(
      int dek_length, const std::string column_keys, FileKeyWrapper& key_wrapper);

  std::shared_ptr<KmsClientFactory> kms_client_factory_;
};

}  // namespace encryption

}  // namespace parquet

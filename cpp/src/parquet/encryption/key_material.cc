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

#include "arrow/json/object_parser.h"
#include "arrow/json/object_writer.h"

#include "parquet/encryption/key_material.h"
#include "parquet/encryption/key_metadata.h"
#include "parquet/exception.h"

using ::arrow::json::internal::ObjectParser;
using ::arrow::json::internal::ObjectWriter;

namespace parquet {
namespace encryption {

constexpr const char KeyMaterial::kKeyMaterialTypeField[];
constexpr const char KeyMaterial::kKeyMaterialType1[];

constexpr const char KeyMaterial::kFooterKeyIdInFile[];
constexpr const char KeyMaterial::kColumnKeyIdInFilePrefix[];

constexpr const char KeyMaterial::kIsFooterKeyField[];
constexpr const char KeyMaterial::kDoubleWrappingField[];
constexpr const char KeyMaterial::kKmsInstanceIdField[];
constexpr const char KeyMaterial::kKmsInstanceUrlField[];
constexpr const char KeyMaterial::kMasterKeyIdField[];
constexpr const char KeyMaterial::kWrappedDataEncryptionKeyField[];
constexpr const char KeyMaterial::kKeyEncryptionKeyIdField[];
constexpr const char KeyMaterial::kWrappedKeyEncryptionKeyField[];

KeyMaterial::KeyMaterial(bool is_footer_key, const std::string& kms_instance_id,
                         const std::string& kms_instance_url,
                         const std::string& master_key_id, bool is_double_wrapped,
                         const std::string& kek_id,
                         const std::string& encoded_wrapped_kek,
                         const std::string& encoded_wrapped_dek)
    : is_footer_key_(is_footer_key),
      kms_instance_id_(kms_instance_id),
      kms_instance_url_(kms_instance_url),
      master_key_id_(master_key_id),
      is_double_wrapped_(is_double_wrapped),
      kek_id_(kek_id),
      encoded_wrapped_kek_(encoded_wrapped_kek),
      encoded_wrapped_dek_(encoded_wrapped_dek) {}

KeyMaterial KeyMaterial::Parse(const std::string& key_material_string) {
  ObjectParser json_parser;
  ::arrow::Status status = json_parser.Parse(key_material_string);
  if (!status.ok()) {
    throw ParquetException("Failed to parse key material " + key_material_string);
  }

  // External key material - extract "key material type", and make sure it is supported
  std::string key_material_type;
  PARQUET_ASSIGN_OR_THROW(key_material_type,
                          json_parser.GetString(kKeyMaterialTypeField));
  if (kKeyMaterialType1 != key_material_type) {
    throw ParquetException("Wrong key material type: " + key_material_type + " vs " +
                           kKeyMaterialType1);
  }
  // Parse other fields (common to internal and external key material)
  return Parse(&json_parser);
}

KeyMaterial KeyMaterial::Parse(const ObjectParser* key_material_json) {
  // 2. Check if "key material" belongs to file footer key
  bool is_footer_key;
  PARQUET_ASSIGN_OR_THROW(is_footer_key, key_material_json->GetBool(kIsFooterKeyField));
  std::string kms_instance_id;
  std::string kms_instance_url;
  if (is_footer_key) {
    // 3.  For footer key, extract KMS Instance ID
    PARQUET_ASSIGN_OR_THROW(kms_instance_id,
                            key_material_json->GetString(kKmsInstanceIdField));
    // 4.  For footer key, extract KMS Instance URL
    PARQUET_ASSIGN_OR_THROW(kms_instance_url,
                            key_material_json->GetString(kKmsInstanceUrlField));
  }
  // 5. Extract master key ID
  std::string master_key_id;
  PARQUET_ASSIGN_OR_THROW(master_key_id, key_material_json->GetString(kMasterKeyIdField));
  // 6. Extract wrapped DEK
  std::string encoded_wrapped_dek;
  PARQUET_ASSIGN_OR_THROW(encoded_wrapped_dek,
                          key_material_json->GetString(kWrappedDataEncryptionKeyField));
  std::string kek_id;
  std::string encoded_wrapped_kek;
  // 7. Check if "key material" was generated in double wrapping mode
  bool is_double_wrapped;
  PARQUET_ASSIGN_OR_THROW(is_double_wrapped,
                          key_material_json->GetBool(kDoubleWrappingField));
  if (is_double_wrapped) {
    // 8. In double wrapping mode, extract KEK ID
    PARQUET_ASSIGN_OR_THROW(kek_id,
                            key_material_json->GetString(kKeyEncryptionKeyIdField));
    // 9. In double wrapping mode, extract wrapped KEK
    PARQUET_ASSIGN_OR_THROW(encoded_wrapped_kek,
                            key_material_json->GetString(kWrappedKeyEncryptionKeyField));
  }

  return KeyMaterial(is_footer_key, kms_instance_id, kms_instance_url, master_key_id,
                     is_double_wrapped, kek_id, encoded_wrapped_kek, encoded_wrapped_dek);
}

std::string KeyMaterial::SerializeToJson(
    bool is_footer_key, const std::string& kms_instance_id,
    const std::string& kms_instance_url, const std::string& master_key_id,
    bool is_double_wrapped, const std::string& kek_id,
    const std::string& encoded_wrapped_kek, const std::string& encoded_wrapped_dek,
    bool is_internal_storage) {
  ObjectWriter json_writer;
  json_writer.SetString(kKeyMaterialTypeField, kKeyMaterialType1);

  if (is_internal_storage) {
    // 1. for internal storage, key material and key metadata are the same.
    // adding the "internalStorage" field that belongs to KeyMetadata.
    json_writer.SetBool(KeyMetadata::kKeyMaterialInternalStorageField, true);
  }
  // 2. Write isFooterKey
  json_writer.SetBool(kIsFooterKeyField, is_footer_key);
  if (is_footer_key) {
    // 3. For footer key, write KMS Instance ID
    json_writer.SetString(kKmsInstanceIdField, kms_instance_id);
    // 4. For footer key, write KMS Instance URL
    json_writer.SetString(kKmsInstanceUrlField, kms_instance_url);
  }
  // 5. Write master key ID
  json_writer.SetString(kMasterKeyIdField, master_key_id);
  // 6. Write wrapped DEK
  json_writer.SetString(kWrappedDataEncryptionKeyField, encoded_wrapped_dek);
  // 7. Write isDoubleWrapped
  json_writer.SetBool(kDoubleWrappingField, is_double_wrapped);
  if (is_double_wrapped) {
    // 8. In double wrapping mode, write KEK ID
    json_writer.SetString(kKeyEncryptionKeyIdField, kek_id);
    // 9. In double wrapping mode, write wrapped KEK
    json_writer.SetString(kWrappedKeyEncryptionKeyField, encoded_wrapped_kek);
  }

  return json_writer.Serialize();
}

}  // namespace encryption
}  // namespace parquet

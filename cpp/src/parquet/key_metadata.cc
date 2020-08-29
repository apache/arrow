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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "parquet/exception.h"
#include "parquet/key_metadata.h"

namespace parquet {
namespace encryption {

constexpr const char KeyMetadata::kKeyMaterialInternalStorageField[];
constexpr const char KeyMetadata::kKeyReferenceField[];

KeyMetadata::KeyMetadata(const std::string& key_reference)
    : is_internal_storage_(false), key_material_or_reference_(key_reference) {}

KeyMetadata::KeyMetadata(const KeyMaterial& key_material)
    : is_internal_storage_(true), key_material_or_reference_(key_material) {}

KeyMetadata KeyMetadata::Parse(const std::string& key_metadata) {
  rapidjson::Document document;
  document.Parse(key_metadata.c_str());

  if (document.HasParseError() || !document.IsObject()) {
    throw ParquetException("Failed to parse key metadata " + key_metadata);
  }

  // 1. Extract "key material type", and make sure it is supported
  std::string key_material_type =
      document[KeyMaterial::kKeyMaterialTypeField].GetString();
  if (key_material_type != KeyMaterial::kKeyMaterialType1) {
    throw ParquetException("Wrong key material type: " + key_material_type + " vs " +
                           KeyMaterial::kKeyMaterialType1);
  }

  // 2. Check if "key material" is stored internally in Parquet file key metadata, or is
  // stored externally
  bool is_internal_storage = document[kKeyMaterialInternalStorageField].GetBool();

  if (is_internal_storage) {
    // 3.1 "key material" is stored internally, inside "key metadata" - parse it
    KeyMaterial key_material = KeyMaterial::Parse(document);
    return KeyMetadata(key_material);
  } else {
    // 3.2 "key material" is stored externally. "key metadata" keeps a reference to it
    std::string key_reference = document[kKeyReferenceField].GetString();
    return KeyMetadata(key_reference);
  }
}

// For external material only. For internal material, create serialized KeyMaterial
// directly
std::string KeyMetadata::CreateSerializedForExternalMaterial(
    const std::string& key_reference) {
  rapidjson::Document d;
  auto& allocator = d.GetAllocator();
  rapidjson::Value key_metadata_map(rapidjson::kObjectType);

  key_metadata_map.AddMember(KeyMaterial::kKeyMaterialTypeField,
                             KeyMaterial::kKeyMaterialType1, allocator);
  key_metadata_map.AddMember(kKeyMaterialInternalStorageField, false, allocator);

  rapidjson::Value value(rapidjson::kStringType);
  value.SetString(key_reference.c_str(), allocator);
  key_metadata_map.AddMember(kKeyReferenceField, value, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  key_metadata_map.Accept(writer);

  return buffer.GetString();
}

}  // namespace encryption
}  // namespace parquet

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

#include <string>

#include "parquet/key_material.h"

namespace parquet {
namespace encryption {

// Parquet encryption specification defines "key metadata" as an arbitrary byte array,
// generated by file writers for each encryption key, and passed to the low level API for
// storage in the file footer . The "key metadata" field is made available to file readers
// to enable recovery of the key. This simple interface can be utilized for implementation
// of any key management scheme.
//
// The keytools package (PARQUET-1373) implements one approach, of many possible, to key
// management and to generation of the "key metadata" fields. This approach, based on the
// "envelope encryption" pattern, allows to work with KMS servers. It keeps the actual
// material, required to recover a key, in a "key material" object (see the KeyMaterial
// class for details).
//
// KeyMetadata class writes (and reads) the "key metadata" field as a flat json object,
// with the following fields:
// 1. "keyMaterialType" - a String, with the type of  key material. In the current
// version, only one value is allowed - "PKMT1" (stands for "parquet key management tools,
// version 1")
// 2. "internalStorage" - a boolean. If true, means that "key material" is kept inside the
// "key metadata" field. If false, "key material" is kept externally (outside Parquet
// files) - in this case, "key metadata" keeps a reference to the external "key material".
// 3. "keyReference" - a String, with the reference to the external "key material".
// Written only if internalStorage is false.
//
// If internalStorage is true, "key material" is a part of "key metadata", and the json
// keeps additional fields, described in the KeyMaterial class.
class KeyMetadata {
 public:
  static constexpr char kKeyMaterialInternalStorageField[] = "internalStorage";
  static constexpr char kKeyReferenceField[] = "keyReference";

  static KeyMetadata Parse(const std::string& key_metadata_bytes);

  static std::string CreateSerializedForExternalMaterial(
      const std::string& key_reference);

  bool key_material_stored_internally() const { return is_internal_storage_; }

  const KeyMaterial& key_material() const {
    if (!is_internal_storage_) {
      throw ParquetException("key material is stored externally.");
    }
    return key_material_;
  }

  const std::string& key_reference() const {
    if (is_internal_storage_) {
      throw ParquetException("key material is stored internally.");
    }
    return key_reference_;
  }

 private:
  explicit KeyMetadata(const KeyMaterial& key_material);
  explicit KeyMetadata(const std::string& key_reference);

  bool is_internal_storage_;
  // set if is_internal_storage_ is false
  std::string key_reference_;
  // set if is_internal_storage_ is true
  KeyMaterial key_material_;
};

}  // namespace encryption
}  // namespace parquet

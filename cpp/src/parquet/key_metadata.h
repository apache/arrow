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

class KeyMetadata {
 public:
  static constexpr char KEY_MATERIAL_INTERNAL_STORAGE_FIELD[] = "internalStorage";
  static constexpr char KEY_REFERENCE_FIELD[] = "keyReference";

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

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

#include <set>
#include <string>
#include <unordered_map>

#include "arrow/filesystem/filesystem.h"

#include "parquet/encryption/file_key_material_store.h"
#include "parquet/encryption/file_path.h"

namespace parquet {
namespace encryption {

/// Key material can be stored outside the Parquet file, for example in a separate small
/// file in the same folder. This is important for “key rotation”, when MEKs have to be
/// changed (if compromised; or periodically, just in case) - without modifying the
/// Parquet files (often  immutable).
class PARQUET_EXPORT FileSystemKeyMaterialStore : public FileKeyMaterialStore {
 public:
  static constexpr const char kKetMaterialFilePrefix[] = "_KEY_MATERIAL_FOR_";
  static constexpr const char kTempFilePrefix[] = "_TMP";
  static constexpr const char kKeyMaterialFileSuffix[] = ".json";

  FileSystemKeyMaterialStore() {}

  /// Initializes key material store for a parquet file.
  void initialize(const std::shared_ptr<FilePath>& parquet_file_path, bool temp_store);

  /// Add key material for one encryption key.
  void AddKeyMaterial(std::string key_id_in_file, std::string key_material) {
    key_material_map_.insert({key_id_in_file, key_material});
  }

  /// Get key material
  std::string GetKeyMaterial(std::string key_id_in_file) {
    if (key_material_map_.empty()) {
      LoadKeyMaterialMap();
    }
    auto found = key_material_map_.find(key_id_in_file);
    return found->second;
  }

  /// After key material was added for all keys in the given Parquet file,
  /// save material in persistent store.
  void SaveMaterial();

  /// Remove key material from persistent store. Used in key rotation.
  void RemoveMaterial();

  /// Move key material to another store. Used in key rotation.
  void MoveMaterialTo(std::shared_ptr<FileKeyMaterialStore> target_key_store);

  ///  Returns the Set of all key IDs in this store (for the given Parquet file)
  std::vector<std::string> GetKeyIDSet();

 private:
  std::string GetStorageFilePath() { return key_material_file_->path(); }

  std::string BuildKeyMaterialMapJson();
  void LoadKeyMaterialMap();
  std::shared_ptr<FilePath> key_material_file_;
  /// Maps ID of a key in Parquet file and key material
  std::unordered_map<std::string, std::string> key_material_map_;
};

}  // namespace encryption
}  // namespace parquet

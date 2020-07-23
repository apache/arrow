// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License") = 0; you may not use this file except in compliance
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

namespace parquet {

namespace encryption {

class FileKeyMaterialStore {
 public:
  // Initializes key material store for a parquet file.
  // @param parquetFilePath Parquet file path
  virtual void Initialize(const std::string& parquet_file_path, bool temp_store) = 0;

  // Add key material for one encryption key.
  // @param keyIDInFile ID of the key in Parquet file
  // @param keyMaterial key material
  virtual void AddKeyMaterial(const std::string& key_id_in_file,
                              const std::string& key_material) = 0;

  /**
   * After key material was added for all keys in the given Parquet file,
   * save material in persistent store.
   */
  virtual void SaveMaterial() = 0;

  // Get key material
  // @param keyIDInFile ID of a key in Parquet file
  // @return key material
  virtual std::string GetKeyMaterial(const std::string& key_id_in_file) = 0;

  //@return Set of all key IDs in this store (for the given Parquet file)
  virtual std::set<std::string> GetKeyIdSet() = 0;

  // Remove key material from persistent store. Used in key rotation.
  virtual void RemoveMaterial() = 0;

  // Move key material to another store. Used in key rotation.
  virtual void MoveMaterialTo(FileKeyMaterialStore* target_key_material_store) = 0;
};

}  // namespace encryption

}  // namespace parquet

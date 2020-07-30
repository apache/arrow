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
#include <set>
#include <string>

#include "arrow/io/hdfs.h"
#include "parquet/file_key_material_store.h"

namespace parquet {

namespace encryption {

class HadoopFSKeyMaterialStore : public FileKeyMaterialStore {
 public:
  static constexpr char KEY_MATERIAL_FILE_PREFIX[] = "_KEY_MATERIAL_FOR_";
  static constexpr char TEMP_FILE_PREFIX[] = "_TMP";
  static constexpr char KEY_MATERIAL_FILE_SUFFFIX[] = ".json";

  explicit HadoopFSKeyMaterialStore(
      std::shared_ptr<arrow::io::HadoopFileSystem> hadoop_file_system);

  void Initialize(const std::string& parquet_file_path, bool temp_store) override;

  void AddKeyMaterial(const std::string& key_id_in_file,
                      const std::string& key_material) override;

  std::string GetKeyMaterial(const std::string& key_id_in_file) override;

  void SaveMaterial() override;

  std::set<std::string> GetKeyIdSet() override;

  void RemoveMaterial() override;

  void MoveMaterialTo(FileKeyMaterialStore* key_material_store) override;

  const std::string& storage_file_path() const;

 private:
  void LoadKeyMaterialMap();

  std::shared_ptr<arrow::io::HadoopFileSystem> hadoop_file_system_;
  std::string key_material_file_;
  std::map<std::string, std::string> key_material_map_;
};

}  // namespace encryption

}  // namespace parquet

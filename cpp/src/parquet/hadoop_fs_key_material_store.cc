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

#include "arrow/buffer.h"
#include "arrow/filesystem/path_util.h"
#include "parquet/exception.h"
#include "parquet/hadoop_fs_key_material_store.h"

namespace parquet {

namespace encryption {

std::string ToJsonString(const std::map<std::string, std::string>& m) {
  rapidjson::Document d;
  auto& allocator = d.GetAllocator();
  rapidjson::Value root(rapidjson::kObjectType);
  rapidjson::Value key(rapidjson::kStringType);
  rapidjson::Value value(rapidjson::kStringType);

  for (auto it = m.begin(); it != m.end(); it++) {
    key.SetString(it->first.c_str(), allocator);
    value.SetString(it->second.c_str(), allocator);
    root.AddMember(key, value, allocator);
  }

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  root.Accept(writer);

  return buffer.GetString();
}

bool Json2Map(const std::string& json, std::map<std::string, std::string>& out_map) {
  rapidjson::Document document;
  document.Parse(json.c_str());

  if (document.HasParseError() || !document.IsObject()) {
    return false;
  }
  std::map<std::string, std::string> m;
  for (rapidjson::Value::ConstMemberIterator itr = document.MemberBegin();
       itr != document.MemberEnd(); itr++) {
    if (!itr->value.IsString()) {
      return false;
    }
    m.insert({itr->name.GetString(), itr->value.GetString()});
  }
  out_map = m;
  return true;
}

// TODO use memory pool?
HadoopFSKeyMaterialStore::HadoopFSKeyMaterialStore(
    std::shared_ptr<arrow::io::HadoopFileSystem> hadoop_file_system)
    : hadoop_file_system_(hadoop_file_system) {}

void HadoopFSKeyMaterialStore::Initialize(const std::string& parquet_file_path,
                                          bool temp_store) {
  std::string full_prefix = temp_store ? TEMP_FILE_PREFIX : "";
  full_prefix += KEY_MATERIAL_FILE_PREFIX;
  std::pair<std::string, std::string> parent_and_basename =
      arrow::fs::internal::GetAbstractPathParent(parquet_file_path);
  key_material_file_ = parent_and_basename.first + full_prefix +
                       parent_and_basename.second + KEY_MATERIAL_FILE_SUFFFIX;
}

void HadoopFSKeyMaterialStore::AddKeyMaterial(const std::string& key_id_in_file,
                                              const std::string& key_material) {
  key_material_map_.insert({key_id_in_file, key_material});
}

std::string HadoopFSKeyMaterialStore::GetKeyMaterial(const std::string& key_id_in_file) {
  if (key_material_map_.size() == 0) {
    LoadKeyMaterialMap();
  }
  return key_material_map_[key_id_in_file];
}

void HadoopFSKeyMaterialStore::LoadKeyMaterialMap() {
  std::shared_ptr<arrow::io::HdfsReadableFile> file;

  arrow::Status status = hadoop_file_system_->OpenReadable(key_material_file_, &file);
  arrow::Result<int64_t> result_file_size = file->GetSize();
  if (!result_file_size.ok()) {
    throw new ParquetException("Failed to read key material from " + key_material_file_);
  }

  std::shared_ptr<arrow::Buffer> buffer;
  arrow::Result<int64_t> result_read_size =
      file->Read(result_file_size.ValueUnsafe(), buffer.get());
  if (!result_read_size.ok() ||
      result_read_size.ValueUnsafe() != result_file_size.ValueUnsafe()) {
    throw new ParquetException("Failed to read key material from " + key_material_file_);
  }

  std::string json = buffer->ToString();
  bool ok = Json2Map(json, key_material_map_);
  if (!ok) {
    throw new ParquetException("Failed to parse key material from " + key_material_file_);
  }
}

void HadoopFSKeyMaterialStore::SaveMaterial() {
  std::shared_ptr<arrow::io::HdfsOutputStream> file;
  arrow::Status status =
      hadoop_file_system_->OpenWritable(key_material_file_, false, &file);
  if (!status.ok()) {
    throw new ParquetException("Failed to open key material file " + key_material_file_);
  }
  std::string json = ToJsonString(key_material_map_);
  status = file->Write(&json[0], json.size());

  if (!status.ok()) {
    throw new ParquetException("Failed to save key material in " + key_material_file_);
  }
}

std::set<std::string> HadoopFSKeyMaterialStore::GetKeyIdSet() {
  if (key_material_map_.size() == 0) {
    LoadKeyMaterialMap();
  }
  std::set<std::string> keys;
  for (auto it = key_material_map_.begin(); it != key_material_map_.end(); ++it) {
    keys.insert(it->first);
  }
  return keys;
}

void HadoopFSKeyMaterialStore::RemoveMaterial() {
  arrow::Status status = hadoop_file_system_->Delete(key_material_file_);
  if (!status.ok()) {
    throw new ParquetException("Failed to delete file " + key_material_file_);
  }
}

void HadoopFSKeyMaterialStore::MoveMaterialTo(FileKeyMaterialStore* key_material_store) {
  // Currently supports only moving to a HadoopFSKeyMaterialStore
  HadoopFSKeyMaterialStore* target_store =
      static_cast<HadoopFSKeyMaterialStore*>(key_material_store);
  if (target_store == nullptr) {
    throw new ParquetException(
        "Currently supports only moving to HadoopFSKeyMaterialStore");
  }
  std::string target_key_material_file = target_store->storage_file_path();

  arrow::Status status =
      hadoop_file_system_->Rename(key_material_file_, target_key_material_file);
  throw new ParquetException("Failed to rename file " + key_material_file_);
}

const std::string& HadoopFSKeyMaterialStore::storage_file_path() const {
  return key_material_file_;
}

}  // namespace encryption

}  // namespace parquet

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

#include <string_view>

#include "arrow/filesystem/filesystem.h"
#include "arrow/result.h"
#include "arrow/testing/json_integration.h"
#include "arrow/testing/json_internal.h"

#include "parquet/encryption/file_system_key_material_store.h"
#include "parquet/encryption/key_material.h"
#include "parquet/exception.h"

namespace parquet {
namespace encryption {

constexpr const char FileSystemKeyMaterialStore::kKeyMaterialFilePrefix[];
constexpr const char FileSystemKeyMaterialStore::kTempFilePrefix[];
constexpr const char FileSystemKeyMaterialStore::kKeyMaterialFileSuffix[];

FileSystemKeyMaterialStore::FileSystemKeyMaterialStore(
    const std::string& key_material_file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system) :
  key_material_file_path_{key_material_file_path}, file_system_{file_system} {
}

std::shared_ptr<FileSystemKeyMaterialStore> FileSystemKeyMaterialStore::Make(
    const std::string& parquet_file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system, bool temp_store) {
  ::arrow::fs::FileInfo file_info(parquet_file_path);
  std::string full_prefix =
      (temp_store ? std::string(FileSystemKeyMaterialStore::kTempFilePrefix) : "");
  full_prefix =
      full_prefix + std::string(FileSystemKeyMaterialStore::kKeyMaterialFilePrefix);
  std::string key_material_file_name =
      full_prefix + file_info.base_name() +
      std::string(FileSystemKeyMaterialStore::kKeyMaterialFileSuffix);
  std::string key_material_file_path = file_info.dir_name() + "/" + key_material_file_name;
  return std::make_shared<FileSystemKeyMaterialStore>(key_material_file_path, file_system);
}

void FileSystemKeyMaterialStore::LoadKeyMaterialMap() {
  ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> file =
      file_system_->OpenInputFile(key_material_file_path_);
  std::shared_ptr<::arrow::io::RandomAccessFile> input = file.ValueOrDie();
  rj::Document document;
  ::arrow::Result<std::shared_ptr<Buffer>> buff =
      input->ReadAt(0, input->GetSize().ValueOrDie());
  std::string buff_str = buff.ValueOrDie()->ToString();
  ::std::string_view sv(buff_str);
  document.Parse(sv.data(), sv.size());
  for (rj::Value::ConstMemberIterator itr = document.MemberBegin();
       itr != document.MemberEnd(); ++itr) {
    key_material_map_.insert({itr->name.GetString(), itr->value.GetString()});
  }
}

std::string FileSystemKeyMaterialStore::BuildKeyMaterialMapJson() {
  rj::Document d;
  auto& allocator = d.GetAllocator();
  rj::Value json_map(rj::kObjectType);

  rj::Value key(rj::kStringType);
  rj::Value value(rj::kStringType);
  for (auto it = key_material_map_.begin(); it != key_material_map_.end(); it++) {
    key.SetString(it->first, allocator);
    value.SetString(it->second, allocator);
    json_map.AddMember(key, value, allocator);
  }
  rj::StringBuffer buffer;
  rj::Writer<rj::StringBuffer> writer(buffer);
  json_map.Accept(writer);
  return buffer.GetString();
}

void FileSystemKeyMaterialStore::SaveMaterial() {
  ::arrow::Result<std::shared_ptr<::arrow::io::OutputStream>> sink =
      file_system_->OpenOutputStream(key_material_file_path_);
  auto stream = sink.ValueOrDie();
  std::string key_material_json = BuildKeyMaterialMapJson();
  PARQUET_THROW_NOT_OK(stream->Write(key_material_json));
  PARQUET_THROW_NOT_OK(stream->Flush());
  PARQUET_THROW_NOT_OK(stream->Close());
}

void FileSystemKeyMaterialStore::RemoveMaterial() {
  if (file_system_->DeleteFile(key_material_file_path_) != ::arrow::Status::OK()) {
    throw ParquetException("Failed to delete key material file " + key_material_file_path_);
  }
}

std::vector<std::string> FileSystemKeyMaterialStore::GetKeyIDSet() {
  if (key_material_map_.empty()) {
    LoadKeyMaterialMap();
  }
  std::vector<std::string> keys;
  keys.reserve(key_material_map_.size());

  for (auto kv : key_material_map_) {
    keys.push_back(kv.first);
  }
  return keys;
}

void FileSystemKeyMaterialStore::MoveMaterialTo(
    std::shared_ptr<FileKeyMaterialStore> target_key_store) {
  std::shared_ptr<FileSystemKeyMaterialStore> target_key_file_store =
      std::static_pointer_cast<FileSystemKeyMaterialStore>(target_key_store);
  std::string target_key_material_file = target_key_file_store->GetStorageFilePath();
  if (file_system_->Move(key_material_file_path_, target_key_material_file) != ::arrow::Status::OK()) {
    throw ParquetException("Failed to rename key material file " + key_material_file_path_);
  }
}

}  // namespace encryption
}  // namespace parquet

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
#include "arrow/filesystem/path_util.h"
#include "arrow/json/object_parser.h"
#include "arrow/json/object_writer.h"
#include "arrow/result.h"

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
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system)
    : key_material_file_path_{key_material_file_path}, file_system_{file_system} {}

std::shared_ptr<FileSystemKeyMaterialStore> FileSystemKeyMaterialStore::Make(
    const std::string& parquet_file_path,
    const std::shared_ptr<::arrow::fs::FileSystem>& file_system, bool use_tmp_prefix) {
  if (parquet_file_path.empty()) {
    throw ParquetException(
        "The Parquet file path must be specified when using external key material");
  }
  if (file_system == nullptr) {
    throw ParquetException(
        "A file system must be specified when using external key material");
  }

  ::arrow::fs::FileInfo file_info(parquet_file_path);
  std::stringstream key_material_file_name;
  if (use_tmp_prefix) {
    key_material_file_name << FileSystemKeyMaterialStore::kTempFilePrefix;
  }
  key_material_file_name << FileSystemKeyMaterialStore::kKeyMaterialFilePrefix
                         << file_info.base_name()
                         << FileSystemKeyMaterialStore::kKeyMaterialFileSuffix;

  std::string key_material_file_path = ::arrow::fs::internal::ConcatAbstractPath(
      file_info.dir_name(), key_material_file_name.str());
  return std::make_shared<FileSystemKeyMaterialStore>(key_material_file_path,
                                                      file_system);
}

void FileSystemKeyMaterialStore::LoadKeyMaterialMap() {
  std::shared_ptr<::arrow::io::RandomAccessFile> input;
  PARQUET_ASSIGN_OR_THROW(input, file_system_->OpenInputFile(key_material_file_path_));
  int64_t input_size;
  std::shared_ptr<Buffer> buff;
  PARQUET_ASSIGN_OR_THROW(input_size, input->GetSize());
  PARQUET_ASSIGN_OR_THROW(buff, input->ReadAt(0, input_size));
  std::string buff_str = buff->ToString();
  ::arrow::json::internal::ObjectParser parser;
  auto status = parser.Parse(buff_str);
  PARQUET_THROW_NOT_OK(status);
  PARQUET_ASSIGN_OR_THROW(key_material_map_, parser.GetStringMap());
}

std::string FileSystemKeyMaterialStore::BuildKeyMaterialMapJson() {
  ::arrow::json::internal::ObjectWriter writer;
  for (const auto& it : key_material_map_) {
    writer.SetString(it.first, it.second);
  }
  return writer.Serialize();
}

void FileSystemKeyMaterialStore::SaveMaterial() {
  std::shared_ptr<::arrow::io::OutputStream> stream;
  PARQUET_ASSIGN_OR_THROW(stream,
                          file_system_->OpenOutputStream(key_material_file_path_));
  std::string key_material_json = BuildKeyMaterialMapJson();
  PARQUET_THROW_NOT_OK(stream->Write(key_material_json));
  PARQUET_THROW_NOT_OK(stream->Flush());
  PARQUET_THROW_NOT_OK(stream->Close());
}

void FileSystemKeyMaterialStore::RemoveMaterial() {
  auto status = file_system_->DeleteFile(key_material_file_path_);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "Failed to delete key material file '" << key_material_file_path_
       << "': " << status;
    throw ParquetException(ss.str());
  }
}

std::vector<std::string> FileSystemKeyMaterialStore::GetKeyIDSet() {
  if (key_material_map_.empty()) {
    LoadKeyMaterialMap();
  }
  std::vector<std::string> keys;
  keys.reserve(key_material_map_.size());

  for (const auto& kv : key_material_map_) {
    keys.push_back(kv.first);
  }
  return keys;
}

void FileSystemKeyMaterialStore::MoveMaterialTo(
    std::shared_ptr<FileKeyMaterialStore> target_key_store) {
  auto target_key_file_store =
      std::dynamic_pointer_cast<FileSystemKeyMaterialStore>(target_key_store);
  if (target_key_file_store == nullptr) {
    throw ParquetException(
        "Cannot move key material to a store that is not a FileSystemKeyMaterialStore");
  }
  std::string target_key_material_file = target_key_file_store->GetStorageFilePath();
  auto status = file_system_->Move(key_material_file_path_, target_key_material_file);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "Failed to move key material file '" << key_material_file_path_ << "' to '"
       << target_key_material_file << "': " << status;
    throw ParquetException(ss.str());
  }
}

}  // namespace encryption
}  // namespace parquet

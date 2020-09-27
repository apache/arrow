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

#include "arrow/filesystem/filesystem.h"
#include "arrow/testing/json_integration.h"
#include "arrow/testing/json_internal.h"
#include "arrow/util/io_util.h"
#include "arrow/util/make_unique.h"

#include "parquet/encryption/file_system_key_material_store.h"
#include "parquet/encryption/key_material.h"

namespace parquet {
namespace encryption {

void FileSystemKeyMaterialStore::initialize(
    const std::shared_ptr<FilePath>& parquet_file_path, bool temp_store) {
  std::string full_prefix = (temp_store ? std::string(kTempFilePrefix) : "");
  full_prefix = full_prefix + std::string(kKetMaterialFilePrefix);
  std::string key_material_file_name =
      full_prefix + parquet_file_path->child() + std::string(kKeyMaterialFileSuffix);
  key_material_file_ = ::arrow::internal::make_unique<FilePath>(
      parquet_file_path->parent(), key_material_file_name,
      parquet_file_path->filesystem());
}

void FileSystemKeyMaterialStore::LoadKeyMaterialMap() {
  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> file =
      key_material_file_->OpenReadable();
  arrow::Result<int64_t> file_size;
  std::shared_ptr<arrow::io::RandomAccessFile> input = file.ValueOrDie();
  file_size = input->GetSize();
  rj::Document document;
  arrow::Result<std::shared_ptr<Buffer>> buff = input->ReadAt(0, file_size.ValueOrDie());
  std::string buff_str = buff.ValueOrDie()->ToString();
  document.Parse(buff_str);
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
    key.SetString(it->first.c_str(), allocator);
    value.SetString(it->second.c_str(), allocator);
    json_map.AddMember(key, value, allocator);
  }
  rj::StringBuffer buffer;
  rj::Writer<rj::StringBuffer> writer(buffer);
  json_map.Accept(writer);
  return buffer.GetString();
}

void FileSystemKeyMaterialStore::SaveMaterial() {
  arrow::Result<std::shared_ptr<arrow::io::OutputStream>> sink =
      key_material_file_->OpenWriteable();
  auto stream = sink.ValueOrDie();
  std::string key_material_json = std::move(BuildKeyMaterialMapJson());
  stream->Write(key_material_json);
  stream->Flush();
  stream->Close();
}

void FileSystemKeyMaterialStore::RemoveMaterial() { key_material_file_->DeleteFile(); }

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
    std::shared_ptr<FileSystemKeyMaterialStore> target_key_store) {
  std::string target_key_material_file = target_key_store->GetStorageFilePath();
  std::string source_key_material_file =
      key_material_file_->parent() + key_material_file_->child();
  key_material_file_->Move(source_key_material_file, target_key_material_file);
}

}  // namespace encryption
}  // namespace parquet

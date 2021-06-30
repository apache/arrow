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

#include <llvm/Support/MemoryBuffer.h>

#include <boost/any.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "gandiva/flatbuffer/gandiva_cache_file_generated.h"
#include "gandiva/gandiva_cache_file.pb.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/Path.h"

// modified from boost LRU cache -> the boost cache supported only an
// ordered map.
namespace gandiva {
// a cache which evicts the least recently used item when it is full
template <class Key, class Value>
class LruCache {
 public:
  using key_type = Key;
  using value_type = Value;
  using list_type = std::list<key_type>;
  struct hasher {
    template <typename I>
    std::size_t operator()(const I& i) const {
      return i.Hash();
    }
  };
  using map_type =
      std::unordered_map<key_type, std::pair<value_type, typename list_type::iterator>,
                         hasher>;

  explicit LruCache(size_t capacity, size_t disk_capacity, size_t reserved)
      : cache_capacity_(capacity) {
    disk_reserved_space_ = reserved;
    disk_space_capactiy_ = disk_capacity;
    llvm::sys::fs::current_path(cache_dir_);
    llvm::sys::path::append(cache_dir_, "cache");
    ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Cache dir path: " << std::string(cache_dir_);

    // checkDiskSpace();
    verifyCacheDir();
    checkDiskSpace();

    ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Initial disk usage: "
                     << std::to_string(disk_cache_size_) << " bytes.";
    ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Initial disk number of files: "
                     << std::to_string(disk_cache_files_qty_) << ".";

    const char* env_cache_format = std::getenv("GANDIVA_CACHE_FILE_FORMAT");
    if (env_cache_format != nullptr) {
      env_cache_format_ = env_cache_format;
    } else {
      env_cache_format_ = "PROTOBUF";
    }
  }

  ~LruCache() {}

  size_t size() const { return map_.size(); }

  size_t capacity() const { return cache_capacity_; }

  bool empty() const { return map_.empty(); }

  bool contains(const key_type& key) { return map_.find(key) != map_.end(); }

  void insert(const key_type& key, const value_type& value) {
    typename map_type::iterator i = map_.find(key);
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full
      if (size() >= cache_capacity_) {
        // cache is full, evict the least recently used item
        evict();
      }

      // insert the new item
      lru_list_.push_front(key);
      map_[key] = std::make_pair(value, lru_list_.begin());
      cache_size_ += sizeof(key);
      cache_size_ += sizeof(value.get());
    }
  }

  void insertObject(const key_type& key, const value_type value,
                    size_t object_cache_size) {
    typename map_type::iterator i = map_.find(key);

    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full
      if (getLruCacheSize() >= cache_capacity_) {
        // cache is full, evict the least recently used item
        evitObjectSafely(object_cache_size);
      }

      if (getLruCacheSize() + object_cache_size >= cache_capacity_) {
        // cache will pass the maximum capacity, evict the least recently used items
        evitObjectSafely(object_cache_size);
      }

      // auto casted_value = boost::any_cast<int>(value);

      // insert the new item
      lru_list_.push_front(key);
      map_[key] = std::make_pair(value, lru_list_.begin());
      size_map_[key] = std::make_pair(object_cache_size, lru_list_.begin());
      cache_size_ += object_cache_size;
    }
  }

  arrow::util::optional<value_type> get(const key_type& key) {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);
    if (value_for_key == map_.end()) {
      // value not in cache
      return arrow::util::nullopt;
    }

    // return the value, but first update its place in the most
    // recently used list
    typename list_type::iterator position_in_lru_list = value_for_key->second.second;
    if (position_in_lru_list != lru_list_.begin()) {
      // move item to the front of the most recently used list
      lru_list_.erase(position_in_lru_list);
      lru_list_.push_front(key);

      // update iterator in map
      position_in_lru_list = lru_list_.begin();
      const value_type& value = value_for_key->second.first;
      map_[key] = std::make_pair(value, position_in_lru_list);

      // return the value
      return value;
    } else {
      // the item is already at the front of the most recently
      // used list so just return it
      return value_for_key->second.first;
    }
  }

  void reinsertObject(const key_type& key, const value_type& value,
                      size_t object_cache_size) {
    typename map_type::iterator i = map_.find(key);

    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full
      if (getLruCacheSize() >= cache_capacity_) {
        // cache is full, evict the least recently used item
        evitObjectSafely(object_cache_size);
      }

      if (getLruCacheSize() + object_cache_size >= cache_capacity_) {
        // cache will pass the maximum capacity, evict the least recently used items
        evitObjectSafely(object_cache_size);
      }

      // insert the new item
      lru_list_.push_front(key);
      map_[key] = std::make_pair(value, lru_list_.begin());
      size_map_[key] = std::make_pair(object_cache_size, lru_list_.begin());
      cache_size_ += object_cache_size;
    }
  }

  arrow::util::optional<value_type> getObject(const key_type& key) {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);

    std::string obj_file_name = std::to_string(key.Hash()) + ".cache";
    llvm::SmallString<128> obj_cache_file = cache_dir_;
    llvm::sys::path::append(obj_cache_file, obj_file_name);

    if (value_for_key == map_.end() && !llvm::sys::fs::exists(obj_cache_file.str())) {
      return arrow::util::nullopt;
    }
    if (value_for_key == map_.end()) {
      // value not in cache
      if (llvm::sys::fs::exists(obj_cache_file.str())) {
        // This file is in our disk!

        //        std::unique_ptr<llvm::MemoryBuffer> obj_cache_buffer =
        //            llvm::MemoryBuffer::getMemBufferCopy(obj_cache_file_buffer.get()->getBuffer(),
        //                                                 obj_cache_file_buffer.get()->getBufferIdentifier());

        // Flatbuffer implementation

        std::shared_ptr<llvm::MemoryBuffer> obj_cache_buffer_shared;
        if (env_cache_format_ == "PROTOBUF") {
          obj_cache_buffer_shared =
              readCacheFromProtobufFile(obj_cache_file, obj_file_name, key);
        }
        if (env_cache_format_ == "FLATBUFFER") {
          obj_cache_buffer_shared =
              readCacheFromFlatbufferFile(obj_cache_file, obj_file_name, key);
        }

        ARROW_LOG(DEBUG) << obj_cache_buffer_shared->getBuffer().str();
        reinsertObject(key, obj_cache_buffer_shared,
                       obj_cache_buffer_shared->getBufferSize());

        removeObjectCodeCacheFile(obj_cache_file.c_str(),
                                  obj_cache_buffer_shared->getBufferSize());
        return obj_cache_buffer_shared;
      }
    }

    // return the value, but first update its place in the most
    // recently used list
    typename list_type::iterator position_in_lru_list = value_for_key->second.second;
    if (position_in_lru_list != lru_list_.begin()) {
      // move item to the front of the most recently used list
      lru_list_.erase(position_in_lru_list);
      lru_list_.push_front(key);

      // update iterator in map
      position_in_lru_list = lru_list_.begin();
      const value_type& value = value_for_key->second.first;
      map_[key] = std::make_pair(value, position_in_lru_list);

      // return the value
      return value;
    } else {
      // the item is already at the front of the most recently
      // used list so just return it
      return value_for_key->second.first;
    }
  }

  void clear() {
    map_.clear();
    lru_list_.clear();
    cache_size_ = 0;
    clearCacheDisk();
  }

  std::string toString() {
    auto lru_size = lru_list_.size();
    std::string string = "LRU Cache list size: " + std::to_string(lru_size) + "." +
                         " LRU Cache size: " + std::to_string(cache_size_);
    return string;
  }

  size_t getLruCacheSize() { return cache_size_; }

  llvm::SmallString<128> getCacheDir() { return cache_dir_; }

 private:
  void evict() {
    // evict item from the end of most recently used list
    typename list_type::iterator i = --lru_list_.end();
    map_.erase(*i);
    lru_list_.erase(i);
  }

  void evictObject() {
    // evict item from the end of most recently used list
    typename list_type::iterator i = --lru_list_.end();
    const size_t size_to_decrease = size_map_.find(*i)->second.first;
    const value_type value = map_.find(*i)->second.first;
    saveObjectToCacheDir(*i, value);
    cache_size_ = cache_size_ - size_to_decrease;
    map_.erase(*i);
    size_map_.erase(*i);
    lru_list_.erase(i);
  }

  void evitObjectSafely(size_t object_cache_size) {
    while (cache_size_ + object_cache_size >= cache_capacity_) {
      evictObject();
    }
  }

  std::shared_ptr<llvm::MemoryBuffer> readCacheFromFlatbufferFile(
      llvm::SmallString<128>& obj_cache_file, std::string obj_file_name,
      const key_type& key) {
    std::ifstream cached_file(obj_cache_file.c_str(), std::ios::binary);
    if (!cached_file) {
      std::cerr << "Failed to open the flatbuffer file cache." << std::endl;
      return nullptr;
    }

    std::vector<unsigned char> buffer_from_file(
        std::istreambuf_iterator<char>(cached_file), {});
    cached_file.close();

    auto flatbuffer_cache =
        gandiva::cache::fbs::GetCache(static_cast<void*>(buffer_from_file.data()));

    // Read the obj_code bytes
    auto flatbuffer_obj_code_string = flatbuffer_cache->object_code()->str();

    auto flatbuffer_schema_string = flatbuffer_cache->schema_exprs()->schema()->str();
    auto flatbuffer_exprs = flatbuffer_cache->schema_exprs()->exprs();

    std::vector<std::string> flatbuffer_exprs_string;

    for (auto expr : *flatbuffer_exprs) {
      flatbuffer_exprs_string.push_back(expr->str());
    }

    // Check if the file is really the same by checking its schema and expressions
    // if the file does not has the same schema and expressions, it happened a hash
    // collision and the files are not really the same.
    bool isReallySame =
        key.checkCacheFile(flatbuffer_schema_string, flatbuffer_exprs_string);
    if (!isReallySame) {
    }

    bool status = verifyFlatbufferContent(flatbuffer_cache, obj_file_name, key);

    if (status == true) {
      auto obj_code_ref = llvm::StringRef(flatbuffer_obj_code_string);
      auto ref_id_pb = llvm::StringRef(splitDirPath(obj_cache_file.c_str(), "/").back());

      std::unique_ptr<llvm::MemoryBuffer> obj_cache_buffer =
          llvm::MemoryBuffer::getMemBufferCopy(obj_code_ref, ref_id_pb);
      return std::move(obj_cache_buffer);
    }
    return nullptr;
  }

  std::shared_ptr<llvm::MemoryBuffer> readCacheFromProtobufFile(
      llvm::SmallString<128>& obj_cache_file, std::string obj_file_name,
      const key_type& key) {
    gandiva::cache::proto::SchemaExprsPairAndObjectCode schema_exprs_obj_code;

    // Read the protobuf file cache.
    std::fstream input(obj_cache_file.c_str(), std::ios::in | std::ios::binary);
    if (!schema_exprs_obj_code.ParseFromIstream(&input)) {
      std::cerr << "Failed to parse the proto buf file cache." << std::endl;
      // return -1;
      return nullptr;
    }

    bool status = verifyProtobufContent(schema_exprs_obj_code, obj_file_name, key);

    if (status != false) {
      const std::string obj_code_pb(schema_exprs_obj_code.objectcode());

      llvm::StringRef ref_id_pb(obj_cache_file.c_str());
      llvm::StringRef obj_code_ref(obj_code_pb);

      //      auto obj_cache_file_buffer =
      //          llvm::MemoryBuffer::getFile(obj_cache_file, -1, true, false);
      std::unique_ptr<llvm::MemoryBuffer> obj_cache_buffer =
          llvm::MemoryBuffer::getMemBufferCopy(obj_code_ref, ref_id_pb);
      // std::shared_ptr<llvm::MemoryBuffer> obj_cache_buffer_shared =
      // std::move(obj_cache_buffer);
      return std::move(obj_cache_buffer);
    }

    return nullptr;
  }

  void saveWithFlatbuffer(key_type& key, const value_type value,
                          std::string obj_file_name,
                          llvm::SmallString<128> obj_cache_file) {
    // Flatbuffer implementation
    auto value_ref_string = value->getBuffer().str();
    int8_t buffer[value_ref_string.length()];
    std::copy(value_ref_string.begin(), value_ref_string.end(), buffer);

    auto flatbuffer_obj_code = flatbuffer_builder_.CreateString(value_ref_string);
    auto flatbuffer_schema = flatbuffer_builder_.CreateString(key.getSchemaString());
    auto flatbuffer_exprs =
        flatbuffer_builder_.CreateVectorOfStrings(key.getExprsString());

    auto flatbuffer_schema_exprs_pair = gandiva::cache::fbs::CreateSchemaExpressionsPair(
        flatbuffer_builder_, flatbuffer_schema, flatbuffer_exprs);
    auto flatbuffer_cache = gandiva::cache::fbs::CreateCache(
        flatbuffer_builder_, flatbuffer_schema_exprs_pair, flatbuffer_obj_code);

    flatbuffer_builder_.Finish(flatbuffer_cache);

    uint8_t* pre_file_buffer = flatbuffer_builder_.GetBufferPointer();
    int64_t pre_file_buffer_size = flatbuffer_builder_.GetSize();

    std::ofstream cache_file(obj_cache_file.c_str(), std::ios::out | std::ios::binary);
    cache_file.write(reinterpret_cast<char*>(pre_file_buffer), pre_file_buffer_size);
    auto file_size = cache_file.tellp();
    cache_file.close();

    disk_cache_size_ += file_size;
    disk_cache_files_qty_ += 1;

    std::pair<std::string, size_t> file_and_size =
        std::make_pair(obj_file_name, file_size);
    cached_files_map_[std::to_string(key.Hash())] = file_and_size;

    updateCacheInfoFile();
    updateCacheListFile();
  }

  bool verifyFlatbufferContent(const cache::fbs::Cache* flatbuffer_cache,
                               std::string obj_file_name, const key_type& key) {
    auto flatbuffer_obj_code_string = flatbuffer_cache->object_code()->str();

    auto flatbuffer_schema_string = flatbuffer_cache->schema_exprs()->schema()->str();
    auto flatbuffer_exprs = flatbuffer_cache->schema_exprs()->exprs();

    std::vector<std::string> flatbuffer_exprs_string;

    for (auto expr : *flatbuffer_exprs) {
      flatbuffer_exprs_string.push_back(expr->str());
    }

    bool isReallySame =
        key.checkCacheFile(flatbuffer_schema_string, flatbuffer_exprs_string);
    if (isReallySame) {
      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG][PROTOBUF]File " << obj_file_name
                       << " already exists.";
      return true;
    }
    ARROW_LOG(WARNING)
        << "File with the file name " << obj_file_name
        << " already exists, but they are not the same, cache had a hash collision.";
    return false;
  }

  void verifyFlatbufferFile(key_type& key, std::string obj_file_name,
                            std::ifstream& cached_file) {
    std::vector<unsigned char> buffer_from_file(
        std::istreambuf_iterator<char>(cached_file), {});
    cached_file.close();

    auto flatbuffer_cache =
        gandiva::cache::fbs::GetCache(static_cast<void*>(buffer_from_file.data()));

    bool status = verifyFlatbufferContent(flatbuffer_cache, obj_file_name, key);
    if (status == false) {
      ARROW_LOG(WARNING) << "There was an error verifying the cached flatbuffer file.";
    }
  }

  void saveWithProtobuf(key_type& key, const value_type value, std::string obj_file_name,
                        llvm::SmallString<128> obj_cache_file) {
    gandiva::cache::proto::SchemaExpressionsPair schema_exprs_pair;
    gandiva::cache::proto::SchemaExprsPairAndObjectCode schema_exprs_pair_obj_code;

    std::vector<std::string> exprs = key.getExprsString();

    for (auto& expr : exprs) {
      schema_exprs_pair.add_expressions(expr);
    }

    schema_exprs_pair.set_schema(key.getSchemaString());

    schema_exprs_pair_obj_code.set_allocated_schemaexpressionpair(&schema_exprs_pair);

    schema_exprs_pair_obj_code.set_objectcode(value->getBuffer().str());

    std::ofstream new_cache_file;

    new_cache_file.open(obj_cache_file.c_str(), std::ios::out | std::ios::binary);

    schema_exprs_pair_obj_code.SerializeToOstream(&new_cache_file);

    auto file_size = new_cache_file.tellp();

    new_cache_file.close();

    // Prevents the allocated schema_exprs_pair to be persisted across runs.
    schema_exprs_pair_obj_code.release_schemaexpressionpair();

    disk_cache_size_ += file_size;
    disk_cache_files_qty_ += 1;

    std::pair<std::string, size_t> file_and_size =
        std::make_pair(obj_file_name, file_size);
    cached_files_map_[std::to_string(key.Hash())] = file_and_size;

    updateCacheInfoFile();
    updateCacheListFile();
  }

  bool verifyProtobufContent(
      gandiva::cache::proto::SchemaExprsPairAndObjectCode& schema_exprs_pair_obj_code,
      std::string obj_file_name, const key_type& key) {
    //    auto protobuf_obj_code_string = schema_exprs_pair_obj_code.objectcode().c_str();
    auto protobuf_schema_string =
        schema_exprs_pair_obj_code.schemaexpressionpair().schema();
    std::vector<std::string> protobuf_exprs_string;

    for (auto& expr : schema_exprs_pair_obj_code.schemaexpressionpair().expressions()) {
      protobuf_exprs_string.push_back(expr);
    }

    bool isReallySame = key.checkCacheFile(protobuf_schema_string, protobuf_exprs_string);

    if (isReallySame) {
      ARROW_LOG(DEBUG) << "[OBJ-CACHE][PROTOBUF]File " << obj_file_name
                       << " already exists.";
      return true;
    }
    ARROW_LOG(WARNING)
        << "File with the file name " << obj_file_name
        << " already exists, but they are not the same, cache had a hash collision.";
    return false;
  }

  void verifyProtobufFile(key_type& key, std::string obj_file_name,
                          std::ifstream& cached_file) {
    gandiva::cache::proto::SchemaExpressionsPair schema_exprs_pair;
    gandiva::cache::proto::SchemaExprsPairAndObjectCode schema_exprs_pair_obj_code;

    schema_exprs_pair_obj_code.ParseFromIstream(&cached_file);

    cached_file.close();

    bool status = verifyProtobufContent(schema_exprs_pair_obj_code, obj_file_name, key);
    if (status == false) {
      ARROW_LOG(WARNING) << "There was an error verifying the cached protobuf file.";
    }
  }

  void saveObjectToCacheDir(key_type& key, const value_type value) {
    std::string obj_file_name = std::to_string(key.Hash()) + ".cache";

    llvm::SmallString<128> obj_cache_file = cache_dir_;
    llvm::sys::path::append(obj_cache_file, obj_file_name);

    size_t new_cache_size = value->getBufferSize() + disk_cache_size_;
    if (value->getBufferSize() >= disk_space_capactiy_) {
      ARROW_LOG(DEBUG) << "The cache file is to big!";
      return;
    }

    if (new_cache_size >= disk_space_capactiy_) {
      ARROW_LOG(DEBUG) << "Cache directory is full, it will be freed some space!";
      freeCacheDir(value->getBufferSize());
    }

    if (!llvm::sys::fs::exists(cache_dir_.str()) &&
        llvm::sys::fs::create_directory(cache_dir_.str())) {
      fprintf(stderr, "Unable to create cache directory\n");
      return;
    }

    if (!llvm::sys::fs::exists(obj_cache_file.str())) {
      // This file isn't in our disk, so we need to save it to the disk!
      // std::error_code ErrStr;
      // llvm::raw_fd_ostream CachedObjectFile(obj_cache_file.c_str(), ErrStr);
      // CachedObjectFile << value->getBuffer();
      // disk_cache_size_ +=  value->getBufferSize();
      // disk_cache_files_qty_ += 1;
      // CachedObjectFile.close();

      if (env_cache_format_ == "PROTOBUF") {
        saveWithProtobuf(key, value, obj_file_name, obj_cache_file);
      }
      if (env_cache_format_ == "FLATBUFFER") {
        saveWithFlatbuffer(key, value, obj_file_name, obj_cache_file);
      }

    } else {
      std::ifstream cached_file(obj_cache_file.c_str(), std::ios::binary);

      if (env_cache_format_ == "PROTOBUF") {
        verifyProtobufFile(key, obj_file_name, cached_file);
      }
      if (env_cache_format_ == "FLATBUFFER") {
        verifyFlatbufferFile(key, obj_file_name, cached_file);
      }
    }
  }

  void removeObjectCodeCacheFile(const char* filename, size_t file_size) {
    disk_cache_size_ -= file_size;
    disk_cache_files_qty_ = disk_cache_files_qty_ - 1;
    std::string file = splitDirPath(filename, "/").back();
    // std::string uuid_string = file.substr(file.find("-")+1, file.find("."));
    // uuid_string = uuid_string.substr(0, uuid_string.find("."));
    std::string key_string = file.substr(file.find("-") + 1, file.find("."));
    key_string = key_string.substr(0, key_string.find("."));
    cached_files_map_.erase(key_string);

    remove(filename);

    updateCacheInfoFile();
    updateCacheListFile();
  }

  void updateCacheInfoFile() {
    // Reads the disk cache info
    std::string cache_info_filename = "cache.info";
    llvm::SmallString<128> cache_info = cache_dir_;
    llvm::sys::path::append(cache_info, cache_info_filename);

    std::fstream cache_info_file;

    cache_info_file.open(cache_info.c_str(), std::ios::out);

    if (!cache_info_file) {
      ARROW_LOG(DEBUG)
          << "[DEBUG][CACHE-LOG]: Can not find the cache.info file while updating!";
      cache_info_file.close();
    } else {
      cache_info_file << "disk-usage=" << std::to_string(disk_cache_size_) << std::endl;
      cache_info_file << "number-of-files=" << std::to_string(disk_cache_files_qty_)
                      << std::endl;
      cache_info_file.close();
      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Updated cache.info file!";
    }
  }

  void updateCacheListFile() {
    // Reads the disk cache list
    std::string cache_list_filename = "cache.list";
    llvm::SmallString<128> cache_list = cache_dir_;
    llvm::sys::path::append(cache_list, cache_list_filename);

    std::fstream cache_list_file;

    cache_list_file.open(cache_list.c_str(), std::ios::in);

    if (!cache_list_file) {
      ARROW_LOG(DEBUG)
          << "[DEBUG][CACHE-LOG]: Can not find the cache.list file while updating!";
      cache_list_file.close();
    } else {
      std::string line;
      int n_of_lines = 0;

      while (std::getline(cache_list_file, line)) {
        ++n_of_lines;
      }

      for (int i = 0; i < n_of_lines; ++i) {
        std::string filename_and_size;

        cache_list_file >> filename_and_size;

        if (filename_and_size != "") {
          std::string filename = filename_and_size.substr(0, filename_and_size.find("_"));
          std::string size_string =
              filename_and_size.substr(filename_and_size.find("_") + 1);
          std::string key_string =
              filename.substr(filename.find("-") + 1, filename.find("."));

          if (size_string != "") {
            size_t size = std::stoul(size_string);
            std::pair<std::string, size_t> file_pair = std::make_pair(filename, size);
            cached_files_map_[key_string] = file_pair;
          }
        }
      }

      cache_list_file.close();
    }

    cache_list_file.open(cache_list.c_str(), std::ios::out);

    if (!cache_list_file) {
      ARROW_LOG(DEBUG)
          << "[DEBUG][CACHE-LOG]: Can not find the cache.list file while updating!";
      cache_list_file.close();
    } else {
      for (auto& item : cached_files_map_) {
        std::string file_name = item.second.first;
        size_t file_size = item.second.second;
        cache_list_file << file_name << "_" << file_size << std::endl;
      }

      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Updated cache.list file!";
      cache_list_file.close();
    }
  }

  void verifyCacheDir() {
    auto dir_iterator = boost::filesystem::directory_iterator(cache_dir_.c_str());
    size_t file_count = 0;
    size_t size_count = 0;
    for (auto& entry : dir_iterator) {
      auto entry_path = entry.path().string();
      std::string filename = splitDirPath(entry_path, "/").back();
      std::string key_string = filename.substr(filename.find("-") + 1);
      key_string = key_string.substr(0, key_string.find("."));
      auto entry_extension = entry_path.substr(entry_path.find(".") + 1);
      if (entry_extension == "cache") {
        ++file_count;
        size_count += boost::filesystem::file_size(entry_path);
        std::pair<std::string, size_t> file_pair =
            std::make_pair(filename, boost::filesystem::file_size(entry_path));
        cached_files_map_[key_string] = file_pair;
      }
    }

    // Reads the disk cache info
    std::string cache_info_filename = "cache.info";
    llvm::SmallString<128> cache_info = cache_dir_;
    llvm::sys::path::append(cache_info, cache_info_filename);
    std::fstream cache_info_file;
    cache_info_file.open(cache_info.c_str(), std::ios::in);

    if (!cache_info_file) {
      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Can not find the cache.info file!";
      cache_info_file.close();

      // create the cache.info file
      cache_info_file.open(cache_info.c_str(), std::ios::out);
      cache_info_file << "disk-usage=0" << std::endl;
      cache_info_file << "number-of-files=0" << std::endl;
      cache_info_file.close();
    } else {
      std::string disk_usage_str;
      std::string disk_number_of_files_str;

      cache_info_file >> disk_usage_str;
      cache_info_file >> disk_number_of_files_str;

      auto disk_cache_size_string = disk_usage_str.substr(disk_usage_str.find("=") + 1);

      if (disk_cache_size_string == "") {
        disk_cache_size_string = "0";
      }

      auto disk_cache_size_holder = std::stoul(disk_cache_size_string);

      auto disk_cache_files_qty_string =
          disk_number_of_files_str.substr(disk_number_of_files_str.find("=") + 1);

      if (disk_cache_files_qty_string == "") {
        disk_cache_files_qty_string = "0";
      }

      auto disk_cache_files_qty_holder = std::stoul(disk_cache_files_qty_string);

      if (disk_cache_size_holder != size_count ||
          disk_cache_files_qty_holder != file_count) {
        cache_info_file.close();
        cache_info_file.open(cache_info.c_str(), std::ios::out);
        cache_info_file << "disk-usage=" << std::to_string(size_count) << std::endl;
        cache_info_file << "number-of-files=" << std::to_string(file_count) << std::endl;
        cache_info_file.close();
        disk_cache_size_ = size_count;
        disk_cache_files_qty_ = file_count;
      } else {
        disk_cache_size_ = disk_cache_size_holder;
        disk_cache_files_qty_ = disk_cache_files_qty_holder;
        cache_info_file.close();
      }
    }

    // Read cache.list file
    std::string cache_list_filename = "cache.list";
    llvm::SmallString<128> cache_list = cache_dir_;
    llvm::sys::path::append(cache_list, cache_list_filename);
    std::fstream cache_list_file;
    cache_list_file.open(cache_list.c_str(), std::ios::out);

    if (!cache_list_file) {
      ARROW_LOG(DEBUG)
          << "[DEBUG][CACHE-LOG]: Can not find, or create, the cache.list file!";
      cache_list_file.close();
    } else {
      for (auto& item : cached_files_map_) {
        std::string file_name = item.second.first;
        size_t file_size = item.second.second;
        cache_list_file << file_name << "_" << file_size << std::endl;
      }
      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Updated cache.list file!";
      cache_list_file.close();
    }
  }

  std::vector<std::string> splitDirPath(std::string s, std::string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    std::string token;
    std::vector<std::string> res;

    while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
      token = s.substr(pos_start, pos_end - pos_start);
      pos_start = pos_end + delim_len;
      res.push_back(token);
    }

    res.push_back(s.substr(pos_start));
    return res;
  }

  void freeCacheDir(size_t size) {
    ARROW_LOG(DEBUG) << "Entered freeCacheDir...";
    while (disk_space_capactiy_ < disk_cache_size_ + size) {
      ARROW_LOG(DEBUG) << "Capacity: " << std::to_string(disk_space_capactiy_);
      ARROW_LOG(DEBUG) << "Capacity: " << std::to_string(disk_cache_size_ + size);
      ARROW_LOG(DEBUG) << "Entered freeCacheDir while loop...";
      std::pair<llvm::SmallString<128>, size_t> file_pair =
          getLargerFilePathInsideCache();
      llvm::SmallString<128> file = file_pair.first;
      size_t file_size = file_pair.second;

      removeObjectCodeCacheFile(file.c_str(), file_size);
    }
  }

  std::pair<llvm::SmallString<128>, size_t> getLargerFilePathInsideCache() {
    std::string larger_file;
    size_t larger_size = 0;
    for (auto& item : cached_files_map_) {
      auto file_size = item.second.second;
      auto file = item.second.first;
      if (file_size > larger_size) {
        larger_size = file_size;
        larger_file = file;
      }
    }
    llvm::SmallString<128> obj_file = cache_dir_;
    llvm::sys::path::append(obj_file, larger_file);

    std::pair<llvm::SmallString<128>, size_t> file_pair =
        std::make_pair(obj_file, larger_size);
    return file_pair;
  }

  void clearCacheDisk() {
    boost::system::error_code error_code;
    boost::filesystem::remove_all(cache_dir_.c_str(), error_code);

    if (error_code.value() != 0) {
      fprintf(stderr, "Unable to delete cache directory\n");
      return;
    }

    if (!llvm::sys::fs::exists(cache_dir_.str()) &&
        llvm::sys::fs::create_directory(cache_dir_.str())) {
      fprintf(stderr, "Unable to create cache directory\n");
      return;
    }
  }

  void checkDiskSpace() {
    auto disk_info = boost::filesystem::space(cache_dir_.c_str());

    auto disk_space_ten_percent = static_cast<size_t>(round(disk_info.available * 0.10));

    if (disk_space_ten_percent < disk_reserved_space_) {
      disk_reserved_space_ = disk_space_ten_percent;
      disk_space_capactiy_ = std::min(
          disk_space_capactiy_, static_cast<size_t>(round(disk_info.available * 0.90)));
    }

    ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Disk total space capacity: "
                     << disk_info.capacity << " bytes.";
    ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Disk total space available: "
                     << disk_info.available << " bytes.";
    ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Disk space available to cache: "
                     << disk_space_capactiy_ - disk_cache_size_ << " bytes.";
  }

 private:
  map_type map_;
  list_type lru_list_;
  size_t cache_capacity_;
  size_t cache_size_ = 0;
  std::unordered_map<key_type, std::pair<size_t, typename list_type::iterator>, hasher>
      size_map_;
  llvm::SmallString<128> cache_dir_;
  size_t disk_cache_size_ = 0;
  size_t disk_cache_files_qty_ = 0;
  size_t disk_reserved_space_ = 0;
  size_t disk_space_capactiy_ = 0;
  size_t disk_cache_space_available_ = 0;
  std::unordered_map<std::string, std::pair<std::string, size_t>> cached_files_map_;
  flatbuffers::FlatBufferBuilder flatbuffer_builder_;
  std::string env_cache_format_;
};
}  // namespace gandiva

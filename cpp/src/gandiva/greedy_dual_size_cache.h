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

#include <llvm/ADT/SmallString.h>
#include <llvm/Support/MemoryBuffer.h>

#include <boost/filesystem.hpp>
#include <list>
#include <queue>
#include <set>
#include <unordered_map>
#include <utility>

#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "gandiva/flatbuffer/gandiva_cache_file_generated.h"
#include "gandiva/gandiva_cache_file.pb.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/Path.h"

// modified cache to support evict policy using the GreedyDual-Size algorithm.
namespace gandiva {
// Defines a base value object supported on the cache that may contain properties
template <typename ValueType>
class ValueCacheObject {
 public:
  ValueCacheObject(ValueType module, uint64_t cost, size_t size)
      : module(module), cost(cost), size(size) {}
  ValueType module;
  uint64_t cost;
  size_t size;
  bool operator<(const ValueCacheObject& other) const { return cost < other.cost; }
};

class CacheObjectFileMetadata {
 public:
  CacheObjectFileMetadata() = default;

  CacheObjectFileMetadata(std::string name, size_t size, uint64_t cost)
      : name(name), size(size), cost(cost) {}

  ~CacheObjectFileMetadata() = default;

  std::string name;
  size_t size = 0;
  uint64_t cost = 0;
  llvm::SmallString<128> filepath;

  bool operator<(const CacheObjectFileMetadata& other) const { return size < other.size; }

  int8_t IsBiggerThan(const CacheObjectFileMetadata& other) const {
    if (other.size > size) {
      return -1;
    }
    if (size > other.size) {
      return 1;
    }
    return 0;
  }

  bool IsEqual(const CacheObjectFileMetadata& other) {
    if (other.name == name && other.size == size) {
      return true;
    }
    return false;
  }

  std::string ToString() const {
    return name + "-" + std::to_string(size) + "_" + std::to_string(cost);
  }
};

// A particular cache based on the GreedyDual-Size cache which is a generalization of LRU
// which defines costs for each cache values.
// The algorithm associates a cost, C, with each cache value. Initially, when the value
// is brought into cache, C is set to be the cost related to the value (the cost is
// always non-negative). When a replacement needs to be made, the value with the lowest C
// cost is replaced, and then all values reduce their C costs by the minimum value of C
// over all the values already in the cache.
// If a value is accessed, its C value is restored to its initial cost. Thus, the C costs
// of recently accessed values retain a larger portion of the original cost than those of
// values that have not been accessed for a long time. The C costs are reduced as time
// goes and are restored when accessed.

template <class Key, class Value>
class GreedyDualSizeCache {
  // inner class to define the priority item
  class PriorityItem {
   public:
    PriorityItem(uint64_t actual_priority, uint64_t original_priority, Key key)
        : actual_priority(actual_priority),
          original_priority(original_priority),
          cache_key(key) {}
    // this ensure that the items with low priority stays in the beginning of the queue,
    // so it can be the one removed by evict operation
    bool operator<(const PriorityItem& other) const {
      return actual_priority < other.actual_priority;
    }
    uint64_t actual_priority;
    uint64_t original_priority;
    Key cache_key;
  };

 public:
  struct hasher {
    template <typename I>
    std::size_t operator()(const I& i) const {
      return i.Hash();
    }
  };
  // a map from 'key' to a pair of Value and a pointer to the priority value
  using map_type = std::unordered_map<
      Key, std::pair<ValueCacheObject<Value>, typename std::set<PriorityItem>::iterator>,
      hasher>;

  //  explicit GreedyDualSizeCache(size_t capacity) : inflation_(0), capacity_(capacity)
  //  {}
  explicit GreedyDualSizeCache(size_t capacity, size_t disk_capacity, size_t reserved)
      : inflation_(0), capacity_(capacity) {
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

  ~GreedyDualSizeCache() = default;

  size_t size() const { return map_.size(); }

  size_t capacity() const { return capacity_; }

  bool empty() const { return map_.empty(); }

  bool contains(const Key& key) { return map_.find(key) != map_.end(); }

  void insert(const Key& key, const ValueCacheObject<Value>& value) {
    typename map_type::iterator i = map_.find(key);
    // check if element is not in the cache to add it
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full, to evict an item
      // if it is necessary
      if (size() >= capacity_) {
        evict();
      }

      // insert the new item
      auto item =
          priority_set_.insert(PriorityItem(value.cost + inflation_, value.cost, key));
      // save on map the value and the priority item iterator position
      map_.emplace(key, std::make_pair(value, item.first));
    }
  }

  void InsertObjectCode(const Key& key, const ValueCacheObject<Value>& value) {
    typename map_type::iterator i = map_.find(key);

    if (i == map_.end()) {
      // must guarantee that the value.size is smaller than the capacity
      if (value.size < capacity_) {
        // insert item into the cache, but first check if it is full
        if (cache_size_ >= capacity_) {
          // cache is full, evict some items
          evitObjectSafely(value.size);
        }

        if (cache_size_ + value.size >= capacity_) {
          // cache will pass the maximum capacity, evict some items
          evitObjectSafely(value.size);
        }

        // insert the new item into memory
        auto item =
            priority_set_.insert(PriorityItem(value.cost + inflation_, value.cost, key));
        // save on map the value and the priority item iterator position
        map_.emplace(key, std::make_pair(value, item.first));
        cache_size_ += value.size;
      }
    } else {
      // if value is too big for memory cache, save it directly to disk
      CacheObjectFileMetadata fileMetadata(std::to_string(key.Hash()), value.size, value.cost);
      auto key_save = key;
      saveObjectToCacheDir(key_save, value.module, fileMetadata);
    }
  }

  arrow::util::optional<ValueCacheObject<Value>> get(const Key& key) {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);
    if (value_for_key == map_.end()) {
      // value not in cache
      return arrow::util::nullopt;
    }
    PriorityItem item = *value_for_key->second.second;
    // if the value was found on the cache, update its cost (original + inflation)
    if (item.actual_priority != item.original_priority + inflation_) {
      priority_set_.erase(value_for_key->second.second);
      auto iter = priority_set_.insert(PriorityItem(
          item.original_priority + inflation_, item.original_priority, item.cache_key));
      value_for_key->second.second = iter.first;
    }
    return value_for_key->second.first;
  }

  arrow::util::optional<ValueCacheObject<Value>> GetObjectCode(const Key& key) {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);

    CacheObjectFileMetadata fileMetadata;
    fileMetadata.name = std::to_string(key.Hash());
    std::string obj_file_name = fileMetadata.name + ".cache";
    llvm::SmallString<128> obj_cache_file = cache_dir_;
    llvm::sys::path::append(obj_cache_file, obj_file_name);
    fileMetadata.filepath = obj_cache_file;

    if (value_for_key == map_.end() && !llvm::sys::fs::exists(obj_cache_file.str())) {
      // value not in cache
      return arrow::util::nullopt;
    }

    if (value_for_key == map_.end()) {
      if (llvm::sys::fs::exists(obj_cache_file.str())) {
        // This file is in our disk!

        std::shared_ptr<llvm::MemoryBuffer> obj_cache_buffer_shared;
        if (env_cache_format_ == "PROTOBUF") {
          obj_cache_buffer_shared = readCacheFromProtobufFile(fileMetadata, key);
        }

        if (env_cache_format_ == "FLATBUFFER") {
          obj_cache_buffer_shared = readCacheFromFlatbufferFile(fileMetadata, key);
        }

        fileMetadata.size = obj_cache_buffer_shared->getBufferSize();
        auto fileMetadataInSet = findMetadaInMap(fileMetadata);
        if (!fileMetadata.name.empty()) {
          fileMetadata.cost = fileMetadataInSet.cost;
        }
        ValueCacheObject<Value> obj_cache_value(obj_cache_buffer_shared,
                                                fileMetadata.size, fileMetadata.cost);
        reinsertObject(key, obj_cache_value, fileMetadata);
        removeObjectCodeCacheFile(fileMetadata);
        return obj_cache_value;
      }
    }

    PriorityItem item = *value_for_key->second.second;
    // if the value was found on the cache, update its cost (original + inflation)
    if (item.actual_priority != item.original_priority + inflation_) {
      priority_set_.erase(value_for_key->second.second);
      auto iter = priority_set_.insert(PriorityItem(
          item.original_priority + inflation_, item.original_priority, item.cache_key));
      value_for_key->second.second = iter.first;
    }
    return value_for_key->second.first;
  }

  void clear() {
    map_.clear();
    priority_set_.clear();
  }

  size_t GetCacheSize() { return cache_size_; }

  std::string ToString() {
    size_t cache_map_length = map_.size();
    return "Cache has " + std::to_string(cache_map_length) + " items," +
           " with total size of " + std::to_string(cache_size_) + " bytes.";
  }

 private:
  void evict() {
    // TODO: inflation overflow is unlikely to happen but needs to be handled
    //  for correctness.
    // evict item from the beginning of the set. This set is ordered from the
    // lower priority value to the higher priority value.
    typename std::set<PriorityItem>::iterator i = priority_set_.begin();
    // update the inflation cost related to the evicted item
    inflation_ = (*i).actual_priority;
    map_.erase((*i).cache_key);
    priority_set_.erase(i);
  }

  void evictObject() {
    // TODO: inflation overflow is unlikely to happen but needs to be handled
    //  for correctness.
    // evict item from the beginning of the set. This set is ordered from the
    // lower priority value to the higher priority value.
    typename std::set<PriorityItem>::iterator i = priority_set_.begin();
    // update the inflation cost related to the evicted item
    inflation_ = (*i).actual_priority;

    // Create the cache file metadata object, and save the cache to disk
    const Value value = map_.find((*i).cache_key)->second.first.module;
    size_t size_to_decrease = map_.find((*i).cache_key)->second.first.size;
    uint64_t cost = map_.find((*i).cache_key)->second.first.cost;
    CacheObjectFileMetadata fileMetadata(std::to_string((*i).cache_key.Hash()),
                                         size_to_decrease, cost);
    auto key = (*i).cache_key;
    saveObjectToCacheDir(key, value, fileMetadata);

    cache_size_ -= size_to_decrease;
    map_.erase((*i).cache_key);
    priority_set_.erase(i);
  }

  //  void evictObject() {
  //    // evict item from the end of most recently used list
  //    typename list_type::iterator i = --lru_list_.end();
  //    const size_t size_to_decrease = size_map_.find(*i)->second.first;
  //    const Value value = map_.find((*i).cache_key);
  //    saveObjectToCacheDir(*i, value.);
  //    cache_size_ = cache_size_ - size_to_decrease;
  //    map_.erase(*i);
  //    size_map_.erase(*i);
  //    lru_list_.erase(i);
  //  }

  void evitObjectSafely(size_t file_size) {
    while (cache_size_ + file_size >= capacity_) {
      if (cache_size_ == 0) {
        ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG] Cache size is already zero! ";
      }
      evictObject();
    }
  }

  void reinsertObject(const Key& key, const ValueCacheObject<Value>& value,
                      CacheObjectFileMetadata fileMetadata) {
    typename map_type::iterator i = map_.find(key);

    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full
      if (cache_size_ >= capacity_) {
        // cache is full, evict the least recently used item
        evitObjectSafely(fileMetadata.size);
      }

      if (cache_size_ + fileMetadata.size >= capacity_) {
        // cache will pass the maximum capacity, evict the least recently used items
        evitObjectSafely(fileMetadata.size);
      }

      // insert the new item into memory
      auto item =
          priority_set_.insert(PriorityItem(value.cost + inflation_, value.cost, key));
      // save on map the value and the priority item iterator position
      map_.emplace(key, std::make_pair(value, item.first));
      cache_size_ += value.size;

      // Removes the disk file to put it back to memory
      // cached_files_set_.erase(fileMetadata);
      cached_files_map_.erase(key.Hash());
      // Update the cache list and info files
      updateCacheListFile();
      updateCacheInfoFile();
    }
  }

  // ------ New implementation below ------
  //  std::shared_ptr<llvm::MemoryBuffer> readCacheFromFlatbufferFile(
  //      llvm::SmallString<128>& obj_cache_file, std::string obj_file_name, const Key&
  //      key) {
  //    std::ifstream cached_file(obj_cache_file.c_str(), std::ios::binary);
  //    if (!cached_file) {
  //      std::cerr << "Failed to open the flatbuffer file cache." << std::endl;
  //      return nullptr;
  //    }
  //
  //    std::vector<unsigned char> buffer_from_file(
  //        std::istreambuf_iterator<char>(cached_file), {});
  //    cached_file.close();
  //
  //    auto flatbuffer_cache =
  //        gandiva::cache::fbs::GetCache(static_cast<void*>(buffer_from_file.data()));
  //
  //    // Read the obj_code bytes
  //    auto flatbuffer_obj_code_string = flatbuffer_cache->object_code()->str();
  //
  //    auto flatbuffer_schema_string = flatbuffer_cache->schema_exprs()->schema()->str();
  //    auto flatbuffer_exprs = flatbuffer_cache->schema_exprs()->exprs();
  //
  //    std::vector<std::string> flatbuffer_exprs_string;
  //
  //    for (auto expr : *flatbuffer_exprs) {
  //      flatbuffer_exprs_string.push_back(expr->str());
  //    }
  //
  //    // Check if the file is really the same by checking its schema and expressions
  //    // if the file does not has the same schema and expressions, it happened a hash
  //    // collision and the files are not really the same.
  //    bool isReallySame =
  //        key.checkCacheFile(flatbuffer_schema_string, flatbuffer_exprs_string);
  //    if (!isReallySame) {
  //    }
  //
  //    bool status = verifyFlatbufferContent(flatbuffer_cache, obj_file_name, key);
  //
  //    if (status == true) {
  //      auto obj_code_ref = llvm::StringRef(flatbuffer_obj_code_string);
  //      auto ref_id_pb = llvm::StringRef(splitDirPath(obj_cache_file.c_str(),
  //      "/").back());
  //
  //      std::unique_ptr<llvm::MemoryBuffer> obj_cache_buffer =
  //          llvm::MemoryBuffer::getMemBufferCopy(obj_code_ref, ref_id_pb);
  //      return std::move(obj_cache_buffer);
  //    }
  //    return nullptr;
  //  }

  /// Reads from cache file with flatbuffer implementation
  std::shared_ptr<llvm::MemoryBuffer> readCacheFromFlatbufferFile(
      CacheObjectFileMetadata fileMetadata, const Key& key) {
    std::ifstream cached_file(fileMetadata.filepath.c_str(), std::ios::binary);
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

    bool status =
        verifyFlatbufferContent(flatbuffer_cache, fileMetadata.name + ".cache", key);

    if (status) {
      auto obj_code_ref = llvm::StringRef(flatbuffer_obj_code_string);
      auto ref_id_pb =
          llvm::StringRef(splitDirPath(fileMetadata.filepath.c_str(), "/").back());

      std::unique_ptr<llvm::MemoryBuffer> obj_cache_buffer =
          llvm::MemoryBuffer::getMemBufferCopy(obj_code_ref, ref_id_pb);
      return std::move(obj_cache_buffer);
    }
    return nullptr;
  }

  // ------ New implementation below ------
  //  std::shared_ptr<llvm::MemoryBuffer> readCacheFromProtobufFile(
  //      llvm::SmallString<128>& obj_cache_file, std::string obj_file_name, const Key&
  //      key) {
  //    gandiva::cache::proto::SchemaExprsPairAndObjectCode schema_exprs_obj_code;
  //
  //    // Read the protobuf file cache.
  //    std::fstream input(obj_cache_file.c_str(), std::ios::in | std::ios::binary);
  //    if (!schema_exprs_obj_code.ParseFromIstream(&input)) {
  //      std::cerr << "Failed to parse the proto buf file cache." << std::endl;
  //      // return -1;
  //      return nullptr;
  //    }
  //
  //    bool status = verifyProtobufContent(schema_exprs_obj_code, obj_file_name, key);
  //
  //    if (status != false) {
  //      const std::string obj_code_pb(schema_exprs_obj_code.objectcode());
  //
  //      llvm::StringRef ref_id_pb(obj_cache_file.c_str());
  //      llvm::StringRef obj_code_ref(obj_code_pb);
  //
  //      //      auto obj_cache_file_buffer =
  //      //          llvm::MemoryBuffer::getFile(obj_cache_file, -1, true, false);
  //      std::unique_ptr<llvm::MemoryBuffer> obj_cache_buffer =
  //          llvm::MemoryBuffer::getMemBufferCopy(obj_code_ref, ref_id_pb);
  //      // std::shared_ptr<llvm::MemoryBuffer> obj_cache_buffer_shared =
  //      // std::move(obj_cache_buffer);
  //      return std::move(obj_cache_buffer);
  //    }
  //
  //    return nullptr;
  //  }

  /// Reads from cache file with protobuf implementation
  std::shared_ptr<llvm::MemoryBuffer> readCacheFromProtobufFile(
      CacheObjectFileMetadata fileMetadata, const Key& key) {
    gandiva::cache::proto::SchemaExprsPairAndObjectCode schema_exprs_obj_code;

    // Read the protobuf file cache.
    std::fstream input(fileMetadata.filepath.c_str(), std::ios::in | std::ios::binary);
    if (!schema_exprs_obj_code.ParseFromIstream(&input)) {
      std::cerr << "Failed to parse the proto buf file cache." << std::endl;
      return nullptr;
    }

    bool status =
        verifyProtobufContent(schema_exprs_obj_code, fileMetadata.name + ".cache", key);

    if (status) {
      const std::string& obj_code_pb(schema_exprs_obj_code.objectcode());

      llvm::StringRef ref_id_pb(fileMetadata.filepath.c_str());
      llvm::StringRef obj_code_ref(obj_code_pb);
      std::unique_ptr<llvm::MemoryBuffer> obj_cache_buffer =
          llvm::MemoryBuffer::getMemBufferCopy(obj_code_ref, ref_id_pb);
      return std::move(obj_cache_buffer);
    }

    return nullptr;
  }

  // ------ New implementation below ------
  //  void saveWithFlatbuffer(Key& key, const Value value, std::string obj_file_name,
  //                          llvm::SmallString<128> obj_cache_file) {
  //    // Flatbuffer implementation
  //    auto value_ref_string = value->module.getBuffer().str();
  //    int8_t buffer[value_ref_string.length()];
  //    std::copy(value_ref_string.begin(), value_ref_string.end(), buffer);
  //
  //    auto flatbuffer_obj_code = flatbuffer_builder_.CreateString(value_ref_string);
  //    auto flatbuffer_schema = flatbuffer_builder_.CreateString(key.getSchemaString());
  //    auto flatbuffer_exprs =
  //        flatbuffer_builder_.CreateVectorOfStrings(key.getExprsString());
  //
  //    auto flatbuffer_schema_exprs_pair =
  //    gandiva::cache::fbs::CreateSchemaExpressionsPair(
  //        flatbuffer_builder_, flatbuffer_schema, flatbuffer_exprs);
  //    auto flatbuffer_cache = gandiva::cache::fbs::CreateCache(
  //        flatbuffer_builder_, flatbuffer_schema_exprs_pair, flatbuffer_obj_code);
  //
  //    flatbuffer_builder_.Finish(flatbuffer_cache);
  //
  //    uint8_t* pre_file_buffer = flatbuffer_builder_.GetBufferPointer();
  //    int64_t pre_file_buffer_size = flatbuffer_builder_.GetSize();
  //
  //    std::ofstream cache_file(obj_cache_file.c_str(), std::ios::out |
  //    std::ios::binary); cache_file.write(reinterpret_cast<char*>(pre_file_buffer),
  //    pre_file_buffer_size); auto file_size = cache_file.tellp(); cache_file.close();
  //
  //    disk_cache_size_ += file_size;
  //    disk_cache_files_qty_ += 1;
  //
  //    std::pair<std::string, size_t> file_and_size =
  //        std::make_pair(obj_file_name, file_size);
  //    cached_files_map_[std::to_string(key.Hash())] = file_and_size;
  //
  //    CacheObjectFileMetadata fileMetadata(obj_file_name, file_size, 1);
  //    cached_files_set_.insert(fileMetadata);
  //
  //    updateCacheInfoFile();
  //    updateCacheListFile();
  //  }

  /// Saves the cache file using flatbuffer implementation
  void saveWithFlatbuffer(Key& key, const Value value,
                          CacheObjectFileMetadata fileMetadata) {
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

    std::ofstream cache_file(fileMetadata.filepath.c_str(),
                             std::ios::out | std::ios::binary);
    cache_file.write(reinterpret_cast<char*>(pre_file_buffer), pre_file_buffer_size);
    auto file_size = cache_file.tellp();
    cache_file.close();

    disk_cache_size_ += file_size;
    disk_cache_files_qty_ += 1;
    // cached_files_set_.insert(fileMetadata);
    cached_files_map_[key.Hash()] = fileMetadata;

    updateCacheInfoFile();
    updateCacheListFile();
  }

  /// Verifies the content of a cache file that uses flatbuffer implementation
  bool verifyFlatbufferContent(const cache::fbs::Cache* flatbuffer_cache,
                               std::string obj_file_name, const Key& key) {
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

  /// Verifies the content of a cache file that uses flatbuffer implementation
  void verifyFlatbufferFile(Key& key, std::string obj_file_name,
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

  // ------ New implementation below ------
  //  void saveWithProtobuf(Key& key, const Value value, std::string obj_file_name,
  //                        llvm::SmallString<128> obj_cache_file) {
  //    gandiva::cache::proto::SchemaExpressionsPair schema_exprs_pair;
  //    gandiva::cache::proto::SchemaExprsPairAndObjectCode schema_exprs_pair_obj_code;
  //
  //    std::vector<std::string> exprs = key.getExprsString();
  //
  //    for (auto& expr : exprs) {
  //      schema_exprs_pair.add_expressions(expr);
  //    }
  //
  //    schema_exprs_pair.set_schema(key.getSchemaString());
  //
  //    schema_exprs_pair_obj_code.set_allocated_schemaexpressionpair(&schema_exprs_pair);
  //
  //    schema_exprs_pair_obj_code.set_objectcode(value->module.getBuffer().str());
  //
  //    std::ofstream new_cache_file;
  //
  //    new_cache_file.open(obj_cache_file.c_str(), std::ios::out | std::ios::binary);
  //
  //    schema_exprs_pair_obj_code.SerializeToOstream(&new_cache_file);
  //
  //    auto file_size = new_cache_file.tellp();
  //
  //    new_cache_file.close();
  //
  //    // Prevents the allocated schema_exprs_pair to be persisted across runs.
  //    schema_exprs_pair_obj_code.release_schemaexpressionpair();
  //
  //    disk_cache_size_ += file_size;
  //    disk_cache_files_qty_ += 1;
  //
  //    std::pair<std::string, size_t> file_and_size =
  //        std::make_pair(obj_file_name, file_size);
  //    cached_files_map_[std::to_string(key.Hash())] = file_and_size;
  //
  //    CacheObjectFileMetadata fileMetadata(obj_file_name, file_size, 1);
  //    cached_files_set_.insert(fileMetadata);
  //
  //    updateCacheInfoFile();
  //    updateCacheListFile();
  //  }

  /// Saves the cache file using protobuf implementation
  void saveWithProtobuf(Key& key, const Value value,
                        CacheObjectFileMetadata fileMetadata) {
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

    new_cache_file.open(fileMetadata.filepath.c_str(), std::ios::out | std::ios::binary);

    schema_exprs_pair_obj_code.SerializeToOstream(&new_cache_file);

    auto file_size = new_cache_file.tellp();

    new_cache_file.close();

    // Prevents the allocated schema_exprs_pair to be persisted across runs.
    schema_exprs_pair_obj_code.release_schemaexpressionpair();

    disk_cache_size_ += file_size;
    disk_cache_files_qty_ += 1;
    // cached_files_set_.insert(fileMetadata);
    cached_files_map_[key.Hash()] = fileMetadata;

    updateCacheInfoFile();
    updateCacheListFile();
  }

  /// Verifies the content of a cache file that uses protobuf implementation
  bool verifyProtobufContent(
      gandiva::cache::proto::SchemaExprsPairAndObjectCode& schema_exprs_pair_obj_code,
      std::string obj_file_name, const Key& key) {
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

  /// Verifies the cache file that uses protobuf implementation
  void verifyProtobufFile(Key& key, std::string obj_file_name,
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

  // ------ New implementation below ------
  //  void saveObjectToCacheDir(Key& key, const Value value) {
  //    std::string obj_file_name = std::to_string(key.Hash()) + ".cache";
  //
  //    llvm::SmallString<128> obj_cache_file = cache_dir_;
  //    llvm::sys::path::append(obj_cache_file, obj_file_name);
  //
  //    size_t new_cache_size = value->getBufferSize() + disk_cache_size_;
  //    if (value->getBufferSize() >= disk_space_capactiy_) {
  //      ARROW_LOG(DEBUG) << "The cache file is to big!";
  //      return;
  //    }
  //
  //    if (new_cache_size >= disk_space_capactiy_) {
  //      ARROW_LOG(DEBUG) << "Cache directory is full, it will be freed some space!";
  //      freeCacheDir(value->getBufferSize());
  //    }
  //
  //    if (!llvm::sys::fs::exists(cache_dir_.str()) &&
  //        llvm::sys::fs::create_directory(cache_dir_.str())) {
  //      fprintf(stderr, "Unable to create cache directory\n");
  //      return;
  //    }
  //
  //    if (!llvm::sys::fs::exists(obj_cache_file.str())) {
  //      // This file isn't in our disk, so we need to save it to the disk!
  //      // std::error_code ErrStr;
  //      // llvm::raw_fd_ostream CachedObjectFile(obj_cache_file.c_str(), ErrStr);
  //      // CachedObjectFile << value->getBuffer();
  //      // disk_cache_size_ +=  value->getBufferSize();
  //      // disk_cache_files_qty_ += 1;
  //      // CachedObjectFile.close();
  //
  //      if (env_cache_format_ == "PROTOBUF") {
  //        saveWithProtobuf(key, value, obj_file_name, obj_cache_file);
  //      }
  //      if (env_cache_format_ == "FLATBUFFER") {
  //        saveWithFlatbuffer(key, value, obj_file_name, obj_cache_file);
  //      }
  //
  //    } else {
  //      std::ifstream cached_file(obj_cache_file.c_str(), std::ios::binary);
  //
  //      if (env_cache_format_ == "PROTOBUF") {
  //        verifyProtobufFile(key, obj_file_name, cached_file);
  //      }
  //      if (env_cache_format_ == "FLATBUFFER") {
  //        verifyFlatbufferFile(key, obj_file_name, cached_file);
  //      }
  //    }
  //  }

  // ------ New implementation below ------
  //  void saveObjectToCacheDir(Key& key, const Value value,
  //                            const CacheObjectFileMetadata& fileMetadata) {
  //    std::string obj_file_name = fileMetadata.name + ".cache";
  //
  //    llvm::SmallString<128> obj_cache_file = cache_dir_;
  //    llvm::sys::path::append(obj_cache_file, obj_file_name);
  //
  //    size_t new_cache_size = fileMetadata.size + disk_cache_size_;
  //    if (fileMetadata.size >= disk_space_capactiy_) {
  //      ARROW_LOG(DEBUG) << "The cache file is to big to cache on disk.";
  //      return;
  //    }
  //
  //    if (new_cache_size >= disk_space_capactiy_) {
  //      ARROW_LOG(DEBUG) << "Cache directory is full, it will be freed some space!";
  //      freeCacheDir(fileMetadata.size);
  //    }
  //
  //    if (!llvm::sys::fs::exists(cache_dir_.str()) &&
  //        llvm::sys::fs::create_directory(cache_dir_.str())) {
  //      fprintf(stderr, "Unable to create cache directory\n");
  //      return;
  //    }
  //
  //    if (!llvm::sys::fs::exists(obj_cache_file.str())) {
  //      // This file isn't in our disk, so we need to save it to the disk!
  //      // std::error_code ErrStr;
  //      // llvm::raw_fd_ostream CachedObjectFile(obj_cache_file.c_str(), ErrStr);
  //      // CachedObjectFile << value->getBuffer();
  //      // disk_cache_size_ +=  value->getBufferSize();
  //      // disk_cache_files_qty_ += 1;
  //      // CachedObjectFile.close();
  //
  //      if (env_cache_format_ == "PROTOBUF") {
  //        saveWithProtobuf(key, value, obj_file_name, obj_cache_file);
  //      }
  //      if (env_cache_format_ == "FLATBUFFER") {
  //        saveWithFlatbuffer(key, value, obj_file_name, obj_cache_file);
  //      }
  //
  //    } else {
  //      std::ifstream cached_file(obj_cache_file.c_str(), std::ios::binary);
  //
  //      if (env_cache_format_ == "PROTOBUF") {
  //        verifyProtobufFile(key, obj_file_name, cached_file);
  //      }
  //      if (env_cache_format_ == "FLATBUFFER") {
  //        verifyFlatbufferFile(key, obj_file_name, cached_file);
  //      }
  //    }
  //  }

  /// Saves the cache object code to disk
  void saveObjectToCacheDir(Key& key, const Value value,
                            CacheObjectFileMetadata fileMetadata) {
    std::string obj_file_name = fileMetadata.name + ".cache";

    llvm::SmallString<128> obj_cache_file = cache_dir_;
    llvm::sys::path::append(obj_cache_file, obj_file_name);
    fileMetadata.filepath = obj_cache_file;

    size_t new_cache_size = fileMetadata.size + disk_cache_size_;
    if (fileMetadata.size >= disk_space_capactiy_) {
      ARROW_LOG(DEBUG) << "The cache file is to big to cache on disk.";
      return;
    }

    if (new_cache_size >= disk_space_capactiy_) {
      ARROW_LOG(DEBUG) << "Cache directory is full, it will be freed some space!";
      freeCacheDir(fileMetadata.size);
    }

    if (!llvm::sys::fs::exists(cache_dir_.str()) &&
        llvm::sys::fs::create_directory(cache_dir_.str())) {
      fprintf(stderr, "Unable to create cache directory\n");
      return;
    }

    if (!llvm::sys::fs::exists(obj_cache_file.str())) {
      if (env_cache_format_ == "PROTOBUF") {
        saveWithProtobuf(key, value, fileMetadata);
      }
      if (env_cache_format_ == "FLATBUFFER") {
        saveWithFlatbuffer(key, value, fileMetadata);
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

  //  void removeObjectCodeCacheFile(const char* filename, size_t file_size) {
  //    disk_cache_size_ -= file_size;
  //    disk_cache_files_qty_ = disk_cache_files_qty_ - 1;
  //    std::string file = splitDirPath(filename, "/").back();
  //    std::string key_string = file.substr(file.find('-') + 1, file.find('.'));
  //    key_string = key_string.substr(0, key_string.find("."));
  //    //cached_files_map_.erase(key_string);
  //    cached_files_set_.find()
  //
  //    remove(filename);
  //
  //    updateCacheInfoFile();
  //    updateCacheListFile();
  //  }

  /// Removes the object code cache file from disk
  void removeObjectCodeCacheFile(const CacheObjectFileMetadata& fileMetadata) {
    disk_cache_size_ -= fileMetadata.size;
    disk_cache_files_qty_ = disk_cache_files_qty_ - 1;
    // cached_files_set_.erase(fileMetadata);
    cached_files_map_.erase(std::stoul(fileMetadata.name));
    remove(fileMetadata.filepath.str().str().c_str());
    updateCacheInfoFile();
    updateCacheListFile();
  }

  /// Updates the cache.info file with the new metadata about the cache
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

  // ------ New implementation below ------
  //  void updateCacheListFile() {
  //    // Reads the disk cache list
  //    std::string cache_list_filename = "cache.list";
  //    llvm::SmallString<128> cache_list = cache_dir_;
  //    llvm::sys::path::append(cache_list, cache_list_filename);
  //
  //    std::fstream cache_list_file;
  //
  //    cache_list_file.open(cache_list.c_str(), std::ios::in);
  //
  //    if (!cache_list_file) {
  //      ARROW_LOG(DEBUG)
  //          << "[DEBUG][CACHE-LOG]: Can not find the cache.list file while updating
  //          it!";
  //      cache_list_file.close();
  //    } else {
  //      std::string line;
  //      int n_of_lines = 0;
  //
  //      while (std::getline(cache_list_file, line)) {
  //        ++n_of_lines;
  //      }
  //
  //      for (int i = 0; i < n_of_lines; ++i) {
  //        std::string filename_and_size;
  //
  //        cache_list_file >> filename_and_size;
  //
  //        if (filename_and_size != "") {
  //          std::string filename = filename_and_size.substr(0,
  //          filename_and_size.find("_")); std::string size_string =
  //              filename_and_size.substr(filename_and_size.find("_") + 1);
  //          std::string key_string =
  //              filename.substr(filename.find("-") + 1, filename.find("."));
  //
  //          if (size_string != "") {
  //            size_t size = std::stoul(size_string);
  //            std::pair<std::string, size_t> file_pair = std::make_pair(filename, size);
  //            cached_files_map_[key_string] = file_pair;
  //          }
  //        }
  //      }
  //
  //      cache_list_file.close();
  //    }
  //
  //    cache_list_file.open(cache_list.c_str(), std::ios::out);
  //
  //    if (!cache_list_file) {
  //      ARROW_LOG(DEBUG)
  //          << "[DEBUG][CACHE-LOG]: Can not find the cache.list file while updating!";
  //      cache_list_file.close();
  //    } else {
  //      for (auto& item : cached_files_map_) {
  //        std::string file_name = item.second.first;
  //        size_t file_size = item.second.second;
  //        cache_list_file << file_name << "_" << file_size << std::endl;
  //      }
  //
  //      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Updated cache.list file!";
  //      cache_list_file.close();
  //    }
  //  }

  /// Updates the cache.list file with the new list of cached files
  void updateCacheListFile() {
    // Reads the disk cache list
    std::string cache_list_filename = "cache.list";
    llvm::SmallString<128> cache_list = cache_dir_;
    llvm::sys::path::append(cache_list, cache_list_filename);

    std::fstream cache_list_file;

    cache_list_file.open(cache_list.c_str(), std::ios::out);

    if (!cache_list_file) {
      ARROW_LOG(DEBUG)
          << "[DEBUG][CACHE-LOG]: Can not find the cache.list file while reading it!";
      cache_list_file.close();
    } else {
      for (auto& item : cached_files_map_) {
        cache_list_file << item.second.ToString() << std::endl;
      }
      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Read cache.list file!";
      cache_list_file.close();
    }
  }

  // ------ New implementation below ------
  //  void verifyCacheDir() {
  //    auto dir_iterator = boost::filesystem::directory_iterator(cache_dir_.c_str());
  //    size_t file_count = 0;
  //    size_t size_count = 0;
  //    for (auto& entry : dir_iterator) {
  //      auto entry_path = entry.path().string();
  //      std::string filename = splitDirPath(entry_path, "/").back();
  //      std::string key_string = filename.substr(filename.find("-") + 1);
  //      key_string = key_string.substr(0, key_string.find("."));
  //      auto entry_extension = entry_path.substr(entry_path.find(".") + 1);
  //      if (entry_extension == "cache") {
  //        ++file_count;
  //        size_count += boost::filesystem::file_size(entry_path);
  //        std::pair<std::string, size_t> file_pair =
  //            std::make_pair(filename, boost::filesystem::file_size(entry_path));
  //        cached_files_map_[key_string] = file_pair;
  //      }
  //    }
  //
  //    // Reads the disk cache info
  //    std::string cache_info_filename = "cache.info";
  //    llvm::SmallString<128> cache_info = cache_dir_;
  //    llvm::sys::path::append(cache_info, cache_info_filename);
  //    std::fstream cache_info_file;
  //    cache_info_file.open(cache_info.c_str(), std::ios::in);
  //
  //    if (!cache_info_file) {
  //      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Can not find the cache.info file!";
  //      cache_info_file.close();
  //
  //      // create the cache.info file
  //      cache_info_file.open(cache_info.c_str(), std::ios::out);
  //      cache_info_file << "disk-usage=0" << std::endl;
  //      cache_info_file << "number-of-files=0" << std::endl;
  //      cache_info_file.close();
  //    } else {
  //      std::string disk_usage_str;
  //      std::string disk_number_of_files_str;
  //
  //      cache_info_file >> disk_usage_str;
  //      cache_info_file >> disk_number_of_files_str;
  //
  //      auto disk_cache_size_string = disk_usage_str.substr(disk_usage_str.find("=") +
  //      1);
  //
  //      if (disk_cache_size_string == "") {
  //        disk_cache_size_string = "0";
  //      }
  //
  //      auto disk_cache_size_holder = std::stoul(disk_cache_size_string);
  //
  //      auto disk_cache_files_qty_string =
  //          disk_number_of_files_str.substr(disk_number_of_files_str.find("=") + 1);
  //
  //      if (disk_cache_files_qty_string == "") {
  //        disk_cache_files_qty_string = "0";
  //      }
  //
  //      auto disk_cache_files_qty_holder = std::stoul(disk_cache_files_qty_string);
  //
  //      if (disk_cache_size_holder != size_count ||
  //          disk_cache_files_qty_holder != file_count) {
  //        cache_info_file.close();
  //        cache_info_file.open(cache_info.c_str(), std::ios::out);
  //        cache_info_file << "disk-usage=" << std::to_string(size_count) << std::endl;
  //        cache_info_file << "number-of-files=" << std::to_string(file_count) <<
  //        std::endl; cache_info_file.close(); disk_cache_size_ = size_count;
  //        disk_cache_files_qty_ = file_count;
  //      } else {
  //        disk_cache_size_ = disk_cache_size_holder;
  //        disk_cache_files_qty_ = disk_cache_files_qty_holder;
  //        cache_info_file.close();
  //      }
  //    }
  //
  //    // Read cache.list file
  //    std::string cache_list_filename = "cache.list";
  //    llvm::SmallString<128> cache_list = cache_dir_;
  //    llvm::sys::path::append(cache_list, cache_list_filename);
  //    std::fstream cache_list_file;
  //    cache_list_file.open(cache_list.c_str(), std::ios::out);
  //
  //    if (!cache_list_file) {
  //      ARROW_LOG(DEBUG)
  //          << "[DEBUG][CACHE-LOG]: Can not find, or create, the cache.list file!";
  //      cache_list_file.close();
  //    } else {
  //      for (auto& item : cached_files_map_) {
  //        std::string file_name = item.second.first;
  //        size_t file_size = item.second.second;
  //        cache_list_file << file_name << "_" << file_size << std::endl;
  //      }
  //      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Updated cache.list file!";
  //      cache_list_file.close();
  //    }
  //  }

  /// Verifies cache directory.
  ///
  /// It will read all the files on cache dir, update internal data structures and also
  /// update the cache.info and cache.list files.
  void verifyCacheDir() {
    boost::filesystem::create_directory(cache_dir_.c_str());
    auto dir_iterator = boost::filesystem::directory_iterator(cache_dir_.c_str());
    size_t file_count = 0;
    size_t size_count = 0;
    std::set<std::string> set_file_name;

    // Get the amount of cache files and its names.
    for (auto& entry : dir_iterator) {
      auto entry_path = entry.path().string();
      llvm::SmallString<128> filepath((llvm::StringRef(entry_path)));
      std::string filename = splitDirPath(entry_path, "/").back();
      std::string file_extension = filename.substr(filename.find('.') + 1);
      if (file_extension == "cache") {
        std::string name = filename.substr(0, filename.find('.'));
        set_file_name.insert(name);
        ++file_count;
        size_count += boost::filesystem::file_size(entry_path);
      }
    }

    // Reads the disk cache info and update it.
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
      cache_info_file << "disk-usage=" + std::to_string(size_count) << std::endl;
      cache_info_file << "number-of-files=" + std::to_string(file_count) << std::endl;
      cache_info_file.close();
      disk_cache_size_ = size_count;
      disk_cache_files_qty_ = file_count;
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
      //      for (auto& item : cached_files_map_) {
      //        cache_list_file << item.second.ToString() << std::endl;
      //      }
      std::string line;
      while (std::getline(cache_list_file, line)) {
        cache_list_file >> line;
        if (!line.empty()) {
          std::string name = line.substr(0, line.find('-'));
          size_t size = std::stoul(line.substr(line.find('-') + 1, line.find('_')));
          uint64_t cost = std::stoul(line.substr(line.find('_') + 1));
          CacheObjectFileMetadata fileMetadata(name, size, cost);
          cached_files_map_[std::stoul(name)] = fileMetadata;
        }
      }
      ARROW_LOG(DEBUG) << "[DEBUG][CACHE-LOG]: Read cache.list file!";
      cache_list_file.close();
    }
  }

  /// Splits the cache directory path
  std::vector<std::string> splitDirPath(const std::string& s,
                                        const std::string& delimiter) {
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

  /// Finds a given file metadata in the sets in memory
  CacheObjectFileMetadata findMetadaInMap(CacheObjectFileMetadata metadataToFind) {
    CacheObjectFileMetadata metadataToReturn;
    for (auto& item : cached_files_map_) {
      if (metadataToFind.IsEqual(item.second)) {
        metadataToReturn = item.second;
      }
    }
    return metadataToReturn;
  }

  // ------ New implementation below ------
  //  void freeCacheDir(size_t size) {
  //    ARROW_LOG(DEBUG) << "Entered freeCacheDir...";
  //    while (disk_space_capactiy_ < disk_cache_size_ + size) {
  //      ARROW_LOG(DEBUG) << "Capacity: " << std::to_string(disk_space_capactiy_);
  //      ARROW_LOG(DEBUG) << "Cache size + file size: "
  //                       << std::to_string(disk_cache_size_ + size);
  //      ARROW_LOG(DEBUG) << "Entered freeCacheDir while loop...";
  //      std::pair<llvm::SmallString<128>, size_t> file_pair =
  //          getLargerFilePathInsideCache();
  //      llvm::SmallString<128> file = file_pair.first;
  //      size_t file_size = file_pair.second;
  //
  //      removeObjectCodeCacheFile(file.c_str(), file_size);
  //    }
  //  }

  /// Frees cache disk by removing the larger cached file
  void freeCacheDir(size_t size) {
    ARROW_LOG(DEBUG) << "Entered freeCacheDir...";
    while (disk_space_capactiy_ < disk_cache_size_ + size) {
      ARROW_LOG(DEBUG) << "Capacity: " << std::to_string(disk_space_capactiy_);
      ARROW_LOG(DEBUG) << "Cache size + file size: "
                       << std::to_string(disk_cache_size_ + size);
      ARROW_LOG(DEBUG) << "Entered freeCacheDir while loop...";
      CacheObjectFileMetadata file_to_remove = getLargerFileInsideCache();

      removeObjectCodeCacheFile(file_to_remove);
    }
  }

  // ------ New implementation below ------
  //  std::pair<llvm::SmallString<128>, size_t> getLargerFilePathInsideCache() {
  //    std::string larger_file;
  //    size_t larger_size = 0;
  //    for (auto& item : cached_files_map_) {
  //      auto file_size = item.second.second;
  //      auto file = item.second.first;
  //      if (file_size > larger_size) {
  //        larger_size = file_size;
  //        larger_file = file;
  //      }
  //    }
  //    llvm::SmallString<128> obj_file = cache_dir_;
  //    llvm::sys::path::append(obj_file, larger_file);
  //
  //    std::pair<llvm::SmallString<128>, size_t> file_pair =
  //        std::make_pair(obj_file, larger_size);
  //    return file_pair;
  //  }

  /// Gets the larger cached file
  CacheObjectFileMetadata getLargerFileInsideCache() {
    CacheObjectFileMetadata larger_file;
    size_t larger_size = 0;
    for (auto& item : cached_files_map_) {
      auto file_size = item.second.size;
      auto file = item.second;
      if (file_size > larger_size) {
        larger_size = file_size;
        larger_file = file;
      }
    }
    return larger_file;
  }

  /// Clears the cache directory
  void clearCacheDisk() {
    boost::system::error_code error_code;
    boost::filesystem::remove_all(cache_dir_.c_str(), error_code);

    if (error_code.value() != 0) {
      fprintf(stderr, "Unable to delete cache directory\n");
      return;
    }

    if (!llvm::sys::fs::exists(cache_dir_.str()) &&
        llvm::sys::fs::create_directory(cache_dir_.str())) {
      fprintf(stderr, "Unable to recreate cache directory\n");
      return;
    }
  }

  /// Checks the local disk
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

  map_type map_;
  std::set<PriorityItem> priority_set_;
  uint64_t inflation_;
  size_t capacity_;
  size_t cache_size_ = 0;
  llvm::SmallString<128> cache_dir_;
  size_t disk_cache_size_ = 0;
  size_t disk_cache_files_qty_ = 0;
  size_t disk_reserved_space_ = 0;
  size_t disk_space_capactiy_ = 0;
  size_t disk_cache_space_available_ = 0;
  std::string env_cache_format_;
  std::unordered_map<size_t, CacheObjectFileMetadata> cached_files_map_;
  std::set<CacheObjectFileMetadata> cached_files_set_;
  flatbuffers::FlatBufferBuilder flatbuffer_builder_;
};
}  // namespace gandiva

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

#include <functional>
#include <unordered_map>
#include <utility>

#include "arrow/util/mutex.h"

namespace arrow {
namespace util {

template <typename V>
class ConcurrentMap {
 public:
  void Insert(const std::string& key, const V& value) {
    auto lock = mutex_.Lock();
    map_.insert({key, value});
  }

  void Assign(const std::string& key, const V& value) {
    auto lock = mutex_.Lock();
    map_[key] = value;
  }

  V AssignIfNotExist(const std::string& key, std::function<V()> compute_value_func) {
    auto lock = mutex_.Lock();
    auto it = map_.find(key);
    if (it == map_.end()) {
      map_.insert({key, compute_value_func()});
    }
    return map_.at(key);
  }

  void Erase(const std::string& key) {
    auto lock = mutex_.Lock();
    map_.erase(key);
  }

  void Clear() {
    auto lock = mutex_.Lock();
    map_.clear();
  }

  std::pair<bool, V> Find(const std::string& key) {
    auto lock = mutex_.Lock();
    auto it = map_.find(key);
    if (it != map_.end()) {
      return std::make_pair(true, it->second);
    }
    return std::make_pair(false, V());
  }

  size_t size() {
    auto lock = mutex_.Lock();
    return map_.size();
  }

 private:
  std::unordered_map<std::string, V> map_;
  arrow::util::Mutex mutex_;
};

}  // namespace util
}  // namespace arrow

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 */

#ifndef JNI_ID_TO_MODULE_MAP_H
#define JNI_ID_TO_MODULE_MAP_H

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "arrow/util/macros.h"

namespace arrow {
namespace jni {

/**
 * An utility class that map module id to module pointers.
 * @tparam Holder class of the object to hold.
 */
template <typename Holder>
class ConcurrentMap {
 public:
  ConcurrentMap() : module_id_(init_module_id_) {}

  jlong Insert(Holder holder) {
    std::lock_guard<std::mutex> lock(mtx_);
    jlong result = module_id_++;
    map_.insert(std::pair<jlong, Holder>(result, holder));
    return result;
  }

  void Erase(jlong module_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    map_.erase(module_id);
  }

  Holder Lookup(jlong module_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = map_.find(module_id);
    if (it != map_.end()) {
      return it->second;
    }
    return NULLPTR;
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(mtx_);
    map_.clear();
  }

 private:
  // Initialize the module id starting value to a number greater than zero
  // to allow for easier debugging of uninitialized java variables.
  static constexpr int init_module_id_ = 4;

  int64_t module_id_;
  std::mutex mtx_;
  // map from module ids returned to Java and module pointers
  std::unordered_map<jlong, Holder> map_;
};

}  // namespace jni
}  // namespace arrow

#endif  // JNI_ID_TO_MODULE_MAP_H

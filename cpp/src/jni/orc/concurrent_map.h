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
class concurrentMap {
 public:
  concurrentMap() : module_id_(kInitModuleId) {}

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
    Holder result = NULLPTR;
    try {
      result = map_.at(module_id);
    } catch (const std::out_of_range& e) {
    }
    if (result != NULLPTR) {
      return result;
    }
    std::lock_guard<std::mutex> lock(mtx_);
    try {
      result = map_.at(module_id);
    } catch (const std::out_of_range& e) {
    }
    return result;
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(mtx_);
    map_.clear();
  }

 private:
  // starting value of the module_id.
  static constexpr int kInitModuleId = 4;

  int64_t module_id_;
  std::mutex mtx_;
  // map from module ids returned to Java and module pointers
  std::unordered_map<jlong, Holder> map_;
};

}  // namespace jni
}  // namespace arrow

#endif  // JNI_ID_TO_MODULE_MAP_H

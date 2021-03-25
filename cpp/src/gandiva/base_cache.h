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

#include <list>
#include <unordered_map>
#include <utility>

#include "arrow/util/optional.h"

namespace gandiva {

template <class Key, class Value>
class BaseCache {
 public:
  explicit BaseCache<Key, Value>(size_t capacity) : cache_capacity_(capacity) {};

  BaseCache<Key, Value>() = default;

  size_t size();

  size_t capacity();

  bool empty();

  bool contains(const Key& key);

  void insert(const Key& key, const Value& value);

  void insert(const Key& key, const Value& value, const u_long value_to_order);

  arrow::util::optional<Value> get(const Key& key);

  void clear();

 private:
  void evict();

 protected:
  size_t cache_capacity_;
 };
}  // namespace gandiva

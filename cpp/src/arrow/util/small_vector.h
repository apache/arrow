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

#include <initializer_list>
#include <utility>
#include <vector>

#include "arrow/util/variant.h"

namespace arrow {
namespace util {

/// A minimal vector-like container which holds up to a single element without allocation.
template <typename T>
class small_vector {
 public:
  using value_type = T;
  using iterator = T*;
  using const_iterator = const T*;

  small_vector() = default;

  small_vector(std::initializer_list<value_type> list) {
    if (list.size() == 1) {
      storage_ = *list.begin();
    } else {
      set_vector(list);
    }
  }

  explicit small_vector(std::vector<value_type> vector) {
    if (vector.size() == 1) {
      storage_ = std::move(vector[0]);
    } else {
      set_vector(std::move(vector));
    }
  }

  size_t size() const {
    if (holds_alternative<value_type>(storage_)) {
      return 1;
    }
    return get_vector().size();
  }

  bool empty() const { return size() == 0; }

  void clear() { set_vector({}); }

  void push_back(value_type element) {
    switch (size()) {
      case 0:
        storage_ = std::move(element);
        return;
      case 1:
        set_vector({std::move(get<value_type>(storage_)), std::move(element)});
        return;
      default:
        get_vector().push_back(std::move(element));
        return;
    }
  }

  template <typename... Args>
  void emplace_back(Args&&... args) {
    push_back(value_type(std::forward<Args>(args)...));
  }

  value_type* data() {
    return size() != 1 ? get_vector().data() : &get<value_type>(storage_);
  }

  const value_type* data() const {
    return size() != 1 ? get_vector().data() : &get<value_type>(storage_);
  }

  value_type& operator[](size_t i) { return data()[i]; }
  const value_type& operator[](size_t i) const { return data()[i]; }

  iterator begin() { return data(); }
  iterator end() { return data() + size(); }

  const_iterator begin() const { return data(); }
  const_iterator end() const { return data() + size(); }

 private:
  std::vector<value_type>& get_vector() { return get<std::vector<value_type>>(storage_); }

  const std::vector<value_type>& get_vector() const {
    return get<std::vector<value_type>>(storage_);
  }

  void set_vector(std::vector<value_type> v) { storage_ = std::move(v); }

  variant<std::vector<value_type>, value_type> storage_;
};

}  // namespace util
}  // namespace arrow

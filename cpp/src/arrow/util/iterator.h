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

#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/status.h"

namespace arrow {

/// \brief A generic Iterator that can return errors
template <typename T>
class Iterator {
 public:
  static_assert(std::is_assignable<T, std::nullptr_t>::value,
                "nullptr is used to signal completion");

  virtual ~Iterator() = default;

  /// \brief Return the next element of the sequence, nullptr when the
  /// iteration is completed
  virtual Status Next(T* out) = 0;
  virtual bool Done() = 0;
};

template <typename T>
class VectorIterator : public Iterator<T> {
 public:
  explicit VectorIterator(std::vector<T> v) : elements_(std::move(v)) {}

  Status Next(T* out) override {
    *out = i_ == elements_.size() ? nullptr : elements_[i_++];
    return Status::OK();
  }

 private:
  std::vector<T> elements_;
  size_t i_ = 0;
};

template <typename T>
VectorIterator<T> MakeIterator(std::vector<T> v) {
  return VectorIterator<T>(std::move(v));
}

}  // namespace arrow

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

#include <string>
#include <unordered_set>

#include "arrow/util/hashing.h"
#include "gandiva/arrow.h"
#include "gandiva/decimal_scalar.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

/// Function Holder for IN Expressions
template <typename Type>
class InHolder {
 public:
  explicit InHolder(const std::unordered_set<Type>& values) {
    values_.max_load_factor(0.25f);
    for (auto& value : values) {
      values_.insert(value);
    }
  }

  bool HasValue(Type value) const { return values_.count(value) == 1; }

 private:
  std::unordered_set<Type> values_;
};

template <>
class InHolder<gandiva::DecimalScalar128> {
 public:
  explicit InHolder(const std::unordered_set<gandiva::DecimalScalar128>& values) {
    values_.max_load_factor(0.25f);
    for (auto& value : values) {
      values_.insert(value);
    }
  }

  bool HasValue(gandiva::DecimalScalar128 value) const {
    return values_.count(value) == 1;
  }

 private:
  std::unordered_set<gandiva::DecimalScalar128> values_;
};

template <>
class InHolder<std::string> {
 public:
  explicit InHolder(std::unordered_set<std::string> values) : values_(std::move(values)) {
    values_lookup_.max_load_factor(0.25f);
    for (const std::string& value : values_) {
      values_lookup_.emplace(value);
    }
  }

  bool HasValue(std::string_view value) const { return values_lookup_.count(value) == 1; }

 private:
  struct string_view_hash {
   public:
    std::size_t operator()(std::string_view v) const {
      return arrow::internal::ComputeStringHash<0>(v.data(), v.length());
    }
  };

  std::unordered_set<std::string_view, string_view_hash> values_lookup_;
  const std::unordered_set<std::string> values_;
};

}  // namespace gandiva

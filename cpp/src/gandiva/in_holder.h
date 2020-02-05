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

#include "gandiva/arrow.h"
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

}  // namespace gandiva

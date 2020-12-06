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

#include <algorithm>
#include <utility>
#include <vector>

#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

template <typename T>
std::vector<T> DeleteVectorElement(const std::vector<T>& values, size_t index) {
  DCHECK(!values.empty());
  DCHECK_LT(index, values.size());
  std::vector<T> out;
  out.reserve(values.size() - 1);
  for (size_t i = 0; i < index; ++i) {
    out.push_back(values[i]);
  }
  for (size_t i = index + 1; i < values.size(); ++i) {
    out.push_back(values[i]);
  }
  return out;
}

template <typename T>
std::vector<T> AddVectorElement(const std::vector<T>& values, size_t index,
                                T new_element) {
  DCHECK_LE(index, values.size());
  std::vector<T> out;
  out.reserve(values.size() + 1);
  for (size_t i = 0; i < index; ++i) {
    out.push_back(values[i]);
  }
  out.emplace_back(std::move(new_element));
  for (size_t i = index; i < values.size(); ++i) {
    out.push_back(values[i]);
  }
  return out;
}

template <typename T>
std::vector<T> ReplaceVectorElement(const std::vector<T>& values, size_t index,
                                    T new_element) {
  DCHECK_LE(index, values.size());
  std::vector<T> out;
  out.reserve(values.size());
  for (size_t i = 0; i < index; ++i) {
    out.push_back(values[i]);
  }
  out.emplace_back(std::move(new_element));
  for (size_t i = index + 1; i < values.size(); ++i) {
    out.push_back(values[i]);
  }
  return out;
}

template <typename T, typename Predicate>
std::vector<T> FilterVector(std::vector<T> values, Predicate&& predicate) {
  auto new_end =
      std::remove_if(values.begin(), values.end(), std::forward<Predicate>(predicate));
  values.erase(new_end, values.end());
  return values;
}

}  // namespace internal
}  // namespace arrow

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

#ifndef ARROW_UTIL_STL_H
#define ARROW_UTIL_STL_H

#include <vector>

#include "arrow/util/logging.h"

namespace arrow {

template <typename T>
inline std::vector<T> DeleteVectorElement(const std::vector<T>& values, size_t index) {
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
inline std::vector<T> AddVectorElement(const std::vector<T>& values, size_t index,
                                       const T& new_element) {
  DCHECK_LE(index, values.size());
  std::vector<T> out;
  out.reserve(values.size() + 1);
  for (size_t i = 0; i < index; ++i) {
    out.push_back(values[i]);
  }
  out.push_back(new_element);
  for (size_t i = index; i < values.size(); ++i) {
    out.push_back(values[i]);
  }
  return out;
}

}  // namespace arrow

#endif  // ARROW_UTIL_STL_H

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

#include <sstream>

#include "arrow/compute/ordering.h"
#include "arrow/util/unreachable.h"

namespace arrow {
namespace compute {

bool SortKey::Equals(const SortKey& other) const {
  return target == other.target && order == other.order;
}

std::string SortKey::ToString() const {
  std::stringstream ss;
  ss << target.ToString() << ' ';
  switch (order) {
    case SortOrder::Ascending:
      ss << "ASC";
      break;
    case SortOrder::Descending:
      ss << "DESC";
      break;
  }
  return ss.str();
}

bool Ordering::IsSuborderOf(const Ordering& other) const {
  if (sort_keys_.empty()) {
    // The implicit ordering is a subordering of nothing.  The unordered ordering
    // is a subordering of everything
    return !is_implicit_;
  }
  if (null_placement_ != other.null_placement_) {
    return false;
  }
  if (sort_keys_.size() > other.sort_keys_.size()) {
    return false;
  }
  for (std::size_t key_idx = 0; key_idx < sort_keys_.size(); key_idx++) {
    if (sort_keys_[key_idx] != other.sort_keys_[key_idx]) {
      return false;
    }
  }
  return true;
}

bool Ordering::Equals(const Ordering& other) const {
  return null_placement_ == other.null_placement_ && sort_keys_ == other.sort_keys_;
}

std::string Ordering::ToString() const {
  std::stringstream ss;
  ss << "[";
  bool first = true;
  for (const auto& key : sort_keys_) {
    if (first) {
      first = false;
    } else {
      ss << ", ";
    }
    ss << key.ToString();
  }
  ss << "]";
  switch (null_placement_) {
    case NullPlacement::AtEnd:
      ss << " nulls last";
      break;
    case NullPlacement::AtStart:
      ss << " nulls first";
      break;
    default:
      Unreachable();
  }
  return ss.str();
}

}  // namespace compute
}  // namespace arrow

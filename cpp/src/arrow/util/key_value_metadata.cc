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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"

using std::size_t;

namespace arrow {

static std::vector<std::string> UnorderedMapKeys(
    const std::unordered_map<std::string, std::string>& map) {
  std::vector<std::string> keys;
  keys.reserve(map.size());
  for (const auto& pair : map) {
    keys.push_back(pair.first);
  }
  return keys;
}

static std::vector<std::string> UnorderedMapValues(
    const std::unordered_map<std::string, std::string>& map) {
  std::vector<std::string> values;
  values.reserve(map.size());
  for (const auto& pair : map) {
    values.push_back(pair.second);
  }
  return values;
}

KeyValueMetadata::KeyValueMetadata() : keys_(), values_() {}

KeyValueMetadata::KeyValueMetadata(
    const std::unordered_map<std::string, std::string>& map)
    : keys_(UnorderedMapKeys(map)), values_(UnorderedMapValues(map)) {
  ARROW_CHECK_EQ(keys_.size(), values_.size());
}

KeyValueMetadata::KeyValueMetadata(const std::vector<std::string>& keys,
                                   const std::vector<std::string>& values)
    : keys_(keys), values_(values) {
  ARROW_CHECK_EQ(keys.size(), values.size());
}

void KeyValueMetadata::ToUnorderedMap(
    std::unordered_map<std::string, std::string>* out) const {
  DCHECK_NE(out, nullptr);
  const int64_t n = size();
  out->reserve(n);
  for (int64_t i = 0; i < n; ++i) {
    out->insert(std::make_pair(key(i), value(i)));
  }
}

void KeyValueMetadata::Append(const std::string& key, const std::string& value) {
  keys_.push_back(key);
  values_.push_back(value);
}

void KeyValueMetadata::reserve(int64_t n) {
  DCHECK_GE(n, 0);
  const auto m = static_cast<size_t>(n);
  keys_.reserve(m);
  values_.reserve(m);
}

int64_t KeyValueMetadata::size() const {
  DCHECK_EQ(keys_.size(), values_.size());
  return static_cast<int64_t>(keys_.size());
}

const std::string& KeyValueMetadata::key(int64_t i) const {
  DCHECK_GE(i, 0);
  DCHECK_LT(static_cast<size_t>(i), keys_.size());
  return keys_[i];
}

const std::string& KeyValueMetadata::value(int64_t i) const {
  DCHECK_GE(i, 0);
  DCHECK_LT(static_cast<size_t>(i), values_.size());
  return values_[i];
}

std::vector<std::pair<std::string, std::string>> KeyValueMetadata::sorted_pairs() const {
  std::vector<std::pair<std::string, std::string>> pairs;
  pairs.reserve(size());

  auto indices = internal::ArgSort(keys_);
  for (const auto i : indices) {
    pairs.emplace_back(keys_[i], values_[i]);
  }
  return pairs;
}

int KeyValueMetadata::FindKey(const std::string& key) const {
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (keys_[i] == key) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

std::shared_ptr<KeyValueMetadata> KeyValueMetadata::Copy() const {
  return std::make_shared<KeyValueMetadata>(keys_, values_);
}

bool KeyValueMetadata::Equals(const KeyValueMetadata& other) const {
  if (size() != other.size()) {
    return false;
  }

  auto indices = internal::ArgSort(keys_);
  auto other_indices = internal::ArgSort(other.keys_);

  for (int64_t i = 0; i < size(); ++i) {
    auto j = indices[i];
    auto k = other_indices[i];
    if (keys_[j] != other.keys_[k] || values_[j] != other.values_[k]) {
      return false;
    }
  }
  return true;
}

std::string KeyValueMetadata::ToString() const {
  std::stringstream buffer;

  buffer << "\n-- metadata --";
  for (int64_t i = 0; i < size(); ++i) {
    buffer << "\n" << keys_[i] << ": " << values_[i];
  }

  return buffer.str();
}

std::shared_ptr<KeyValueMetadata> key_value_metadata(
    const std::unordered_map<std::string, std::string>& pairs) {
  return std::make_shared<KeyValueMetadata>(pairs);
}

std::shared_ptr<KeyValueMetadata> key_value_metadata(
    const std::vector<std::string>& keys, const std::vector<std::string>& values) {
  return std::make_shared<KeyValueMetadata>(keys, values);
}

}  // namespace arrow

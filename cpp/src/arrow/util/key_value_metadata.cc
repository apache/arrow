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
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/sort.h"

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

KeyValueMetadata::KeyValueMetadata(std::vector<std::string> keys,
                                   std::vector<std::string> values)
    : keys_(std::move(keys)), values_(std::move(values)) {
  ARROW_CHECK_EQ(keys.size(), values.size());
}

std::shared_ptr<KeyValueMetadata> KeyValueMetadata::Make(
    std::vector<std::string> keys, std::vector<std::string> values) {
  return std::make_shared<KeyValueMetadata>(std::move(keys), std::move(values));
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

Result<std::string> KeyValueMetadata::Get(const std::string& key) const {
  auto index = FindKey(key);
  if (index < 0) {
    return Status::KeyError(key);
  } else {
    return value(index);
  }
}

Status KeyValueMetadata::Delete(int64_t index) {
  keys_.erase(keys_.begin() + index);
  values_.erase(values_.begin() + index);
  return Status::OK();
}

Status KeyValueMetadata::DeleteMany(std::vector<int64_t> indices) {
  std::sort(indices.begin(), indices.end());
  const int64_t size = static_cast<int64_t>(keys_.size());
  indices.push_back(size);

  int64_t shift = 0;
  for (int64_t i = 0; i < static_cast<int64_t>(indices.size() - 1); ++i) {
    ++shift;
    const auto start = indices[i] + 1;
    const auto stop = indices[i + 1];
    DCHECK_GE(start, 0);
    DCHECK_LE(start, size);
    DCHECK_GE(stop, 0);
    DCHECK_LE(stop, size);
    for (int64_t index = start; index < stop; ++index) {
      keys_[index - shift] = std::move(keys_[index]);
      values_[index - shift] = std::move(values_[index]);
    }
  }
  keys_.resize(size - shift);
  values_.resize(size - shift);
  return Status::OK();
}

Status KeyValueMetadata::Delete(const std::string& key) {
  auto index = FindKey(key);
  if (index < 0) {
    return Status::KeyError(key);
  } else {
    return Delete(index);
  }
}

Status KeyValueMetadata::Set(const std::string& key, const std::string& value) {
  auto index = FindKey(key);
  if (index < 0) {
    Append(key, value);
  } else {
    keys_[index] = key;
    values_[index] = value;
  }
  return Status::OK();
}

bool KeyValueMetadata::Contains(const std::string& key) const {
  return FindKey(key) >= 0;
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

std::shared_ptr<KeyValueMetadata> KeyValueMetadata::Merge(
    const KeyValueMetadata& other) const {
  std::unordered_set<std::string> observed_keys;
  std::vector<std::string> result_keys;
  std::vector<std::string> result_values;

  result_keys.reserve(keys_.size());
  result_values.reserve(keys_.size());

  for (int64_t i = 0; i < other.size(); ++i) {
    const auto& key = other.key(i);
    auto it = observed_keys.find(key);
    if (it == observed_keys.end()) {
      result_keys.push_back(key);
      result_values.push_back(other.value(i));
      observed_keys.insert(key);
    }
  }
  for (size_t i = 0; i < keys_.size(); ++i) {
    auto it = observed_keys.find(keys_[i]);
    if (it == observed_keys.end()) {
      result_keys.push_back(keys_[i]);
      result_values.push_back(values_[i]);
      observed_keys.insert(keys_[i]);
    }
  }

  return std::make_shared<KeyValueMetadata>(std::move(result_keys),
                                            std::move(result_values));
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

std::shared_ptr<KeyValueMetadata> key_value_metadata(std::vector<std::string> keys,
                                                     std::vector<std::string> values) {
  return std::make_shared<KeyValueMetadata>(std::move(keys), std::move(values));
}

}  // namespace arrow

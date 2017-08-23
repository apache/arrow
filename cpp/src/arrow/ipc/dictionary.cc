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

#include "arrow/ipc/dictionary.h"

#include <cstdint>
#include <memory>
#include <sstream>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {
namespace ipc {

DictionaryMemo::DictionaryMemo() {}

// Returns KeyError if dictionary not found
Status DictionaryMemo::GetDictionary(int64_t id,
                                     std::shared_ptr<Array>* dictionary) const {
  auto it = id_to_dictionary_.find(id);
  if (it == id_to_dictionary_.end()) {
    std::stringstream ss;
    ss << "Dictionary with id " << id << " not found";
    return Status::KeyError(ss.str());
  }
  *dictionary = it->second;
  return Status::OK();
}

int64_t DictionaryMemo::GetId(const std::shared_ptr<Array>& dictionary) {
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  auto it = dictionary_to_id_.find(address);
  if (it != dictionary_to_id_.end()) {
    // Dictionary already observed, return the id
    return it->second;
  } else {
    int64_t new_id = static_cast<int64_t>(dictionary_to_id_.size());
    dictionary_to_id_[address] = new_id;
    id_to_dictionary_[new_id] = dictionary;
    return new_id;
  }
}

bool DictionaryMemo::HasDictionary(const std::shared_ptr<Array>& dictionary) const {
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  auto it = dictionary_to_id_.find(address);
  return it != dictionary_to_id_.end();
}

bool DictionaryMemo::HasDictionaryId(int64_t id) const {
  auto it = id_to_dictionary_.find(id);
  return it != id_to_dictionary_.end();
}

Status DictionaryMemo::AddDictionary(int64_t id,
                                     const std::shared_ptr<Array>& dictionary) {
  if (HasDictionaryId(id)) {
    std::stringstream ss;
    ss << "Dictionary with id " << id << " already exists";
    return Status::KeyError(ss.str());
  }
  intptr_t address = reinterpret_cast<intptr_t>(dictionary.get());
  id_to_dictionary_[id] = dictionary;
  dictionary_to_id_[address] = id;
  return Status::OK();
}

}  // namespace ipc
}  // namespace arrow

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
#include <utility>

#include "arrow/status.h"

namespace arrow {
namespace ipc {
namespace internal {

DictionaryMemo::DictionaryMemo() {}

// Returns KeyError if dictionary not found
Status DictionaryMemo::GetDictionary(int64_t id,
                                     std::shared_ptr<Array>* dictionary) const {
  auto it = id_to_dictionary_.find(id);
  if (it == id_to_dictionary_.end()) {
    return Status::KeyError("Dictionary with id ", id, " not found");
  }
  *dictionary = it->second;
  return Status::OK();
}

int64_t DictionaryMemo::GetOrAssignId(const std::shared_ptr<Field>& field) {
  intptr_t address = reinterpret_cast<intptr_t>(field.get());
  auto it = field_to_id_.find(address);
  if (it != field_to_id_.end()) {
    // Field already observed, return the id
    return it->second;
  } else {
    int64_t new_id = static_cast<int64_t>(field_to_id_.size());
    field_to_id_[address] = new_id;
    id_to_field_[new_id] = field;
    return new_id;
  }
}

Status DictionaryMemo::GetId(const Field& field, int64_t* id) const {
  intptr_t address = reinterpret_cast<intptr_t>(&field);
  auto it = field_to_id_.find(address);
  if (it != field_to_id_.end()) {
    // Field already observed, return the id
    *id = it->second;
    return Status::OK();
  } else {
    return Status::KeyError("Field with memory address ", address, " not found");
  }
}

bool DictionaryMemo::HasDictionary(const std::shared_ptr<Field>& field) const {
  intptr_t address = reinterpret_cast<intptr_t>(field.get());
  auto it = field_to_id_.find(address);
  return it != field_to_id_.end();
}

bool DictionaryMemo::HasDictionaryId(int64_t id) const {
  auto it = id_to_dictionary_.find(id);
  return it != id_to_dictionary_.end();
}

Status DictionaryMemo::AddDictionary(int64_t id,
                                     const std::shared_ptr<Array>& dictionary) {
  if (HasDictionaryId(id)) {
    return Status::KeyError("Dictionary with id ", id, " already exists");
  }
  id_to_dictionary_[id] = dictionary;
  return Status::OK();
}

}  // namespace internal
}  // namespace ipc
}  // namespace arrow

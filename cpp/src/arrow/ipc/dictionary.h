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

// Tools for dictionaries in IPC context

#ifndef ARROW_IPC_DICTIONARY_H
#define ARROW_IPC_DICTIONARY_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
class Field;

namespace io {

class InputStream;
class OutputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {

using DictionaryMap = std::unordered_map<int64_t, std::shared_ptr<Array>>;
using DictionaryTypeMap = std::unordered_map<int64_t, std::shared_ptr<Field>>;

// Memoization data structure for handling shared dictionaries
class ARROW_EXPORT DictionaryMemo {
 public:
  DictionaryMemo();

  // Returns KeyError if dictionary not found
  Status GetDictionary(int64_t id, std::shared_ptr<Array>* dictionary) const;

  /// Return id for dictionary, computing new id if necessary
  int64_t GetId(const std::shared_ptr<Array>& dictionary);

  bool HasDictionary(const std::shared_ptr<Array>& dictionary) const;
  bool HasDictionaryId(int64_t id) const;

  // Add a dictionary to the memo with a particular id. Returns KeyError if
  // that dictionary already exists
  Status AddDictionary(int64_t id, const std::shared_ptr<Array>& dictionary);

  const DictionaryMap& id_to_dictionary() const { return id_to_dictionary_; }

  int size() const { return static_cast<int>(id_to_dictionary_.size()); }

 private:
  // Dictionary memory addresses, to track whether a dictionary has been seen
  // before
  std::unordered_map<intptr_t, int64_t> dictionary_to_id_;

  // Map of dictionary id to dictionary array
  DictionaryMap id_to_dictionary_;

  DISALLOW_COPY_AND_ASSIGN(DictionaryMemo);
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_DICTIONARY_H

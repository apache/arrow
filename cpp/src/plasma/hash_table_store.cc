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

#include <memory>
#include <mutex>

#include "plasma/hash_table_store.h"

namespace plasma {

std::mutex mtx;

Status HashTableStore::Connect(const std::string& endpoint,
                               std::shared_ptr<ExternalStoreHandle>* handle) {
  *handle = std::make_shared<HashTableStoreHandle>(table_);
  return Status::OK();
}

HashTableStoreHandle::HashTableStoreHandle(hash_table_t& table) : table_(table) {}

Status HashTableStoreHandle::Put(const std::vector<ObjectID>& ids,
                                 const std::vector<std::shared_ptr<Buffer>>& data) {
  for (size_t i = 0; i < ids.size(); ++i) {
    std::lock_guard<std::mutex> lock(mtx);
    table_[ids[i]] = data[i]->ToString();
  }
  return Status::OK();
}

Status HashTableStoreHandle::Get(const std::vector<ObjectID>& ids,
                                 std::vector<std::string>& data) {
  data.resize(ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    bool valid;
    hash_table_t::iterator result;
    {
      std::lock_guard<std::mutex> lock(mtx);
      result = table_.find(ids[i]);
      valid = result != table_.end();
    }
    if (valid) {
      data[i] = result->second;
    }
  }
  return Status::OK();
}

REGISTER_EXTERNAL_STORE("hashtable", HashTableStore);

}  // namespace plasma

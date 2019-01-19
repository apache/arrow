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

#include "hash_table_store.h"

namespace plasma {

std::shared_ptr<ExternalStoreHandle> HashTableStore::Connect(const std::string &endpoint) {
  return std::make_shared<HashTableStoreHandle>(table_, mtx_);
}

HashTableStoreHandle::HashTableStoreHandle(hash_table_t &table, std::mutex &mtx)
    : table_(table), mtx_(mtx) {
}

Status HashTableStoreHandle::Put(size_t num_objects, const ObjectID *ids, const std::string *data) {
  for (size_t i = 0; i < num_objects; ++i) {
    std::lock_guard<std::mutex> lock(mtx_);
    table_[ids[i]] = data[i];
  }
  return Status::OK();
}

Status HashTableStoreHandle::Get(size_t num_objects, const ObjectID *ids, std::string *data) {
  for (size_t i = 0; i < num_objects; ++i) {
    bool valid;
    hash_table_t::iterator result;
    {
      std::lock_guard<std::mutex> lock(mtx_);
      result = table_.find(ids[i]);
      valid = result != table_.end();
    }
    if (valid) {
      data[i] = result->second;
    } else {
      data[i].clear();
    }
  }
  return Status::OK();
}

REGISTER_EXTERNAL_STORE("hashtable", HashTableStore);

}

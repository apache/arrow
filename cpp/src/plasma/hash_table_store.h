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

#ifndef HASH_TABLE_STORE_H
#define HASH_TABLE_STORE_H

#include <mutex>
#include "external_store.h"

namespace plasma {

// This is a sample implementation for an external store, for illustration
// purposes only.

typedef std::unordered_map<ObjectID, std::string> hash_table_t;

class HashTableStoreHandle : public ExternalStoreHandle {
 public:
  HashTableStoreHandle(hash_table_t& table, std::mutex& mtx);

  Status Get(size_t num_objects, const ObjectID *ids, std::string *data) override;
  Status Put(size_t num_objects, const ObjectID *ids, const std::string *data) override;

 private:
  hash_table_t& table_;
  std::mutex& mtx_;
};

class HashTableStore : public ExternalStore {
 public:
  HashTableStore() = default;

  std::shared_ptr<ExternalStoreHandle> Connect(const std::string &endpoint) override;
 private:
  hash_table_t table_;
  std::mutex mtx_;
};

}

#endif // HASH_TABLE_STORE_H

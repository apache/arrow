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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "plasma/external_store.h"

namespace plasma {

// This is a sample implementation for an external store, for illustration
// purposes only.

typedef std::unordered_map<ObjectID, std::string> hash_table_t;

class HashTableStoreHandle : public ExternalStoreHandle {
 public:
  HashTableStoreHandle(hash_table_t& table, std::mutex& mtx);

  Status Get(const std::vector<ObjectID> &ids,
             std::vector<std::string> &data) override;
  Status Put(const std::vector<ObjectID> &ids,
             const std::vector<std::shared_ptr<Buffer>> &data) override;

 private:
  hash_table_t& table_;
  std::mutex& mtx_;
};

class HashTableStore : public ExternalStore {
 public:
  HashTableStore() = default;

  Status Connect(const std::string &endpoint,
                 std::shared_ptr<ExternalStoreHandle> *handle) override;
 private:
  hash_table_t table_;
  std::mutex mtx_;
};

} // namespace plasma

#endif // HASH_TABLE_STORE_H

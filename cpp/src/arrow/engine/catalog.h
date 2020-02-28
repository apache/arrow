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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/variant.h"

namespace arrow {

namespace dataset {
class Dataset;
}

namespace engine {

/// Catalog is made of named Table/Dataset to be referenced in LogicalPlans.
class Catalog {
 public:
  class Entry;

  using Key = std::string;
  using Value = Entry;
  using KeyValue = std::pair<Key, Value>;

  static Result<std::shared_ptr<Catalog>> Make(const std::vector<KeyValue>& tables);

  Result<Value> Get(const Key& name) const;
  Result<std::shared_ptr<Schema>> GetSchema(const Key& name) const;

  class Entry {
   public:
    enum Kind {
      TABLE = 0,
      DATASET,
      UNKNOWN,
    };

    explicit Entry(std::shared_ptr<Table> table);
    explicit Entry(std::shared_ptr<dataset::Dataset> dataset);

    Kind kind() const;
    std::shared_ptr<Table> table() const;
    std::shared_ptr<dataset::Dataset> dataset() const;

    std::shared_ptr<Schema> schema() const;

    bool operator==(const Entry& other) const;

   private:
    util::variant<std::shared_ptr<Table>, std::shared_ptr<dataset::Dataset>> entry_;
  };

 private:
  friend class CatalogBuilder;
  explicit Catalog(std::unordered_map<Key, Value> tables);

  std::unordered_map<Key, Value> tables_;
};

class CatalogBuilder {
 public:
  using Key = Catalog::Key;
  using Value = Catalog::Value;
  using KeyValue = Catalog::KeyValue;

  Status Add(const Key& key, const Value& value);
  Status Add(const Key& key, std::shared_ptr<Table>);
  Status Add(const Key& key, std::shared_ptr<dataset::Dataset>);
  Status Add(const KeyValue& key_value);

  Result<std::shared_ptr<Catalog>> Finish();

 private:
  std::unordered_map<Key, Value> tables_;
};

}  // namespace engine
}  // namespace arrow

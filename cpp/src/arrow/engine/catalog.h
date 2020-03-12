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

#include "arrow/engine/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/variant.h"

namespace arrow {

namespace dataset {
class Dataset;
}

namespace engine {

/// Catalog is made of named Table/Dataset to be referenced in LogicalPlans.
class ARROW_EN_EXPORT Catalog {
 public:
  class Entry;

  static Result<std::shared_ptr<Catalog>> Make(const std::vector<Entry>& tables);

  Result<Entry> Get(const std::string& name) const;
  Result<std::shared_ptr<Schema>> GetSchema(const std::string& name) const;

  class Entry {
   public:
    Entry(std::shared_ptr<dataset::Dataset> dataset, std::string name);
    Entry(std::shared_ptr<Table> table, std::string name);

    const std::string& name() const { return name_; }

    const std::shared_ptr<dataset::Dataset>& dataset() const;

    const std::shared_ptr<Schema>& schema() const;

    bool operator==(const Entry& other) const;

   private:
    std::shared_ptr<dataset::Dataset> dataset_;
    std::string name_;
  };

 private:
  friend class CatalogBuilder;
  explicit Catalog(std::unordered_map<std::string, Entry> datasets);

  std::unordered_map<std::string, Entry> datasets_;
};

class ARROW_EN_EXPORT CatalogBuilder {
 public:
  Status Add(Catalog::Entry entry);
  Status Add(std::string name, std::shared_ptr<dataset::Dataset>);
  Status Add(std::string name, std::shared_ptr<Table>);

  Result<std::shared_ptr<Catalog>> Finish();

 private:
  std::unordered_map<std::string, Catalog::Entry> datasets_;
};

}  // namespace engine
}  // namespace arrow

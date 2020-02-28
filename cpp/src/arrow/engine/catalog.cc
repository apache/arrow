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

#include "arrow/engine/catalog.h"

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"

#include "arrow/dataset/dataset.h"

namespace arrow {
namespace engine {

//
// Catalog
//

Catalog::Catalog(std::unordered_map<Key, Value> tables) : tables_(std::move(tables)) {}

Result<Catalog::Value> Catalog::Get(const Key& key) const {
  auto value = tables_.find(key);
  if (value != tables_.end()) return value->second;
  return Status::KeyError("Table '", key, "' not found in catalog.");
}

Result<std::shared_ptr<Schema>> Catalog::GetSchema(const Key& key) const {
  auto as_schema = [](const Value& v) -> Result<std::shared_ptr<Schema>> {
    return v.schema();
  };
  return Get(key).Map(as_schema);
}

Result<std::shared_ptr<Catalog>> Catalog::Make(const std::vector<KeyValue>& tables) {
  CatalogBuilder builder;

  for (const auto& key_val : tables) {
    RETURN_NOT_OK(builder.Add(key_val));
  }

  return builder.Finish();
}

//
// Catalog::Entry
//

using Entry = Catalog::Entry;

Entry::Entry(std::shared_ptr<Table> table) : entry_(std::move(table)) {}
Entry::Entry(std::shared_ptr<dataset::Dataset> dataset) : entry_(std::move(dataset)) {}

Entry::Kind Entry::kind() const {
  if (util::holds_alternative<std::shared_ptr<Table>>(entry_)) {
    return TABLE;
  }

  if (util::holds_alternative<std::shared_ptr<dataset::Dataset>>(entry_)) {
    return DATASET;
  }

  return UNKNOWN;
}

std::shared_ptr<Table> Entry::table() const {
  if (kind() == TABLE) return util::get<std::shared_ptr<Table>>(entry_);
  return nullptr;
}

std::shared_ptr<dataset::Dataset> Entry::dataset() const {
  if (kind() == DATASET) return util::get<std::shared_ptr<dataset::Dataset>>(entry_);
  return nullptr;
}

bool Entry::operator==(const Entry& other) const { return entry_ == other.entry_; }

std::shared_ptr<Schema> Entry::schema() const {
  switch (kind()) {
    case TABLE:
      return table()->schema();
    case DATASET:
      return dataset()->schema();
    default:
      return nullptr;
  }

  return nullptr;
}

//
// CatalogBuilder
//

Status CatalogBuilder::Add(const Key& key, const Value& value) {
  if (key.empty()) {
    return Status::Invalid("Key in catalog can't be empty");
  }

  switch (value.kind()) {
    case Entry::TABLE: {
      if (value.table() == nullptr) {
        return Status::Invalid("Table entry can't be null.");
      }
      break;
    }
    case Entry::DATASET: {
      if (value.dataset() == nullptr) {
        return Status::Invalid("Table entry can't be null.");
      }
      break;
    }
    default:
      return Status::NotImplemented("Unknown entry kind");
  }

  auto inserted = tables_.insert({key, value});
  if (!inserted.second) {
    return Status::KeyError("Table '", key, "' already in catalog.");
  }

  return Status::OK();
}

Status CatalogBuilder::Add(const Key& key, std::shared_ptr<Table> table) {
  return Add(key, Entry(std::move(table)));
}

Status CatalogBuilder::Add(const Key& key, std::shared_ptr<dataset::Dataset> dataset) {
  return Add(key, Entry(std::move(dataset)));
}

Status CatalogBuilder::Add(const KeyValue& key_value) {
  return Add(key_value.first, key_value.second);
}

Result<std::shared_ptr<Catalog>> CatalogBuilder::Finish() {
  return std::shared_ptr<Catalog>(new Catalog(std::move(tables_)));
}

}  // namespace engine
}  // namespace arrow

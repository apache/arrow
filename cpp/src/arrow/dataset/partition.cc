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

#include "arrow/dataset/partition.h"

#include <algorithm>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/scalar.h"
#include "arrow/util/iterator.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace dataset {

using util::string_view;

Result<std::shared_ptr<Expression>> Partitioning::Parse(const std::string& path) const {
  ExpressionVector expressions;
  int i = 0;

  for (auto segment : fs::internal::SplitAbstractPath(path)) {
    ARROW_ASSIGN_OR_RAISE(auto expr, Parse(segment, i++));
    if (expr->Equals(true)) {
      continue;
    }

    expressions.push_back(std::move(expr));
  }

  return and_(std::move(expressions));
}

std::shared_ptr<Partitioning> Partitioning::Default() {
  return std::make_shared<DefaultPartitioning>();
}

Result<std::shared_ptr<Expression>> SegmentDictionaryPartitioning::Parse(
    const std::string& segment, int i) const {
  if (static_cast<size_t>(i) < dictionaries_.size()) {
    auto it = dictionaries_[i].find(segment);
    if (it != dictionaries_[i].end()) {
      return it->second;
    }
  }

  return scalar(true);
}

Result<std::shared_ptr<Expression>> KeyValuePartitioning::ConvertKey(
    const Key& key, const Schema& schema) {
  auto field = schema.GetFieldByName(key.name);
  if (field == nullptr) {
    return scalar(true);
  }

  ARROW_ASSIGN_OR_RAISE(auto converted, Scalar::Parse(field->type(), key.value));
  return equal(field_ref(field->name()), scalar(converted));
}

Result<std::shared_ptr<Expression>> KeyValuePartitioning::Parse(
    const std::string& segment, int i) const {
  if (auto key = ParseKey(segment, i)) {
    return ConvertKey(*key, *schema_);
  }

  return scalar(true);
}

util::optional<KeyValuePartitioning::Key> DirectoryPartitioning::ParseKey(
    const std::string& segment, int i) const {
  if (i >= schema_->num_fields()) {
    return util::nullopt;
  }

  return Key{schema_->field(i)->name(), segment};
}

inline bool AllIntegral(const std::vector<std::string>& reprs) {
  return std::all_of(reprs.begin(), reprs.end(), [](string_view repr) {
    // TODO(bkietz) use ParseUnsigned or so
    return repr.find_first_not_of("0123456789") == string_view::npos;
  });
}

inline std::shared_ptr<Schema> InferSchema(
    const std::map<std::string, std::vector<std::string>>& name_to_values) {
  std::vector<std::shared_ptr<Field>> fields(name_to_values.size());

  size_t field_index = 0;
  for (const auto& name_values : name_to_values) {
    auto type = AllIntegral(name_values.second) ? int32() : utf8();
    fields[field_index++] = field(name_values.first, type);
  }
  return ::arrow::schema(std::move(fields));
}

class DirectoryPartitioningFactory : public PartitioningFactory {
 public:
  explicit DirectoryPartitioningFactory(std::vector<std::string> field_names)
      : field_names_(std::move(field_names)) {}

  Result<std::shared_ptr<Schema>> Inspect(
      const std::vector<string_view>& paths) const override {
    std::map<std::string, std::vector<std::string>> name_to_values;

    for (auto path : paths) {
      size_t field_index = 0;
      for (auto&& segment : fs::internal::SplitAbstractPath(path.to_string())) {
        if (field_index == field_names_.size()) break;

        name_to_values[field_names_[field_index++]].push_back(std::move(segment));
      }
    }

    // ensure that the fields are ordered by field_names_
    return SchemaFromColumnNames(InferSchema(name_to_values), field_names_);
  }

  Result<std::shared_ptr<Partitioning>> Finish(
      const std::shared_ptr<Schema>& schema) const override {
    for (const auto& field_name : field_names_) {
      if (schema->GetFieldIndex(field_name) == -1) {
        return Status::TypeError("no field named '", field_name, "' in schema", *schema);
      }
    }

    // drop fields which aren't in field_names_
    auto out_schema = SchemaFromColumnNames(schema, field_names_);

    return std::make_shared<DirectoryPartitioning>(std::move(out_schema));
  }

 private:
  std::vector<std::string> field_names_;
};

std::shared_ptr<PartitioningFactory> DirectoryPartitioning::MakeFactory(
    std::vector<std::string> field_names) {
  return std::shared_ptr<PartitioningFactory>(
      new DirectoryPartitioningFactory(std::move(field_names)));
}

util::optional<KeyValuePartitioning::Key> HivePartitioning::ParseKey(
    const std::string& segment) {
  auto name_end = string_view(segment).find_first_of('=');
  if (name_end == string_view::npos) {
    return util::nullopt;
  }

  return Key{segment.substr(0, name_end), segment.substr(name_end + 1)};
}

class HivePartitioningFactory : public PartitioningFactory {
 public:
  Result<std::shared_ptr<Schema>> Inspect(
      const std::vector<string_view>& paths) const override {
    std::map<std::string, std::vector<std::string>> name_to_values;

    for (auto path : paths) {
      for (auto&& segment : fs::internal::SplitAbstractPath(path.to_string())) {
        if (auto key = HivePartitioning::ParseKey(segment)) {
          name_to_values[key->name].push_back(std::move(key->value));
        }
      }
    }

    return InferSchema(name_to_values);
  }

  Result<std::shared_ptr<Partitioning>> Finish(
      const std::shared_ptr<Schema>& schema) const override {
    return std::shared_ptr<Partitioning>(new HivePartitioning(schema));
  }
};

std::shared_ptr<PartitioningFactory> HivePartitioning::MakeFactory() {
  return std::shared_ptr<PartitioningFactory>(new HivePartitioningFactory());
}

}  // namespace dataset
}  // namespace arrow

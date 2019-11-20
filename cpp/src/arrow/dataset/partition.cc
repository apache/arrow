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
#include <memory>
#include <utility>

#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/scalar.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

Result<ExpressionPtr> PartitionKeysScheme::ConvertKey(const Key& key,
                                                      const Schema& schema) {
  auto field = schema.GetFieldByName(key.name);
  if (field == nullptr) {
    return scalar(true);
  }

  std::shared_ptr<Scalar> converted;
  RETURN_NOT_OK(Scalar::Parse(field->type(), key.value, &converted));
  return equal(field_ref(field->name()), scalar(converted));
}

Result<ExpressionPtr> PartitionScheme::Parse(const std::string& path) const {
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

Result<ExpressionPtr> PartitionKeysScheme::Parse(const std::string& segment,
                                                 int i) const {
  if (auto key = ParseKey(segment, i)) {
    return ConvertKey(*key, *schema_);
  }
  return scalar(true);
}

util::optional<PartitionKeysScheme::Key> SchemaPartitionScheme::ParseKey(
    const std::string& segment, int i) const {
  if (i >= schema_->num_fields()) {
    return util::nullopt;
  }

  return Key{schema_->field(i)->name(), segment};
}

util::optional<PartitionKeysScheme::Key> HivePartitionScheme::ParseKey(
    const std::string& segment, int) const {
  static std::regex hive_style("^([^=]+)=(.*)$");

  std::smatch matches;
  if (!std::regex_match(segment, matches, hive_style) || matches.size() != 3) {
    return util::nullopt;
  }

  return Key{matches[1].str(), matches[2].str()};
}

Result<PathPartitions> ApplyPartitionScheme(const PartitionScheme& scheme,
                                            std::vector<fs::FileStats> files,
                                            PathPartitions* out) {
  return ApplyPartitionScheme(scheme, "", std::move(files));
}

Result<PathPartitions> ApplyPartitionScheme(const PartitionScheme& scheme,
                                            const std::string& base_dir,
                                            std::vector<fs::FileStats> files) {
  PathPartitions partitions;

  for (const auto& file : files) {
    if (file.path().substr(0, base_dir.size()) != base_dir) continue;
    auto path = file.path().substr(base_dir.size());

    ARROW_ASSIGN_OR_RAISE(auto partition, scheme.Parse(path));
    partitions.emplace(std::move(path), std::move(partition));
  }

  return partitions;
}

}  // namespace dataset
}  // namespace arrow

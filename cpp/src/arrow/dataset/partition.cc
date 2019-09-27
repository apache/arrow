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

#include <memory>
#include <utility>

#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/scalar.h"
#include "arrow/util/iterator.h"
#include "arrow/util/stl.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace dataset {

using util::string_view;

Status ConvertPartitionKeys(const std::vector<UnconvertedKey>& keys, const Schema& schema,
                            std::shared_ptr<Expression>* out) {
  std::vector<std::shared_ptr<Expression>> subexpressions;

  for (const auto& key : keys) {
    auto field = schema.GetFieldByName(key.name);
    if (field == nullptr) {
      continue;
    }

    std::shared_ptr<Scalar> converted;
    RETURN_NOT_OK(Scalar::Parse(field->type(), key.value, &converted));
    subexpressions.push_back(equal(field_ref(field->name()), scalar(converted)));
  }

  *out = and_(subexpressions);
  return Status::OK();
}

Status PartitionScheme::Parse(const std::string& path,
                              std::shared_ptr<Expression>* out) const {
  std::string unconsumed;
  return Parse(path, &unconsumed, out);
}

Status ConstantPartitionScheme::Parse(const std::string& path, std::string* unconsumed,
                                      std::shared_ptr<Expression>* out) const {
  if (!string_view(path).starts_with(ignored_)) {
    return Status::Invalid("path \"", path, "\" did not contain required prefix \"",
                           ignored_, "\"");
  }

  *unconsumed = path.substr(ignored_.size());
  *out = expression_;
  return Status::OK();
}

Status ChainPartitionScheme::Parse(const std::string& path, std::string* unconsumed,
                                   std::shared_ptr<Expression>* out) const {
  *unconsumed = path;
  std::vector<std::shared_ptr<Expression>> subexpressions;

  for (const auto& scheme : schemes_) {
    std::shared_ptr<Expression> expr;
    RETURN_NOT_OK(scheme->Parse(*unconsumed, unconsumed, &expr));
    subexpressions.push_back(std::move(expr));
  }

  *out = and_(subexpressions);
  return Status::OK();
}

Status FieldPartitionScheme::Parse(const std::string& path, std::string* unconsumed,
                                   std::shared_ptr<Expression>* out) const {
  auto segments = fs::internal::SplitAbstractPath(path);
  if (segments.size() == 0) {
    return Status::Invalid("cannot parse a path with no segments");
  }

  *unconsumed =
      "/" + fs::internal::JoinAbstractPath(segments.begin() + 1, segments.end());
  std::vector<UnconvertedKey> keys = {{field_->name(), segments[0]}};
  auto schm = schema({field_});
  return ConvertPartitionKeys(keys, *schm, out);
}

std::vector<UnconvertedKey> HivePartitionScheme::GetUnconvertedKeys(
    const std::string& path, std::string* unconsumed) const {
  *unconsumed = path;

  std::vector<UnconvertedKey> keys;
  std::smatch matches;
  // TODO(bkietz) use RE2 and named groups
  static std::regex hive_style("^/([^=/]+)=([^/]*)(.*)$");
  while (std::regex_match(*unconsumed, matches, hive_style) && matches.size() == 4) {
    if (schema_->GetFieldByName(matches[1]) == nullptr) {
      break;
    }
    keys.push_back({matches[1], matches[2]});
    *unconsumed = matches[3];
  }

  return keys;
}

Status HivePartitionScheme::Parse(const std::string& path, std::string* unconsumed,
                                  std::shared_ptr<Expression>* out) const {
  return ConvertPartitionKeys(GetUnconvertedKeys(path, unconsumed), *schema_, out);
}

}  // namespace dataset
}  // namespace arrow

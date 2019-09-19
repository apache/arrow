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

namespace arrow {
namespace dataset {

using util::string_view;

bool ParseOneKey(string_view path, string_view* key, string_view* value,
                 string_view* parent) {
  auto parent_end = path.find_last_of(fs::internal::kSep);
  if (parent_end == string_view::npos) {
    return false;
  }

  auto key_length = path.substr(parent_end + 1).find_first_of('=');
  if (key_length == string_view::npos) {
    return false;
  }

  *key = path.substr(parent_end + 1, key_length);
  *value = path.substr(parent_end + 1 + key_length + 1);
  *parent = path.substr(0, parent_end);
  return true;
}

bool HivePartitionScheme::CanParse(string_view path) const {
  // at worst we return an empty expression
  return true;
}

Status HivePartitionScheme::Parse(string_view path,
                                  std::shared_ptr<Expression>* out) const {
  *out = nullptr;
  string_view key, value;
  while (ParseOneKey(path, &key, &value, &path)) {
    auto name = key.to_string();
    auto field = schema_->GetFieldByName(name);
    if (field == nullptr) {
      continue;
    }

    std::shared_ptr<Scalar> scalar;
    RETURN_NOT_OK(Scalar::Parse(field->type(), value, &scalar));
    auto expr = equal(field_ref(name), ScalarExpression::Make(scalar));

    if (*out == nullptr) {
      *out = std::move(expr);
    } else {
      *out = and_(std::move(expr), std::move(*out));
    }
  }

  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow

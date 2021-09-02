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

#include <arrow/util/hash_util.h>
#include <stddef.h>

#include <boost/any.hpp>
#include <boost/lexical_cast.hpp>
#include <sstream>
#include "gandiva/expression.h"
#include "gandiva/filter.h"
#include "gandiva/projector.h"

namespace gandiva {

class BaseCacheKey {
 public:
  BaseCacheKey(Expression& expr, std::string type) : type_(type) {
    static const int32_t kSeedValue = 4;
    std::string expr_as_string = expr.ToString();
    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, expr_as_string);
    hash_code_ = result_hash;
  };

  BaseCacheKey(ProjectorCacheKey& key, std::string type) : type_(type) {
    static const int32_t kSeedValue = 4;
    size_t key_hash = key.Hash();
    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, key_hash);
    hash_code_ = result_hash;
    schema_ = key.schema();
  };

  BaseCacheKey(FilterCacheKey& key, std::string type) : type_(type) {
    static const size_t kSeedValue = 4;
    size_t key_hash = key.Hash();
    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, key_hash);
    hash_code_ = result_hash;
    schema_ = key.schema();
  };

  BaseCacheKey(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<Expression> expr,
               std::string type)
      : type_(type) {
    static const int32_t kSeedValue = 4;
    size_t result_hash = kSeedValue;
    std::string schema_string = schema->ToString();
    std::string expr_string = expr->ToString();
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, schema_string);
    arrow::internal::hash_combine(result_hash, expr_string);
    hash_code_ = result_hash;
  };

  size_t Hash() const { return hash_code_; }

  std::string Type() const { return type_; }

  boost::any GetInnerKey() { return key_; }

  bool operator==(const BaseCacheKey& other) const {
    if (hash_code_ != other.hash_code_) {
      return false;
    }
    return true;
  };

  bool operator!=(const BaseCacheKey& other) const { return !(*this == other); }

 private:
  uint64_t hash_code_;
  std::string type_;
  SchemaPtr schema_;
};

}  // namespace gandiva

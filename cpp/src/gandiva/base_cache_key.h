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

#ifndef ARROW_BASE_CACHE_KEY_H
#define ARROW_BASE_CACHE_KEY_H

#include <arrow/util/hash_util.h>
#include <stddef.h>

#include <boost/any.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/variant.hpp>
#include <sstream>

#include "gandiva/expression.h"
#include "gandiva/filter.h"
#include "gandiva/projector.h"

namespace gandiva {

class BaseCacheKey {
 public:
  BaseCacheKey(Expression& expr, std::string type) : type_(type) {
    static const int kSeedValue = 4;
    std::string expr_as_string = expr.ToString();
    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, expr_as_string);
    hash_code_ = result_hash;

    // Generate the same UUID based on the hash_code
    boost::uuids::name_generator_sha1 gen(boost::uuids::ns::oid());
    uuid_ = gen(std::to_string(result_hash));
  };

  BaseCacheKey(ProjectorCacheKey& key, std::string type, std::vector<ExpressionPtr> exprs)
      : type_(type) {
    // static const int kSeedValue = 4;
    // size_t key_hash = key.Hash();
    // size_t result_hash = kSeedValue;
    // arrow::internal::hash_combine(result_hash, type);
    // arrow::internal::hash_combine(result_hash, key_hash);
    // hash_code_ = result_hash;
    hash_code_ = key.Hash();
    key_ = key;
    key_ = key;

    schema_string_ = key.schema()->ToString(true);

    for (auto& expr : exprs) {
      exprs_string_.push_back(expr->ToString());
    }

    // Generate the same UUID based on the hash_code
    boost::uuids::name_generator_sha1 gen(boost::uuids::ns::oid());
    uuid_ = gen(std::to_string(hash_code_));
  };

  BaseCacheKey(FilterCacheKey& key, std::string type, ConditionPtr expr) : type_(type) {
    // static const int kSeedValue = 4;
    // size_t key_hash = key.Hash();
    // size_t result_hash = kSeedValue;
    // arrow::internal::hash_combine(result_hash, type);
    // arrow::internal::hash_combine(result_hash, key_hash);
    // hash_code_ = result_hash;
    hash_code_ = key.Hash();
    key_ = key;

    schema_string_ = key.schema()->ToString(true);

    exprs_string_.push_back(expr->ToString());

    // Generate the same UUID based on the hash_code
    boost::uuids::name_generator_sha1 gen(boost::uuids::ns::oid());
    uuid_ = gen(std::to_string(hash_code_));
  };

  BaseCacheKey(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<Expression> expr,
               std::string type)
      : type_(type) {
    static const int kSeedValue = 4;
    unsigned long int result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, schema->ToString());
    arrow::internal::hash_combine(result_hash, expr->ToString());
    hash_code_ = result_hash;

    // Generate the same UUID based on the hash_code
    boost::uuids::name_generator_sha1 gen(boost::uuids::ns::oid());
    uuid_ = gen(std::to_string(result_hash));
  };

  /// Constructor used only for tests
  BaseCacheKey(std::string type, std::string value) : type_(type) {
    static const int kSeedValue = 4;

    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, value);

    exprs_string_.push_back(value);
    schema_string_ = value;

    hash_code_ = result_hash;

    // Generate the same UUID based on the hash_code
    boost::uuids::name_generator_sha1 gen(boost::uuids::ns::oid());
    uuid_ = gen(std::to_string(result_hash));
  };

  /// Constructor used only for tests involving caching objects code
  BaseCacheKey(std::string type, SchemaPtr schema, std::vector<ExpressionPtr> exprs)
      : type_(type) {
    static const int kSeedValue = 4;

    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, schema->ToString());

    for (auto& expr : exprs) {
      auto expr_string = expr->ToString();
      arrow::internal::hash_combine(result_hash, expr_string);
      exprs_string_.push_back(expr_string);
    }

    hash_code_ = result_hash;
    schema_string_ = schema->ToString();

    // Generate the same UUID based on the hash_code
    boost::uuids::name_generator_sha1 gen(boost::uuids::ns::oid());
    uuid_ = gen(std::to_string(result_hash));
  };

  size_t Hash() const { return hash_code_; }

  boost::uuids::uuid Uuid() const { return uuid_; }

  std::string Type() const { return type_; }

  std::string getUuidString() const {
    std::string uuid_string = "";
    std::stringstream ss;
    ss << uuid_;
    return ss.str();
  }

  std::string getSchemaString() const { return schema_string_; }

  std::vector<std::string> getExprsString() const { return exprs_string_; }

  bool checkCacheFile(const std::string& schema,
                      const std::vector<std::string>& exprs) const {
    if (schema_string_ != schema) {
      return false;
    }

    if (exprs_string_ != exprs) {
      return false;
    }
    return true;
  }

  bool operator==(const BaseCacheKey& other) const {
    if (hash_code_ != other.hash_code_) {
      return false;
    }

    if (uuid_ != other.uuid_) {
      return false;
    }

    if (exprs_string_ != other.exprs_string_) {
      return false;
    }

    if (schema_string_ != other.schema_string_) {
      return false;
    }

    return true;
  };

  bool operator!=(const BaseCacheKey& other) const { return !(*this == other); }

 private:
  uint64_t hash_code_;
  std::string type_;
  boost::uuids::uuid uuid_;
  boost::any key_ = nullptr;
  std::vector<std::string> exprs_string_;
  std::string schema_string_;
};

}  // namespace gandiva

#endif  // ARROW_BASE_CACHE_KEY_H

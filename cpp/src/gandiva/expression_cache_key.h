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

#include <stddef.h>

#include <thread>

#include "arrow/util/hash_util.h"
#include "gandiva/arrow.h"
#include "gandiva/configuration.h"
#include "gandiva/expression.h"
#include "gandiva/selection_vector.h"
#include "gandiva/visibility.h"

namespace gandiva {

class ExpressionCacheKey {
 public:
  ExpressionCacheKey(SchemaPtr schema, std::shared_ptr<Configuration> configuration,
                     ExpressionVector expression_vector, SelectionVector::Mode mode)
      : schema_(schema), mode_(mode), uniqifier_(0), configuration_(configuration) {
    static const int kSeedValue = 4;
    size_t result = kSeedValue;
    for (auto& expr : expression_vector) {
      std::string expr_as_string = expr->ToString();
      expressions_as_strings_.push_back(expr_as_string);
      arrow::internal::hash_combine(result, expr_as_string);
      UpdateUniqifier(expr_as_string);
    }
    arrow::internal::hash_combine(result, static_cast<size_t>(mode));
    arrow::internal::hash_combine(result, configuration->Hash());
    arrow::internal::hash_combine(result, schema_->ToString());
    arrow::internal::hash_combine(result, uniqifier_);
    hash_code_ = result;
  }

  ExpressionCacheKey(SchemaPtr schema, std::shared_ptr<Configuration> configuration,
                     Expression& expression)
      : schema_(schema),
        mode_(SelectionVector::MODE_NONE),
        uniqifier_(0),
        configuration_(configuration) {
    static const int kSeedValue = 4;
    size_t result = kSeedValue;
    expressions_as_strings_.push_back(expression.ToString());
    UpdateUniqifier(expression.ToString());

    arrow::internal::hash_combine(result, configuration->Hash());
    arrow::internal::hash_combine(result, schema_->ToString());
    arrow::internal::hash_combine(result, uniqifier_);
    hash_code_ = result;
  }

  void UpdateUniqifier(const std::string& expr) {
    if (uniqifier_ == 0) {
      // caching of expressions with re2 patterns causes lock contention. So, use
      // multiple instances to reduce contention.
      if (expr.find(" like(") != std::string::npos) {
        uniqifier_ = std::hash<std::thread::id>()(std::this_thread::get_id()) % 16;
      }
    }
  }

  size_t Hash() const { return hash_code_; }

  bool operator==(const ExpressionCacheKey& other) const {
    if (hash_code_ != other.hash_code_) {
      return false;
    }

    if (!(schema_->Equals(*other.schema_, true))) {
      return false;
    }

    if (configuration_ != other.configuration_) {
      return false;
    }

    if (mode_ != other.mode_) {
      return false;
    }

    if (expressions_as_strings_ != other.expressions_as_strings_) {
      return false;
    }

    if (uniqifier_ != other.uniqifier_) {
      return false;
    }

    return true;
  }

  bool operator!=(const ExpressionCacheKey& other) const { return !(*this == other); }

 private:
  size_t hash_code_;
  SchemaPtr schema_;
  std::vector<std::string> expressions_as_strings_;
  SelectionVector::Mode mode_;
  uint32_t uniqifier_;
  std::shared_ptr<Configuration> configuration_;
};

}  // namespace gandiva

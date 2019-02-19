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

#ifndef GANDIVA_FILTER_CACHE_KEY_H
#define GANDIVA_FILTER_CACHE_KEY_H

#include <memory>
#include <string>
#include <thread>

#include "boost/functional/hash.hpp"
#include "gandiva/arrow.h"
#include "gandiva/filter.h"

namespace gandiva {
class FilterCacheKey {
 public:
  FilterCacheKey(SchemaPtr schema, std::shared_ptr<Configuration> configuration,
                 Expression& expression)
      : schema_(schema), configuration_(configuration), uniqifier_(0) {
    static const int kSeedValue = 4;
    size_t result = kSeedValue;
    expression_as_string_ = expression.ToString();
    UpdateUniqifier(expression_as_string_);
    boost::hash_combine(result, expression_as_string_);
    boost::hash_combine(result, configuration);
    boost::hash_combine(result, schema_->ToString());
    boost::hash_combine(result, uniqifier_);
    hash_code_ = result;
  }

  std::size_t Hash() const { return hash_code_; }

  bool operator==(const FilterCacheKey& other) const {
    // arrow schema does not overload equality operators.
    if (!(schema_->Equals(*other.schema().get(), true))) {
      return false;
    }

    if (configuration_ != other.configuration_) {
      return false;
    }

    if (expression_as_string_ != other.expression_as_string_) {
      return false;
    }

    if (uniqifier_ != other.uniqifier_) {
      return false;
    }
    return true;
  }

  bool operator!=(const FilterCacheKey& other) const { return !(*this == other); }

  SchemaPtr schema() const { return schema_; }

  std::string ToString() const {
    std::stringstream ss;
    // indent, window, indent_size, null_rep and skip new lines.
    arrow::PrettyPrintOptions options{0, 10, 2, "null", true};
    DCHECK_OK(PrettyPrint(*schema_.get(), options, &ss));

    ss << "Condition: [" << expression_as_string_ << "]";
    return ss.str();
  }

 private:
  void UpdateUniqifier(const std::string& expr) {
    // caching of expressions with re2 patterns causes lock contention. So, use
    // multiple instances to reduce contention.
    if (expr.find(" like(") != std::string::npos) {
      uniqifier_ = std::hash<std::thread::id>()(std::this_thread::get_id()) % 16;
    }
  }

  const SchemaPtr schema_;
  const std::shared_ptr<Configuration> configuration_;
  std::string expression_as_string_;
  size_t hash_code_;
  uint32_t uniqifier_;
};
}  // namespace gandiva
#endif  // GANDIVA_FILTER_CACHE_KEY_H

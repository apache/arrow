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

#ifndef GANDIVA_PROJECTOR_CACHE_KEY_H
#define GANDIVA_PROJECTOR_CACHE_KEY_H

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/projector.h"

namespace gandiva {
class ProjectorCacheKey {
 public:
  ProjectorCacheKey(SchemaPtr schema, std::shared_ptr<Configuration> configuration,
                    ExpressionVector expression_vector, SelectionVector::Mode mode)
      : schema_(schema), configuration_(configuration), mode_(mode), uniqifier_(0) {
    static const int kSeedValue = 4;
    size_t result = kSeedValue;
    for (auto& expr : expression_vector) {
      std::string expr_as_string = expr->ToString();
      expressions_as_strings_.push_back(expr_as_string);
      boost::hash_combine(result, expr_as_string);
      UpdateUniqifier(expr_as_string);
    }
    boost::hash_combine(result, mode);
    boost::hash_combine(result, configuration->Hash());
    boost::hash_combine(result, schema_->ToString());
    boost::hash_combine(result, uniqifier_);
    hash_code_ = result;
  }

  std::size_t Hash() const { return hash_code_; }

  bool operator==(const ProjectorCacheKey& other) const {
    // arrow schema does not overload equality operators.
    if (!(schema_->Equals(*other.schema().get(), true))) {
      return false;
    }

    if (*configuration_ != *other.configuration_) {
      return false;
    }

    if (expressions_as_strings_ != other.expressions_as_strings_) {
      return false;
    }

    if (mode_ != other.mode_) {
      return false;
    }

    if (uniqifier_ != other.uniqifier_) {
      return false;
    }
    return true;
  }

  bool operator!=(const ProjectorCacheKey& other) const { return !(*this == other); }

  SchemaPtr schema() const { return schema_; }

  std::string ToString() const {
    std::stringstream ss;
    // indent, window, indent_size, null_rep and skip new lines.
    arrow::PrettyPrintOptions options{0, 10, 2, "null", true};
    DCHECK_OK(PrettyPrint(*schema_.get(), options, &ss));

    ss << "Expressions: [";
    bool first = true;
    for (auto& expr : expressions_as_strings_) {
      if (first) {
        first = false;
      } else {
        ss << ", ";
      }

      ss << expr;
    }
    ss << "]";
    return ss.str();
  }

 private:
  void UpdateUniqifier(const std::string& expr) {
    if (uniqifier_ == 0) {
      // caching of expressions with re2 patterns causes lock contention. So, use
      // multiple instances to reduce contention.
      if (expr.find(" like(") != std::string::npos) {
        uniqifier_ = std::hash<std::thread::id>()(std::this_thread::get_id()) % 16;
      }
    }
  }

  const SchemaPtr schema_;
  const std::shared_ptr<Configuration> configuration_;
  SelectionVector::Mode mode_;
  std::vector<std::string> expressions_as_strings_;
  size_t hash_code_;
  uint32_t uniqifier_;
};
}  // namespace gandiva
#endif  // GANDIVA_PROJECTOR_CACHE_KEY_H

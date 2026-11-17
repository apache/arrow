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

#include <memory>
#include <string>
#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/configuration.h"
#include "gandiva/expression.h"
#include "gandiva/selection_vector.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Key of the compiled expression cache, built from the expression tree.
class GANDIVA_EXPORT ExpressionCacheKey {
 public:
  ExpressionCacheKey(SchemaPtr schema, std::shared_ptr<Configuration> configuration,
                     ExpressionVector expression_vector, SelectionVector::Mode mode);

  ExpressionCacheKey(SchemaPtr schema, std::shared_ptr<Configuration> configuration,
                     Expression& expression);

  void UpdateUniquifier(bool has_like);

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

    if (uniquifier_ != other.uniquifier_) {
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
  uint32_t uniquifier_;
  std::shared_ptr<Configuration> configuration_;
};

}  // namespace gandiva

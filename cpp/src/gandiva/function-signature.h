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

#ifndef GANDIVA_FUNCTION_SIGNATURE_H
#define GANDIVA_FUNCTION_SIGNATURE_H

#include <sstream>
#include <string>
#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/logging.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Signature for a function : includes the base name, input param types and
/// output types.
class GANDIVA_EXPORT FunctionSignature {
 public:
  FunctionSignature(const std::string& base_name, const DataTypeVector& param_types,
                    DataTypePtr ret_type)
      : base_name_(base_name), param_types_(param_types), ret_type_(ret_type) {
    DCHECK_GT(base_name.length(), 0);
    for (auto it = param_types_.begin(); it != param_types_.end(); it++) {
      DCHECK(*it);
    }
    DCHECK(ret_type);
  }

  bool operator==(const FunctionSignature& other) const;

  /// calculated based on base_name, datatpype id of parameters and datatype id
  /// of return type.
  std::size_t Hash() const;

  DataTypePtr ret_type() const { return ret_type_; }

  const std::string& base_name() const { return base_name_; }

  DataTypeVector param_types() const { return param_types_; }

  std::string ToString() const;

 private:
  bool DataTypeEquals(const DataTypePtr left, const DataTypePtr right) const {
    if (left->id() == right->id()) {
      switch (left->id()) {
        case arrow::Type::DECIMAL: {
          // For decimal types, the precision/scale isn't part of the signature.
          auto dleft = arrow::internal::checked_cast<arrow::DecimalType*>(left.get());
          auto dright = arrow::internal::checked_cast<arrow::DecimalType*>(right.get());
          return (dleft != NULL) && (dright != NULL) &&
                 (dleft->byte_width() == dright->byte_width());
        }
        default:
          return left->Equals(right);
      }
    } else {
      return false;
    }
  }

  std::string base_name_;
  DataTypeVector param_types_;
  DataTypePtr ret_type_;
};

}  // namespace gandiva

#endif  // GANDIVA_FUNCTION_SIGNATURE_H

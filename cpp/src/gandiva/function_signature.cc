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

#include "gandiva/function_signature.h"

#include <cstddef>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/checked_cast.h"
#include "arrow/util/hash_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

using arrow::internal::AsciiEqualsCaseInsensitive;
using arrow::internal::AsciiToLower;
using arrow::internal::checked_cast;
using arrow::internal::hash_combine;

namespace gandiva {

bool DataTypeEquals(const DataTypePtr& left, const DataTypePtr& right) {
  if (left->id() == right->id()) {
    switch (left->id()) {
      case arrow::Type::DECIMAL: {
        // For decimal types, the precision/scale isn't part of the signature.
        auto dleft = checked_cast<arrow::DecimalType*>(left.get());
        auto dright = checked_cast<arrow::DecimalType*>(right.get());
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

FunctionSignature::FunctionSignature(std::string base_name, DataTypeVector param_types,
                                     DataTypePtr ret_type)
    : base_name_(std::move(base_name)),
      param_types_(std::move(param_types)),
      ret_type_(std::move(ret_type)) {
  DCHECK_GT(base_name_.length(), 0);
  for (auto it = param_types_.begin(); it != param_types_.end(); it++) {
    DCHECK(*it);
  }
  DCHECK(ret_type_);
}

bool FunctionSignature::operator==(const FunctionSignature& other) const {
  if (param_types_.size() != other.param_types_.size() ||
      !DataTypeEquals(ret_type_, other.ret_type_) ||
      !AsciiEqualsCaseInsensitive(base_name_, other.base_name_)) {
    return false;
  }

  for (size_t idx = 0; idx < param_types_.size(); idx++) {
    if (!DataTypeEquals(param_types_[idx], other.param_types_[idx])) {
      return false;
    }
  }
  return true;
}

/// calculated based on name, datatype id of parameters and datatype id
/// of return type.
std::size_t FunctionSignature::Hash() const {
  static const size_t kSeedValue = 17;
  size_t result = kSeedValue;
  hash_combine(result, AsciiToLower(base_name_));
  hash_combine(result, static_cast<size_t>(ret_type_->id()));
  // not using hash_range since we only want to include the id from the data type
  for (auto& param_type : param_types_) {
    hash_combine(result, static_cast<size_t>(param_type->id()));
  }
  return result;
}

std::string FunctionSignature::ToString() const {
  std::stringstream s;

  s << ret_type_->ToString() << " " << base_name_ << "(";
  for (uint32_t i = 0; i < param_types_.size(); i++) {
    if (i > 0) {
      s << ", ";
    }

    s << param_types_[i]->ToString();
  }

  s << ")";
  return s.str();
}

}  // namespace gandiva

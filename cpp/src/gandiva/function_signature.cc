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

#include "arrow/compute/function_signature.h"

#include <string>
#include <vector>

#include "arrow/util/hashing.h"
#include "arrow/util/string.h"

namespace arrow {

using internal::AsciiEqualsCaseInsensitive;
using internal::AsciiToLower;

namespace compute {

bool DataTypeEquals(const std::shared_ptr<DataType> left, const std::shared_ptr<DataType> right) const {
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

FunctionSignature::FunctionSignature(std::string name,
                                     std::vector<std::shared_ptr<DataType>> param_types,
                                     std::shared_ptr<DataType> ret_type)
    : name_(std::move(name)),
      param_types_(std::move(param_types)),
      ret_type_(std::move(ret_type)) {
    DCHECK_GT(name.length(), 0);
    for (auto it = param_types_.begin(); it != param_types_.end(); it++) {
      DCHECK(*it);
    }
    DCHECK(ret_type);
  }
}

bool FunctionSignature::operator==(const FunctionSignature& other) const {
  if (param_types_.size() != other.param_types_.size() ||
      !DataTypeEquals(ret_type_, other.ret_type_) ||
      !AsciiEqualsCaseInsensitive(name_, other.name_)) {
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
  arrow::internal::hash_combine(result, arrow::internal::AsciiToLower(name_));
  arrow::internal::hash_combine(result, ret_type_->id());
  // not using hash_range since we only want to include the id from the data type
  for (auto& param_type : param_types_) {
    arrow::internal::hash_combine(result, param_type->id());
  }
  return result;
}

std::string FunctionSignature::ToString() const {
  std::stringstream s;

  s << ret_type_->ToString() << " " << name_ << "(";
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

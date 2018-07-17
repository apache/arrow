// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <gandiva/function_signature.h>

#include "boost/functional/hash.hpp"

namespace gandiva {

bool FunctionSignature::operator==(const FunctionSignature &other) const {
  if (param_types_.size() != other.param_types_.size() ||
      !DataTypeEquals(ret_type_, other.ret_type_) || base_name_ != other.base_name_) {
    return false;
  }

  for (size_t idx = 0; idx < param_types_.size(); idx++) {
    if (!DataTypeEquals(param_types_[idx], other.param_types_[idx])) {
      return false;
    }
  }
  return true;
}

/// calculated based on base_name, datatpype id of parameters and datatype id
/// of return type.
std::size_t FunctionSignature::Hash() const {
  static const size_t kSeedValue = 17;
  size_t result = kSeedValue;
  boost::hash_combine(result, base_name_);
  boost::hash_combine(result, ret_type_->id());
  // not using hash_range since we only want to include the id from the data type
  for (auto &param_type : param_types_) {
    boost::hash_combine(result, param_type->id());
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

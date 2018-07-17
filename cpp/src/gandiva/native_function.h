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

#ifndef GANDIVA_NATIVE_FUNCTION_H
#define GANDIVA_NATIVE_FUNCTION_H

#include <memory>
#include <string>
#include <vector>

#include "gandiva/function_signature.h"

namespace gandiva {

enum ResultNullableType {
  /// result validity is an intersection of the validity of the children.
  RESULT_NULL_IF_NULL,
  /// result is always valid.
  RESULT_NULL_NEVER,
  /// result validity depends on some internal logic.
  RESULT_NULL_INTERNAL,
};

/// \brief Holder for the mapping from a function in an expression to a
/// precompiled function.
class NativeFunction {
 public:
  const FunctionSignature &signature() const { return signature_; }
  std::string pc_name() const { return pc_name_; }
  ResultNullableType result_nullable_type() const { return result_nullable_type_; }
  bool param_null_safe() const { return param_null_safe_; }

 private:
  NativeFunction(const std::string &base_name, const DataTypeVector &param_types,
                 DataTypePtr ret_type, bool param_null_safe,
                 const ResultNullableType &result_nullable_type,
                 const std::string &pc_name)
      : signature_(base_name, param_types, ret_type),
        param_null_safe_(param_null_safe),
        result_nullable_type_(result_nullable_type),
        pc_name_(pc_name) {}

  FunctionSignature signature_;

  /// attributes
  bool param_null_safe_;
  ResultNullableType result_nullable_type_;

  /// pre-compiled function name.
  std::string pc_name_;

  friend class FunctionRegistry;
};

}  // end namespace gandiva

#endif  // GANDIVA_NATIVE_FUNCTION_H

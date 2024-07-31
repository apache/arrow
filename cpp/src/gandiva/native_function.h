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

#include <memory>
#include <string>
#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/function_signature.h"
#include "gandiva/visibility.h"

namespace gandiva {

enum ResultNullableType {
  /// result validity is an intersection of the validity of the children.
  kResultNullIfNull,
  /// result is always valid.
  kResultNullNever,
  /// result validity depends on some internal logic.
  kResultNullInternal,
};

/// \brief Holder for the mapping from a function in an expression to a
/// precompiled function.
class GANDIVA_EXPORT NativeFunction {
 public:
  // function attributes.
  static constexpr int32_t kNeedsContext = (1 << 1);
  static constexpr int32_t kNeedsFunctionHolder = (1 << 2);
  static constexpr int32_t kCanReturnErrors = (1 << 3);

  const std::vector<FunctionSignature>& signatures() const { return signatures_; }
  std::string pc_name() const { return pc_name_; }
  ResultNullableType result_nullable_type() const { return result_nullable_type_; }

  bool NeedsContext() const { return (flags_ & kNeedsContext) != 0; }
  bool NeedsFunctionHolder() const { return (flags_ & kNeedsFunctionHolder) != 0; }
  bool CanReturnErrors() const { return (flags_ & kCanReturnErrors) != 0; }

  NativeFunction(const std::string& base_name, const std::vector<std::string>& aliases,
                 const DataTypeVector& param_types, const DataTypePtr& ret_type,
                 const ResultNullableType& result_nullable_type, std::string pc_name,
                 int32_t flags = 0)
      : signatures_(),
        flags_(flags),
        result_nullable_type_(result_nullable_type),
        pc_name_(std::move(pc_name)) {
    signatures_.emplace_back(base_name, param_types, ret_type);
    for (auto& func_name : aliases) {
      signatures_.emplace_back(func_name, param_types, ret_type);
    }
  }

 private:
  std::vector<FunctionSignature> signatures_;

  /// attributes
  int32_t flags_;
  ResultNullableType result_nullable_type_;

  /// pre-compiled function name.
  std::string pc_name_;
};

}  // end namespace gandiva

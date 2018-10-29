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

#ifndef GANDIVA_FUNCTION_HOLDER_REGISTRY_H
#define GANDIVA_FUNCTION_HOLDER_REGISTRY_H

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "arrow/status.h"

#include "gandiva/function_holder.h"
#include "gandiva/like_holder.h"
#include "gandiva/node.h"
#include "gandiva/to_date_holder.h"

namespace gandiva {

#define LAMBDA_MAKER(derived)                               \
  [](const FunctionNode& node, FunctionHolderPtr* holder) { \
    std::shared_ptr<derived> derived_instance;              \
    auto status = derived::Make(node, &derived_instance);   \
    if (status.ok()) {                                      \
      *holder = derived_instance;                           \
    }                                                       \
    return status;                                          \
  }

/// Static registry of function holders.
class FunctionHolderRegistry {
 public:
  using maker_type = std::function<Status(const FunctionNode&, FunctionHolderPtr*)>;
  using map_type = std::unordered_map<std::string, maker_type>;

  static Status Make(const std::string& name, const FunctionNode& node,
                     FunctionHolderPtr* holder) {
    auto found = makers().find(name);
    if (found == makers().end()) {
      return Status::Invalid("function holder not registered for function " + name);
    }

    return found->second(node, holder);
  }

 private:
  static map_type& makers() {
    static map_type maker_map = {
        {"like", LAMBDA_MAKER(LikeHolder)},
        {"to_date", LAMBDA_MAKER(ToDateHolder)},
    };
    return maker_map;
  }
};

}  // namespace gandiva

#endif  // GANDIVA_FUNCTION_HOLDER_REGISTRY_H

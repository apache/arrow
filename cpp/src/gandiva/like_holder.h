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

#ifndef GANDIVA_LIKE_HOLDER_H
#define GANDIVA_LIKE_HOLDER_H

#include <memory>
#include <string>

#include <re2/re2.h>
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/status.h"

namespace gandiva {

#ifdef GDV_HELPERS
namespace helpers {
#endif

/// Function Holder for SQL 'like'
class LikeHolder : public FunctionHolder {
 public:
  ~LikeHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<LikeHolder>* holder);

  static Status Make(const std::string& sql_pattern, std::shared_ptr<LikeHolder>* holder);

  // Try and optimise a function node with a "like" pattern.
  static const FunctionNode TryOptimize(const FunctionNode& node);

  /// Return true if the data matches the pattern.
  bool operator()(const std::string& data) { return RE2::FullMatch(data, regex_); }

 private:
  explicit LikeHolder(const std::string& pattern) : pattern_(pattern), regex_(pattern) {}

  std::string pattern_;  // posix pattern string, to help debugging
  RE2 regex_;            // compiled regex for the pattern

  static RE2 starts_with_regex_;  // pre-compiled pattern for matching starts_with
  static RE2 ends_with_regex_;    // pre-compiled pattern for matching ends_with
};

#ifdef GDV_HELPERS
}  // namespace helpers
#endif

}  // namespace gandiva

#endif  // GANDIVA_LIKE_HOLDER_H

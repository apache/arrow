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

#ifndef GANDIVA_REGEXP_MATCHES_HOLDER_H
#define GANDIVA_REGEXP_MATCHES_HOLDER_H

#include <string>
#include <memory>

#include <re2/re2.h>

#include "gandiva/like_holder.h"

namespace gandiva {

/// Function Holder for 'regexp_matches' and 'regexp_like' functions
class GANDIVA_EXPORT RegexpMatchesHolder : public LikeHolder {
 public:
  ~RegexpMatchesHolder() override = default;

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<RegexpMatchesHolder>* holder);

  static Status Make(const std::string& pcre_pattern,
                     std::shared_ptr<RegexpMatchesHolder>* holder);

  // Try and optimise a function node with a "regexp_matches" pattern.
  static const FunctionNode TryOptimize(const FunctionNode& node);

  /// Return true if there is a match in the data.
  bool operator()(const std::string& data) override {
    return RE2::PartialMatch(data, regex_);
  }

 private:
  explicit RegexpMatchesHolder(const std::string& pattern) :
                               pattern_(pattern), regex_(pattern) {}

  std::string pattern_;  // posix pattern string, to help debugging
  RE2 regex_;            // compiled regex for the pattern

  static RE2 starts_with_regex_;  // pre-compiled pattern for matching starts_with
  static RE2 ends_with_regex_;    // pre-compiled pattern for matching ends_with
};
} // namespace gandiva

#endif //GANDIVA_REGEXP_MATCHES_HOLDER_H

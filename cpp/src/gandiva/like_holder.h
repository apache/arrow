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

#include <re2/re2.h>

#include <memory>
#include <string>

#include "arrow/status.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/regex_util.h"
#include "gandiva/visibility.h"

namespace gandiva {

class GANDIVA_EXPORT RegexpMatchesHolder : public FunctionHolder {
 public:
  static Status Make(const FunctionNode& node,
                     std::shared_ptr<RegexpMatchesHolder>* holder);

  static Status Make(const std::string& pcre_pattern,
                     std::shared_ptr<RegexpMatchesHolder>* holder);

  static Status Make(const std::string& pcre_pattern,
                     std::shared_ptr<RegexpMatchesHolder>* holder,
                     RE2::Options regex_ops);

  /// Try and optimise a function node with a "regexp_matches" pattern.
  static const FunctionNode TryOptimize(const FunctionNode& node);

  /// Return true if there is a match in the data.
  bool operator()(const std::string& data) { return RE2::PartialMatch(data, regex_); }

 protected:
  static Status ValidateArguments(const FunctionNode& node);
  static Result<std::string> GetPattern(const FunctionNode& node);

 private:
  explicit RegexpMatchesHolder(const std::string& pattern)
      : pattern_(pattern), regex_(pattern) {}

  RegexpMatchesHolder(const std::string& pattern, RE2::Options regex_op)
      : pattern_(pattern), regex_(pattern, regex_op) {}

  std::string pattern_;  // posix pattern string, to help debugging
  RE2 regex_;            // compiled regex for the pattern

  static RE2 starts_with_regex_;  // pre-compiled pattern for matching starts_with
  static RE2 ends_with_regex_;    // pre-compiled pattern for matching ends_with
  static RE2 is_substr_regex_;    // pre-compiled pattern for matching is_substr
};

class GANDIVA_EXPORT SQLLikeHolder : public RegexpMatchesHolder {
 public:
  static Status Make(const FunctionNode& node, std::shared_ptr<SQLLikeHolder>* holder);

  static Status Make(const std::string& sql_pattern,
                     std::shared_ptr<SQLLikeHolder>* holder);

  static Status Make(const std::string& sql_pattern, const std::string& escape_char,
                     std::shared_ptr<SQLLikeHolder>* holder);

  static Status Make(const std::string& sql_pattern,
                     std::shared_ptr<SQLLikeHolder>* holder, RE2::Options regex_ops);

  static Status Make(const std::string& sql_pattern, const std::string& escape_char,
                     std::shared_ptr<SQLLikeHolder>* holder, RE2::Options regex_ops);

  /// Try and optimise a function node with a "like" pattern.
  static const FunctionNode TryOptimize(const FunctionNode& node);

 protected:
  static Result<std::string> GetPattern(const FunctionNode& node);
  static Result<std::string> GetPattern(const std::string& sql_pattern,
                                        const std::string& escape_char);
  static Result<std::string> GetSQLPattern(const FunctionNode& node);
  static Result<std::string> GetEscapeChar(const FunctionNode& node);
};

}  // namespace gandiva

#endif  // GANDIVA_REGEXP_MATCHES_HOLDER_H

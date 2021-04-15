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

#include <re2/re2.h>

#include <memory>
#include <string>

#include "arrow/status.h"
#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Function Holder for SQL 'like'
class GANDIVA_EXPORT LikeHolder : public FunctionHolder {
 public:
  ~LikeHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<LikeHolder>* holder);

  static Status Make(const std::string& sql_pattern, std::shared_ptr<LikeHolder>* holder);

  static Status Make(const std::string& sql_pattern, const std::string& escape_char,
                     std::shared_ptr<LikeHolder>* holder);

  static Status Make(const std::string& sql_pattern, std::shared_ptr<LikeHolder>* holder,
                     RE2::Options regex_op);

  // Try and optimise a function node with a "like" pattern.
  static const FunctionNode TryOptimize(const FunctionNode& node);

  /// Return true if the data matches the pattern.
  bool operator()(const std::string& data) { return RE2::FullMatch(data, regex_); }

 private:
  explicit LikeHolder(const std::string& pattern) : pattern_(pattern), regex_(pattern) {}

  LikeHolder(const std::string& pattern, RE2::Options regex_op)
      : pattern_(pattern), regex_(pattern, regex_op) {}

  std::string pattern_;  // posix pattern string, to help debugging
  RE2 regex_;            // compiled regex for the pattern

  static RE2 starts_with_regex_;  // pre-compiled pattern for matching starts_with
  static RE2 ends_with_regex_;    // pre-compiled pattern for matching ends_with
  static RE2 is_substr_regex_;    // pre-compiled pattern for matching is_substr
};

/// Function Holder for 'regexp_extract' function
class GANDIVA_EXPORT ExtractHolder : public FunctionHolder {
 public:
  ~ExtractHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<ExtractHolder>* holder);

  static Status Make(const std::string& sql_pattern,
                     std::shared_ptr<ExtractHolder>* holder);

  /// Extracts the matching text from a string using a regex
  const char* operator()(ExecutionContext* ctx, const char* user_input,
                         int32_t user_input_len, int32_t extract_index,
                         int32_t* out_length);

 private:
  // The pattern must be enclosed inside an outside group to be able to catch the string
  // piece that matched with the entire regex when the user define the group "0". It is
  // used because the RE2 library does not provide that defined behavior by default.
  explicit ExtractHolder(const std::string& pattern) : regex_("(" + pattern + ")") {
    num_groups_pattern_ = regex_.NumberOfCapturingGroups();
  }

  RE2 regex_;                   // compiled regex for the pattern
  int32_t num_groups_pattern_;  // number of groups that user defined inside the regex
};

}  // namespace gandiva

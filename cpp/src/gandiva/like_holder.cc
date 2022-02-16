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

#include "gandiva/like_holder.h"

#include <regex>
#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {
RE2 LikeHolder::starts_with_regex_(R"(([^\.\*])*\.\*)");
RE2 LikeHolder::ends_with_regex_(R"(\.\*([^\.\*])*)");
RE2 LikeHolder::is_substr_regex_(R"(\.\*([^\.\*])*\.\*)");

std::string& RemovePatternEscapeChars(const FunctionNode& node, std::string& pattern) {
  if (node.children().size() != 2) {
    auto escape_char = dynamic_cast<LiteralNode*>(node.children().at(2).get());
    pattern.erase(std::remove(pattern.begin(), pattern.end(),
                              arrow::util::get<std::string>(escape_char->holder()).at(0)),
                  pattern.end());  // remove escape chars
  } else {
    pattern.erase(std::remove(pattern.begin(), pattern.end(), '\\'), pattern.end());
  }
  return pattern;
}

// Short-circuit pattern matches for the following common sub cases :
// - starts_with, ends_with and is_substr
const FunctionNode LikeHolder::TryOptimize(const FunctionNode& node) {
  std::shared_ptr<LikeHolder> holder;
  auto status = Make(node, &holder);
  if (status.ok()) {
    std::string& pattern = holder->pattern_;
    auto literal_type = node.children().at(1)->return_type();

    if (RE2::FullMatch(pattern, starts_with_regex_)) {
      auto prefix = pattern.substr(0, pattern.length() - 2);  // trim .*
      auto parsed_prefix = RemovePatternEscapeChars(node, prefix);
      auto prefix_node = std::make_shared<LiteralNode>(
          literal_type, LiteralHolder(parsed_prefix), false);
      return FunctionNode("starts_with", {node.children().at(0), prefix_node},
                          node.return_type());
    } else if (RE2::FullMatch(pattern, ends_with_regex_)) {
      auto suffix = pattern.substr(2);  // skip .*
      auto parsed_suffix = RemovePatternEscapeChars(node, suffix);
      auto suffix_node = std::make_shared<LiteralNode>(
          literal_type, LiteralHolder(parsed_suffix), false);
      return FunctionNode("ends_with", {node.children().at(0), suffix_node},
                          node.return_type());
    } else if (RE2::FullMatch(pattern, is_substr_regex_)) {
      auto substr =
          pattern.substr(2, pattern.length() - 4);  // trim starting and ending .*
      auto parsed_substr = RemovePatternEscapeChars(node, substr);
      auto substr_node = std::make_shared<LiteralNode>(
          literal_type, LiteralHolder(parsed_substr), false);
      return FunctionNode("is_substr", {node.children().at(0), substr_node},
                          node.return_type());
    }
  }

  // Could not optimize, return original node.
  return node;
}

Status LikeHolder::Make(const FunctionNode& node, std::shared_ptr<LikeHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() != 2 && node.children().size() != 3,
                  Status::Invalid("'like' function requires two or three parameters"));

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr,
      Status::Invalid("'like' function requires a literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !IsArrowStringLiteral(literal_type),
      Status::Invalid(
          "'like' function requires a string literal as the second parameter"));

  RE2::Options regex_op;
  if (node.descriptor()->name() == "ilike") {
    regex_op.set_case_sensitive(false);  // set case-insensitive for ilike function.

    return Make(arrow::util::get<std::string>(literal->holder()), holder, regex_op);
  }
  if (node.children().size() == 2) {
    return Make(arrow::util::get<std::string>(literal->holder()), holder);
  } else {
    auto escape_char = dynamic_cast<LiteralNode*>(node.children().at(2).get());
    ARROW_RETURN_IF(
        escape_char == nullptr,
        Status::Invalid("'like' function requires a literal as the third parameter"));

    auto escape_char_type = escape_char->return_type()->id();
    ARROW_RETURN_IF(
        !IsArrowStringLiteral(escape_char_type),
        Status::Invalid(
            "'like' function requires a string literal as the third parameter"));
    return Make(arrow::util::get<std::string>(literal->holder()),
                arrow::util::get<std::string>(escape_char->holder()), holder);
  }
}

Status LikeHolder::Make(const std::string& sql_pattern,
                        std::shared_ptr<LikeHolder>* holder) {
  std::string pcre_pattern;
  ARROW_RETURN_NOT_OK(RegexUtil::SqlLikePatternToPcre(sql_pattern, pcre_pattern));

  auto lholder = std::shared_ptr<LikeHolder>(new LikeHolder(pcre_pattern));
  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", pcre_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}

Status LikeHolder::Make(const std::string& sql_pattern, const std::string& escape_char,
                        std::shared_ptr<LikeHolder>* holder) {
  ARROW_RETURN_IF(escape_char.length() > 1,
                  Status::Invalid("The length of escape char ", escape_char,
                                  " in 'like' function is greater than 1"));
  std::string pcre_pattern;
  if (escape_char.length() == 1) {
    ARROW_RETURN_NOT_OK(
        RegexUtil::SqlLikePatternToPcre(sql_pattern, escape_char.at(0), pcre_pattern));
  } else {
    ARROW_RETURN_NOT_OK(RegexUtil::SqlLikePatternToPcre(sql_pattern, pcre_pattern));
  }

  auto lholder = std::shared_ptr<LikeHolder>(new LikeHolder(pcre_pattern));
  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", pcre_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}

Status LikeHolder::Make(const std::string& sql_pattern,
                        std::shared_ptr<LikeHolder>* holder, RE2::Options regex_op) {
  std::string pcre_pattern;
  ARROW_RETURN_NOT_OK(RegexUtil::SqlLikePatternToPcre(sql_pattern, pcre_pattern));

  std::shared_ptr<LikeHolder> lholder;
  lholder = std::shared_ptr<LikeHolder>(new LikeHolder(pcre_pattern, regex_op));

  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", pcre_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}
}  // namespace gandiva

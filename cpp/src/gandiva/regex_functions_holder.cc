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

#include "gandiva/regex_functions_holder.h"
#include <regex>
#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {

RE2 RegexpExpressionsHolder::starts_with_regex_(R"(\^([\w\s]+)(\.\*)?)");
RE2 RegexpExpressionsHolder::ends_with_regex_(R"((\.\*)?([\w\s]+)\$)");
RE2 RegexpExpressionsHolder::is_substr_regex_(R"((\w|\s)*)");

// Short-circuit pattern matches for the three common sub cases :
// - starts_with, ends_with, and contains.
const FunctionNode RegexpExpressionsHolder::TryOptimize(const FunctionNode& node) {
  std::shared_ptr<RegexpExpressionsHolder> holder;
  auto status = Make(node, &holder);
  if (status.ok()) {
    std::string& pattern = holder->pattern_;
    auto literal_type = node.children().at(1)->return_type();
    std::string substr;
    if (RE2::FullMatch(pattern, starts_with_regex_, &substr)) {
      auto prefix_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(substr), false);
      return FunctionNode("starts_with", {node.children().at(0), prefix_node},
                          node.return_type());
    } else if (RE2::FullMatch(pattern, ends_with_regex_, (void*)NULL, &substr)) {
      auto suffix_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(substr), false);
      return FunctionNode("ends_with", {node.children().at(0), suffix_node},
                          node.return_type());
    } else if (RE2::FullMatch(pattern, is_substr_regex_)) {
      auto substr_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(pattern), false);
      return FunctionNode("is_substr", {node.children().at(0), substr_node},
                          node.return_type());
    }
  }

  // Could not optimize, return original node.
  return node;
}

const FunctionNode SQLLikeHolder::TryOptimize(const FunctionNode& node) {
  if (node.descriptor()->name() == "ilike") {
    // Optimizations don't work for case-insensitive matching
    return node;
  }

  std::string pcre_pattern;
  auto pattern_result = GetPattern(node);
  if (!pattern_result.ok()) {
    return node;
  } else {
    pcre_pattern = pattern_result.ValueOrDie();
  }

  auto literal_type = node.children().at(1)->return_type();
  auto pcre_node =
      std::make_shared<LiteralNode>(literal_type, LiteralHolder(pcre_pattern), false);

  // There is only one optimizer and it is inside the
  // "RegexpExpressionsHolder::TryOptimize" method. So, a new node redirecting to
  // "regexp_matches" (would could be 'regexp_like' or 'rlike') is created to call the
  // "RegexpExpressionsHolder::TryOptimize" method.
  auto new_node = FunctionNode("regexp_matches", {node.children().at(0), pcre_node},
                               node.return_type());
  auto optimized_node = RegexpExpressionsHolder::TryOptimize(new_node);

  // If it couldn't optimize (starts_with_regex_, ends_with_regex_, is_substr_regex_), it
  // doesn't want to use the new node created, but the original one that arrived via
  // parameter.
  if (optimized_node.descriptor()->name() != "regexp_matches") {
    return optimized_node;
  } else {
    return node;
  }
}

Result<std::string> RegexpExpressionsHolder::GetPattern(const FunctionNode& node) {
  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  auto pattern = arrow::util::get<std::string>(literal->holder());
  return pattern;
}

Result<std::string> SQLLikeHolder::GetPattern(const FunctionNode& node) {
  std::string sql_pattern;
  ARROW_ASSIGN_OR_RAISE(sql_pattern, GetSQLPattern(node));

  std::string escape_char;
  ARROW_ASSIGN_OR_RAISE(escape_char, GetEscapeChar(node));

  return GetPattern(sql_pattern, escape_char);
}

Result<std::string> SQLLikeHolder::GetPattern(const std::string& sql_pattern,
                                              const std::string& escape_char) {
  std::string pcre_pattern;
  if (escape_char.length() == 1) {
    ARROW_RETURN_NOT_OK(
        RegexUtil::SqlLikePatternToPcre(sql_pattern, escape_char.at(0), pcre_pattern));
  } else {
    ARROW_RETURN_NOT_OK(RegexUtil::SqlLikePatternToPcre(sql_pattern, pcre_pattern));
  }
  return pcre_pattern;
}

Status RegexpExpressionsHolder::Make(const std::string& pcre_pattern,
                                     std::shared_ptr<RegexpExpressionsHolder>* holder) {
  auto lholder =
      std::shared_ptr<RegexpExpressionsHolder>(new RegexpExpressionsHolder(pcre_pattern));
  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", pcre_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}

Status RegexpExpressionsHolder::Make(const std::string& pcre_pattern,
                                     std::shared_ptr<RegexpExpressionsHolder>* holder,
                                     RE2::Options regex_ops) {
  auto lholder = std::shared_ptr<RegexpExpressionsHolder>(
      new RegexpExpressionsHolder(pcre_pattern, regex_ops));
  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", pcre_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}

Status RegexpExpressionsHolder::Make(const FunctionNode& node,
                                     std::shared_ptr<RegexpExpressionsHolder>* holder) {
  // Add regexp_matches validation
  ARROW_RETURN_IF(node.children().size() != 2,
                  Status::Invalid("'regexp_matches' function requires two parameters"));

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr,
      Status::Invalid(
          "'regexp_matches' function requires a literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !IsArrowStringLiteral(literal_type),
      Status::Invalid(
          "'regexp_matches' function requires a string literal as the second parameter"));

  ARROW_ASSIGN_OR_RAISE(std::string pattern, GetPattern(node));

  return Make(pattern, holder);
}

Result<std::string> SQLLikeHolder::GetSQLPattern(const FunctionNode& node) {
  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr,
      Status::Invalid("'like' function requires a literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !IsArrowStringLiteral(literal_type),
      Status::Invalid(
          "'like' function requires a string literal as the second parameter"));

  return arrow::util::get<std::string>(literal->holder());
}

Result<std::string> SQLLikeHolder::GetEscapeChar(const FunctionNode& node) {
  std::string escape_char = "";
  if (node.children().size() == 3) {
    auto escape_node = dynamic_cast<LiteralNode*>(node.children().at(2).get());
    ARROW_RETURN_IF(
        escape_node == nullptr,
        Status::Invalid("'like' function requires a literal as the third parameter"));

    auto escape_char_type = escape_node->return_type()->id();
    ARROW_RETURN_IF(
        !IsArrowStringLiteral(escape_char_type),
        Status::Invalid(
            "'like' function requires a string literal as the third parameter"));
    escape_char = arrow::util::get<std::string>(escape_node->holder());
  }
  return escape_char;
}

Status SQLLikeHolder::Make(const FunctionNode& node,
                           std::shared_ptr<SQLLikeHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() != 2 && node.children().size() != 3,
                  Status::Invalid("'like' function requires two or three parameters"));

  std::string sql_pattern;
  ARROW_ASSIGN_OR_RAISE(sql_pattern, GetSQLPattern(node));

  std::string escape_char;
  ARROW_ASSIGN_OR_RAISE(escape_char, GetEscapeChar(node));

  RE2::Options regex_op;
  if (node.descriptor()->name() == "ilike") {
    regex_op.set_case_sensitive(false);  // set case-insensitive for ilike function.
  }

  return Make(sql_pattern, escape_char, holder, regex_op);
}

Status SQLLikeHolder::Make(const std::string& sql_pattern,
                           std::shared_ptr<SQLLikeHolder>* holder) {
  RE2::Options regex_op;
  return Make(sql_pattern, "", holder, regex_op);
}

Status SQLLikeHolder::Make(const std::string& sql_pattern, const std::string& escape_char,
                           std::shared_ptr<SQLLikeHolder>* holder) {
  RE2::Options regex_op;
  return Make(sql_pattern, escape_char, holder, regex_op);
}

Status SQLLikeHolder::Make(const std::string& sql_pattern,
                           std::shared_ptr<SQLLikeHolder>* holder,
                           RE2::Options regex_op) {
  return Make(sql_pattern, "", holder, regex_op);
}

Status SQLLikeHolder::Make(const std::string& sql_pattern, const std::string& escape_char,
                           std::shared_ptr<SQLLikeHolder>* holder,
                           RE2::Options regex_op) {
  ARROW_RETURN_IF(escape_char.length() > 1,
                  Status::Invalid("The length of escape char ", escape_char,
                                  " in 'like' function is greater than 1"));
  std::string pcre_pattern;
  ARROW_ASSIGN_OR_RAISE(pcre_pattern, GetPattern(sql_pattern, escape_char));

  std::shared_ptr<RegexpExpressionsHolder> base_holder;
  ARROW_RETURN_NOT_OK(
      RegexpExpressionsHolder::Make(pcre_pattern, &base_holder, regex_op));

  *holder = std::static_pointer_cast<SQLLikeHolder>(base_holder);
  return Status::OK();
}

Status ReplaceHolder::Make(const FunctionNode& node,
                           std::shared_ptr<ReplaceHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() != 3,
                  Status::Invalid("'replace' function requires three parameters"));

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr,
      Status::Invalid("'replace' function requires a literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !(literal_type == arrow::Type::STRING || literal_type == arrow::Type::BINARY),
      Status::Invalid(
          "'replace' function requires a string literal as the second parameter"));

  return Make(arrow::util::get<std::string>(literal->holder()), holder);
}

Status ReplaceHolder::Make(const std::string& sql_pattern,
                           std::shared_ptr<ReplaceHolder>* holder) {
  auto lholder = std::shared_ptr<ReplaceHolder>(new ReplaceHolder(sql_pattern));
  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", sql_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}

void ReplaceHolder::return_error(ExecutionContext* context, std::string& data,
                                 std::string& replace_string) {
  std::string err_msg = "Error replacing '" + replace_string + "' on the given string '" +
                        data + "' for the given pattern: " + pattern_;
  context->set_error_msg(err_msg.c_str());
}

Status ExtractHolder::Make(const FunctionNode& node,
                           std::shared_ptr<ExtractHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() != 3,
                  Status::Invalid("'extract' function requires three parameters"));

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr || !IsArrowStringLiteral(literal->return_type()->id()),
      Status::Invalid("'extract' function requires a literal as the second parameter"));

  return ExtractHolder::Make(arrow::util::get<std::string>(literal->holder()), holder);
}

Status ExtractHolder::Make(const std::string& sql_pattern,
                           std::shared_ptr<ExtractHolder>* holder) {
  auto lholder = std::shared_ptr<ExtractHolder>(new ExtractHolder(sql_pattern));
  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", sql_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}

const char* ExtractHolder::operator()(ExecutionContext* ctx, const char* user_input,
                                      int32_t user_input_len, int32_t extract_index,
                                      int32_t* out_length) {
  if (extract_index < 0 || extract_index >= num_groups_pattern_) {
    ctx->set_error_msg("Index to extract out of range");
    *out_length = 0;
    return "";
  }

  std::string user_input_as_str(user_input, user_input_len);

  // Create the vectors that will store the arguments to be captured by the regex
  // groups.
  std::vector<std::string> arguments_as_str(num_groups_pattern_);
  std::vector<RE2::Arg> arguments(num_groups_pattern_);
  std::vector<RE2::Arg*> arguments_ptrs(num_groups_pattern_);

  for (int32_t i = 0; i < num_groups_pattern_; i++) {
    // Bind argument to string from vector.
    arguments[i] = &arguments_as_str[i];
    // Save pointer to argument.
    arguments_ptrs[i] = &arguments[i];
  }

  re2::StringPiece piece(user_input_as_str);
  if (!RE2::FindAndConsumeN(&piece, regex_, arguments_ptrs.data(), num_groups_pattern_)) {
    *out_length = 0;
    return "";
  }

  auto out_str = arguments_as_str[extract_index];
  *out_length = static_cast<int32_t>(out_str.size());

  // This condition treats the case where the return is an empty string
  if (*out_length == 0) {
    return "";
  }

  char* result_buffer = reinterpret_cast<char*>(ctx->arena()->Allocate(*out_length));
  if (result_buffer == NULLPTR) {
    ctx->set_error_msg("Could not allocate memory for result");
    *out_length = 0;
    return "";
  }

  memcpy(result_buffer, out_str.data(), *out_length);
  return result_buffer;
}

}  // namespace gandiva

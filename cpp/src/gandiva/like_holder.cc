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

RE2 LikeHolder::starts_with_regex_(R"((\w|\s)*\.\*)");
RE2 LikeHolder::ends_with_regex_(R"(\.\*(\w|\s)*)");

// Short-circuit pattern matches for the two common sub cases :
// - starts_with and ends_with.
const FunctionNode LikeHolder::TryOptimize(const FunctionNode& node) {
  std::shared_ptr<LikeHolder> holder;
  auto status = Make(node, &holder);
  if (status.ok()) {
    std::string& pattern = holder->pattern_;
    auto literal_type = node.children().at(1)->return_type();

    if (RE2::FullMatch(pattern, starts_with_regex_)) {
      auto prefix = pattern.substr(0, pattern.length() - 2);  // trim .*
      auto prefix_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(prefix), false);
      return FunctionNode("starts_with", {node.children().at(0), prefix_node},
                          node.return_type());
    } else if (RE2::FullMatch(pattern, ends_with_regex_)) {
      auto suffix = pattern.substr(2);  // skip .*
      auto suffix_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(suffix), false);
      return FunctionNode("ends_with", {node.children().at(0), suffix_node},
                          node.return_type());
    }
  }

  // didn't hit any of the optimisation paths. return original.
  return node;
}

Status LikeHolder::Make(const FunctionNode& node, std::shared_ptr<LikeHolder>* holder) {
  if (node.children().size() != 2) {
    return Status::Invalid("'like' function requires two parameters");
  }

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  if (literal == nullptr) {
    return Status::Invalid("'like' function requires a literal as the second parameter");
  }

  auto literal_type = literal->return_type()->id();
  if (literal_type != arrow::Type::STRING && literal_type != arrow::Type::BINARY) {
    return Status::Invalid(
        "'like' function requires a string literal as the second parameter");
  }
  auto pattern = boost::get<std::string>(literal->holder());
  return Make(pattern, holder);
}

Status LikeHolder::Make(const std::string& sql_pattern,
                        std::shared_ptr<LikeHolder>* holder) {
  std::string pcre_pattern;
  auto status = RegexUtil::SqlLikePatternToPcre(sql_pattern, pcre_pattern);
  ARROW_RETURN_NOT_OK(status);

  auto lholder = std::shared_ptr<LikeHolder>(new LikeHolder(pcre_pattern));
  if (!lholder->regex_.ok()) {
    return Status::Invalid("building re2 regex failed for pattern " + pcre_pattern);
  }

  *holder = lholder;
  return Status::OK();
}

}  // namespace gandiva

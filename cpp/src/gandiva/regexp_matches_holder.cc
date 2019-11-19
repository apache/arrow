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

#include "gandiva/regexp_matches_holder.h"

#include <regex>
#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {

  RE2 RegexpMatchesHolder::starts_with_regex_(R"(\^([\w\s]+)(\.\*)?)");
  RE2 RegexpMatchesHolder::ends_with_regex_(R"((\.\*)?([\w\s]+)\$)");

  // Short-circuit pattern matches for the two common sub cases :
  // - starts_with and ends_with.
  const FunctionNode RegexpMatchesHolder::TryOptimize(const FunctionNode& node) {
    std::shared_ptr<RegexpMatchesHolder> holder;
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
      } else if (RE2::FullMatch(pattern, ends_with_regex_, (void *)NULL, &substr)) {
        auto suffix_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(substr), false);
        return FunctionNode("ends_with", {node.children().at(0), suffix_node},
                            node.return_type());
      }
    }

    // Could not optimize, return original node.
    return node;
  }

  Status RegexpMatchesHolder::Make(const FunctionNode& node,
                                   std::shared_ptr<RegexpMatchesHolder>* holder) {
    std::string pcre_pattern;
    ARROW_RETURN_NOT_OK(LikeHolder::Make(node, &pcre_pattern));
    return Make(pcre_pattern, holder);
  }

  Status RegexpMatchesHolder::Make(const std::string& pcre_pattern,
                          std::shared_ptr<RegexpMatchesHolder>* holder) {
    auto lholder = std::shared_ptr<RegexpMatchesHolder>(
      new RegexpMatchesHolder(pcre_pattern));
    ARROW_RETURN_IF(!lholder->regex_.ok(),
                    Status::Invalid("Building RE2 pattern '", pcre_pattern, "' failed"));

    *holder = lholder;
    return Status::OK();
  }

}  // namespace gandiva

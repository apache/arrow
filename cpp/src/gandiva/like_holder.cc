// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "codegen/like_holder.h"

#include <regex>
#include "codegen/node.h"
#include "codegen/regex_util.h"

namespace gandiva {

#ifdef GDV_HELPERS
namespace helpers {
#endif

Status LikeHolder::Make(const FunctionNode &node, std::shared_ptr<LikeHolder> *holder) {
  if (node.children().size() != 2) {
    return Status::Invalid("'like' function requires two parameters");
  }

  auto literal = dynamic_cast<LiteralNode *>(node.children().at(1).get());
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

Status LikeHolder::Make(const std::string &sql_pattern,
                        std::shared_ptr<LikeHolder> *holder) {
  std::string posix_pattern;
  auto status = RegexUtil::SqlLikePatternToPosix(sql_pattern, posix_pattern);
  GANDIVA_RETURN_NOT_OK(status);

  *holder = std::shared_ptr<LikeHolder>(new LikeHolder(posix_pattern));
  return Status::OK();
}

#ifdef GDV_HELPERS
}
#endif

}  // namespace gandiva

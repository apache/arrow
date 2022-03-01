//
// Created by frank on 01/03/2022.
//

#include "gandiva/parse_url_holder.h"
namespace gandiva {
RE2 ParseUrlHolder::url_regex_(
    R"(^((http[s]?|ftp):\/)?\/?([^:\/\s]+)((\/\w+)*\/)([\w\-\.]+[^#?\s]+)(.*)?(#[\w\-]+)?$)");

Status ParseUrlHolder::Make(const gandiva::FunctionNode& node,
                            std::shared_ptr<ParseUrlHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() != 2 && node.children().size() != 3,
                  Status::Invalid("'like' function requires two or three parameters"));
  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr,
      Status::Invalid("'parse_url' function requires a literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !IsArrowStringLiteral(literal_type),
      Status::Invalid(
          "'parse_url' function requires a string literal as the second parameter"));

  if (node.children().size() == 2) {
    return Make(arrow::util::get<std::string>(literal->holder()), holder);
  } else {
    auto key_literal = dynamic_cast<LiteralNode*>(node.children().at(2).get());
    ARROW_RETURN_IF(
        !IsArrowStringLiteral(key_literal->return_type()->id()) && key_literal == nullptr,
        Status::Invalid("'parse_url' function requires a string literal as the third "
                        "parameter for the query key"));
    return Make(arrow::util::get<std::string>(literal->holder()),
                arrow::util::get<std::string>(key_literal->holder()), holder);
  }
}
}  // namespace gandiva

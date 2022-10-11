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
#include <map>
#include <string>

#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "gandiva/arrow.h"
#include "gandiva/gandiva_aliases.h"

#include "gandiva/grammar.hh"
// Give Flex the prototype of yylex we want ...
#define YY_DECL gandiva::grammar::symbol_type yylex(gandiva::Parser& parser)
// ... and declare it for the parser's sake.
YY_DECL;

namespace gandiva {
/// \brief Conducting the whole scanning and parsing of gandiva expressions.
class Parser {
 public:
  explicit Parser(std::shared_ptr<arrow::Schema> schema) : schema_(std::move(schema)) {}

  /// Run the parser.
  Status Parse(const std::string& exp_str, NodePtr* node_ptr);
  gandiva::location& location() { return location_; }

 private:
  /// Handling the scanner.
  void scan_begin(const std::string& exp_str);
  void scan_end();

  /// This parser's schema
  std::shared_ptr<arrow::Schema> schema_;

  /// The token's location used by the scanner.
  gandiva::location location_;

  /// The result node pointer.
  NodePtr* node_ptr_{NULLPTR};

  /// error message
  std::string error_message_;

  /// grammar needs to set the result node ptr and error message.
  friend grammar;
};
}  // namespace gandiva

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

%skeleton "lalr1.cc" // -*- C++ -*-
%require "3.2"
%header

%define api.token.raw

%define api.token.constructor
%define api.value.type variant
%define parse.assert
%define api.namespace {gandiva}
%define api.parser.class {grammar}

%code requires {
  #include <string>
  #include "gandiva/gandiva_aliases.h"
  #include "gandiva/node.h"

  namespace gandiva{
    class Parser;
    class location;
    enum class IntegerSuffix {u8, u16, u32, u64, i8, i16, i32, i64};
    enum class FloatSuffix {f32, f64};

    struct IntegerLiteralWithSuffix {
      std::string text;
      IntegerSuffix suffix;
    };

    struct FloatLiteralWithSuffix {
      std::string text;
      FloatSuffix suffix;
    };
  }

  #define MAKE_LITERAL_NODE(output, input, ctype, arrow_type, func, loc)                        \
    try {                                                                                       \
      auto num = func(input);                                                                   \
      output = std::make_shared<gandiva::LiteralNode>(                                          \
          arrow_type, gandiva::LiteralHolder(static_cast<ctype>(num)), false);                  \
    } catch (const std::out_of_range& e) {                                                      \
      throw syntax_error(loc, "out of range");                                                  \
    } catch (const std::invalid_argument& e) {                                                  \
      throw syntax_error(loc, "invalid argument");                                              \
    }

  // Check numetic_limits if func return type and ctype mismatch
  // We check all integers for portability
  #define MAKE_LITERAL_NODE_CHECKED(output, input, ctype, arrow_type, func, loc)                \
    try {                                                                                       \
      auto num = func(input);                                                                   \
      if (num < std::numeric_limits<ctype>::min() || num > std::numeric_limits<ctype>::max()) { \
        throw syntax_error(loc, "out of range");                                                \
      }                                                                                         \
      output = std::make_shared<gandiva::LiteralNode>(                                          \
          arrow_type, gandiva::LiteralHolder(static_cast<ctype>(num)), false);                  \
    } catch (const std::out_of_range& e) {                                                      \
      throw syntax_error(loc, "out of range");                                                  \
    } catch (const std::invalid_argument& e) {                                                  \
      throw syntax_error(loc, "invalid argument");                                              \
    }
}

// The parsing context.
%param { Parser& parser}

%locations

%define parse.trace
%define parse.error detailed
%define parse.lac full

%code {
  #include "gandiva/parser.h"

}

%define api.token.prefix {TOK_}
%token
  IF      "if"
  ELSE    "else"
  IN      "in"
  NOT     "not"
  AND     "and"
  OR      "or"
  TRUE    "true"
  FALSE   "false"
  MINUS   "-"
  PLUS    "+"
  STAR    "*"
  SLASH   "/"
  LPAREN  "("
  RPAREN  ")"
  MODOLO  "%"
  POWER   "^"
;

%token <std::string> IDENTIFIER "identifier";
%token <std::string> INT_LITERAL "int_literal";
%token <IntegerLiteralWithSuffix> INT_LITERAL_WITH_SUFFIX "int_literal_with_suffix";
%token <std::string> FLOAT_LITERAL "float_literal";
%token <FloatLiteralWithSuffix> FLOAT_LITERAL_WITH_SUFFIX "float_literal_with_suffix";
%token <std::string> STRING_LITERAL "string_literal";
%nterm <NodePtr> exp;
%nterm <NodePtr> literal;
%nterm <NodePtr> field;

%%
%start exp;

%left "-";
%left "+";
%left "*";
%left "/";
%precedence NEG;
%left "^";
%left "(" ")";

exp:
  literal { *parser.node_ptr_ = $1; }
| field { *parser.node_ptr_ = $1; }
;

literal:
  INT_LITERAL { MAKE_LITERAL_NODE_CHECKED($$, $1, uint64_t, nullptr, std::stoull, @1); }
| INT_LITERAL_WITH_SUFFIX {
    switch ($1.suffix) {
      case IntegerSuffix::u8: MAKE_LITERAL_NODE_CHECKED($$, $1.text, uint8_t, arrow::uint8(), std::stoul, @1); break;
      case IntegerSuffix::u16: MAKE_LITERAL_NODE_CHECKED($$, $1.text, uint16_t, arrow::uint16(), std::stoul, @1); break;
      case IntegerSuffix::u32: MAKE_LITERAL_NODE_CHECKED($$, $1.text, uint32_t, arrow::uint32(), std::stoul, @1); break;
      case IntegerSuffix::u64: MAKE_LITERAL_NODE_CHECKED($$, $1.text, uint64_t, arrow::uint64(), std::stoull, @1); break;
      case IntegerSuffix::i8: MAKE_LITERAL_NODE_CHECKED($$, $1.text, int8_t, arrow::int8(), std::stoi, @1); break;
      case IntegerSuffix::i16: MAKE_LITERAL_NODE_CHECKED($$, $1.text, int16_t, arrow::int16(), std::stoi, @1); break;
      case IntegerSuffix::i32: MAKE_LITERAL_NODE_CHECKED($$, $1.text, int32_t, arrow::int32(), std::stol, @1); break;
      case IntegerSuffix::i64: MAKE_LITERAL_NODE_CHECKED($$, $1.text, int64_t, arrow::int64(), std::stoll, @1); break;
    }
  }
// negative literals need special consideration because of overflow issue, e.g. -128 is a valid i8 but 128 is not.
| "-" INT_LITERAL %prec NEG { MAKE_LITERAL_NODE_CHECKED($$, "-" + $2, int64_t, nullptr, std::stoll, @2); }
| "-" INT_LITERAL_WITH_SUFFIX %prec NEG {
    switch ($2.suffix) {
      case IntegerSuffix::i8: MAKE_LITERAL_NODE_CHECKED($$, "-" + $2.text, int8_t, arrow::int8(), std::stoi, @2); break;
      case IntegerSuffix::i16: MAKE_LITERAL_NODE_CHECKED($$, "-" + $2.text, int16_t, arrow::int16(), std::stoi, @2); break;
      case IntegerSuffix::i32: MAKE_LITERAL_NODE_CHECKED($$, "-" + $2.text, int32_t, arrow::int32(), std::stol, @2); break;
      case IntegerSuffix::i64: MAKE_LITERAL_NODE_CHECKED($$, "-" + $2.text, int64_t, arrow::int64(), std::stoll, @2); break;
      default: throw syntax_error(@2, "wrong suffix for nagative number"); break;
    }
  }
| FLOAT_LITERAL {MAKE_LITERAL_NODE($$, $1, double, nullptr, std::stod, @1)}
| FLOAT_LITERAL_WITH_SUFFIX {
    switch ($1.suffix) {
      case FloatSuffix::f32: MAKE_LITERAL_NODE($$, $1.text, float, arrow::float32(), std::stof, @1); break;
      case FloatSuffix::f64: MAKE_LITERAL_NODE($$, $1.text, double, arrow::float64(), std::stod, @1); break;
    }
  }
| "-" FLOAT_LITERAL %prec NEG {MAKE_LITERAL_NODE($$, "-" + $2, double, nullptr, std::stod, @2)}
| "-" FLOAT_LITERAL_WITH_SUFFIX %prec NEG {
    switch ($2.suffix) {
      case FloatSuffix::f32: MAKE_LITERAL_NODE($$, "-" + $2.text, float, arrow::float32(), std::stof, @2); break;
      case FloatSuffix::f64: MAKE_LITERAL_NODE($$, "-" + $2.text, double, arrow::float64(), std::stod, @2); break;
    }
  }
| STRING_LITERAL {
    std::string content = $1.substr(1, $1.size() - 2);
    $$ = std::make_shared<gandiva::LiteralNode>(arrow::utf8(), gandiva::LiteralHolder(content), false);
  }
| "true" {$$ = std::make_shared<gandiva::LiteralNode>(arrow::boolean(), gandiva::LiteralHolder(true), false);}
| "false" {$$ = std::make_shared<gandiva::LiteralNode>(arrow::boolean(), gandiva::LiteralHolder(false), false);}
;

field: 
  IDENTIFIER {
    auto field_ptr = parser.schema_->GetFieldByName($1);
    if (field_ptr == nullptr) {
      throw syntax_error(@1, "not defined in schema");
    }
    $$ = std::make_shared<gandiva::FieldNode>(field_ptr);
  }
;
%%

void gandiva::grammar::error (const location_type& l, const std::string& m) {
  std::stringstream sstream;
  sstream << l << ": " << m;
  parser.error_message_ = sstream.str();
}

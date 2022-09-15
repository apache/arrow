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
%require "3.8"
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
}

// The parsing context.
%param { Parser& parser}

%locations

/* %define parse.trace
%define parse.error detailed
%define parse.lac full */

%code {
  #include "gandiva/parser.h"

}

%define api.token.prefix {TOK_}
%token
  IF                          "if"
  ELSE                        "else"
  IN                          "in"
  NOT                         "not"
  AND                         "and"
  OR                          "or"
  TRUE                        "true"
  FALSE                       "false"
  MINUS                       "-"
  PLUS                        "+"
  STAR                        "*"
  SLASH                       "/"
  LPAREN                      "("
  RPAREN                      ")"
  MODOLO                      "%"
  BITWISE_OR                  "|"
  BITWISE_XOR                 "^"
  BITWISE_AND                 "&"
  BITWISE_NOT                 "~"
  EQUAL                       "==" 
  NOT_EQUAL                   "!="
  LESS_THAN                   "<" 
  LESS_THAN_OR_EQUAL_TO       "<=" 
  GREATER_THAN                ">" 
  GREATER_THAN_OR_EQUAL_TO    ">="
  COMMA                       ","
;

%token <std::string> IDENTIFIER "identifier";
%token <std::string> INT_LITERAL "int_literal";
%token <IntegerLiteralWithSuffix> INT_LITERAL_WITH_SUFFIX "int_literal_with_suffix";
%token <std::string> FLOAT_LITERAL "float_literal";
%token <FloatLiteralWithSuffix> FLOAT_LITERAL_WITH_SUFFIX "float_literal_with_suffix";
%token <std::string> STRING_LITERAL "string_literal";
%nterm <NodePtr> exp;
%nterm <NodePtr> term;
%nterm <NodePtr> literal;
%nterm <NodePtr> field;
%nterm <NodePtr> function;
%nterm <NodePtr> infix_function;
%nterm <NodePtr> named_function;
%nterm <NodeVector> args;
%nterm <NodePtr> arg;

%%
%start exp;

%left "or";
%left "and";
%left "|";
%left "^";
%left "&";

%left "==" "!=";
%left "<" "<=" ">" ">=";
%left "-" "+";
%left "*" "/" "%";
%precedence NEG;
%left "(" ")";

exp: term {*parser.node_ptr_ = std::move($1);}

term:
  literal {$$ = std::move($1);}
| field {$$ = std::move($1);}
| function {$$ = std::move($1);}
| "(" term ")" {$$ = std::move($2);}

literal:
  INT_LITERAL { MAKE_LITERAL_NODE($$, $1, uint64_t, nullptr, std::stoull, @1); }
| INT_LITERAL_WITH_SUFFIX {
    switch ($1.suffix) {
      case IntegerSuffix::u8: MAKE_LITERAL_NODE($$, $1.text, uint8_t, arrow::uint8(), std::stoull, @1); break;
      case IntegerSuffix::u16: MAKE_LITERAL_NODE($$, $1.text, uint16_t, arrow::uint16(), std::stoull, @1); break;
      case IntegerSuffix::u32: MAKE_LITERAL_NODE($$, $1.text, uint32_t, arrow::uint32(), std::stoull, @1); break;
      case IntegerSuffix::u64: MAKE_LITERAL_NODE($$, $1.text, uint64_t, arrow::uint64(), std::stoull, @1); break;
      case IntegerSuffix::i8: MAKE_LITERAL_NODE($$, $1.text, int8_t, arrow::int8(), std::stoull, @1); break;
      case IntegerSuffix::i16: MAKE_LITERAL_NODE($$, $1.text, int16_t, arrow::int16(), std::stoull, @1); break;
      case IntegerSuffix::i32: MAKE_LITERAL_NODE($$, $1.text, int32_t, arrow::int32(), std::stoull, @1); break;
      case IntegerSuffix::i64: MAKE_LITERAL_NODE($$, $1.text, int64_t, arrow::int64(), std::stoull, @1); break;
    }
  }
| FLOAT_LITERAL {MAKE_LITERAL_NODE($$, $1, double, nullptr, std::stod, @1)}
| FLOAT_LITERAL_WITH_SUFFIX {
    switch ($1.suffix) {
      case FloatSuffix::f32: MAKE_LITERAL_NODE($$, $1.text, float, arrow::float32(), std::stof, @1); break;
      case FloatSuffix::f64: MAKE_LITERAL_NODE($$, $1.text, double, arrow::float64(), std::stod, @1); break;
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

function:
  infix_function {$$ = std::move($1);}
| named_function {$$ = std::move($1);}
;

infix_function:
  "-" term %prec NEG {$$ = std::make_shared<gandiva::FunctionNode>("negative", gandiva::NodeVector{std::move($2)}, nullptr);}
| "not" term %prec NEG {$$ = std::make_shared<gandiva::FunctionNode>("not", gandiva::NodeVector{std::move($2)}, nullptr);}
| "~" term %prec NEG {$$ = std::make_shared<gandiva::FunctionNode>("bitwise_not", gandiva::NodeVector{std::move($2)}, nullptr);}
| term "+" term {$$ = std::make_shared<gandiva::FunctionNode>("add", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "-" term {$$ = std::make_shared<gandiva::FunctionNode>("substract", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "*" term {$$ = std::make_shared<gandiva::FunctionNode>("multiply", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "/" term {$$ = std::make_shared<gandiva::FunctionNode>("div", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "%" term {$$ = std::make_shared<gandiva::FunctionNode>("mod", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "&" term {$$ = std::make_shared<gandiva::FunctionNode>("bitwise_and", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "|" term {$$ = std::make_shared<gandiva::FunctionNode>("bitwise_or", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "^" term {$$ = std::make_shared<gandiva::FunctionNode>("bitwise_xor", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "==" term {$$ = std::make_shared<gandiva::FunctionNode>("equal", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "!=" term {$$ = std::make_shared<gandiva::FunctionNode>("not_equal", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term ">" term {$$ = std::make_shared<gandiva::FunctionNode>("greater_than", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term ">=" term {$$ = std::make_shared<gandiva::FunctionNode>("greater_than_or_equal_to", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "<" term {$$ = std::make_shared<gandiva::FunctionNode>("less_than", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
| term "<=" term {$$ = std::make_shared<gandiva::FunctionNode>("less_than_or_equal_to", gandiva::NodeVector{std::move($1), std::move($3)}, nullptr);}
;

named_function:
  IDENTIFIER "(" args ")" {$$ = std::make_shared<gandiva::FunctionNode>($1, $3, nullptr);}
;

args:
  arg {$$ = std::vector<gandiva::NodePtr>{$1};}
| args "," arg {
    $1.emplace_back(std::move($3));
    $$ = std::move($1);
  }
;

arg:
  term {$$ = std::move($1);}
%%

void gandiva::grammar::error (const location_type& l, const std::string& m) {
  std::stringstream sstream;
  sstream << l << ": " << m;
  parser.error_message_ = sstream.str();
}

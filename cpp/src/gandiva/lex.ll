/* Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 Modified from the official C++ example of GNU Bison */

%{ /* -*- C++ -*- */
# include <cerrno>
# include <climits>
# include <cstdlib>
# include <cstring> // strerror
# include <string>
# include "grammar.hh"
# include "gandiva/parser.h"
%}

%{
#if defined __clang__
# define CLANG_VERSION (__clang_major__ * 100 + __clang_minor__)
#endif

// Clang and ICC like to pretend they are GCC.
#if defined __GNUC__ && !defined __clang__ && !defined __ICC
# define GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)
#endif

// Pacify warnings in yy_init_buffer (observed with Flex 2.6.4)
// and GCC 6.4.0, 7.3.0 with -O3.
#if defined GCC_VERSION && 600 <= GCC_VERSION
# pragma GCC diagnostic ignored "-Wnull-dereference"
#endif

// This example uses Flex's C back end, yet compiles it as C++.
// So expect warnings about C style casts and NULL.
#if defined CLANG_VERSION && 500 <= CLANG_VERSION
# pragma clang diagnostic ignored "-Wold-style-cast"
# pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#elif defined GCC_VERSION && 407 <= GCC_VERSION
# pragma GCC diagnostic ignored "-Wold-style-cast"
# pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif

#define FLEX_VERSION (YY_FLEX_MAJOR_VERSION * 100 + YY_FLEX_MINOR_VERSION)

// Old versions of Flex (2.5.35) generate an incomplete documentation comment.
//
//  In file included from src/scan-code-c.c:3:
//  src/scan-code.c:2198:21: error: empty paragraph passed to '@param' command
//        [-Werror,-Wdocumentation]
//   * @param line_number
//     ~~~~~~~~~~~~~~~~~^
//  1 error generated.
#if FLEX_VERSION < 206 && defined CLANG_VERSION
# pragma clang diagnostic ignored "-Wdocumentation"
#endif

// Old versions of Flex (2.5.35) use 'register'.  Warnings introduced in
// GCC 7 and Clang 6.
#if FLEX_VERSION < 206
# if defined CLANG_VERSION && 600 <= CLANG_VERSION
#  pragma clang diagnostic ignored "-Wdeprecated-register"
# elif defined GCC_VERSION && 700 <= GCC_VERSION
#  pragma GCC diagnostic ignored "-Wregister"
# endif
#endif

#if FLEX_VERSION <= 206
# if defined CLANG_VERSION
#  pragma clang diagnostic ignored "-Wconversion"
#  pragma clang diagnostic ignored "-Wdocumentation"
#  pragma clang diagnostic ignored "-Wshorten-64-to-32"
#  pragma clang diagnostic ignored "-Wsign-conversion"
#  pragma clang diagnostic ignored "-Wsign-compare"
# elif defined GCC_VERSION
#  pragma GCC diagnostic ignored "-Wconversion"
#  pragma GCC diagnostic ignored "-Wsign-conversion"
# endif
#endif

// Flex 2.6.4, GCC 9
// warning: useless cast to type 'int' [-Wuseless-cast]
// 1361 |   YY_CURRENT_BUFFER_LVALUE->yy_buf_size = (int) (new_size - 2);
//      |                                                 ^
#if defined GCC_VERSION && 900 <= GCC_VERSION
# pragma GCC diagnostic ignored "-Wuseless-cast"
#endif
%}

%option noyywrap nounput noinput batch

id    [a-zA-Z][a-zA-Z_0-9]*
int   [0-9]+
float [0-9]+\.[0-9]+
blank [ \t\r]

%{
  // Code run each time a pattern is matched.
  # define YY_USER_ACTION  loc.columns (yyleng);
%}
%%
%{
  // A handy shortcut to the location held by the Parser.
  gandiva::location& loc = parser.location();
  // Code run each time yylex is called.
  loc.step ();
%}
{blank}+   loc.step ();
\n+        loc.lines (yyleng); loc.step ();

"if"          return gandiva::grammar::make_IF                          (loc);      
"else"        return gandiva::grammar::make_ELSE                        (loc);    
"in"          return gandiva::grammar::make_IN                          (loc);      
(not|!)       return gandiva::grammar::make_NOT                         (loc);     
(and|&&)      return gandiva::grammar::make_AND                         (loc);     
(or|\|\|)     return gandiva::grammar::make_OR                          (loc);      
"true"        return gandiva::grammar::make_TRUE                        (loc);      
"false"       return gandiva::grammar::make_FALSE                       (loc);      
"-"           return gandiva::grammar::make_MINUS                       (loc);   
"+"           return gandiva::grammar::make_PLUS                        (loc);    
"*"           return gandiva::grammar::make_STAR                        (loc);    
"/"           return gandiva::grammar::make_SLASH                       (loc);   
"("           return gandiva::grammar::make_LPAREN                      (loc);  
")"           return gandiva::grammar::make_RPAREN                      (loc);  
"%"           return gandiva::grammar::make_MODOLO                      (loc);  
"|"           return gandiva::grammar::make_BITWISE_OR                  (loc);
"^"           return gandiva::grammar::make_BITWISE_XOR                 (loc);
"&"           return gandiva::grammar::make_BITWISE_AND                 (loc);
"~"           return gandiva::grammar::make_BITWISE_NOT                 (loc);
"=="          return gandiva::grammar::make_EQUAL                       (loc);
"!="          return gandiva::grammar::make_NOT_EQUAL                   (loc);
"<"           return gandiva::grammar::make_LESS_THAN                   (loc);
"<="          return gandiva::grammar::make_LESS_THAN_OR_EQUAL_TO       (loc);
">"           return gandiva::grammar::make_GREATER_THAN                (loc);
">="          return gandiva::grammar::make_GREATER_THAN_OR_EQUAL_TO    (loc);
","           return gandiva::grammar::make_COMMA                       (loc);
"{"           return gandiva::grammar::make_LBRACKET                    (loc);
"}"           return gandiva::grammar::make_RBRACKET                    (loc);

{id}       return gandiva::grammar::make_IDENTIFIER (yytext, loc);

{int}      return gandiva::grammar::make_INT_LITERAL (yytext, loc);
{int}"u8"  return gandiva::grammar::make_INT_LITERAL_WITH_SUFFIX (gandiva::IntegerLiteralWithSuffix{yytext, gandiva::IntegerSuffix::u8}, loc);
{int}"u16" return gandiva::grammar::make_INT_LITERAL_WITH_SUFFIX (gandiva::IntegerLiteralWithSuffix{yytext, gandiva::IntegerSuffix::u16}, loc);
{int}"u32" return gandiva::grammar::make_INT_LITERAL_WITH_SUFFIX (gandiva::IntegerLiteralWithSuffix{yytext, gandiva::IntegerSuffix::u32}, loc);
{int}"u64" return gandiva::grammar::make_INT_LITERAL_WITH_SUFFIX (gandiva::IntegerLiteralWithSuffix{yytext, gandiva::IntegerSuffix::u64}, loc);
{int}"i8"  return gandiva::grammar::make_INT_LITERAL_WITH_SUFFIX (gandiva::IntegerLiteralWithSuffix{yytext, gandiva::IntegerSuffix::i8}, loc);
{int}"i16" return gandiva::grammar::make_INT_LITERAL_WITH_SUFFIX (gandiva::IntegerLiteralWithSuffix{yytext, gandiva::IntegerSuffix::i16}, loc);
{int}"i32" return gandiva::grammar::make_INT_LITERAL_WITH_SUFFIX (gandiva::IntegerLiteralWithSuffix{yytext, gandiva::IntegerSuffix::i32}, loc);
{int}"i64" return gandiva::grammar::make_INT_LITERAL_WITH_SUFFIX (gandiva::IntegerLiteralWithSuffix{yytext, gandiva::IntegerSuffix::i64}, loc);

{float}      return gandiva::grammar::make_FLOAT_LITERAL (yytext, loc);
{float}"f32"  return gandiva::grammar::make_FLOAT_LITERAL_WITH_SUFFIX (gandiva::FloatLiteralWithSuffix{yytext, gandiva::FloatSuffix::f32}, loc);
{float}"f64"  return gandiva::grammar::make_FLOAT_LITERAL_WITH_SUFFIX (gandiva::FloatLiteralWithSuffix{yytext, gandiva::FloatSuffix::f64}, loc);
{int}"f32"  return gandiva::grammar::make_FLOAT_LITERAL_WITH_SUFFIX (gandiva::FloatLiteralWithSuffix{yytext, gandiva::FloatSuffix::f32}, loc);
{int}"f64"  return gandiva::grammar::make_FLOAT_LITERAL_WITH_SUFFIX (gandiva::FloatLiteralWithSuffix{yytext, gandiva::FloatSuffix::f64}, loc);


\"([^\\\"]|\\.)*\"     return gandiva::grammar::make_STRING_LITERAL (yytext, loc); 
\'([^\\\']|\\.)*\'     return gandiva::grammar::make_STRING_LITERAL (yytext, loc);

<<EOF>>    return gandiva::grammar::make_YYEOF (loc);
%%

void gandiva::Parser::scan_begin(const std::string& exp_str) {
  yy_scan_string(exp_str.c_str());
}

void gandiva::Parser::scan_end() {
  yy_delete_buffer(YY_CURRENT_BUFFER);
}

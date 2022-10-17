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

#include <cctype>
#include <charconv>
#include <string_view>
#include <type_traits>
#include <unordered_map>

#include "arrow/compute/exec/expression.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {
static void ConsumeWhitespace(std::string_view& view) {
  constexpr const char* kWhitespaces = " \f\n\r\t\v";
  size_t first_nonwhitespace = view.find_first_not_of(kWhitespaces);
  view.remove_prefix(first_nonwhitespace);
}

static std::string_view ExtractUntil(std::string_view& view,
                                     const std::string_view separators) {
  size_t separator = view.find_first_of(separators);
  std::string_view result = view.substr(0, separator);
  view.remove_prefix(separator);
  return result;
}

static std::string_view TrimUntilNextSeparator(std::string_view& view) {
  constexpr const char* separators = " \f\n\r\t\v),";
  return ExtractUntil(view, separators);
}

static std::string_view ExtractArgument(std::string_view& view) {
  constexpr const char* separators = ",)";
  return ExtractUntil(view, separators);
}

static const std::unordered_map<std::string_view, std::shared_ptr<DataType>>
    kNameToSimpleType = {
        {"null", null()},
        {"boolean", boolean()},
        {"int8", int8()},
        {"int16", int16()},
        {"int32", int32()},
        {"int64", int64()},
        {"uint8", uint8()},
        {"uint16", uint16()},
        {"uint32", uint32()},
        {"uint64", uint64()},
        {"float16", float16()},
        {"float32", float32()},
        {"float64", float64()},
        {"utf8", utf8()},
        {"large_utf8", large_utf8()},
        {"binary", binary()},
        {"large_binary", large_binary()},
        {"date32", date32()},
        {"date64", date64()},
        {"day_time_interval", day_time_interval()},
        {"month_interval", month_interval()},
        {"month_day_nano_interval", month_day_nano_interval()},
};

static Result<std::shared_ptr<DataType>> ParseDataType(std::string_view& type);

// Takes the args list not including the enclosing parentheses
using InstantiateTypeFn =
    std::add_pointer_t<Result<std::shared_ptr<DataType>>(std::string_view&)>;

static Result<int32_t> ParseInt32(std::string_view& args) {
  ConsumeWhitespace(args);
  int32_t result;
  auto [finish, ec] = std::from_chars(args.data(), args.data() + args.size(), result);
  if (ec == std::errc::invalid_argument)
    return Status::Invalid("Could not parse ", args, " as an int32!");

  args.remove_prefix(finish - args.data());
  return result;
}

static Status ParseComma(std::string_view& args) {
  ConsumeWhitespace(args);
  if (args.empty() || args[0] != ',')
    return Status::Invalid("Expected comma-separated args list near ", args);
  args.remove_prefix(1);
  return Status::OK();
}

static Result<std::shared_ptr<DataType>> ParseFixedSizeBinary(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(int32_t byte_width, ParseInt32(args));
  return fixed_size_binary(byte_width);
}

static Result<std::pair<int32_t, int32_t>> ParseDecimalArgs(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(int32_t precision, ParseInt32(args));
  RETURN_NOT_OK(ParseComma(args));
  ARROW_ASSIGN_OR_RAISE(int32_t scale, ParseInt32(args));
  return std::pair<int32_t, int32_t>(precision, scale);
}

static Result<std::shared_ptr<DataType>> ParseDecimal(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(auto ps, ParseDecimalArgs(args));
  return decimal(ps.first, ps.second);
}

static Result<std::shared_ptr<DataType>> ParseDecimal128(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(auto ps, ParseDecimalArgs(args));
  return decimal128(ps.first, ps.second);
}

static Result<std::shared_ptr<DataType>> ParseDecimal256(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(auto ps, ParseDecimalArgs(args));
  return decimal256(ps.first, ps.second);
}

static Result<std::shared_ptr<DataType>> ParseList(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> list_type, ParseDataType(args));
  return list(std::move(list_type));
}

static Result<std::shared_ptr<DataType>> ParseLargeList(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> list_type, ParseDataType(args));
  return large_list(std::move(list_type));
}

static Result<std::shared_ptr<DataType>> ParseMap(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> key_type, ParseDataType(args));
  RETURN_NOT_OK(ParseComma(args));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> value_type, ParseDataType(args));
  return map(std::move(key_type), std::move(value_type));
}

static Result<std::shared_ptr<DataType>> ParseFixedSizeList(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> list_type, ParseDataType(args));
  RETURN_NOT_OK(ParseComma(args));
  ARROW_ASSIGN_OR_RAISE(int32_t size, ParseInt32(args));
  return fixed_size_list(std::move(list_type), size);
}

static Result<TimeUnit::type> ParseTimeUnit(std::string_view& args) {
  ConsumeWhitespace(args);
  if (args.empty()) return Status::Invalid("Expected a time unit near ", args);

  const std::string_view options[4] = {"SECOND", "MILLI", "MICRO", "NANO"};
  for (size_t i = 0; i < 4; i++) {
    if (args.find(options[i]) == 0) {
      args.remove_prefix(options[i].size());
      return TimeUnit::values()[i];
    }
  }
  return Status::Invalid("Unrecognized TimeUnit ", args);
}

static Result<std::shared_ptr<DataType>> ParseDuration(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(TimeUnit::type unit, ParseTimeUnit(args));
  return duration(unit);
}

static Result<std::shared_ptr<DataType>> ParseTimestamp(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(TimeUnit::type unit, ParseTimeUnit(args));
  return timestamp(unit);
}

static Result<std::shared_ptr<DataType>> ParseTime32(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(TimeUnit::type unit, ParseTimeUnit(args));
  return time32(unit);
}

static Result<std::shared_ptr<DataType>> ParseTime64(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(TimeUnit::type unit, ParseTimeUnit(args));
  return time64(unit);
}

static Result<std::shared_ptr<DataType>> ParseDictionary(std::string_view& args) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> key_type, ParseDataType(args));
  RETURN_NOT_OK(ParseComma(args));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> value_type, ParseDataType(args));
  return dictionary(std::move(key_type), std::move(value_type));
}

static const std::unordered_map<std::string_view, InstantiateTypeFn>
    kNameToParameterizedType = {
        {"fixed_size_binary", ParseFixedSizeBinary},
        {"decimal", ParseDecimal},
        {"decimal128", ParseDecimal128},
        {"decimal256", ParseDecimal256},
        {"list", ParseList},
        {"large_list", ParseLargeList},
        {"map", ParseMap},
        {"fixed_size_list", ParseFixedSizeList},
        {"duration", ParseDuration},
        {"timestamp", ParseTimestamp},
        {"time32", ParseTime32},
        {"time64", ParseTime64},
        {"dictionary", ParseDictionary},
};

static Result<Expression> ParseExpr(std::string_view& expr);

static Result<Expression> ParseCall(std::string_view& expr) {
  ConsumeWhitespace(expr);
  if (expr.empty()) return Status::Invalid("Found empty expression");

  std::string_view function_name = ExtractUntil(expr, "(");
  if (expr.empty())
    return Status::Invalid("Expected argument list after function name", function_name);
  expr.remove_prefix(1);  // Remove the open paren

  std::vector<Expression> args;
  do {
    ConsumeWhitespace(expr);
    if (expr.empty())
      return Status::Invalid("Found unterminated expression argument list");
    if (expr[0] == ')') break;
    if (!args.empty()) RETURN_NOT_OK(ParseComma(expr));

    ARROW_ASSIGN_OR_RAISE(Expression arg, ParseExpr(expr));
    args.emplace_back(std::move(arg));
  } while (true);

  expr.remove_prefix(1);  // Remove the close paren
  return call(std::string(function_name), std::move(args));
}

static Result<Expression> ParseFieldRef(std::string_view& expr) {
  if (expr.empty()) return Status::Invalid("Found an empty named fieldref");

  std::string_view dot_path = ExtractArgument(expr);
  ARROW_ASSIGN_OR_RAISE(FieldRef field, FieldRef::FromDotPath(dot_path));
  return field_ref(std::move(field));
}

static Result<std::shared_ptr<DataType>> ParseParameterizedDataType(
    std::string_view& type) {
  size_t lparen = type.find_first_of("(");
  if (lparen == std::string_view::npos) return Status::Invalid("Unknown type ", type);

  std::string_view base_type_name = type.substr(0, lparen);
  type.remove_prefix(lparen + 1);
  auto it = kNameToParameterizedType.find(base_type_name);
  if (it == kNameToParameterizedType.end())
    return Status::Invalid("Unknown base type name ", base_type_name);

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> parsed_type, it->second(type));
  ConsumeWhitespace(type);
  if (type.empty() || type[0] != ')')
    return Status::Invalid("Unterminated data type arg list!");
  type.remove_prefix(1);
  return parsed_type;
}

static Result<std::shared_ptr<DataType>> ParseDataType(std::string_view& type) {
  auto it = kNameToSimpleType.find(type);
  if (it == kNameToSimpleType.end()) return ParseParameterizedDataType(type);
  return it->second;
}

static Result<Expression> ParseLiteral(std::string_view& expr) {
  ARROW_DCHECK(expr[0] == '$');
  expr.remove_prefix(1);
  size_t colon = expr.find_first_of(":");
  std::string_view type_name = expr.substr(0, colon);
  expr.remove_prefix(colon);
  if (expr.empty()) return Status::Invalid("Found an unterminated literal!");

  ARROW_DCHECK_EQ(expr[0], ':');
  expr.remove_prefix(1);

  std::string_view value = TrimUntilNextSeparator(expr);

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<DataType> type, ParseDataType(type_name));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> scalar, Scalar::Parse(type, value));
  return literal(std::move(scalar));
}

static Result<Expression> ParseExpr(std::string_view& expr) {
  ConsumeWhitespace(expr);
  if (expr.empty()) return Status::Invalid("Expression is empty!");
  switch (expr[0]) {
    case '.':
    case '[':
      return ParseFieldRef(expr);
    case '$':
      return ParseLiteral(expr);
    default:
      return ParseCall(expr);
  }
}

Result<Expression> Expression::FromString(std::string_view expr) {
  return ParseExpr(expr);
}
}  // namespace compute
}  // namespace arrow

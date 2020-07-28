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

#include "arrow/compute/api_scalar.h"

#include <memory>
#include <sstream>
#include <string>

#include "arrow/compute/exec.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

#define SCALAR_EAGER_UNARY(NAME, REGISTRY_NAME)              \
  Result<Datum> NAME(const Datum& value, ExecContext* ctx) { \
    return CallFunction(REGISTRY_NAME, {value}, ctx);        \
  }

#define SCALAR_EAGER_BINARY(NAME, REGISTRY_NAME)                                \
  Result<Datum> NAME(const Datum& left, const Datum& right, ExecContext* ctx) { \
    return CallFunction(REGISTRY_NAME, {left, right}, ctx);                     \
  }

// ----------------------------------------------------------------------
// Arithmetic

#define SCALAR_ARITHMETIC_BINARY(NAME, REGISTRY_NAME, REGISTRY_CHECKED_NAME)           \
  Result<Datum> NAME(const Datum& left, const Datum& right, ArithmeticOptions options, \
                     ExecContext* ctx) {                                               \
    auto func_name = (options.check_overflow) ? REGISTRY_CHECKED_NAME : REGISTRY_NAME; \
    return CallFunction(func_name, {left, right}, ctx);                                \
  }

SCALAR_ARITHMETIC_BINARY(Add, "add", "add_checked")
SCALAR_ARITHMETIC_BINARY(Subtract, "subtract", "subtract_checked")
SCALAR_ARITHMETIC_BINARY(Multiply, "multiply", "multiply_checked")

// ----------------------------------------------------------------------
// Set-related operations

static Result<Datum> ExecSetLookup(const std::string& func_name, const Datum& data,
                                   const Datum& value_set, bool add_nulls_to_hash_table,
                                   ExecContext* ctx) {
  if (!value_set.is_arraylike()) {
    return Status::Invalid("Set lookup value set must be Array or ChunkedArray");
  }

  if (value_set.length() > 0 && !data.type()->Equals(value_set.type())) {
    std::stringstream ss;
    ss << "Array type didn't match type of values set: " << data.type()->ToString()
       << " vs " << value_set.type()->ToString();
    return Status::Invalid(ss.str());
  }
  SetLookupOptions options(value_set, !add_nulls_to_hash_table);
  return CallFunction(func_name, {data}, &options, ctx);
}

Result<Datum> IsIn(const Datum& values, const Datum& value_set, ExecContext* ctx) {
  return ExecSetLookup("is_in", values, value_set,
                       /*add_nulls_to_hash_table=*/false, ctx);
}

Result<Datum> IndexIn(const Datum& values, const Datum& value_set, ExecContext* ctx) {
  return ExecSetLookup("index_in", values, value_set,
                       /*add_nulls_to_hash_table=*/true, ctx);
}

// ----------------------------------------------------------------------
// Boolean functions

SCALAR_EAGER_UNARY(Invert, "invert")
SCALAR_EAGER_BINARY(And, "and")
SCALAR_EAGER_BINARY(KleeneAnd, "and_kleene")
SCALAR_EAGER_BINARY(Or, "or")
SCALAR_EAGER_BINARY(KleeneOr, "or_kleene")
SCALAR_EAGER_BINARY(Xor, "xor")

// ----------------------------------------------------------------------

Result<Datum> Compare(const Datum& left, const Datum& right, CompareOptions options,
                      ExecContext* ctx) {
  std::string func_name;
  switch (options.op) {
    case CompareOperator::EQUAL:
      func_name = "equal";
      break;
    case CompareOperator::NOT_EQUAL:
      func_name = "not_equal";
      break;
    case CompareOperator::GREATER:
      func_name = "greater";
      break;
    case CompareOperator::GREATER_EQUAL:
      func_name = "greater_equal";
      break;
    case CompareOperator::LESS:
      func_name = "less";
      break;
    case CompareOperator::LESS_EQUAL:
      func_name = "less_equal";
      break;
  }
  return CallFunction(func_name, {left, right}, &options, ctx);
}

// ----------------------------------------------------------------------
// Validity functions

SCALAR_EAGER_UNARY(IsValid, "is_valid")
SCALAR_EAGER_UNARY(IsNull, "is_null")

Result<Datum> FillNull(const Datum& values, const Datum& fill_value, ExecContext* ctx) {
  return CallFunction("fill_null", {values, fill_value}, ctx);
}

}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/expression.h"

#include <memory>
#include <sstream>
#include <utility>

#include "arrow/compute/logical_type.h"
#include "arrow/compute/operation.h"
#include "arrow/status.h"

namespace arrow {
namespace compute {

Expr::Expr(std::shared_ptr<Operation> op) : op_(std::move(op)) {}

ValueExpr::ValueExpr(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type,
                     Rank rank)
    : Expr(std::move(op)), type_(std::move(type)), rank_(rank) {}

std::shared_ptr<LogicalType> ValueExpr::type() const { return type_; }

ScalarExpr::ScalarExpr(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type)
    : ValueExpr(std::move(op), std::move(type), ValueExpr::SCALAR) {}

ArrayExpr::ArrayExpr(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type)
    : ValueExpr(std::move(op), std::move(type), ValueExpr::ARRAY) {}

std::string ArrayExpr::kind() const {
  std::stringstream ss;
  ss << "array[" << type_->ToString() << "]";
  return ss.str();
}

std::string ScalarExpr::kind() const {
  std::stringstream ss;
  ss << "scalar[" << type_->ToString() << "]";
  return ss.str();
}

// ----------------------------------------------------------------------

#define SIMPLE_EXPR_FACTORY(NAME, TYPE)                       \
  std::shared_ptr<Expr> NAME(std::shared_ptr<Operation> op) { \
    return std::make_shared<TYPE>(std::move(op));             \
  }

namespace scalar {

#define SCALAR_EXPR_METHODS(NAME)           \
  NAME::NAME(std::shared_ptr<Operation> op) \
      : ScalarExpr(std::move(op), std::make_shared<type::NAME>()) {}

SCALAR_EXPR_METHODS(Null)
SCALAR_EXPR_METHODS(Bool)
SCALAR_EXPR_METHODS(Int8)
SCALAR_EXPR_METHODS(Int16)
SCALAR_EXPR_METHODS(Int32)
SCALAR_EXPR_METHODS(Int64)
SCALAR_EXPR_METHODS(UInt8)
SCALAR_EXPR_METHODS(UInt16)
SCALAR_EXPR_METHODS(UInt32)
SCALAR_EXPR_METHODS(UInt64)
SCALAR_EXPR_METHODS(HalfFloat)
SCALAR_EXPR_METHODS(Float)
SCALAR_EXPR_METHODS(Double)
SCALAR_EXPR_METHODS(Binary)
SCALAR_EXPR_METHODS(Utf8)

SIMPLE_EXPR_FACTORY(null, Null);
SIMPLE_EXPR_FACTORY(boolean, Bool);
SIMPLE_EXPR_FACTORY(int8, Int8);
SIMPLE_EXPR_FACTORY(int16, Int16);
SIMPLE_EXPR_FACTORY(int32, Int32);
SIMPLE_EXPR_FACTORY(int64, Int64);
SIMPLE_EXPR_FACTORY(uint8, UInt8);
SIMPLE_EXPR_FACTORY(uint16, UInt16);
SIMPLE_EXPR_FACTORY(uint32, UInt32);
SIMPLE_EXPR_FACTORY(uint64, UInt64);
SIMPLE_EXPR_FACTORY(half_float, HalfFloat);
SIMPLE_EXPR_FACTORY(float_, Float);
SIMPLE_EXPR_FACTORY(double_, Double);
SIMPLE_EXPR_FACTORY(binary, Binary);
SIMPLE_EXPR_FACTORY(utf8, Utf8);

List::List(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type)
    : ScalarExpr(std::move(op), std::move(type)) {}

Struct::Struct(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type)
    : ScalarExpr(std::move(op), std::move(type)) {}

}  // namespace scalar

namespace array {

#define ARRAY_EXPR_METHODS(NAME)            \
  NAME::NAME(std::shared_ptr<Operation> op) \
      : ArrayExpr(std::move(op), std::make_shared<type::NAME>()) {}

ARRAY_EXPR_METHODS(Null)
ARRAY_EXPR_METHODS(Bool)
ARRAY_EXPR_METHODS(Int8)
ARRAY_EXPR_METHODS(Int16)
ARRAY_EXPR_METHODS(Int32)
ARRAY_EXPR_METHODS(Int64)
ARRAY_EXPR_METHODS(UInt8)
ARRAY_EXPR_METHODS(UInt16)
ARRAY_EXPR_METHODS(UInt32)
ARRAY_EXPR_METHODS(UInt64)
ARRAY_EXPR_METHODS(HalfFloat)
ARRAY_EXPR_METHODS(Float)
ARRAY_EXPR_METHODS(Double)
ARRAY_EXPR_METHODS(Binary)
ARRAY_EXPR_METHODS(Utf8)

SIMPLE_EXPR_FACTORY(null, Null);
SIMPLE_EXPR_FACTORY(boolean, Bool);
SIMPLE_EXPR_FACTORY(int8, Int8);
SIMPLE_EXPR_FACTORY(int16, Int16);
SIMPLE_EXPR_FACTORY(int32, Int32);
SIMPLE_EXPR_FACTORY(int64, Int64);
SIMPLE_EXPR_FACTORY(uint8, UInt8);
SIMPLE_EXPR_FACTORY(uint16, UInt16);
SIMPLE_EXPR_FACTORY(uint32, UInt32);
SIMPLE_EXPR_FACTORY(uint64, UInt64);
SIMPLE_EXPR_FACTORY(half_float, HalfFloat);
SIMPLE_EXPR_FACTORY(float_, Float);
SIMPLE_EXPR_FACTORY(double_, Double);
SIMPLE_EXPR_FACTORY(binary, Binary);
SIMPLE_EXPR_FACTORY(utf8, Utf8);

List::List(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type)
    : ArrayExpr(std::move(op), std::move(type)) {}

Struct::Struct(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type)
    : ArrayExpr(std::move(op), std::move(type)) {}

}  // namespace array
}  // namespace compute
}  // namespace arrow

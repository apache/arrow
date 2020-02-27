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

#include "arrow/engine/expression.h"
#include "arrow/scalar.h"
#include "arrow/type.h"

namespace arrow {
namespace engine {

//
// ExprType
//

ExprType ExprType::Scalar(std::shared_ptr<DataType> type) {
  return ExprType(std::move(type), Shape::SCALAR);
}

ExprType ExprType::Array(std::shared_ptr<DataType> type) {
  return ExprType(std::move(type), Shape::ARRAY);
}

ExprType ExprType::Table(std::shared_ptr<Schema> schema) {
  return ExprType(std::move(schema), Shape::TABLE);
}

ExprType::ExprType(std::shared_ptr<Schema> schema, Shape shape)
    : type_(std::move(schema)), shape_(shape) {
  DCHECK_EQ(shape, Shape::TABLE);
}

ExprType::ExprType(std::shared_ptr<DataType> type, Shape shape)
    : type_(std::move(type)), shape_(shape) {
  DCHECK_NE(shape, Shape::TABLE);
}

std::shared_ptr<Schema> ExprType::schema() const {
  if (shape_ == TABLE) {
    return util::get<std::shared_ptr<Schema>>(type_);
  }

  return nullptr;
}

std::shared_ptr<DataType> ExprType::data_type() const {
  if (shape_ != TABLE) {
    return util::get<std::shared_ptr<DataType>>(type_);
  }

  return nullptr;
}

bool ExprType::Equals(const ExprType& type) const {
  if (this == &type) {
    return true;
  }

  if (shape() != type.shape()) {
    return false;
  }

  switch (shape()) {
    case SCALAR:
      return data_type()->Equals(type.data_type());
    case ARRAY:
      return data_type()->Equals(type.data_type());
    case TABLE:
      return schema()->Equals(type.schema());
    default:
      break;
  }

  return false;
}

bool ExprType::operator==(const ExprType& rhs) const { return Equals(rhs); }

#define PRECONDITION(cond, ...)            \
  do {                                     \
    if (ARROW_PREDICT_FALSE(!(cond))) {    \
      return Status::Invalid(__VA_ARGS__); \
    }                                      \
  } while (false)

//
// ScalarExpr
//

ScalarExpr::ScalarExpr(std::shared_ptr<Scalar> scalar)
    : Expr(SCALAR_LITERAL), scalar_(std::move(scalar)) {}

Result<std::shared_ptr<ScalarExpr>> ScalarExpr::Make(std::shared_ptr<Scalar> scalar) {
  PRECONDITION(scalar != nullptr, "ScalarExpr's scalar must be non-null");

  return std::shared_ptr<ScalarExpr>(new ScalarExpr(std::move(scalar)));
}

ExprType ScalarExpr::type() const { return ExprType::Scalar(scalar_->type); }

//
// FieldRefExpr
//

FieldRefExpr::FieldRefExpr(std::shared_ptr<Field> field)
    : Expr(FIELD_REFERENCE), field_(std::move(field)) {}

Result<std::shared_ptr<FieldRefExpr>> FieldRefExpr::Make(std::shared_ptr<Field> field) {
  PRECONDITION(field != nullptr, "FieldRefExpr's field must be non-null");

  return std::shared_ptr<FieldRefExpr>(new FieldRefExpr(std::move(field)));
}

ExprType FieldRefExpr::type() const { return ExprType::Scalar(field_->type()); }

//
// Comparisons
//

Status ValidateCompareOpInputs(std::shared_ptr<Expr> left, std::shared_ptr<Expr> right) {
  PRECONDITION(left != nullptr, "EqualCmpExpr's left operand must be non-null");
  PRECONDITION(right != nullptr, "EqualCmpExpr's right operand must be non-null");
  // TODO(fsaintjacques): Add support for broadcast.
  return Status::OK();
}

#define COMPARE_MAKE_IMPL(ExprClass)                                                     \
  Result<std::shared_ptr<ExprClass>> ExprClass::Make(std::shared_ptr<Expr> left,         \
                                                     std::shared_ptr<Expr> right) {      \
    RETURN_NOT_OK(ValidateCompareOpInputs(left, right));                                 \
    return std::shared_ptr<ExprClass>(new ExprClass(std::move(left), std::move(right))); \
  }

COMPARE_MAKE_IMPL(EqualCmpExpr)
COMPARE_MAKE_IMPL(NotEqualCmpExpr)
COMPARE_MAKE_IMPL(GreaterThanCmpExpr)
COMPARE_MAKE_IMPL(GreaterEqualThanCmpExpr)
COMPARE_MAKE_IMPL(LowerThanCmpExpr)
COMPARE_MAKE_IMPL(LowerEqualThanCmpExpr)

#undef COMPARE_MAKE_IMPL

//
// ScanRelExpr
//

ScanRelExpr::ScanRelExpr(Catalog::Entry input)
    : Expr(SCAN_REL), input_(std::move(input)) {}

Result<std::shared_ptr<ScanRelExpr>> ScanRelExpr::Make(Catalog::Entry input) {
  return std::shared_ptr<ScanRelExpr>(new ScanRelExpr(std::move(input)));
}

ExprType ScanRelExpr::type() const { return ExprType::Table(input_.schema()); }

//
// FilterRelExpr
//

Result<std::shared_ptr<FilterRelExpr>> FilterRelExpr::Make(
    std::shared_ptr<Expr> input, std::shared_ptr<Expr> predicate) {
  PRECONDITION(input != nullptr, "FilterRelExpr's input must be non-null.");
  PRECONDITION(input->type().IsTable(), "FilterRelExpr's input must be a table.");
  PRECONDITION(predicate != nullptr, "FilterRelExpr's predicate must be non-null.");
  PRECONDITION(predicate->type().IsPredicate(),
               "FilterRelExpr's predicate must be a predicate");

  // TODO(fsaintjacques): check fields referenced in predicate are found in
  // input.

  return std::shared_ptr<FilterRelExpr>(
      new FilterRelExpr(std::move(input), std::move(predicate)));
}

FilterRelExpr::FilterRelExpr(std::shared_ptr<Expr> input, std::shared_ptr<Expr> predicate)
    : Expr(FILTER_REL), input_(std::move(input)), predicate_(std::move(predicate)) {}

ExprType FilterRelExpr::type() const { return ExprType::Table(input_->type().schema()); }

#undef PRECONDITION

}  // namespace engine
}  // namespace arrow

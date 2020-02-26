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

//
// Expr
//

//
// ScalarExpr
//

ScalarExpr::ScalarExpr(std::shared_ptr<Scalar> scalar)
    : Expr(SCALAR), scalar_(std::move(scalar)) {}

Result<std::shared_ptr<ScalarExpr>> ScalarExpr::Make(std::shared_ptr<Scalar> scalar) {
  if (scalar == nullptr) {
    return Status::Invalid("ScalarExpr's scalar must be non-null");
  }

  return std::shared_ptr<ScalarExpr>(new ScalarExpr(std::move(scalar)));
}

ExprType ScalarExpr::type() const { return ExprType::Scalar(scalar_->type); }

//
// FieldRefExpr
//

FieldRefExpr::FieldRefExpr(std::shared_ptr<Field> field)
    : Expr(FIELD_REF), field_(std::move(field)) {}

Result<std::shared_ptr<FieldRefExpr>> FieldRefExpr::Make(std::shared_ptr<Field> field) {
  if (field == nullptr) {
    return Status::Invalid("FieldRefExpr's field must be non-null");
  }

  return std::shared_ptr<FieldRefExpr>(new FieldRefExpr(std::move(field)));
}

ExprType FieldRefExpr::type() const { return ExprType::Scalar(field_->type()); }

//
// ScanRelExpr
//

ScanRelExpr::ScanRelExpr(Catalog::Entry input)
    : Expr(SCAN_REL), input_(std::move(input)) {}

Result<std::shared_ptr<Expr>> ScanRelExpr::Make(Catalog::Entry input) {
  return std::shared_ptr<ScanRelExpr>(new ScanRelExpr(std::move(input)));
}

ExprType ScanRelExpr::type() const { return ExprType::Table(input_.schema()); }

//
// FilterRelExpr
//

Result<std::shared_ptr<Expr>> FilterRelExpr::Make(std::shared_ptr<Expr> input,
                                                  std::shared_ptr<Expr> predicate) {
  if (input == nullptr) {
    return Status::Invalid("FilterRelExpr's input must be non-null.");
  }

  if (predicate == nullptr) {
    return Status::Invalid("FilterRelExpr's predicate must be non-null.");
  }

  if (!predicate->type().Equals(ExprType::Scalar(boolean()))) {
    return Status::Invalid("Filter's predicate expression must be a boolean scalar");
  }

  return std::shared_ptr<FilterRelExpr>(
      new FilterRelExpr(std::move(input), std::move(predicate)));
}

FilterRelExpr::FilterRelExpr(std::shared_ptr<Expr> input, std::shared_ptr<Expr> predicate)
    : Expr(FILTER_REL), input_(std::move(input)), predicate_(std::move(predicate)) {
  DCHECK_NE(input_, nullptr);
  DCHECK_NE(predicate_, nullptr);
}

ExprType FilterRelExpr::type() const { return ExprType::Table(input_->type().schema()); }

}  // namespace engine
}  // namespace arrow

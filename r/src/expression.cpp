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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)

// [[arrow::export]]
ds::ExpressionPtr dataset___expr__field_ref(std::string name) {
  return std::make_shared<ds::FieldExpression>(std::move(name));
}

// [[arrow::export]]
std::shared_ptr<ds::ComparisonExpression> dataset___expr__equal(
    const ds::ExpressionPtr& lhs, const ds::ExpressionPtr& rhs) {
  return ds::equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::ComparisonExpression> dataset___expr__not_equal(
    const ds::ExpressionPtr& lhs, const ds::ExpressionPtr& rhs) {
  return ds::not_equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::ComparisonExpression> dataset___expr__greater(
    const ds::ExpressionPtr& lhs, const ds::ExpressionPtr& rhs) {
  return ds::greater(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::ComparisonExpression> dataset___expr__greater_equal(
    const ds::ExpressionPtr& lhs, const ds::ExpressionPtr& rhs) {
  return ds::greater_equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::ComparisonExpression> dataset___expr__less(
    const ds::ExpressionPtr& lhs, const ds::ExpressionPtr& rhs) {
  return ds::less(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::ComparisonExpression> dataset___expr__less_equal(
    const ds::ExpressionPtr& lhs, const ds::ExpressionPtr& rhs) {
  return ds::less_equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::InExpression> dataset___expr__in(
    const ds::ExpressionPtr& lhs, const std::shared_ptr<arrow::Array>& rhs) {
  return std::make_shared<ds::InExpression>(lhs->In(rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::AndExpression> dataset___expr__and(const ds::ExpressionPtr& lhs,
                                                       const ds::ExpressionPtr& rhs) {
  return ds::and_(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::OrExpression> dataset___expr__or(const ds::ExpressionPtr& lhs,
                                                     const ds::ExpressionPtr& rhs) {
  return ds::or_(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::NotExpression> dataset___expr__not(const ds::ExpressionPtr& lhs) {
  return ds::not_(lhs);
}

// [[arrow::export]]
std::shared_ptr<ds::IsValidExpression> dataset___expr__is_valid(
    const ds::ExpressionPtr& lhs) {
  return std::make_shared<ds::IsValidExpression>(lhs->IsValid());
}

// [[arrow::export]]
std::shared_ptr<ds::ScalarExpression> dataset___expr__scalar(SEXP x) {
  switch (TYPEOF(x)) {
    case LGLSXP:
      return ds::scalar(Rf_asLogical(x));
    case REALSXP:
      return ds::scalar(Rf_asReal(x));
    case INTSXP:
      return ds::scalar(Rf_asInteger(x));
    default:
      // TODO more types (character, factor, Date, POSIXt, etc.)
      Rcpp::stop(
          tfm::format("R object of type %s not supported", Rf_type2char(TYPEOF(x))));
  }
  return nullptr;
}

// [[arrow::export]]
std::string dataset___expr__ToString(const ds::ExpressionPtr& x) { return x->ToString(); }

#endif

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

#include <arrow/compute/api_scalar.h>
#include <arrow/dataset/api.h>
namespace ds = ::arrow::dataset;

std::shared_ptr<ds::Expression> Share(ds::Expression expr) {
  return std::make_shared<ds::Expression>(std::move(expr));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__field_ref(std::string name) {
  return Share(ds::field_ref(std::move(name)));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__equal(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return Share(ds::equal(*lhs, *rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__not_equal(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return Share(ds::not_equal(*lhs, *rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__greater(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return Share(ds::greater(*lhs, *rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__greater_equal(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return Share(ds::greater_equal(*lhs, *rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__less(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return Share(ds::less(*lhs, *rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__less_equal(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return Share(ds::less_equal(*lhs, *rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__in(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<arrow::Array>& rhs) {
  return Share(ds::call("is_in", {*lhs}, arrow::compute::SetLookupOptions{rhs}));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__and(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return Share(ds::and_(*lhs, *rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__or(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return Share(ds::or_(*lhs, *rhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__not(
    const std::shared_ptr<ds::Expression>& lhs) {
  return Share(ds::not_(*lhs));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__is_valid(
    const std::shared_ptr<ds::Expression>& lhs) {
  return Share(ds::call("is_valid", {*lhs}));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__scalar(
    const std::shared_ptr<arrow::Scalar>& x) {
  return Share(ds::literal(x));
}

// [[arrow::export]]
std::string dataset___expr__ToString(const std::shared_ptr<ds::Expression>& x) {
  return x->ToString();
}

#endif

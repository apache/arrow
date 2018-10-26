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

#include <arrow/util/decimal.h>
#include "arrow_types.h"

using namespace Rcpp;

template <typename Vector, typename value_type>
ComplexVector IntVector_to_Decimal128(Vector x) {
  auto n = x.size();
  ComplexVector res(no_init(n));
  auto p = reinterpret_cast<arrow::Decimal128*>(res.begin());
  auto p_x = reinterpret_cast<value_type*>(x.begin());

  for (R_xlen_t i = 0; i < n; i++, ++p, ++p_x) {
    *p = arrow::Decimal128(*p_x);
  }

  return res;
}

// [[Rcpp::export]]
ComplexVector IntegerVector_to_Decimal128(IntegerVector_ x) {
  return IntVector_to_Decimal128<IntegerVector_, int32_t>(x);
}

// [[Rcpp::export]]
ComplexVector Integer64Vector_to_Decimal128(NumericVector_ x) {
  return IntVector_to_Decimal128<NumericVector_, int64_t>(x);
}

template <typename OutputVector, typename value_type>
OutputVector Decimal128_To_Int(ComplexVector_ x, value_type NA) {
  auto n = x.size();
  OutputVector res(no_init(n));

  auto p_res = reinterpret_cast<value_type*>(res.begin());
  auto p_x = reinterpret_cast<arrow::Decimal128*>(x.begin());

  for (R_xlen_t i = 0; i < n; i++, ++p_x, ++p_res) {
    auto status = p_x->ToInteger<value_type>(p_res);
    if (!status.ok()) {
      *p_res = NA;
    }
  }

  return res;
}

// [[Rcpp::export]]
NumericVector Decimal128_To_Integer64(ComplexVector_ x) {
  NumericVector res = Decimal128_To_Int<NumericVector, int64_t>(x, arrow::r::NA_INT64);
  res.attr("class") = "integer64";
  return res;
}

// [[Rcpp::export]]
IntegerVector Decimal128_To_Integer(ComplexVector_ x) {
  return Decimal128_To_Int<IntegerVector, int32_t>(x, NA_INTEGER);
}

// [[Rcpp::export]]
CharacterVector format_decimal128(arrow::r::Decimal128Record record) {
  auto data = record.data();
  auto n = data.size();
  auto p = reinterpret_cast<arrow::Decimal128*>(data.begin());
  CharacterVector res(no_init(n));
  for (R_xlen_t i = 0; i < n; i++, ++p) {
    res[i] = p->ToString(record.scale());
  }
  return res;
}

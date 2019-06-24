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
bool Buffer__is_mutable(const std::shared_ptr<arrow::Buffer>& buffer) {
  return buffer->is_mutable();
}

// [[arrow::export]]
void Buffer__ZeroPadding(const std::shared_ptr<arrow::Buffer>& buffer) {
  buffer->ZeroPadding();
}

// [[arrow::export]]
int64_t Buffer__capacity(const std::shared_ptr<arrow::Buffer>& buffer) {
  return buffer->capacity();
}

// [[arrow::export]]
int64_t Buffer__size(const std::shared_ptr<arrow::Buffer>& buffer) {
  return buffer->size();
}

// [[arrow::export]]
std::shared_ptr<arrow::Buffer> r___RBuffer__initialize(SEXP x) {
  switch (TYPEOF(x)) {
    case RAWSXP:
      return std::make_shared<arrow::r::RBuffer<RAWSXP>>(x);
    case REALSXP:
      return std::make_shared<arrow::r::RBuffer<REALSXP>>(x);
    case INTSXP:
      return std::make_shared<arrow::r::RBuffer<INTSXP>>(x);
    case CPLXSXP:
      return std::make_shared<arrow::r::RBuffer<CPLXSXP>>(x);
    default:
      Rcpp::stop(
          tfm::format("R object of type %s not supported", Rf_type2char(TYPEOF(x))));
  }
  return nullptr;
}

// [[arrow::export]]
Rcpp::RawVector Buffer__data(const std::shared_ptr<arrow::Buffer>& buffer) {
  return Rcpp::RawVector(buffer->data(), buffer->data() + buffer->size());
}

#endif

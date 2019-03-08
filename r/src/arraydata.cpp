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

using Rcpp::List;
using Rcpp::wrap;

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> ArrayData__get_type(
    const std::shared_ptr<arrow::ArrayData>& x) {
  return x->type;
}

// [[Rcpp::export]]
int ArrayData__get_length(const std::shared_ptr<arrow::ArrayData>& x) {
  return x->length;
}

// [[Rcpp::export]]
int ArrayData__get_null_count(const std::shared_ptr<arrow::ArrayData>& x) {
  return x->null_count;
}

// [[Rcpp::export]]
int ArrayData__get_offset(const std::shared_ptr<arrow::ArrayData>& x) {
  return x->offset;
}

// [[Rcpp::export]]
List ArrayData__buffers(const std::shared_ptr<arrow::ArrayData>& x) {
  return wrap(x->buffers);
}

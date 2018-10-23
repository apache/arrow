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

#include "arrow_types.h"

// [[Rcpp::export]]
int Column__length(const std::shared_ptr<arrow::Column>& column) {
  return column->length();
}

// [[Rcpp::export]]
int Column__null_count(const std::shared_ptr<arrow::Column>& column) {
  return column->null_count();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Column__type(
    const std::shared_ptr<arrow::Column>& column) {
  return column->type();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ChunkedArray> Column__data(
    const std::shared_ptr<arrow::Column>& column) {
  return column->data();
}

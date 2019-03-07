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
int ChunkedArray__length(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->length();
}

// [[Rcpp::export]]
int ChunkedArray__null_count(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->null_count();
}

// [[Rcpp::export]]
int ChunkedArray__num_chunks(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->num_chunks();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> ChunkedArray__chunk(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int i) {
  return chunked_array->chunk(i);
}

// [[Rcpp::export]]
List ChunkedArray__chunks(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return wrap(chunked_array->chunks());
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> ChunkedArray__type(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->type();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkArray__Slice1(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int offset) {
  return chunked_array->Slice(offset);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkArray__Slice2(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int offset, int length) {
  return chunked_array->Slice(offset, length);
}

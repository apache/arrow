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

#if defined(ARROW_R_WITH_ARROW)

#include <arrow/chunked_array.h>

// [[arrow::export]]
int ChunkedArray__length(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->length();
}

// [[arrow::export]]
int ChunkedArray__null_count(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->null_count();
}

// [[arrow::export]]
int ChunkedArray__num_chunks(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->num_chunks();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> ChunkedArray__chunk(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int i) {
  arrow::r::validate_index(i, chunked_array->num_chunks());
  return chunked_array->chunk(i);
}

// [[arrow::export]]
List ChunkedArray__chunks(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return wrap(chunked_array->chunks());
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> ChunkedArray__type(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->type();
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Slice1(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int offset) {
  arrow::r::validate_slice_offset(offset, chunked_array->length());
  return chunked_array->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Slice2(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int offset, int length) {
  arrow::r::validate_slice_offset(offset, chunked_array->length());
  arrow::r::validate_slice_length(length, chunked_array->length() - offset);
  return chunked_array->Slice(offset, length);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__View(
    const std::shared_ptr<arrow::ChunkedArray>& array,
    const std::shared_ptr<arrow::DataType>& type) {
  return ValueOrStop(array->View(type));
}

// [[arrow::export]]
void ChunkedArray__Validate(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  StopIfNotOk(chunked_array->Validate());
}

// [[arrow::export]]
bool ChunkedArray__Equals(const std::shared_ptr<arrow::ChunkedArray>& x,
                          const std::shared_ptr<arrow::ChunkedArray>& y) {
  return x->Equals(y);
}

// [[arrow::export]]
std::string ChunkedArray__ToString(const std::shared_ptr<arrow::ChunkedArray>& x) {
  return x->ToString();
}

#endif

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

using Rcpp::LogicalVector;
using Rcpp::no_init;

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__Slice1(const std::shared_ptr<arrow::Array>& array,
                                            int offset) {
  return array->Slice(offset);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__Slice2(const std::shared_ptr<arrow::Array>& array,
                                            int offset, int length) {
  return array->Slice(offset, length);
}

// [[Rcpp::export]]
bool Array__IsNull(const std::shared_ptr<arrow::Array>& x, int i) { return x->IsNull(i); }

// [[Rcpp::export]]
bool Array__IsValid(const std::shared_ptr<arrow::Array>& x, int i) {
  return x->IsValid(i);
}

// [[Rcpp::export]]
int Array__length(const std::shared_ptr<arrow::Array>& x) { return x->length(); }

// [[Rcpp::export]]
int Array__offset(const std::shared_ptr<arrow::Array>& x) { return x->offset(); }

// [[Rcpp::export]]
int Array__null_count(const std::shared_ptr<arrow::Array>& x) { return x->null_count(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Array__type(const std::shared_ptr<arrow::Array>& x) {
  return x->type();
}

// [[Rcpp::export]]
std::string Array__ToString(const std::shared_ptr<arrow::Array>& x) {
  return x->ToString();
}

// [[Rcpp::export]]
arrow::Type::type Array__type_id(const std::shared_ptr<arrow::Array>& x) {
  return x->type_id();
}

// [[Rcpp::export]]
bool Array__Equals(const std::shared_ptr<arrow::Array>& lhs,
                   const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->Equals(rhs);
}

// [[Rcpp::export]]
bool Array__ApproxEquals(const std::shared_ptr<arrow::Array>& lhs,
                         const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->ApproxEquals(rhs);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ArrayData> Array__data(
    const std::shared_ptr<arrow::Array>& array) {
  return array->data();
}

// [[Rcpp::export]]
bool Array__RangeEquals(const std::shared_ptr<arrow::Array>& self,
                        const std::shared_ptr<arrow::Array>& other, int start_idx,
                        int end_idx, int other_start_idx) {
  return self->RangeEquals(*other, start_idx, end_idx, other_start_idx);
}

// [[Rcpp::export]]
LogicalVector Array__Mask(const std::shared_ptr<arrow::Array>& array) {
  if (array->null_count() == 0) {
    return LogicalVector(array->length(), true);
  }

  auto n = array->length();
  LogicalVector res(no_init(n));
  arrow::internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                              array->offset(), n);
  for (int64_t i = 0; i < n; i++, bitmap_reader.Next()) {
    res[i] = bitmap_reader.IsSet();
  }
  return res;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> DictionaryArray__indices(
    const std::shared_ptr<arrow::DictionaryArray>& array) {
  return array->indices();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> DictionaryArray__dictionary(
    const std::shared_ptr<arrow::DictionaryArray>& array) {
  return array->dictionary();
}

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

#if defined(ARROW_R_WITH_ARROW)

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Slice1(const std::shared_ptr<arrow::Array>& array,
                                            int offset) {
  return array->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Slice2(const std::shared_ptr<arrow::Array>& array,
                                            int offset, int length) {
  return array->Slice(offset, length);
}

// [[arrow::export]]
bool Array__IsNull(const std::shared_ptr<arrow::Array>& x, int i) { return x->IsNull(i); }

// [[arrow::export]]
bool Array__IsValid(const std::shared_ptr<arrow::Array>& x, int i) {
  return x->IsValid(i);
}

// [[arrow::export]]
int Array__length(const std::shared_ptr<arrow::Array>& x) { return x->length(); }

// [[arrow::export]]
int Array__offset(const std::shared_ptr<arrow::Array>& x) { return x->offset(); }

// [[arrow::export]]
int Array__null_count(const std::shared_ptr<arrow::Array>& x) { return x->null_count(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Array__type(const std::shared_ptr<arrow::Array>& x) {
  return x->type();
}

// [[arrow::export]]
std::string Array__ToString(const std::shared_ptr<arrow::Array>& x) {
  return x->ToString();
}

// [[arrow::export]]
arrow::Type::type Array__type_id(const std::shared_ptr<arrow::Array>& x) {
  return x->type_id();
}

// [[arrow::export]]
bool Array__Equals(const std::shared_ptr<arrow::Array>& lhs,
                   const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->Equals(rhs);
}

// [[arrow::export]]
bool Array__ApproxEquals(const std::shared_ptr<arrow::Array>& lhs,
                         const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->ApproxEquals(rhs);
}

// [[arrow::export]]
std::shared_ptr<arrow::ArrayData> Array__data(
    const std::shared_ptr<arrow::Array>& array) {
  return array->data();
}

// [[arrow::export]]
bool Array__RangeEquals(const std::shared_ptr<arrow::Array>& self,
                        const std::shared_ptr<arrow::Array>& other, int start_idx,
                        int end_idx, int other_start_idx) {
  return self->RangeEquals(*other, start_idx, end_idx, other_start_idx);
}

// [[arrow::export]]
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

// [[arrow::export]]
std::shared_ptr<arrow::Array> DictionaryArray__indices(
    const std::shared_ptr<arrow::DictionaryArray>& array) {
  return array->indices();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> DictionaryArray__dictionary(
    const std::shared_ptr<arrow::DictionaryArray>& array) {
  return array->dictionary();
}

#endif

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

using namespace Rcpp;
// [[Rcpp::plugins(cpp11)]]

// [[Rcpp::export]]
std::shared_ptr<arrow::ArrayData> ArrayData_initialize(const std::shared_ptr<arrow::DataType>& type, int length, int null_count, int offset){
  return arrow::ArrayData::Make( type, length, {nullptr, nullptr}, null_count, offset);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> ArrayData_get_type(const std::shared_ptr<arrow::ArrayData>& x){
  return x->type;
}

// [[Rcpp::export]]
int ArrayData_get_length(const std::shared_ptr<arrow::ArrayData>& x){
  return x->length;
}

// [[Rcpp::export]]
int ArrayData_get_null_count(const std::shared_ptr<arrow::ArrayData>& x){
  return x->null_count;
}

// [[Rcpp::export]]
int ArrayData_get_offset(const std::shared_ptr<arrow::ArrayData>& x){
  return x->offset;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array_initialize(const std::shared_ptr<arrow::ArrayData>& data_){
  return MakeArray(data_);
}

// [[Rcpp::export]]
bool Array_IsNull(const std::shared_ptr<arrow::Array>& x, int i){
  return x->IsNull(i);
}

// [[Rcpp::export]]
bool Array_IsValid(const std::shared_ptr<arrow::Array>& x, int i){
  return x->IsValid(i);
}

// [[Rcpp::export]]
int Array_length(const std::shared_ptr<arrow::Array>& x){
  return x->length();
}

// [[Rcpp::export]]
int Array_offset(const std::shared_ptr<arrow::Array>& x){
  return x->offset();
}

// [[Rcpp::export]]
int Array_null_count(const std::shared_ptr<arrow::Array>& x){
  return x->null_count();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Array_type(const std::shared_ptr<arrow::Array>& x){
  // TODO: this is just an xp for now, the R6 DataType class should dispatch it to a real R6 object somehow
  return x->type();
}

// [[Rcpp::export]]
arrow::Type::type Array_type_id(const std::shared_ptr<arrow::Array>& x){
  return x->type_id();
}

// TODO: null_bitmap when class Buffer is available

// [[Rcpp::export]]
bool Array_Equals(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs){
  return lhs->Equals(rhs);
}

// [[Rcpp::export]]
bool Array_ApproxEquals(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs){
  return lhs->ApproxEquals(rhs);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ArrayData> Array_data(const std::shared_ptr<arrow::Array>& array){
  return array->data();
}

// //' @export
// // [[Rcpp::export]]
// xptr_ArrayBuilder ArrayBuilder(xptr_DataType xptr_type) {
//   if (!Rf_inherits(xptr_type, "arrow::DataType")) stop("incompatible");
//
//   std::shared_ptr<arrow::DataType>& type = *xptr_type;
//
//   auto memory_pool = arrow::default_memory_pool();
//   std::unique_ptr<arrow::ArrayBuilder>* arrow_builder =
//       new std::unique_ptr<arrow::ArrayBuilder>;
//   auto status = arrow::MakeBuilder(memory_pool, type, arrow_builder);
//
//   xptr_ArrayBuilder res(arrow_builder);
//   res.attr("class") =
//       CharacterVector::create(DEMANGLE(**arrow_builder), "arrow::ArrayBuilder");
//   return res;
// }
//
// // [[Rcpp::export]]
// int ArrayBuilder__num_children(xptr_ArrayBuilder xptr_type) {
//   return (*xptr_type)->num_children();
// }

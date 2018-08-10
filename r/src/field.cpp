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

#include <Rcpp.h>
#include "arrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp;

//' @export
// [[Rcpp::export]]
xptr_Field field_pointer(const std::string& name, xptr_DataType type, bool nullable = true) {
  auto ptr = arrow::field(name, *type, nullable);
  xptr_Field res(new std::shared_ptr<arrow::Field>(ptr));
  res.attr("name") = ptr->name();
  res.attr("class") = CharacterVector::create("arrow::Field");
  return res;
}

// [[Rcpp::export]]
std::string Field_ToString(xptr_Field type) {
  return std::shared_ptr<arrow::Field>(*type)->ToString();
}

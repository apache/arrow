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
xptr_ArrayBuilder ArrayBuilder(xptr_DataType xptr_type) {
  if (!Rf_inherits(xptr_type, "arrow::DataType")) stop("incompatible");

  std::shared_ptr<arrow::DataType>& type = *xptr_type;

  auto memory_pool = arrow::default_memory_pool();
  std::unique_ptr<arrow::ArrayBuilder>* arrow_builder =
      new std::unique_ptr<arrow::ArrayBuilder>;
  auto status = arrow::MakeBuilder(memory_pool, type, arrow_builder);

  xptr_ArrayBuilder res(arrow_builder);
  res.attr("class") =
      CharacterVector::create(DEMANGLE(**arrow_builder), "arrow::ArrayBuilder");
  return res;
}

// [[Rcpp::export]]
int ArrayBuilder__num_children(xptr_ArrayBuilder xptr_type) {
  return (*xptr_type)->num_children();
}

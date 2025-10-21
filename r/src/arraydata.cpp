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

#include <arrow/array/data.h>

// [[arrow::export]]
std::shared_ptr<arrow::DataType> ArrayData__get_type(
    const std::shared_ptr<arrow::ArrayData>& x) {
  return x->type;
}

// [[arrow::export]]
r_vec_size ArrayData__get_length(const std::shared_ptr<arrow::ArrayData>& x) {
  return r_vec_size(x->length);
}

// [[arrow::export]]
r_vec_size ArrayData__get_null_count(const std::shared_ptr<arrow::ArrayData>& x) {
  return r_vec_size(x->null_count);
}

// [[arrow::export]]
r_vec_size ArrayData__get_offset(const std::shared_ptr<arrow::ArrayData>& x) {
  return r_vec_size(x->offset);
}

// [[arrow::export]]
cpp11::list ArrayData__buffers(const std::shared_ptr<arrow::ArrayData>& x) {
  return arrow::r::to_r_list(x->buffers);
}

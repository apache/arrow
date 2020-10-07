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

#if defined(ARROW_R_WITH_ARROW)

// [[arrow::export]]
bool Buffer__is_mutable(const std::shared_ptr<arrow::Buffer>& buffer) {
  return buffer->is_mutable();
}

// [[arrow::export]]
void Buffer__ZeroPadding(const std::shared_ptr<arrow::Buffer>& buffer) {
  buffer->ZeroPadding();
}

// [[arrow::export]]
int64_t Buffer__capacity(const std::shared_ptr<arrow::Buffer>& buffer) {
  return buffer->capacity();
}

// [[arrow::export]]
int64_t Buffer__size(const std::shared_ptr<arrow::Buffer>& buffer) {
  return buffer->size();
}

// [[arrow::export]]
R6 r___RBuffer__initialize(SEXP x) {
  switch (TYPEOF(x)) {
    case RAWSXP:
      return cpp11::r6(std::make_shared<arrow::r::RBuffer<cpp11::raws>>(x), "Buffer");
    case REALSXP:
      return cpp11::r6(std::make_shared<arrow::r::RBuffer<cpp11::doubles>>(x), "Buffer");
    case INTSXP:
      return cpp11::r6(std::make_shared<arrow::r::RBuffer<cpp11::integers>>(x), "Buffer");
    case CPLXSXP:
      return cpp11::r6(
          std::make_shared<arrow::r::RBuffer<arrow::r::complexs>>(arrow::r::complexs(x)),
          "Buffer");
    default:
      break;
  }
  cpp11::stop("R object of type <%s> not supported", Rf_type2char(TYPEOF(x)));
}

// [[arrow::export]]
cpp11::writable::raws Buffer__data(const std::shared_ptr<arrow::Buffer>& buffer) {
  return cpp11::writable::raws(buffer->data(), buffer->data() + buffer->size());
}

// [[arrow::export]]
bool Buffer__Equals(const std::shared_ptr<arrow::Buffer>& x,
                    const std::shared_ptr<arrow::Buffer>& y) {
  return x->Equals(*y.get());
}

#endif

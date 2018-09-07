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
using namespace arrow;

namespace arrow{
namespace r{

template <int RTYPE, typename Vec = Rcpp::Vector<RTYPE> >
class SimpleRBuffer : public arrow::Buffer {
public:

  SimpleRBuffer(Vec vec) :
    Buffer(reinterpret_cast<const uint8_t*>(vec.begin()), vec.size() * sizeof(typename Vec::stored_type) ),
    vec_(vec)
  {}

private:

  // vec_ holds the memory
  Vec vec_;
};

template <int RTYPE, typename Type>
std::shared_ptr<arrow::Array> SimpleArray(SEXP x){
  // a simple buffer that owns the memory of `x`
  auto buffer = std::make_shared<SimpleRBuffer<RTYPE>>(x);
  auto type = std::make_shared<Type>();

  auto data = ArrayData::Make(
    type,
    LENGTH(x),
    {nullptr, buffer}, /* for now we just use a nullptr for the null bitmap buffer */
    0, /*null_count */
    0 /*offset*/
  );

  // return the right Array class
  return std::make_shared<arrow::NumericArray<Type>>(data);
}

}
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> rvector_to_Array(SEXP x){
  switch(TYPEOF(x)){
  case INTSXP:
    if (Rf_isFactor(x)) {
      break;
    }
    return arrow::r::SimpleArray<INTSXP, arrow::Int32Type>(x);
  case REALSXP:
    // TODO: Dates, ...
    return arrow::r::SimpleArray<REALSXP, arrow::DoubleType>(x);
  case RAWSXP:
    return arrow::r::SimpleArray<REALSXP, arrow::DoubleType>(x);
  default:
    break;
  }

  stop("not handled");
  return nullptr;
}




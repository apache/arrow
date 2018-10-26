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
#include <arrow/util/decimal.h>

using namespace Rcpp;

// [[Rcpp::export]]
CharacterVector format_decimal128(ComplexVector_ data, int scale){
  auto n = data.size();
  auto p = reinterpret_cast<arrow::Decimal128*>(data.begin());
  CharacterVector res(no_init(n));
  for (R_xlen_t i =0; i<n; i++, ++p) {
    res[i] = p->ToString(scale);
  }

  return res;
}

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

namespace arrow {
namespace r {
SEXP symbols::units = Rf_install("units");
SEXP symbols::xp = Rf_install(".:xp:.");
SEXP symbols::dot_Internal = Rf_install(".Internal");
SEXP symbols::inspect = Rf_install("inspect");

void inspect(SEXP obj) {
  Rcpp::Shield<SEXP> call_inspect(Rf_lang2(symbols::inspect, obj));
  Rcpp::Shield<SEXP> call_internal(Rf_lang2(symbols::dot_Internal, call_inspect));
  Rf_eval(call_internal, R_GlobalEnv);
}

}  // namespace r
}  // namespace arrow

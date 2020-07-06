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
SEXP symbols::tzone = Rf_install("tzone");
SEXP symbols::xp = Rf_install(".:xp:.");
SEXP symbols::dot_Internal = Rf_install(".Internal");
SEXP symbols::inspect = Rf_install("inspect");
SEXP symbols::row_names = Rf_install("row.names");
SEXP symbols::serialize_arrow_r_metadata = Rf_install(".serialize_arrow_r_metadata");
SEXP symbols::as_list = Rf_install("as.list");
SEXP symbols::ptype = Rf_install("ptype");

SEXP preserved_strings(std::initializer_list<std::string> list) {
  size_t n = list.size();
  SEXP s = Rf_allocVector(STRSXP, n);
  R_PreserveObject(s);

  auto it = list.begin();
  for (size_t i = 0; i < n; i++, ++it) {
    SET_STRING_ELT(s, i, Rf_mkCharLen(it->c_str(), it->size()));
  }

  return s;
}

SEXP get_empty_raw() {
  SEXP res = Rf_allocVector(RAWSXP, 0);
  R_PreserveObject(res);
  return res;
}

SEXP data::classes_POSIXct = preserved_strings({"POSIXct", "POSIXt"});
SEXP data::classes_metadata_r = preserved_strings({"arrow_r_metadata"});
SEXP data::classes_factor = preserved_strings({"factor"});
SEXP data::classes_ordered = preserved_strings({"ordered", "factor"});

SEXP data::names_metadata = preserved_strings({"attributes", "columns"});
SEXP data::classes_vctrs_list_of =
    preserved_strings({"vctrs_list_of", "vctrs_vctr", "list"});
SEXP data::empty_raw = get_empty_raw();

void inspect(SEXP obj) {
  Rcpp::Shield<SEXP> call_inspect(Rf_lang2(symbols::inspect, obj));
  Rcpp::Shield<SEXP> call_internal(Rf_lang2(symbols::dot_Internal, call_inspect));
  Rf_eval(call_internal, R_GlobalEnv);
}

SEXP get_arrow_ns() {
  SEXP name = PROTECT(Rf_ScalarString(Rf_mkChar("arrow")));
  SEXP ns = R_FindNamespace(name);
  R_PreserveObject(ns);
  UNPROTECT(1);
  return ns;
}

SEXP ns::arrow = get_arrow_ns();

}  // namespace r
}  // namespace arrow

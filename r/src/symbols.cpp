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
SEXP symbols::byte_width = Rf_install("byte_width");
SEXP symbols::list_size = Rf_install("list_size");

// persistently protect `x` and return it
SEXP precious(SEXP x) {
  R_PreserveObject(x);
  return x;
}

// return R string vector, e.g.
// strings({"foo", "bar"}) returns a size 2 STRSXP
SEXP strings(std::initializer_list<std::string> list) {
  size_t n = list.size();
  SEXP s = PROTECT(Rf_allocVector(STRSXP, n));

  auto it = list.begin();
  for (size_t i = 0; i < n; i++, ++it) {
    SET_STRING_ELT(s, i, Rf_mkCharLen(it->c_str(), it->size()));
  }

  UNPROTECT(1);
  return s;
}

// returns the namespace environment for package `name`
SEXP r_namespace(std::string name) {
  SEXP s_name = PROTECT(strings({name}));
  SEXP ns = R_FindNamespace(s_name);
  UNPROTECT(1);
  return ns;
}

SEXP data::classes_POSIXct = precious(strings({"POSIXct", "POSIXt"}));
SEXP data::classes_metadata_r = precious(strings({"arrow_r_metadata"}));
SEXP data::classes_vctrs_list_of =
    precious(strings({"vctrs_list_of", "vctrs_vctr", "list"}));

SEXP data::classes_arrow_binary =
    precious(strings({"arrow_binary", "arrow_binary_vctr", "vctrs_vctr", "list"}));
SEXP data::classes_arrow_large_binary =
    precious(strings({"arrow_large_binary", "arrow_binary_vctr", "vctrs_vctr", "list"}));
SEXP data::classes_arrow_fixed_size_binary = precious(
    strings({"arrow_fixed_size_binary", "arrow_binary_vctr", "vctrs_vctr", "list"}));

SEXP data::classes_arrow_list = precious(
    strings({"arrow_list", "arrow_list_of", "vctrs_list_of", "vctrs_vctr", "list"}));
SEXP data::classes_arrow_large_list = precious(strings(
    {"arrow_large_list", "arrow_list_of", "vctrs_list_of", "vctrs_vctr", "list"}));
SEXP data::classes_arrow_fixed_size_list = precious(strings(
    {"arrow_fixed_size_list", "arrow_list_of", "vctrs_list_of", "vctrs_vctr", "list"}));

SEXP data::names_metadata = precious(strings({"attributes", "columns"}));
SEXP data::empty_raw = precious(Rf_allocVector(RAWSXP, 0));

SEXP ns::arrow = precious(r_namespace("arrow"));

void inspect(SEXP obj) {
  Rcpp::Shield<SEXP> call_inspect(Rf_lang2(symbols::inspect, obj));
  Rcpp::Shield<SEXP> call_internal(Rf_lang2(symbols::dot_Internal, call_inspect));
  Rf_eval(call_internal, R_GlobalEnv);
}

}  // namespace r
}  // namespace arrow

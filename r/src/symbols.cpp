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

SEXP get_classes_POSIXct() {
  SEXP classes = Rf_allocVector(STRSXP, 2);
  R_PreserveObject(classes);
  SET_STRING_ELT(classes, 0, Rf_mkChar("POSIXct"));
  SET_STRING_ELT(classes, 1, Rf_mkChar("POSIXt"));
  return classes;
}

SEXP get_classes_metadata_r() {
  SEXP classes = Rf_mkString("arrow_r_metadata");
  R_PreserveObject(classes);
  return classes;
}

SEXP get_names_metadata() {
  SEXP names = Rf_allocVector(STRSXP, 2);
  R_PreserveObject(names);
  SET_STRING_ELT(names, 0, Rf_mkChar("attributes"));
  SET_STRING_ELT(names, 1, Rf_mkChar("columns"));
  return names;
}

SEXP get_classes_vctrs_list_of() {
  SEXP classes = Rf_allocVector(STRSXP, 3);
  R_PreserveObject(classes);
  SET_STRING_ELT(classes, 0, Rf_mkChar("vctrs_list_of"));
  SET_STRING_ELT(classes, 1, Rf_mkChar("vctrs_vctr"));
  SET_STRING_ELT(classes, 2, Rf_mkChar("list"));
  return classes;
}

SEXP get_empty_raw() {
  SEXP res = Rf_allocVector(RAWSXP, 0);
  R_PreserveObject(res);
  return res;
}

SEXP data::classes_POSIXct = get_classes_POSIXct();
SEXP data::classes_metadata_r = get_classes_metadata_r();
SEXP data::names_metadata = get_names_metadata();
SEXP data::classes_vctrs_list_of = get_classes_vctrs_list_of();
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

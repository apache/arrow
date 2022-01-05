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
SEXP symbols::arrow_attributes = Rf_install("arrow_attributes");
SEXP symbols::new_ = Rf_install("new");
SEXP symbols::create = Rf_install("create");
SEXP symbols::arrow = Rf_install("arrow");

// persistently protect `x` and return it
SEXP precious(SEXP x) {
  PROTECT(x);
  R_PreserveObject(x);
  UNPROTECT(1);
  return x;
}

// returns the namespace environment for package `name`
SEXP precious_namespace(std::string name) {
  SEXP s_name = PROTECT(cpp11::writable::strings({name}));
  SEXP ns = R_FindNamespace(s_name);
  R_PreserveObject(ns);
  UNPROTECT(1);

  return ns;
}
SEXP data::classes_POSIXct = precious(cpp11::writable::strings({"POSIXct", "POSIXt"}));
SEXP data::classes_metadata_r = precious(cpp11::writable::strings({"arrow_r_metadata"}));
SEXP data::classes_vctrs_list_of =
    precious(cpp11::writable::strings({"vctrs_list_of", "vctrs_vctr", "list"}));
SEXP data::classes_tbl_df =
    precious(cpp11::writable::strings({"tbl_df", "tbl", "data.frame"}));

SEXP data::classes_arrow_binary =
    precious(cpp11::writable::strings({"arrow_binary", "vctrs_vctr", "list"}));
SEXP data::classes_arrow_large_binary =
    precious(cpp11::writable::strings({"arrow_large_binary", "vctrs_vctr", "list"}));
SEXP data::classes_arrow_fixed_size_binary =
    precious(cpp11::writable::strings({"arrow_fixed_size_binary", "vctrs_vctr", "list"}));
SEXP data::classes_factor = precious(cpp11::writable::strings({"factor"}));
SEXP data::classes_ordered = precious(cpp11::writable::strings({"ordered", "factor"}));

SEXP data::classes_arrow_list = precious(
    cpp11::writable::strings({"arrow_list", "vctrs_list_of", "vctrs_vctr", "list"}));
SEXP data::classes_arrow_large_list = precious(cpp11::writable::strings(
    {"arrow_large_list", "vctrs_list_of", "vctrs_vctr", "list"}));
SEXP data::classes_arrow_fixed_size_list = precious(cpp11::writable::strings(
    {"arrow_fixed_size_list", "vctrs_list_of", "vctrs_vctr", "list"}));

SEXP data::names_metadata = precious(cpp11::writable::strings({"attributes", "columns"}));

SEXP ns::arrow = precious_namespace("arrow");

void inspect(SEXP obj) {
  SEXP call_inspect = PROTECT(Rf_lang2(symbols::inspect, obj));
  SEXP call_internal = PROTECT(Rf_lang2(symbols::dot_Internal, call_inspect));
  Rf_eval(call_internal, R_GlobalEnv);
  UNPROTECT(2);
}

}  // namespace r
}  // namespace arrow

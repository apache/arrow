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

namespace Rcpp {
namespace internal {

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_shared_ptr_tag) {
  return Rcpp::XPtr<std::shared_ptr<typename T::element_type>>(
      new std::shared_ptr<typename T::element_type>(x));
}

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_unique_ptr_tag) {
  return Rcpp::XPtr<std::unique_ptr<typename T::element_type>>(
      new std::unique_ptr<typename T::element_type>(const_cast<T&>(x).release()));
}

}  // namespace internal

}  // namespace Rcpp

namespace Rcpp {
using NumericVector_ = Rcpp::Vector<REALSXP, Rcpp::NoProtectStorage>;
using IntegerVector_ = Rcpp::Vector<INTSXP, Rcpp::NoProtectStorage>;
using LogicalVector_ = Rcpp::Vector<LGLSXP, Rcpp::NoProtectStorage>;
using StringVector_ = Rcpp::Vector<STRSXP, Rcpp::NoProtectStorage>;
using CharacterVector_ = StringVector_;
using RawVector_ = Rcpp::Vector<RAWSXP, Rcpp::NoProtectStorage>;
using List_ = Rcpp::Vector<VECSXP, Rcpp::NoProtectStorage>;

template <int RTYPE>
inline constexpr typename Rcpp::Vector<RTYPE>::stored_type default_value() {
  return Rcpp::Vector<RTYPE>::get_na();
}
template <>
inline constexpr Rbyte default_value<RAWSXP>() {
  return 0;
}

}  // namespace Rcpp

namespace arrow {
namespace r {

template <typename T>
inline std::shared_ptr<T> extract(SEXP x) {
  return Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<T>>(x);
}

}  // namespace r
}  // namespace arrow

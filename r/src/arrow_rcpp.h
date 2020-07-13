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

#include <RcppCommon.h>

#include <limits>
#include <memory>
#include <utility>
#include <vector>
#undef Free

namespace arrow {
namespace r {
struct symbols {
  static SEXP units;
  static SEXP tzone;
  static SEXP xp;
  static SEXP dot_Internal;
  static SEXP inspect;
  static SEXP row_names;
  static SEXP serialize_arrow_r_metadata;
  static SEXP as_list;
  static SEXP ptype;
  static SEXP byte_width;
  static SEXP list_size;
};

struct data {
  static SEXP classes_POSIXct;
  static SEXP classes_metadata_r;
  static SEXP classes_vctrs_list_of;

  static SEXP classes_arrow_binary;
  static SEXP classes_arrow_large_binary;
  static SEXP classes_arrow_fixed_size_binary;

  static SEXP classes_arrow_list;
  static SEXP classes_arrow_large_list;
  static SEXP classes_arrow_fixed_size_list;

  static SEXP classes_factor;
  static SEXP classes_ordered;

  static SEXP names_metadata;
  static SEXP empty_raw;
};

struct ns {
  static SEXP arrow;
};

}  // namespace r
}  // namespace arrow

namespace Rcpp {
namespace internal {

template <typename Pointer>
Pointer r6_to_smart_pointer(SEXP self) {
  return reinterpret_cast<Pointer>(
      R_ExternalPtrAddr(Rf_findVarInFrame(self, arrow::r::symbols::xp)));
}

}  // namespace internal

template <typename T>
class ConstReferenceSmartPtrInputParameter {
 public:
  using const_reference = const T&;

  explicit ConstReferenceSmartPtrInputParameter(SEXP self)
      : ptr(internal::r6_to_smart_pointer<const T*>(self)) {}

  inline operator const_reference() { return *ptr; }

 private:
  const T* ptr;
};

template <typename T>
class ConstReferenceVectorSmartPtrInputParameter {
 public:
  using const_reference = const std::vector<T>&;

  explicit ConstReferenceVectorSmartPtrInputParameter(SEXP self) : vec() {
    R_xlen_t n = XLENGTH(self);
    for (R_xlen_t i = 0; i < n; i++) {
      vec.push_back(*internal::r6_to_smart_pointer<const T*>(VECTOR_ELT(self, i)));
    }
  }

  inline operator const_reference() { return vec; }

 private:
  std::vector<T> vec;
};

namespace traits {

template <typename T>
struct input_parameter<const std::shared_ptr<T>&> {
  typedef typename Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<T>> type;
};

template <typename T>
struct input_parameter<const std::unique_ptr<T>&> {
  typedef typename Rcpp::ConstReferenceSmartPtrInputParameter<std::unique_ptr<T>> type;
};

template <typename T>
struct input_parameter<const std::vector<std::shared_ptr<T>>&> {
  typedef typename Rcpp::ConstReferenceVectorSmartPtrInputParameter<std::shared_ptr<T>>
      type;
};

struct wrap_type_shared_ptr_tag {};
struct wrap_type_unique_ptr_tag {};

template <typename T>
struct wrap_type_traits<std::shared_ptr<T>> {
  using wrap_category = wrap_type_shared_ptr_tag;
};

template <typename T>
struct wrap_type_traits<std::unique_ptr<T>> {
  using wrap_category = wrap_type_unique_ptr_tag;
};

}  // namespace traits

namespace internal {

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_shared_ptr_tag);

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_unique_ptr_tag);

}  // namespace internal
}  // namespace Rcpp

#include <Rcpp.h>

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

}  // namespace Rcpp

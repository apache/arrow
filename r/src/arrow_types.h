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

#pragma once

#include <limits>
#include <memory>
#include <vector>

#include <RcppCommon.h>
#undef Free

namespace arrow {
namespace r {
struct symbols {
  static SEXP units;
  static SEXP xp;
  static SEXP dot_Internal;
  static SEXP inspect;
  static SEXP row_names;
};
}  // namespace r
}  // namespace arrow

#define STOP_IF_NOT(TEST, MSG)    \
  do {                            \
    if (!(TEST)) Rcpp::stop(MSG); \
  } while (0)

#define STOP_IF_NOT_OK(s) STOP_IF_NOT(s.ok(), s.ToString())

template <typename T>
struct NoDelete {
  inline void operator()(T* ptr) {}
};

namespace Rcpp {
namespace internal {

template <typename Pointer>
Pointer r6_to_smart_pointer(SEXP self) {
  return reinterpret_cast<Pointer>(
      EXTPTR_PTR(Rf_findVarInFrame(self, arrow::r::symbols::xp)));
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

namespace traits {

template <typename T>
struct input_parameter<const std::shared_ptr<T>&> {
  typedef typename Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<T>> type;
};

template <typename T>
struct input_parameter<const std::unique_ptr<T>&> {
  typedef typename Rcpp::ConstReferenceSmartPtrInputParameter<std::unique_ptr<T>> type;
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

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/csv/reader.h>
#include <arrow/io/compressed.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/feather.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/json/reader.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>

RCPP_EXPOSED_ENUM_NODECL(arrow::Type::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::DateUnit)
RCPP_EXPOSED_ENUM_NODECL(arrow::TimeUnit::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::StatusCode)
RCPP_EXPOSED_ENUM_NODECL(arrow::io::FileMode::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::ipc::Message::Type)
RCPP_EXPOSED_ENUM_NODECL(arrow::Compression::type)

SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array);
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array);
std::shared_ptr<arrow::Array> Array__from_vector(SEXP x, SEXP type);
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_arrays(SEXP, SEXP);
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_dataframe(Rcpp::DataFrame tbl);

namespace arrow {
namespace r {

std::shared_ptr<arrow::Array> Array__from_vector(
    SEXP x, const std::shared_ptr<arrow::DataType>& type, bool type_infered);

template <typename T>
std::vector<std::shared_ptr<T>> List_to_shared_ptr_vector(SEXP x) {
  std::vector<std::shared_ptr<T>> vec;
  R_xlen_t n = Rf_xlength(x);
  for (R_xlen_t i = 0; i < n; i++) {
    Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<T>> ptr(VECTOR_ELT(x, i));
    vec.push_back(ptr);
  }
  return vec;
}

void inspect(SEXP obj);

// the integer64 sentinel
constexpr int64_t NA_INT64 = std::numeric_limits<int64_t>::min();

template <int RTYPE, typename Vec = Rcpp::Vector<RTYPE>>
class RBuffer : public MutableBuffer {
 public:
  explicit RBuffer(Vec vec)
      : MutableBuffer(reinterpret_cast<uint8_t*>(vec.begin()),
                      vec.size() * sizeof(typename Vec::stored_type)),
        vec_(vec) {}

 private:
  // vec_ holds the memory
  Vec vec_;
};

}  // namespace r
}  // namespace arrow

#endif

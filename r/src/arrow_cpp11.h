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

#include <limits>
#include <memory>
#include <utility>
#include <vector>
#undef Free

namespace cpp11 {

template <typename T>
SEXP as_sexp(const std::shared_ptr<T>& ptr);

template <typename T>
SEXP as_sexp(const std::vector<std::shared_ptr<T>>& vec);

}  // namespace cpp11

#include <cpp11.hpp>

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
  static SEXP classes_tbl_df;

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

template <typename Pointer>
Pointer r6_to_pointer(SEXP self) {
  return reinterpret_cast<Pointer>(
      R_ExternalPtrAddr(Rf_findVarInFrame(self, arrow::r::symbols::xp)));
}

// T is either std::shared_ptr<U> or std::unique_ptr<U>
// e.g. T = std::shared_ptr<arrow::Array>
template <typename T>
class ExternalPtrInput {
 public:
  explicit ExternalPtrInput(SEXP self) : ptr_(r6_to_pointer<const T*>(self)) {}

  operator const T&() const { return *ptr_; }

 private:
  const T* ptr_;
};

template <typename T>
class VectorExternalPtrInput {
 public:
  explicit VectorExternalPtrInput(SEXP self) : vec_(XLENGTH(self)) {
    R_xlen_t i = 0;
    for (auto& element : vec_) {
      element = *r6_to_pointer<const T*>(VECTOR_ELT(self, i++));
    }
  }
  operator const std::vector<T>&() const { return vec_; }

 private:
  std::vector<T> vec_;
};

template <typename T>
class DefaultInput {
 public:
  explicit DefaultInput(SEXP from) : from_(from) {}

  operator T() const { return cpp11::as_cpp<T>(from_); }

 private:
  SEXP from_;
};

template <typename T>
class ConstReferenceInput {
 public:
  explicit ConstReferenceInput(SEXP from) : obj_(cpp11::as_cpp<T>(from)) {}

  using const_reference = const T&;
  operator const_reference() const { return obj_; }

 private:
  T obj_;
};

template <typename T>
struct Input {
  using type = DefaultInput<T>;
};

template <typename T>
struct Input<const T&> {
  using type = ConstReferenceInput<typename std::decay<T>::type>;
};

template <typename T>
struct Input<const std::shared_ptr<T>&> {
  using type = ExternalPtrInput<std::shared_ptr<T>>;
};

template <typename T>
struct Input<const std::unique_ptr<T>&> {
  using type = ExternalPtrInput<std::unique_ptr<T>>;
};

template <typename T>
struct Input<const std::vector<std::shared_ptr<T>>&> {
  using type = VectorExternalPtrInput<std::shared_ptr<T>>;
};

template <typename Rvector, typename T, typename ToVectorElement>
Rvector to_r_vector(const std::vector<std::shared_ptr<T>>& x,
                    ToVectorElement&& to_element) {
  auto n = x.size();
  Rvector out(n);
  for (int i = 0; i < n; i++) {
    out[i] = to_element(x[i]);
  }
  return out;
}

template <typename T, typename ToString>
cpp11::writable::strings to_r_strings(const std::vector<std::shared_ptr<T>>& x,
                                      ToString&& to_string) {
  return to_r_vector<cpp11::writable::strings>(x, std::forward<ToString>(to_string));
}

template <typename T>
cpp11::writable::list to_r_list(const std::vector<std::shared_ptr<T>>& x) {
  auto as_sexp = [](const std::shared_ptr<T>& t) { return cpp11::as_sexp(t); };
  return to_r_vector<cpp11::writable::list>(x, as_sexp);
}

template <typename T, typename ToListElement>
cpp11::writable::list to_r_list(const std::vector<std::shared_ptr<T>>& x,
                                ToListElement&& to_element) {
  auto as_sexp = [&](const std::shared_ptr<T>& t) {
    return cpp11::as_sexp(to_element(t));
  };
  return to_r_vector<cpp11::writable::list>(x, as_sexp);
}

inline cpp11::writable::integers short_row_names(int n) { return {NA_INTEGER, -n}; }

template <typename T>
std::vector<T> from_r_list(cpp11::list args) {
  std::vector<T> vec;
  R_xlen_t n = args.size();
  for (R_xlen_t i = 0; i < n; i++) {
    vec.push_back(cpp11::as_cpp<T>(args[i]));
  }
  return vec;
}

}  // namespace r
}  // namespace arrow

namespace cpp11 {

template <typename T>
using enable_if_shared_ptr = typename std::enable_if<
    std::is_same<std::shared_ptr<typename T::element_type>, T>::value, T>::type;

template <typename T>
enable_if_shared_ptr<T> as_cpp(SEXP from) {
  return arrow::r::ExternalPtrInput<T>(from);
}

template <typename T>
SEXP as_sexp(const std::shared_ptr<T>& ptr) {
  return cpp11::external_pointer<std::shared_ptr<T>>(new std::shared_ptr<T>(ptr));
}

template <typename T>
SEXP as_sexp(const std::vector<std::shared_ptr<T>>& vec) {
  return arrow::r::to_r_list(vec);
}

template <typename E>
enable_if_enum<E, SEXP> as_sexp(E e) {
  return as_sexp(static_cast<int>(e));
}

}  // namespace cpp11

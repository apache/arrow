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

template <typename E, typename std::enable_if<std::is_enum<E>::value>::type* = nullptr>
SEXP as_sexp(E e);

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

class Index {
 public:
  Index(SEXP x) : index_(validate_index(x)) {}

  inline operator R_xlen_t() const { return index_; }

 private:
  R_xlen_t index_;

  static R_xlen_t validate_index(SEXP x) {
    if (XLENGTH(x) == 1) {
      switch (TYPEOF(x)) {
        case INTSXP:
          return INTEGER_ELT(x, 0);
        case REALSXP:
          if (cpp11::is_convertable_without_loss_to_integer(REAL_ELT(x, 0)))
            return REAL_ELT(x, 0);
        case LGLSXP:
          return LOGICAL_ELT(x, 0);
        default:
          break;
      }
    }

    cpp11::stop("Expected single integer value");
    return 0;
  }
};

template <typename Pointer>
Pointer r6_to_pointer(SEXP self) {
  return reinterpret_cast<Pointer>(
      R_ExternalPtrAddr(Rf_findVarInFrame(self, arrow::r::symbols::xp)));
}

template <typename T, template <class> class SmartPtr>
class ConstRefSmartPtrInput {
 public:
  using const_reference = const SmartPtr<T>&;

  explicit ConstRefSmartPtrInput(SEXP self)
      : ptr(r6_to_pointer<const SmartPtr<T>*>(self)) {}

  inline operator const_reference() { return *ptr; }

 private:
  // this class host
  const SmartPtr<T>* ptr;
};

template <typename T, template <class> class SmartPtr>
class ConstRefVectorSmartPtrInput {
 public:
  using const_reference = const std::vector<SmartPtr<T>>&;

  explicit ConstRefVectorSmartPtrInput(SEXP self) : vec() {
    R_xlen_t n = XLENGTH(self);
    for (R_xlen_t i = 0; i < n; i++) {
      vec.push_back(*r6_to_pointer<const SmartPtr<T>*>(VECTOR_ELT(self, i)));
    }
  }

  inline operator const_reference() { return vec; }

 private:
  std::vector<SmartPtr<T>> vec;
};

template <typename T>
class default_input {
 public:
  explicit default_input(SEXP from) : from_(from) {}

  operator T() const { return cpp11::as_cpp<T>(from_); }

 private:
  SEXP from_;
};

template <typename T>
class const_reference_input {
 public:
  explicit const_reference_input(SEXP from) : obj_(cpp11::as_cpp<T>(from)) {}

  using const_reference = const T&;
  operator const_reference() const { return obj_; }

 private:
  T obj_;
};

template <typename T>
struct input {
  using type = default_input<T>;
};

template <typename T>
struct input<const T&> {
  using type = const_reference_input<typename std::decay<T>::type>;
};

template <typename T>
struct input<const std::shared_ptr<T>&> {
  using type = ConstRefSmartPtrInput<T, std::shared_ptr>;
};

template <typename T>
using default_unique_ptr = std::unique_ptr<T, std::default_delete<T>>;

template <typename T>
struct input<const std::unique_ptr<T>&> {
  using type = ConstRefSmartPtrInput<T, default_unique_ptr>;
};

template <typename T>
struct input<const std::vector<std::shared_ptr<T>>&> {
  using type = ConstRefVectorSmartPtrInput<T, std::shared_ptr>;
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
  return arrow::r::ConstRefSmartPtrInput<typename T::element_type, std::shared_ptr>(from);
}

template <typename T>
SEXP as_sexp(const std::shared_ptr<T>& ptr) {
  return cpp11::external_pointer<std::shared_ptr<T>>(new std::shared_ptr<T>(ptr));
}

template <typename T>
SEXP as_sexp(const std::vector<std::shared_ptr<T>>& vec) {
  return arrow::r::to_r_list(vec);
}

template <typename E, typename std::enable_if<std::is_enum<E>::value>::type*>
SEXP as_sexp(E e) {
  return as_sexp(static_cast<int>(e));
}

}  // namespace cpp11

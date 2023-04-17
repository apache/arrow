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

#include <cstring>  // for strlen
#include <limits>
#include <memory>
#include <utility>
#include <vector>
#undef Free

#include <cpp11.hpp>
#include <cpp11/altrep.hpp>

#include "./nameof.h"

// borrowed from enc package
// because R does not make these macros available (i.e. from Defn.h)
#define UTF8_MASK (1 << 3)
#define ASCII_MASK (1 << 6)

#define IS_ASCII(x) (LEVELS(x) & ASCII_MASK)
#define IS_UTF8(x) (LEVELS(x) & UTF8_MASK)

// For context, see:
// https://github.com/r-devel/r-svn/blob/6418faeb6f5d87d3d9b92b8978773bc3856b4b6f/src/main/altrep.c#L37
#define ALTREP_CLASS_SERIALIZED_CLASS(x) ATTRIB(x)
#define ALTREP_SERIALIZED_CLASS_PKGSYM(x) CADR(x)

#if (R_VERSION < R_Version(3, 5, 0))
#define LOGICAL_RO(x) ((const int*)LOGICAL(x))
#define INTEGER_RO(x) ((const int*)INTEGER(x))
#define REAL_RO(x) ((const double*)REAL(x))
#define COMPLEX_RO(x) ((const Rcomplex*)COMPLEX(x))
#define STRING_PTR_RO(x) ((const SEXP*)STRING_PTR(x))
#define RAW_RO(x) ((const Rbyte*)RAW(x))
#define DATAPTR_RO(x) ((const void*)STRING_PTR(x))
#define DATAPTR(x) (void*)STRING_PTR(x)
#endif

namespace arrow {
namespace r {

template <typename T>
struct Pointer {
  Pointer() : ptr_(new T()) {}
  explicit Pointer(SEXP x) {
    if (TYPEOF(x) == EXTPTRSXP) {
      ptr_ = (T*)R_ExternalPtrAddr(x);
    } else if (TYPEOF(x) == STRSXP && Rf_length(x) == 1) {
      // User passed a character representation of the pointer address
      SEXP char0 = STRING_ELT(x, 0);
      if (char0 == NA_STRING) {
        cpp11::stop("Can't convert NA_character_ to pointer");
      }

      const char* input_chars = CHAR(char0);
      char* endptr;
      uint64_t ptr_value = strtoull(input_chars, &endptr, 0);
      if (endptr != (input_chars + strlen(input_chars))) {
        cpp11::stop("Can't parse '%s' as a 64-bit integer address", input_chars);
      }

      ptr_ = reinterpret_cast<T*>(static_cast<uintptr_t>(ptr_value));
    } else if (Rf_inherits(x, "integer64") && Rf_length(x) == 1) {
      // User passed an integer64(1) of the pointer address
      // an integer64 is a REALSXP under the hood, with the bytes
      // of each double reinterpreted as an int64.
      uint64_t ptr_value;
      memcpy(&ptr_value, REAL(x), sizeof(uint64_t));
      ptr_ = reinterpret_cast<T*>(static_cast<uintptr_t>(ptr_value));
    } else if (TYPEOF(x) == RAWSXP && Rf_length(x) == sizeof(T*)) {
      // User passed a raw(<pointer size>) with the literal bytes of the
      // pointer.
      memcpy(&ptr_, RAW(x), sizeof(T*));
    } else if (TYPEOF(x) == REALSXP && Rf_length(x) == 1) {
      // User passed a double(1) of the static-casted pointer address.
      ptr_ = reinterpret_cast<T*>(static_cast<uintptr_t>(REAL(x)[0]));
    } else {
      cpp11::stop("Can't convert input object to pointer");
    }
  }

  inline operator SEXP() const { return R_MakeExternalPtr(ptr_, R_NilValue, R_NilValue); }

  inline operator T*() const { return ptr_; }

  inline void finalize() { delete ptr_; }

  T* ptr_;
};

// until cpp11 has a similar class
class complexs {
 public:
  using value_type = Rcomplex;

  explicit complexs(SEXP x) : data_(x) {}

  inline R_xlen_t size() const { return XLENGTH(data_); }

  inline operator SEXP() const { return data_; }

 private:
  cpp11::sexp data_;
};

// functions that need to be called from an unwind_protect()
namespace unsafe {

inline const char* utf8_string(SEXP s) {
  if (!IS_UTF8(s) && !IS_ASCII(s)) {
    return Rf_translateCharUTF8(s);
  } else {
    return CHAR(s);
  }
}

inline R_xlen_t r_string_size(SEXP s) {
  if (s == NA_STRING) {
    return 0;
  } else if (IS_ASCII(s) || IS_UTF8(s)) {
    return XLENGTH(s);
  } else {
    return strlen(Rf_translateCharUTF8(s));
  }
}

}  // namespace unsafe

inline SEXP utf8_strings(SEXP x) {
  return cpp11::unwind_protect([x] {
    R_xlen_t n = XLENGTH(x);

    // if `x` is an altrep of some sort, this will
    // materialize upfront. That's usually better because
    // the loop touches all strings
    const SEXP* p_x = STRING_PTR_RO(x);

    for (R_xlen_t i = 0; i < n; i++, ++p_x) {
      SEXP s = *p_x;
      if (s != NA_STRING && !IS_UTF8(s) && !IS_ASCII(s)) {
        SET_STRING_ELT(x, i, Rf_mkCharCE(Rf_translateCharUTF8(s), CE_UTF8));
      }
    }
    return x;
  });
}

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
  static SEXP arrow_attributes;
  static SEXP new_;
  static SEXP create;
  static SEXP arrow;
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
};

struct ns {
  static SEXP arrow;
};

template <typename Pointer>
Pointer r6_to_pointer(SEXP self) {
  if (!Rf_inherits(self, "ArrowObject")) {
    std::string type_name = arrow::util::nameof<
        cpp11::decay_t<typename std::remove_pointer<Pointer>::type>>();
    cpp11::stop("Invalid R object for %s, must be an ArrowObject", type_name.c_str());
  }

  SEXP xp = Rf_findVarInFrame(self, arrow::r::symbols::xp);
  if (xp == R_NilValue) {
    cpp11::stop("Invalid: self$`.:xp:.` is NULL");
  }

  void* p = R_ExternalPtrAddr(xp);
  if (p == nullptr) {
    SEXP klass = Rf_getAttrib(self, R_ClassSymbol);
    cpp11::stop("Invalid <%s>, external pointer to null", CHAR(STRING_ELT(klass, 0)));
  }
  return reinterpret_cast<Pointer>(p);
}

template <typename T>
void r6_reset_pointer(SEXP r6) {
  SEXP xp = Rf_findVarInFrame(r6, arrow::r::symbols::xp);
  void* p = R_ExternalPtrAddr(xp);
  if (p != nullptr) {
    delete reinterpret_cast<const std::shared_ptr<T>*>(p);
    R_SetExternalPtrAddr(xp, nullptr);
  }
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
  R_xlen_t n = x.size();
  Rvector out(n);
  for (R_xlen_t i = 0; i < n; i++) {
    out[i] = to_element(x[i]);
  }
  return out;
}

template <typename T, typename ToString>
cpp11::writable::strings to_r_strings(const std::vector<std::shared_ptr<T>>& x,
                                      ToString&& to_string) {
  return to_r_vector<cpp11::writable::strings>(x, std::forward<ToString>(to_string));
}

template <typename T, typename ToListElement>
cpp11::writable::list to_r_list(const std::vector<std::shared_ptr<T>>& x,
                                ToListElement&& to_element) {
  auto as_sexp = [&](const std::shared_ptr<T>& t) { return to_element(t); };
  return to_r_vector<cpp11::writable::list>(x, as_sexp);
}

template <typename T>
cpp11::writable::list to_r_list(const std::vector<std::shared_ptr<T>>& x);

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

bool GetBoolOption(const std::string& name, bool default_);

// A version of vctrs::vec_size() limited to the types that are
// supported at the C++ level. We currently handle record-style
// vectors (e.g., POSIXlt) at the R level such that by the time
// they get to C++ they are just a data.frame. This version also
// supports long vectors.
static inline R_xlen_t vec_size(SEXP x) {
  if (Rf_inherits(x, "data.frame")) {
    if (Rf_length(x) > 0) {
      return Rf_xlength(VECTOR_ELT(x, 0));
    } else {
      // This will expand the rownames if attr(x, "row.names") is ALTREP;
      // however, this is probably not an important performance consideration
      // since zero-column data.frames do not occur in many workflows.
      return Rf_xlength(Rf_getAttrib(x, R_RowNamesSymbol));
    }
  } else {
    return Rf_xlength(x);
  }
}

}  // namespace r
}  // namespace arrow

namespace cpp11 {

template <typename T>
SEXP to_r6(const std::shared_ptr<T>& ptr, const char* r6_class_name) {
  if (ptr == nullptr) return R_NilValue;

  cpp11::external_pointer<std::shared_ptr<T>> xp(new std::shared_ptr<T>(ptr));
  SEXP r6_class = Rf_install(r6_class_name);

  if (Rf_findVarInFrame3(arrow::r::ns::arrow, r6_class, FALSE) == R_UnboundValue) {
    cpp11::stop("No arrow R6 class named '%s'", r6_class_name);
  }

  // make call:  <symbol>$new(<x>)
  SEXP call = PROTECT(Rf_lang3(R_DollarSymbol, r6_class, arrow::r::symbols::new_));
  SEXP call2 = PROTECT(Rf_lang2(call, xp));

  // and then eval in arrow::
  SEXP r6 = PROTECT(Rf_eval(call2, arrow::r::ns::arrow));

  UNPROTECT(3);
  return r6;
}

/// This trait defines a single static function which returns the name of the R6 class
/// which corresponds to T. By default, this is just the c++ class name with any
/// namespaces stripped, for example the R6 class for arrow::ipc::RecordBatchStreamReader
/// is simply named "RecordBatchStreamReader".
///
/// Some classes require specializations of this trait. For example the R6 classes which
/// wrap arrow::csv::ReadOptions and arrow::json::ReadOptions would collide if both were
/// named "ReadOptions", so they are named "CsvReadOptions" and "JsonReadOptions"
/// respectively. Other classes such as arrow::Array are base classes and the proper R6
/// class name must be derived by examining a discriminant like Array::type_id.
///
/// All specializations are located in arrow_types.h
template <typename T>
struct r6_class_name;

template <typename T>
SEXP to_r6(const std::shared_ptr<T>& x) {
  if (x == nullptr) return R_NilValue;

  return to_r6(x, cpp11::r6_class_name<T>::get(x));
}

}  // namespace cpp11

namespace arrow {
namespace r {

template <typename T>
cpp11::writable::list to_r_list(const std::vector<std::shared_ptr<T>>& x) {
  auto as_sexp = [&](const std::shared_ptr<T>& t) { return cpp11::to_r6<T>(t); };
  return to_r_vector<cpp11::writable::list>(x, as_sexp);
}

}  // namespace r
}  // namespace arrow

struct r_vec_size {
  explicit r_vec_size(R_xlen_t x) : value(x) {}

  R_xlen_t value;
};

namespace cpp11 {

template <typename T>
using enable_if_shared_ptr = typename std::enable_if<
    std::is_same<std::shared_ptr<typename T::element_type>, T>::value, T>::type;

template <typename T>
enable_if_shared_ptr<T> as_cpp(SEXP from) {
  return arrow::r::ExternalPtrInput<T>(from);
}

template <typename E>
enable_if_enum<E, SEXP> as_sexp(E e) {
  return as_sexp(static_cast<int>(e));
}

template <typename T>
SEXP as_sexp(const std::shared_ptr<T>& ptr) {
  return cpp11::to_r6<T>(ptr);
}

inline SEXP as_sexp(r_vec_size size) {
  R_xlen_t x = size.value;
  if (x > std::numeric_limits<int>::max()) {
    return Rf_ScalarReal(x);
  } else {
    return Rf_ScalarInteger(x);
  }
}

}  // namespace cpp11

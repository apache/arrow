// cpp11 version: 0.2.1.9000
// vendored on: 2020-08-20
#pragma once

#include <algorithm>         // for min
#include <array>             // for array
#include <initializer_list>  // for initializer_list

#include "cpp11/R.hpp"                // for Rboolean, SEXP, SEXPREC, Rf_all...
#include "cpp11/attribute_proxy.hpp"  // for attribute_proxy
#include "cpp11/named_arg.hpp"        // for named_arg
#include "cpp11/protect.hpp"          // for protect_sexp, release_protect
#include "cpp11/r_vector.hpp"         // for r_vector, r_vector<>::proxy
#include "cpp11/sexp.hpp"             // for sexp

// Specializations for logicals

namespace cpp11 {

template <>
inline SEXP r_vector<Rboolean>::valid_type(SEXP data) {
  if (TYPEOF(data) != LGLSXP) {
    throw type_error(LGLSXP, TYPEOF(data));
  }
  return data;
}

template <>
inline Rboolean r_vector<Rboolean>::operator[](const R_xlen_t pos) const {
  return is_altrep_ ? static_cast<Rboolean>(LOGICAL_ELT(data_, pos)) : data_p_[pos];
}

template <>
inline Rboolean* r_vector<Rboolean>::get_p(bool is_altrep, SEXP data) {
  if (is_altrep) {
    return nullptr;
  } else {
    return reinterpret_cast<Rboolean*>(LOGICAL(data));
  }
}

template <>
inline void r_vector<Rboolean>::const_iterator::fill_buf(R_xlen_t pos) {
  length_ = std::min(64_xl, data_->size() - pos);
  LOGICAL_GET_REGION(data_->data_, pos, length_, reinterpret_cast<int*>(buf_.data()));
  block_start_ = pos;
}

typedef r_vector<Rboolean> logicals;

namespace writable {

template <>
inline typename r_vector<Rboolean>::proxy& r_vector<Rboolean>::proxy::operator=(
    const Rboolean& rhs) {
  if (is_altrep_) {
    SET_LOGICAL_ELT(data_, index_, rhs);
  } else {
    *p_ = rhs;
  }
  return *this;
}

template <>
inline r_vector<Rboolean>::proxy::operator Rboolean() const {
  if (p_ == nullptr) {
    return static_cast<Rboolean>(LOGICAL_ELT(data_, index_));
  } else {
    return *p_;
  }
}

template <>
inline r_vector<Rboolean>::r_vector(std::initializer_list<Rboolean> il)
    : cpp11::r_vector<Rboolean>(Rf_allocVector(LGLSXP, il.size())), capacity_(il.size()) {
  protect_ = protect_sexp(data_);
  auto it = il.begin();
  for (R_xlen_t i = 0; i < capacity_; ++i, ++it) {
    SET_LOGICAL_ELT(data_, i, *it);
  }
}

template <>
inline r_vector<Rboolean>::r_vector(std::initializer_list<named_arg> il)
    : cpp11::r_vector<Rboolean>(safe[Rf_allocVector](LGLSXP, il.size())),
      capacity_(il.size()) {
  protect_ = protect_sexp(data_);
  int n_protected = 0;

  try {
    unwind_protect([&] {
      Rf_setAttrib(data_, R_NamesSymbol, Rf_allocVector(STRSXP, capacity_));
      SEXP names = PROTECT(Rf_getAttrib(data_, R_NamesSymbol));
      ++n_protected;
      auto it = il.begin();
      for (R_xlen_t i = 0; i < capacity_; ++i, ++it) {
        data_p_[i] = static_cast<Rboolean>(LOGICAL_ELT(it->value(), 0));
        SET_STRING_ELT(names, i, Rf_mkCharCE(it->name(), CE_UTF8));
      }
      UNPROTECT(n_protected);
    });
  } catch (const unwind_exception& e) {
    release_protect(protect_);
    UNPROTECT(n_protected);
    throw e;
  }
}

template <>
inline void r_vector<Rboolean>::reserve(R_xlen_t new_capacity) {
  data_ = data_ == R_NilValue ? safe[Rf_allocVector](LGLSXP, new_capacity)
                              : safe[Rf_xlengthgets](data_, new_capacity);
  SEXP old_protect = protect_;
  protect_ = protect_sexp(data_);

  release_protect(old_protect);

  data_p_ = reinterpret_cast<Rboolean*>(LOGICAL(data_));
  capacity_ = new_capacity;
}

template <>
inline void r_vector<Rboolean>::push_back(Rboolean value) {
  while (length_ >= capacity_) {
    reserve(capacity_ == 0 ? 1 : capacity_ *= 2);
  }
  if (is_altrep_) {
    SET_LOGICAL_ELT(data_, length_, value);
  } else {
    data_p_[length_] = value;
  }
  ++length_;
}

typedef r_vector<Rboolean> logicals;

}  // namespace writable

inline bool is_na(Rboolean x) { return x == NA_LOGICAL; }
}  // namespace cpp11

// cpp11 version: 0.3.1.1
// vendored on: 2021-08-11
#pragma once

#include <algorithm>         // for min
#include <array>             // for array
#include <initializer_list>  // for initializer_list

#include "R_ext/Arith.h"        // for ISNA
#include "cpp11/R.hpp"          // for SEXP, SEXPREC, Rf_allocVector, REAL
#include "cpp11/as.hpp"         // for as_sexp
#include "cpp11/named_arg.hpp"  // for named_arg
#include "cpp11/protect.hpp"    // for SEXP, SEXPREC, REAL_ELT, R_Preserve...
#include "cpp11/r_vector.hpp"   // for vector, vector<>::proxy, vector<>::...
#include "cpp11/sexp.hpp"       // for sexp

// Specializations for doubles

namespace cpp11 {

template <>
inline SEXP r_vector<double>::valid_type(SEXP data) {
  if (TYPEOF(data) != REALSXP) {
    throw type_error(REALSXP, TYPEOF(data));
  }
  return data;
}

template <>
inline double r_vector<double>::operator[](const R_xlen_t pos) const {
  // NOPROTECT: likely too costly to unwind protect every elt
  return is_altrep_ ? REAL_ELT(data_, pos) : data_p_[pos];
}

template <>
inline double* r_vector<double>::get_p(bool is_altrep, SEXP data) {
  if (is_altrep) {
    return nullptr;
  } else {
    return REAL(data);
  }
}

template <>
inline void r_vector<double>::const_iterator::fill_buf(R_xlen_t pos) {
  length_ = std::min(64_xl, data_->size() - pos);
  REAL_GET_REGION(data_->data_, pos, length_, buf_.data());
  block_start_ = pos;
}

typedef r_vector<double> doubles;

namespace writable {

template <>
inline typename r_vector<double>::proxy& r_vector<double>::proxy::operator=(
    const double& rhs) {
  if (is_altrep_) {
    // NOPROTECT: likely too costly to unwind protect every set elt
    SET_REAL_ELT(data_, index_, rhs);
  } else {
    *p_ = rhs;
  }
  return *this;
}

template <>
inline r_vector<double>::proxy::operator double() const {
  if (p_ == nullptr) {
    // NOPROTECT: likely too costly to unwind protect every elt
    return REAL_ELT(data_, index_);
  } else {
    return *p_;
  }
}

template <>
inline r_vector<double>::r_vector(std::initializer_list<double> il)
    : cpp11::r_vector<double>(as_sexp(il)), capacity_(il.size()) {}

template <>
inline r_vector<double>::r_vector(std::initializer_list<named_arg> il)
    : cpp11::r_vector<double>(safe[Rf_allocVector](REALSXP, il.size())),
      capacity_(il.size()) {
  protect_ = preserved.insert(data_);
  int n_protected = 0;

  try {
    unwind_protect([&] {
      Rf_setAttrib(data_, R_NamesSymbol, Rf_allocVector(STRSXP, capacity_));
      SEXP names = PROTECT(Rf_getAttrib(data_, R_NamesSymbol));
      ++n_protected;
      auto it = il.begin();
      for (R_xlen_t i = 0; i < capacity_; ++i, ++it) {
        data_p_[i] = REAL_ELT(it->value(), 0);
        SET_STRING_ELT(names, i, Rf_mkCharCE(it->name(), CE_UTF8));
      }
      UNPROTECT(n_protected);
    });
  } catch (const unwind_exception& e) {
    preserved.release(protect_);
    UNPROTECT(n_protected);
    throw e;
  }
}

template <>
inline void r_vector<double>::reserve(R_xlen_t new_capacity) {
  data_ = data_ == R_NilValue ? safe[Rf_allocVector](REALSXP, new_capacity)
                              : safe[Rf_xlengthgets](data_, new_capacity);
  SEXP old_protect = protect_;
  protect_ = preserved.insert(data_);
  preserved.release(old_protect);

  data_p_ = REAL(data_);
  capacity_ = new_capacity;
}

template <>
inline void r_vector<double>::push_back(double value) {
  while (length_ >= capacity_) {
    reserve(capacity_ == 0 ? 1 : capacity_ *= 2);
  }
  if (is_altrep_) {
    SET_REAL_ELT(data_, length_, value);
  } else {
    data_p_[length_] = value;
  }
  ++length_;
}

typedef r_vector<double> doubles;

}  // namespace writable

template <>
inline double na() {
  return NA_REAL;
}

template <>
inline bool is_na(const double& x) {
  return ISNA(x);
}
}  // namespace cpp11

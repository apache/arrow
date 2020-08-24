// cpp11 version: 0.2.1.9000
// vendored on: 2020-08-20
#pragma once

#include <stddef.h>  // for size_t

#include <string>  // for string, basic_string

#include "cpp11/R.hpp"                // for SEXP, SEXPREC, REAL_ELT, R_NilV...
#include "cpp11/attribute_proxy.hpp"  // for attribute_proxy
#include "cpp11/protect.hpp"          // for protect_sexp, release_protect

namespace cpp11 {

/// Converting to SEXP
class sexp {
 private:
  SEXP data_ = R_NilValue;
  SEXP protect_ = R_NilValue;

 public:
  sexp() = default;
  sexp(SEXP data) : data_(data), protect_(protect_sexp(data_)) {
    // REprintf("created %x %x : %i\n", data_, protect_, protect_head_size());
  }
  sexp(const sexp& rhs) {
    data_ = rhs.data_;
    protect_ = protect_sexp(data_);
    // REprintf("copied %x new protect %x : %i\n", rhs.data_, protect_,
    // protect_head_size());
  }
  sexp(sexp&& rhs) {
    data_ = rhs.data_;
    protect_ = rhs.protect_;

    rhs.data_ = R_NilValue;
    rhs.protect_ = R_NilValue;

    // REprintf("moved %x : %i\n", rhs.data_, protect_head_size());
  }
  sexp& operator=(const sexp& rhs) {
    data_ = rhs.data_;
    protect_ = protect_sexp(data_);
    // REprintf("assigned %x : %i\n", rhs.data_, protect_head_size());
    return *this;
  }

  // void swap(sexp& rhs) {
  // sexp tmp(rhs);
  // rhs = *this;
  //*this = tmp;
  //}

  ~sexp() { release_protect(protect_); }

  attribute_proxy<sexp> attr(const char* name) const {
    return attribute_proxy<sexp>(*this, name);
  }

  attribute_proxy<sexp> attr(const std::string& name) const {
    return attribute_proxy<sexp>(*this, name.c_str());
  }

  attribute_proxy<sexp> attr(SEXP name) const {
    return attribute_proxy<sexp>(*this, name);
  }

  attribute_proxy<sexp> names() const {
    return attribute_proxy<sexp>(*this, R_NamesSymbol);
  }

  operator SEXP() const { return data_; }
  operator double() const { return REAL_ELT(data_, 0); }
  operator size_t() const { return REAL_ELT(data_, 0); }
  operator bool() const { return LOGICAL_ELT(data_, 0); }
  SEXP data() const { return data_; }
};

}  // namespace cpp11

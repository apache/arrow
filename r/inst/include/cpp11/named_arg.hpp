// cpp11 version: 0.2.1.9000
// vendored on: 2020-08-20
#pragma once

#include <stddef.h>  // for size_t

#include <initializer_list>  // for initializer_list

#include "cpp11/R.hpp"   // for SEXP, SEXPREC, literals
#include "cpp11/as.hpp"  // for as_sexp

namespace cpp11 {
class named_arg {
 public:
  constexpr explicit named_arg(const char* name) : name_(name), value_(nullptr) {}
  named_arg& operator=(std::initializer_list<int> il) {
    value_ = as_sexp(il);
    return *this;
  }

  template <typename T>
  named_arg& operator=(T rhs) {
    value_ = as_sexp(rhs);
    return *this;
  }

  template <typename T>
  named_arg& operator=(std::initializer_list<T> rhs) {
    value_ = as_sexp(rhs);
    return *this;
  }

  const char* name() const { return name_; }
  SEXP value() const { return value_; }

 private:
  const char* name_;
  SEXP value_;
};

namespace literals {

constexpr named_arg operator"" _nm(const char* name, std::size_t) {
  return named_arg(name);
}

}  // namespace literals

using namespace literals;

}  // namespace cpp11

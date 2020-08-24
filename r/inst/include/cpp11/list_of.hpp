// cpp11 version: 0.2.1.9000
// vendored on: 2020-08-20
#pragma once

#include <string>  // for string, basic_string

#include "cpp11/R.hpp"     // for R_xlen_t, SEXP, SEXPREC, LONG_VECTOR_SUPPORT
#include "cpp11/list.hpp"  // for list

namespace cpp11 {

template <typename T>
class list_of : public list {
 public:
  list_of(const list& data) : list(data) {}

#ifdef LONG_VECTOR_SUPPORT
  T operator[](int pos) { return operator[](static_cast<R_xlen_t>(pos)); }
#endif

  T operator[](R_xlen_t pos) { return list::operator[](pos); }

  T operator[](const char* pos) { return list::operator[](pos); }

  T operator[](const std::string& pos) { return list::operator[](pos.c_str()); }
};

namespace writable {
template <typename T>
class list_of : public writable::list {
 public:
  list_of(const list& data) : writable::list(data) {}
  list_of(R_xlen_t n) : writable::list(n) {}

#ifdef LONG_VECTOR_SUPPORT
  T operator[](int pos) { return operator[](static_cast<R_xlen_t>(pos)); }
#endif

  T operator[](R_xlen_t pos) {
    return static_cast<SEXP>(writable::list::operator[](pos));
  }

  T operator[](const char* pos) {
    return static_cast<SEXP>(writable::list::operator[](pos));
  }

  T operator[](const std::string& pos) {
    return static_cast<SEXP>(writable::list::operator[](pos.c_str()));
  }
};
}  // namespace writable

}  // namespace cpp11

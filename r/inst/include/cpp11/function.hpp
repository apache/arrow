// cpp11 version: 0.3.1.9000
// vendored on: 2021-08-11
#pragma once

#include <string.h>  // for strcmp

#include <cstdio>   // for snprintf
#include <string>   // for string, basic_string
#include <utility>  // for forward

#include "cpp11/R.hpp"          // for SEXP, SEXPREC, CDR, Rf_install, SETCAR
#include "cpp11/as.hpp"         // for as_sexp
#include "cpp11/named_arg.hpp"  // for named_arg
#include "cpp11/protect.hpp"    // for protect, protect::function, safe
#include "cpp11/sexp.hpp"       // for sexp

namespace cpp11 {

class function {
 public:
  function(SEXP data) : data_(data) {}

  template <typename... Args>
  sexp operator()(Args&&... args) const {
    // Size of the arguments plus one for the function name itself
    R_xlen_t num_args = sizeof...(args) + 1;

    sexp call(safe[Rf_allocVector](LANGSXP, num_args));

    construct_call(call, data_, std::forward<Args>(args)...);

    return safe[Rf_eval](call, R_GlobalEnv);
  }

 private:
  SEXP data_;

  template <typename... Args>
  SEXP construct_call(SEXP val, const named_arg& arg, Args&&... args) const {
    SETCAR(val, arg.value());
    SET_TAG(val, safe[Rf_install](arg.name()));
    val = CDR(val);
    return construct_call(val, std::forward<Args>(args)...);
  }

  // Construct the call recursively, each iteration adds an Arg to the pairlist.
  // We need
  template <typename T, typename... Args>
  SEXP construct_call(SEXP val, const T& arg, Args&&... args) const {
    SETCAR(val, as_sexp(arg));
    val = CDR(val);
    return construct_call(val, std::forward<Args>(args)...);
  }

  // Base case, just return
  SEXP construct_call(SEXP val) const { return val; }
};

class package {
 public:
  package(const char* name) : data_(get_namespace(name)) {}
  package(const std::string& name) : data_(get_namespace(name.c_str())) {}
  function operator[](const char* name) {
    return safe[Rf_findFun](safe[Rf_install](name), data_);
  }
  function operator[](const std::string& name) { return operator[](name.c_str()); }

 private:
  static SEXP get_namespace(const char* name) {
    if (strcmp(name, "base") == 0) {
      return R_BaseEnv;
    }
    sexp name_sexp = safe[Rf_install](name);
    return safe[Rf_findVarInFrame](R_NamespaceRegistry, name_sexp);
  }

  SEXP data_;
};

#ifdef CPP11_USE_FMT
template <typename... Args>
void message(const char* fmt_arg, Args... args) {
  static auto R_message = cpp11::package("base")["message"];
  std::string msg = fmt::format(fmt_arg, args...);
  R_message(msg.c_str());
}

template <typename... Args>
void message(const std::string& fmt_arg, Args... args) {
  static auto R_message = cpp11::package("base")["message"];
  std::string msg = fmt::format(fmt_arg, args...);
  R_message(msg.c_str());
}
#else
template <typename... Args>
void message(const char* fmt_arg, Args... args) {
  static auto R_message = cpp11::package("base")["message"];
  char buff[1024];
  int msg = std::snprintf(buff, 1024, fmt_arg, args...);
  if (msg >= 0 && msg < 1024) {
    R_message(buff);
  }
}

template <typename... Args>
void message(const std::string& fmt_arg, Args... args) {
  static auto R_message = cpp11::package("base")["message"];
  char buff[1024];
  int msg = std::snprintf(buff, 1024, fmt_arg.c_str(), args...);
  if (msg >= 0 && msg < 1024) {
    R_message(buff);
  }
}
#endif

}  // namespace cpp11

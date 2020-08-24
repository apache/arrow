// cpp11 version: 0.2.1.9000
// vendored on: 2020-08-20
#pragma once

#include <csetjmp>    // for longjmp, setjmp, jmp_buf
#include <exception>  // for exception
#include <stdexcept>  // for std::runtime_error
#include <string>     // for string, basic_string
#include <tuple>      // for tuple, make_tuple

// NB: cpp11/R.hpp must precede R_ext includes so our definition of Rboolean is used
#include "cpp11/R.hpp"  // for SEXP, SEXPREC, CDR, R_NilValue, CAR, R_Pres...

#include "R_ext/Error.h"  // for Rf_error, Rf_warning
#include "R_ext/Print.h"  // for REprintf
#include "R_ext/Utils.h"  // for R_CheckUserInterrupt
#include "Rversion.h"     // for R_VERSION, R_Version

#if defined(R_VERSION) && R_VERSION >= R_Version(3, 5, 0)
#define HAS_UNWIND_PROTECT
#endif

namespace cpp11 {
class unwind_exception : public std::exception {
 public:
  SEXP token;
  unwind_exception(SEXP token_) : token(token_) {}
};

static SEXP preserve(SEXP obj) {
  PROTECT(obj);
  R_PreserveObject(obj);
  UNPROTECT(1);
  return obj;
}

static SEXP protect_list = preserve(Rf_cons(R_NilValue, R_NilValue));

inline SEXP protect_sexp(SEXP obj) {
  if (obj == R_NilValue) {
    return R_NilValue;
  }
#ifdef CPP11_USE_PRESERVE_OBJECT
  R_PreserveObject(obj);
  return obj;
#endif
  PROTECT(obj);

  // Add a new cell that points to the previous end.
  SEXP cell = PROTECT(Rf_cons(protect_list, CDR(protect_list)));
  SET_TAG(cell, obj);

  SETCDR(protect_list, cell);
  if (CDR(cell) != R_NilValue) {
    SETCAR(CDR(cell), cell);
  }

  UNPROTECT(2);

  return cell;
}

inline void print_protect() {
  SEXP head = protect_list;
  while (head != R_NilValue) {
    REprintf("%x CAR: %x CDR: %x TAG: %x\n", head, CAR(head), CDR(head), TAG(head));
    head = CDR(head);
  }
  REprintf("---\n");
}

/* This is currently unused, but client packages could use it to free leaked resources in
 * older R versions if needed */
inline void release_existing_protections() {
#if !defined(CPP11_USE_PRESERVE_OBJECT)
  SEXP first = CDR(protect_list);
  if (first != R_NilValue) {
    SETCAR(first, R_NilValue);
    SETCDR(protect_list, R_NilValue);
  }
#endif
}

inline void release_protect(SEXP protect) {
  if (protect == R_NilValue) {
    return;
  }
#ifdef CPP11_USE_PRESERVE_OBJECT
  R_ReleaseObject(protect);
  return;
#endif

  SEXP before = CAR(protect);
  SEXP after = CDR(protect);

  if (before == R_NilValue && after == R_NilValue) {
    Rf_error("should never happen");
  }

  SETCDR(before, after);
  if (after != R_NilValue) {
    SETCAR(after, before);
  }
}

#ifdef HAS_UNWIND_PROTECT

/// Unwind Protection from C longjmp's, like those used in R error handling
///
/// @param code The code to which needs to be protected, as a nullary callable
template <typename Fun, typename = typename std::enable_if<std::is_same<
                            decltype(std::declval<Fun&&>()()), SEXP>::value>::type>
SEXP unwind_protect(Fun&& code) {
  static SEXP token = [] {
    SEXP res = R_MakeUnwindCont();
    R_PreserveObject(res);
    return res;
  }();

  std::jmp_buf jmpbuf;
  if (setjmp(jmpbuf)) {
    throw unwind_exception(token);
  }

  return R_UnwindProtect(
      [](void* data) -> SEXP {
        auto callback = static_cast<decltype(&code)>(data);
        return static_cast<Fun&&>(*callback)();
      },
      &code,
      [](void* jmpbuf, Rboolean jump) {
        if (jump) {
          // We need to first jump back into the C++ stacks because you can't safely throw
          // exceptions from C stack frames.
          longjmp(*static_cast<std::jmp_buf*>(jmpbuf), 1);
        }
      },
      &jmpbuf, token);
}

template <typename Fun, typename = typename std::enable_if<std::is_same<
                            decltype(std::declval<Fun&&>()()), void>::value>::type>
void unwind_protect(Fun&& code) {
  (void)unwind_protect([&] {
    std::forward<Fun>(code)();
    return R_NilValue;
  });
}

template <typename Fun, typename R = decltype(std::declval<Fun&&>()())>
typename std::enable_if<!std::is_same<R, SEXP>::value && !std::is_same<R, void>::value,
                        R>::type
unwind_protect(Fun&& code) {
  R out;
  (void)unwind_protect([&] {
    out = std::forward<Fun>(code)();
    return R_NilValue;
  });
  return out;
}

#else
// Don't do anything if we don't have unwind protect. This will leak C++ resources,
// including those held by cpp11 objects, but the other alternatives are also not great.
template <typename Fun>
decltype(std::declval<Fun&&>()()) unwind_protect(Fun&& code) {
  return std::forward<Fun>(code)();
}
#endif

namespace detail {

template <size_t...>
struct index_sequence {
  using type = index_sequence;
};

template <typename, size_t>
struct appended_sequence;

template <std::size_t... I, std::size_t J>
struct appended_sequence<index_sequence<I...>, J> : index_sequence<I..., J> {};

template <size_t N>
struct make_index_sequence
    : appended_sequence<typename make_index_sequence<N - 1>::type, N - 1> {};

template <>
struct make_index_sequence<0> : index_sequence<> {};

template <typename F, typename... Aref, size_t... I>
decltype(std::declval<F&&>()(std::declval<Aref>()...)) apply(
    F&& f, std::tuple<Aref...>&& a, const index_sequence<I...>&) {
  return std::forward<F>(f)(std::get<I>(std::move(a))...);
}

template <typename F, typename... Aref>
decltype(std::declval<F&&>()(std::declval<Aref>()...)) apply(F&& f,
                                                             std::tuple<Aref...>&& a) {
  return apply(std::forward<F>(f), std::move(a), make_index_sequence<sizeof...(Aref)>{});
}

// overload to silence a compiler warning that the (empty) tuple parameter is set but
// unused
template <typename F>
decltype(std::declval<F&&>()()) apply(F&& f, std::tuple<>&&) {
  return std::forward<F>(f)();
}

template <typename F, typename... Aref>
struct closure {
  decltype(std::declval<F*>()(std::declval<Aref>()...)) operator()() && {
    return apply(ptr_, std::move(arefs_));
  }
  F* ptr_;
  std::tuple<Aref...> arefs_;
};

}  // namespace detail

struct protect {
  template <typename F>
  struct function {
    template <typename... A>
    decltype(std::declval<F*>()(std::declval<A&&>()...)) operator()(A&&... a) const {
      // workaround to support gcc4.8, which can't capture a parameter pack
      return unwind_protect(
          detail::closure<F, A&&...>{ptr_, std::forward_as_tuple(std::forward<A>(a)...)});
    }

    F* ptr_;
  };

  /// May not be applied to a function bearing attributes, which interfere with linkage on
  /// some compilers; use an appropriately attributed alternative. (For example, Rf_error
  /// bears the [[noreturn]] attribute and must be protected with safe.noreturn rather
  /// than safe.operator[]).
  template <typename F>
  constexpr function<F> operator[](F* raw) const {
    return {raw};
  }

  template <typename F>
  struct noreturn_function {
    template <typename... A>
    void operator() [[noreturn]] (A&&... a) const {
      // workaround to support gcc4.8, which can't capture a parameter pack
      unwind_protect(
          detail::closure<F, A&&...>{ptr_, std::forward_as_tuple(std::forward<A>(a)...)});
      // Compiler hint to allow [[noreturn]] attribute; this is never executed since
      // the above call will not return.
      throw std::runtime_error("[[noreturn]]");
    }
    F* ptr_;
  };

  template <typename F>
  constexpr noreturn_function<F> noreturn(F* raw) const {
    return {raw};
  }
};
constexpr struct protect safe = {};

inline void check_user_interrupt() { safe[R_CheckUserInterrupt](); }

template <typename... Args>
void stop [[noreturn]] (const char* fmt, Args... args) {
  safe.noreturn(Rf_error)(fmt, args...);
}

template <typename... Args>
void stop [[noreturn]] (const std::string& fmt, Args... args) {
  safe.noreturn(Rf_error)(fmt.c_str(), args...);
}

template <typename... Args>
void warning(const char* fmt, Args... args) {
  safe[Rf_warning](fmt, args...);
}

template <typename... Args>
void warning(const std::string& fmt, Args... args) {
  safe[Rf_warning](fmt.c_str(), args...);
}

}  // namespace cpp11

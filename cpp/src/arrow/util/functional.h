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

#pragma once

#include <memory>
#include <tuple>
#include <type_traits>

#include "arrow/type_traits.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

/// Helper struct for examining lambdas and other callables.
/// TODO(bkietz) support function pointers
struct call_traits {
 public:
  template <typename R, typename... A>
  static std::false_type is_overloaded_impl(R(A...));

  template <typename F>
  static std::false_type is_overloaded_impl(decltype(&F::operator())*);

  template <typename F>
  static std::true_type is_overloaded_impl(...);

  template <typename F, typename R, typename... A>
  static R return_type_impl(R (F::*)(A...));

  template <typename F, typename R, typename... A>
  static R return_type_impl(R (F::*)(A...) const);

  template <std::size_t I, typename F, typename R, typename... A>
  static typename std::tuple_element<I, std::tuple<A...>>::type argument_type_impl(
      R (F::*)(A...));

  template <std::size_t I, typename F, typename R, typename... A>
  static typename std::tuple_element<I, std::tuple<A...>>::type argument_type_impl(
      R (F::*)(A...) const);

  template <std::size_t I, typename F, typename R, typename... A>
  static typename std::tuple_element<I, std::tuple<A...>>::type argument_type_impl(
      R (F::*)(A...) &&);

  /// bool constant indicating whether F is a callable with more than one possible
  /// signature. Will be true_type for objects which define multiple operator() or which
  /// define a template operator()
  template <typename F>
  using is_overloaded =
      decltype(is_overloaded_impl<typename std::decay<F>::type>(NULLPTR));

  template <typename F, typename T = void>
  using enable_if_overloaded = typename std::enable_if<is_overloaded<F>::value, T>::type;

  template <typename F, typename T = void>
  using disable_if_overloaded =
      typename std::enable_if<!is_overloaded<F>::value, T>::type;

  /// If F is not overloaded, the argument types of its call operator can be
  /// extracted via call_traits::argument_type<Index, F>
  template <std::size_t I, typename F>
  using argument_type = decltype(argument_type_impl<I>(&std::decay<F>::type::operator()));

  template <typename F>
  using return_type = decltype(return_type_impl(&std::decay<F>::type::operator()));

  template <typename F, typename T, typename RT = T>
  using enable_if_return =
      typename std::enable_if<std::is_same<return_type<F>, T>::value, RT>;
};

/// A type erased callable object which may only be invoked once.
/// It can be constructed from any lambda which matches the provided call signature.
/// Invoking it results in destruction of the lambda, freeing any state/references
/// immediately. Invoking a default constructed FnOnce or one which has already been
/// invoked will segfault.
template <typename Signature>
class FnOnce;

template <typename R, typename... A>
class FnOnce<R(A...)> {
 public:
  FnOnce() = default;

  template <typename Fn,
            typename = typename std::enable_if<std::is_convertible<
                typename std::result_of<Fn && (A...)>::type, R>::value>::type>
  FnOnce(Fn fn) : impl_(new FnImpl<Fn>(std::move(fn))) {  // NOLINT runtime/explicit
  }

  explicit operator bool() const { return impl_ != NULLPTR; }

  R operator()(A... a) && {
    auto bye = std::move(impl_);
    return bye->invoke(std::forward<A&&>(a)...);
  }

 private:
  struct Impl {
    virtual ~Impl() = default;
    virtual R invoke(A&&... a) = 0;
  };

  template <typename Fn>
  struct FnImpl : Impl {
    explicit FnImpl(Fn fn) : fn_(std::move(fn)) {}
    R invoke(A&&... a) override { return std::move(fn_)(std::forward<A&&>(a)...); }
    Fn fn_;
  };

  std::unique_ptr<Impl> impl_;
};

// // By default std::function will make a copy of whatever it is wrapping.  However, some
// // callables might be move-only.  This extension allows you to create a std::function
// from
// // a move-only target.  This function will then own the target.
// //
// // TODO: I got this from
// // https://stackoverflow.com/questions/25330716/move-only-version-of-stdfunction what
// // needs to be done to use it?  Any kind of citing or attribution?
// template <typename T>
// class unique_function : public std::function<T> {
//   template <typename Fn, typename En = void>
//   struct wrapper;

//   // specialization for CopyConstructible Fn
//   template <typename Fn>
//   struct wrapper<Fn, enable_if_t<std::is_copy_constructible<Fn>::value>> {
//     Fn fn;

//     template <typename Res, typename... Args>
//     Res operator()(Args&&... args) {
//       return fn(std::forward<Args>(args)...);
//     }
//   };

//   // specialization for MoveConstructible-only Fn
//   template <typename Fn>
//   struct wrapper<Fn, enable_if_t<!std::is_copy_constructible<Fn>::value &&
//                                  std::is_move_constructible<Fn>::value>> {
//     Fn fn;

//     wrapper(Fn&& fn) : fn(std::forward<Fn>(fn)) {}

//     ARROW_DEFAULT_MOVE_AND_ASSIGN(wrapper);
//     //    ARROW_DISALLOW_COPY_AND_ASSIGN(wrapper);

//     // TODO: It seems safer to delete these (done above).  Why didn't SO do that?
//     // these two functions are instantiated by std::function
//     // and are never called
//     wrapper(const wrapper& rhs) : fn(const_cast<Fn&&>(rhs.fn)) {
//       throw 0;
//     }  // hack to initialize fn for non-DefaultContructible types
//     wrapper& operator=(wrapper&) { throw 0; }

//     template <typename Res, typename... Args>
//     Res operator()(Args&&... args) {
//       return fn(std::forward<Args>(args)...);
//     }
//   };

//   using base = std::function<T>;

//  public:
//   unique_function() noexcept = default;
//   unique_function(std::nullptr_t) noexcept : base(nullptr) {}

//   template <typename Fn>
//   unique_function(Fn&& f) : base(wrapper<Fn>{std::forward<Fn>(f)}) {}

//   ARROW_DEFAULT_MOVE_AND_ASSIGN(unique_function);
//   ARROW_DISALLOW_COPY_AND_ASSIGN(unique_function);

//   unique_function& operator=(std::nullptr_t) {
//     base::operator=(nullptr);
//     return *this;
//   }

//   template <typename Fn>
//   unique_function& operator=(Fn&& f) {
//     base::operator=(wrapper<Fn>{std::forward<Fn>(f)});
//     return *this;
//   }

//   using base::operator();
// };

}  // namespace internal
}  // namespace arrow

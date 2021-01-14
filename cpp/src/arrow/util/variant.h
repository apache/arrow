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

#include <cstddef>
#include <exception>
#include <type_traits>
#include <utility>

#include "arrow/util/macros.h"
#include "arrow/util/type_traits.h"

namespace arrow {
namespace util {

/// \brief a std::variant-like discriminated union
///
/// Simplifications from std::variant:
///
/// - Strictly defaultable. The first type of T... should be nothrow default constructible
///   and it will be used for default Variants.
///
/// - Never valueless_by_exception. std::variant supports a state outside those specified
///   by T... to which it can return in the event that a constructor throws. If a Variant
///   would become valueless_by_exception it will instead return to its default state.
///
/// - Strictly nothrow move constructible and assignable
///
/// - Less sophisticated type deduction. std::variant<bool, std::string>("hello") will
///   intelligently construct std::string while Variant<bool, std::string>("hello") will
///   construct bool.
///
/// - Either both copy constructible and assignable or neither (std::variant independently
///   enables copy construction and copy assignment). Variant is copy constructible if
///   each of T... is copy constructible and assignable.
///
/// - Slimmer interface; several members of std::variant are omitted.
///
/// - Throws no exceptions; if a bad_variant_access would be thrown Variant will instead
///   segfault (nullptr dereference).
///
/// - Mutable visit takes a pointer instead of mutable reference or rvalue reference,
///   which is more conformant with our code style.
template <typename... T>
class Variant;

namespace detail {

template <typename T, typename = void>
struct is_equality_comparable : std::false_type {};

template <typename T>
struct is_equality_comparable<
    T, typename std::enable_if<std::is_convertible<
           decltype(std::declval<T>() == std::declval<T>()), bool>::value>::type>
    : std::true_type {};

template <bool C, typename T, typename E>
using conditional_t = typename std::conditional<C, T, E>::type;

template <typename T>
struct type_constant {
  using type = T;
};

template <typename...>
struct first;

template <typename H, typename... T>
struct first<H, T...> {
  using type = H;
};

template <typename T>
using decay_t = typename std::decay<T>::type;

template <bool...>
struct all : std::true_type {};

template <bool H, bool... T>
struct all<H, T...> : conditional_t<H, all<T...>, std::false_type> {};

struct delete_copy_constructor {
  template <typename>
  struct type {
    type() = default;
    type(const type& other) = delete;
    type& operator=(const type& other) = delete;
  };
};

struct explicit_copy_constructor {
  template <typename Copyable>
  struct type {
    type() = default;
    type(const type& other) { static_cast<const Copyable&>(other).copy_to(this); }
    type& operator=(const type& other) {
      static_cast<Copyable*>(this)->destroy();
      static_cast<const Copyable&>(other).copy_to(this);
      return *this;
    }
  };
};

template <typename... T>
struct VariantStorage {
  VariantStorage() = default;
  VariantStorage(const VariantStorage&) {}
  VariantStorage& operator=(const VariantStorage&) { return *this; }
  VariantStorage(VariantStorage&&) noexcept {}
  VariantStorage& operator=(VariantStorage&&) noexcept { return *this; }
  ~VariantStorage() {
    static_assert(offsetof(VariantStorage, data_) == 0,
                  "(void*)&VariantStorage::data_ == (void*)this");
  }

  typename arrow::internal::aligned_union<0, T...>::type data_;
  uint8_t index_ = 0;
};

template <typename V, typename...>
struct VariantImpl;

template <typename... T>
struct VariantImpl<Variant<T...>> : VariantStorage<T...> {
  static void index_of() noexcept {}
  void destroy() noexcept {}
  void move_to(...) noexcept {}
  void copy_to(...) const {}

  template <typename R, typename Visitor>
  [[noreturn]] R visit_const(Visitor&& visitor) const {
    std::terminate();
  }
  template <typename R, typename Visitor>
  [[noreturn]] R visit_mutable(Visitor&& visitor) {
    std::terminate();
  }
};

template <typename... M, typename H, typename... T>
struct VariantImpl<Variant<M...>, H, T...> : VariantImpl<Variant<M...>, T...> {
  using VariantType = Variant<M...>;
  using Impl = VariantImpl<VariantType, T...>;

  static constexpr uint8_t kIndex = sizeof...(M) - sizeof...(T) - 1;

  VariantImpl() = default;

  using VariantImpl<VariantType, T...>::VariantImpl;
  using Impl::operator=;
  using Impl::index_of;

  explicit VariantImpl(H value) {
    new (this) H(std::move(value));
    this->index_ = kIndex;
  }

  VariantImpl& operator=(H value) {
    static_cast<VariantType*>(this)->destroy();
    new (this) H(std::move(value));
    this->index_ = kIndex;
    return *this;
  }

  H& cast_this() { return *reinterpret_cast<H*>(this); }
  const H& cast_this() const { return *reinterpret_cast<const H*>(this); }

  void move_to(VariantType* target) noexcept {
    if (this->index_ == kIndex) {
      new (target) H(std::move(cast_this()));
      target->index_ = kIndex;
    } else {
      Impl::move_to(target);
    }
  }

  // Templated to avoid instantiation in case H is not copy constructible
  template <typename Void>
  void copy_to(Void* generic_target) const {
    const auto target = static_cast<VariantType*>(generic_target);
    try {
      if (this->index_ == kIndex) {
        new (target) H(cast_this());
        target->index_ = kIndex;
      } else {
        Impl::copy_to(target);
      }
    } catch (...) {
      target->construct_default();
      throw;
    }
  }

  void destroy() noexcept {
    if (this->index_ == kIndex) {
      if (!std::is_trivially_destructible<H>::value) {
        cast_this().~H();
      }
    } else {
      Impl::destroy();
    }
  }

  static constexpr std::integral_constant<uint8_t, kIndex> index_of(
      const type_constant<H>&) {
    return {};
  }

  template <typename R, typename Visitor>
  R visit_const(Visitor&& visitor) const {
    if (this->index_ == kIndex) {
      return std::forward<Visitor>(visitor)(cast_this());
    }
    return Impl::template visit_const<R>(std::forward<Visitor>(visitor));
  }

  template <typename R, typename Visitor>
  R visit_mutable(Visitor&& visitor) {
    if (this->index_ == kIndex) {
      return std::forward<Visitor>(visitor)(&cast_this());
    }
    return Impl::template visit_mutable<R>(std::forward<Visitor>(visitor));
  }
};

}  // namespace detail

template <typename... T>
class Variant : detail::VariantImpl<Variant<T...>, T...>,
                detail::conditional_t<
                    detail::all<(std::is_copy_constructible<T>::value &&
                                 std::is_copy_assignable<T>::value)...>::value,
                    detail::explicit_copy_constructor,
                    detail::delete_copy_constructor>::template type<Variant<T...>> {
  template <typename U>
  static constexpr uint8_t index_of() {
    return Impl::index_of(detail::type_constant<U>{});
  }

  using Impl = detail::VariantImpl<Variant<T...>, T...>;

 public:
  using default_type = typename util::detail::first<T...>::type;

  Variant() noexcept { construct_default(); }

  Variant(const Variant& other) = default;
  Variant& operator=(const Variant& other) = default;

  using Impl::Impl;
  using Impl::operator=;

  Variant(Variant&& other) noexcept { other.move_to(this); }

  Variant& operator=(Variant&& other) noexcept {
    this->destroy();
    other.move_to(this);
    return *this;
  }

  ~Variant() {
    static_assert(offsetof(Variant, data_) == 0, "(void*)&Variant::data_ == (void*)this");
    this->destroy();
  }

  /// \brief Return the zero-based type index of the value held by the variant
  uint8_t index() const noexcept { return this->index_; }

  /// \brief Get a const pointer to the value held by the variant
  ///
  /// If the type given as template argument doesn't match, a null pointer is returned.
  template <typename U, uint8_t I = index_of<U>()>
  const U* get() const noexcept {
    return index() == I ? reinterpret_cast<const U*>(this) : NULLPTR;
  }

  /// \brief Get a pointer to the value held by the variant
  ///
  /// If the type given as template argument doesn't match, a null pointer is returned.
  template <typename U, uint8_t I = index_of<U>()>
  U* get() noexcept {
    return index() == I ? reinterpret_cast<U*>(this) : NULLPTR;
  }

  /// \brief Replace the value held by the variant
  ///
  /// The intended type must be given as a template argument.
  /// The value is constructed in-place using the given function arguments.
  template <typename U, typename... A, uint8_t I = index_of<U>()>
  void emplace(A&&... args) try {
    this->destroy();
    new (this) U(std::forward<A>(args)...);
    this->index_ = I;
  } catch (...) {
    construct_default();
    throw;
  }

  template <typename U, typename E, typename... A, uint8_t I = index_of<U>()>
  void emplace(std::initializer_list<E> il, A&&... args) try {
    this->destroy();
    new (this) U(il, std::forward<A>(args)...);
    this->index_ = I;
  } catch (...) {
    construct_default();
    throw;
  }

  /// \brief Swap with another variant's contents
  void swap(Variant& other) noexcept {  // NOLINT google-runtime-references
    Variant tmp = std::move(other);
    other = std::move(*this);
    *this = std::move(tmp);
  }

  using Impl::visit_const;
  using Impl::visit_mutable;

 private:
  void construct_default() noexcept {
    new (this) default_type();
    this->index_ = 0;
  }

  template <typename V>
  friend struct detail::explicit_copy_constructor::type;

  template <typename V, typename...>
  friend struct detail::VariantImpl;
};

/// \brief Call polymorphic visitor on a const variant's value
///
/// The visitor will receive a const reference to the value held by the variant.
/// It must define overloads for each possible variant type.
/// The overloads should all return the same type (no attempt
/// is made to find a generalized return type).
template <typename Visitor, typename... T,
          typename R = decltype(std::declval<Visitor&&>()(
              std::declval<const typename Variant<T...>::default_type&>()))>
R visit(Visitor&& visitor, const util::Variant<T...>& v) {
  return v.template visit_const<R>(std::forward<Visitor>(visitor));
}

/// \brief Call polymorphic visitor on a non-const variant's value
///
/// The visitor will receive a pointer to the value held by the variant.
/// It must define overloads for each possible variant type.
/// The overloads should all return the same type (no attempt
/// is made to find a generalized return type).
template <typename Visitor, typename... T,
          typename R = decltype(std::declval<Visitor&&>()(
              std::declval<typename Variant<T...>::default_type*>()))>
R visit(Visitor&& visitor, util::Variant<T...>* v) {
  return v->template visit_mutable<R>(std::forward<Visitor>(visitor));
}

/// \brief Get a const reference to the value held by the variant
///
/// If the type given as template argument doesn't match, behavior is undefined
/// (a null pointer will be dereferenced).
template <typename U, typename... T>
const U& get(const Variant<T...>& v) {
  return *v.template get<U>();
}

/// \brief Get a reference to the value held by the variant
///
/// If the type given as template argument doesn't match, behavior is undefined
/// (a null pointer will be dereferenced).
template <typename U, typename... T>
U& get(Variant<T...>& v) {
  return *v.template get<U>();
}

/// \brief Get a const pointer to the value held by the variant
///
/// If the type given as template argument doesn't match, a nullptr is returned.
template <typename U, typename... T>
const U* get_if(const Variant<T...>* v) {
  return v->template get<U>();
}

/// \brief Get a pointer to the value held by the variant
///
/// If the type given as template argument doesn't match, a nullptr is returned.
template <typename U, typename... T>
U* get_if(Variant<T...>* v) {
  return v->template get<U>();
}

namespace detail {

template <typename... T>
struct VariantsEqual {
  template <typename U>
  bool operator()(const U& r) const {
    return get<U>(l_) == r;
  }
  const Variant<T...>& l_;
};

}  // namespace detail

template <typename... T, typename = typename std::enable_if<detail::all<
                             detail::is_equality_comparable<T>::value...>::value>>
bool operator==(const Variant<T...>& l, const Variant<T...>& r) {
  if (l.index() != r.index()) return false;
  return visit(detail::VariantsEqual<T...>{l}, r);
}

template <typename... T>
auto operator!=(const Variant<T...>& l, const Variant<T...>& r) -> decltype(l == r) {
  return !(l == r);
}

/// \brief Return whether the variant holds a value of the given type
template <typename U, typename... T>
bool holds_alternative(const Variant<T...>& v) {
  return v.template get<U>();
}

}  // namespace util
}  // namespace arrow

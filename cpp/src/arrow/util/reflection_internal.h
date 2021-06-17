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

#include <array>
#include <string>
#include <tuple>
#include <utility>

namespace arrow {
namespace internal {

namespace detail {

#ifdef _MSC_VER
#define ARROW_PRETTY_FUNCTION __FUNCSIG__
#else
#define ARROW_PRETTY_FUNCTION __PRETTY_FUNCTION__
#endif

template <typename T>
const char* raw() {
  return ARROW_PRETTY_FUNCTION;
}

template <typename T>
size_t raw_sizeof() {
  return sizeof(ARROW_PRETTY_FUNCTION);
}

#undef ARROW_PRETTY_FUNCTION

constexpr bool starts_with(char const* haystack, char const* needle) {
  return needle[0] == '\0' ||
         (haystack[0] == needle[0] && starts_with(haystack + 1, needle + 1));
}

constexpr size_t search(char const* haystack, char const* needle) {
  return haystack[0] == '\0' || starts_with(haystack, needle)
             ? 0
             : search(haystack + 1, needle) + 1;
}

const size_t typename_prefix = search(raw<double>(), "double");

template <typename T>
size_t struct_class_prefix() {
#ifdef _MSC_VER
  return starts_with(raw<T>() + typename_prefix, "struct ")
             ? 7
             : starts_with(raw<T>() + typename_prefix, "class ") ? 6 : 0;
#else
  return 0;
#endif
}

template <typename T>
size_t typename_length() {
  // raw_sizeof<T>() - raw_sizeof<double>() ==
  //     (length of T's name) - strlen("double")
  // (length of T's name) ==
  //     raw_sizeof<T>() - raw_sizeof<double>() + strlen("double")
  return raw_sizeof<T>() - struct_class_prefix<T>() - raw_sizeof<double>() + 6;
}

template <typename T>
const char* typename_begin() {
  return raw<T>() + struct_class_prefix<T>() + typename_prefix;
}

template <class Class, typename Type, Type Class::*Ptr>
struct member_pointer_constant {
  static constexpr auto value = Ptr;
};

struct HasCrib {
  double crib;
  using crib_constant = member_pointer_constant<HasCrib, double, &HasCrib::crib>;
};

const size_t membername_boilerplate = search(raw<HasCrib::crib_constant>(), "crib") -
                                      typename_length<HasCrib>() * 2 -
                                      typename_length<double>();

template <class Class, typename Type, Type Class::*Ptr>
size_t membername_prefix() {
  return membername_boilerplate + typename_length<Class>() * 2 + typename_length<Type>();
}

const size_t membername_suffix = raw_sizeof<HasCrib::crib_constant>() -
                                 membername_prefix<HasCrib, double, &HasCrib::crib>() -
                                 4;  //  == strlen("crib")

template <class Class, typename Type, Type Class::*Ptr>
const char* membername_begin() {
  return raw<member_pointer_constant<Class, Type, Ptr>>() +
         membername_prefix<Class, Type, Ptr>();
}

template <class Class, typename Type, Type Class::*Ptr>
size_t membername_length() {
  return raw_sizeof<member_pointer_constant<Class, Type, Ptr>>() -
         membername_prefix<Class, Type, Ptr>() - membername_suffix;
}

}  // namespace detail

template <typename T>
std::string nameof(bool strip_namespace = false) {
  std::string name{detail::typename_begin<T>(), detail::typename_length<T>()};
  if (strip_namespace) {
    auto i = name.find_last_of("::");
    if (i != std::string::npos) {
      name = name.substr(i + 1);
    }
  }
  return name;
}

template <size_t...>
struct index_sequence {};

template <size_t N, size_t Head = N, size_t... Tail>
struct make_index_sequence_impl;

template <size_t N>
using make_index_sequence = typename make_index_sequence_impl<N>::type;

template <typename... T>
using index_sequence_for = make_index_sequence<sizeof...(T)>;

template <size_t N, size_t... I>
struct make_index_sequence_impl<N, 0, I...> {
  using type = index_sequence<I...>;
};

template <size_t N, size_t H, size_t... I>
struct make_index_sequence_impl : make_index_sequence_impl<N, H - 1, H - 1, I...> {};

static_assert(std::is_base_of<index_sequence<>, make_index_sequence<0>>::value, "");
static_assert(std::is_base_of<index_sequence<0, 1, 2>, make_index_sequence<3>>::value,
              "");

template <typename Props, typename Fn, size_t... I>
void ForEachPropertyImpl(Fn&& fn, const index_sequence<I...>&) {
  struct {
  } dummy;
  std::make_tuple((std::forward<Fn>(fn)(std::get<I>(Props{}), I), dummy)...);
}

template <typename Props, typename Fn>
void ForEachProperty(Fn&& fn) {
  ForEachPropertyImpl<Props>(std::forward<Fn>(fn),
                             make_index_sequence<std::tuple_size<Props>::value>{});
}

template <typename Class, typename Type, Type Class::*Ptr>
struct DataMember {
  using type = Type;

  static constexpr auto ptr = Ptr;

  static constexpr const type& get(const Class& obj) { return obj.*ptr; }

  static void set(Class* obj, type value) { (*obj).*ptr = std::move(value); }

  static std::string name() {
    return std::string(detail::membername_begin<Class, Type, Ptr>(),
                       detail::membername_length<Class, Type, Ptr>());
  }
};

template <typename Class>
struct ReflectionTraits {};

}  // namespace internal
}  // namespace arrow

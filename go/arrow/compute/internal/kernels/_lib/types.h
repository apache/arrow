// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// corresponds to datatype.go's arrow.Type
enum class arrtype : int {
    NULL,
    BOOL,
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT16,
    FLOAT32,
    FLOAT64
};

// Copied from <type_traits> since we use -target x86_64-target-none
// makes life easier rather than creating is_integral, etc. templates
// ourselves

/// remove_cv
  template<typename _Tp>
    struct remove_cv
    { using type = _Tp; };

  template<typename _Tp>
    struct remove_cv<const _Tp>
    { using type = _Tp; };

  template<typename _Tp>
    struct remove_cv<volatile _Tp>
    { using type = _Tp; };

  template<typename _Tp>
    struct remove_cv<const volatile _Tp>
    { using type = _Tp; };

// __remove_cv_t (std::remove_cv_t for C++11).
  template<typename _Tp>
    using __remove_cv_t = typename remove_cv<_Tp>::type;


  /// integral_constant
  template<typename _Tp, _Tp __v>
    struct integral_constant
    {
      static constexpr _Tp                  value = __v;
      typedef _Tp                           value_type;
      typedef integral_constant<_Tp, __v>   type;
      constexpr operator value_type() const noexcept { return value; }
#if __cplusplus > 201103L

#define __cpp_lib_integral_constant_callable 201304

      constexpr value_type operator()() const noexcept { return value; }
#endif
    };

  template<typename _Tp, _Tp __v>
    constexpr _Tp integral_constant<_Tp, __v>::value;


  /// The type used as a compile-time boolean with true value.
  using true_type =  integral_constant<bool, true>;

  /// The type used as a compile-time boolean with false value.
  using false_type = integral_constant<bool, false>;

  /// @cond undocumented
  /// bool_constant for C++11
  template<bool __v>
    using __bool_constant = integral_constant<bool, __v>;
  /// @endcond

#if __cplusplus >= 201703L
# define __cpp_lib_bool_constant 201505
  /// Alias template for compile-time boolean constant types.
  /// @since C++17
  template<bool __v>
    using bool_constant = integral_constant<bool, __v>;
#endif

  /// is_same
  template<typename _Tp, typename _Up>
    struct is_same
#ifdef _GLIBCXX_HAVE_BUILTIN_IS_SAME
    : public integral_constant<bool, __is_same(_Tp, _Up)>
#else
    : public false_type
#endif
    { };

#ifndef _GLIBCXX_HAVE_BUILTIN_IS_SAME
  template<typename _Tp>
    struct is_same<_Tp, _Tp>
    : public true_type
    { };
#endif


  template<bool, typename, typename>
    struct conditional;

  /// @cond undocumented
  template <typename _Type>
    struct __type_identity
    { using type = _Type; };

  template<typename _Tp>
    using __type_identity_t = typename __type_identity<_Tp>::type;

  template<typename...>
    struct __or_;

  template<>
    struct __or_<>
    : public false_type
    { };

  template<typename _B1>
    struct __or_<_B1>
    : public _B1
    { };

  template<typename _B1, typename _B2>
    struct __or_<_B1, _B2>
    : public conditional<_B1::value, _B1, _B2>::type
    { };

  template<typename _B1, typename _B2, typename _B3, typename... _Bn>
    struct __or_<_B1, _B2, _B3, _Bn...>
    : public conditional<_B1::value, _B1, __or_<_B2, _B3, _Bn...>>::type
    { };

  template<typename...>
    struct __and_;

  template<>
    struct __and_<>
    : public true_type
    { };

  template<typename _B1>
    struct __and_<_B1>
    : public _B1
    { };

  template<typename _B1, typename _B2>
    struct __and_<_B1, _B2>
    : public conditional<_B1::value, _B2, _B1>::type
    { };

  template<typename _B1, typename _B2, typename _B3, typename... _Bn>
    struct __and_<_B1, _B2, _B3, _Bn...>
    : public conditional<_B1::value, __and_<_B2, _B3, _Bn...>, _B1>::type
    { };

  template<typename _Pp>
    struct __not_
    : public __bool_constant<!bool(_Pp::value)>
    { };
  /// @endcond

#if __cplusplus >= 201703L

  /// @cond undocumented
  template<typename... _Bn>
    inline constexpr bool __or_v = __or_<_Bn...>::value;
  template<typename... _Bn>
    inline constexpr bool __and_v = __and_<_Bn...>::value;
  /// @endcond
#endif

  /// remove_reference
  template<typename _Tp>
    struct remove_reference
    { typedef _Tp   type; };

  template<typename _Tp>
    struct remove_reference<_Tp&>
    { typedef _Tp   type; };

  template<typename _Tp>
    struct remove_reference<_Tp&&>
    { typedef _Tp   type; };


// Primary template.
  /// Define a member typedef `type` only if a boolean constant is true.
  template<bool, typename _Tp = void>
    struct enable_if
    { };

  // Partial specialization for true.
  template<typename _Tp>
    struct enable_if<true, _Tp>
    { typedef _Tp type; };

  /// @cond undocumented

  // __enable_if_t (std::enable_if_t for C++11)
  template<bool _Cond, typename _Tp = void>
    using __enable_if_t = typename enable_if<_Cond, _Tp>::type;

  // Helper for SFINAE constraints
  template<typename... _Cond>
    using _Require = __enable_if_t<__and_<_Cond...>::value>;


/// Alias template for enable_if
  template<bool _Cond, typename _Tp = void>
    using enable_if_t = typename enable_if<_Cond, _Tp>::type;

  // __remove_cvref_t (std::remove_cvref_t for C++11).
  template<typename _Tp>
    using __remove_cvref_t
     = typename remove_cv<typename remove_reference<_Tp>::type>::type;
  /// @endcond

  // Primary template.
  /// Define a member typedef @c type to one of two argument types.
  template<bool _Cond, typename _Iftrue, typename _Iffalse>
    struct conditional
    { typedef _Iftrue type; };

  // Partial specialization for false.
  template<typename _Iftrue, typename _Iffalse>
    struct conditional<false, _Iftrue, _Iffalse>
    { typedef _Iffalse type; };


/// @cond undocumented
  template<typename _Tp, typename... _Types>
    using __is_one_of = __or_<is_same<_Tp, _Types>...>;

  /// @cond undocumented
  template<typename>
    struct __is_integral_helper
    : public false_type { };

  template<>
    struct __is_integral_helper<bool>
    : public true_type { };

  template<>
    struct __is_integral_helper<char>
    : public true_type { };

  template<>
    struct __is_integral_helper<signed char>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned char>
    : public true_type { };

  // We want is_integral<wchar_t> to be true (and make_signed/unsigned to work)
  // even when libc doesn't provide working <wchar.h> and related functions,
  // so check __WCHAR_TYPE__ instead of _GLIBCXX_USE_WCHAR_T.
#ifdef __WCHAR_TYPE__
  template<>
    struct __is_integral_helper<wchar_t>
    : public true_type { };
#endif

#ifdef _GLIBCXX_USE_CHAR8_T
  template<>
    struct __is_integral_helper<char8_t>
    : public true_type { };
#endif

  template<>
    struct __is_integral_helper<char16_t>
    : public true_type { };

  template<>
    struct __is_integral_helper<char32_t>
    : public true_type { };

  template<>
    struct __is_integral_helper<short>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned short>
    : public true_type { };

  template<>
    struct __is_integral_helper<int>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned int>
    : public true_type { };

  template<>
    struct __is_integral_helper<long>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned long>
    : public true_type { };

  template<>
    struct __is_integral_helper<long long>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned long long>
    : public true_type { };

  // Conditionalizing on __STRICT_ANSI__ here will break any port that
  // uses one of these types for size_t.
#if defined(__GLIBCXX_TYPE_INT_N_0)
  template<>
    struct __is_integral_helper<__GLIBCXX_TYPE_INT_N_0>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned __GLIBCXX_TYPE_INT_N_0>
    : public true_type { };
#endif
#if defined(__GLIBCXX_TYPE_INT_N_1)
  template<>
    struct __is_integral_helper<__GLIBCXX_TYPE_INT_N_1>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned __GLIBCXX_TYPE_INT_N_1>
    : public true_type { };
#endif
#if defined(__GLIBCXX_TYPE_INT_N_2)
  template<>
    struct __is_integral_helper<__GLIBCXX_TYPE_INT_N_2>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned __GLIBCXX_TYPE_INT_N_2>
    : public true_type { };
#endif
#if defined(__GLIBCXX_TYPE_INT_N_3)
  template<>
    struct __is_integral_helper<__GLIBCXX_TYPE_INT_N_3>
    : public true_type { };

  template<>
    struct __is_integral_helper<unsigned __GLIBCXX_TYPE_INT_N_3>
    : public true_type { };
#endif
  /// @endcond

  /// is_integral
  template<typename _Tp>
    struct is_integral
    : public __is_integral_helper<__remove_cv_t<_Tp>>::type
    { };

  /// @cond undocumented
  template<typename>
    struct __is_floating_point_helper
    : public false_type { };

  template<>
    struct __is_floating_point_helper<float>
    : public true_type { };

  template<>
    struct __is_floating_point_helper<double>
    : public true_type { };

  template<>
    struct __is_floating_point_helper<long double>
    : public true_type { };

  /// is_floating_point
  template<typename _Tp>
    struct is_floating_point
    : public __is_floating_point_helper<__remove_cv_t<_Tp>>::type
    { };


  // Check if a type is one of the unsigned integer types.
  template<typename _Tp>
    using __is_unsigned_integer = __is_one_of<__remove_cv_t<_Tp>,
	  unsigned char, unsigned short, unsigned int, unsigned long,
	  unsigned long long
#if defined(__GLIBCXX_TYPE_INT_N_0)
	  , unsigned __GLIBCXX_TYPE_INT_N_0
#endif
#if defined(__GLIBCXX_TYPE_INT_N_1)
	  , unsigned __GLIBCXX_TYPE_INT_N_1
#endif
#if defined(__GLIBCXX_TYPE_INT_N_2)
	  , unsigned __GLIBCXX_TYPE_INT_N_2
#endif
#if defined(__GLIBCXX_TYPE_INT_N_3)
	  , unsigned __GLIBCXX_TYPE_INT_N_3
#endif
	  >;


  // Check if a type is one of the signed integer types.
  template<typename _Tp>
    using __is_signed_integer = __is_one_of<__remove_cv_t<_Tp>,
	  signed char, signed short, signed int, signed long,
	  signed long long
#if defined(__GLIBCXX_TYPE_INT_N_0)
	  , signed __GLIBCXX_TYPE_INT_N_0
#endif
#if defined(__GLIBCXX_TYPE_INT_N_1)
	  , signed __GLIBCXX_TYPE_INT_N_1
#endif
#if defined(__GLIBCXX_TYPE_INT_N_2)
	  , signed __GLIBCXX_TYPE_INT_N_2
#endif
#if defined(__GLIBCXX_TYPE_INT_N_3)
	  , signed __GLIBCXX_TYPE_INT_N_3
#endif
	  >;


  /// is_arithmetic
  template<typename _Tp>
    struct is_arithmetic
    : public __or_<is_integral<_Tp>, is_floating_point<_Tp>>::type
    { };


  /// @cond undocumented
  template<typename _Tp,
	   bool = is_arithmetic<_Tp>::value>
    struct __is_signed_helper
    : public false_type { };

  template<typename _Tp>
    struct __is_signed_helper<_Tp, true>
    : public integral_constant<bool, _Tp(-1) < _Tp(0)>
    { };
  /// @endcond

  /// is_signed
  template<typename _Tp>
    struct is_signed
    : public __is_signed_helper<_Tp>::type
    { };

  /// is_unsigned
  template<typename _Tp>
    struct is_unsigned
    : public __and_<is_arithmetic<_Tp>, __not_<is_signed<_Tp>>>
    { };

template <typename _Tp>
  inline constexpr bool is_integral_v = is_integral<_Tp>::value;
template <typename _Tp>
  inline constexpr bool is_floating_point_v = is_floating_point<_Tp>::value;
template <typename _Tp>
  inline constexpr bool is_signed_v = is_signed<_Tp>::value;
template <typename _Tp>
  inline constexpr bool is_unsigned_v = is_unsigned<_Tp>::value;
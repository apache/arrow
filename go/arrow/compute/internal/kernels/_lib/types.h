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

#pragma once

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


#define _LIBCPP_TEMPLATE_VIS
#define _LIBCPP_CONSTEXPR constexpr
#define _LIBCPP_INLINE_VISIBILITY
#define _LIBCPP_STD_VER 17
#define _LIBCPP_NODEBUG
#define _LIBCPP_HAS_NO_CHAR8_T
#define _NOEXCEPT noexcept
#define _NOEXCEPT_(x) noexcept(x)
#define _LIBCPP_HIDE_FROM_ABI

using size_t = uint64_t;

// copied from libcxx/include/__type_traits/integral_constant.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

template <class _Tp, _Tp __v>
struct _LIBCPP_TEMPLATE_VIS integral_constant
{
  static _LIBCPP_CONSTEXPR const _Tp      value = __v;
  typedef _Tp               value_type;
  typedef integral_constant type;
  _LIBCPP_INLINE_VISIBILITY
  _LIBCPP_CONSTEXPR operator value_type() const _NOEXCEPT {return value;}
#if _LIBCPP_STD_VER > 11
  _LIBCPP_INLINE_VISIBILITY
  constexpr value_type operator ()() const _NOEXCEPT {return value;}
#endif
};

template <class _Tp, _Tp __v>
_LIBCPP_CONSTEXPR const _Tp integral_constant<_Tp, __v>::value;

typedef integral_constant<bool, true>  true_type;
typedef integral_constant<bool, false> false_type;

template <bool _Val>
using _BoolConstant _LIBCPP_NODEBUG = integral_constant<bool, _Val>;

#if _LIBCPP_STD_VER > 14
template <bool __b>
using bool_constant = integral_constant<bool, __b>;
#endif

// copied from libcxx/include/__type_traits/remove_const.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#if __has_builtin(__remove_const)
template <class _Tp>
struct remove_const {
  using type _LIBCPP_NODEBUG = __remove_const(_Tp);
};

template <class _Tp>
using __remove_const_t = __remove_const(_Tp);
#else
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS remove_const            {typedef _Tp type;};
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS remove_const<const _Tp> {typedef _Tp type;};

template <class _Tp>
using __remove_const_t = typename remove_const<_Tp>::type;
#endif // __has_builtin(__remove_const)

#if _LIBCPP_STD_VER > 11
template <class _Tp> using remove_const_t = __remove_const_t<_Tp>;
#endif

// copied from libcxx/include/__type_traits/remove_volatile.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#if __has_builtin(__remove_volatile)
template <class _Tp>
struct remove_volatile {
  using type _LIBCPP_NODEBUG = __remove_volatile(_Tp);
};

template <class _Tp>
using __remove_volatile_t = __remove_volatile(_Tp);
#else
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS remove_volatile               {typedef _Tp type;};
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS remove_volatile<volatile _Tp> {typedef _Tp type;};

template <class _Tp>
using __remove_volatile_t = typename remove_volatile<_Tp>::type;
#endif // __has_builtin(__remove_volatile)

#if _LIBCPP_STD_VER > 11
template <class _Tp> using remove_volatile_t = __remove_volatile_t<_Tp>;
#endif

// copied from libcxx/include/__type_traits/remove_cv.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#if __has_builtin(__remove_cv)
template <class _Tp>
struct remove_cv {
  using type _LIBCPP_NODEBUG = __remove_cv(_Tp);
};

template <class _Tp>
using __remove_cv_t = __remove_cv(_Tp);
#else
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS remove_cv
{typedef __remove_volatile_t<__remove_const_t<_Tp> > type;};

template <class _Tp>
using __remove_cv_t = __remove_volatile_t<__remove_const_t<_Tp> >;
#endif // __has_builtin(__remove_cv)

#if _LIBCPP_STD_VER > 11
template <class _Tp> using remove_cv_t = __remove_cv_t<_Tp>;
#endif

// copied from libcxx/include/__type_traits/is_floating_point.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//


template <class _Tp> struct __libcpp_is_floating_point              : public false_type {};
template <>          struct __libcpp_is_floating_point<float>       : public true_type {};
template <>          struct __libcpp_is_floating_point<double>      : public true_type {};
template <>          struct __libcpp_is_floating_point<long double> : public true_type {};

template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_floating_point
    : public __libcpp_is_floating_point<__remove_cv_t<_Tp> > {};

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_floating_point_v = is_floating_point<_Tp>::value;
#endif

// copied from libcxx/include/__type_traits/is_integral.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//


template <class _Tp> struct __libcpp_is_integral                     { enum { value = 0 }; };
template <>          struct __libcpp_is_integral<bool>               { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<char>               { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<signed char>        { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<unsigned char>      { enum { value = 1 }; };
#ifndef _LIBCPP_HAS_NO_WIDE_CHARACTERS
template <>          struct __libcpp_is_integral<wchar_t>            { enum { value = 1 }; };
#endif
#ifndef _LIBCPP_HAS_NO_CHAR8_T
template <>          struct __libcpp_is_integral<char8_t>            { enum { value = 1 }; };
#endif
template <>          struct __libcpp_is_integral<char16_t>           { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<char32_t>           { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<short>              { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<unsigned short>     { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<int>                { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<unsigned int>       { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<long>               { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<unsigned long>      { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<long long>          { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<unsigned long long> { enum { value = 1 }; };
#ifndef _LIBCPP_HAS_NO_INT128
template <>          struct __libcpp_is_integral<__int128_t>         { enum { value = 1 }; };
template <>          struct __libcpp_is_integral<__uint128_t>        { enum { value = 1 }; };
#endif

#if __has_builtin(__is_integral)

template <class _Tp>
struct _LIBCPP_TEMPLATE_VIS is_integral : _BoolConstant<__is_integral(_Tp)> { };

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_integral_v = __is_integral(_Tp);
#endif

#else

template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_integral
    : public _BoolConstant<__libcpp_is_integral<__remove_cv_t<_Tp> >::value> {};

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_integral_v = is_integral<_Tp>::value;
#endif

#endif // __has_builtin(__is_integral)

// copied from libcxx/include/__type_traits/is_arithmetic.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//


template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_arithmetic
    : public integral_constant<bool, is_integral<_Tp>::value      ||
                                     is_floating_point<_Tp>::value> {};

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_arithmetic_v = is_arithmetic<_Tp>::value;
#endif

// copied from libcxx/include/__type_traits/is_signed.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#if __has_builtin(__is_signed)

template<class _Tp>
struct _LIBCPP_TEMPLATE_VIS is_signed : _BoolConstant<__is_signed(_Tp)> { };

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_signed_v = __is_signed(_Tp);
#endif

#else // __has_builtin(__is_signed)

template <class _Tp, bool = is_integral<_Tp>::value>
struct __libcpp_is_signed_impl : public _BoolConstant<(_Tp(-1) < _Tp(0))> {};

template <class _Tp>
struct __libcpp_is_signed_impl<_Tp, false> : public true_type {};  // floating point

template <class _Tp, bool = is_arithmetic<_Tp>::value>
struct __libcpp_is_signed : public __libcpp_is_signed_impl<_Tp> {};

template <class _Tp> struct __libcpp_is_signed<_Tp, false> : public false_type {};

template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_signed : public __libcpp_is_signed<_Tp> {};

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_signed_v = is_signed<_Tp>::value;
#endif

#endif // __has_builtin(__is_signed)


// copied from libcxx/include/__type_traits/is_unsigned.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//


// Before AppleClang 14, __is_unsigned returned true for enums with signed underlying type.
#if __has_builtin(__is_unsigned) && !(defined(_LIBCPP_APPLE_CLANG_VER) && _LIBCPP_APPLE_CLANG_VER < 1400)

template<class _Tp>
struct _LIBCPP_TEMPLATE_VIS is_unsigned : _BoolConstant<__is_unsigned(_Tp)> { };

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_unsigned_v = __is_unsigned(_Tp);
#endif

#else // __has_builtin(__is_unsigned)

template <class _Tp, bool = is_integral<_Tp>::value>
struct __libcpp_is_unsigned_impl : public _BoolConstant<(_Tp(0) < _Tp(-1))> {};

template <class _Tp>
struct __libcpp_is_unsigned_impl<_Tp, false> : public false_type {};  // floating point

template <class _Tp, bool = is_arithmetic<_Tp>::value>
struct __libcpp_is_unsigned : public __libcpp_is_unsigned_impl<_Tp> {};

template <class _Tp> struct __libcpp_is_unsigned<_Tp, false> : public false_type {};

template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_unsigned : public __libcpp_is_unsigned<_Tp> {};

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_unsigned_v = is_unsigned<_Tp>::value;
#endif

#endif // __has_builtin(__is_unsigned)

// copied from libcxx/include/__type_traits/is_same.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

template <class _Tp, class _Up>
struct _LIBCPP_TEMPLATE_VIS is_same : _BoolConstant<__is_same(_Tp, _Up)> { };

#if _LIBCPP_STD_VER > 14
template <class _Tp, class _Up>
inline constexpr bool is_same_v = __is_same(_Tp, _Up);
#endif

// copied from libcxx/include/__type_traits/conditional.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

template <bool>
struct _IfImpl;

template <>
struct _IfImpl<true> {
  template <class _IfRes, class _ElseRes>
  using _Select _LIBCPP_NODEBUG = _IfRes;
};

template <>
struct _IfImpl<false> {
  template <class _IfRes, class _ElseRes>
  using _Select _LIBCPP_NODEBUG = _ElseRes;
};

template <bool _Cond, class _IfRes, class _ElseRes>
using _If _LIBCPP_NODEBUG = typename _IfImpl<_Cond>::template _Select<_IfRes, _ElseRes>;

template <bool _Bp, class _If, class _Then>
    struct _LIBCPP_TEMPLATE_VIS conditional {typedef _If type;};
template <class _If, class _Then>
    struct _LIBCPP_TEMPLATE_VIS conditional<false, _If, _Then> {typedef _Then type;};

#if _LIBCPP_STD_VER > 11
template <bool _Bp, class _IfRes, class _ElseRes>
using conditional_t = typename conditional<_Bp, _IfRes, _ElseRes>::type;
#endif

// Helper so we can use "conditional_t" in all language versions.
template <bool _Bp, class _If, class _Then> using __conditional_t = typename conditional<_Bp, _If, _Then>::type;

// copied from libcxx/include/__type_traits/is_const.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#if __has_builtin(__is_const)

template <class _Tp>
struct _LIBCPP_TEMPLATE_VIS is_const : _BoolConstant<__is_const(_Tp)> { };

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_const_v = __is_const(_Tp);
#endif

#else

template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_const            : public false_type {};
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_const<_Tp const> : public true_type {};

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_const_v = is_const<_Tp>::value;
#endif

#endif // __has_builtin(__is_const)

// copied from libcxx/include/__type_traits/is_volatile.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#if __has_builtin(__is_volatile)

template <class _Tp>
struct _LIBCPP_TEMPLATE_VIS is_volatile : _BoolConstant<__is_volatile(_Tp)> { };

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_volatile_v = __is_volatile(_Tp);
#endif

#else

template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_volatile               : public false_type {};
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_volatile<_Tp volatile> : public true_type {};

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_volatile_v = is_volatile<_Tp>::value;
#endif

#endif // __has_builtin(__is_volatile)

// copied from libcxx/include/__type_traits/remove_reference.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#if __has_builtin(__remove_reference_t)
template <class _Tp>
struct remove_reference {
  using type _LIBCPP_NODEBUG = __remove_reference_t(_Tp);
};

template <class _Tp>
using __libcpp_remove_reference_t = __remove_reference_t(_Tp);
#else
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS remove_reference        {typedef _LIBCPP_NODEBUG _Tp type;};
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS remove_reference<_Tp&>  {typedef _LIBCPP_NODEBUG _Tp type;};
template <class _Tp> struct _LIBCPP_TEMPLATE_VIS remove_reference<_Tp&&> {typedef _LIBCPP_NODEBUG _Tp type;};

template <class _Tp>
using __libcpp_remove_reference_t = typename remove_reference<_Tp>::type;
#endif // __has_builtin(__remove_reference_t)

#if _LIBCPP_STD_VER > 11
template <class _Tp> using remove_reference_t = __libcpp_remove_reference_t<_Tp>;
#endif

// copied from libcxx/include/__type_traits/apply_cv.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

template <class _Tp, class _Up, bool = is_const<__libcpp_remove_reference_t<_Tp> >::value,
                             bool = is_volatile<__libcpp_remove_reference_t<_Tp> >::value>
struct __apply_cv
{
    typedef _LIBCPP_NODEBUG _Up type;
};

template <class _Tp, class _Up>
struct __apply_cv<_Tp, _Up, true, false>
{
    typedef _LIBCPP_NODEBUG const _Up type;
};

template <class _Tp, class _Up>
struct __apply_cv<_Tp, _Up, false, true>
{
    typedef volatile _Up type;
};

template <class _Tp, class _Up>
struct __apply_cv<_Tp, _Up, true, true>
{
    typedef const volatile _Up type;
};

template <class _Tp, class _Up>
struct __apply_cv<_Tp&, _Up, false, false>
{
    typedef _Up& type;
};

template <class _Tp, class _Up>
struct __apply_cv<_Tp&, _Up, true, false>
{
    typedef const _Up& type;
};

template <class _Tp, class _Up>
struct __apply_cv<_Tp&, _Up, false, true>
{
    typedef volatile _Up& type;
};

template <class _Tp, class _Up>
struct __apply_cv<_Tp&, _Up, true, true>
{
    typedef const volatile _Up& type;
};

// copied from libcxx/include/__type_traits/apply_cv.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

struct __nat
{
#ifndef _LIBCPP_CXX03_LANG
    __nat() = delete;
    __nat(const __nat&) = delete;
    __nat& operator=(const __nat&) = delete;
    ~__nat() = delete;
#endif
};

// copied from libcxx/include/__type_traits/type_list.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//


template <class _Hp, class _Tp>
struct __type_list
{
    typedef _Hp _Head;
    typedef _Tp _Tail;
};

template <class _TypeList, size_t _Size, bool = _Size <= sizeof(typename _TypeList::_Head)> struct __find_first;

template <class _Hp, class _Tp, size_t _Size>
struct __find_first<__type_list<_Hp, _Tp>, _Size, true>
{
    typedef _LIBCPP_NODEBUG _Hp type;
};

template <class _Hp, class _Tp, size_t _Size>
struct __find_first<__type_list<_Hp, _Tp>, _Size, false>
{
    typedef _LIBCPP_NODEBUG typename __find_first<_Tp, _Size>::type type;
};

// copied from libcxx/include/__type_traits/is_enum.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

template <class _Tp> struct _LIBCPP_TEMPLATE_VIS is_enum
    : public integral_constant<bool, __is_enum(_Tp)> {};

#if _LIBCPP_STD_VER > 14
template <class _Tp>
inline constexpr bool is_enum_v = __is_enum(_Tp);
#endif

// copied from libcxx/include/__type_traits/make_unsigned.h
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#if __has_builtin(__make_unsigned)

template <class _Tp>
using __make_unsigned_t = __make_unsigned(_Tp);

#else
typedef
    __type_list<unsigned char,
    __type_list<unsigned short,
    __type_list<unsigned int,
    __type_list<unsigned long,
    __type_list<unsigned long long,
#  ifndef _LIBCPP_HAS_NO_INT128
    __type_list<__uint128_t,
#  endif
    __nat
#  ifndef _LIBCPP_HAS_NO_INT128
    >
#  endif
    > > > > > __unsigned_types;

template <class _Tp, bool = is_integral<_Tp>::value || is_enum<_Tp>::value>
struct __make_unsigned {};

template <class _Tp>
struct __make_unsigned<_Tp, true>
{
    typedef typename __find_first<__unsigned_types, sizeof(_Tp)>::type type;
};

template <> struct __make_unsigned<bool,               true> {};
template <> struct __make_unsigned<  signed short,     true> {typedef unsigned short     type;};
template <> struct __make_unsigned<unsigned short,     true> {typedef unsigned short     type;};
template <> struct __make_unsigned<  signed int,       true> {typedef unsigned int       type;};
template <> struct __make_unsigned<unsigned int,       true> {typedef unsigned int       type;};
template <> struct __make_unsigned<  signed long,      true> {typedef unsigned long      type;};
template <> struct __make_unsigned<unsigned long,      true> {typedef unsigned long      type;};
template <> struct __make_unsigned<  signed long long, true> {typedef unsigned long long type;};
template <> struct __make_unsigned<unsigned long long, true> {typedef unsigned long long type;};
#  ifndef _LIBCPP_HAS_NO_INT128
template <> struct __make_unsigned<__int128_t,         true> {typedef __uint128_t        type;};
template <> struct __make_unsigned<__uint128_t,        true> {typedef __uint128_t        type;};
#  endif

template <class _Tp>
using __make_unsigned_t = typename __apply_cv<_Tp, typename __make_unsigned<__remove_cv_t<_Tp> >::type>::type;

#endif // __has_builtin(__make_unsigned)

template <class _Tp>
struct make_unsigned {
  using type _LIBCPP_NODEBUG = __make_unsigned_t<_Tp>;
};

#if _LIBCPP_STD_VER > 11
template <class _Tp> using make_unsigned_t = __make_unsigned_t<_Tp>;
#endif

#ifndef _LIBCPP_CXX03_LANG
template <class _Tp>
_LIBCPP_HIDE_FROM_ABI constexpr
__make_unsigned_t<_Tp> __to_unsigned_like(_Tp __x) noexcept {
    return static_cast<__make_unsigned_t<_Tp> >(__x);
}
#endif

template <class _Tp, class _Up>
using __copy_unsigned_t = __conditional_t<is_unsigned<_Tp>::value, __make_unsigned_t<_Up>, _Up>;

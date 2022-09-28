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


#define _LIBCPP_TEMPLATE_VIS
#define _LIBCPP_CONSTEXPR constexpr
#define _LIBCPP_INLINE_VISIBILITY
#define _LIBCPP_STD_VER 17
#define _LIBCPP_NODEBUG
#define _LIBCPP_HAS_NO_CHAR8_T
#define _NOEXCEPT noexcept
#define _NOEXCEPT_(x) noexcept(x)

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

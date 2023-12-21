/***************************************************************************
 * Copyright (c) Johan Mabille, Sylvain Corlay, Wolf Vollprecht and         *
 * Martin Renou                                                             *
 * Copyright (c) QuantStack                                                 *
 * Copyright (c) Serge Guelton                                              *
 *                                                                          *
 * Distributed under the terms of the BSD 3-Clause License.                 *
 *                                                                          *
 * The full license is in the file LICENSE, distributed with this software. *
 ****************************************************************************/

#include "xsimd/types/xsimd_utils.hpp"
#include "xsimd/xsimd.hpp"
#include "gtest/gtest.h"

template <class T>
struct scalar_type
{
    using type = T;
};
template <class T, class A>
struct scalar_type<xsimd::batch<T, A>>
{
    using type = T;
};

template <class T>
T extract(T const& value) { return value; }

template <class T, class A>
T extract(xsimd::batch<T, A> const& batch) { return batch.get(0); }

template <class T, class A>
bool extract(xsimd::batch_bool<T, A> const& batch) { return batch.get(0); }

/*
 * Functions that apply on scalar types only
 */

template <typename T>
class xsimd_api_scalar_types_functions : public ::testing::Test
{
    using value_type = typename scalar_type<T>::type;

public:
    void test_bitofsign()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::bitofsign(T(val))), val < 0);
    }

    void test_bitwise_and()
    {
        value_type val0(1);
        value_type val1(3);
        xsimd::as_unsigned_integer_t<value_type> ival0, ival1, ir;
        std::memcpy((void*)&ival0, (void*)&val0, sizeof(val0));
        std::memcpy((void*)&ival1, (void*)&val1, sizeof(val1));
        value_type r;
        ir = ival0 & ival1;
        std::memcpy((void*)&r, (void*)&ir, sizeof(ir));
        EXPECT_EQ(extract(xsimd::bitwise_and(T(val0), T(val1))), r);
    }

    void test_bitwise_andnot()
    {
        value_type val0(1);
        value_type val1(3);
        xsimd::as_unsigned_integer_t<value_type> ival0, ival1, ir;
        std::memcpy((void*)&ival0, (void*)&val0, sizeof(val0));
        std::memcpy((void*)&ival1, (void*)&val1, sizeof(val1));
        value_type r;
        ir = ival0 & ~ival1;
        std::memcpy((void*)&r, (void*)&ir, sizeof(ir));
        EXPECT_EQ(extract(xsimd::bitwise_andnot(T(val0), T(val1))), r);
    }

    void test_bitwise_not()
    {
        value_type val(1);
        xsimd::as_unsigned_integer_t<value_type> ival, ir;
        std::memcpy((void*)&ival, (void*)&val, sizeof(val));
        value_type r;
        ir = ~ival;
        std::memcpy((void*)&r, (void*)&ir, sizeof(ir));
        EXPECT_EQ(extract(xsimd::bitwise_not(T(val))), r);
    }

    void test_bitwise_or()
    {
        value_type val0(1);
        value_type val1(4);
        xsimd::as_unsigned_integer_t<value_type> ival0, ival1, ir;
        std::memcpy((void*)&ival0, (void*)&val0, sizeof(val0));
        std::memcpy((void*)&ival1, (void*)&val1, sizeof(val1));
        value_type r;
        ir = ival0 | ival1;
        std::memcpy((void*)&r, (void*)&ir, sizeof(ir));
        EXPECT_EQ(extract(xsimd::bitwise_or(T(val0), T(val1))), r);
    }

    void test_bitwise_xor()
    {
        value_type val0(1);
        value_type val1(2);
        xsimd::as_unsigned_integer_t<value_type> ival0, ival1, ir;
        std::memcpy((void*)&ival0, (void*)&val0, sizeof(val0));
        std::memcpy((void*)&ival1, (void*)&val1, sizeof(val1));
        value_type r;
        ir = ival0 ^ ival1;
        std::memcpy((void*)&r, (void*)&ir, sizeof(ir));
        EXPECT_EQ(extract(xsimd::bitwise_xor(T(val0), T(val1))), r);
    }

    void test_clip()
    {
        value_type val0(5);
        value_type val1(2);
        value_type val2(3);
        EXPECT_EQ(extract(xsimd::clip(T(val0), T(val1), T(val2))), val0 <= val1 ? val1 : (val0 >= val2 ? val2 : val0));
    }

    void test_ge()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::ge(T(val0), T(val1))), val0 >= val1);
    }

    void test_gt()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::gt(T(val0), T(val1))), val0 > val1);
    }

    void test_le()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::le(T(val0), T(val1))), val0 <= val1);
    }

    void test_lt()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::lt(T(val0), T(val1))), val0 < val1);
    }

    void test_max()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::max(T(val0), T(val1))), std::max(val0, val1));
    }

    void test_min()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::min(T(val0), T(val1))), std::min(val0, val1));
    }

    void test_remainder()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::remainder(T(val0), T(val1))), val0 - xsimd::as_integer_t<value_type>(val0) / xsimd::as_integer_t<value_type>(val1));
    }
    void test_sign()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::sign(T(val))), val == 0 ? 0 : val > 0 ? 1
                                                                       : -1);
    }
    void test_signnz()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::signnz(T(val))), val == 0 ? 1 : val > 0 ? 1
                                                                         : -1);
    }
};

using ScalarTypes = ::testing::Types<
    char, unsigned char, signed char, short, unsigned short, int, unsigned int, long, unsigned long, float, double
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE
    ,
    xsimd::batch<char>, xsimd::batch<unsigned char>, xsimd::batch<signed char>, xsimd::batch<short>, xsimd::batch<unsigned short>, xsimd::batch<int>, xsimd::batch<unsigned int>, xsimd::batch<long>, xsimd::batch<unsigned long>, xsimd::batch<float>
#if defined(XSIMD_WITH_NEON) && !defined(XSIMD_WITH_NEON64)
    ,
    xsimd::batch<double>
#endif
#endif
    >;

TYPED_TEST_SUITE(xsimd_api_scalar_types_functions, ScalarTypes);

TYPED_TEST(xsimd_api_scalar_types_functions, bitofsign)
{
    this->test_bitofsign();
}

TYPED_TEST(xsimd_api_scalar_types_functions, bitwise_and)
{
    this->test_bitwise_and();
}

TYPED_TEST(xsimd_api_scalar_types_functions, bitwise_andnot)
{
    this->test_bitwise_andnot();
}

TYPED_TEST(xsimd_api_scalar_types_functions, bitwise_not)
{
    this->test_bitwise_not();
}

TYPED_TEST(xsimd_api_scalar_types_functions, bitwise_or)
{
    this->test_bitwise_or();
}

TYPED_TEST(xsimd_api_scalar_types_functions, bitwise_xor)
{
    this->test_bitwise_xor();
}

TYPED_TEST(xsimd_api_scalar_types_functions, clip)
{
    this->test_clip();
}

TYPED_TEST(xsimd_api_scalar_types_functions, ge)
{
    this->test_ge();
}

TYPED_TEST(xsimd_api_scalar_types_functions, gt)
{
    this->test_gt();
}

TYPED_TEST(xsimd_api_scalar_types_functions, le)
{
    this->test_le();
}

TYPED_TEST(xsimd_api_scalar_types_functions, lt)
{
    this->test_lt();
}

TYPED_TEST(xsimd_api_scalar_types_functions, max)
{
    this->test_max();
}

TYPED_TEST(xsimd_api_scalar_types_functions, min)
{
    this->test_min();
}

TYPED_TEST(xsimd_api_scalar_types_functions, remainder)
{
    this->test_remainder();
}

TYPED_TEST(xsimd_api_scalar_types_functions, sign)
{
    this->test_sign();
}

TYPED_TEST(xsimd_api_scalar_types_functions, signnz)
{
    this->test_signnz();
}

/*
 * Functions that apply on integral types only
 */

template <typename T>
class xsimd_api_integral_types_functions : public ::testing::Test
{
    using value_type = typename scalar_type<T>::type;

public:
    void test_mod()
    {
        value_type val0(5);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::mod(T(val0), T(val1))), val0 % val1);
    }
    void test_sadd()
    {
        value_type val0(122);
        value_type val1(std::numeric_limits<value_type>::max());
        EXPECT_EQ(extract(xsimd::sadd(T(val0), T(val1))), (val0 > std::numeric_limits<value_type>::max() - val1) ? std::numeric_limits<value_type>::max() : (val0 + val1));
    }
    void test_ssub()
    {
        value_type val0(122);
        value_type val1(121);
        EXPECT_EQ(extract(xsimd::ssub(T(val0), T(val1))), (val0 < std::numeric_limits<value_type>::min() + val1) ? std::numeric_limits<value_type>::min() : (val0 - val1));
    }
};

using IntegralTypes = ::testing::Types<
    char, unsigned char, signed char, short, unsigned short, int, unsigned int, long, unsigned long
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE
    ,
    xsimd::batch<char>, xsimd::batch<unsigned char>, xsimd::batch<signed char>, xsimd::batch<short>, xsimd::batch<unsigned short>, xsimd::batch<int>, xsimd::batch<unsigned int>, xsimd::batch<long>, xsimd::batch<unsigned long>
#endif
    >;

TYPED_TEST_SUITE(xsimd_api_integral_types_functions, IntegralTypes);

TYPED_TEST(xsimd_api_integral_types_functions, mod)
{
    this->test_mod();
}

TYPED_TEST(xsimd_api_integral_types_functions, sadd)
{
    this->test_sadd();
}

TYPED_TEST(xsimd_api_integral_types_functions, ssub)
{
    this->test_ssub();
}

/*
 * Functions that apply on floating points types only
 */

template <typename T>
class xsimd_api_float_types_functions : public ::testing::Test
{
    using value_type = typename scalar_type<T>::type;

public:
    void test_acos()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::acos(T(val))), std::acos(val));
    }
    void test_acosh()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::acosh(T(val))), std::acosh(val));
    }
    void test_asin()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::asin(T(val))), std::asin(val));
    }
    void test_asinh()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::asinh(T(val))), std::asinh(val));
    }
    void test_atan()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::atan(T(val))), std::atan(val));
    }
    void test_atan2()
    {
        value_type val0(0);
        value_type val1(1);
        EXPECT_EQ(extract(xsimd::atan2(T(val0), T(val1))), std::atan2(val0, val1));
    }
    void test_atanh()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::atanh(T(val))), std::atanh(val));
    }
    void test_cbrt()
    {
        value_type val(8);
        EXPECT_EQ(extract(xsimd::cbrt(T(val))), std::cbrt(val));
    }
    void test_ceil()
    {
        value_type val(1.5);
        EXPECT_EQ(extract(xsimd::ceil(T(val))), std::ceil(val));
    }

    void test_copysign()
    {
        value_type val0(2);
        value_type val1(-1);
        EXPECT_EQ(extract(xsimd::copysign(T(val0), T(val1))), (value_type)std::copysign(val0, val1));
    }
    void test_cos()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::cos(T(val))), std::cos(val));
    }
    void test_cosh()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::cosh(T(val))), std::cosh(val));
    }
    void test_exp()
    {
        value_type val(2);
        EXPECT_EQ(extract(xsimd::exp(T(val))), std::exp(val));
    }
    void test_exp10()
    {
        value_type val(2);
        EXPECT_EQ(extract(xsimd::exp10(T(val))), std::pow(value_type(10), val));
    }
    void test_exp2()
    {
        value_type val(2);
        EXPECT_EQ(extract(xsimd::exp2(T(val))), std::exp2(val));
    }
    void test_expm1()
    {
        value_type val(2);
        EXPECT_EQ(extract(xsimd::expm1(T(val))), std::expm1(val));
    }
    void test_erf()
    {
        value_type val(2);
        EXPECT_EQ(extract(xsimd::erf(T(val))), std::erf(val));
    }
    void test_erfc()
    {
        // FIXME: can we do better?
        value_type val(0);
        EXPECT_NEAR(extract(xsimd::erfc(T(val))), std::erfc(val), 10e-7);
    }
    void test_fabs()
    {
        value_type val(-3);
        EXPECT_EQ(extract(xsimd::fabs(T(val))), std::abs(val));
    }
    void test_fdim()
    {
        value_type val0(-3);
        value_type val1(1);
        EXPECT_EQ(extract(xsimd::fdim(T(val0), T(val1))), std::fdim(val0, val1));
    }
    void test_floor()
    {
        value_type val(3.1);
        EXPECT_EQ(extract(xsimd::floor(T(val))), std::floor(val));
    }
    void test_fmax()
    {
        value_type val0(3);
        value_type val1(1);
        EXPECT_EQ(extract(xsimd::fmax(T(val0), T(val1))), std::fmax(val0, val1));
    }
    void test_fmin()
    {
        value_type val0(3);
        value_type val1(1);
        EXPECT_EQ(extract(xsimd::fmin(T(val0), T(val1))), std::fmin(val0, val1));
    }
    void test_fmod()
    {
        value_type val0(3);
        value_type val1(1);
        EXPECT_EQ(extract(xsimd::fmin(T(val0), T(val1))), std::fmin(val0, val1));
    }
    void test_frexp()
    {
        value_type val(3.3);
        int res;
        typename std::conditional<std::is_floating_point<T>::value, int, xsimd::as_integer_t<T>>::type vres;
        EXPECT_EQ(extract(xsimd::frexp(T(val), vres)), std::frexp(val, &res));
        EXPECT_EQ(extract(vres), res);
    }
    void test_hypot()
    {
        value_type val0(3);
        value_type val1(1);
        EXPECT_EQ(extract(xsimd::hypot(T(val0), T(val1))), std::hypot(val0, val1));
    }
    void test_is_even()
    {
        value_type val(4);
        EXPECT_EQ(extract(xsimd::is_even(T(val))), (val == long(val)) && (long(val) % 2 == 0));
    }
    void test_is_flint()
    {
        value_type val(4.1);
        EXPECT_EQ(extract(xsimd::is_flint(T(val))), (val == long(val)));
    }
    void test_is_odd()
    {
        value_type val(4);
        EXPECT_EQ(extract(xsimd::is_odd(T(val))), (val == long(val)) && (long(val) % 2 == 1));
    }
    void test_isinf()
    {
        value_type val(4);
        EXPECT_EQ(extract(xsimd::isinf(T(val))), std::isinf(val));
    }
    void test_isfinite()
    {
        value_type val(4);
        EXPECT_EQ(extract(xsimd::isfinite(T(val))), std::isfinite(val));
    }
    void test_isnan()
    {
        value_type val(4);
        EXPECT_EQ(extract(xsimd::isnan(T(val))), std::isnan(val));
    }
    void test_ldexp()
    {
        value_type val0(4);
        xsimd::as_integer_t<value_type> val1(2);
        EXPECT_EQ(extract(xsimd::ldexp(T(val0), xsimd::as_integer_t<T>(val1))), std::ldexp(val0, val1));
    }
    void test_lgamma()
    {
        value_type val(2);
        EXPECT_EQ(extract(xsimd::lgamma(T(val))), std::lgamma(val));
    }
    void test_log()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::log(T(val))), std::log(val));
    }

    void test_log2()
    {
        value_type val(2);
        EXPECT_EQ(extract(xsimd::log2(T(val))), std::log2(val));
    }

    void test_log10()
    {
        value_type val(10);
        EXPECT_EQ(extract(xsimd::log10(T(val))), std::log10(val));
    }

    void test_log1p()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::log1p(T(val))), std::log1p(val));
    }
    void test_nearbyint()
    {
        value_type val(3.1);
        EXPECT_EQ(extract(xsimd::nearbyint(T(val))), std::nearbyint(val));
    }
    void test_nearbyint_as_int()
    {
        value_type val(3.1);
        EXPECT_EQ(extract(xsimd::nearbyint_as_int(T(val))), long(std::nearbyint(val)));
    }
    void test_nextafter()
    {
        value_type val0(3);
        value_type val1(4);
        EXPECT_EQ(extract(xsimd::nextafter(T(val0), T(val1))), std::nextafter(val0, val1));
    }
    void test_polar()
    {
        value_type val0(3);
        value_type val1(4);
        EXPECT_EQ(extract(xsimd::polar(T(val0), T(val1))), std::polar(val0, val1));
    }
    void test_pow()
    {
        value_type val0(2);
        value_type val1(3);
        int ival1 = 4;
        EXPECT_EQ(extract(xsimd::pow(T(val0), T(val1))), std::pow(val0, val1));
        EXPECT_EQ(extract(xsimd::pow(T(val0), ival1)), std::pow(val0, ival1));
    }
    void test_reciprocal()
    {
        value_type val(1);
        EXPECT_NEAR(extract(xsimd::reciprocal(T(val))), value_type(1) / val, 10e-2);
    }
    void test_rint()
    {
        value_type val(3.1);
        EXPECT_EQ(extract(xsimd::rint(T(val))), std::rint(val));
    }
    void test_round()
    {
        value_type val(3.1);
        EXPECT_EQ(extract(xsimd::round(T(val))), std::round(val));
    }
    void test_rsqrt()
    {
        value_type val(4);
        EXPECT_NEAR(extract(xsimd::rsqrt(T(val))), value_type(1) / std::sqrt(val), 10e-4);
    }
    void test_sin()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::sin(T(val))), std::sin(val));
    }
    void test_sincos()
    {
        value_type val(0);
        auto vres = xsimd::sincos(T(val));
        EXPECT_EQ(extract(vres.first), std::sin(val));
        EXPECT_EQ(extract(vres.second), std::cos(val));
    }
    void test_sinh()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::sinh(T(val))), std::sinh(val));
    }
    void test_sqrt()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::sqrt(T(val))), std::sqrt(val));
    }
    void test_tan()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::tan(T(val))), std::tan(val));
    }
    void test_tanh()
    {
        value_type val(0);
        EXPECT_EQ(extract(xsimd::tanh(T(val))), std::tanh(val));
    }
    void test_tgamma()
    {
        value_type val(2);
        EXPECT_EQ(extract(xsimd::tgamma(T(val))), std::tgamma(val));
    }
    void test_trunc()
    {
        value_type val(2.1);
        EXPECT_EQ(extract(xsimd::trunc(T(val))), std::trunc(val));
    }
};

using FloatTypes = ::testing::Types<float, double
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE
                                    ,
                                    xsimd::batch<float>
#if defined(XSIMD_WITH_NEON) && !defined(XSIMD_WITH_NEON64)
                                    ,
                                    xsimd::batch<double>
#endif
#endif
                                    >;
TYPED_TEST_SUITE(xsimd_api_float_types_functions, FloatTypes);

TYPED_TEST(xsimd_api_float_types_functions, acos)
{
    this->test_acos();
}

TYPED_TEST(xsimd_api_float_types_functions, acosh)
{
    this->test_acosh();
}

TYPED_TEST(xsimd_api_float_types_functions, asin)
{
    this->test_asin();
}

TYPED_TEST(xsimd_api_float_types_functions, asinh)
{
    this->test_asinh();
}

TYPED_TEST(xsimd_api_float_types_functions, atan)
{
    this->test_atan();
}

TYPED_TEST(xsimd_api_float_types_functions, atan2)
{
    this->test_atan2();
}

TYPED_TEST(xsimd_api_float_types_functions, atanh)
{
    this->test_atanh();
}

TYPED_TEST(xsimd_api_float_types_functions, cbrt)
{
    this->test_cbrt();
}

TYPED_TEST(xsimd_api_float_types_functions, ceil)
{
    this->test_ceil();
}

TYPED_TEST(xsimd_api_float_types_functions, copysign)
{
    this->test_copysign();
}

TYPED_TEST(xsimd_api_float_types_functions, cos)
{
    this->test_cos();
}

TYPED_TEST(xsimd_api_float_types_functions, cosh)
{
    this->test_cosh();
}

TYPED_TEST(xsimd_api_float_types_functions, exp)
{
    this->test_exp();
}

TYPED_TEST(xsimd_api_float_types_functions, exp10)
{
    this->test_exp10();
}

TYPED_TEST(xsimd_api_float_types_functions, exp2)
{
    this->test_exp2();
}

TYPED_TEST(xsimd_api_float_types_functions, expm1)
{
    this->test_expm1();
}

TYPED_TEST(xsimd_api_float_types_functions, erf)
{
    this->test_erf();
}

TYPED_TEST(xsimd_api_float_types_functions, erfc)
{
    this->test_erfc();
}

TYPED_TEST(xsimd_api_float_types_functions, fabs)
{
    this->test_fabs();
}

TYPED_TEST(xsimd_api_float_types_functions, fdim)
{
    this->test_fdim();
}

TYPED_TEST(xsimd_api_float_types_functions, floor)
{
    this->test_floor();
}

TYPED_TEST(xsimd_api_float_types_functions, fmax)
{
    this->test_fmax();
}

TYPED_TEST(xsimd_api_float_types_functions, fmin)
{
    this->test_fmin();
}

TYPED_TEST(xsimd_api_float_types_functions, fmod)
{
    this->test_fmod();
}
TYPED_TEST(xsimd_api_float_types_functions, frexp)
{
    this->test_frexp();
}
TYPED_TEST(xsimd_api_float_types_functions, hypot)
{
    this->test_hypot();
}
TYPED_TEST(xsimd_api_float_types_functions, is_even)
{
    this->test_is_even();
}
TYPED_TEST(xsimd_api_float_types_functions, is_flint)
{
    this->test_is_flint();
}
TYPED_TEST(xsimd_api_float_types_functions, is_odd)
{
    this->test_is_odd();
}
TYPED_TEST(xsimd_api_float_types_functions, isinf)
{
    this->test_isinf();
}
TYPED_TEST(xsimd_api_float_types_functions, isfinite)
{
    this->test_isfinite();
}
TYPED_TEST(xsimd_api_float_types_functions, isnan)
{
    this->test_isnan();
}
TYPED_TEST(xsimd_api_float_types_functions, ldexp)
{
    this->test_ldexp();
}
TYPED_TEST(xsimd_api_float_types_functions, lgamma)
{
    this->test_lgamma();
}

TYPED_TEST(xsimd_api_float_types_functions, log)
{
    this->test_log();
}

TYPED_TEST(xsimd_api_float_types_functions, log2)
{
    this->test_log2();
}

TYPED_TEST(xsimd_api_float_types_functions, log10)
{
    this->test_log10();
}

TYPED_TEST(xsimd_api_float_types_functions, log1p)
{
    this->test_log1p();
}

TYPED_TEST(xsimd_api_float_types_functions, nearbyint)
{
    this->test_nearbyint();
}

TYPED_TEST(xsimd_api_float_types_functions, nearbyint_as_int)
{
    this->test_nearbyint_as_int();
}

TYPED_TEST(xsimd_api_float_types_functions, nextafter)
{
    this->test_nextafter();
}

TYPED_TEST(xsimd_api_float_types_functions, polar)
{
    this->test_polar();
}

TYPED_TEST(xsimd_api_float_types_functions, pow)
{
    this->test_pow();
}

TYPED_TEST(xsimd_api_float_types_functions, reciprocal)
{
    this->test_reciprocal();
}

TYPED_TEST(xsimd_api_float_types_functions, rint)
{
    this->test_rint();
}

TYPED_TEST(xsimd_api_float_types_functions, round)
{
    this->test_round();
}

TYPED_TEST(xsimd_api_float_types_functions, rsqrt)
{
    this->test_rsqrt();
}

TYPED_TEST(xsimd_api_float_types_functions, sin)
{
    this->test_sin();
}

TYPED_TEST(xsimd_api_float_types_functions, sincos)
{
    this->test_sincos();
}

TYPED_TEST(xsimd_api_float_types_functions, sinh)
{
    this->test_sinh();
}

TYPED_TEST(xsimd_api_float_types_functions, sqrt)
{
    this->test_sqrt();
}

TYPED_TEST(xsimd_api_float_types_functions, tan)
{
    this->test_tan();
}

TYPED_TEST(xsimd_api_float_types_functions, tanh)
{
    this->test_tanh();
}

TYPED_TEST(xsimd_api_float_types_functions, tgamma)
{
    this->test_tgamma();
}

TYPED_TEST(xsimd_api_float_types_functions, trunc)
{
    this->test_trunc();
}

/*
 * Functions that apply on complex and floating point types only
 */

template <typename T>
class xsimd_api_complex_types_functions : public ::testing::Test
{
    using value_type = typename scalar_type<T>::type;

public:
    void test_arg()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::arg(T(val))), std::arg(val));
    }

    void test_conj()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::conj(T(val))), std::conj(val));
    }

    void test_norm()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::norm(T(val))), std::norm(val));
    }

    void test_proj()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::proj(T(val))), std::proj(val));
    }
};

using ComplexTypes = ::testing::Types<float, double, std::complex<float>, std::complex<double>
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE
                                      ,
                                      xsimd::batch<float>, xsimd::batch<std::complex<float>>
#if defined(XSIMD_WITH_NEON) && !defined(XSIMD_WITH_NEON64)
                                      ,
                                      xsimd::batch<double>, xsimd::batch<std::complex<double>>
#endif
#endif
                                      >;
TYPED_TEST_SUITE(xsimd_api_complex_types_functions, ComplexTypes);

TYPED_TEST(xsimd_api_complex_types_functions, arg)
{
    this->test_arg();
}

TYPED_TEST(xsimd_api_complex_types_functions, conj)
{
    this->test_conj();
}

TYPED_TEST(xsimd_api_complex_types_functions, norm)
{
    this->test_norm();
}

TYPED_TEST(xsimd_api_complex_types_functions, proj)
{
    this->test_proj();
}

/*
 * Functions that apply on all signed types
 */
template <typename T>
class xsimd_api_all_signed_types_functions : public ::testing::Test
{
    using value_type = typename scalar_type<T>::type;

public:
    void test_abs()
    {
        value_type val(-1);
        EXPECT_EQ(extract(xsimd::abs(T(val))), std::abs(val));
    }

    void test_fnms()
    {
        value_type val0(1);
        value_type val1(3);
        value_type val2(5);
        EXPECT_EQ(extract(xsimd::fnms(T(val0), T(val1), T(val2))), -(val0 * val1) - val2);
    }

    void test_neg()
    {
        value_type val(-1);
        EXPECT_EQ(extract(xsimd::neg(T(val))), -val);
    }
};

using AllSignedTypes = ::testing::Types<
    signed char, short, int, long, float, double,
    std::complex<float>, std::complex<double>
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE
    ,
    xsimd::batch<signed char>, xsimd::batch<short>, xsimd::batch<int>, xsimd::batch<long>, xsimd::batch<float>, xsimd::batch<std::complex<float>>
#if defined(XSIMD_WITH_NEON) && !defined(XSIMD_WITH_NEON64)
    ,
    xsimd::batch<double>, xsimd::batch<std::complex<double>>
#endif
#endif
    >;
TYPED_TEST_SUITE(xsimd_api_all_signed_types_functions, AllSignedTypes);

TYPED_TEST(xsimd_api_all_signed_types_functions, abs)
{
    this->test_abs();
}
TYPED_TEST(xsimd_api_all_signed_types_functions, fnms)
{
    this->test_fnms();
}
TYPED_TEST(xsimd_api_all_signed_types_functions, neg)
{
    this->test_neg();
}

/*
 * Functions that apply on all types
 */

template <typename T>
class xsimd_api_all_types_functions : public ::testing::Test
{
    using value_type = typename scalar_type<T>::type;

public:
    void test_add()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::add(T(val0), T(val1))), val0 + val1);
    }

    void test_div()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::div(T(val0), T(val1))), val0 / val1);
    }

    void test_eq()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::eq(T(val0), T(val1))), val0 == val1);
    }

    void test_fma()
    {
        value_type val0(1);
        value_type val1(3);
        value_type val2(5);
        EXPECT_EQ(extract(xsimd::fma(T(val0), T(val1), T(val2))), val0 * val1 + val2);
    }

    void test_fms()
    {
        value_type val0(1);
        value_type val1(5);
        value_type val2(3);
        EXPECT_EQ(extract(xsimd::fms(T(val0), T(val1), T(val2))), val0 * val1 - val2);
    }

    void test_fnma()
    {
        value_type val0(1);
        value_type val1(3);
        value_type val2(5);
        EXPECT_EQ(extract(xsimd::fnma(T(val0), T(val1), T(val2))), -(val0 * val1) + val2);
    }

    void test_mul()
    {
        value_type val0(2);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::mul(T(val0), T(val1))), val0 * val1);
    }
    void test_neq()
    {
        value_type val0(1);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::neq(T(val0), T(val1))), val0 != val1);
    }
    void test_pos()
    {
        value_type val(1);
        EXPECT_EQ(extract(xsimd::pos(T(val))), +val);
    }
    void test_select()
    {
        value_type val0(2);
        value_type val1(3);
        EXPECT_EQ(extract(xsimd::select(T(val0) != T(val1), T(val0), T(val1))), val0 != val1 ? val0 : val1);
    }
    void test_sub()
    {
        value_type val0(3);
        value_type val1(2);
        EXPECT_EQ(extract(xsimd::sub(T(val0), T(val1))), val0 - val1);
    }
};

using AllTypes = ::testing::Types<
    char, unsigned char, signed char, short, unsigned short, int, unsigned int, long, unsigned long, float, double,
    std::complex<float>, std::complex<double>
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE
    ,
    xsimd::batch<char>, xsimd::batch<unsigned char>, xsimd::batch<signed char>, xsimd::batch<short>, xsimd::batch<unsigned short>, xsimd::batch<int>, xsimd::batch<unsigned int>, xsimd::batch<long>, xsimd::batch<unsigned long>, xsimd::batch<float>, xsimd::batch<std::complex<float>>
#if defined(XSIMD_WITH_NEON) && !defined(XSIMD_WITH_NEON64)
    ,
    xsimd::batch<double>, xsimd::batch<std::complex<double>>
#endif
#endif
    >;
TYPED_TEST_SUITE(xsimd_api_all_types_functions, AllTypes);

TYPED_TEST(xsimd_api_all_types_functions, add)
{
    this->test_add();
}

TYPED_TEST(xsimd_api_all_types_functions, div)
{
    this->test_div();
}

TYPED_TEST(xsimd_api_all_types_functions, eq)
{
    this->test_eq();
}

TYPED_TEST(xsimd_api_all_types_functions, fma)
{
    this->test_fma();
}

TYPED_TEST(xsimd_api_all_types_functions, fms)
{
    this->test_fms();
}

TYPED_TEST(xsimd_api_all_types_functions, fnma)
{
    this->test_fnma();
}

TYPED_TEST(xsimd_api_all_types_functions, mul)
{
    this->test_mul();
}

TYPED_TEST(xsimd_api_all_types_functions, neq)
{
    this->test_neq();
}

TYPED_TEST(xsimd_api_all_types_functions, pos)
{
    this->test_pos();
}
TYPED_TEST(xsimd_api_all_types_functions, select)
{
    this->test_select();
}
TYPED_TEST(xsimd_api_all_types_functions, sub)
{
    this->test_sub();
}

/*
 * Functions that apply only to floating point types
 */
template <typename T>
class xsimd_api_all_floating_point_types_functions : public ::testing::Test
{
    using value_type = typename scalar_type<T>::type;

public:
    void test_neq_nan()
    {
        value_type valNaN(std::numeric_limits<value_type>::signaling_NaN());
        value_type val1(1.0);
        EXPECT_EQ(extract(xsimd::neq(T(valNaN), T(val1))), valNaN != val1);
    }
};

using AllFloatingPointTypes = ::testing::Types<
    float, double,
    std::complex<float>, std::complex<double>
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE
    ,
    xsimd::batch<float>, xsimd::batch<std::complex<float>>
#if defined(XSIMD_WITH_NEON) && !defined(XSIMD_WITH_NEON64)
    ,
    xsimd::batch<double>, xsimd::batch<std::complex<double>>
#endif
#endif
    >;

TYPED_TEST_SUITE(xsimd_api_all_floating_point_types_functions, AllFloatingPointTypes);
TYPED_TEST(xsimd_api_all_floating_point_types_functions, neq_nan)
{
    this->test_neq_nan();
}
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

#include "xsimd/xsimd.hpp"
#ifndef XSIMD_NO_SUPPORTED_ARCHITECTURE

#include "test_utils.hpp"
#include "xsimd/xsimd.hpp"

template <class B>
class complex_trigonometric_test : public testing::Test
{
protected:
    using batch_type = B;
    using real_batch_type = typename B::real_batch;
    using value_type = typename B::value_type;
    using real_value_type = typename value_type::value_type;
    static constexpr size_t size = B::size;
    using vector_type = std::vector<value_type>;

    size_t nb_input;
    vector_type input;
    vector_type ainput;
    vector_type atan_input;
    vector_type expected;
    vector_type res;

    complex_trigonometric_test()
    {
        nb_input = size * 10000;
        input.resize(nb_input);
        ainput.resize(nb_input);
        atan_input.resize(nb_input);
        for (size_t i = 0; i < nb_input; ++i)
        {
            input[i] = value_type(real_value_type(0.) + i * real_value_type(80.) / nb_input,
                                  real_value_type(0.1) + i * real_value_type(56.) / nb_input);
            ainput[i] = value_type(real_value_type(-1.) + real_value_type(2.) * i / nb_input,
                                   real_value_type(-1.1) + real_value_type(2.1) * i / nb_input);
            atan_input[i] = value_type(real_value_type(-10.) + i * real_value_type(20.) / nb_input,
                                       real_value_type(-9.) + i * real_value_type(21.) / nb_input);
        }
        expected.resize(nb_input);
        res.resize(nb_input);
    }

    void test_sin()
    {
        std::transform(input.cbegin(), input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::sin; return sin(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, input, i);
            out = sin(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("sin");
    }

    void test_cos()
    {
        std::transform(input.cbegin(), input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::cos; return cos(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, input, i);
            out = cos(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("cos");
    }

    void test_sincos()
    {
        vector_type expected2(nb_input), res2(nb_input);
        std::transform(input.cbegin(), input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::sin; return sin(v); });
        std::transform(input.cbegin(), input.cend(), expected2.begin(),
                       [](const value_type& v)
                       { using std::cos; return cos(v); });
        batch_type in, out1, out2;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, input, i);
            std::tie(out1, out2) = sincos(in);
            detail::store_batch(out1, res, i);
            detail::store_batch(out2, res2, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("sincos(sin)");
        diff = detail::get_nb_diff(res2, expected2);
        EXPECT_EQ(diff, 0) << print_function_name("sincos(cos)");
    }

    void test_tan()
    {
        test_conditional_tan<real_value_type>();
    }

    void test_asin()
    {
        std::transform(ainput.cbegin(), ainput.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::asin; return asin(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, ainput, i);
            out = asin(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("asin");
    }

    void test_acos()
    {
        std::transform(ainput.cbegin(), ainput.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::acos; return acos(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, ainput, i);
            out = acos(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("acos");
    }

    void test_atan()
    {
        std::transform(atan_input.cbegin(), atan_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::atan; return atan(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, atan_input, i);
            out = atan(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("atan");
    }

private:
    void test_tan_impl()
    {
        std::transform(input.cbegin(), input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::tan; return tan(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, input, i);
            out = tan(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("tan");
    }

    template <class T, typename std::enable_if<!std::is_same<T, float>::value, int>::type = 0>
    void test_conditional_tan()
    {
        test_tan_impl();
    }

    template <class T, typename std::enable_if<std::is_same<T, float>::value, int>::type = 0>
    void test_conditional_tan()
    {
#if (XSIMD_ARM_INSTR_SET >= XSIMD_ARM7_NEON_VERSION)
#if DEBUG_ACCURACY
        test_tan_impl();
#endif
#else
        test_tan_impl();
#endif
    }
};

TYPED_TEST_SUITE(complex_trigonometric_test, batch_complex_types, simd_test_names);

TYPED_TEST(complex_trigonometric_test, sin)
{
    this->test_sin();
}

TYPED_TEST(complex_trigonometric_test, cos)
{
    this->test_cos();
}

TYPED_TEST(complex_trigonometric_test, sincos)
{
    this->test_sincos();
}

TYPED_TEST(complex_trigonometric_test, tan)
{
    this->test_tan();
}

TYPED_TEST(complex_trigonometric_test, asin)
{
    this->test_asin();
}

TYPED_TEST(complex_trigonometric_test, acos)
{
    this->test_acos();
}

TYPED_TEST(complex_trigonometric_test, atan)
{
    this->test_atan();
}
#endif

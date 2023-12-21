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

template <class B>
class complex_exponential_test : public testing::Test
{
protected:
    using batch_type = B;
    using real_batch_type = typename B::real_batch;
    using value_type = typename B::value_type;
    using real_value_type = typename value_type::value_type;
    static constexpr size_t size = B::size;
    using vector_type = std::vector<value_type>;

    size_t nb_input;
    vector_type exp_input;
    vector_type huge_exp_input;
    vector_type log_input;
    vector_type expected;
    vector_type res;

    complex_exponential_test()
    {
        nb_input = 10000 * size;
        exp_input.resize(nb_input);
        huge_exp_input.resize(nb_input);
        log_input.resize(nb_input);
        for (size_t i = 0; i < nb_input; ++i)
        {
            exp_input[i] = value_type(real_value_type(-1.5) + i * real_value_type(3) / nb_input,
                                      real_value_type(-1.3) + i * real_value_type(2) / nb_input);
            huge_exp_input[i] = value_type(real_value_type(0), real_value_type(102.12) + i * real_value_type(100.) / nb_input);
            log_input[i] = value_type(real_value_type(0.001 + i * 100 / nb_input),
                                      real_value_type(0.002 + i * 110 / nb_input));
        }
        expected.resize(nb_input);
        res.resize(nb_input);
    }

    void test_exp()
    {
        std::transform(exp_input.cbegin(), exp_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::exp; return exp(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, exp_input, i);
            out = exp(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("exp");
    }

    void test_expm1()
    {
        std::transform(exp_input.cbegin(), exp_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using xsimd::expm1; return expm1(v); });

        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, exp_input, i);
            out = expm1(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("expm1");
    }

    void test_huge_exp()
    {
        std::transform(huge_exp_input.cbegin(), huge_exp_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::exp; return exp(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, huge_exp_input, i);
            out = exp(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("huge exp");
    }

    void test_log()
    {
        std::transform(log_input.cbegin(), log_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::log; return log(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, log_input, i);
            out = log(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("log");
    }

    void test_log2()
    {
        std::transform(log_input.cbegin(), log_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using xsimd::log2; return log2(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, log_input, i);
            out = log2(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("log2");
    }

    void test_log10()
    {
        std::transform(log_input.cbegin(), log_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::log10; return log10(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, log_input, i);
            out = log10(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("log10");
    }

    void test_log1p()
    {
        std::transform(log_input.cbegin(), log_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using xsimd::log1p; return log1p(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, log_input, i);
            out = log1p(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("log1p");
    }

    void test_sign()
    {
        std::transform(log_input.cbegin(), log_input.cend(), expected.begin(),
                       [](const value_type& v)
                       { using xsimd::sign; return sign(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, log_input, i);
            out = sign(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("sign");
    }
};

TYPED_TEST_SUITE(complex_exponential_test, batch_complex_types, simd_test_names);

TYPED_TEST(complex_exponential_test, exp)
{
    this->test_exp();
}

TYPED_TEST(complex_exponential_test, expm1)
{
    this->test_expm1();
}

TYPED_TEST(complex_exponential_test, huge_exp)
{
    this->test_huge_exp();
}

TYPED_TEST(complex_exponential_test, log)
{
    this->test_log();
}

TYPED_TEST(complex_exponential_test, log2)
{
    this->test_log2();
}

TYPED_TEST(complex_exponential_test, log10)
{
    this->test_log10();
}

TYPED_TEST(complex_exponential_test, log1p)
{
    this->test_log1p();
}

TYPED_TEST(complex_exponential_test, sign)
{
    this->test_sign();
}
#endif

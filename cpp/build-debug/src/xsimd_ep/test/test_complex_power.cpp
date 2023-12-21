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
class complex_power_test : public testing::Test
{
protected:
    using batch_type = B;
    using real_batch_type = typename B::real_batch;
    using value_type = typename B::value_type;
    using real_value_type = typename value_type::value_type;
    static constexpr size_t size = B::size;
    using vector_type = std::vector<value_type>;
    using real_vector_type = std::vector<real_value_type>;

    size_t nb_input;
    vector_type lhs_nn;
    vector_type lhs_pn;
    vector_type lhs_np;
    vector_type lhs_pp;
    vector_type rhs;
    vector_type expected;
    vector_type res;

    complex_power_test()
    {
        nb_input = 10000 * size;
        lhs_nn.resize(nb_input);
        lhs_pn.resize(nb_input);
        lhs_np.resize(nb_input);
        lhs_pp.resize(nb_input);
        rhs.resize(nb_input);
        for (size_t i = 0; i < nb_input; ++i)
        {
            real_value_type real = (real_value_type(i) / 4 + real_value_type(1.2) * std::sqrt(real_value_type(i + 0.25))) / 100;
            real_value_type imag = (real_value_type(i) / 7 + real_value_type(1.7) * std::sqrt(real_value_type(i + 0.37))) / 100;
            lhs_nn[i] = value_type(-real, -imag);
            lhs_pn[i] = value_type(real, -imag);
            lhs_np[i] = value_type(-real, imag);
            lhs_pp[i] = value_type(real, imag);
            rhs[i] = value_type(real_value_type(10.2) / (i + 2) + real_value_type(0.25),
                                real_value_type(9.1) / (i + 3) + real_value_type(0.45));
        }
        expected.resize(nb_input);
        res.resize(nb_input);
    }

    void test_abs()
    {
        real_vector_type real_expected(nb_input), real_res(nb_input);
        std::transform(lhs_np.cbegin(), lhs_np.cend(), real_expected.begin(),
                       [](const value_type& v)
                       { using std::abs; return abs(v); });
        batch_type in;
        real_batch_type out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, lhs_np, i);
            out = abs(in);
            detail::store_batch(out, real_res, i);
        }
        size_t diff = detail::get_nb_diff(real_res, real_expected);
        EXPECT_EQ(diff, 0) << print_function_name("abs");
    }

    void test_arg()
    {
        real_vector_type real_expected(nb_input), real_res(nb_input);
        std::transform(lhs_np.cbegin(), lhs_np.cend(), real_expected.begin(),
                       [](const value_type& v)
                       { using std::arg; return arg(v); });
        batch_type in;
        real_batch_type out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, lhs_np, i);
            out = arg(in);
            detail::store_batch(out, real_res, i);
        }
        size_t diff = detail::get_nb_diff(real_res, real_expected);
        EXPECT_EQ(diff, 0) << print_function_name("arg");
    }

    void test_pow()
    {
        test_conditional_pow<real_value_type>();
    }

    void test_sqrt_nn()
    {
        std::transform(lhs_nn.cbegin(), lhs_nn.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::sqrt; return sqrt(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, lhs_nn, i);
            out = sqrt(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("sqrt_nn");
    }

    void test_sqrt_pn()
    {
        std::transform(lhs_pn.cbegin(), lhs_pn.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::sqrt; return sqrt(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, lhs_pn, i);
            out = sqrt(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("sqrt_pn");
    }

    void test_sqrt_np()
    {
        std::transform(lhs_np.cbegin(), lhs_np.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::sqrt; return sqrt(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, lhs_np, i);
            out = sqrt(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("sqrt_nn");
    }

    void test_sqrt_pp()
    {
        std::transform(lhs_pp.cbegin(), lhs_pp.cend(), expected.begin(),
                       [](const value_type& v)
                       { using std::sqrt; return sqrt(v); });
        batch_type in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(in, lhs_pp, i);
            out = sqrt(in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("sqrt_pp");
    }

private:
    void test_pow_impl()
    {
        std::transform(lhs_np.cbegin(), lhs_np.cend(), rhs.cbegin(), expected.begin(),
                       [](const value_type& l, const value_type& r)
                       { using std::pow; return pow(l, r); });
        batch_type lhs_in, rhs_in, out;
        for (size_t i = 0; i < nb_input; i += size)
        {
            detail::load_batch(lhs_in, lhs_np, i);
            detail::load_batch(rhs_in, rhs, i);
            out = pow(lhs_in, rhs_in);
            detail::store_batch(out, res, i);
        }
        size_t diff = detail::get_nb_diff(res, expected);
        EXPECT_EQ(diff, 0) << print_function_name("pow");
    }

    template <class T, typename std::enable_if<!std::is_same<T, float>::value, int>::type = 0>
    void test_conditional_pow()
    {
        test_pow_impl();
    }

    template <class T, typename std::enable_if<std::is_same<T, float>::value, int>::type = 0>
    void test_conditional_pow()
    {

#if (XSIMD_X86_INSTR_SET >= XSIMD_X86_AVX512_VERSION) || (XSIMD_ARM_INSTR_SET >= XSIMD_ARM7_NEON_VERSION)
#if DEBUG_ACCURACY
        test_pow_impl();
#endif
#else
        test_pow_impl();
#endif
    }
};

TYPED_TEST_SUITE(complex_power_test, batch_complex_types, simd_test_names);

TYPED_TEST(complex_power_test, abs)
{
    this->test_abs();
}

TYPED_TEST(complex_power_test, arg)
{
    this->test_arg();
}

TYPED_TEST(complex_power_test, pow)
{
    this->test_pow();
}

TYPED_TEST(complex_power_test, sqrt_nn)
{
    this->test_sqrt_nn();
}

TYPED_TEST(complex_power_test, sqrt_pn)
{
    this->test_sqrt_pn();
}

TYPED_TEST(complex_power_test, sqrt_np)
{
    this->test_sqrt_np();
}

TYPED_TEST(complex_power_test, sqrt_pp)
{
    this->test_sqrt_pp();
}
#endif

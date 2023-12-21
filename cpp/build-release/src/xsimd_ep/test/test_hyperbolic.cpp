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
class hyperbolic_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;
    using vector_type = std::vector<value_type>;

    size_t nb_input;
    vector_type input;
    vector_type acosh_input;
    vector_type atanh_input;
    vector_type expected;
    vector_type res;

    hyperbolic_test()
    {
        nb_input = size * 10000;
        input.resize(nb_input);
        acosh_input.resize(nb_input);
        atanh_input.resize(nb_input);
        for (size_t i = 0; i < nb_input; ++i)
        {
            input[i] = value_type(-1.5) + i * value_type(3) / nb_input;
            acosh_input[i] = value_type(1.) + i * value_type(3) / nb_input;
            atanh_input[i] = value_type(-0.95) + i * value_type(1.9) / nb_input;
        }
        expected.resize(nb_input);
        res.resize(nb_input);
    }

    void test_hyperbolic_functions()
    {
        // sinh
        {
            std::transform(input.cbegin(), input.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::sinh(v); });
            batch_type in, out;
            for (size_t i = 0; i < nb_input; i += size)
            {
                detail::load_batch(in, input, i);
                out = sinh(in);
                detail::store_batch(out, res, i);
            }
            size_t diff = detail::get_nb_diff(res, expected);
            EXPECT_EQ(diff, 0) << print_function_name("sinh");
        }
        // cosh
        {
            std::transform(input.cbegin(), input.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::cosh(v); });
            batch_type in, out;
            for (size_t i = 0; i < nb_input; i += size)
            {
                detail::load_batch(in, input, i);
                out = cosh(in);
                detail::store_batch(out, res, i);
            }
            size_t diff = detail::get_nb_diff(res, expected);
            EXPECT_EQ(diff, 0) << print_function_name("cosh");
        }
        // tanh
        {
            std::transform(input.cbegin(), input.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::tanh(v); });
            batch_type in, out;
            for (size_t i = 0; i < nb_input; i += size)
            {
                detail::load_batch(in, input, i);
                out = tanh(in);
                detail::store_batch(out, res, i);
            }
            size_t diff = detail::get_nb_diff(res, expected);
            EXPECT_EQ(diff, 0) << print_function_name("tanh");
        }
    }

    void test_reciprocal_functions()
    {
        // asinh
        {
            std::transform(input.cbegin(), input.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::asinh(v); });
            batch_type in, out;
            for (size_t i = 0; i < nb_input; i += size)
            {
                detail::load_batch(in, input, i);
                out = asinh(in);
                detail::store_batch(out, res, i);
            }
            size_t diff = detail::get_nb_diff(res, expected);
            EXPECT_EQ(diff, 0) << print_function_name("asinh");
        }
        // acosh
        {
            std::transform(acosh_input.cbegin(), acosh_input.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::acosh(v); });
            batch_type in, out;
            for (size_t i = 0; i < nb_input; i += size)
            {
                detail::load_batch(in, acosh_input, i);
                out = acosh(in);
                detail::store_batch(out, res, i);
            }
            size_t diff = detail::get_nb_diff(res, expected);
            EXPECT_EQ(diff, 0) << print_function_name("acosh");
        }
        // atanh
        {
            std::transform(atanh_input.cbegin(), atanh_input.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::atanh(v); });
            batch_type in, out;
            for (size_t i = 0; i < nb_input; i += size)
            {
                detail::load_batch(in, atanh_input, i);
                out = atanh(in);
                detail::store_batch(out, res, i);
            }
            size_t diff = detail::get_nb_diff(res, expected);
            EXPECT_EQ(diff, 0) << print_function_name("atanh");
        }
    }
};

TYPED_TEST_SUITE(hyperbolic_test, batch_float_types, simd_test_names);

TYPED_TEST(hyperbolic_test, hyperbolic)
{
    this->test_hyperbolic_functions();
}

TYPED_TEST(hyperbolic_test, reciprocal)
{
    this->test_reciprocal_functions();
}
#endif

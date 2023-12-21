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
class traits_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;

    void test_simd_traits()
    {
        using traits_type = xsimd::simd_traits<value_type>;
        EXPECT_EQ(traits_type::size, batch_type::size);
        constexpr bool same_type = std::is_same<B, typename traits_type::type>::value;
        EXPECT_TRUE(same_type);
        using batch_bool_type = xsimd::batch_bool<value_type>;
        constexpr bool same_bool_type = std::is_same<batch_bool_type, typename traits_type::bool_type>::value;
        EXPECT_TRUE(same_bool_type);

        using vector_traits_type = xsimd::simd_traits<std::vector<value_type>>;
        EXPECT_EQ(vector_traits_type::size, 1);
        constexpr bool vec_same_type = std::is_same<typename vector_traits_type::type, std::vector<value_type>>::value;
        EXPECT_TRUE(vec_same_type);
    }

    void test_revert_simd_traits()
    {
        using traits_type = xsimd::revert_simd_traits<batch_type>;
        EXPECT_EQ(traits_type::size, batch_type::size);
        constexpr bool same_type = std::is_same<value_type, typename traits_type::type>::value;
        EXPECT_TRUE(same_type);
    }

    void test_simd_return_type()
    {
        using rtype1 = xsimd::simd_return_type<value_type, float>;
        constexpr bool res1 = std::is_same<rtype1, xsimd::batch<float>>::value;
        EXPECT_TRUE(res1);

        using rtype2 = xsimd::simd_return_type<bool, value_type>;
        constexpr bool res2 = std::is_same<rtype2, xsimd::batch_bool<value_type>>::value;
        EXPECT_TRUE(res2);
    }
};

TYPED_TEST_SUITE(traits_test, batch_types, simd_test_names);

TYPED_TEST(traits_test, simd_traits)
{
    this->test_simd_traits();
}

TYPED_TEST(traits_test, revert_simd_traits)
{
    this->test_revert_simd_traits();
}

TYPED_TEST(traits_test, simd_return_type)
{
    this->test_simd_return_type();
}

template <class B>
class complex_traits_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;

    void test_simd_traits()
    {
        using traits_type = xsimd::simd_traits<value_type>;
        EXPECT_EQ(traits_type::size, batch_type::size);
        constexpr bool same_type = std::is_same<B, typename traits_type::type>::value;
        EXPECT_TRUE(same_type);
        using batch_bool_type = xsimd::batch_bool<typename value_type::value_type>;
        constexpr bool same_bool_type = std::is_same<batch_bool_type, typename traits_type::bool_type>::value;
        EXPECT_TRUE(same_bool_type);

        using vector_traits_type = xsimd::simd_traits<std::vector<value_type>>;
        EXPECT_EQ(vector_traits_type::size, 1);
        constexpr bool vec_same_type = std::is_same<typename vector_traits_type::type, std::vector<value_type>>::value;
        EXPECT_TRUE(vec_same_type);
    }

    void test_revert_simd_traits()
    {
        using traits_type = xsimd::revert_simd_traits<batch_type>;
        EXPECT_EQ(traits_type::size, batch_type::size);
        constexpr bool same_type = std::is_same<value_type, typename traits_type::type>::value;
        EXPECT_TRUE(same_type);
    }

    void test_simd_return_type()
    {
        using rtype1 = xsimd::simd_return_type<value_type, float>;
        constexpr bool res1 = std::is_same<rtype1, xsimd::batch<std::complex<float>>>::value;
        EXPECT_TRUE(res1);

        using rtype2 = xsimd::simd_return_type<bool, value_type>;
        constexpr bool res2 = std::is_same<rtype2, xsimd::batch_bool<typename value_type::value_type>>::value;
        EXPECT_TRUE(res2);
    }
};

TYPED_TEST_SUITE(complex_traits_test, batch_complex_types, simd_test_names);

TYPED_TEST(complex_traits_test, simd_traits)
{
    this->test_simd_traits();
}

TYPED_TEST(complex_traits_test, revert_simd_traits)
{
    this->test_revert_simd_traits();
}

TYPED_TEST(complex_traits_test, simd_return_type)
{
    this->test_simd_return_type();
}
#endif

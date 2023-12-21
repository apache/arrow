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

using namespace std::placeholders;

template <class B>
class constant_batch_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;
    using array_type = std::array<value_type, size>;
    using bool_array_type = std::array<bool, size>;
    using batch_bool_type = typename batch_type::batch_bool_type;

    struct generator
    {
        static constexpr value_type get(size_t index, size_t /*size*/)
        {
            return index % 2 ? 0 : 1;
        }
    };

    void test_init_from_generator() const
    {
        array_type expected;
        size_t i = 0;
        std::generate(expected.begin(), expected.end(),
                      [&i]()
                      { return generator::get(i++, size); });
        constexpr auto b = xsimd::make_batch_constant<batch_type, generator>();
        EXPECT_BATCH_EQ((batch_type)b, expected)
            << print_function_name("batch(value_type)");
    }

    struct arange
    {
        static constexpr value_type get(size_t index, size_t /*size*/)
        {
            return index;
        }
    };

    void test_init_from_generator_arange() const
    {
        array_type expected;
        size_t i = 0;
        std::generate(expected.begin(), expected.end(),
                      [&i]()
                      { return arange::get(i++, size); });
        constexpr auto b = xsimd::make_batch_constant<batch_type, arange>();
        EXPECT_BATCH_EQ((batch_type)b, expected)
            << print_function_name("batch(value_type)");
    }

    struct constant
    {
        static constexpr value_type get(size_t /*index*/, size_t /*size*/)
        {
            return 3;
        }
    };

    void test_init_from_constant() const
    {
        array_type expected;
        std::fill(expected.begin(), expected.end(), constant::get(0, 0));
        constexpr auto b = xsimd::make_batch_constant<batch_type, constant>();
        EXPECT_BATCH_EQ((batch_type)b, expected)
            << print_function_name("batch(value_type)");
    }
};

TYPED_TEST_SUITE(constant_batch_test, batch_int_types, simd_test_names);

TYPED_TEST(constant_batch_test, init_from_generator)
{
    this->test_init_from_generator();
}

TYPED_TEST(constant_batch_test, init_from_generator_arange)
{
    this->test_init_from_generator_arange();
}

TYPED_TEST(constant_batch_test, init_from_constant)
{
    this->test_init_from_constant();
}

template <class B>
class constant_bool_batch_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;
    using array_type = std::array<value_type, size>;
    using bool_array_type = std::array<bool, size>;
    using batch_bool_type = typename batch_type::batch_bool_type;

    struct generator
    {
        static constexpr bool get(size_t index, size_t /*size*/)
        {
            return index % 2;
        }
    };

    void test_init_from_generator() const
    {
        bool_array_type expected;
        size_t i = 0;
        std::generate(expected.begin(), expected.end(),
                      [&i]()
                      { return generator::get(i++, size); });
        constexpr auto b = xsimd::make_batch_bool_constant<batch_type, generator>();
        EXPECT_BATCH_EQ((batch_bool_type)b, expected)
            << print_function_name("batch_bool_constant(value_type)");
    }

    struct split
    {
        static constexpr bool get(size_t index, size_t size)
        {
            return index < size / 2;
        }
    };

    void test_init_from_generator_split() const
    {
        bool_array_type expected;
        size_t i = 0;
        std::generate(expected.begin(), expected.end(),
                      [&i]()
                      { return split::get(i++, size); });
        constexpr auto b = xsimd::make_batch_bool_constant<batch_type, split>();
        EXPECT_BATCH_EQ((batch_bool_type)b, expected)
            << print_function_name("batch_bool_constant(value_type)");
    }
};

TYPED_TEST_SUITE(constant_bool_batch_test, batch_int_types, simd_test_names);

TYPED_TEST(constant_bool_batch_test, init_from_generator)
{
    this->test_init_from_generator();
}

TYPED_TEST(constant_bool_batch_test, init_from_generator_split)
{
    this->test_init_from_generator_split();
}
#endif

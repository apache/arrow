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

namespace xsimd
{
    template <typename T, std::size_t N>
    struct init_swizzle_base
    {
        using swizzle_vector_type = std::array<T, N>;
        swizzle_vector_type lhs_in, exped_reverse, exped_fill, exped_dup;

        template <int... Indices>
        std::vector<swizzle_vector_type> create_swizzle_vectors()
        {
            std::vector<swizzle_vector_type> vects;

            /* Generate input data */
            for (std::size_t i = 0; i < N; ++i)
            {
                lhs_in[i] = 2 * i + 1;
            }
            vects.push_back(std::move(lhs_in));

            /* Expected reversed data */
            for (std::size_t i = 0; i < N; ++i)
            {
                exped_reverse[i] = lhs_in[N - 1 - i];
                exped_fill[i] = lhs_in[N - 1];
                exped_dup[i] = lhs_in[2 * (i / 2)];
            }
            vects.push_back(std::move(exped_reverse));
            vects.push_back(std::move(exped_fill));
            vects.push_back(std::move(exped_dup));

            return vects;
        }
    };
}

template <class T>
struct Reversor
{
    static constexpr T get(T i, T n)
    {
        return n - 1 - i;
    }
};

template <class T>
struct Last
{
    static constexpr T get(T, T n)
    {
        return n - 1;
    }
};

template <class T>
struct Dup
{
    static constexpr T get(T i, T)
    {
        return 2 * (i / 2);
    }
};

template <class T>
struct as_index
{
    using type = xsimd::as_unsigned_integer_t<T>;
};

template <class T, class A>
struct as_index<xsimd::batch<std::complex<T>, A>> : as_index<xsimd::batch<T, A>>
{
};

template <class B>
class insert_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;

    insert_test()
    {
        std::cout << "insert tests" << std::endl;
    }

    void insert_first()
    {
        value_type fill_value = 0;
        value_type sentinel_value = 1;
        batch_type v(fill_value);
        batch_type w = insert(v, sentinel_value, ::xsimd::index<0>());
        std::array<value_type, batch_type::size> data;
        w.store_unaligned(data.data());
        EXPECT_SCALAR_EQ(data.front(), sentinel_value);
        for (size_t i = 1; i < batch_type::size; ++i)
            EXPECT_SCALAR_EQ(data[i], fill_value);
    }

    void insert_last()
    {
        value_type fill_value = 0;
        value_type sentinel_value = 1;
        batch_type v(fill_value);
        batch_type w = insert(v, sentinel_value, ::xsimd::index<batch_type::size - 1>());
        std::array<value_type, batch_type::size> data;
        w.store_unaligned(data.data());
        for (size_t i = 0; i < batch_type::size - 1; ++i)
            EXPECT_SCALAR_EQ(data[i], fill_value);
        EXPECT_SCALAR_EQ(data.back(), sentinel_value);
    }
};

TYPED_TEST_SUITE(insert_test, batch_types, simd_test_names);

TYPED_TEST(insert_test, insert_first)
{
    this->insert_first();
}

TYPED_TEST(insert_test, insert_last)
{
    this->insert_last();
}

template <class B>
class swizzle_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;

    swizzle_test()
    {
        std::cout << "swizzle tests" << std::endl;
    }

    void swizzle_reverse()
    {
        xsimd::init_swizzle_base<value_type, size> swizzle_base;
        auto swizzle_vecs = swizzle_base.create_swizzle_vectors();
        auto v_lhs = swizzle_vecs[0];
        auto v_exped = swizzle_vecs[1];

        B b_lhs = B::load_unaligned(v_lhs.data());
        B b_exped = B::load_unaligned(v_exped.data());

        using index_type = typename as_index<batch_type>::type;
        B b_res = xsimd::swizzle(b_lhs, xsimd::make_batch_constant<index_type, Reversor<typename index_type::value_type>>());
        EXPECT_BATCH_EQ(b_res, b_exped) << print_function_name("swizzle reverse test");
    }

    void swizzle_fill()
    {
        xsimd::init_swizzle_base<value_type, size> swizzle_base;
        auto swizzle_vecs = swizzle_base.create_swizzle_vectors();
        auto v_lhs = swizzle_vecs[0];
        auto v_exped = swizzle_vecs[2];

        B b_lhs = B::load_unaligned(v_lhs.data());
        B b_exped = B::load_unaligned(v_exped.data());

        using index_type = typename as_index<batch_type>::type;
        B b_res = xsimd::swizzle(b_lhs, xsimd::make_batch_constant<index_type, Last<typename index_type::value_type>>());
        EXPECT_BATCH_EQ(b_res, b_exped) << print_function_name("swizzle fill test");
    }

    void swizzle_dup()
    {
        xsimd::init_swizzle_base<value_type, size> swizzle_base;
        auto swizzle_vecs = swizzle_base.create_swizzle_vectors();
        auto v_lhs = swizzle_vecs[0];
        auto v_exped = swizzle_vecs[3];

        B b_lhs = B::load_unaligned(v_lhs.data());
        B b_exped = B::load_unaligned(v_exped.data());

        using index_type = typename as_index<batch_type>::type;
        B b_res = xsimd::swizzle(b_lhs, xsimd::make_batch_constant<index_type, Dup<typename index_type::value_type>>());
        EXPECT_BATCH_EQ(b_res, b_exped) << print_function_name("swizzle dup test");
    }
};

TYPED_TEST_SUITE(swizzle_test, batch_swizzle_types, simd_test_names);

TYPED_TEST(swizzle_test, swizzle_reverse)
{
    this->swizzle_reverse();
}

TYPED_TEST(swizzle_test, swizzle_fill)
{
    this->swizzle_fill();
}

TYPED_TEST(swizzle_test, swizzle_dup)
{
    this->swizzle_dup();
}

#endif

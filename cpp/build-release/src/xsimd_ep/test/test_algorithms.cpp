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
#include "xsimd/stl/algorithms.hpp"
#include <numeric>

struct binary_functor
{
    template <class T>
    T operator()(const T& a, const T& b) const
    {
        return a + b;
    }
};

struct unary_functor
{
    template <class T>
    T operator()(const T& a) const
    {
        return -a;
    }
};

template <typename T>
class algorithms : public testing::Test
{
public:
    using vector = std::vector<T>;
    using aligned_vector = std::vector<T, xsimd::aligned_allocator<T>>;
};

#if XSIMD_WITH_NEON && !XSIMD_WITH_NEON64
using algorithms_types = ::testing::Types<float, std::complex<float>>;
#else
using algorithms_types = ::testing::Types<float, double, std::complex<float>, std::complex<double>>;
#endif

TYPED_TEST_SUITE(algorithms, algorithms_types);

TYPED_TEST(algorithms, binary_transform)
{
    using vector = typename TestFixture::vector;
    using aligned_vector = typename TestFixture::aligned_vector;

    vector expected(93);
    vector a(93, 123), b(93, 123), c(93);
    aligned_vector aa(93, 123), ba(93, 123), ca(93);

    std::transform(a.begin(), a.end(), b.begin(), expected.begin(),
                   binary_functor {});

    xsimd::transform(a.begin(), a.end(), b.begin(), c.begin(),
                     binary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), c.begin()) && expected.size() == c.size());
    std::fill(c.begin(), c.end(), -1); // erase

    xsimd::transform(aa.begin(), aa.end(), ba.begin(), c.begin(),
                     binary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), c.begin()) && expected.size() == c.size());
    std::fill(c.begin(), c.end(), -1); // erase

    xsimd::transform(aa.begin(), aa.end(), b.begin(), c.begin(),
                     binary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), c.begin()) && expected.size() == c.size());
    std::fill(c.begin(), c.end(), -1); // erase

    xsimd::transform(a.begin(), a.end(), ba.begin(), c.begin(),
                     binary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), c.begin()) && expected.size() == c.size());
    std::fill(c.begin(), c.end(), -1); // erase

    xsimd::transform(aa.begin(), aa.end(), ba.begin(), ca.begin(),
                     binary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), ca.begin()) && expected.size() == ca.size());
    std::fill(ca.begin(), ca.end(), -1); // erase

    xsimd::transform(aa.begin(), aa.end(), b.begin(), ca.begin(),
                     binary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), ca.begin()) && expected.size() == ca.size());
    std::fill(ca.begin(), ca.end(), -1); // erase

    xsimd::transform(a.begin(), a.end(), ba.begin(), ca.begin(),
                     binary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), ca.begin()) && expected.size() == ca.size());
    std::fill(ca.begin(), ca.end(), -1); // erase
}

TYPED_TEST(algorithms, unary_transform)
{
    using vector = typename TestFixture::vector;
    using aligned_vector = typename TestFixture::aligned_vector;

    vector expected(93);
    vector a(93, 123), c(93);
    aligned_vector aa(93, 123), ca(93);

    std::transform(a.begin(), a.end(), expected.begin(),
                   unary_functor {});

    xsimd::transform(a.begin(), a.end(), c.begin(),
                     unary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), c.begin()) && expected.size() == c.size());
    std::fill(c.begin(), c.end(), -1); // erase

    xsimd::transform(aa.begin(), aa.end(), c.begin(),
                     unary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), c.begin()) && expected.size() == c.size());
    std::fill(c.begin(), c.end(), -1); // erase

    xsimd::transform(a.begin(), a.end(), ca.begin(),
                     unary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), ca.begin()) && expected.size() == ca.size());
    std::fill(ca.begin(), ca.end(), -1); // erase

    xsimd::transform(aa.begin(), aa.end(), ca.begin(),
                     unary_functor {});
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), ca.begin()) && expected.size() == ca.size());
    std::fill(ca.begin(), ca.end(), -1); // erase
}

template <class T>
using test_allocator_type = xsimd::aligned_allocator<T>;

#if XSIMD_WITH_NEON && !XSIMD_WITH_NEON64
using test_value_type = float;
#else
using test_value_type = double;
#endif

class xsimd_reduce : public ::testing::Test
{
public:
    using aligned_vec_t = std::vector<test_value_type, test_allocator_type<test_value_type>>;

    static constexpr std::size_t num_elements = 4 * xsimd::batch<test_value_type>::size;
    static constexpr std::size_t small_num = xsimd::batch<test_value_type>::size - 1;

    aligned_vec_t vec = aligned_vec_t(num_elements, 123.);
    aligned_vec_t small_vec = aligned_vec_t(small_num, 42.);
    test_value_type init = 1337.;

    struct multiply
    {
        template <class T>
        T operator()(const T& a, const T& b) const
        {
            return a * b;
        }
    };
};

TEST_F(xsimd_reduce, unaligned_begin_unaligned_end)
{
    auto const begin = std::next(vec.begin());
    auto const end = std::prev(vec.end());

    EXPECT_EQ(std::accumulate(begin, end, init), xsimd::reduce(begin, end, init));

    if (small_vec.size() > 1)
    {
        auto const sbegin = std::next(small_vec.begin());
        auto const send = std::prev(small_vec.end());

        EXPECT_EQ(std::accumulate(sbegin, send, init), xsimd::reduce(sbegin, send, init));
    }
}

TEST_F(xsimd_reduce, unaligned_begin_aligned_end)
{
    auto const begin = std::next(vec.begin());
    auto const end = vec.end();

    EXPECT_EQ(std::accumulate(begin, end, init), xsimd::reduce(begin, end, init));

    if (small_vec.size() > 1)
    {
        auto const sbegin = std::next(small_vec.begin());
        auto const send = small_vec.end();

        EXPECT_EQ(std::accumulate(sbegin, send, init), xsimd::reduce(sbegin, send, init));
    }
}

TEST_F(xsimd_reduce, aligned_begin_unaligned_end)
{
    auto const begin = vec.begin();
    auto const end = std::prev(vec.end());

    EXPECT_EQ(std::accumulate(begin, end, init), xsimd::reduce(begin, end, init));

    if (small_vec.size() > 1)
    {
        auto const sbegin = small_vec.begin();
        auto const send = std::prev(small_vec.end());

        EXPECT_EQ(std::accumulate(sbegin, send, init), xsimd::reduce(sbegin, send, init));
    }
}

TEST_F(xsimd_reduce, aligned_begin_aligned_end)
{
    auto const begin = vec.begin();
    auto const end = vec.end();

    EXPECT_EQ(std::accumulate(begin, end, init), xsimd::reduce(begin, end, init));

    if (small_vec.size() > 1)
    {
        auto const sbegin = small_vec.begin();
        auto const send = small_vec.end();

        EXPECT_EQ(std::accumulate(sbegin, send, init), xsimd::reduce(sbegin, send, init));
    }
}

TEST_F(xsimd_reduce, using_custom_binary_function)
{
    auto const begin = vec.begin();
    auto const end = vec.end();

    if (std::is_same<aligned_vec_t::value_type, double>::value)
    {
        EXPECT_DOUBLE_EQ(std::accumulate(begin, end, init, multiply {}), xsimd::reduce(begin, end, init, multiply {}));
    }
    else
    {
        EXPECT_FLOAT_EQ(std::accumulate(begin, end, init, multiply {}), xsimd::reduce(begin, end, init, multiply {}));
    }

    if (small_vec.size() > 1)
    {
        auto const sbegin = small_vec.begin();
        auto const send = small_vec.end();

        if (std::is_same<aligned_vec_t::value_type, double>::value)
        {
            EXPECT_DOUBLE_EQ(std::accumulate(sbegin, send, init, multiply {}), xsimd::reduce(sbegin, send, init, multiply {}));
        }
        else
        {
            EXPECT_FLOAT_EQ(std::accumulate(sbegin, send, init, multiply {}), xsimd::reduce(sbegin, send, init, multiply {}));
        }
    }
}

#if XSIMD_X86_INSTR_SET > XSIMD_VERSION_NUMBER_NOT_AVAILABLE || XSIMD_ARM_INSTR_SET > XSIMD_VERSION_NUMBER_NOT_AVAILABLE
TEST(algorithms, iterator)
{
    std::vector<float, test_allocator_type<float>> a(10 * 16, 0.2), b(1000, 2.), c(1000, 3.);

    std::iota(a.begin(), a.end(), 0.f);
    std::vector<float> a_cpy(a.begin(), a.end());

    using batch_type = xsimd::batch<float>;
    auto begin = xsimd::aligned_iterator<batch_type>(&a[0]);
    auto end = xsimd::aligned_iterator<batch_type>(&a[0] + a.size());

    for (; begin != end; ++begin)
    {
        *begin = *begin / 2.f;
    }

    for (auto& el : a_cpy)
    {
        el /= 2.f;
    }

    EXPECT_TRUE(a.size() == a_cpy.size() && std::equal(a.begin(), a.end(), a_cpy.begin()));

    begin = xsimd::aligned_iterator<batch_type>(&a[0]);
    *begin = sin(*begin);

    for (std::size_t i = 0; i < batch_type::size; ++i)
    {
        EXPECT_NEAR(a[i], sinf(a_cpy[i]), 1e-6);
    }

#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
    std::vector<std::complex<double>, test_allocator_type<std::complex<double>>> ca(10 * 16, std::complex<double>(0.2));
    using cbatch_type = xsimd::batch_type < std::complex<double>;
    auto cbegin = xsimd::aligned_iterator<cbatch_type>(&ca[0]);
    auto cend = xsimd::aligned_iterator<cbatch_type>(&ca[0] + a.size());

    for (; cbegin != cend; ++cbegin)
    {
        *cbegin = (*cbegin + std::complex<double>(0, .3)) / 2.;
    }

    cbegin = xsimd::aligned_iterator<cbatch_type>(&ca[0]);
    *cbegin = sin(*cbegin);
    *cbegin = sqrt(*cbegin);
    auto real_part = abs(*(cbegin));
    (void)real_part;
#endif
}
#endif
#endif

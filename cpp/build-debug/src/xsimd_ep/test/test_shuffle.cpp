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

#include <numeric>

namespace
{
    template <typename T, std::size_t N>
    struct init_shuffle_base
    {
        using shuffle_vector_type = std::array<T, N>;
        shuffle_vector_type lhs_in, rhs_in, exp_lo, exp_hi;

        std::vector<shuffle_vector_type> create_vectors()
        {
            std::vector<shuffle_vector_type> vects;
            vects.reserve(4);

            constexpr size_t K = 128 / (sizeof(T) * 8);
            constexpr size_t P = N / K;

            /* Generate input data: lhs, rhs */
            for (size_t p = 0; p < P; ++p)
            {
                for (size_t i = 0; i < K; ++i)
                {
                    lhs_in[i + p * K] = 2 * i + 1;
                    rhs_in[i + p * K] = 2 * i + 2;
                }
            }
            vects.push_back(std::move(lhs_in));
            vects.push_back(std::move(rhs_in));

            /* Expected shuffle data */
            for (size_t p = 0; p < P; ++p)
            {
                for (size_t i = 0, j = 0; i < K / 2; ++i, j = j + 2)
                {
                    exp_lo[j + p * K] = lhs_in[i];
                    exp_hi[j + p * K] = lhs_in[i + K / 2];

                    exp_lo[j + 1 + p * K] = rhs_in[i];
                    exp_hi[j + 1 + p * K] = rhs_in[i + K / 2];
                }
            }
            vects.push_back(std::move(exp_lo));
            vects.push_back(std::move(exp_hi));

            return vects;
        }
    };
}

template <class B>
class shuffle_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;

    shuffle_test()
    {
        std::cout << "shuffle-128 test" << std::endl;
    }

    void shuffle_low_high()
    {
        init_shuffle_base<value_type, size> shuffle_base;
        auto shuffle_base_vecs = shuffle_base.create_vectors();
        auto v_lhs = shuffle_base_vecs[0];
        auto v_rhs = shuffle_base_vecs[1];
        auto v_exp_lo = shuffle_base_vecs[2];
        auto v_exp_hi = shuffle_base_vecs[3];

        B b_lhs = B::load_unaligned(v_lhs.data());
        B b_rhs = B::load_unaligned(v_rhs.data());
        B b_exp_lo = B::load_unaligned(v_exp_lo.data());
        B b_exp_hi = B::load_unaligned(v_exp_hi.data());

        B b_res_lo = xsimd::zip_lo(b_lhs, b_rhs);
        EXPECT_BATCH_EQ(b_res_lo, b_exp_lo) << print_function_name("zip low test");

        B b_res_hi = xsimd::zip_hi(b_lhs, b_rhs);
        EXPECT_BATCH_EQ(b_res_hi, b_exp_hi) << print_function_name("zip high test");
    }
};

TYPED_TEST_SUITE(shuffle_test, batch_types, simd_test_names);

TYPED_TEST(shuffle_test, shuffle_low_high)
{
    this->shuffle_low_high();
}

namespace
{
    template <typename T, std::size_t N>
    struct init_slide_base
    {
        using slide_vector_type = std::array<T, N>;
        slide_vector_type v_in,
            v_left0, v_left_full, v_left_half, v_left_above_half, v_left_below_half, v_left_one,
            v_right0, v_right_full, v_right_half, v_right_above_half, v_right_below_half, v_right_one;
        static constexpr unsigned full_slide = N * sizeof(T);
        static constexpr unsigned half_slide = full_slide / 2;
        static constexpr unsigned above_half_slide = half_slide + half_slide / 2;
        static constexpr unsigned below_half_slide = half_slide / 2;
        static constexpr bool activate_above_below_checks = above_half_slide / sizeof(T) * sizeof(T) == above_half_slide;

        init_slide_base()
        {
            std::iota(v_in.begin(), v_in.end(), 1);

            v_left0 = v_in;

            std::fill(v_left_full.begin(), v_left_full.end(), 0);

            std::fill(v_left_half.begin(), v_left_half.end(), 0);
            std::iota(v_left_half.begin() + half_slide / sizeof(T), v_left_half.end(), 1);

            std::fill(v_left_one.begin(), v_left_one.end(), 0);
            std::iota(v_left_one.begin() + 1, v_left_one.end(), 1);

            if (activate_above_below_checks)
            {
                std::fill(v_left_above_half.begin(), v_left_above_half.end(), 0);
                std::iota(v_left_above_half.begin() + above_half_slide / sizeof(T), v_left_above_half.end(), 1);

                std::fill(v_left_below_half.begin(), v_left_below_half.end(), 0);
                std::iota(v_left_below_half.begin() + below_half_slide / sizeof(T), v_left_below_half.end(), 1);
            }

            v_right0 = v_in;

            std::fill(v_right_full.begin(), v_right_full.end(), 0);

            std::fill(v_right_half.begin(), v_right_half.end(), 0);
            std::iota(v_right_half.begin(), v_right_half.begin() + half_slide / sizeof(T), v_in[half_slide / sizeof(T)]);

            std::fill(v_right_one.begin(), v_right_one.end(), 0);
            std::iota(v_right_one.begin(), v_right_one.begin() + full_slide / sizeof(T) - 1, v_in[1]);

            if (activate_above_below_checks)
            {
                std::fill(v_right_above_half.begin(), v_right_above_half.end(), 0);
                std::iota(v_right_above_half.begin(), v_right_above_half.begin() + (full_slide - above_half_slide) / sizeof(T), v_in[above_half_slide / sizeof(T)]);

                std::fill(v_right_below_half.begin(), v_right_below_half.end(), 0);
                std::iota(v_right_below_half.begin(), v_right_below_half.begin() + (full_slide - below_half_slide) / sizeof(T), v_in[below_half_slide / sizeof(T)]);
            }
        }
    };
}

template <class B>
class slide_test : public testing::Test, init_slide_base<typename B::value_type, B::size>
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;
    using base = init_slide_base<typename B::value_type, B::size>;
    using base::above_half_slide;
    using base::activate_above_below_checks;
    using base::below_half_slide;
    using base::full_slide;
    using base::half_slide;

    slide_test()
    {
        std::cout << "slide test" << std::endl;
    }

    void slide_left()
    {
        B b_in = B::load_unaligned(this->v_in.data());
        B b_left0 = B::load_unaligned(this->v_left0.data());
        B b_left_full = B::load_unaligned(this->v_left_full.data());
        B b_left_half = B::load_unaligned(this->v_left_half.data());
        B b_left_one = B::load_unaligned(this->v_left_one.data());
        B b_left_above_half = B::load_unaligned(this->v_left_above_half.data());
        B b_left_below_half = B::load_unaligned(this->v_left_below_half.data());

        B b_res_left0 = xsimd::slide_left<0>(b_in);
        EXPECT_BATCH_EQ(b_res_left0, b_left0) << print_function_name("slide_left 0");

        B b_res_left_full = xsimd::slide_left<full_slide>(b_in);
        EXPECT_BATCH_EQ(b_res_left_full, b_left_full) << print_function_name("slide_left full");

        B b_res_left_half = xsimd::slide_left<half_slide>(b_in);
        EXPECT_BATCH_EQ(b_res_left_half, b_left_half) << print_function_name("slide_left half_slide");

        B b_res_left_one = xsimd::slide_left<sizeof(value_type)>(b_in);
        EXPECT_BATCH_EQ(b_res_left_one, b_left_one) << print_function_name("slide_left one_slide");

        if (activate_above_below_checks)
        {
            B b_res_left_above_half = xsimd::slide_left<above_half_slide>(b_in);
            EXPECT_BATCH_EQ(b_res_left_above_half, b_left_above_half) << print_function_name("slide_left above_half_slide");

            B b_res_left_below_half = xsimd::slide_left<below_half_slide>(b_in);
            EXPECT_BATCH_EQ(b_res_left_below_half, b_left_below_half) << print_function_name("slide_left below_half_slide");
        }
    }

    void slide_right()
    {
        B b_in = B::load_unaligned(this->v_in.data());
        B b_right0 = B::load_unaligned(this->v_right0.data());
        B b_right_full = B::load_unaligned(this->v_right_full.data());
        B b_right_half = B::load_unaligned(this->v_right_half.data());
        B b_right_one = B::load_unaligned(this->v_right_one.data());
        B b_right_above_half = B::load_unaligned(this->v_right_above_half.data());
        B b_right_below_half = B::load_unaligned(this->v_right_below_half.data());

        B b_res_right0 = xsimd::slide_right<0>(b_in);
        EXPECT_BATCH_EQ(b_res_right0, b_right0) << print_function_name("slide_right 0");

        B b_res_right_full = xsimd::slide_right<full_slide>(b_in);
        EXPECT_BATCH_EQ(b_res_right_full, b_right_full) << print_function_name("slide_right full");

        B b_res_right_half = xsimd::slide_right<half_slide>(b_in);
        EXPECT_BATCH_EQ(b_res_right_half, b_right_half) << print_function_name("slide_right half_slide");

        B b_res_right_one = xsimd::slide_right<sizeof(value_type)>(b_in);
        EXPECT_BATCH_EQ(b_res_right_one, b_right_one) << print_function_name("slide_right one_slide");

        if (activate_above_below_checks)
        {
            B b_res_right_above_half = xsimd::slide_right<above_half_slide>(b_in);
            EXPECT_BATCH_EQ(b_res_right_above_half, b_right_above_half) << print_function_name("slide_right above_half_slide");

            B b_res_right_below_half = xsimd::slide_right<below_half_slide>(b_in);
            EXPECT_BATCH_EQ(b_res_right_below_half, b_right_below_half) << print_function_name("slide_right below_half_slide");
        }
    }
};

TYPED_TEST_SUITE(slide_test, batch_int_types, simd_test_names);
TYPED_TEST(slide_test, slide_left)
{
    this->slide_left();
}
TYPED_TEST(slide_test, slide_right)
{
    this->slide_right();
}
#endif

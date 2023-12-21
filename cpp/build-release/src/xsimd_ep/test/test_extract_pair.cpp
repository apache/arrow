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
    struct init_extract_pair_base
    {
        using extract_vector_type = std::array<T, N>;
        extract_vector_type lhs_in, rhs_in, exped;

        std::vector<extract_vector_type> create_extract_vectors(const int index)
        {
            std::vector<extract_vector_type> vects;
            vects.reserve(3);

            int num = static_cast<int>(N);
            /* Generate input data: lhs, rhs */
            for (int i = 0; i < num; ++i)
            {
                lhs_in[i] = 2 * i + 1;
                rhs_in[i] = 2 * i + 2;
            }
            vects.push_back(std::move(lhs_in));
            vects.push_back(std::move(rhs_in));

            /* Expected shuffle data */
            for (int i = 0; i < (num - index); ++i)
            {
                exped[i] = rhs_in[i + index];
                if (i < index)
                {
                    exped[num - 1 - i] = lhs_in[index - 1 - i];
                }
            }
            vects.push_back(std::move(exped));

            return vects;
        }
    };
}

template <class B>
class extract_pair_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;

    extract_pair_test()
    {
        std::cout << "shffle_extract_pair tests" << std::endl;
    }

    void extract_pair_128()
    {
        xsimd::init_extract_pair_base<value_type, size> extract_pair_base;
        auto extract_pair_vecs = extract_pair_base.create_extract_vectors(1);
        auto v_lhs = extract_pair_vecs[0];
        auto v_rhs = extract_pair_vecs[1];
        auto v_exped = extract_pair_vecs[2];

        B b_lhs = B::load_unaligned(v_lhs.data());
        B b_rhs = B::load_unaligned(v_rhs.data());
        B b_exped = B::load_unaligned(v_exped.data());

        /* Only Test 128bit */
        if ((sizeof(value_type) * size) == 16)
        {
            B b_res = xsimd::extract_pair(b_lhs, b_rhs, 1);
            EXPECT_BATCH_EQ(b_res, b_exped) << print_function_name("extract_pair 128 test");
        }
    }
};

TYPED_TEST_SUITE(extract_pair_test, batch_types, simd_test_names);

TYPED_TEST(extract_pair_test, extract_pair_128)
{
    this->extract_pair_128();
}
#endif

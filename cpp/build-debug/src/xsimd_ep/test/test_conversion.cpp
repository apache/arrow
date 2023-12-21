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

#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
template <class CP>
class conversion_test : public testing::Test
{
protected:
    static constexpr size_t N = CP::size;
    static constexpr size_t A = CP::alignment;

    using int32_batch = xsimd::batch<int32_t>;
    using int64_batch = xsimd::batch<int64_t>;
    using float_batch = xsimd::batch<float>;
    using double_batch = xsimd::batch<double>;

    using uint8_batch = xsimd::batch<uint8_t>;
    using uint16_batch = xsimd::batch<uint16_t>;
    using uint32_batch = xsimd::batch<uint32_t>;
    using uint64_batch = xsimd::batch<uint64_t>;

    using int32_vector = std::vector<int32_t, xsimd::default_allocator<int32_t>>;
    using int64_vector = std::vector<int64_t, xsimd::default_allocator<int64_t>>;
    using float_vector = std::vector<float, xsimd::default_allocator<float>>;
    using double_vector = std::vector<double, xsimd::default_allocator<double>>;

    using uint8_vector = std::vector<uint8_t, xsimd::aligned_allocator<uint8_t, A>>;

    /*int32_batch i32pos;
    int32_batch i32neg;
    int64_batch i64pos;
    int64_batch i64neg;
    float_batch fpos;
    float_batch fneg;
    double_batch dpos;
    double_batch dneg;*/

    int32_vector fposres;
    int32_vector fnegres;
    int64_vector dposres;
    int64_vector dnegres;
    float_vector i32posres;
    float_vector i32negres;
    double_vector i64posres;
    double_vector i64negres;

    uint8_vector ui8res;

    conversion_test()
        : fposres(2 * N, 7)
        , fnegres(2 * N, -6)
        , dposres(N, 5)
        , dnegres(N, -1)
        , i32posres(2 * N, float(2))
        , i32negres(2 * N, float(-3))
        , i64posres(N, double(2))
        , i64negres(N, double(-3))
        , ui8res(8 * N, 4)
    {
    }

    void test_to_int32()
    {
        float_batch fpos(float(7.4)), fneg(float(-6.2));
        int32_vector fvres(int32_batch::size);
        {
            int32_batch fbres = to_int(fpos);
            fbres.store_aligned(fvres.data());
            EXPECT_VECTOR_EQ(fvres, fposres) << print_function_name("to_int(positive float)");
        }
        {
            int32_batch fbres = to_int(fneg);
            fbres.store_aligned(fvres.data());
            EXPECT_VECTOR_EQ(fvres, fnegres) << print_function_name("to_int(negative float)");
        }
    }

    void test_to_int64()
    {
        double_batch dpos(double(5.4)), dneg(double(-1.2));
        int64_vector dvres(int64_batch::size);
        {
            int64_batch dbres = to_int(dpos);
            dbres.store_aligned(dvres.data());
            EXPECT_VECTOR_EQ(dvres, dposres) << print_function_name("to_int(positive double)");
        }
        {
            int64_batch dbres = to_int(dneg);
            dbres.store_aligned(dvres.data());
            EXPECT_VECTOR_EQ(dvres, dnegres) << print_function_name("to_int(negative double)");
        }
    }

    void test_to_float()
    {
        int32_batch i32pos(2), i32neg(-3);
        float_vector i32vres(float_batch::size);
        {
            float_batch i32bres = to_float(i32pos);
            i32bres.store_aligned(i32vres.data());
            EXPECT_VECTOR_EQ(i32vres, i32posres) << print_function_name("to_float(positive int32)");
        }
        {
            float_batch i32bres = to_float(i32neg);
            i32bres.store_aligned(i32vres.data());
            EXPECT_VECTOR_EQ(i32vres, i32negres) << print_function_name("to_float(negative int32)");
        }
    }

    void test_to_double()
    {
        int64_batch i64pos(2), i64neg(-3);
        double_vector i64vres(double_batch::size);
        {
            double_batch i64bres = to_float(i64pos);
            i64bres.store_aligned(i64vres.data());
            EXPECT_VECTOR_EQ(i64vres, i64posres) << print_function_name("to_float(positive int64)");
        }
        {
            double_batch i64bres = to_float(i64neg);
            i64bres.store_aligned(i64vres.data());
            EXPECT_VECTOR_EQ(i64vres, i64negres) << print_function_name("to_float(negative int64)");
        }
    }

    void test_u8_casting()
    {
        uint8_batch ui8tmp(4);
        uint8_vector ui8vres(uint8_batch::size);
        {
            uint16_batch ui16casting = xsimd::bitwise_cast<uint16_batch>(ui8tmp);
            uint8_batch ui8casting = xsimd::bitwise_cast<uint8_batch>(ui16casting);
            ui8casting.store_aligned(ui8vres.data());
            EXPECT_VECTOR_EQ(ui8vres, ui8res) << print_function_name("u8_to_16");
        }
        {
            uint32_batch ui32casting = xsimd::bitwise_cast<uint32_batch>(ui8tmp);
            uint8_batch ui8casting = xsimd::bitwise_cast<uint8_batch>(ui32casting);
            ui8casting.store_aligned(ui8vres.data());
            EXPECT_VECTOR_EQ(ui8vres, ui8res) << print_function_name("u8_to_32");
        }
        {
            uint64_batch ui64casting = xsimd::bitwise_cast<uint64_batch>(ui8tmp);
            uint8_batch ui8casting = xsimd::bitwise_cast<uint8_batch>(ui64casting);
            ui8casting.store_aligned(ui8vres.data());
            EXPECT_VECTOR_EQ(ui8vres, ui8res) << print_function_name("u8_to_64");
        }
    }
};

TYPED_TEST_SUITE(conversion_test, conversion_types, conversion_test_names);

TYPED_TEST(conversion_test, to_int32)
{
    this->test_to_int32();
}

TYPED_TEST(conversion_test, to_int64)
{
    this->test_to_int64();
}

TYPED_TEST(conversion_test, to_float)
{
    this->test_to_float();
}

TYPED_TEST(conversion_test, to_double)
{
    this->test_to_double();
}

TYPED_TEST(conversion_test, u8_casting)
{
    this->test_u8_casting();
}

#endif
#endif

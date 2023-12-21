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
class bitwise_cast_test : public testing::Test
{
protected:
    static constexpr size_t N = CP::size;

    using int32_batch = xsimd::batch<int32_t>;
    using int64_batch = xsimd::batch<int64_t>;
    using float_batch = xsimd::batch<float>;
    using double_batch = xsimd::batch<double>;

    using int32_vector = std::vector<int32_t, xsimd::default_allocator<int32_t>>;
    using int64_vector = std::vector<int64_t, xsimd::default_allocator<int64_t>>;
    using float_vector = std::vector<float, xsimd::default_allocator<float>>;
    using double_vector = std::vector<double, xsimd::default_allocator<double>>;

    int32_vector ftoi32_res;
    int32_vector dtoi32_res;
    int64_vector ftoi64_res;
    int64_vector dtoi64_res;
    float_vector i32tof_res;
    float_vector i64tof_res;
    float_vector dtof_res;
    double_vector i32tod_res;
    double_vector i64tod_res;
    double_vector ftod_res;

    bitwise_cast_test()
        : ftoi32_res(2 * N)
        , dtoi32_res(2 * N)
        , ftoi64_res(N)
        , dtoi64_res(N)
        , i32tof_res(2 * N)
        , i64tof_res(2 * N)
        , dtof_res(2 * N)
        , i32tod_res(N)
        , i64tod_res(N)
        , ftod_res(N)
    {
        {
            int32_batch input = i32_input();
            bitcast b;
            b.i32[0] = input.get(0);
            b.i32[1] = input.get(1);
            std::fill(i32tof_res.begin(), i32tof_res.end(), b.f[0]);
            std::fill(i32tod_res.begin(), i32tod_res.end(), b.d);
        }
        {
            int64_batch input = i64_input();
            bitcast b;
            b.i64 = input.get(0);
            std::fill(i64tod_res.begin(), i64tod_res.end(), b.d);
            for (size_t i = 0; i < N; ++i)
            {
                i64tof_res[2 * i] = b.f[0];
                i64tof_res[2 * i + 1] = b.f[1];
            }
        }
        {
            float_batch input = f_input();
            bitcast b;
            b.f[0] = input.get(0);
            b.f[1] = input.get(1);
            std::fill(ftoi32_res.begin(), ftoi32_res.end(), b.i32[0]);
            std::fill(ftoi64_res.begin(), ftoi64_res.end(), b.i64);
            std::fill(ftod_res.begin(), ftod_res.end(), b.d);
        }
        {
            double_batch input = d_input();
            bitcast b;
            b.d = input.get(0);
            // std::fill(dtoi32_res.begin(), dtoi32_res.end(), b.i32[0]);
            std::fill(dtoi64_res.begin(), dtoi64_res.end(), b.i64);
            for (size_t i = 0; i < N; ++i)
            {
                dtoi32_res[2 * i] = b.i32[0];
                dtoi32_res[2 * i + 1] = b.i32[1];
                dtof_res[2 * i] = b.f[0];
                dtof_res[2 * i + 1] = b.f[1];
            }
        }
    }

    void test_to_int32()
    {
        int32_vector i32vres(int32_batch::size);
        {
            int32_batch i32bres = xsimd::bitwise_cast<int32_batch>(f_input());
            i32bres.store_aligned(i32vres.data());
            EXPECT_VECTOR_EQ(i32vres, ftoi32_res) << print_function_name("to_int32(float)");
        }
        {
            int32_batch i32bres = xsimd::bitwise_cast<int32_batch>(d_input());
            i32bres.store_aligned(i32vres.data());
            EXPECT_VECTOR_EQ(i32vres, dtoi32_res) << print_function_name("to_int32(double)");
        }
    }

    void test_to_int64()
    {
        int64_vector i64vres(int64_batch::size);
        {
            int64_batch i64bres = xsimd::bitwise_cast<int64_batch>(f_input());
            i64bres.store_aligned(i64vres.data());
            EXPECT_VECTOR_EQ(i64vres, ftoi64_res) << print_function_name("to_int64(float)");
        }
        {
            int64_batch i64bres = xsimd::bitwise_cast<int64_batch>(d_input());
            i64bres.store_aligned(i64vres.data());
            EXPECT_VECTOR_EQ(i64vres, dtoi64_res) << print_function_name("to_int64(double)");
        }
    }

    void test_to_float()
    {
        float_vector fvres(float_batch::size);
        {
            float_batch fbres = xsimd::bitwise_cast<float_batch>(i32_input());
            fbres.store_aligned(fvres.data());
            EXPECT_VECTOR_EQ(fvres, i32tof_res) << print_function_name("to_float(int32_t)");
        }
        {
            float_batch fbres = xsimd::bitwise_cast<float_batch>(i64_input());
            fbres.store_aligned(fvres.data());
            EXPECT_VECTOR_EQ(fvres, i64tof_res) << print_function_name("to_float(int64_t)");
        }
        {
            float_batch fbres = xsimd::bitwise_cast<float_batch>(d_input());
            fbres.store_aligned(fvres.data());
            EXPECT_VECTOR_EQ(fvres, dtof_res) << print_function_name("to_float(double)");
        }
    }

    void test_to_double()
    {
        double_vector dvres(double_batch::size);
        {
            double_batch dbres = xsimd::bitwise_cast<double_batch>(i32_input());
            dbres.store_aligned(dvres.data());
            EXPECT_VECTOR_EQ(dvres, i32tod_res) << print_function_name("to_double(int32_t)");
        }
        {
            double_batch dbres = xsimd::bitwise_cast<double_batch>(i64_input());
            dbres.store_aligned(dvres.data());
            EXPECT_VECTOR_EQ(dvres, i64tod_res) << print_function_name("to_double(int64_t)");
        }
        {
            double_batch dbres = xsimd::bitwise_cast<double_batch>(f_input());
            dbres.store_aligned(dvres.data());
            EXPECT_VECTOR_EQ(dvres, ftod_res) << print_function_name("to_double(float)");
        }
    }

private:
    int32_batch i32_input() const
    {
        return int32_batch(2);
    }

    int64_batch i64_input() const
    {
        return int64_batch(2);
    }

    float_batch f_input() const
    {
        return float_batch(3.);
    }

    double_batch d_input() const
    {
        return double_batch(2.5e17);
    }

    union bitcast
    {
        float f[2];
        int32_t i32[2];
        int64_t i64;
        double d;
    };
};

TYPED_TEST_SUITE(bitwise_cast_test, conversion_types, conversion_test_names);

TYPED_TEST(bitwise_cast_test, to_int32)
{
    this->test_to_int32();
}

TYPED_TEST(bitwise_cast_test, to_int64)
{
    this->test_to_int64();
}

TYPED_TEST(bitwise_cast_test, to_float)
{
    this->test_to_float();
}

TYPED_TEST(bitwise_cast_test, to_double)
{
    this->test_to_double();
}
#endif
#endif

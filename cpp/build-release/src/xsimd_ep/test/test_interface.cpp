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

#include <cstddef>
#include <numeric>
#include <vector>

#include "gtest/gtest.h"

#include "xsimd/config/xsimd_instruction_set.hpp"

#ifdef XSIMD_INSTR_SET_AVAILABLE

#include "xsimd/xsimd.hpp"

struct interface_tester
{
    std::vector<float, xsimd::default_allocator<float>> fvec;
    std::vector<int32_t, xsimd::default_allocator<int32_t>> ivec;
    std::vector<float, xsimd::default_allocator<float>> fres;
    std::vector<int32_t, xsimd::default_allocator<int32_t>> ires;

    interface_tester();

    static const std::size_t SIZE = xsimd::simd_traits<float>::size;
};

interface_tester::interface_tester()
    : fvec(SIZE)
    , ivec(SIZE)
    , fres(SIZE)
    , ires(SIZE)
{
    std::iota(fvec.begin(), fvec.end(), 1.f);
    std::iota(ivec.begin(), ivec.end(), 1);
}

TEST(xsimd, set_simd)
{
    interface_tester t;
    xsimd::simd_type<float> r1 = xsimd::set_simd(t.fvec[0]);
    EXPECT_EQ(r1[0], t.fvec[0]);

    xsimd::simd_type<float> r2 = xsimd::set_simd<int32_t, float>(t.ivec[0]);
    EXPECT_EQ(r2[0], t.fvec[0]);
}

TEST(xsimd, load_store_aligned)
{
    interface_tester t;
    xsimd::simd_type<float> r1 = xsimd::load_aligned(&t.fvec[0]);
    xsimd::store_aligned(&t.fres[0], r1);
    EXPECT_EQ(t.fvec, t.fres);

    xsimd::simd_type<float> r2 = xsimd::load_aligned<int32_t, float>(&t.ivec[0]);
    xsimd::store_aligned(&t.fres[0], r2);
    EXPECT_EQ(t.fvec, t.fres);

    xsimd::simd_type<float> r3 = xsimd::load_aligned(&t.fvec[0]);
    xsimd::store_aligned<int32_t, float>(&t.ires[0], r3);
    EXPECT_EQ(t.ivec, t.ires);
}

TEST(xsimd, load_store_unaligned)
{
    interface_tester t;
    xsimd::simd_type<float> r1 = xsimd::load_unaligned(&t.fvec[0]);
    xsimd::store_unaligned(&t.fres[0], r1);
    EXPECT_EQ(t.fvec, t.fres);

    xsimd::simd_type<float> r2 = xsimd::load_unaligned<int32_t, float>(&t.ivec[0]);
    xsimd::store_unaligned(&t.fres[0], r2);
    EXPECT_EQ(t.fvec, t.fres);

    xsimd::simd_type<float> r3 = xsimd::load_unaligned(&t.fvec[0]);
    xsimd::store_unaligned<int32_t, float>(&t.ires[0], r3);
    EXPECT_EQ(t.ivec, t.ires);
}

TEST(xsimd, load_store_simd_aligned)
{
    interface_tester t;
    xsimd::simd_type<float> r1 = xsimd::load_simd(&t.fvec[0], xsimd::aligned_mode());
    xsimd::store_simd(&t.fres[0], r1, xsimd::aligned_mode());
    EXPECT_EQ(t.fvec, t.fres);

    xsimd::simd_type<float> r2 = xsimd::load_simd<int32_t, float>(&t.ivec[0], xsimd::aligned_mode());
    xsimd::store_simd(&t.fres[0], r2, xsimd::aligned_mode());
    EXPECT_EQ(t.fvec, t.fres);

    xsimd::simd_type<float> r3 = xsimd::load_simd(&t.fvec[0], xsimd::aligned_mode());
    xsimd::store_simd<int32_t, float>(&t.ires[0], r3, xsimd::aligned_mode());
    EXPECT_EQ(t.ivec, t.ires);
}

TEST(xsimd, load_store_simd_unaligned)
{
    interface_tester t;
    xsimd::simd_type<float> r1 = xsimd::load_simd(&t.fvec[0], xsimd::unaligned_mode());
    xsimd::store_simd(&t.fres[0], r1, xsimd::unaligned_mode());
    EXPECT_EQ(t.fvec, t.fres);

    xsimd::simd_type<float> r2 = xsimd::load_simd<int32_t, float>(&t.ivec[0], xsimd::unaligned_mode());
    xsimd::store_simd(&t.fres[0], r2, xsimd::unaligned_mode());
    EXPECT_EQ(t.fvec, t.fres);

    xsimd::simd_type<float> r3 = xsimd::load_simd(&t.fvec[0], xsimd::unaligned_mode());
    xsimd::store_simd<int32_t, float>(&t.ires[0], r3, xsimd::unaligned_mode());
    EXPECT_EQ(t.ivec, t.ires);
}
#endif // XSIMD_INSTR_SET_AVAILABLE
#endif

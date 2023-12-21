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

#include <type_traits>
#include <vector>

#include "gtest/gtest.h"

#include "xsimd/memory/xsimd_aligned_allocator.hpp"
#include "xsimd/memory/xsimd_alignment.hpp"

struct mock_container
{
};

TEST(xsimd, alignment)
{
    using u_vector_type = std::vector<double>;
    using a_vector_type = std::vector<double, xsimd::default_allocator<double>>;

    using u_vector_align = xsimd::container_alignment_t<u_vector_type>;
    using a_vector_align = xsimd::container_alignment_t<a_vector_type>;
    using mock_align = xsimd::container_alignment_t<mock_container>;

    EXPECT_TRUE((std::is_same<u_vector_align, xsimd::unaligned_mode>::value));
    EXPECT_TRUE((std::is_same<a_vector_align, xsimd::aligned_mode>::value));
    EXPECT_TRUE((std::is_same<mock_align, xsimd::unaligned_mode>::value));
}
#endif

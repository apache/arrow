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

#include <numeric>
#include <random>
#include <type_traits>

#include "test_sum.hpp"
#include "test_utils.hpp"

static_assert(xsimd::default_arch::supported(), "default arch must be supported");
static_assert(xsimd::supported_architectures::contains<xsimd::default_arch>(), "default arch is supported");
static_assert(xsimd::all_architectures::contains<xsimd::default_arch>(), "default arch is a valid arch");
// static_assert(!(xsimd::x86_arch::supported() && xsimd::arm::supported()), "either x86 or arm, but not both");

struct check_supported
{
    template <class Arch>
    void operator()(Arch) const
    {
        static_assert(Arch::supported(), "not supported?");
    }
};

TEST(arch, supported)
{
    xsimd::supported_architectures::for_each(check_supported {});
}

TEST(arch, name)
{
    constexpr char const* name = xsimd::default_arch::name();
    (void)name;
}

struct check_available
{
    template <class Arch>
    void operator()(Arch) const
    {
        EXPECT_TRUE(Arch::available());
    }
};

TEST(arch, available)
{
    EXPECT_TRUE(xsimd::default_arch::available());
}

TEST(arch, arch_list_alignment)
{
    static_assert(xsimd::arch_list<xsimd::generic>::alignment() == 0,
                  "generic");
    static_assert(xsimd::arch_list<xsimd::sse2>::alignment()
                      == xsimd::sse2::alignment(),
                  "one architecture");
    static_assert(xsimd::arch_list<xsimd::avx512f, xsimd::sse2>::alignment()
                      == xsimd::avx512f::alignment(),
                  "two architectures");
}

struct get_arch_version
{
    template <class Arch>
    unsigned operator()(Arch) { return Arch::version(); }
};

TEST(arch, dispatcher)
{
    float data[17] = { 1.f, 2.f, 3.f, 4.f, 5.f, 6.f, 7.f, 8.f, 9.f, 10.f, 11.f, 12.f, 13.f, 14.f, 15.f, 16.f, 17.f };
    float ref = std::accumulate(std::begin(data), std::end(data), 0.f);

    // platform specific
    {
        auto dispatched = xsimd::dispatch(sum {});
        float res = dispatched(data, 17);
        EXPECT_EQ(ref, res);
    }

#if XSIMD_WITH_AVX && XSIMD_WITH_SSE2
    static_assert(xsimd::supported_architectures::contains<xsimd::avx>() && xsimd::supported_architectures::contains<xsimd::sse2>(), "consistent supported architectures");
    {
        auto dispatched = xsimd::dispatch<xsimd::arch_list<xsimd::avx, xsimd::sse2>>(sum {});
        float res = dispatched(data, 17);
        EXPECT_EQ(ref, res);
    }

    // check that we pick the most appropriate version
    {
        auto dispatched = xsimd::dispatch<xsimd::arch_list<xsimd::sse3, xsimd::sse2>>(get_arch_version {});
        unsigned expected = xsimd::available_architectures().best >= xsimd::sse3::version()
            ? xsimd::sse3::version()
            : xsimd::sse2::version();
        EXPECT_EQ(expected, dispatched());
    }
#endif
}

TEST(arch, fixed_size_types)
{
    using batch4f = xsimd::make_sized_batch_t<float, 4>;
    using batch2d = xsimd::make_sized_batch_t<double, 2>;
    using batch4i32 = xsimd::make_sized_batch_t<int32_t, 4>;
    using batch4u32 = xsimd::make_sized_batch_t<uint32_t, 4>;

    using batch8f = xsimd::make_sized_batch_t<float, 8>;
    using batch4d = xsimd::make_sized_batch_t<double, 4>;
    using batch8i32 = xsimd::make_sized_batch_t<int32_t, 8>;
    using batch8u32 = xsimd::make_sized_batch_t<uint32_t, 8>;

#if XSIMD_WITH_SSE2 || XSIMD_WITH_NEON || XSIMD_WITH_NEON64 || XSIMD_WITH_SVE
    EXPECT_EQ(4, size_t(batch4f::size));
    EXPECT_EQ(4, size_t(batch4i32::size));
    EXPECT_EQ(4, size_t(batch4u32::size));

    EXPECT_TRUE(bool(std::is_same<float, batch4f::value_type>::value));
    EXPECT_TRUE(bool(std::is_same<int32_t, batch4i32::value_type>::value));
    EXPECT_TRUE(bool(std::is_same<uint32_t, batch4u32::value_type>::value));

#if XSIMD_WITH_SSE2 || XSIMD_WITH_NEON64 || XSIMD_WITH_SVE
    EXPECT_EQ(2, size_t(batch2d::size));
    EXPECT_TRUE(bool(std::is_same<double, batch2d::value_type>::value));
#else
    EXPECT_TRUE(bool(std::is_same<void, batch2d>::value));
#endif

#endif
#if !XSIMD_WITH_AVX && !XSIMD_WITH_FMA3 && !(XSIMD_WITH_SVE && XSIMD_SVE_BITS == 256)
    EXPECT_TRUE(bool(std::is_same<void, batch8f>::value));
    EXPECT_TRUE(bool(std::is_same<void, batch4d>::value));
    EXPECT_TRUE(bool(std::is_same<void, batch8i32>::value));
    EXPECT_TRUE(bool(std::is_same<void, batch8u32>::value));
#else
    EXPECT_EQ(8, size_t(batch8f::size));
    EXPECT_EQ(8, size_t(batch8i32::size));
    EXPECT_EQ(8, size_t(batch8u32::size));
    EXPECT_EQ(4, size_t(batch4d::size));

    EXPECT_TRUE(bool(std::is_same<float, batch8f::value_type>::value));
    EXPECT_TRUE(bool(std::is_same<double, batch4d::value_type>::value));
    EXPECT_TRUE(bool(std::is_same<int32_t, batch8i32::value_type>::value));
    EXPECT_TRUE(bool(std::is_same<uint32_t, batch8u32::value_type>::value));
#endif
}

template <class T>
static bool try_load()
{
    static_assert(std::is_same<xsimd::batch<T>, decltype(xsimd::load_aligned(std::declval<T*>()))>::value,
                  "loading the expected type");
    static_assert(std::is_same<xsimd::batch<T>, decltype(xsimd::load_unaligned(std::declval<T*>()))>::value,
                  "loading the expected type");
    return true;
}

template <class... Tys>
void try_loads()
{
    (void)std::initializer_list<bool> { try_load<Tys>()... };
}

TEST(arch, default_load)
{
    // make sure load_aligned / load_unaligned work for the default arch and
    // return the appropriate type.
    using type_list = xsimd::mpl::type_list<short, int, long, float, std::complex<float>
#if XSIMD_WITH_NEON64 || !XSIMD_WITH_NEON
                                            ,
                                            double, std::complex<double>
#endif
                                            >;
    try_loads<type_list>();
}

#endif

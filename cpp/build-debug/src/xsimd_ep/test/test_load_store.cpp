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

#include <random>

#include "test_utils.hpp"

template <class B>
class load_store_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    using index_type = typename xsimd::as_integer_t<batch_type>;
    template <class T>
    using allocator = xsimd::default_allocator<T, typename B::arch_type>;
    static constexpr size_t size = B::size;
    using array_type = std::array<value_type, size>;
    using int8_vector_type = std::vector<int8_t, allocator<int8_t>>;
    using uint8_vector_type = std::vector<uint8_t, allocator<uint8_t>>;
    using int16_vector_type = std::vector<int16_t, allocator<int16_t>>;
    using uint16_vector_type = std::vector<uint16_t, allocator<uint16_t>>;
    using int32_vector_type = std::vector<int32_t, allocator<int32_t>>;
    using uint32_vector_type = std::vector<uint32_t, allocator<uint32_t>>;
    using int64_vector_type = std::vector<int64_t, allocator<int64_t>>;
    using uint64_vector_type = std::vector<uint64_t, allocator<uint64_t>>;
#ifdef XSIMD_32_BIT_ABI
    using long_vector_type = std::vector<long, allocator<long>>;
    using ulong_vector_type = std::vector<unsigned long, allocator<unsigned long>>;
#endif
    using float_vector_type = std::vector<float, allocator<float>>;
    using double_vector_type = std::vector<double, allocator<double>>;

    int8_vector_type i8_vec;
    uint8_vector_type ui8_vec;
    int16_vector_type i16_vec;
    uint16_vector_type ui16_vec;
    int32_vector_type i32_vec;
    uint32_vector_type ui32_vec;
    int64_vector_type i64_vec;
    uint64_vector_type ui64_vec;
#ifdef XSIMD_32_BIT_ABI
    long_vector_type l_vec;
    ulong_vector_type ul_vec;
#endif
    float_vector_type f_vec;
    double_vector_type d_vec;

    array_type expected;

    load_store_test()
    {
        init_test_vector(i8_vec);
        init_test_vector(ui8_vec);
        init_test_vector(i16_vec);
        init_test_vector(ui16_vec);
        init_test_vector(i32_vec);
        init_test_vector(ui32_vec);
        init_test_vector(i64_vec);
        init_test_vector(ui64_vec);
#ifdef XSIMD_32_BIT_ABI
        init_test_vector(l_vec);
        init_test_vector(ul_vec);
#endif
        init_test_vector(f_vec);
        init_test_vector(d_vec);
    }

    void test_load()
    {
        test_load_impl(i8_vec, "load int8_t");
        test_load_impl(ui8_vec, "load uint8_t");
        test_load_impl(i16_vec, "load int16_t");
        test_load_impl(ui16_vec, "load uint16_t");
        test_load_impl(i32_vec, "load int32_t");
        test_load_impl(ui32_vec, "load uint32_t");
        test_load_impl(i64_vec, "load int64_t");
        test_load_impl(ui64_vec, "load uint64_t");
#ifdef XSIMD_32_BIT_ABI
        test_load_impl(l_vec, "load long");
        test_load_impl(ul_vec, "load unsigned long");
#endif
        test_load_impl(f_vec, "load float");
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        test_load_impl(d_vec, "load double");
#endif
    }

    void test_store()
    {
        test_store_impl(i8_vec, "load int8_t");
        test_store_impl(ui8_vec, "load uint8_t");
        test_store_impl(i16_vec, "load int16_t");
        test_store_impl(ui16_vec, "load uint16_t");
        test_store_impl(i32_vec, "load int32_t");
        test_store_impl(ui32_vec, "load uint32_t");
        test_store_impl(i64_vec, "load int64_t");
        test_store_impl(ui64_vec, "load uint64_t");
#ifdef XSIMD_32_BIT_ABI
        test_store_impl(l_vec, "load long");
        test_store_impl(ul_vec, "load unsigned long");
#endif
        test_store_impl(f_vec, "load float");
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        test_store_impl(d_vec, "load double");
#endif
    }
    void test_gather()
    {
        test_gather_impl(i8_vec, "gather int8_t");
        test_gather_impl(ui8_vec, "gather uint8_t");
        test_gather_impl(i16_vec, "gather int16_t");
        test_gather_impl(ui16_vec, "gather uint16_t");
        test_gather_impl(i32_vec, "gather int32_t");
        test_gather_impl(ui32_vec, "gather uint32_t");
        test_gather_impl(i64_vec, "gather int64_t");
        test_gather_impl(ui64_vec, "gather uint64_t");
#ifdef XSIMD_32_BIT_ABI
        test_gather_impl(l_vec, "gather long");
        test_gather_impl(ul_vec, "gather unsigned long");
#endif
        test_gather_impl(f_vec, "gather float");
        test_gather_impl(f_vec, "gather float");
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        test_gather_impl(d_vec, "gather double");
#endif
    }

    void test_scatter()
    {
        test_scatter_impl(i8_vec, "scatter int8_t");
        test_scatter_impl(ui8_vec, "scatter uint8_t");
        test_scatter_impl(i16_vec, "scatter int16_t");
        test_scatter_impl(ui16_vec, "scatter uint16_t");
        test_scatter_impl(i32_vec, "scatter int32_t");
        test_scatter_impl(ui32_vec, "scatter uint32_t");
        test_scatter_impl(i64_vec, "scatter int64_t");
        test_scatter_impl(ui64_vec, "scatter uint64_t");
#ifdef XSIMD_32_BIT_ABI
        test_scatter_impl(l_vec, "scatter long");
        test_scatter_impl(ul_vec, "scatter unsigned long");
#endif
        test_scatter_impl(f_vec, "scatter float");
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        test_scatter_impl(d_vec, "scatter double");
#endif
    }

private:
    template <class V>
    void test_load_impl(const V& v, const std::string& name)
    {
        std::copy(v.cbegin(), v.cend(), expected.begin());

        batch_type b = batch_type::load_unaligned(v.data());
        EXPECT_BATCH_EQ(b, expected) << print_function_name(name + " unaligned");

        b = batch_type::load_aligned(v.data());
        EXPECT_BATCH_EQ(b, expected) << print_function_name(name + " aligned");

        b = xsimd::load_as<value_type>(v.data(), xsimd::unaligned_mode());
        EXPECT_BATCH_EQ(b, expected) << print_function_name(name + " unaligned (load_as)");

        b = xsimd::load_as<value_type>(v.data(), xsimd::aligned_mode());
        EXPECT_BATCH_EQ(b, expected) << print_function_name(name + " aligned (load_as)");
    }

    template <class V>
    void test_store_impl(const V& v, const std::string& name)
    {
        batch_type b = batch_type::load_aligned(v.data());
        V res(size);

        b.store_unaligned(res.data());
        EXPECT_VECTOR_EQ(res, v) << print_function_name(name + " unaligned");

        b.store_aligned(res.data());
        EXPECT_VECTOR_EQ(res, v) << print_function_name(name + " aligned");

        xsimd::store_as(res.data(), b, xsimd::unaligned_mode());
        EXPECT_VECTOR_EQ(res, v) << print_function_name(name + " unaligned (store_as)");

        xsimd::store_as(res.data(), b, xsimd::aligned_mode());
        EXPECT_VECTOR_EQ(res, v) << print_function_name(name + " aligned (store_as)");
    }

    template <class V>
    void test_gather_impl(const V& v, const std::string& name)
    {
        std::copy(v.cbegin(), v.cend(), expected.begin());
        index_type index = xsimd::detail::make_sequence_as_batch<index_type>();
        batch_type b = batch_type::gather(v.data(), index);
        EXPECT_BATCH_EQ(b, expected) << print_function_name(name + " (in order)");

        std::reverse_copy(v.cbegin(), v.cend(), expected.begin());
        std::array<typename index_type::value_type, index_type::size> index_reverse;
        index.store_unaligned(index_reverse.data());
        std::reverse(index_reverse.begin(), index_reverse.end());
        index = index_type::load_unaligned(index_reverse.data());
        b = batch_type::gather(v.data(), index);
        EXPECT_BATCH_EQ(b, expected) << print_function_name(name + " (in reverse order)");
    }

    template <class V>
    void test_scatter_impl(const V& v, const std::string& name)
    {
        batch_type b = batch_type::load_aligned(v.data());
        index_type index = xsimd::detail::make_sequence_as_batch<index_type>();
        V res(size);

        b.scatter(res.data(), index);
        EXPECT_VECTOR_EQ(res, v) << print_function_name(name + " (in order)");

        V reverse_v(size);
        std::reverse_copy(v.cbegin(), v.cend(), reverse_v.begin());
        std::array<typename index_type::value_type, index_type::size> reverse_index;
        index.store_unaligned(reverse_index.data());
        std::reverse(reverse_index.begin(), reverse_index.end());
        index = index_type::load_unaligned(reverse_index.data());
        b.scatter(res.data(), index);
        EXPECT_VECTOR_EQ(res, reverse_v) << print_function_name(name + " (in reverse order)");
    }

    template <class V>
    void init_test_vector(V& vec)
    {
        vec.resize(size);

        value_type min = value_type(0);
        value_type max = value_type(100);

        std::default_random_engine generator;
        std::uniform_int_distribution<int> distribution(min, max);

        auto gen = [&distribution, &generator]()
        {
            return static_cast<value_type>(distribution(generator));
        };

        std::generate(vec.begin(), vec.end(), gen);
    }
};

TYPED_TEST_SUITE(load_store_test, batch_types, simd_test_names);

TYPED_TEST(load_store_test, load)
{
    this->test_load();
}

TYPED_TEST(load_store_test, store)
{
    this->test_store();
}

TYPED_TEST(load_store_test, gather)
{
    this->test_gather();
}

TYPED_TEST(load_store_test, scatter)
{
    this->test_scatter();
}
#endif

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

#include <vector>

#include "test_utils.hpp"

namespace xsimd
{

    int popcount(int v)
    {
        // from https://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetKernighan
        int c; // c accumulates the total bits set in v
        for (c = 0; v; c++)
        {
            v &= v - 1; // clear the least significant bit set
        }
        return c;
    }

    template <class T, std::size_t N>
    struct get_bool_base
    {
        using vector_type = std::array<bool, N>;

        std::vector<vector_type> almost_all_false()
        {
            std::vector<vector_type> vectors;
            vectors.reserve(N);
            for (size_t i = 0; i < N; ++i)
            {
                vector_type v;
                v.fill(false);
                v[i] = true;
                vectors.push_back(std::move(v));
            }
            return vectors;
        }

        std::vector<vector_type> almost_all_true()
        {
            auto vectors = almost_all_false();
            flip(vectors);
            return vectors;
        }

        void flip(vector_type& vec)
        {
            std::transform(vec.begin(), vec.end(), vec.begin(), std::logical_not<bool> {});
        }

        void flip(std::vector<vector_type>& vectors)
        {
            for (auto& vec : vectors)
            {
                flip(vec);
            }
        }
    };

    template <class T, size_t N = T::size>
    struct get_bool;

    template <class T>
    struct get_bool<batch_bool<T>, 2> : public get_bool_base<T, 2>
    {
        using type = batch_bool<T>;
        type all_true = type(true);
        type all_false = type(false);
        type half = { 0, 1 };
        type ihalf = { 1, 0 };
        type interspersed = { 0, 1 };
    };

    template <class T>
    struct get_bool<batch_bool<T>, 4> : public get_bool_base<T, 4>
    {
        using type = batch_bool<T>;

        type all_true = true;
        type all_false = false;
        type half = { 0, 0, 1, 1 };
        type ihalf = { 1, 1, 0, 0 };
        type interspersed = { 0, 1, 0, 1 };
    };

    template <class T>
    struct get_bool<batch_bool<T>, 8> : public get_bool_base<T, 8>
    {
        using type = batch_bool<T>;
        type all_true = true;
        type all_false = false;
        type half = { 0, 0, 0, 0, 1, 1, 1, 1 };
        type ihalf = { 1, 1, 1, 1, 0, 0, 0, 0 };
        type interspersed = { 0, 1, 0, 1, 0, 1, 0, 1 };
    };

    template <class T>
    struct get_bool<batch_bool<T>, 16> : public get_bool_base<T, 16>
    {
        using type = batch_bool<T>;
        type all_true = true;
        type all_false = false;
        type half = { 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1 };
        type ihalf = { 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 };
        type interspersed = { 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1 };
    };

    template <class T>
    struct get_bool<batch_bool<T>, 32> : public get_bool_base<T, 32>
    {
        using type = batch_bool<T>;
        type all_true = true;
        type all_false = false;
        type half = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
        type ihalf = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        type interspersed = { 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1 };
    };

    template <class T>
    struct get_bool<batch_bool<T>, 64> : public get_bool_base<T, 64>
    {
        using type = batch_bool<T>;
        type all_true = true;
        type all_false = false;
        type half = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
        type ihalf = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        type interspersed = { 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1 };
    };

}

template <class B>
class batch_bool_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;
    using batch_bool_type = typename B::batch_bool_type;
    using array_type = std::array<value_type, size>;
    using bool_array_type = std::array<bool, size>;

    array_type lhs;
    array_type rhs;
    bool_array_type all_true;
    bool_array_type ba;

    batch_bool_test()
    {
        for (size_t i = 0; i < size; ++i)
        {
            lhs[i] = value_type(i);
            rhs[i] = i == 0 % 2 ? lhs[i] : lhs[i] * 2;
            all_true[i] = true;
            ba[i] = i == 0 % 2 ? true : false;
        }
    }

    void test_constructors() const
    {
        bool_array_type res;
        batch_bool_type b(true);
        b.store_unaligned(res.data());
        EXPECT_EQ(res, all_true) << print_function_name("batch_bool(bool)");

        batch_bool_type c { true };
        c.store_unaligned(res.data());
        EXPECT_EQ(res, all_true) << print_function_name("batch_bool{bool}");
    }

    void test_load_store() const
    {
        bool_array_type res;
        batch_bool_type b(batch_bool_type::load_unaligned(ba.data()));
        b.store_unaligned(res.data());
        EXPECT_EQ(res, ba) << print_function_name("load_unaligned / store_unaligned");

        alignas(xsimd::default_arch::alignment()) bool_array_type arhs(this->ba);
        alignas(xsimd::default_arch::alignment()) bool_array_type ares;
        b = batch_bool_type::load_aligned(arhs.data());
        b.store_aligned(ares.data());
        EXPECT_EQ(ares, arhs) << print_function_name("load_aligned / store_aligned");
    }

    void test_any_all() const
    {
        auto bool_g = xsimd::get_bool<batch_bool_type> {};
        // any
        {
            auto any_check_false = (batch_lhs() != batch_lhs());
            bool any_res_false = xsimd::any(any_check_false);
            EXPECT_FALSE(any_res_false) << print_function_name("any (false)");
            auto any_check_true = (batch_lhs() == batch_rhs());
            bool any_res_true = xsimd::any(any_check_true);
            EXPECT_TRUE(any_res_true) << print_function_name("any (true)");

            for (const auto& vec : bool_g.almost_all_false())
            {
                batch_bool_type b = batch_bool_type::load_unaligned(vec.data());
                bool any_res = xsimd::any(b);
                EXPECT_TRUE(any_res) << print_function_name("any (almost_all_false)");
            }

            for (const auto& vec : bool_g.almost_all_true())
            {
                batch_bool_type b = batch_bool_type::load_unaligned(vec.data());
                bool any_res = xsimd::any(b);
                EXPECT_TRUE(any_res) << print_function_name("any (almost_all_true)");
            }
        }
        // all
        {
            auto all_check_false = (batch_lhs() == batch_rhs());
            bool all_res_false = xsimd::all(all_check_false);
            EXPECT_FALSE(all_res_false) << print_function_name("all (false)");
            auto all_check_true = (batch_lhs() == batch_lhs());
            bool all_res_true = xsimd::all(all_check_true);
            EXPECT_TRUE(all_res_true) << print_function_name("all (true)");

            for (const auto& vec : bool_g.almost_all_false())
            {
                // TODO: implement batch_bool(bool*)
                // It currently compiles (need to understand why) but does not
                // give expected result
                batch_bool_type b = batch_bool_type::load_unaligned(vec.data());
                bool all_res = xsimd::all(b);
                EXPECT_FALSE(all_res) << print_function_name("all (almost_all_false)");
            }

            for (const auto& vec : bool_g.almost_all_true())
            {
                batch_bool_type b = batch_bool_type::load_unaligned(vec.data());
                bool all_res = xsimd::all(b);
                EXPECT_FALSE(all_res) << print_function_name("all (almost_all_true)");
            }
        }
        // none
        {
            auto none_check_false = (batch_lhs() == batch_rhs());
            bool none_res_false = xsimd::none(none_check_false);
            EXPECT_FALSE(none_res_false) << print_function_name("none (false)");
            auto none_check_true = (batch_lhs() != batch_lhs());
            bool none_res_true = xsimd::none(none_check_true);
            EXPECT_TRUE(none_res_true) << print_function_name("none (true)");

            for (const auto& vec : bool_g.almost_all_false())
            {
                batch_bool_type b = batch_bool_type::load_unaligned(vec.data());
                bool none_res = xsimd::none(b);
                EXPECT_FALSE(none_res) << print_function_name("none (almost_all_false)");
            }

            for (const auto& vec : bool_g.almost_all_true())
            {
                batch_bool_type b = batch_bool_type::load_unaligned(vec.data());
                bool none_res = xsimd::none(b);
                EXPECT_FALSE(none_res) << print_function_name("none (almost_all_true)");
            }
        }
    }

    void test_logical_operations() const
    {
        auto bool_g = xsimd::get_bool<batch_bool_type> {};
        size_t s = size;
        // operator!=
        {
            bool res = xsimd::all(bool_g.half != bool_g.ihalf);
            EXPECT_TRUE(res) << print_function_name("operator!=");
        }
        // operator==
        {
            bool res = xsimd::all(bool_g.half == !bool_g.ihalf);
            EXPECT_TRUE(res) << print_function_name("operator==");
        }
        // operator &&
        {
            batch_bool_type res = bool_g.half && bool_g.ihalf;
            bool_array_type ares;
            res.store_unaligned(ares.data());
            size_t nb_false = std::count(ares.cbegin(), ares.cend(), false);
            EXPECT_EQ(nb_false, s) << print_function_name("operator&&");
        }
        // operator ||
        {
            batch_bool_type res = bool_g.half || bool_g.ihalf;
            bool_array_type ares;
            res.store_unaligned(ares.data());
            size_t nb_true = std::count(ares.cbegin(), ares.cend(), true);
            EXPECT_EQ(nb_true, s) << print_function_name("operator||");
        }
        // operator ^
        {
            batch_bool_type res = bool_g.half ^ bool_g.ihalf;
            bool_array_type ares;
            res.store_unaligned(ares.data());
            size_t nb_true = std::count(ares.cbegin(), ares.cend(), true);
            EXPECT_EQ(nb_true, s) << print_function_name("operator^");
        }
        // bitwise_andnot
        {
            batch_bool_type res = xsimd::bitwise_andnot(bool_g.half, bool_g.half);
            bool_array_type ares;
            res.store_unaligned(ares.data());
            size_t nb_false = std::count(ares.cbegin(), ares.cend(), false);
            EXPECT_EQ(nb_false, s) << print_function_name("bitwise_andnot");
        }
    }

    void test_bitwise_operations() const
    {
        auto bool_g = xsimd::get_bool<batch_bool_type> {};
        // operator~
        {
            bool res = xsimd::all(bool_g.half == ~bool_g.ihalf);
            EXPECT_TRUE(res) << print_function_name("operator~");
        }
        // operator|
        {
            bool res = xsimd::all((bool_g.half | bool_g.ihalf) == bool_g.all_true);
            EXPECT_TRUE(res) << print_function_name("operator|");
        }
        // operator&
        {
            bool res = xsimd::all((bool_g.half & bool_g.ihalf) == bool_g.all_false);
            EXPECT_TRUE(res) << print_function_name("operator&");
        }
    }

    void test_mask() const
    {
        auto bool_g = xsimd::get_bool<batch_bool_type> {};
        const uint64_t full_mask = ((uint64_t)-1) >> (64 - batch_bool_type::size);
        EXPECT_EQ(bool_g.all_false.mask(), 0);
        EXPECT_EQ(batch_bool_type::from_mask(bool_g.all_false.mask()).mask(), bool_g.all_false.mask());

        EXPECT_EQ(bool_g.all_true.mask(), full_mask);
        EXPECT_EQ(batch_bool_type::from_mask(bool_g.all_true.mask()).mask(), bool_g.all_true.mask());

        EXPECT_EQ(bool_g.half.mask(), full_mask & ((uint64_t)-1) << (batch_bool_type::size / 2));
        EXPECT_EQ(batch_bool_type::from_mask(bool_g.half.mask()).mask(), bool_g.half.mask());

        EXPECT_EQ(bool_g.ihalf.mask(), full_mask & ~(((uint64_t)-1) << (batch_bool_type::size / 2)));
        EXPECT_EQ(batch_bool_type::from_mask(bool_g.ihalf.mask()).mask(), bool_g.ihalf.mask());

        EXPECT_EQ(bool_g.interspersed.mask(), full_mask & 0xAAAAAAAAAAAAAAAAul);
        EXPECT_EQ(batch_bool_type::from_mask(bool_g.interspersed.mask()).mask(), bool_g.interspersed.mask());
    }

private:
    batch_type batch_lhs() const
    {
        return batch_type::load_unaligned(lhs.data());
    }

    batch_type batch_rhs() const
    {
        return batch_type::load_unaligned(rhs.data());
    }
};

TYPED_TEST_SUITE(batch_bool_test, batch_types, simd_test_names);

TYPED_TEST(batch_bool_test, constructors)
{
    this->test_constructors();
}

TYPED_TEST(batch_bool_test, load_store)
{
    this->test_load_store();
}

TYPED_TEST(batch_bool_test, any_all)
{
    this->test_any_all();
}

TYPED_TEST(batch_bool_test, logical_operations)
{
    this->test_logical_operations();
}

TYPED_TEST(batch_bool_test, bitwise_operations)
{
    this->test_bitwise_operations();
}

TYPED_TEST(batch_bool_test, mask)
{
    this->test_mask();
}
#endif

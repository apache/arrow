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

#include <cmath>
#include <functional>
#include <numeric>
#include <sstream>

#include "test_utils.hpp"

using namespace std::placeholders;

template <class B>
class batch_test : public testing::Test
{
protected:
    using batch_type = B;
    using value_type = typename B::value_type;
    static constexpr size_t size = B::size;
    using array_type = std::array<value_type, size>;
    using bool_array_type = std::array<bool, size>;

    array_type lhs;
    array_type rhs;
    value_type scalar;

    batch_test()
    {
        init_operands();
    }

    void test_stream_dump() const
    {
        array_type res;
        batch_type b = batch_type::load_unaligned(lhs.data());
        b.store_unaligned(res.data());

        std::ostringstream b_dump;
        b_dump << b;

        std::ostringstream res_dump;
        res_dump << '(';
        for (std::size_t i = 0; i < res.size() - 1; ++i)
            res_dump << res[i] << ", ";
        res_dump << res.back() << ')';

        EXPECT_EQ(res_dump.str(), b_dump.str()) << print_function_name("stream dump");
    }

    void test_load_store() const
    {
        array_type res;
        batch_type b = batch_type::load_unaligned(lhs.data());
        b.store_unaligned(res.data());
        EXPECT_EQ(res, lhs) << print_function_name("load_unaligned / store_unaligned");

        alignas(xsimd::default_arch::alignment()) array_type arhs(this->rhs);
        alignas(xsimd::default_arch::alignment()) array_type ares;
        b = batch_type::load_aligned(arhs.data());
        b.store_aligned(ares.data());
        EXPECT_EQ(ares, rhs) << print_function_name("load_aligned / store_aligned");
    }

    void test_constructors() const
    {
        array_type tmp;
        std::fill(tmp.begin(), tmp.end(), value_type(2));
        batch_type b0a(2);
        EXPECT_EQ(b0a, tmp) << print_function_name("batch(value_type)");

        batch_type b0b { 2 };
        EXPECT_EQ(b0b, tmp) << print_function_name("batch{value_type}");

        batch_type b1 = batch_type::load_unaligned(lhs.data());
        EXPECT_EQ(b1, lhs) << print_function_name("batch(value_type*)");
    }

    void test_static_builders() const
    {
        {
            array_type expected;
            std::fill(expected.begin(), expected.end(), value_type(2));

            auto res = batch_type::broadcast(value_type(2));
            EXPECT_EQ(res, expected) << print_function_name("batch::broadcast");
        }
        {
            array_type res;
            auto b = batch_type::load_unaligned(lhs.data());
            b.store_unaligned(res.data());
            EXPECT_EQ(res, lhs) << print_function_name("batch::load_unaligned");
        }
        {
            alignas(xsimd::default_arch::alignment()) array_type arhs(this->rhs);
            alignas(xsimd::default_arch::alignment()) array_type ares;
            auto b = batch_type::load_aligned(arhs.data());
            b.store_aligned(ares.data());
            EXPECT_EQ(ares, rhs) << print_function_name("batch::load_aligned");
        }
    }

    void test_access_operator() const
    {
        batch_type res = batch_lhs();
        for (size_t i = 0; i < size; ++i)
        {
            EXPECT_EQ(res.get(i), lhs[i]) << print_function_name("get(") << i << ")";
        }
    }

    void test_arithmetic() const
    {
        // +batch
        {
            array_type expected = lhs;
            batch_type res = +batch_lhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("+batch");
        }
        // -batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::negate<value_type>());
            batch_type res = -batch_lhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("-batch");
        }
        // batch + batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::plus<value_type>());
            batch_type res = batch_lhs() + batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch + batch");
        }
        // batch + scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::plus<value_type>(), _1, scalar));
            batch_type lres = batch_lhs() + scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch + scalar");
            batch_type rres = scalar + batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("scalar + batch");
        }
        // batch - batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::minus<value_type>());
            batch_type res = batch_lhs() - batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch - batch");
        }
        // batch - scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::minus<value_type>(), _1, scalar));
            batch_type lres = batch_lhs() - scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch - scalar");
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::minus<value_type>(), scalar, _1));
            batch_type rres = scalar - batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("scalar - batch");
        }
        // batch * batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::multiplies<value_type>());
            batch_type res = batch_lhs() * batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch * batch");
        }
        // batch * scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::multiplies<value_type>(), _1, scalar));
            batch_type lres = batch_lhs() * scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch * scalar");
            batch_type rres = scalar * batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("scalar * batch");
        }
        // batch / batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::divides<value_type>());
            batch_type res = batch_lhs() / batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch / batch");
        }
        // batch / scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::divides<value_type>(), _1, scalar));
            batch_type lres = batch_lhs() / scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch / scalar");
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::divides<value_type>(), scalar, _1));
            batch_type rres = scalar / batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("scalar / batch");
        }
    }

    void test_saturated_arithmetic() const
    {
#ifdef T
        // batch + batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), xsimd::sadd<value_type>);
            batch_type res = xsimd::sadd(batch_lhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("sadd(batch, batch)");
        }
        // batch + scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), [this](value_type x)
                           { return xsimd::sadd(x, scalar); });
            batch_type lres = xsimd::sadd(batch_lhs(), scalar);
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("sadd(batch, scalar)");
            batch_type rres = xsimd::sadd(scalar, batch_lhs());
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("sadd(scalar, batch)");
        }
        // batch - batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), [](value_type x, value_type y)
                           { return xsimd::ssub(x, y); });
            batch_type res = xsimd::ssub(batch_lhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("ssub(batch, batch)");
        }
        // batch - scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), [this](value_type x)
                           { return xsimd::ssub(x, scalar); });
            batch_type lres = xsimd::ssub(batch_lhs(), scalar);
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("ssub(batch, scalar)");
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), [this](value_type x)
                           { return xsimd::ssub(scalar, x); });
            batch_type rres = xsimd::ssub(scalar, batch_lhs());
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("ssub(scalar, batch)");
        }
#endif
    }

    void test_computed_assignment() const
    {
        // batch += batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::plus<value_type>());
            batch_type res = batch_lhs();
            res += batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch += batch");
        }
        // batch += scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::plus<value_type>(), _1, scalar));
            batch_type res = batch_lhs();
            res += scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch += scalar");
        }
        // batch -= batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::minus<value_type>());
            batch_type res = batch_lhs();
            res -= batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch -= batch");
        }
        // batch -= scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::minus<value_type>(), _1, scalar));
            batch_type res = batch_lhs();
            res -= scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch -= scalar");
        }
        // batch *= batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::multiplies<value_type>());
            batch_type res = batch_lhs();
            res *= batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch *= batch");
        }
        // batch *= scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::multiplies<value_type>(), _1, scalar));
            batch_type res = batch_lhs();
            res *= scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch *= scalar");
        }
        // batch /= batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::divides<value_type>());
            batch_type res = batch_lhs();
            res /= batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch /= batch");
        }
        // batch /= scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::divides<value_type>(), _1, scalar));
            batch_type res = batch_lhs();
            res /= scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch /= scalar");
        }
    }

    void test_comparison() const
    {

        // batch == batch
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l == r; });
            auto res = batch_lhs() == batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch == batch");
        }
        // batch == scalar
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [this](const value_type& l)
                           { return l == scalar; });
            auto res = batch_lhs() == scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch == scalar");
        }
        // batch != batch
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l != r; });
            auto res = batch_lhs() != batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch != batch");
        }
        // batch != scalar
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [this](const value_type& l)
                           { return l != scalar; });
            auto res = batch_lhs() != scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch != scalar");
        }
        // batch < batch
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l < r; });
            auto res = batch_lhs() < batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch < batch");
        }
        // batch < scalar
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [this](const value_type& l)
                           { return l < scalar; });
            auto res = batch_lhs() < scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch < scalar");
        }

        // batch <= batch
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l <= r; });
            auto res = batch_lhs() <= batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch <= batch");
        }
        // batch <= scalar
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [this](const value_type& l)
                           { return l <= scalar; });
            auto res = batch_lhs() <= scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch <= scalar");
        }

        // batch > batch
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l > r; });
            auto res = batch_lhs() > batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch > batch");
        }
        // batch > scalar
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [this](const value_type& l)
                           { return l > scalar; });
            auto res = batch_lhs() > scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch > scalar");
        }
        // batch >= batch
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l >= r; });
            auto res = batch_lhs() >= batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch >= batch");
        }
        // batch >= scalar
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [this](const value_type& l)
                           { return l >= scalar; });
            auto res = batch_lhs() >= scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch >= scalar");
        }
    }

    void test_logical() const
    {
        // batch && batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::logical_and<value_type>());
            batch_type res = batch_lhs() && batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch && batch");
        }
        // batch && scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::logical_and<value_type>(), _1, scalar));
            batch_type lres = batch_lhs() && scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch && scalar");
            batch_type rres = scalar && batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("scalar && batch");
        }
        // batch || batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(), std::logical_or<value_type>());
            batch_type res = batch_lhs() || batch_rhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch && batch");
        }
        // batch || scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::logical_or<value_type>(), _1, scalar));
            batch_type lres = batch_lhs() || scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch || scalar");
            batch_type rres = scalar || batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("scalar || batch");
        }
    }

    void test_min_max() const
    {
        // min
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return std::min(l, r); });
            batch_type res = min(batch_lhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("min");
        }
        // min limit case
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type&, const value_type& r)
                           { return std::min(std::numeric_limits<value_type>::min(), r); });
            batch_type res = xsimd::min(batch_type(std::numeric_limits<value_type>::min()), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("min limit");
        }
        // fmin
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return std::fmin(l, r); });
            batch_type res = min(batch_lhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("fmin");
        }
        // max
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return std::max(l, r); });
            batch_type res = max(batch_lhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("max");
        }
        // max limit case
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type&, const value_type& r)
                           { return std::max(std::numeric_limits<value_type>::max(), r); });
            batch_type res = xsimd::max(batch_type(std::numeric_limits<value_type>::max()), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("max limit");
        }
        // fmax
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return std::fmax(l, r); });
            batch_type res = fmax(batch_lhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("fmax");
        }
    }

    void test_fused_operations() const
    {
        // fma
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.begin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l * r + r; });
            // Warning: ADL seems to not work correctly on Windows, thus the full qualified call
            batch_type res = xsimd::fma(batch_lhs(), batch_rhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("fma");
        }
        // fms
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.begin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l * r - r; });
            batch_type res = fms(batch_lhs(), batch_rhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("fms");
        }
        // fnma
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.begin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return -l * r + r; });
            batch_type res = fnma(batch_lhs(), batch_rhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("fnma");
        }
        // fnms
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.begin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return -l * r - r; });
            batch_type res = fnms(batch_lhs(), batch_rhs(), batch_rhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("fnms");
        }
    }

    void test_abs() const
    {
        // abs
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& l)
                           { return ::detail::uabs(l); });
            batch_type res = abs(batch_lhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("abs");
        }
        // fabs
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& l)
                           { return std::fabs(l); });
            batch_type res = fabs(batch_lhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("fabs");
        }
    }

    void test_horizontal_operations() const
    {
        // reduce_add
        {
            value_type expected = std::accumulate(lhs.cbegin(), lhs.cend(), value_type(0));
            value_type res = reduce_add(batch_lhs());
            EXPECT_SCALAR_EQ(res, expected) << print_function_name("reduce_add");
        }
        // reduce_max
        {
            value_type expected = *std::max_element(lhs.cbegin(), lhs.cend());
            value_type res = reduce_max(batch_lhs());
            EXPECT_SCALAR_EQ(res, expected) << print_function_name("reduce_max");
        }
        // reduce_min
        {
            value_type expected = *std::min_element(lhs.cbegin(), lhs.cend());
            value_type res = reduce_min(batch_lhs());
            EXPECT_SCALAR_EQ(res, expected) << print_function_name("reduce_min");
        }
    }

    void test_boolean_conversions() const
    {
        using batch_bool_type = typename batch_type::batch_bool_type;
        // batch = true
        {
            batch_bool_type tbt(true);
            batch_type expected = batch_type(value_type(1));
            batch_type res = (batch_type)tbt;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch = true");
        }
        // batch = false
        {
            batch_bool_type fbt(false);
            batch_type expected = batch_type(value_type(0));
            batch_type res = (batch_type)fbt;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch = false");
        }
        // !batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& l)
                           { return !l; });
            batch_type res = (batch_type)!batch_lhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("!batch");
        }
        // bitwise_cast
        {
            batch_bool_type fbt(false);
            batch_type expected = batch_type(value_type(0));
            batch_type res = bitwise_cast(fbt);
            EXPECT_BATCH_EQ(res, expected) << print_function_name("bitwise_cast");
        }
        // bitwise not
        {
            batch_bool_type fbt(true);
            batch_type expected = batch_type(value_type(0));
            batch_type res = ~bitwise_cast(fbt);
            EXPECT_BATCH_EQ(res, expected) << print_function_name("~batch");
        }
    }

    void test_iterator() const
    {
#if 0
    // FIXME: I don't like that API
        array_type expected = lhs;
        batch_type v = batch_lhs();
        array_type res;
        // iterator
        {
            std::copy(v.begin(), v.end(), res.begin());
            EXPECT_EQ(res, expected) << print_function_name("iterator");
        }
        // constant iterator
        {
            std::copy(v.cbegin(), v.cend(), res.begin());
            EXPECT_EQ(res, expected) << print_function_name("const iterator");
        }
        // reverse iterator
        {
            std::copy(v.rbegin(), v.rend(), res.rbegin());
            EXPECT_EQ(res, expected) << print_function_name("reverse iterator");
        }
        // constant reverse iterator
        {
            std::copy(v.crbegin(), v.crend(), res.rbegin());
            EXPECT_EQ(res, expected) << print_function_name("const reverse iterator");
        }
#endif
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

    template <class T = value_type>
    xsimd::enable_integral_t<T, void> init_operands()
    {
        for (size_t i = 0; i < size; ++i)
        {
            bool negative_lhs = std::is_signed<T>::value && (i % 2 == 1);
            lhs[i] = value_type(i) * (negative_lhs ? -10 : 10);
            if (lhs[i] == value_type(0))
            {
                lhs[i] += value_type(1);
            }
            rhs[i] = value_type(i) + value_type(4);
        }
        scalar = value_type(3);
    }

    template <class T = value_type>
    xsimd::enable_floating_point_t<T, void> init_operands()
    {
        for (size_t i = 0; i < size; ++i)
        {
            lhs[i] = value_type(i) / 4 + value_type(1.2) * std::sqrt(value_type(i + 0.25));
            if (lhs[i] == value_type(0))
            {
                lhs[i] += value_type(0.1);
            }
            rhs[i] = value_type(10.2) / (i + 2) + value_type(0.25);
        }
        scalar = value_type(1.2);
    }
};

TYPED_TEST_SUITE(batch_test, batch_types, simd_test_names);

TYPED_TEST(batch_test, stream_dump)
{
    this->test_stream_dump();
}

TYPED_TEST(batch_test, load_store)
{
    this->test_load_store();
}

TYPED_TEST(batch_test, constructors)
{
    this->test_constructors();
}

TYPED_TEST(batch_test, static_builders)
{
    this->test_static_builders();
}

TYPED_TEST(batch_test, access_operator)
{
    this->test_access_operator();
}

TYPED_TEST(batch_test, arithmetic)
{
    this->test_arithmetic();
}

TYPED_TEST(batch_test, saturated_arithmetic)
{
    this->test_saturated_arithmetic();
}

TYPED_TEST(batch_test, computed_assignment)
{
    this->test_computed_assignment();
}

TYPED_TEST(batch_test, comparison)
{
    this->test_comparison();
}
TYPED_TEST(batch_test, logical)
{
    this->test_logical();
}

TYPED_TEST(batch_test, min_max)
{
    this->test_min_max();
}

TYPED_TEST(batch_test, fused_operations)
{
    this->test_fused_operations();
}

TYPED_TEST(batch_test, abs)
{
    this->test_abs();
}

TYPED_TEST(batch_test, horizontal_operations)
{
    this->test_horizontal_operations();
}

TYPED_TEST(batch_test, boolean_conversions)
{
    this->test_boolean_conversions();
}

TYPED_TEST(batch_test, iterator)
{
    this->test_iterator();
}
#endif

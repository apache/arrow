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

#include "test_utils.hpp"

using namespace std::placeholders;

template <class B>
class batch_complex_test : public testing::Test
{
protected:
    using batch_type = xsimd::simd_type<typename B::value_type>;
    using arch_type = typename B::arch_type;
    using real_batch_type = typename B::real_batch;
    using value_type = typename B::value_type;
    using real_value_type = typename value_type::value_type;
    static constexpr size_t size = B::size;
    using array_type = std::array<value_type, size>;
    using bool_array_type = std::array<bool, size>;
    using real_array_type = std::array<real_value_type, size>;

    array_type lhs;
    array_type rhs;
    value_type scalar;
    real_value_type real_scalar;

#ifdef XSIMD_ENABLE_XTL_COMPLEX
    using xtl_value_type = xtl::xcomplex<real_value_type, real_value_type, true>;
    using xtl_array_type = std::array<xtl_value_type, size>;
#endif

    batch_complex_test()
    {
        scalar = value_type(real_value_type(1.4), real_value_type(2.3));
        real_scalar = scalar.real();
        for (size_t i = 0; i < size; ++i)
        {
            lhs[i] = value_type(real_value_type(i) / real_value_type(4) + real_value_type(1.2) * std::sqrt(real_value_type(i + 0.25)),
                                real_value_type(i) / real_value_type(5));
            rhs[i] = value_type(real_value_type(10.2) / real_value_type(i + 2) + real_value_type(0.25), real_value_type(i) / real_value_type(3.2));
        }
    }

    void test_load_store() const
    {
        {
            array_type res;
            batch_type b = batch_type::load_unaligned(lhs.data());
            b.store_unaligned(res.data());
            EXPECT_EQ(res, lhs) << print_function_name("load_unaligned / store_unaligned complex*");

            alignas(arch_type::alignment()) array_type arhs(this->rhs);
            alignas(arch_type::alignment()) array_type ares;
            b = batch_type::load_aligned(arhs.data());
            b.store_aligned(ares.data());
            EXPECT_EQ(ares, rhs) << print_function_name("load_aligned / store_aligned complex*");
        }

        {
            real_array_type real, imag, res_real, res_imag;
            for (size_t i = 0; i < size; ++i)
            {
                real[i] = lhs[i].real();
                imag[i] = lhs[i].imag();
            }
            batch_type b = batch_type::load_unaligned(real.data(), imag.data());
            b.store_unaligned(res_real.data(), res_imag.data());
            EXPECT_EQ(res_real, real) << print_function_name("load_unaligned / store_unaligned (real*, real*)");

            alignas(arch_type::alignment()) real_array_type areal, aimag, ares_real, ares_imag;
            for (size_t i = 0; i < size; ++i)
            {
                areal[i] = lhs[i].real();
                aimag[i] = lhs[i].imag();
            }
            b = batch_type::load_aligned(areal.data(), aimag.data());
            b.store_aligned(ares_real.data(), ares_imag.data());
            EXPECT_EQ(ares_real, areal) << print_function_name("load_aligned / store_aligned (real*, real*)");
        }
        {
            real_array_type real, imag, res_real, res_imag;
            for (size_t i = 0; i < size; ++i)
            {
                real[i] = lhs[i].real();
                imag[i] = 0;
            }
            batch_type b = batch_type::load_unaligned(real.data());
            b.store_unaligned(res_real.data(), res_imag.data());
            EXPECT_EQ(res_real, real) << print_function_name("load_unaligned / store_unaligned (real*)");
            EXPECT_EQ(res_imag, imag) << print_function_name("load_unaligned / store_unaligned (real*)");

            alignas(arch_type::alignment()) real_array_type areal, aimag, ares_real, ares_imag;
            for (size_t i = 0; i < size; ++i)
            {
                areal[i] = lhs[i].real();
                aimag[i] = 0;
            }
            b = batch_type::load_aligned(areal.data());
            b.store_aligned(ares_real.data(), ares_imag.data());
            EXPECT_EQ(ares_real, areal) << print_function_name("load_aligned / store_aligned (real*)");
            EXPECT_EQ(ares_imag, aimag) << print_function_name("load_aligned / store_aligned (real*)");
        }
    }
#ifdef XSIMD_ENABLE_XTL_COMPLEX
    void test_load_store_xtl() const
    {
        xtl_array_type tmp;
        std::fill(tmp.begin(), tmp.end(), xtl_value_type(2, 3));
        batch_type b0(xtl_value_type(2, 3));
        EXPECT_EQ(b0, tmp) << print_function_name("batch(value_type)");

        batch_type b1 = xsimd::load_as<xtl_value_type>(tmp.data(), xsimd::aligned_mode());
        EXPECT_EQ(b1, tmp) << print_function_name("load_as<value_type> aligned");

        batch_type b2 = xsimd::load_as<xtl_value_type>(tmp.data(), xsimd::unaligned_mode());
        EXPECT_EQ(b2, tmp) << print_function_name("load_as<value_type> unaligned");

        xsimd::store_as(tmp.data(), b1, xsimd::aligned_mode());
        EXPECT_EQ(b1, tmp) << print_function_name("store_as<value_type> aligned");

        xsimd::store_as(tmp.data(), b2, xsimd::unaligned_mode());
        EXPECT_EQ(b2, tmp) << print_function_name("store_as<value_type> unaligned");
    }
#endif

    void test_constructors() const
    {
        array_type tmp;
        std::fill(tmp.begin(), tmp.end(), value_type(2, 3));
        batch_type b0a(value_type(2, 3));
        EXPECT_EQ(b0a, tmp) << print_function_name("batch(value_type)");

        batch_type b0b(value_type(2, 3));
        EXPECT_EQ(b0b, tmp) << print_function_name("batch{value_type}");

        std::fill(tmp.begin(), tmp.end(), value_type(real_scalar));
        batch_type b1(real_scalar);
        EXPECT_EQ(b1, tmp) << print_function_name("batch(real_value_type)");

        real_array_type real, imag;
        for (size_t i = 0; i < size; ++i)
        {
            real[i] = lhs[i].real();
            imag[i] = lhs[i].imag();
            tmp[i] = value_type(real[i]);
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

        // batch + real_batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l + r.real(); });
            batch_type lres = batch_lhs() + batch_rhs().real();
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch + real_batch");
            batch_type rres = batch_rhs().real() + batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("real_batch + batch");
        }
        // batch + real_scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::plus<value_type>(), _1, real_scalar));
            batch_type lres = batch_lhs() + real_scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch + real_scalar");
            batch_type rres = real_scalar + batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("real_scalar + batch");
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
        // batch - real_batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l - r.real(); });
            batch_type lres = batch_lhs() - batch_rhs().real();
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch - real_batch");
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return r.real() - l; });
            batch_type rres = batch_rhs().real() - batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("real_batch - batch");
        }
        // batch - real_scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::minus<value_type>(), _1, real_scalar));
            batch_type lres = batch_lhs() - real_scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch - real_scalar");
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::minus<value_type>(), real_scalar, _1));
            batch_type rres = real_scalar - batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("real_scalar - batch");
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
        // batch * real_batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l * r.real(); });
            batch_type lres = batch_lhs() * batch_rhs().real();
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch * real_batch");
            batch_type rres = batch_rhs().real() * batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("real_batch * batch");
        }
        // batch * real_scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::multiplies<value_type>(), _1, real_scalar));
            batch_type lres = batch_lhs() * real_scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch * real_scalar");
            batch_type rres = real_scalar * batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("real_scalar * batch");
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
        // batch / real_batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l / r.real(); });
            batch_type lres = batch_lhs() / batch_rhs().real();
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch / real_batch");
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return r.real() / l; });
            batch_type rres = batch_rhs().real() / batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("real_batch / batch");
        }
        // batch - real_scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::divides<value_type>(), _1, real_scalar));
            batch_type lres = batch_lhs() / real_scalar;
            EXPECT_BATCH_EQ(lres, expected) << print_function_name("batch / real_scalar");
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::divides<value_type>(), real_scalar, _1));
            batch_type rres = real_scalar / batch_lhs();
            EXPECT_BATCH_EQ(rres, expected) << print_function_name("real_scalar / batch");
        }
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
        // batch += real_batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l + r.real(); });
            batch_type res = batch_lhs();
            res += batch_rhs().real();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch += real_batch");
        }
        // batch += real_scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::plus<value_type>(), _1, real_scalar));
            batch_type res = batch_lhs();
            res += real_scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch += real_scalar");
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
        // batch -= real_batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l - r.real(); });
            batch_type res = batch_lhs();
            res -= batch_rhs().real();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch -= real_batch");
        }
        // batch -= real_scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::minus<value_type>(), _1, real_scalar));
            batch_type res = batch_lhs();
            res -= real_scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch -= real_scalar");
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
        // batch *= real_batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l * r.real(); });
            batch_type res = batch_lhs();
            res *= batch_rhs().real();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch *= real_batch");
        }
        // batch *= real_scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::multiplies<value_type>(), _1, real_scalar));
            batch_type res = batch_lhs();
            res *= real_scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch *= real_scalar");
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
        // batch /= real_batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.cbegin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l / r.real(); });
            batch_type res = batch_lhs();
            res /= batch_rhs().real();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch /= real_batch");
        }
        // batch /= real_scalar
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(), std::bind(std::divides<value_type>(), _1, real_scalar));
            batch_type res = batch_lhs();
            res /= real_scalar;
            EXPECT_BATCH_EQ(res, expected) << print_function_name("batch /= real_scalar");
        }
    }

    void test_conj_norm_proj() const
    {
        // conj
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& v)
                           { using std::conj; return conj(v); });
            batch_type res = conj(batch_lhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("conj");
        }
        // norm
        {
            real_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& v)
                           { using std::norm; return norm(v); });
            real_batch_type res = norm(batch_lhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("norm");
        }
        // proj
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& v)
                           { using std::proj; return proj(v); });
            batch_type res = proj(batch_lhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("proj");
        }
    }

    void test_conj_norm_proj_real() const
    {
        // conj real batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::conj(std::real(v)); });
            batch_type res = conj(real(batch_lhs()));
            EXPECT_BATCH_EQ(res, expected) << print_function_name("conj(real batch)");
        }
        // norm real batch
        {
            real_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::norm(std::real(v)); });
            real_batch_type res = norm(real(batch_lhs()));
            EXPECT_BATCH_EQ(res, expected) << print_function_name("norm(real_batch)");
        }
        // proj real batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& v)
                           { return std::proj(std::real(v)); });
            batch_type res = proj(real(batch_lhs()));
            EXPECT_BATCH_EQ(res, expected) << print_function_name("proj(real_batch)");
        }
    }

    void test_polar() const
    {
        // polar w/ magnitude/phase
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.begin(), expected.begin(),
                           [](const value_type& v_lhs, const value_type& v_rhs)
                           { return std::polar(std::real(v_lhs), std::real(v_rhs)); });
            batch_type res = polar(real(batch_lhs()), real(batch_rhs()));
            EXPECT_BATCH_EQ(res, expected) << print_function_name("polar");
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
    }

    void test_fused_operations() const
    {
        // fma
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), rhs.begin(), expected.begin(),
                           [](const value_type& l, const value_type& r)
                           { return l * r + r; });
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

    void test_boolean_conversion() const
    {
        // !batch
        {
            array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& l)
                           { return l == value_type(0); });
            batch_type res = (batch_type)!batch_lhs();
            EXPECT_BATCH_EQ(res, expected) << print_function_name("!batch");
        }
    }

    void test_isnan() const
    {
        {
            bool_array_type expected;
            std::transform(lhs.cbegin(), lhs.cend(), expected.begin(),
                           [](const value_type& l)
                           { return std::isnan(l.real()) || std::isnan(l.imag()); });
            typename batch_type::batch_bool_type res = isnan(batch_lhs());
            EXPECT_BATCH_EQ(res, expected) << print_function_name("isnan");
        }
    }

private:
    batch_type batch_lhs() const
    {
        batch_type res = batch_type::load_unaligned(lhs.data());
        return res;
    }

    batch_type batch_rhs() const
    {
        batch_type res = batch_type::load_unaligned(rhs.data());
        return res;
    }
};

TYPED_TEST_SUITE(batch_complex_test, batch_complex_types, simd_test_names);

TYPED_TEST(batch_complex_test, load_store)
{
    this->test_load_store();
}

#ifdef XSIMD_ENABLE_XTL_COMPLEX
TYPED_TEST(batch_complex_test, load_store_xtl)
{
    this->test_load_store_xtl();
}
#endif

TYPED_TEST(batch_complex_test, constructors)
{
    this->test_constructors();
}

TYPED_TEST(batch_complex_test, access_operator)
{
    this->test_access_operator();
}

TYPED_TEST(batch_complex_test, arithmetic)
{
    this->test_arithmetic();
}

TYPED_TEST(batch_complex_test, computed_assignment)
{
    this->test_computed_assignment();
}

TYPED_TEST(batch_complex_test, conj_norm_proj)
{
    this->test_conj_norm_proj();
}

TYPED_TEST(batch_complex_test, conj_norm_proj_real)
{
    this->test_conj_norm_proj_real();
}

TYPED_TEST(batch_complex_test, polar)
{
    this->test_polar();
}

TYPED_TEST(batch_complex_test, horizontal_operations)
{
    this->test_horizontal_operations();
}

TYPED_TEST(batch_complex_test, fused_operations)
{
    this->test_fused_operations();
}

TYPED_TEST(batch_complex_test, boolean_conversion)
{
    this->test_boolean_conversion();
}

TYPED_TEST(batch_complex_test, isnan)
{
    this->test_isnan();
}
#endif

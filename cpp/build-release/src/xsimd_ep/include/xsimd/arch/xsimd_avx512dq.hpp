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

#ifndef XSIMD_AVX512_DQHPP
#define XSIMD_AVX512_D_HPP

#include "../types/xsimd_avx512dq_register.hpp"

namespace xsimd
{

    namespace kernel
    {
        using namespace types;

        // bitwise_and
        template <class A>
        inline batch<float, A> bitwise_and(batch<float, A> const& self, batch<float, A> const& other, requires_arch<avx512dq>) noexcept
        {
            return _mm512_and_ps(self, other);
        }
        template <class A>
        inline batch<double, A> bitwise_and(batch<double, A> const& self, batch<double, A> const& other, requires_arch<avx512dq>) noexcept
        {
            return _mm512_and_pd(self, other);
        }

        // bitwise_andnot
        template <class A>
        inline batch<float, A> bitwise_andnot(batch<float, A> const& self, batch<float, A> const& other, requires_arch<avx512dq>) noexcept
        {
            return _mm512_andnot_ps(other, self);
        }
        template <class A>
        inline batch<double, A> bitwise_andnot(batch<double, A> const& self, batch<double, A> const& other, requires_arch<avx512dq>) noexcept
        {
            return _mm512_andnot_pd(other, self);
        }

        // bitwise_or
        template <class A>
        inline batch<float, A> bitwise_or(batch<float, A> const& self, batch<float, A> const& other, requires_arch<avx512dq>) noexcept
        {
            return _mm512_or_ps(self, other);
        }
        template <class A>
        inline batch<double, A> bitwise_or(batch<double, A> const& self, batch<double, A> const& other, requires_arch<avx512dq>) noexcept
        {
            return _mm512_or_pd(self, other);
        }

        template <class A, class T>
        inline batch_bool<T, A> bitwise_or(batch_bool<T, A> const& self, batch_bool<T, A> const& other, requires_arch<avx512dq>) noexcept
        {
            using register_type = typename batch_bool<T, A>::register_type;
            return register_type(self.data | other.data);
        }

        // bitwise_xor
        template <class A>
        inline batch<float, A> bitwise_xor(batch<float, A> const& self, batch<float, A> const& other, requires_arch<avx512dq>) noexcept
        {
            return _mm512_xor_ps(self, other);
        }
        template <class A>
        inline batch<double, A> bitwise_xor(batch<double, A> const& self, batch<double, A> const& other, requires_arch<avx512dq>) noexcept
        {
            return _mm512_xor_pd(self, other);
        }

        // nearbyint_as_int
        template <class A>
        inline batch<int64_t, A> nearbyint_as_int(batch<double, A> const& self,
                                                  requires_arch<avx512dq>) noexcept
        {
            return _mm512_cvtpd_epi64(self);
        }

        // convert
        namespace detail
        {
            template <class A>
            inline batch<double, A> fast_cast(batch<int64_t, A> const& x, batch<double, A> const&, requires_arch<avx512dq>) noexcept
            {
                return _mm512_cvtepi64_pd(self);
            }

            template <class A>
            inline batch<int64_t, A> fast_cast(batch<double, A> const& self, batch<int64_t, A> const&, requires_arch<avx512dq>) noexcept
            {
                return _mm512_cvttpd_epi64(self);
            }

        }

    }

}

#endif

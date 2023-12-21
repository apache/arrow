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

#ifndef XSIMD_AVX2_HPP
#define XSIMD_AVX2_HPP

#include <complex>
#include <type_traits>

#include "../types/xsimd_avx2_register.hpp"

namespace xsimd
{

    namespace kernel
    {
        using namespace types;

        // abs
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> abs(batch<T, A> const& self, requires_arch<avx2>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_abs_epi8(self);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_abs_epi16(self);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_abs_epi32(self);
                }
                else
                {
                    return abs(self, avx {});
                }
            }
            return self;
        }

        // add
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> add(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm256_add_epi8(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm256_add_epi16(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_add_epi32(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                return _mm256_add_epi64(self, other);
            }
            else
            {
                return add(self, other, avx {});
            }
        }

        // bitwise_and
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_and(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            return _mm256_and_si256(self, other);
        }
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> bitwise_and(batch_bool<T, A> const& self, batch_bool<T, A> const& other, requires_arch<avx2>) noexcept
        {
            return _mm256_and_si256(self, other);
        }

        // bitwise_andnot
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_andnot(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            return _mm256_andnot_si256(other, self);
        }
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> bitwise_andnot(batch_bool<T, A> const& self, batch_bool<T, A> const& other, requires_arch<avx2>) noexcept
        {
            return _mm256_andnot_si256(other, self);
        }

        // bitwise_not
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_not(batch<T, A> const& self, requires_arch<avx2>) noexcept
        {
            return _mm256_xor_si256(self, _mm256_set1_epi32(-1));
        }
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> bitwise_not(batch_bool<T, A> const& self, requires_arch<avx2>) noexcept
        {
            return _mm256_xor_si256(self, _mm256_set1_epi32(-1));
        }

        // bitwise_lshift
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_lshift(batch<T, A> const& self, int32_t other, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm256_slli_epi16(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_slli_epi32(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                return _mm256_slli_epi64(self, other);
            }
            else
            {
                return bitwise_lshift(self, other, avx {});
            }
        }

        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_lshift(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_sllv_epi32(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                return _mm256_sllv_epi64(self, other);
            }
            else
            {
                return bitwise_lshift(self, other, avx {});
            }
        }

        // bitwise_or
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_or(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            return _mm256_or_si256(self, other);
        }
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> bitwise_or(batch_bool<T, A> const& self, batch_bool<T, A> const& other, requires_arch<avx2>) noexcept
        {
            return _mm256_or_si256(self, other);
        }

        // bitwise_rshift
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_rshift(batch<T, A> const& self, int32_t other, requires_arch<avx2>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    __m256i sign_mask = _mm256_set1_epi16((0xFF00 >> other) & 0x00FF);
                    __m256i cmp_is_negative = _mm256_cmpgt_epi8(_mm256_setzero_si256(), self);
                    __m256i res = _mm256_srai_epi16(self, other);
                    return _mm256_or_si256(
                        detail::fwd_to_sse([](__m128i s, __m128i o) noexcept
                                           { return bitwise_and(batch<T, sse4_2>(s), batch<T, sse4_2>(o), sse4_2 {}); },
                                           sign_mask, cmp_is_negative),
                        _mm256_andnot_si256(sign_mask, res));
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_srai_epi16(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_srai_epi32(self, other);
                }
                else
                {
                    return bitwise_rshift(self, other, avx {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_srli_epi16(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_srli_epi32(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
                {
                    return _mm256_srli_epi64(self, other);
                }
                else
                {
                    return bitwise_rshift(self, other, avx {});
                }
            }
        }

        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_rshift(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_srav_epi32(self, other);
                }
                else
                {
                    return bitwise_rshift(self, other, avx {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_srlv_epi32(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
                {
                    return _mm256_srlv_epi64(self, other);
                }
                else
                {
                    return bitwise_rshift(self, other, avx {});
                }
            }
        }

        // bitwise_xor
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_xor(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            return _mm256_xor_si256(self, other);
        }
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_xor(batch_bool<T, A> const& self, batch_bool<T, A> const& other, requires_arch<avx2>) noexcept
        {
            return _mm256_xor_si256(self, other);
        }

        // complex_low
        template <class A>
        inline batch<double, A> complex_low(batch<std::complex<double>, A> const& self, requires_arch<avx2>) noexcept
        {
            __m256d tmp0 = _mm256_permute4x64_pd(self.real(), _MM_SHUFFLE(3, 1, 1, 0));
            __m256d tmp1 = _mm256_permute4x64_pd(self.imag(), _MM_SHUFFLE(1, 2, 0, 0));
            return _mm256_blend_pd(tmp0, tmp1, 10);
        }

        // complex_high
        template <class A>
        inline batch<double, A> complex_high(batch<std::complex<double>, A> const& self, requires_arch<avx2>) noexcept
        {
            __m256d tmp0 = _mm256_permute4x64_pd(self.real(), _MM_SHUFFLE(3, 3, 1, 2));
            __m256d tmp1 = _mm256_permute4x64_pd(self.imag(), _MM_SHUFFLE(3, 2, 2, 0));
            return _mm256_blend_pd(tmp0, tmp1, 10);
        }

        // fast_cast
        namespace detail
        {

            template <class A>
            inline batch<float, A> fast_cast(batch<uint32_t, A> const& v, batch<float, A> const&, requires_arch<avx2>) noexcept
            {
                // see https://stackoverflow.com/questions/34066228/how-to-perform-uint32-float-conversion-with-sse
                __m256i msk_lo = _mm256_set1_epi32(0xFFFF);
                __m256 cnst65536f = _mm256_set1_ps(65536.0f);

                __m256i v_lo = _mm256_and_si256(v, msk_lo); /* extract the 16 lowest significant bits of self                             */
                __m256i v_hi = _mm256_srli_epi32(v, 16); /* 16 most significant bits of v                                                 */
                __m256 v_lo_flt = _mm256_cvtepi32_ps(v_lo); /* No rounding                                                                   */
                __m256 v_hi_flt = _mm256_cvtepi32_ps(v_hi); /* No rounding                                                                   */
                v_hi_flt = _mm256_mul_ps(cnst65536f, v_hi_flt); /* No rounding                                                                   */
                return _mm256_add_ps(v_hi_flt, v_lo_flt); /* Rounding may occur here, mul and add may fuse to fma for haswell and newer    */
            }

            template <class A>
            inline batch<double, A> fast_cast(batch<uint64_t, A> const& x, batch<double, A> const&, requires_arch<avx2>) noexcept
            {
                // from https://stackoverflow.com/questions/41144668/how-to-efficiently-perform-double-int64-conversions-with-sse-avx
                // adapted to avx
                __m256i xH = _mm256_srli_epi64(x, 32);
                xH = _mm256_or_si256(xH, _mm256_castpd_si256(_mm256_set1_pd(19342813113834066795298816.))); //  2^84
                __m256i mask = _mm256_setr_epi16(0xFFFF, 0xFFFF, 0x0000, 0x0000, 0xFFFF, 0xFFFF, 0x0000, 0x0000,
                                                 0xFFFF, 0xFFFF, 0x0000, 0x0000, 0xFFFF, 0xFFFF, 0x0000, 0x0000);
                __m256i xL = _mm256_or_si256(_mm256_and_si256(mask, x), _mm256_andnot_si256(mask, _mm256_castpd_si256(_mm256_set1_pd(0x0010000000000000)))); //  2^52
                __m256d f = _mm256_sub_pd(_mm256_castsi256_pd(xH), _mm256_set1_pd(19342813118337666422669312.)); //  2^84 + 2^52
                return _mm256_add_pd(f, _mm256_castsi256_pd(xL));
            }

            template <class A>
            inline batch<double, A> fast_cast(batch<int64_t, A> const& x, batch<double, A> const&, requires_arch<avx2>) noexcept
            {
                // from https://stackoverflow.com/questions/41144668/how-to-efficiently-perform-double-int64-conversions-with-sse-avx
                // adapted to avx
                __m256i xH = _mm256_srai_epi32(x, 16);
                xH = _mm256_and_si256(xH, _mm256_setr_epi16(0x0000, 0x0000, 0xFFFF, 0xFFFF, 0x0000, 0x0000, 0xFFFF, 0xFFFF, 0x0000, 0x0000, 0xFFFF, 0xFFFF, 0x0000, 0x0000, 0xFFFF, 0xFFFF));
                xH = _mm256_add_epi64(xH, _mm256_castpd_si256(_mm256_set1_pd(442721857769029238784.))); //  3*2^67
                __m256i mask = _mm256_setr_epi16(0xFFFF, 0xFFFF, 0xFFFF, 0x0000, 0xFFFF, 0xFFFF, 0xFFFF, 0x0000,
                                                 0xFFFF, 0xFFFF, 0xFFFF, 0x0000, 0xFFFF, 0xFFFF, 0xFFFF, 0x0000);
                __m256i xL = _mm256_or_si256(_mm256_and_si256(mask, x), _mm256_andnot_si256(mask, _mm256_castpd_si256(_mm256_set1_pd(0x0010000000000000)))); //  2^52
                __m256d f = _mm256_sub_pd(_mm256_castsi256_pd(xH), _mm256_set1_pd(442726361368656609280.)); //  3*2^67 + 2^52
                return _mm256_add_pd(f, _mm256_castsi256_pd(xL));
            }
        }

        // eq
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> eq(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm256_cmpeq_epi8(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm256_cmpeq_epi16(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_cmpeq_epi32(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                return _mm256_cmpeq_epi64(self, other);
            }
            else
            {
                return eq(self, other, avx {});
            }
        }

        // gather
        template <class T, class A, class U, detail::enable_sized_integral_t<T, 4> = 0, detail::enable_sized_integral_t<U, 4> = 0>
        inline batch<T, A> gather(batch<T, A> const&, T const* src, batch<U, A> const& index,
                                  kernel::requires_arch<avx2>) noexcept
        {
            // scatter for this one is AVX512F+AVX512VL
            return _mm256_i32gather_epi32(reinterpret_cast<const int*>(src), index, sizeof(T));
        }

        template <class T, class A, class U, detail::enable_sized_integral_t<T, 8> = 0, detail::enable_sized_integral_t<U, 8> = 0>
        inline batch<T, A> gather(batch<T, A> const&, T const* src, batch<U, A> const& index,
                                  kernel::requires_arch<avx2>) noexcept
        {
            // scatter for this one is AVX512F+AVX512VL
            return _mm256_i64gather_epi64(reinterpret_cast<const long long int*>(src), index, sizeof(T));
        }

        template <class A, class U,
                  detail::enable_sized_integral_t<U, 4> = 0>
        inline batch<float, A> gather(batch<float, A> const&, float const* src,
                                      batch<U, A> const& index,
                                      kernel::requires_arch<avx2>) noexcept
        {
            // scatter for this one is AVX512F+AVX512VL
            return _mm256_i32gather_ps(src, index, sizeof(float));
        }

        template <class A, class U, detail::enable_sized_integral_t<U, 8> = 0>
        inline batch<double, A> gather(batch<double, A> const&, double const* src,
                                       batch<U, A> const& index,
                                       requires_arch<avx2>) noexcept
        {
            // scatter for this one is AVX512F+AVX512VL
            return _mm256_i64gather_pd(src, index, sizeof(double));
        }

        // gather: handmade conversions
        template <class A, class V, detail::enable_sized_integral_t<V, 4> = 0>
        inline batch<float, A> gather(batch<float, A> const&, double const* src,
                                      batch<V, A> const& index,
                                      requires_arch<avx2>) noexcept
        {
            const batch<double, A> low(_mm256_i32gather_pd(src, _mm256_castsi256_si128(index.data), sizeof(double)));
            const batch<double, A> high(_mm256_i32gather_pd(src, _mm256_extractf128_si256(index.data, 1), sizeof(double)));
            return detail::merge_sse(_mm256_cvtpd_ps(low.data), _mm256_cvtpd_ps(high.data));
        }

        template <class A, class V, detail::enable_sized_integral_t<V, 4> = 0>
        inline batch<int32_t, A> gather(batch<int32_t, A> const&, double const* src,
                                        batch<V, A> const& index,
                                        requires_arch<avx2>) noexcept
        {
            const batch<double, A> low(_mm256_i32gather_pd(src, _mm256_castsi256_si128(index.data), sizeof(double)));
            const batch<double, A> high(_mm256_i32gather_pd(src, _mm256_extractf128_si256(index.data, 1), sizeof(double)));
            return detail::merge_sse(_mm256_cvtpd_epi32(low.data), _mm256_cvtpd_epi32(high.data));
        }

        // lt
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> lt(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_cmpgt_epi8(other, self);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_cmpgt_epi16(other, self);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_cmpgt_epi32(other, self);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
                {
                    return _mm256_cmpgt_epi64(other, self);
                }
                else
                {
                    return lt(self, other, avx {});
                }
            }
            else
            {
                return lt(self, other, avx {});
            }
        }

        // load_complex
        template <class A>
        inline batch<std::complex<float>, A> load_complex(batch<float, A> const& hi, batch<float, A> const& lo, requires_arch<avx2>) noexcept
        {
            using batch_type = batch<float, A>;
            batch_type real = _mm256_castpd_ps(
                _mm256_permute4x64_pd(
                    _mm256_castps_pd(_mm256_shuffle_ps(hi, lo, _MM_SHUFFLE(2, 0, 2, 0))),
                    _MM_SHUFFLE(3, 1, 2, 0)));
            batch_type imag = _mm256_castpd_ps(
                _mm256_permute4x64_pd(
                    _mm256_castps_pd(_mm256_shuffle_ps(hi, lo, _MM_SHUFFLE(3, 1, 3, 1))),
                    _MM_SHUFFLE(3, 1, 2, 0)));
            return { real, imag };
        }
        template <class A>
        inline batch<std::complex<double>, A> load_complex(batch<double, A> const& hi, batch<double, A> const& lo, requires_arch<avx2>) noexcept
        {
            using batch_type = batch<double, A>;
            batch_type real = _mm256_permute4x64_pd(_mm256_unpacklo_pd(hi, lo), _MM_SHUFFLE(3, 1, 2, 0));
            batch_type imag = _mm256_permute4x64_pd(_mm256_unpackhi_pd(hi, lo), _MM_SHUFFLE(3, 1, 2, 0));
            return { real, imag };
        }
        // mask
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline uint64_t mask(batch_bool<T, A> const& self, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return 0xFFFFFFFF & (uint64_t)_mm256_movemask_epi8(self);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                uint64_t mask8 = 0xFFFFFFFF & (uint64_t)_mm256_movemask_epi8(self);
                return detail::mask_lut(mask8) | (detail::mask_lut(mask8 >> 8) << 4) | (detail::mask_lut(mask8 >> 16) << 8) | (detail::mask_lut(mask8 >> 24) << 12);
            }
            else
            {
                return mask(self, avx {});
            }
        }

        // max
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> max(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_max_epi8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_max_epi16(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_max_epi32(self, other);
                }
                else
                {
                    return max(self, other, avx {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_max_epu8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_max_epu16(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_max_epu32(self, other);
                }
                else
                {
                    return max(self, other, avx {});
                }
            }
        }

        // min
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> min(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_min_epi8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_min_epi16(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_min_epi32(self, other);
                }
                else
                {
                    return min(self, other, avx {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_min_epu8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_min_epu16(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                {
                    return _mm256_min_epu32(self, other);
                }
                else
                {
                    return min(self, other, avx {});
                }
            }
        }

        // mul
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> mul(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm256_mullo_epi16(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_mullo_epi32(self, other);
            }
            else
            {
                return mul(self, other, avx {});
            }
        }

        // reduce_add
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline T reduce_add(batch<T, A> const& self, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                __m256i tmp1 = _mm256_hadd_epi32(self, self);
                __m256i tmp2 = _mm256_hadd_epi32(tmp1, tmp1);
                __m128i tmp3 = _mm256_extracti128_si256(tmp2, 1);
                __m128i tmp4 = _mm_add_epi32(_mm256_castsi256_si128(tmp2), tmp3);
                return _mm_cvtsi128_si32(tmp4);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                __m256i tmp1 = _mm256_shuffle_epi32(self, 0x0E);
                __m256i tmp2 = _mm256_add_epi64(self, tmp1);
                __m128i tmp3 = _mm256_extracti128_si256(tmp2, 1);
                __m128i res = _mm_add_epi64(_mm256_castsi256_si128(tmp2), tmp3);
#if defined(__x86_64__)
                return _mm_cvtsi128_si64(res);
#else
                __m128i m;
                _mm_storel_epi64(&m, res);
                int64_t i;
                std::memcpy(&i, &m, sizeof(i));
                return i;
#endif
            }
            else
            {
                return reduce_add(self, avx {});
            }
        }

        // sadd
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> sadd(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_adds_epi8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_adds_epi16(self, other);
                }
                else
                {
                    return sadd(self, other, avx {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_adds_epu8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_adds_epu16(self, other);
                }
                else
                {
                    return sadd(self, other, avx {});
                }
            }
        }

        // select
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> select(batch_bool<T, A> const& cond, batch<T, A> const& true_br, batch<T, A> const& false_br, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm256_blendv_epi8(false_br, true_br, cond);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm256_blendv_epi8(false_br, true_br, cond);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_blendv_epi8(false_br, true_br, cond);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                return _mm256_blendv_epi8(false_br, true_br, cond);
            }
            else
            {
                return select(cond, true_br, false_br, avx {});
            }
        }
        template <class A, class T, bool... Values, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> select(batch_bool_constant<batch<T, A>, Values...> const&, batch<T, A> const& true_br, batch<T, A> const& false_br, requires_arch<avx2>) noexcept
        {
            constexpr int mask = batch_bool_constant<batch<T, A>, Values...>::mask();
            // FIXME: for some reason mask here is not considered as an immediate,
            // but it's okay for _mm256_blend_epi32
            // case 2: return _mm256_blend_epi16(false_br, true_br, mask);
            XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_blend_epi32(false_br, true_br, mask);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                constexpr int imask = detail::interleave(mask);
                return _mm256_blend_epi32(false_br, true_br, imask);
            }
            else
            {
                return select(batch_bool<T, A> { Values... }, true_br, false_br, avx2 {});
            }
        }

        // slide_left
        template <size_t N, class A, class T>
        inline batch<T, A> slide_left(batch<T, A> const& x, requires_arch<avx2>) noexcept
        {
            constexpr unsigned BitCount = N * 8;
            if (BitCount == 0)
            {
                return x;
            }
            if (BitCount >= 256)
            {
                return batch<T, A>(T(0));
            }
            if (BitCount > 128)
            {
                constexpr unsigned M = (BitCount - 128) / 8;
                auto y = _mm256_bslli_epi128(x, M);
                return _mm256_permute2x128_si256(y, y, 0x28);
            }
            if (BitCount == 128)
            {
                return _mm256_permute2x128_si256(x, x, 0x28);
            }
            // shifting by [0, 128[ bits
            constexpr unsigned M = BitCount / 8;
            auto y = _mm256_bslli_epi128(x, M);
            auto z = _mm256_bsrli_epi128(x, 16 - M);
            auto w = _mm256_permute2x128_si256(z, z, 0x28);
            return _mm256_or_si256(y, w);
        }

        // slide_right
        template <size_t N, class A, class T>
        inline batch<T, A> slide_right(batch<T, A> const& x, requires_arch<avx2>) noexcept
        {
            constexpr unsigned BitCount = N * 8;
            if (BitCount == 0)
            {
                return x;
            }
            if (BitCount >= 256)
            {
                return batch<T, A>(T(0));
            }
            if (BitCount > 128)
            {
                constexpr unsigned M = (BitCount - 128) / 8;
                auto y = _mm256_bsrli_epi128(x, M);
                return _mm256_permute2x128_si256(y, y, 0x81);
            }
            if (BitCount == 128)
            {
                return _mm256_permute2x128_si256(x, x, 0x81);
            }
            // shifting by [0, 128[ bits
            constexpr unsigned M = BitCount / 8;
            auto y = _mm256_bsrli_epi128(x, M);
            auto z = _mm256_bslli_epi128(x, 16 - M);
            auto w = _mm256_permute2x128_si256(z, z, 0x81);
            return _mm256_or_si256(y, w);
        }

        // ssub
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> ssub(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_subs_epi8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_subs_epi16(self, other);
                }
                else
                {
                    return ssub(self, other, avx {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm256_subs_epu8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm256_subs_epu16(self, other);
                }
                else
                {
                    return ssub(self, other, avx {});
                }
            }
        }

        // sub
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> sub(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm256_sub_epi8(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm256_sub_epi16(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_sub_epi32(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                return _mm256_sub_epi64(self, other);
            }
            else
            {
                return sub(self, other, avx {});
            }
        }

        // swizzle
        template <class A, uint32_t V0, uint32_t V1, uint32_t V2, uint32_t V3, uint32_t V4, uint32_t V5, uint32_t V6, uint32_t V7>
        inline batch<float, A> swizzle(batch<float, A> const& self, batch_constant<batch<uint32_t, A>, V0, V1, V2, V3, V4, V5, V6, V7> mask, requires_arch<avx2>) noexcept
        {
            return _mm256_permutevar8x32_ps(self, (batch<uint32_t, A>)mask);
        }

        template <class A, uint64_t V0, uint64_t V1, uint64_t V2, uint64_t V3>
        inline batch<double, A> swizzle(batch<double, A> const& self, batch_constant<batch<uint64_t, A>, V0, V1, V2, V3>, requires_arch<avx2>) noexcept
        {
            constexpr auto mask = detail::shuffle(V0, V1, V2, V3);
            return _mm256_permute4x64_pd(self, mask);
        }

        template <class A, uint64_t V0, uint64_t V1, uint64_t V2, uint64_t V3>
        inline batch<uint64_t, A> swizzle(batch<uint64_t, A> const& self, batch_constant<batch<uint64_t, A>, V0, V1, V2, V3>, requires_arch<avx2>) noexcept
        {
            constexpr auto mask = detail::shuffle(V0, V1, V2, V3);
            return _mm256_permute4x64_epi64(self, mask);
        }
        template <class A, uint64_t V0, uint64_t V1, uint64_t V2, uint64_t V3>
        inline batch<int64_t, A> swizzle(batch<int64_t, A> const& self, batch_constant<batch<uint64_t, A>, V0, V1, V2, V3> mask, requires_arch<avx2>) noexcept
        {
            return bitwise_cast<batch<int64_t, A>>(swizzle(bitwise_cast<batch<uint64_t, A>>(self), mask, avx2 {}));
        }
        template <class A, uint32_t V0, uint32_t V1, uint32_t V2, uint32_t V3, uint32_t V4, uint32_t V5, uint32_t V6, uint32_t V7>
        inline batch<uint32_t, A> swizzle(batch<uint32_t, A> const& self, batch_constant<batch<uint32_t, A>, V0, V1, V2, V3, V4, V5, V6, V7> mask, requires_arch<avx2>) noexcept
        {
            return _mm256_permutevar8x32_epi32(self, (batch<uint32_t, A>)mask);
        }
        template <class A, uint32_t V0, uint32_t V1, uint32_t V2, uint32_t V3, uint32_t V4, uint32_t V5, uint32_t V6, uint32_t V7>
        inline batch<int32_t, A> swizzle(batch<int32_t, A> const& self, batch_constant<batch<uint32_t, A>, V0, V1, V2, V3, V4, V5, V6, V7> mask, requires_arch<avx2>) noexcept
        {
            return bitwise_cast<batch<int32_t, A>>(swizzle(bitwise_cast<batch<uint32_t, A>>(self), mask, avx2 {}));
        }

        // zip_hi
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> zip_hi(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm256_unpackhi_epi8(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm256_unpackhi_epi16(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_unpackhi_epi32(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                return _mm256_unpackhi_epi64(self, other);
            }
            else
            {
                assert(false && "unsupported arch/op combination");
                return {};
            }
        }

        // zip_lo
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> zip_lo(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx2>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm256_unpacklo_epi8(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm256_unpacklo_epi16(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
            {
                return _mm256_unpacklo_epi32(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
            {
                return _mm256_unpacklo_epi64(self, other);
            }
            else
            {
                assert(false && "unsupported arch/op combination");
                return {};
            }
        }
    }
}

#endif

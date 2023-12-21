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

#ifndef XSIMD_AVX512BW_HPP
#define XSIMD_AVX512BW_HPP

#include <array>
#include <type_traits>

#include "../types/xsimd_avx512bw_register.hpp"

namespace xsimd
{

    namespace kernel
    {
        using namespace types;

        namespace detail
        {
            template <class A, class T, int Cmp>
            inline batch_bool<T, A> compare_int_avx512bw(batch<T, A> const& self, batch<T, A> const& other) noexcept
            {
                using register_type = typename batch_bool<T, A>::register_type;
                if (std::is_signed<T>::value)
                {
                    XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                    {
                        return (register_type)_mm512_cmp_epi8_mask(self, other, Cmp);
                    }
                    else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                    {
                        return (register_type)_mm512_cmp_epi16_mask(self, other, Cmp);
                    }
                    else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                    {
                        return (register_type)_mm512_cmp_epi32_mask(self, other, Cmp);
                    }
                    else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
                    {
                        return (register_type)_mm512_cmp_epi64_mask(self, other, Cmp);
                    }
                }
                else
                {
                    XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                    {
                        return (register_type)_mm512_cmp_epu8_mask(self, other, Cmp);
                    }
                    else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                    {
                        return (register_type)_mm512_cmp_epu16_mask(self, other, Cmp);
                    }
                    else XSIMD_IF_CONSTEXPR(sizeof(T) == 4)
                    {
                        return (register_type)_mm512_cmp_epu32_mask(self, other, Cmp);
                    }
                    else XSIMD_IF_CONSTEXPR(sizeof(T) == 8)
                    {
                        return (register_type)_mm512_cmp_epu64_mask(self, other, Cmp);
                    }
                }
            }
        }

        // abs
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> abs(batch<T, A> const& self, requires_arch<avx512bw>) noexcept
        {
            if (std::is_unsigned<T>::value)
            {
                return self;
            }

            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm512_abs_epi8(self);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm512_abs_epi16(self);
            }
            else
            {
                return abs(self, avx512dq {});
            }
        }

        // add
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> add(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm512_add_epi8(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm512_add_epi16(self, other);
            }
            else
            {
                return add(self, other, avx512dq {});
            }
        }

        // bitwise_lshift
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_lshift(batch<T, A> const& self, int32_t other, requires_arch<avx512bw>) noexcept
        {
#if defined(XSIMD_AVX512_SHIFT_INTRINSICS_IMM_ONLY)
            XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm512_sllv_epi16(self, _mm512_set1_epi16(other));
#else
            XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm512_slli_epi16(self, other);
#endif
            }
            else
            {
                return bitwise_lshift(self, other, avx512dq {});
            }
        }

        // bitwise_rshift
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> bitwise_rshift(batch<T, A> const& self, int32_t other, requires_arch<avx512bw>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    __m512i sign_mask = _mm512_set1_epi16((0xFF00 >> other) & 0x00FF);
                    __m512i zeros = _mm512_setzero_si512();
                    __mmask64 cmp_is_negative_mask = _mm512_cmpgt_epi8_mask(zeros, self);
                    __m512i cmp_sign_mask = _mm512_mask_blend_epi8(cmp_is_negative_mask, zeros, sign_mask);
#if defined(XSIMD_AVX512_SHIFT_INTRINSICS_IMM_ONLY)
                    __m512i res = _mm512_srav_epi16(self, _mm512_set1_epi16(other));
#else
                    __m512i res = _mm512_srai_epi16(self, other);
#endif
                    return _mm512_or_si512(cmp_sign_mask, _mm512_andnot_si512(sign_mask, res));
#if defined(XSIMD_AVX512_SHIFT_INTRINSICS_IMM_ONLY)
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_srav_epi16(self, _mm512_set1_epi16(other));
#else
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_srai_epi16(self, other);
#endif
                }
                else
                {
                    return bitwise_rshift(self, other, avx512dq {});
                }
            }
            else
            {
#if defined(XSIMD_AVX512_SHIFT_INTRINSICS_IMM_ONLY)
                XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_srlv_epi16(self, _mm512_set1_epi16(other));
#else
                XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_srli_epi16(self, other);
#endif
                }
                else
                {
                    return bitwise_rshift(self, other, avx512dq {});
                }
            }
        }

        // eq
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> eq(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            return detail::compare_int_avx512bw<A, T, _MM_CMPINT_EQ>(self, other);
        }

        // ge
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> ge(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            return detail::compare_int_avx512bw<A, T, _MM_CMPINT_GE>(self, other);
        }

        // gt
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> gt(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            return detail::compare_int_avx512bw<A, T, _MM_CMPINT_GT>(self, other);
        }

        // le
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> le(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            return detail::compare_int_avx512bw<A, T, _MM_CMPINT_LE>(self, other);
        }

        // lt
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> lt(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            return detail::compare_int_avx512bw<A, T, _MM_CMPINT_LT>(self, other);
        }

        // max
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> max(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm512_max_epi8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_max_epi16(self, other);
                }
                else
                {
                    return max(self, other, avx512dq {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm512_max_epu8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_max_epu16(self, other);
                }
                else
                {
                    return max(self, other, avx512dq {});
                }
            }
        }

        // min
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> min(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm512_min_epi8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_min_epi16(self, other);
                }
                else
                {
                    return min(self, other, avx512dq {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm512_min_epu8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_min_epu16(self, other);
                }
                else
                {
                    return min(self, other, avx512dq {});
                }
            }
        }

        // mul
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> mul(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                __m512i upper = _mm512_and_si512(_mm512_mullo_epi16(self, other), _mm512_srli_epi16(_mm512_set1_epi16(-1), 8));
                __m512i lower = _mm512_slli_epi16(_mm512_mullo_epi16(_mm512_srli_epi16(self, 8), _mm512_srli_epi16(other, 8)), 8);
                return _mm512_or_si512(upper, lower);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm512_mullo_epi16(self, other);
            }
            else
            {
                return mul(self, other, avx512dq {});
            }
        }

        // neq
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch_bool<T, A> neq(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            return detail::compare_int_avx512bw<A, T, _MM_CMPINT_NE>(self, other);
        }

        // sadd
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> sadd(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm512_adds_epi8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_adds_epi16(self, other);
                }
                else
                {
                    return sadd(self, other, avx512dq {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm512_adds_epu8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_adds_epu16(self, other);
                }
                else
                {
                    return sadd(self, other, avx512dq {});
                }
            }
        }

        // select
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> select(batch_bool<T, A> const& cond, batch<T, A> const& true_br, batch<T, A> const& false_br, requires_arch<avx512bw>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm512_mask_blend_epi8(cond, false_br, true_br);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm512_mask_blend_epi16(cond, false_br, true_br);
            }
            else
            {
                return select(cond, true_br, false_br, avx512dq {});
            }
        }

        // slide_left
        namespace detail
        {
            template <size_t... Is>
            constexpr std::array<uint64_t, sizeof...(Is)> make_slide_perm_hi(::xsimd::detail::index_sequence<Is...>)
            {
                return { (Is == 0 ? 8 : Is - 1)... };
            }

            template <size_t N, size_t... Is>
            constexpr std::array<uint16_t, sizeof...(Is)> make_slide_left_pattern(::xsimd::detail::index_sequence<Is...>)
            {
                return { (Is >= N ? Is - N : 0)... };
            }
            template <size_t N, size_t... Is>
            constexpr std::array<uint16_t, sizeof...(Is)> make_slide_left_mask(::xsimd::detail::index_sequence<Is...>)
            {
                return { (Is >= N ? 0xFFFF : 0x0000)... };
            }
        }

        template <size_t N, class A, class T>
        inline batch<T, A> slide_left(batch<T, A> const& x, requires_arch<avx512bw>) noexcept
        {
            constexpr unsigned BitCount = N * 8;
            if (BitCount == 0)
            {
                return x;
            }
            if (BitCount >= 512)
            {
                return batch<T, A>(T(0));
            }
            batch<T, A> xx;
            if (N & 1)
            {
                alignas(32) uint64_t buffer[8];
                _mm512_store_epi64(&buffer[0], x);
                for (int i = 7; i > 0; --i)
                    buffer[i] = (buffer[i] << 8) | (buffer[i - 1] >> 56);
                buffer[0] = buffer[0] << 8;
                xx = _mm512_load_epi64(&buffer[0]);

                alignas(32) auto slide_perm = detail::make_slide_perm_hi(::xsimd::detail::make_index_sequence<512 / 64>());
                __m512i xl = _mm512_slli_epi64(x, 8);
                __m512i xr = _mm512_srli_epi64(x, 56);
                xr = _mm512_permutex2var_epi64(xr, _mm512_load_epi64(slide_perm.data()), _mm512_setzero_si512());
                xx = _mm512_or_si512(xr, xl);
                if (N == 1)
                    return xx;
            }
            else
            {
                xx = x;
            }
            alignas(32) auto slide_pattern = detail::make_slide_left_pattern<N / 2>(::xsimd::detail::make_index_sequence<512 / 16>());
            alignas(32) auto slide_mask = detail::make_slide_left_mask<N / 2>(::xsimd::detail::make_index_sequence<512 / 16>());
            return _mm512_and_si512(_mm512_permutexvar_epi16(_mm512_load_epi32(slide_pattern.data()), xx), _mm512_load_epi32(slide_mask.data()));
        }

        // slide_right
        namespace detail
        {
            template <size_t... Is>
            constexpr std::array<uint64_t, sizeof...(Is)> make_slide_perm_low(::xsimd::detail::index_sequence<Is...>)
            {
                return { (Is + 1)... };
            }

            template <size_t N, size_t... Is>
            constexpr std::array<uint16_t, sizeof...(Is)> make_slide_right_pattern(::xsimd::detail::index_sequence<Is...>)
            {
                return { (Is < (32 - N) ? Is + N : 0)... };
            }
            template <size_t N, size_t... Is>
            constexpr std::array<uint16_t, sizeof...(Is)> make_slide_right_mask(::xsimd::detail::index_sequence<Is...>)
            {
                return { (Is < 32 - N ? 0xFFFF : 0x0000)... };
            }
        }
        template <size_t N, class A, class T>
        inline batch<T, A> slide_right(batch<T, A> const& x, requires_arch<avx512bw>) noexcept
        {
            constexpr unsigned BitCount = N * 8;
            if (BitCount == 0)
            {
                return x;
            }
            if (BitCount >= 512)
            {
                return batch<T, A>(T(0));
            }
            batch<T, A> xx;
            if (N & 1)
            {
                alignas(32) auto slide_perm = detail::make_slide_perm_low(::xsimd::detail::make_index_sequence<512 / 64>());
                __m512i xr = _mm512_srli_epi64(x, 8);
                __m512i xl = _mm512_slli_epi64(x, 56);
                xl = _mm512_permutex2var_epi64(xl, _mm512_load_epi64(slide_perm.data()), _mm512_setzero_si512());
                xx = _mm512_or_si512(xr, xl);
                if (N == 1)
                    return xx;
            }
            else
            {
                xx = x;
            }
            alignas(32) auto slide_pattern = detail::make_slide_right_pattern<N / 2>(::xsimd::detail::make_index_sequence<512 / 16>());
            alignas(32) auto slide_mask = detail::make_slide_right_mask<N / 2>(::xsimd::detail::make_index_sequence<512 / 16>());
            return _mm512_and_si512(_mm512_permutexvar_epi16(_mm512_load_epi32(slide_pattern.data()), xx), _mm512_load_epi32(slide_mask.data()));
        }

        // ssub
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> ssub(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            if (std::is_signed<T>::value)
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm512_subs_epi8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_subs_epi16(self, other);
                }
                else
                {
                    return ssub(self, other, avx512dq {});
                }
            }
            else
            {
                XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
                {
                    return _mm512_subs_epu8(self, other);
                }
                else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
                {
                    return _mm512_subs_epu16(self, other);
                }
                else
                {
                    return ssub(self, other, avx512dq {});
                }
            }
        }

        // sub
        template <class A, class T, class = typename std::enable_if<std::is_integral<T>::value, void>::type>
        inline batch<T, A> sub(batch<T, A> const& self, batch<T, A> const& other, requires_arch<avx512bw>) noexcept
        {
            XSIMD_IF_CONSTEXPR(sizeof(T) == 1)
            {
                return _mm512_sub_epi8(self, other);
            }
            else XSIMD_IF_CONSTEXPR(sizeof(T) == 2)
            {
                return _mm512_sub_epi16(self, other);
            }
            else
            {
                return sub(self, other, avx512dq {});
            }
        }

        // swizzle

        template <class A, uint16_t... Vs>
        inline batch<uint16_t, A> swizzle(batch<uint16_t, A> const& self, batch_constant<batch<uint16_t, A>, Vs...> mask, requires_arch<avx512bw>) noexcept
        {
            return _mm512_permutexvar_epi16((batch<uint16_t, A>)mask, self);
        }

        template <class A, uint16_t... Vs>
        inline batch<int16_t, A> swizzle(batch<int16_t, A> const& self, batch_constant<batch<uint16_t, A>, Vs...> mask, requires_arch<avx512bw>) noexcept
        {
            return bitwise_cast<batch<int16_t, A>>(swizzle(bitwise_cast<batch<uint16_t, A>>(self), mask, avx512bw {}));
        }

        template <class A, uint8_t... Vs>
        inline batch<uint8_t, A> swizzle(batch<uint8_t, A> const& self, batch_constant<batch<uint8_t, A>, Vs...> mask, requires_arch<avx512bw>) noexcept
        {
            return _mm512_shuffle_epi8(self, (batch<uint8_t, A>)mask);
        }

        template <class A, uint8_t... Vs>
        inline batch<int8_t, A> swizzle(batch<int8_t, A> const& self, batch_constant<batch<uint8_t, A>, Vs...> mask, requires_arch<avx512bw>) noexcept
        {
            return bitwise_cast<batch<int8_t, A>>(swizzle(bitwise_cast<batch<uint8_t, A>>(self), mask, avx512bw {}));
        }
    }
}

#endif

/***************************************************************************
 * Copyright (c) Johan Mabille, Sylvain Corlay, Wolf Vollprecht and         *
 * Martin Renou                                                             *
 * Copyright (c) QuantStack                                                 *
 * Copyright (c) Serge Guelton                                              *
 * Copyright (c) Yibo Cai                                                   *
 *                                                                          *
 * Distributed under the terms of the BSD 3-Clause License.                 *
 *                                                                          *
 * The full license is in the file LICENSE, distributed with this software. *
 ****************************************************************************/

#ifndef XSIMD_SVE_HPP
#define XSIMD_SVE_HPP

#include <complex>
#include <type_traits>

#include "../types/xsimd_sve_register.hpp"

namespace xsimd
{
    template <class batch_type, typename batch_type::value_type... Values>
    struct batch_constant;

    namespace kernel
    {
        namespace detail
        {
            using xsimd::index;
            using xsimd::types::detail::sve_vector_type;

            // predicate creation
            inline svbool_t sve_ptrue_impl(index<1>) noexcept { return svptrue_b8(); }
            inline svbool_t sve_ptrue_impl(index<2>) noexcept { return svptrue_b16(); }
            inline svbool_t sve_ptrue_impl(index<4>) noexcept { return svptrue_b32(); }
            inline svbool_t sve_ptrue_impl(index<8>) noexcept { return svptrue_b64(); }

            template <class T>
            svbool_t sve_ptrue() noexcept { return sve_ptrue_impl(index<sizeof(T)> {}); }

            template <class T>
            svbool_t sve_pfalse() noexcept { return svpfalse(); }

            // count active lanes in a predicate
            inline uint64_t sve_pcount_impl(svbool_t p, index<1>) noexcept { return svcntp_b8(p, p); }
            inline uint64_t sve_pcount_impl(svbool_t p, index<2>) noexcept { return svcntp_b16(p, p); }
            inline uint64_t sve_pcount_impl(svbool_t p, index<4>) noexcept { return svcntp_b32(p, p); }
            inline uint64_t sve_pcount_impl(svbool_t p, index<8>) noexcept { return svcntp_b64(p, p); }

            template <class T>
            inline uint64_t sve_pcount(svbool_t p) noexcept { return sve_pcount_impl(p, index<sizeof(T)> {}); }

            // enable for signed integers
            template <class T>
            using sve_enable_signed_int_t = typename std::enable_if<std::is_integral<T>::value && std::is_signed<T>::value, int>::type;

            // enable for unsigned integers
            template <class T>
            using sve_enable_unsigned_int_t = typename std::enable_if<std::is_integral<T>::value && !std::is_signed<T>::value, int>::type;

            // enable for floating points
            template <class T>
            using sve_enable_floating_point_t = typename std::enable_if<std::is_floating_point<T>::value, int>::type;

            // enable for signed integers or floating points
            template <class T>
            using sve_enable_signed_int_or_floating_point_t = typename std::enable_if<std::is_signed<T>::value, int>::type;

            // enable for all SVE supported types
            template <class T>
            using sve_enable_all_t = typename std::enable_if<std::is_arithmetic<T>::value, int>::type;
        } // namespace detail

        /*********
         * Load *
         *********/

        namespace detail
        {
            // "char" is not allowed in SVE load/store operations
            using sve_fix_char_t_impl = typename std::conditional<std::is_signed<char>::value, int8_t, uint8_t>::type;

            template <class T>
            using sve_fix_char_t = typename std::conditional<std::is_same<char, typename std::decay<T>::type>::value,
                                                             sve_fix_char_t_impl, T>::type;
        }

        // TODO: gather

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> load_aligned(T const* src, convert<T>, requires_arch<sve>) noexcept
        {
            return svld1(detail::sve_ptrue<T>(), reinterpret_cast<detail::sve_fix_char_t<T> const*>(src));
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> load_unaligned(T const* src, convert<T>, requires_arch<sve>) noexcept
        {
            return load_aligned<A>(src, convert<T>(), sve {});
        }

        // load_complex
        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<std::complex<T>, A> load_complex_aligned(std::complex<T> const* mem, convert<std::complex<T>>, requires_arch<sve>) noexcept
        {
            const T* buf = reinterpret_cast<const T*>(mem);
            const auto tmp = svld2(detail::sve_ptrue<T>(), buf);
            const auto real = svget2(tmp, 0);
            const auto imag = svget2(tmp, 1);
            return batch<std::complex<T>, A> { real, imag };
        }

        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<std::complex<T>, A> load_complex_unaligned(std::complex<T> const* mem, convert<std::complex<T>>, requires_arch<sve>) noexcept
        {
            return load_complex_aligned<A>(mem, convert<std::complex<T>> {}, sve {});
        }

        /*********
         * Store *
         *********/

        // TODO: scatter

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline void store_aligned(T* dst, batch<T, A> const& src, requires_arch<sve>) noexcept
        {
            svst1(detail::sve_ptrue<T>(), reinterpret_cast<detail::sve_fix_char_t<T>*>(dst), src);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline void store_unaligned(T* dst, batch<T, A> const& src, requires_arch<sve>) noexcept
        {
            store_aligned<A>(dst, src, sve {});
        }

        // store_complex
        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline void store_complex_aligned(std::complex<T>* dst, batch<std::complex<T>, A> const& src, requires_arch<sve>) noexcept
        {
            using v2type = typename std::conditional<(sizeof(T) == 4), svfloat32x2_t, svfloat64x2_t>::type;
            v2type tmp {};
            tmp = svset2(tmp, 0, src.real());
            tmp = svset2(tmp, 1, src.imag());
            T* buf = reinterpret_cast<T*>(dst);
            svst2(detail::sve_ptrue<T>(), buf, tmp);
        }

        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline void store_complex_unaligned(std::complex<T>* dst, batch<std::complex<T>, A> const& src, requires_arch<sve>) noexcept
        {
            store_complex_aligned(dst, src, sve {});
        }

        /********************
         * Scalar to vector *
         ********************/

        // broadcast
        template <class A, class T, detail::enable_sized_unsigned_t<T, 1> = 0>
        inline batch<T, A> broadcast(T arg, requires_arch<sve>) noexcept
        {
            return svdup_n_u8(uint8_t(arg));
        }

        template <class A, class T, detail::enable_sized_signed_t<T, 1> = 0>
        inline batch<T, A> broadcast(T arg, requires_arch<sve>) noexcept
        {
            return svdup_n_s8(int8_t(arg));
        }

        template <class A, class T, detail::enable_sized_unsigned_t<T, 2> = 0>
        inline batch<T, A> broadcast(T arg, requires_arch<sve>) noexcept
        {
            return svdup_n_u16(uint16_t(arg));
        }

        template <class A, class T, detail::enable_sized_signed_t<T, 2> = 0>
        inline batch<T, A> broadcast(T arg, requires_arch<sve>) noexcept
        {
            return svdup_n_s16(int16_t(arg));
        }

        template <class A, class T, detail::enable_sized_unsigned_t<T, 4> = 0>
        inline batch<T, A> broadcast(T arg, requires_arch<sve>) noexcept
        {
            return svdup_n_u32(uint32_t(arg));
        }

        template <class A, class T, detail::enable_sized_signed_t<T, 4> = 0>
        inline batch<T, A> broadcast(T arg, requires_arch<sve>) noexcept
        {
            return svdup_n_s32(int32_t(arg));
        }

        template <class A, class T, detail::enable_sized_unsigned_t<T, 8> = 0>
        inline batch<T, A> broadcast(T arg, requires_arch<sve>) noexcept
        {
            return svdup_n_u64(uint64_t(arg));
        }

        template <class A, class T, detail::enable_sized_signed_t<T, 8> = 0>
        inline batch<T, A> broadcast(T arg, requires_arch<sve>) noexcept
        {
            return svdup_n_s64(int64_t(arg));
        }

        template <class A>
        inline batch<float, A> broadcast(float arg, requires_arch<sve>) noexcept
        {
            return svdup_n_f32(arg);
        }

        template <class A>
        inline batch<double, A> broadcast(double arg, requires_arch<sve>) noexcept
        {
            return svdup_n_f64(arg);
        }

        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<T, A> broadcast(T val, requires_arch<sve>) noexcept
        {
            return broadcast<sve>(val, sve {});
        }

        /**************
         * Arithmetic *
         **************/

        // add
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> add(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svadd_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // sadd
        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> sadd(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svqadd(lhs, rhs);
        }

        // sub
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> sub(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svsub_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // ssub
        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> ssub(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svqsub(lhs, rhs);
        }

        // mul
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> mul(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svmul_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // div
        template <class A, class T, typename std::enable_if<sizeof(T) >= 4, int>::type = 0>
        inline batch<T, A> div(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svdiv_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // max
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> max(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svmax_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // min
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> min(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svmin_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // neg
        template <class A, class T, detail::enable_sized_unsigned_t<T, 1> = 0>
        inline batch<T, A> neg(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svreinterpret_u8(svneg_x(detail::sve_ptrue<T>(), svreinterpret_s8(arg)));
        }

        template <class A, class T, detail::enable_sized_unsigned_t<T, 2> = 0>
        inline batch<T, A> neg(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svreinterpret_u16(svneg_x(detail::sve_ptrue<T>(), svreinterpret_s16(arg)));
        }

        template <class A, class T, detail::enable_sized_unsigned_t<T, 4> = 0>
        inline batch<T, A> neg(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svreinterpret_u32(svneg_x(detail::sve_ptrue<T>(), svreinterpret_s32(arg)));
        }

        template <class A, class T, detail::enable_sized_unsigned_t<T, 8> = 0>
        inline batch<T, A> neg(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svreinterpret_u64(svneg_x(detail::sve_ptrue<T>(), svreinterpret_s64(arg)));
        }

        template <class A, class T, detail::sve_enable_signed_int_or_floating_point_t<T> = 0>
        inline batch<T, A> neg(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svneg_x(detail::sve_ptrue<T>(), arg);
        }

        // abs
        template <class A, class T, detail::sve_enable_unsigned_int_t<T> = 0>
        inline batch<T, A> abs(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return arg;
        }

        template <class A, class T, detail::sve_enable_signed_int_or_floating_point_t<T> = 0>
        inline batch<T, A> abs(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svabs_x(detail::sve_ptrue<T>(), arg);
        }

        /**********************
         * Logical operations *
         **********************/

        // bitwise_and
        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> bitwise_and(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svand_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        template <class A>
        inline batch<float, A> bitwise_and(batch<float, A> const& lhs, batch<float, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto lhs_bits = svreinterpret_u32(lhs);
            const auto rhs_bits = svreinterpret_u32(rhs);
            const auto result_bits = svand_x(detail::sve_ptrue<float>(), lhs_bits, rhs_bits);
            return svreinterpret_f32(result_bits);
        }

        template <class A>
        inline batch<double, A> bitwise_and(batch<double, A> const& lhs, batch<double, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto lhs_bits = svreinterpret_u64(lhs);
            const auto rhs_bits = svreinterpret_u64(rhs);
            const auto result_bits = svand_x(detail::sve_ptrue<double>(), lhs_bits, rhs_bits);
            return svreinterpret_f64(result_bits);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> bitwise_and(batch_bool<T, A> const& lhs, batch_bool<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svand_z(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // bitwise_andnot
        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> bitwise_andnot(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svbic_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        template <class A>
        inline batch<float, A> bitwise_andnot(batch<float, A> const& lhs, batch<float, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto lhs_bits = svreinterpret_u32(lhs);
            const auto rhs_bits = svreinterpret_u32(rhs);
            const auto result_bits = svbic_x(detail::sve_ptrue<float>(), lhs_bits, rhs_bits);
            return svreinterpret_f32(result_bits);
        }

        template <class A>
        inline batch<double, A> bitwise_andnot(batch<double, A> const& lhs, batch<double, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto lhs_bits = svreinterpret_u64(lhs);
            const auto rhs_bits = svreinterpret_u64(rhs);
            const auto result_bits = svbic_x(detail::sve_ptrue<double>(), lhs_bits, rhs_bits);
            return svreinterpret_f64(result_bits);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> bitwise_andnot(batch_bool<T, A> const& lhs, batch_bool<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svbic_z(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // bitwise_or
        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> bitwise_or(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svorr_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        template <class A>
        inline batch<float, A> bitwise_or(batch<float, A> const& lhs, batch<float, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto lhs_bits = svreinterpret_u32(lhs);
            const auto rhs_bits = svreinterpret_u32(rhs);
            const auto result_bits = svorr_x(detail::sve_ptrue<float>(), lhs_bits, rhs_bits);
            return svreinterpret_f32(result_bits);
        }

        template <class A>
        inline batch<double, A> bitwise_or(batch<double, A> const& lhs, batch<double, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto lhs_bits = svreinterpret_u64(lhs);
            const auto rhs_bits = svreinterpret_u64(rhs);
            const auto result_bits = svorr_x(detail::sve_ptrue<double>(), lhs_bits, rhs_bits);
            return svreinterpret_f64(result_bits);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> bitwise_or(batch_bool<T, A> const& lhs, batch_bool<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svorr_z(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // bitwise_xor
        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> bitwise_xor(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return sveor_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        template <class A>
        inline batch<float, A> bitwise_xor(batch<float, A> const& lhs, batch<float, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto lhs_bits = svreinterpret_u32(lhs);
            const auto rhs_bits = svreinterpret_u32(rhs);
            const auto result_bits = sveor_x(detail::sve_ptrue<float>(), lhs_bits, rhs_bits);
            return svreinterpret_f32(result_bits);
        }

        template <class A>
        inline batch<double, A> bitwise_xor(batch<double, A> const& lhs, batch<double, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto lhs_bits = svreinterpret_u64(lhs);
            const auto rhs_bits = svreinterpret_u64(rhs);
            const auto result_bits = sveor_x(detail::sve_ptrue<double>(), lhs_bits, rhs_bits);
            return svreinterpret_f64(result_bits);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> bitwise_xor(batch_bool<T, A> const& lhs, batch_bool<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return sveor_z(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // bitwise_not
        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> bitwise_not(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svnot_x(detail::sve_ptrue<T>(), arg);
        }

        template <class A>
        inline batch<float, A> bitwise_not(batch<float, A> const& arg, requires_arch<sve>) noexcept
        {
            const auto arg_bits = svreinterpret_u32(arg);
            const auto result_bits = svnot_x(detail::sve_ptrue<float>(), arg_bits);
            return svreinterpret_f32(result_bits);
        }

        template <class A>
        inline batch<double, A> bitwise_not(batch<double, A> const& arg, requires_arch<sve>) noexcept
        {
            const auto arg_bits = svreinterpret_u64(arg);
            const auto result_bits = svnot_x(detail::sve_ptrue<double>(), arg_bits);
            return svreinterpret_f64(result_bits);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> bitwise_not(batch_bool<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svnot_z(detail::sve_ptrue<T>(), arg);
        }

        /**********
         * Shifts *
         **********/

        namespace detail
        {
            template <class A, class T, class U>
            inline batch<U, A> sve_to_unsigned_batch_impl(batch<T, A> const& arg, index<1>) noexcept
            {
                return svreinterpret_u8(arg);
            }

            template <class A, class T, class U>
            inline batch<U, A> sve_to_unsigned_batch_impl(batch<T, A> const& arg, index<2>) noexcept
            {
                return svreinterpret_u16(arg);
            }

            template <class A, class T, class U>
            inline batch<U, A> sve_to_unsigned_batch_impl(batch<T, A> const& arg, index<4>) noexcept
            {
                return svreinterpret_u32(arg);
            }

            template <class A, class T, class U>
            inline batch<U, A> sve_to_unsigned_batch_impl(batch<T, A> const& arg, index<8>) noexcept
            {
                return svreinterpret_u64(arg);
            }

            template <class A, class T, class U = as_unsigned_integer_t<T>>
            inline batch<U, A> sve_to_unsigned_batch(batch<T, A> const& arg) noexcept
            {
                return sve_to_unsigned_batch_impl<A, T, U>(arg, index<sizeof(T)> {});
            }
        } // namespace detail

        // bitwise_lshift
        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> bitwise_lshift(batch<T, A> const& arg, int n, requires_arch<sve>) noexcept
        {
            constexpr std::size_t size = sizeof(typename batch<T, A>::value_type) * 8;
            assert(0 <= n && static_cast<std::size_t>(n) < size && "index in bounds");
            return svlsl_x(detail::sve_ptrue<T>(), arg, n);
        }

        template <class A, class T, detail::enable_integral_t<T> = 0>
        inline batch<T, A> bitwise_lshift(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svlsl_x(detail::sve_ptrue<T>(), lhs, detail::sve_to_unsigned_batch<A, T>(rhs));
        }

        // bitwise_rshift
        template <class A, class T, detail::sve_enable_unsigned_int_t<T> = 0>
        inline batch<T, A> bitwise_rshift(batch<T, A> const& arg, int n, requires_arch<sve>) noexcept
        {
            constexpr std::size_t size = sizeof(typename batch<T, A>::value_type) * 8;
            assert(0 <= n && static_cast<std::size_t>(n) < size && "index in bounds");
            return svlsr_x(detail::sve_ptrue<T>(), arg, static_cast<T>(n));
        }

        template <class A, class T, detail::sve_enable_unsigned_int_t<T> = 0>
        inline batch<T, A> bitwise_rshift(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svlsr_x(detail::sve_ptrue<T>(), lhs, rhs);
        }

        template <class A, class T, detail::sve_enable_signed_int_t<T> = 0>
        inline batch<T, A> bitwise_rshift(batch<T, A> const& arg, int n, requires_arch<sve>) noexcept
        {
            constexpr std::size_t size = sizeof(typename batch<T, A>::value_type) * 8;
            assert(0 <= n && static_cast<std::size_t>(n) < size && "index in bounds");
            return svasr_x(detail::sve_ptrue<T>(), arg, static_cast<as_unsigned_integer_t<T>>(n));
        }

        template <class A, class T, detail::sve_enable_signed_int_t<T> = 0>
        inline batch<T, A> bitwise_rshift(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svasr_x(detail::sve_ptrue<T>(), lhs, detail::sve_to_unsigned_batch<A, T>(rhs));
        }

        /**************
         * Reductions *
         **************/

        // reduce_add
        template <class A, class T, class V = typename batch<T, A>::value_type, detail::sve_enable_all_t<T> = 0>
        inline V reduce_add(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            // sve integer reduction results are promoted to 64 bits
            return static_cast<V>(svaddv(detail::sve_ptrue<T>(), arg));
        }

        // reduce_max
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline T reduce_max(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svmaxv(detail::sve_ptrue<T>(), arg);
        }

        // reduce_min
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline T reduce_min(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svminv(detail::sve_ptrue<T>(), arg);
        }

        // haddp
        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<T, A> haddp(const batch<T, A>* row, requires_arch<sve>) noexcept
        {
            constexpr std::size_t size = batch<T, A>::size;
            T sums[size];
            for (std::size_t i = 0; i < size; ++i)
            {
                sums[i] = reduce_add(row[i], sve {});
            }
            return svld1(detail::sve_ptrue<T>(), sums);
        }

        /***************
         * Comparisons *
         ***************/

        // eq
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> eq(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svcmpeq(detail::sve_ptrue<T>(), lhs, rhs);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> eq(batch_bool<T, A> const& lhs, batch_bool<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            const auto neq_result = sveor_z(detail::sve_ptrue<T>(), lhs, rhs);
            return svnot_z(detail::sve_ptrue<T>(), neq_result);
        }

        // neq
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> neq(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svcmpne(detail::sve_ptrue<T>(), lhs, rhs);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> neq(batch_bool<T, A> const& lhs, batch_bool<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return sveor_z(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // lt
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> lt(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svcmplt(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // le
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> le(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svcmple(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // gt
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> gt(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svcmpgt(detail::sve_ptrue<T>(), lhs, rhs);
        }

        // ge
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch_bool<T, A> ge(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return svcmpge(detail::sve_ptrue<T>(), lhs, rhs);
        }

        /***************
         * Permutation *
         ***************/

        // swizzle
        template <class A, class T, class I, I... idx>
        inline batch<T, A> swizzle(batch<T, A> const& arg, batch_constant<batch<I, A>, idx...>, requires_arch<sve>) noexcept
        {
            static_assert(batch<T, A>::size == sizeof...(idx), "invalid swizzle indices");
            const batch<I, A> indices { idx... };
            return svtbl(arg, indices);
        }

        template <class A, class T, class I, I... idx>
        inline batch<std::complex<T>, A> swizzle(batch<std::complex<T>, A> const& self,
                                                 batch_constant<batch<I, A>, idx...>,
                                                 requires_arch<sve>) noexcept
        {
            const auto real = swizzle(self.real(), batch_constant<batch<I, A>, idx...> {}, sve {});
            const auto imag = swizzle(self.imag(), batch_constant<batch<I, A>, idx...> {}, sve {});
            return batch<std::complex<T>>(real, imag);
        }

        /*************
         * Selection *
         *************/

        // extract_pair
        namespace detail
        {
            template <class A, class T>
            inline batch<T, A> sve_extract_pair(batch<T, A> const&, batch<T, A> const& /*rhs*/, std::size_t, ::xsimd::detail::index_sequence<>) noexcept
            {
                assert(false && "extract_pair out of bounds");
                return batch<T, A> {};
            }

            template <class A, class T, size_t I, size_t... Is>
            inline batch<T, A> sve_extract_pair(batch<T, A> const& lhs, batch<T, A> const& rhs, std::size_t n, ::xsimd::detail::index_sequence<I, Is...>) noexcept
            {
                if (n == I)
                {
                    return svext(rhs, lhs, I);
                }
                else
                {
                    return sve_extract_pair(lhs, rhs, n, ::xsimd::detail::index_sequence<Is...>());
                }
            }

            template <class A, class T, size_t... Is>
            inline batch<T, A> sve_extract_pair_impl(batch<T, A> const& lhs, batch<T, A> const& rhs, std::size_t n, ::xsimd::detail::index_sequence<0, Is...>) noexcept
            {
                if (n == 0)
                {
                    return rhs;
                }
                else
                {
                    return sve_extract_pair(lhs, rhs, n, ::xsimd::detail::index_sequence<Is...>());
                }
            }
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> extract_pair(batch<T, A> const& lhs, batch<T, A> const& rhs, std::size_t n, requires_arch<sve>) noexcept
        {
            constexpr std::size_t size = batch<T, A>::size;
            assert(n < size && "index in bounds");
            return detail::sve_extract_pair_impl(lhs, rhs, n, ::xsimd::detail::make_index_sequence<size>());
        }

        // select
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> select(batch_bool<T, A> const& cond, batch<T, A> const& a, batch<T, A> const& b, requires_arch<sve>) noexcept
        {
            return svsel(cond, a, b);
        }

        template <class A, class T, bool... b>
        inline batch<T, A> select(batch_bool_constant<batch<T, A>, b...> const&, batch<T, A> const& true_br, batch<T, A> const& false_br, requires_arch<sve>) noexcept
        {
            return select(batch_bool<T, A> { b... }, true_br, false_br, sve {});
        }

        // zip in 128-bit lane as AVX does, unfortunatly we cannot use svzip which truly and neatly zips two vectors
        namespace detail
        {
            template <int N>
            struct sve_128b_lane_zipper
            {
                template <class A, class T>
                inline batch<T, A> zip_lo(batch<T, A> const& lhs, batch<T, A> const& rhs) noexcept
                {
                    const auto indexes = get_shuffle_indexes<A, T>();
                    const auto lhs_shuffled = svtbl(lhs, indexes);
                    const auto rhs_shuffled = svtbl(rhs, indexes);
                    return svzip1(lhs_shuffled, rhs_shuffled);
                }

                template <class A, class T>
                inline batch<T, A> zip_hi(batch<T, A> const& lhs, batch<T, A> const& rhs) noexcept
                {
                    const auto indexes = get_shuffle_indexes<A, T>();
                    const auto lhs_shuffled = svtbl(lhs, indexes);
                    const auto rhs_shuffled = svtbl(rhs, indexes);
                    return svzip2(lhs_shuffled, rhs_shuffled);
                }

                // Pre shuffle per 128-bit lane so we can still use svzip
                // E.g., sve256/int32: abcd efgh -> abef cdgh
                // The shuffle indexes are calculated at compile time
                template <class A, class T, class U = as_unsigned_integer_t<T>>
                inline batch<U, A> get_shuffle_indexes() noexcept
                {
                    constexpr int b128_lanes = N / 128;
                    constexpr int elements_per_b128 = 128 / (sizeof(T) * 8);

                    U indexes[batch<U, A>::size];

                    int index = 0, step = 0;
                    for (int i = 0; i < b128_lanes; ++i)
                    {
                        for (int j = 0; j < elements_per_b128 / 2; ++j)
                        {
                            indexes[index] = index + step;
                            ++index;
                        }
                        step += elements_per_b128 / 2;
                    }
                    step -= elements_per_b128 / 2;
                    for (int i = 0; i < b128_lanes; ++i)
                    {
                        for (int j = 0; j < elements_per_b128 / 2; ++j)
                        {
                            indexes[index] = index - step;
                            ++index;
                        }
                        step -= elements_per_b128 / 2;
                    }

                    return svld1(detail::sve_ptrue<U>(), indexes);
                }
            };

            template <>
            struct sve_128b_lane_zipper<128>
            {
                template <class A, class T>
                inline batch<T, A> zip_lo(batch<T, A> const& lhs, batch<T, A> const& rhs) noexcept
                {
                    return svzip1(lhs, rhs);
                }

                template <class A, class T>
                inline batch<T, A> zip_hi(batch<T, A> const& lhs, batch<T, A> const& rhs) noexcept
                {
                    return svzip2(lhs, rhs);
                }
            };
        } // namespace detail

        // zip_lo
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> zip_lo(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return detail::sve_128b_lane_zipper<XSIMD_SVE_BITS>().zip_lo(lhs, rhs);
        }

        // zip_hi
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> zip_hi(batch<T, A> const& lhs, batch<T, A> const& rhs, requires_arch<sve>) noexcept
        {
            return detail::sve_128b_lane_zipper<XSIMD_SVE_BITS>().zip_hi(lhs, rhs);
        }

        /*****************************
         * Floating-point arithmetic *
         *****************************/

        // rsqrt
        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<T, A> rsqrt(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svrsqrte(arg);
        }

        // sqrt
        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<T, A> sqrt(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svsqrt_x(detail::sve_ptrue<T>(), arg);
        }

        // fused operations
        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<T, A> fma(batch<T, A> const& x, batch<T, A> const& y, batch<T, A> const& z, requires_arch<sve>) noexcept
        {
            return svmad_x(detail::sve_ptrue<T>(), x, y, z);
        }

        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<T, A> fms(batch<T, A> const& x, batch<T, A> const& y, batch<T, A> const& z, requires_arch<sve>) noexcept
        {
            return svmad_x(detail::sve_ptrue<T>(), x, y, -z);
        }

        // reciprocal
        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch<T, A> reciprocal(const batch<T, A>& arg, requires_arch<sve>) noexcept
        {
            return svrecpe(arg);
        }

        /******************************
         * Floating-point conversions *
         ******************************/

        // fast_cast
        namespace detail
        {
            template <class A, class T, detail::enable_sized_integral_t<T, 4> = 0>
            inline batch<float, A> fast_cast(batch<T, A> const& arg, batch<float, A> const&, requires_arch<sve>) noexcept
            {
                return svcvt_f32_x(detail::sve_ptrue<T>(), arg);
            }

            template <class A, class T, detail::enable_sized_integral_t<T, 8> = 0>
            inline batch<double, A> fast_cast(batch<T, A> const& arg, batch<double, A> const&, requires_arch<sve>) noexcept
            {
                return svcvt_f64_x(detail::sve_ptrue<T>(), arg);
            }

            template <class A>
            inline batch<int32_t, A> fast_cast(batch<float, A> const& arg, batch<int32_t, A> const&, requires_arch<sve>) noexcept
            {
                return svcvt_s32_x(detail::sve_ptrue<float>(), arg);
            }

            template <class A>
            inline batch<uint32_t, A> fast_cast(batch<float, A> const& arg, batch<uint32_t, A> const&, requires_arch<sve>) noexcept
            {
                return svcvt_u32_x(detail::sve_ptrue<float>(), arg);
            }

            template <class A>
            inline batch<int64_t, A> fast_cast(batch<double, A> const& arg, batch<int64_t, A> const&, requires_arch<sve>) noexcept
            {
                return svcvt_s64_x(detail::sve_ptrue<double>(), arg);
            }

            template <class A>
            inline batch<uint64_t, A> fast_cast(batch<double, A> const& arg, batch<uint64_t, A> const&, requires_arch<sve>) noexcept
            {
                return svcvt_u64_x(detail::sve_ptrue<double>(), arg);
            }
        }

        /*********
         * Miscs *
         *********/

        // set
        template <class A, class T, class... Args>
        inline batch<T, A> set(batch<T, A> const&, requires_arch<sve>, Args... args) noexcept
        {
            return detail::sve_vector_type<T> { args... };
        }

        template <class A, class T, class... Args>
        inline batch<std::complex<T>, A> set(batch<std::complex<T>, A> const&, requires_arch<sve>,
                                             Args... args_complex) noexcept
        {
            return batch<std::complex<T>>(detail::sve_vector_type<T> { args_complex.real()... },
                                          detail::sve_vector_type<T> { args_complex.imag()... });
        }

        template <class A, class T, class... Args>
        inline batch_bool<T, A> set(batch_bool<T, A> const&, requires_arch<sve>, Args... args) noexcept
        {
            using U = as_unsigned_integer_t<T>;
            const auto values = detail::sve_vector_type<U> { static_cast<U>(args)... };
            const auto zero = broadcast<A, U>(static_cast<U>(0), sve {});
            return svcmpne(detail::sve_ptrue<T>(), values, zero);
        }

        // insert
        namespace detail
        {
            // generate index sequence (iota)
            inline svuint8_t sve_iota_impl(index<1>) noexcept { return svindex_u8(0, 1); }
            inline svuint16_t sve_iota_impl(index<2>) noexcept { return svindex_u16(0, 1); }
            inline svuint32_t sve_iota_impl(index<4>) noexcept { return svindex_u32(0, 1); }
            inline svuint64_t sve_iota_impl(index<8>) noexcept { return svindex_u64(0, 1); }

            template <class T, class V = sve_vector_type<as_unsigned_integer_t<T>>>
            inline V sve_iota() noexcept { return sve_iota_impl(index<sizeof(T)> {}); }
        } // namespace detail

        template <class A, class T, size_t I, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> insert(batch<T, A> const& arg, T val, index<I>, requires_arch<sve>) noexcept
        {
            // create a predicate with only the I-th lane activated
            const auto iota = detail::sve_iota<T>();
            const auto index_predicate = svcmpeq(detail::sve_ptrue<T>(), iota, static_cast<as_unsigned_integer_t<T>>(I));
            return svsel(index_predicate, broadcast<A, T>(val, sve {}), arg);
        }

        // all
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline bool all(batch_bool<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return detail::sve_pcount<T>(arg) == batch_bool<T, A>::size;
        }

        // any
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline bool any(batch_bool<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return svptest_any(arg, arg);
        }

        // bitwise_cast
        template <class A, class T, class R, detail::sve_enable_all_t<T> = 0, detail::enable_sized_unsigned_t<R, 1> = 0>
        inline batch<R, A> bitwise_cast(batch<T, A> const& arg, batch<R, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_u8(arg);
        }

        template <class A, class T, class R, detail::sve_enable_all_t<T> = 0, detail::enable_sized_signed_t<R, 1> = 0>
        inline batch<R, A> bitwise_cast(batch<T, A> const& arg, batch<R, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_s8(arg);
        }

        template <class A, class T, class R, detail::sve_enable_all_t<T> = 0, detail::enable_sized_unsigned_t<R, 2> = 0>
        inline batch<R, A> bitwise_cast(batch<T, A> const& arg, batch<R, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_u16(arg);
        }

        template <class A, class T, class R, detail::sve_enable_all_t<T> = 0, detail::enable_sized_signed_t<R, 2> = 0>
        inline batch<R, A> bitwise_cast(batch<T, A> const& arg, batch<R, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_s16(arg);
        }

        template <class A, class T, class R, detail::sve_enable_all_t<T> = 0, detail::enable_sized_unsigned_t<R, 4> = 0>
        inline batch<R, A> bitwise_cast(batch<T, A> const& arg, batch<R, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_u32(arg);
        }

        template <class A, class T, class R, detail::sve_enable_all_t<T> = 0, detail::enable_sized_signed_t<R, 4> = 0>
        inline batch<R, A> bitwise_cast(batch<T, A> const& arg, batch<R, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_s32(arg);
        }

        template <class A, class T, class R, detail::sve_enable_all_t<T> = 0, detail::enable_sized_unsigned_t<R, 8> = 0>
        inline batch<R, A> bitwise_cast(batch<T, A> const& arg, batch<R, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_u64(arg);
        }

        template <class A, class T, class R, detail::sve_enable_all_t<T> = 0, detail::enable_sized_signed_t<R, 8> = 0>
        inline batch<R, A> bitwise_cast(batch<T, A> const& arg, batch<R, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_s64(arg);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<float, A> bitwise_cast(batch<T, A> const& arg, batch<float, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_f32(arg);
        }

        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<double, A> bitwise_cast(batch<T, A> const& arg, batch<double, A> const&, requires_arch<sve>) noexcept
        {
            return svreinterpret_f64(arg);
        }

        // batch_bool_cast
        template <class A, class T_out, class T_in, detail::sve_enable_all_t<T_in> = 0>
        inline batch_bool<T_out, A> batch_bool_cast(batch_bool<T_in, A> const& arg, batch_bool<T_out, A> const&, requires_arch<sve>) noexcept
        {
            return arg.data;
        }

        // from_bool
        template <class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> from_bool(batch_bool<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return select(arg, batch<T, A>(1), batch<T, A>(0));
        }

        // slide_left
        namespace detail
        {
            template <size_t N>
            struct sve_slider_left
            {
                template <class A, class T>
                inline batch<T, A> operator()(batch<T, A> const& arg) noexcept
                {
                    using u8_vector = batch<uint8_t, A>;
                    const auto left = svdup_n_u8(0);
                    const auto right = bitwise_cast(arg, u8_vector {}, sve {}).data;
                    const u8_vector result(svext(left, right, u8_vector::size - N));
                    return bitwise_cast(result, batch<T, A> {}, sve {});
                }
            };

            template <>
            struct sve_slider_left<0>
            {
                template <class A, class T>
                inline batch<T, A> operator()(batch<T, A> const& arg) noexcept
                {
                    return arg;
                }
            };
        } // namespace detail

        template <size_t N, class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> slide_left(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return detail::sve_slider_left<N>()(arg);
        }

        // slide_right
        namespace detail
        {
            template <size_t N>
            struct sve_slider_right
            {
                template <class A, class T>
                inline batch<T, A> operator()(batch<T, A> const& arg) noexcept
                {
                    using u8_vector = batch<uint8_t, A>;
                    const auto left = bitwise_cast(arg, u8_vector {}, sve {}).data;
                    const auto right = svdup_n_u8(0);
                    const u8_vector result(svext(left, right, N));
                    return bitwise_cast(result, batch<T, A> {}, sve {});
                }
            };

            template <>
            struct sve_slider_right<batch<uint8_t, sve>::size>
            {
                template <class A, class T>
                inline batch<T, A> operator()(batch<T, A> const&) noexcept
                {
                    return batch<T, A> {};
                }
            };
        } // namespace detail

        template <size_t N, class A, class T, detail::sve_enable_all_t<T> = 0>
        inline batch<T, A> slide_right(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return detail::sve_slider_right<N>()(arg);
        }

        // isnan
        template <class A, class T, detail::sve_enable_floating_point_t<T> = 0>
        inline batch_bool<T, A> isnan(batch<T, A> const& arg, requires_arch<sve>) noexcept
        {
            return !(arg == arg);
        }

        // TODO: nearbyint_as_int

    } // namespace kernel
} // namespace xsimd

#endif

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

#ifndef XSIMD_BENCHMARK_HPP
#define XSIMD_BENCHMARK_HPP

#include "xsimd/arch/xsimd_scalar.hpp"
#include "xsimd/xsimd.hpp"
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

namespace xsimd
{
    using duration_type = std::chrono::duration<double, std::milli>;

    template <class T>
    using bench_vector = std::vector<T, xsimd::aligned_allocator<T>>;

    template <class T>
    void init_benchmark(bench_vector<T>& lhs, bench_vector<T>& rhs, bench_vector<T>& res, size_t size)
    {
        lhs.resize(size);
        rhs.resize(size);
        res.resize(size);
        for (size_t i = 0; i < size; ++i)
        {
            lhs[i] = T(0.5) + std::sqrt(T(i)) * T(9.) / T(size);
            rhs[i] = T(10.2) / T(i + 2) + T(0.25);
        }
    }

    template <class T>
    void init_benchmark(bench_vector<T>& op0, bench_vector<T>& op1, bench_vector<T>& op2, bench_vector<T>& res, size_t size)
    {
        op0.resize(size);
        op1.resize(size);
        op2.resize(size);
        res.resize(size);
        for (size_t i = 0; i < size; ++i)
        {
            op0[i] = T(0.5) + std::sqrt(T(i)) * T(9.) / T(size);
            op1[i] = T(10.2) / T(i + 3) + T(0.25);
            op2[i] = T(20.1) / T(i + 2) + T(0.65);
        }
    }

    template <class T>
    void init_benchmark_arctrigo(bench_vector<T>& lhs, bench_vector<T>& rhs, bench_vector<T>& res, size_t size)
    {
        lhs.resize(size);
        rhs.resize(size);
        res.resize(size);
        for (size_t i = 0; i < size; ++i)
        {
            lhs[i] = T(-1.) + T(2.) * T(i) / T(size);
            rhs[i] = T(i) / T(i + 2) + T(0.25);
        }
    }

    enum class init_method
    {
        classic,
        arctrigo
    };

    template <class F, class V>
    duration_type benchmark_scalar(F f, V& lhs, V& res, std::size_t number)
    {
        size_t s = lhs.size();
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (size_t i = 0; i < s; ++i)
            {
                res[i] = f(lhs[i]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class F, class V>
    duration_type benchmark_scalar(F f, V& lhs, V& rhs, V& res, std::size_t number)
    {
        size_t s = lhs.size();
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (size_t i = 0; i < s; ++i)
            {
                res[i] = f(lhs[i], rhs[i]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class F, class V>
    duration_type benchmark_scalar(F f, V& op0, V& op1, V& op2, V& res, std::size_t number)
    {
        size_t s = op0.size();
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (size_t i = 0; i < s; ++i)
            {
                res[i] = f(op0[i], op1[i], op2[i]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class B, class F, class V>
    duration_type benchmark_simd(F f, V& lhs, V& res, std::size_t number)
    {
        std::size_t s = lhs.size();
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (std::size_t i = 0; i <= (s - B::size); i += B::size)
            {
                B blhs = B::load_aligned(&lhs[i]);
                B bres = f(blhs);
                bres.store_aligned(&res[i]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class B, class F, class V>
    duration_type benchmark_simd_unrolled(F f, V& lhs, V& res, std::size_t number)
    {
        std::size_t s = lhs.size();
        std::size_t inc = 4 * B::size;
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (std::size_t i = 0; i <= (s - inc); i += inc)
            {
                size_t j = i + B::size;
                size_t k = j + B::size;
                size_t l = k + B::size;
                B blhs = B::load_aligned(&lhs[i]),
                  blhs2 = B::load_aligned(&lhs[j]),
                  blhs3 = B::load_aligned(&lhs[k]),
                  blhs4 = B::load_aligned(&lhs[l]);
                B bres = f(blhs);
                B bres2 = f(blhs2);
                B bres3 = f(blhs3);
                B bres4 = f(blhs4);
                bres.store_aligned(&res[i]);
                bres2.store_aligned(&res[j]);
                bres3.store_aligned(&res[k]);
                bres4.store_aligned(&res[l]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class B, class F, class V>
    duration_type benchmark_simd(F f, V& lhs, V& rhs, V& res, std::size_t number)
    {
        std::size_t s = lhs.size();
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (std::size_t i = 0; i <= (s - B::size); i += B::size)
            {
                B blhs = B::load_aligned(&lhs[i]),
                  brhs = B::load_aligned(&rhs[i]);
                B bres = f(blhs, brhs);
                bres.store_aligned(&res[i]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class B, class F, class V>
    duration_type benchmark_simd_unrolled(F f, V& lhs, V& rhs, V& res, std::size_t number)
    {
        std::size_t s = lhs.size();
        std::size_t inc = 4 * B::size;
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (std::size_t i = 0; i <= (s - inc); i += inc)
            {
                size_t j = i + B::size;
                size_t k = j + B::size;
                size_t l = k + B::size;
                B blhs = B::load_aligned(&lhs[i]),
                  brhs = B::load_aligned(&rhs[i]),
                  blhs2 = B::load_aligned(&lhs[j]),
                  brhs2 = B::load_aligned(&rhs[j]);
                B blhs3 = B::load_aligned(&lhs[k]),
                  brhs3 = B::load_aligned(&rhs[k]),
                  blhs4 = B::load_aligned(&lhs[l]),
                  brhs4 = B::load_aligned(&rhs[l]);
                B bres = f(blhs, brhs);
                B bres2 = f(blhs2, brhs2);
                B bres3 = f(blhs3, brhs3);
                B bres4 = f(blhs4, brhs4);
                bres.store_aligned(&res[i]);
                bres2.store_aligned(&res[j]);
                bres3.store_aligned(&res[k]);
                bres4.store_aligned(&res[l]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class B, class F, class V>
    duration_type benchmark_simd(F f, V& op0, V& op1, V& op2, V& res, std::size_t number)
    {
        std::size_t s = op0.size();
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (std::size_t i = 0; i <= (s - B::size); i += B::size)
            {
                B bop0 = B::load_aligned(&op0[i]),
                  bop1 = B::load_aligned(&op1[i]),
                  bop2 = B::load_aligned(&op2[i]);
                B bres = f(bop0, bop1, bop2);
                bres.store_aligned(&res[i]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class B, class F, class V>
    duration_type benchmark_simd_unrolled(F f, V& op0, V& op1, V& op2, V& res, std::size_t number)
    {
        std::size_t s = op0.size();
        std::size_t inc = 4 * B::size;
        duration_type t_res = duration_type::max();
        for (std::size_t count = 0; count < number; ++count)
        {
            auto start = std::chrono::steady_clock::now();
            for (std::size_t i = 0; i <= (s - inc); i += inc)
            {
                size_t j = i + B::size;
                size_t k = j + B::size;
                size_t l = k + B::size;
                B bop0_i = B::load_aligned(&op0[i]),
                  bop1_i = B::load_aligned(&op1[i]),
                  bop2_i = B::load_aligned(&op2[i]);
                B bop0_j = B::load_aligned(&op0[j]),
                  bop1_j = B::load_aligned(&op1[j]),
                  bop2_j = B::load_aligned(&op2[j]);
                B bop0_k = B::load_aligned(&op0[k]),
                  bop1_k = B::load_aligned(&op1[k]),
                  bop2_k = B::load_aligned(&op2[k]);
                B bop0_l = B::load_aligned(&op0[l]),
                  bop1_l = B::load_aligned(&op1[l]),
                  bop2_l = B::load_aligned(&op2[l]);
                B bres_i = f(bop0_i, bop1_i, bop2_i);
                B bres_j = f(bop0_j, bop1_j, bop2_j);
                B bres_k = f(bop0_k, bop1_k, bop2_k);
                B bres_l = f(bop0_l, bop1_l, bop2_l);
                bres_i.store_aligned(&res[i]);
                bres_j.store_aligned(&res[j]);
                bres_k.store_aligned(&res[k]);
                bres_l.store_aligned(&res[l]);
            }
            auto end = std::chrono::steady_clock::now();
            auto tmp = end - start;
            t_res = tmp < t_res ? tmp : t_res;
        }
        return t_res;
    }

    template <class F, class OS>
    void run_benchmark_1op(F f, OS& out, std::size_t size, std::size_t iter, init_method init = init_method::classic)
    {
        bench_vector<float> f_lhs, f_rhs, f_res;
        bench_vector<double> d_lhs, d_rhs, d_res;

        switch (init)
        {
        case init_method::classic:
            init_benchmark(f_lhs, f_rhs, f_res, size);
            init_benchmark(d_lhs, d_rhs, d_res, size);
            break;
        case init_method::arctrigo:
            init_benchmark_arctrigo(f_lhs, f_rhs, f_res, size);
            init_benchmark_arctrigo(d_lhs, d_rhs, d_res, size);
            break;
        default:
            init_benchmark(f_lhs, f_rhs, f_res, size);
            init_benchmark(d_lhs, d_rhs, d_res, size);
            break;
        }

#ifndef XSIMD_POLY_BENCHMARKS
        duration_type t_float_scalar = benchmark_scalar(f, f_lhs, f_res, iter);
        duration_type t_double_scalar = benchmark_scalar(f, d_lhs, d_res, iter);
#endif

        duration_type t_float_vector = benchmark_simd<batch<float>>(f, f_lhs, f_res, iter);
        duration_type t_float_vector_u = benchmark_simd_unrolled<batch<float>>(f, f_lhs, f_res, iter);
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        duration_type t_double_vector = benchmark_simd<batch<double>>(f, d_lhs, d_res, iter);
        duration_type t_double_vector_u = benchmark_simd_unrolled<batch<double>>(f, d_lhs, d_res, iter);
#endif

        out << "============================" << std::endl;
        out << f.name() << std::endl;
#ifndef XSIMD_POLY_BENCHMARKS
        out << "scalar float      : " << t_float_scalar.count() << "ms" << std::endl;
#endif
        out << "vector float      : " << t_float_vector.count() << "ms" << std::endl;
        out << "vector float unr  : " << t_float_vector_u.count() << "ms" << std::endl;

#ifndef XSIMD_POLY_BENCHMARKS
        out << "scalar double     : " << t_double_scalar.count() << "ms" << std::endl;
#endif
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        out << "vector double     : " << t_double_vector.count() << "ms" << std::endl;
        out << "vector double unr : " << t_double_vector_u.count() << "ms" << std::endl;
#endif
        out << "============================" << std::endl;
    }

    template <class F, class OS>
    void run_benchmark_2op(F f, OS& out, std::size_t size, std::size_t iter)
    {
        bench_vector<float> f_lhs, f_rhs, f_res;
        bench_vector<double> d_lhs, d_rhs, d_res;

        init_benchmark(f_lhs, f_rhs, f_res, size);
        init_benchmark(d_lhs, d_rhs, d_res, size);

        duration_type t_float_scalar = benchmark_scalar(f, f_lhs, f_rhs, f_res, iter);
        duration_type t_float_vector = benchmark_simd<batch<float>>(f, f_lhs, f_rhs, f_res, iter);
        duration_type t_float_vector_u = benchmark_simd_unrolled<batch<float>>(f, f_lhs, f_rhs, f_res, iter);

        duration_type t_double_scalar = benchmark_scalar(f, d_lhs, d_rhs, d_res, iter);
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        duration_type t_double_vector = benchmark_simd<batch<double>>(f, d_lhs, d_rhs, d_res, iter);
        duration_type t_double_vector_u = benchmark_simd_unrolled<batch<double>>(f, d_lhs, d_rhs, d_res, iter);
#endif

        out << "============================" << std::endl;
        out << default_arch::name() << std::endl;
        out << "scalar float      : " << t_float_scalar.count() << "ms" << std::endl;
        out << "vector float      : " << t_float_vector.count() << "ms" << std::endl;
        out << "vector float unr  : " << t_float_vector_u.count() << "ms" << std::endl;
        out << "scalar double     : " << t_double_scalar.count() << "ms" << std::endl;
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        out << "vector double     : " << t_double_vector.count() << "ms" << std::endl;
        out << "vector double unr : " << t_double_vector_u.count() << "ms" << std::endl;
#endif
        out << "============================" << std::endl;
    }

    template <class F, class OS>
    void run_benchmark_3op(F f, OS& out, std::size_t size, std::size_t iter)
    {
        bench_vector<float> f_op0, f_op1, f_op2, f_res;
        bench_vector<double> d_op0, d_op1, d_op2, d_res;

        init_benchmark(f_op0, f_op1, f_op2, f_res, size);
        init_benchmark(d_op0, d_op1, d_op2, d_res, size);

        duration_type t_float_scalar = benchmark_scalar(f, f_op0, f_op1, f_op2, f_res, iter);
        duration_type t_float_vector = benchmark_simd<batch<float>>(f, f_op0, f_op1, f_op2, f_res, iter);
        duration_type t_float_vector_u = benchmark_simd_unrolled<batch<float>>(f, f_op0, f_op1, f_op2, f_res, iter);
        duration_type t_double_scalar = benchmark_scalar(f, d_op0, d_op1, d_op2, d_res, iter);
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        duration_type t_double_vector = benchmark_simd<batch<double>>(f, d_op0, d_op1, d_op2, d_res, iter);
        duration_type t_double_vector_u = benchmark_simd_unrolled<batch<double>>(f, d_op0, d_op1, d_op2, d_res, iter);
#endif

        out << "============================" << std::endl;
        out << default_arch::name() << std::endl;
        out << "scalar float      : " << t_float_scalar.count() << "ms" << std::endl;
        out << "vector float      : " << t_float_vector.count() << "ms" << std::endl;
        out << "vector float unr  : " << t_float_vector_u.count() << "ms" << std::endl;
        out << "scalar double     : " << t_double_scalar.count() << "ms" << std::endl;
#if !XSIMD_WITH_NEON || XSIMD_WITH_NEON64
        out << "vector double     : " << t_double_vector.count() << "ms" << std::endl;
        out << "vector double unr : " << t_double_vector_u.count() << "ms" << std::endl;
#endif
        out << "============================" << std::endl;
    }

#define DEFINE_OP_FUNCTOR_2OP(OP, NAME)                       \
    struct NAME##_fn                                          \
    {                                                         \
        template <class T>                                    \
        inline T operator()(const T& lhs, const T& rhs) const \
        {                                                     \
            return lhs OP rhs;                                \
        }                                                     \
        inline std::string name() const                       \
        {                                                     \
            return #NAME;                                     \
        }                                                     \
    }

#define DEFINE_FUNCTOR_1OP(FN)                \
    struct FN##_fn                            \
    {                                         \
        template <class T>                    \
        inline T operator()(const T& x) const \
        {                                     \
            using xsimd::FN;                  \
            return FN(x);                     \
        }                                     \
        inline std::string name() const       \
        {                                     \
            return #FN;                       \
        }                                     \
    }

#define DEFINE_FUNCTOR_1OP_TEMPLATE(NAME, FN, N, ...) \
    struct NAME##_##N##_fn                            \
    {                                                 \
        template <class T>                            \
        inline T operator()(const T& x) const         \
        {                                             \
            using xsimd::FN;                          \
            return FN<T, __VA_ARGS__>(x);             \
        }                                             \
        inline std::string name() const               \
        {                                             \
            return #FN " " #N;                        \
        }                                             \
    }

#define DEFINE_FUNCTOR_2OP(FN)                                \
    struct FN##_fn                                            \
    {                                                         \
        template <class T>                                    \
        inline T operator()(const T& lhs, const T& rhs) const \
        {                                                     \
            using xsimd::FN;                                  \
            return FN(lhs, rhs);                              \
        }                                                     \
        inline std::string name() const                       \
        {                                                     \
            return #FN;                                       \
        }                                                     \
    }

#define DEFINE_FUNCTOR_3OP(FN)                                              \
    struct FN##_fn                                                          \
    {                                                                       \
        template <class T>                                                  \
        inline T operator()(const T& op0, const T& op1, const T& op2) const \
        {                                                                   \
            using xsimd::FN;                                                \
            return FN(op0, op1, op2);                                       \
        }                                                                   \
        inline std::string name() const                                     \
        {                                                                   \
            return #FN;                                                     \
        }                                                                   \
    }

    DEFINE_OP_FUNCTOR_2OP(+, add);
    DEFINE_OP_FUNCTOR_2OP(-, sub);
    DEFINE_OP_FUNCTOR_2OP(*, mul);
    DEFINE_OP_FUNCTOR_2OP(/, div);

    DEFINE_FUNCTOR_1OP(exp);
    DEFINE_FUNCTOR_1OP(exp2);
    DEFINE_FUNCTOR_1OP(expm1);
    DEFINE_FUNCTOR_1OP(log);
    DEFINE_FUNCTOR_1OP(log10);
    DEFINE_FUNCTOR_1OP(log2);
    DEFINE_FUNCTOR_1OP(log1p);

    DEFINE_FUNCTOR_1OP(sin);
    DEFINE_FUNCTOR_1OP(cos);
    DEFINE_FUNCTOR_1OP(tan);
    DEFINE_FUNCTOR_1OP(asin);
    DEFINE_FUNCTOR_1OP(acos);
    DEFINE_FUNCTOR_1OP(atan);

    DEFINE_FUNCTOR_1OP(sinh);
    DEFINE_FUNCTOR_1OP(cosh);
    DEFINE_FUNCTOR_1OP(tanh);
    DEFINE_FUNCTOR_1OP(asinh);
    DEFINE_FUNCTOR_1OP(acosh);
    DEFINE_FUNCTOR_1OP(atanh);

    DEFINE_FUNCTOR_2OP(pow);
    DEFINE_FUNCTOR_1OP(sqrt);
    DEFINE_FUNCTOR_1OP(cbrt);
    DEFINE_FUNCTOR_2OP(hypot);

    DEFINE_FUNCTOR_1OP(ceil);
    DEFINE_FUNCTOR_1OP(floor);
    DEFINE_FUNCTOR_1OP(trunc);
    DEFINE_FUNCTOR_1OP(round);
    DEFINE_FUNCTOR_1OP(nearbyint);
    DEFINE_FUNCTOR_1OP(rint);

    DEFINE_FUNCTOR_2OP(fmod);
    DEFINE_FUNCTOR_2OP(remainder);
    DEFINE_FUNCTOR_2OP(fdim);
    DEFINE_FUNCTOR_3OP(clip);
#if 0
DEFINE_FUNCTOR_1OP(isfinite);
DEFINE_FUNCTOR_1OP(isinf);
DEFINE_FUNCTOR_1OP(is_flint);
DEFINE_FUNCTOR_1OP(is_odd);
DEFINE_FUNCTOR_1OP(is_even);
#endif

    DEFINE_FUNCTOR_1OP_TEMPLATE(horner, kernel::horner, 5, 1, 2, 3, 4, 5);
    DEFINE_FUNCTOR_1OP_TEMPLATE(estrin, kernel::estrin, 5, 1, 2, 3, 4, 5);
    DEFINE_FUNCTOR_1OP_TEMPLATE(horner, kernel::horner, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    DEFINE_FUNCTOR_1OP_TEMPLATE(estrin, kernel::estrin, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    DEFINE_FUNCTOR_1OP_TEMPLATE(horner, kernel::horner, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    DEFINE_FUNCTOR_1OP_TEMPLATE(estrin, kernel::estrin, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    DEFINE_FUNCTOR_1OP_TEMPLATE(horner, kernel::horner, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
    DEFINE_FUNCTOR_1OP_TEMPLATE(estrin, kernel::estrin, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
    DEFINE_FUNCTOR_1OP_TEMPLATE(horner, kernel::horner, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
    DEFINE_FUNCTOR_1OP_TEMPLATE(estrin, kernel::estrin, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

}
#endif

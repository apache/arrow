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

#include "xsimd_benchmark.hpp"
#include <map>

void benchmark_operation()
{
    // std::size_t size = 9984;
    std::size_t size = 20000;
    xsimd::run_benchmark_2op(xsimd::add_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_2op(xsimd::sub_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_2op(xsimd::mul_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_2op(xsimd::div_fn(), std::cout, size, 1000);
}

void benchmark_exp_log()
{
    std::size_t size = 20000;
    xsimd::run_benchmark_1op(xsimd::exp_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::exp2_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op(xsimd::expm1_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::log_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::log2_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op(xsimd::log10_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::log1p_fn(), std::cout, size, 1000);
}

void benchmark_trigo()
{
    std::size_t size = 20000;
    xsimd::run_benchmark_1op(xsimd::sin_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::cos_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::tan_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::asin_fn(), std::cout, size, 1000, xsimd::init_method::arctrigo);
    xsimd::run_benchmark_1op(xsimd::acos_fn(), std::cout, size, 1000, xsimd::init_method::arctrigo);
    xsimd::run_benchmark_1op(xsimd::atan_fn(), std::cout, size, 1000, xsimd::init_method::arctrigo);
}

void benchmark_hyperbolic()
{
    std::size_t size = 20000;
    xsimd::run_benchmark_1op(xsimd::sinh_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::cosh_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::tanh_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::asinh_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op(xsimd::acosh_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op(xsimd::atanh_fn(), std::cout, size, 100);
}

void benchmark_power()
{
    std::size_t size = 20000;
    xsimd::run_benchmark_2op(xsimd::pow_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::sqrt_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::cbrt_fn(), std::cout, size, 100);
    xsimd::run_benchmark_2op(xsimd::hypot_fn(), std::cout, size, 1000);
}

void benchmark_rounding()
{
    std::size_t size = 20000;
    xsimd::run_benchmark_1op(xsimd::ceil_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::floor_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::trunc_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::round_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op(xsimd::nearbyint_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op(xsimd::rint_fn(), std::cout, size, 100);
}

#ifdef XSIMD_POLY_BENCHMARKS
void benchmark_poly_evaluation()
{
    std::size_t size = 20000;
    xsimd::run_benchmark_1op(xsimd::horner_5_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::estrin_5_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::horner_10_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::estrin_10_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::horner_12_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::estrin_12_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::horner_14_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::estrin_14_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::horner_16_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_1op(xsimd::estrin_16_fn(), std::cout, size, 1000);
}
#endif

void benchmark_basic_math()
{
    std::size_t size = 20000;
    xsimd::run_benchmark_2op(xsimd::fmod_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_2op(xsimd::remainder_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_2op(xsimd::fdim_fn(), std::cout, size, 1000);
    xsimd::run_benchmark_3op(xsimd::clip_fn(), std::cout, size, 1000);
#if 0
    xsimd::run_benchmark_1op_pred(xsimd::isfinite_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op_pred(xsimd::isinf_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op_pred(xsimd::is_flint_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op_pred(xsimd::is_odd_fn(), std::cout, size, 100);
    xsimd::run_benchmark_1op_pred(xsimd::is_even_fn(), std::cout, size, 100);
#endif
}

int main(int argc, char* argv[])
{
    const std::map<std::string, std::pair<std::string, void (*)()>> fn_map = {
        { "op", { "arithmetic", benchmark_operation } },
        { "exp", { "exponential and logarithm", benchmark_exp_log } },
        { "trigo", { "trigonometric", benchmark_trigo } },
        { "hyperbolic", { "hyperbolic", benchmark_hyperbolic } },
        { "power", { "power", benchmark_power } },
        { "basic_math", { "basic math", benchmark_basic_math } },
        { "rounding", { "rounding", benchmark_rounding } },
#ifdef XSIMD_POLY_BENCHMARKS
        { "utils", { "polynomial evaluation", benchmark_poly_evaluation } },
#endif
    };

    if (argc > 1)
    {
        if (std::string(argv[1]) == "--help" || std::string(argv[1]) == "-h")
        {
            std::cout << "Available options:" << std::endl;
            for (auto const& kv : fn_map)
            {
                std::cout << kv.first << ": run benchmark on " << kv.second.first << " functions" << std::endl;
            }
        }
        else
        {
            for (int i = 1; i < argc; ++i)
            {
                fn_map.at(argv[i]).second();
            }
        }
    }
    else
    {
        for (auto const& kv : fn_map)
        {
            kv.second.second();
        }
    }
    return 0;
}

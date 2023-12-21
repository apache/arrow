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

#ifndef XSIMD_CPUID_HPP
#define XSIMD_CPUID_HPP

#include <algorithm>
#include <cstring>

#if defined(__linux__) && (defined(__ARM_NEON) || defined(_M_ARM))
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif

#if defined(_MSC_VER)
// Contains the definition of __cpuidex
#include <intrin.h>
#endif

#include "../types/xsimd_all_registers.hpp"

namespace xsimd
{
    namespace detail
    {
        struct supported_arch
        {
            unsigned sse2 : 1;
            unsigned sse3 : 1;
            unsigned ssse3 : 1;
            unsigned sse4_1 : 1;
            unsigned sse4_2 : 1;
            unsigned sse4a : 1;
            unsigned fma3_sse : 1;
            unsigned fma4 : 1;
            unsigned xop : 1;
            unsigned avx : 1;
            unsigned fma3_avx : 1;
            unsigned avx2 : 1;
            unsigned fma3_avx2 : 1;
            unsigned avx512f : 1;
            unsigned avx512cd : 1;
            unsigned avx512dq : 1;
            unsigned avx512bw : 1;
            unsigned neon : 1;
            unsigned neon64 : 1;

            // version number of the best arch available
            unsigned best;

            supported_arch() noexcept
            {
                memset(this, 0, sizeof(supported_arch));

#if defined(__aarch64__) || defined(_M_ARM64)
                neon = 1;
                neon64 = 1;
                best = neon64::version();
#elif defined(__ARM_NEON) || defined(_M_ARM)
#if defined(__linux__)
                neon = bool(getauxval(AT_HWCAP) & HWCAP_NEON);
#else
                // that's very conservative :-/
                neon = 0;
#endif
                neon64 = 0;
                best = neon::version() * neon;

#elif defined(__x86_64__) || defined(__i386__) || defined(_M_AMD64) || defined(_M_IX86)
                auto get_cpuid = [](int reg[4], int func_id) noexcept
                {

#if defined(_MSC_VER)
                    __cpuidex(reg, func_id, 0);

#elif defined(__INTEL_COMPILER)
                    __cpuid(reg, func_id);

#elif defined(__GNUC__) || defined(__clang__)

#if defined(__i386__) && defined(__PIC__)
                    // %ebx may be the PIC register
                    __asm__("xchg{l}\t{%%}ebx, %1\n\t"
                            "cpuid\n\t"
                            "xchg{l}\t{%%}ebx, %1\n\t"
                            : "=a"(reg[0]), "=r"(reg[1]), "=c"(reg[2]),
                              "=d"(reg[3])
                            : "a"(func_id), "c"(0));

#else
                    __asm__("cpuid\n\t"
                            : "=a"(reg[0]), "=b"(reg[1]), "=c"(reg[2]),
                              "=d"(reg[3])
                            : "a"(func_id), "c"(0));
#endif

#else
#error "Unsupported configuration"
#endif
                };

                int regs[4];

                get_cpuid(regs, 0x1);

                sse2 = regs[2] >> 26 & 1;
                best = std::max(best, sse2::version() * sse2);

                sse3 = regs[2] >> 0 & 1;
                best = std::max(best, sse3::version() * sse3);

                ssse3 = regs[2] >> 9 & 1;
                best = std::max(best, ssse3::version() * ssse3);

                sse4_1 = regs[2] >> 19 & 1;
                best = std::max(best, sse4_1::version() * sse4_1);

                sse4_2 = regs[2] >> 20 & 1;
                best = std::max(best, sse4_2::version() * sse4_2);

                fma3_sse = regs[2] >> 12 & 1;
                if (sse4_2)
                    best = std::max(best, fma3<xsimd::sse4_2>::version() * fma3_sse);

                get_cpuid(regs, 0x80000001);
                fma4 = regs[2] >> 16 & 1;
                best = std::max(best, fma4::version() * fma4);

                // sse4a = regs[2] >> 6 & 1;
                // best = std::max(best, XSIMD_X86_AMD_SSE4A_VERSION * sse4a);

                // xop = regs[2] >> 11 & 1;
                // best = std::max(best, XSIMD_X86_AMD_XOP_VERSION * xop);

                avx = regs[2] >> 28 & 1;
                best = std::max(best, avx::version() * avx);

                fma3_avx = avx && fma3_sse;
                best = std::max(best, fma3<xsimd::avx>::version() * fma3_avx);

                get_cpuid(regs, 0x7);
                avx2 = regs[1] >> 5 & 1;
                best = std::max(best, avx2::version() * avx2);

                fma3_avx2 = avx2 && fma3_sse;
                best = std::max(best, fma3<xsimd::avx2>::version() * fma3_avx2);

                avx512f = regs[1] >> 16 & 1;
                best = std::max(best, avx512f::version() * avx512f);

                avx512cd = regs[1] >> 28 & 1;
                best = std::max(best, avx512cd::version() * avx512cd * avx512f);

                avx512dq = regs[1] >> 17 & 1;
                best = std::max(best, avx512dq::version() * avx512dq * avx512cd * avx512f);

                avx512bw = regs[1] >> 30 & 1;
                best = std::max(best, avx512bw::version() * avx512bw * avx512dq * avx512cd * avx512f);

#endif
            }
        };
    }

    inline detail::supported_arch available_architectures() noexcept
    {
        static detail::supported_arch supported;
        return supported;
    }
}

#endif

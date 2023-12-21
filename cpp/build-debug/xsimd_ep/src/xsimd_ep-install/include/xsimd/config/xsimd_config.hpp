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

#ifndef XSIMD_CONFIG_HPP
#define XSIMD_CONFIG_HPP

#define XSIMD_VERSION_MAJOR 9
#define XSIMD_VERSION_MINOR 0
#define XSIMD_VERSION_PATCH 1

/**
 * high level free functions
 *
 * @defgroup xsimd_config_macro Instruction Set Detection
 */

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if SSE2 is available at compile-time, to 0 otherwise.
 */
#ifdef __SSE2__
#define XSIMD_WITH_SSE2 1
#else
#define XSIMD_WITH_SSE2 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if SSE3 is available at compile-time, to 0 otherwise.
 */
#ifdef __SSE3__
#define XSIMD_WITH_SSE3 1
#else
#define XSIMD_WITH_SSE3 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if SSSE3 is available at compile-time, to 0 otherwise.
 */
#ifdef __SSSE3__
#define XSIMD_WITH_SSSE3 1
#else
#define XSIMD_WITH_SSSE3 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if SSE4.1 is available at compile-time, to 0 otherwise.
 */
#ifdef __SSE4_1__
#define XSIMD_WITH_SSE4_1 1
#else
#define XSIMD_WITH_SSE4_1 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if SSE4.2 is available at compile-time, to 0 otherwise.
 */
#ifdef __SSE4_2__
#define XSIMD_WITH_SSE4_2 1
#else
#define XSIMD_WITH_SSE4_2 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if AVX is available at compile-time, to 0 otherwise.
 */
#ifdef __AVX__
#define XSIMD_WITH_AVX 1
#else
#define XSIMD_WITH_AVX 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if AVX2 is available at compile-time, to 0 otherwise.
 */
#ifdef __AVX2__
#define XSIMD_WITH_AVX2 1
#else
#define XSIMD_WITH_AVX2 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if FMA3 for SSE is available at compile-time, to 0 otherwise.
 */
#ifdef __FMA__

#if defined(__SSE__) && !defined(__AVX__)
#ifndef XSIMD_WITH_FMA3_SSE // Leave the opportunity to manually disable it, see #643
#define XSIMD_WITH_FMA3_SSE 1
#endif
#else

#if XSIMD_WITH_FMA3_SSE
#error "Manually set XSIMD_WITH_FMA3_SSE is incompatible with current compiler flags"
#endif

#define XSIMD_WITH_FMA3_SSE 0
#endif

#else

#if XSIMD_WITH_FMA3_SSE
#error "Manually set XSIMD_WITH_FMA3_SSE is incompatible with current compiler flags"
#endif

#define XSIMD_WITH_FMA3_SSE 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if FMA3 for AVX is available at compile-time, to 0 otherwise.
 */
#ifdef __FMA__

#if defined(__AVX__)
#ifndef XSIMD_WITH_FMA3_AVX // Leave the opportunity to manually disable it, see #643
#define XSIMD_WITH_FMA3_AVX 1
#endif
#else

#if XSIMD_WITH_FMA3_AVX
#error "Manually set XSIMD_WITH_FMA3_AVX is incompatible with current compiler flags"
#endif

#define XSIMD_WITH_FMA3_AVX 0
#endif

#if defined(__AVX2__)
#ifndef XSIMD_WITH_FMA3_AVX2 // Leave the opportunity to manually disable it, see #643
#define XSIMD_WITH_FMA3_AVX2 1
#endif
#else

#if XSIMD_WITH_FMA3_AVX2
#error "Manually set XSIMD_WITH_FMA3_AVX2 is incompatible with current compiler flags"
#endif

#define XSIMD_WITH_FMA3_AVX2 0
#endif

#else

#if XSIMD_WITH_FMA3_AVX
#error "Manually set XSIMD_WITH_FMA3_AVX is incompatible with current compiler flags"
#endif

#if XSIMD_WITH_FMA3_AVX2
#error "Manually set XSIMD_WITH_FMA3_AVX2 is incompatible with current compiler flags"
#endif

#define XSIMD_WITH_FMA3_AVX 0
#define XSIMD_WITH_FMA3_AVX2 0

#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if FMA4 is available at compile-time, to 0 otherwise.
 */
#ifdef __FMA4__
#define XSIMD_WITH_FMA4 1
#else
#define XSIMD_WITH_FMA4 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if AVX512F is available at compile-time, to 0 otherwise.
 */
#ifdef __AVX512F__
// AVX512 instructions are supported starting with gcc 6
// see https://www.gnu.org/software/gcc/gcc-6/changes.html
// check clang first, newer clang always defines __GNUC__ = 4
#if defined(__clang__) && __clang_major__ >= 6
#define XSIMD_WITH_AVX512F 1
#elif defined(__GNUC__) && __GNUC__ < 6
#define XSIMD_WITH_AVX512F 0
#else
#define XSIMD_WITH_AVX512F 1
#if __GNUC__ == 6
#define XSIMD_AVX512_SHIFT_INTRINSICS_IMM_ONLY 1
#endif
#endif
#else
#define XSIMD_WITH_AVX512F 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if AVX512CD is available at compile-time, to 0 otherwise.
 */
#ifdef __AVX512CD__
// Avoids repeating the GCC workaround over and over
#define XSIMD_WITH_AVX512CD XSIMD_WITH_AVX512F
#else
#define XSIMD_WITH_AVX512CD 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if AVX512DQ is available at compile-time, to 0 otherwise.
 */
#ifdef __AVX512DQ__
#define XSIMD_WITH_AVX512DQ XSIMD_WITH_AVX512F
#else
#define XSIMD_WITH_AVX512DQ 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if AVX512BW is available at compile-time, to 0 otherwise.
 */
#ifdef __AVX512BW__
#define XSIMD_WITH_AVX512BW XSIMD_WITH_AVX512F
#else
#define XSIMD_WITH_AVX512BW 0
#endif

#ifdef __ARM_NEON

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if NEON is available at compile-time, to 0 otherwise.
 */
#if __ARM_ARCH >= 7
#define XSIMD_WITH_NEON 1
#else
#define XSIMD_WITH_NEON 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if NEON64 is available at compile-time, to 0 otherwise.
 */
#ifdef __aarch64__
#define XSIMD_WITH_NEON64 1
#else
#define XSIMD_WITH_NEON64 0
#endif
#else
#define XSIMD_WITH_NEON 0
#define XSIMD_WITH_NEON64 0
#endif

/**
 * @ingroup xsimd_config_macro
 *
 * Set to 1 if SVE is available and bit width is pre-set at compile-time, to 0 otherwise.
 */
#if defined(__ARM_FEATURE_SVE) && defined(__ARM_FEATURE_SVE_BITS) && __ARM_FEATURE_SVE_BITS > 0
#define XSIMD_WITH_SVE 1
#define XSIMD_SVE_BITS __ARM_FEATURE_SVE_BITS
#else
#define XSIMD_WITH_SVE 0
#define XSIMD_SVE_BITS 0
#endif

// Workaround for MSVC compiler
#ifdef _MSC_VER

#if XSIMD_WITH_AVX512
#undef XSIMD_WITH_AVX2
#define XSIMD_WITH_AVX2 1
#endif

#if XSIMD_WITH_AVX2
#undef XSIMD_WITH_AVX
#define XSIMD_WITH_AVX 1
#undef XSIMD_WITH_FMA3_AVX
#define XSIMD_WITH_FMA3_AVX 1
#undef XSIMD_WITH_FMA3_AVX2
#define XSIMD_WITH_FMA3_AVX2 1
#endif

#if XSIMD_WITH_AVX
#undef XSIMD_WITH_SSE4_2
#define XSIMD_WITH_SSE4_2 1
#endif

#if !defined(__clang__) && (defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 2))
#undef XSIMD_WITH_SSE4_2
#define XSIMD_WITH_SSE4_2 1
#endif

#if XSIMD_WITH_SSE4_2
#undef XSIMD_WITH_SSE4_1
#define XSIMD_WITH_SSE4_1 1
#endif

#if XSIMD_WITH_SSE4_1
#undef XSIMD_WITH_SSSE3
#define XSIMD_WITH_SSSE3 1
#endif

#if XSIMD_WITH_SSSE3
#undef XSIMD_WITH_SSE3
#define XSIMD_WITH_SSE3 1
#endif

#if XSIMD_WITH_SSE3 || (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#undef XSIMD_WITH_SSE2
#define XSIMD_WITH_SSE2 1
#endif

#endif

#if !XSIMD_WITH_SSE2 && !XSIMD_WITH_SSE3 && !XSIMD_WITH_SSSE3 && !XSIMD_WITH_SSE4_1 && !XSIMD_WITH_SSE4_2 && !XSIMD_WITH_AVX && !XSIMD_WITH_AVX2 && !XSIMD_WITH_FMA3_SSE && !XSIMD_WITH_FMA4 && !XSIMD_WITH_FMA3_AVX && !XSIMD_WITH_FMA3_AVX2 && !XSIMD_WITH_AVX512F && !XSIMD_WITH_AVX512CD && !XSIMD_WITH_AVX512DQ && !XSIMD_WITH_AVX512BW && !XSIMD_WITH_NEON && !XSIMD_WITH_NEON64 && !XSIMD_WITH_SVE
#define XSIMD_NO_SUPPORTED_ARCHITECTURE
#endif

#endif

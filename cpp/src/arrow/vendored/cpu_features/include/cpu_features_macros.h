// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CPU_FEATURES_INCLUDE_CPU_FEATURES_MACROS_H_
#define CPU_FEATURES_INCLUDE_CPU_FEATURES_MACROS_H_

////////////////////////////////////////////////////////////////////////////////
// Architectures
////////////////////////////////////////////////////////////////////////////////

#if defined(__pnacl__) || defined(__CLR_VER)
#define CPU_FEATURES_ARCH_VM
#endif

#if (defined(_M_IX86) || defined(__i386__)) && !defined(CPU_FEATURES_ARCH_VM)
#define CPU_FEATURES_ARCH_X86_32
#endif

#if (defined(_M_X64) || defined(__x86_64__)) && !defined(CPU_FEATURES_ARCH_VM)
#define CPU_FEATURES_ARCH_X86_64
#endif

#if defined(CPU_FEATURES_ARCH_X86_32) || defined(CPU_FEATURES_ARCH_X86_64)
#define CPU_FEATURES_ARCH_X86
#endif

#if (defined(__arm__) || defined(_M_ARM))
#define CPU_FEATURES_ARCH_ARM
#endif

#if defined(__aarch64__)
#define CPU_FEATURES_ARCH_AARCH64
#endif

#if (defined(CPU_FEATURES_ARCH_AARCH64) || defined(CPU_FEATURES_ARCH_ARM))
#define CPU_FEATURES_ARCH_ANY_ARM
#endif

#if defined(__mips64)
#define CPU_FEATURES_ARCH_MIPS64
#endif

#if defined(__mips__) && !defined(__mips64)  // mips64 also declares __mips__
#define CPU_FEATURES_ARCH_MIPS32
#endif

#if defined(CPU_FEATURES_ARCH_MIPS32) || defined(CPU_FEATURES_ARCH_MIPS64)
#define CPU_FEATURES_ARCH_MIPS
#endif

#if defined(__powerpc__)
#define CPU_FEATURES_ARCH_PPC
#endif

////////////////////////////////////////////////////////////////////////////////
// Os
////////////////////////////////////////////////////////////////////////////////

#if defined(__linux__)
#define CPU_FEATURES_OS_LINUX_OR_ANDROID
#endif

#if defined(__ANDROID__)
#define CPU_FEATURES_OS_ANDROID
#endif

#if (defined(_WIN64) || defined(_WIN32))
#define CPU_FEATURES_OS_WINDOWS
#endif

////////////////////////////////////////////////////////////////////////////////
// Compilers
////////////////////////////////////////////////////////////////////////////////

#if defined(__clang__)
#define CPU_FEATURES_COMPILER_CLANG
#endif

#if defined(__GNUC__) && !defined(__clang__)
#define CPU_FEATURES_COMPILER_GCC
#endif

#if defined(_MSC_VER)
#define CPU_FEATURES_COMPILER_MSC
#endif

////////////////////////////////////////////////////////////////////////////////
// Cpp
////////////////////////////////////////////////////////////////////////////////

#if defined(__cplusplus)
#define CPU_FEATURES_START_CPP_NAMESPACE \
  namespace cpu_features {               \
  extern "C" {
#define CPU_FEATURES_END_CPP_NAMESPACE \
  }                                    \
  }
#else
#define CPU_FEATURES_START_CPP_NAMESPACE
#define CPU_FEATURES_END_CPP_NAMESPACE
#endif

////////////////////////////////////////////////////////////////////////////////
// Compiler flags
////////////////////////////////////////////////////////////////////////////////

// Use the following to check if a feature is known to be available at
// compile time. See README.md for an example.
#if defined(CPU_FEATURES_ARCH_X86)
#define CPU_FEATURES_COMPILED_X86_AES defined(__AES__)
#define CPU_FEATURES_COMPILED_X86_F16C defined(__F16C__)
#define CPU_FEATURES_COMPILED_X86_BMI defined(__BMI__)
#define CPU_FEATURES_COMPILED_X86_BMI2 defined(__BMI2__)
#define CPU_FEATURES_COMPILED_X86_SSE (defined(__SSE__) || (_M_IX86_FP >= 1))
#define CPU_FEATURES_COMPILED_X86_SSE2 (defined(__SSE2__) || (_M_IX86_FP >= 2))
#define CPU_FEATURES_COMPILED_X86_SSE3 defined(__SSE3__)
#define CPU_FEATURES_COMPILED_X86_SSSE3 defined(__SSSE3__)
#define CPU_FEATURES_COMPILED_X86_SSE4_1 defined(__SSE4_1__)
#define CPU_FEATURES_COMPILED_X86_SSE4_2 defined(__SSE4_2__)
#define CPU_FEATURES_COMPILED_X86_AVX defined(__AVX__)
#define CPU_FEATURES_COMPILED_x86_AVX2 defined(__AVX2__)
#endif

#if defined(CPU_FEATURES_ARCH_ANY_ARM)
#define CPU_FEATURES_COMPILED_ANY_ARM_NEON defined(__ARM_NEON__)
#endif

#if defined(CPU_FEATURES_ARCH_MIPS)
#define CPU_FEATURES_COMPILED_MIPS_MSA defined(__mips_msa)
#endif

#endif  // CPU_FEATURES_INCLUDE_CPU_FEATURES_MACROS_H_

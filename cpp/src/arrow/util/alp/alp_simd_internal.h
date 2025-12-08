// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// SIMD-optimized decode functions for ALP compression

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

// Enable NEON on ARM64 if not already defined
#if defined(__aarch64__) && !defined(ARROW_HAVE_NEON)
#define ARROW_HAVE_NEON 1
#endif

#include "arrow/util/simd.h"

namespace arrow::util::alp::internal {

// ----------------------------------------------------------------------
// SIMD decode function declarations

/// \brief Decode ALP values using SIMD (fused unFOR + decode)
///
/// Decodes encoded integers back to floating point values:
/// 1. Add frame of reference (unFOR)
/// 2. Convert to signed integer
/// 3. Convert to float/double
/// 4. Multiply by int_factor (10^factor)
/// 5. Multiply by float_factor (10^-exponent)
///
/// Uses two separate multiplications to preserve exact floating-point behavior
/// matching the original scalar implementation.
///
/// \param[in] encoded_integers pointer to encoded values
/// \param[out] output pointer to output values
/// \param[in] num_elements number of elements to decode
/// \param[in] frame_of_reference frame of reference to add
/// \param[in] int_factor integer factor (10^factor from AlpConstants)
/// \param[in] float_factor floating-point factor (10^-exponent)
template <typename T, typename ExactType, typename SignedExactType>
void DecodeVectorScalar(const ExactType* encoded_integers, T* output,
                        size_t num_elements, ExactType frame_of_reference,
                        int64_t int_factor, T float_factor);

// ----------------------------------------------------------------------
// AVX2 implementations

#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX2)

/// \brief AVX2-optimized decode for double values
///
/// Processes 4 doubles at a time using 256-bit AVX2 registers.
/// \param[in] encoded_integers pointer to encoded uint64_t values
/// \param[out] output pointer to output double values
/// \param[in] num_elements number of elements to decode
/// \param[in] frame_of_reference frame of reference to add
/// \param[in] int_factor integer factor (10^factor)
/// \param[in] float_factor floating-point factor (10^-exponent)
void DecodeVectorAvx2Double(const uint64_t* encoded_integers, double* output,
                            size_t num_elements, uint64_t frame_of_reference,
                            int64_t int_factor, double float_factor);

/// \brief AVX2-optimized decode for float values
///
/// Processes 8 floats at a time using 256-bit AVX2 registers.
/// \param[in] encoded_integers pointer to encoded uint32_t values
/// \param[out] output pointer to output float values
/// \param[in] num_elements number of elements to decode
/// \param[in] frame_of_reference frame of reference to add
/// \param[in] int_factor integer factor (10^factor)
/// \param[in] float_factor floating-point factor (10^-exponent)
void DecodeVectorAvx2Float(const uint32_t* encoded_integers, float* output,
                           size_t num_elements, uint32_t frame_of_reference,
                           int64_t int_factor, float float_factor);

#endif  // ARROW_HAVE_AVX2

// ----------------------------------------------------------------------
// NEON implementations

#if defined(ARROW_HAVE_NEON)

/// \brief NEON-optimized decode for double values
///
/// Processes 2 doubles at a time using 128-bit NEON registers.
/// \param[in] encoded_integers pointer to encoded uint64_t values
/// \param[out] output pointer to output double values
/// \param[in] num_elements number of elements to decode
/// \param[in] frame_of_reference frame of reference to add
/// \param[in] int_factor integer factor (10^factor)
/// \param[in] float_factor floating-point factor (10^-exponent)
void DecodeVectorNeonDouble(const uint64_t* encoded_integers, double* output,
                            size_t num_elements, uint64_t frame_of_reference,
                            int64_t int_factor, double float_factor);

/// \brief NEON-optimized decode for float values
///
/// Processes 4 floats at a time using 128-bit NEON registers.
/// \param[in] encoded_integers pointer to encoded uint32_t values
/// \param[out] output pointer to output float values
/// \param[in] num_elements number of elements to decode
/// \param[in] frame_of_reference frame of reference to add
/// \param[in] int_factor integer factor (10^factor)
/// \param[in] float_factor floating-point factor (10^-exponent)
void DecodeVectorNeonFloat(const uint32_t* encoded_integers, float* output,
                           size_t num_elements, uint32_t frame_of_reference,
                           int64_t int_factor, float float_factor);

#endif  // ARROW_HAVE_NEON

// ----------------------------------------------------------------------
// Scalar implementation (fallback)

template <typename T, typename ExactType, typename SignedExactType>
void DecodeVectorScalar(const ExactType* encoded_integers, T* output,
                        size_t num_elements, ExactType frame_of_reference,
                        int64_t int_factor, T float_factor) {
  for (size_t i = 0; i < num_elements; ++i) {
    // 1. Apply frame of reference (unFOR)
    const ExactType unfored_value = encoded_integers[i] + frame_of_reference;
    // 2. Reinterpret as signed integer
    SignedExactType signed_value;
    std::memcpy(&signed_value, &unfored_value, sizeof(SignedExactType));
    // 3. Convert to float and multiply by two factors (preserves original precision)
    output[i] = static_cast<T>(signed_value) * int_factor * float_factor;
  }
}

// ----------------------------------------------------------------------
// AVX2 inline implementations

#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX2)

inline void DecodeVectorAvx2Double(const uint64_t* encoded_integers, double* output,
                                   size_t num_elements, uint64_t frame_of_reference,
                                   int64_t int_factor, double float_factor) {
  // AVX2: Process 4 doubles at a time (256 bits)
  constexpr size_t kSimdWidth = 4;
  const size_t num_simd_elements = (num_elements / kSimdWidth) * kSimdWidth;

  // Broadcast values to all lanes
  const __m256i for_vec = _mm256_set1_epi64x(static_cast<int64_t>(frame_of_reference));
  const __m256d int_factor_vec = _mm256_set1_pd(static_cast<double>(int_factor));
  const __m256d float_factor_vec = _mm256_set1_pd(float_factor);

  for (size_t i = 0; i < num_simd_elements; i += kSimdWidth) {
    // Load 4 uint64_t values
    __m256i encoded = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(encoded_integers + i));

    // Add frame of reference (unsigned add via signed - same bits)
    __m256i unfored = _mm256_add_epi64(encoded, for_vec);

    // Convert int64 to double
    // AVX2 lacks direct int64->double, so we extract and convert individually
    alignas(32) int64_t temp_ints[4];
    _mm256_store_si256(reinterpret_cast<__m256i*>(temp_ints), unfored);

    __m256d converted = _mm256_set_pd(
        static_cast<double>(temp_ints[3]),
        static_cast<double>(temp_ints[2]),
        static_cast<double>(temp_ints[1]),
        static_cast<double>(temp_ints[0]));

    // Two-step multiplication to match scalar precision:
    // result = value * int_factor * float_factor
    __m256d step1 = _mm256_mul_pd(converted, int_factor_vec);
    __m256d result = _mm256_mul_pd(step1, float_factor_vec);

    // Store result
    _mm256_storeu_pd(output + i, result);
  }

  // Handle remaining elements with scalar code
  for (size_t i = num_simd_elements; i < num_elements; ++i) {
    const uint64_t unfored_value = encoded_integers[i] + frame_of_reference;
    int64_t signed_value;
    std::memcpy(&signed_value, &unfored_value, sizeof(int64_t));
    output[i] = static_cast<double>(signed_value) * int_factor * float_factor;
  }
}

inline void DecodeVectorAvx2Float(const uint32_t* encoded_integers, float* output,
                                  size_t num_elements, uint32_t frame_of_reference,
                                  int64_t int_factor, float float_factor) {
  // AVX2: Process 8 floats at a time (256 bits)
  constexpr size_t kSimdWidth = 8;
  const size_t num_simd_elements = (num_elements / kSimdWidth) * kSimdWidth;

  // Broadcast values to all lanes
  // For float, int_factor is small enough to fit in float without precision loss
  const __m256i for_vec = _mm256_set1_epi32(static_cast<int32_t>(frame_of_reference));
  const __m256 int_factor_vec = _mm256_set1_ps(static_cast<float>(int_factor));
  const __m256 float_factor_vec = _mm256_set1_ps(float_factor);

  for (size_t i = 0; i < num_simd_elements; i += kSimdWidth) {
    // Load 8 uint32_t values
    __m256i encoded = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(encoded_integers + i));

    // Add frame of reference
    __m256i unfored = _mm256_add_epi32(encoded, for_vec);

    // Convert int32 to float (AVX2 has direct instruction for this)
    __m256 converted = _mm256_cvtepi32_ps(unfored);

    // Two-step multiplication to match scalar precision:
    // result = value * int_factor * float_factor
    __m256 step1 = _mm256_mul_ps(converted, int_factor_vec);
    __m256 result = _mm256_mul_ps(step1, float_factor_vec);

    // Store result
    _mm256_storeu_ps(output + i, result);
  }

  // Handle remaining elements with scalar code
  for (size_t i = num_simd_elements; i < num_elements; ++i) {
    const uint32_t unfored_value = encoded_integers[i] + frame_of_reference;
    int32_t signed_value;
    std::memcpy(&signed_value, &unfored_value, sizeof(int32_t));
    output[i] = static_cast<float>(signed_value) * int_factor * float_factor;
  }
}

#endif  // ARROW_HAVE_AVX2

// ----------------------------------------------------------------------
// NEON inline implementations

#if defined(ARROW_HAVE_NEON)

inline void DecodeVectorNeonDouble(const uint64_t* encoded_integers, double* output,
                                   size_t num_elements, uint64_t frame_of_reference,
                                   int64_t int_factor, double float_factor) {
  // NEON: Process 2 doubles at a time (128 bits)
  constexpr size_t kSimdWidth = 2;
  const size_t num_simd_elements = (num_elements / kSimdWidth) * kSimdWidth;

  // Broadcast values to all lanes
  const uint64x2_t for_vec = vdupq_n_u64(frame_of_reference);
  const float64x2_t int_factor_vec = vdupq_n_f64(static_cast<double>(int_factor));
  const float64x2_t float_factor_vec = vdupq_n_f64(float_factor);

  for (size_t i = 0; i < num_simd_elements; i += kSimdWidth) {
    // Load 2 uint64_t values
    uint64x2_t encoded = vld1q_u64(encoded_integers + i);

    // Add frame of reference
    uint64x2_t unfored = vaddq_u64(encoded, for_vec);

    // Reinterpret as signed and convert to double
    int64x2_t signed_vals = vreinterpretq_s64_u64(unfored);
    float64x2_t converted = vcvtq_f64_s64(signed_vals);

    // Two-step multiplication to match scalar precision
    float64x2_t step1 = vmulq_f64(converted, int_factor_vec);
    float64x2_t result = vmulq_f64(step1, float_factor_vec);

    // Store result
    vst1q_f64(output + i, result);
  }

  // Handle remaining elements with scalar code
  for (size_t i = num_simd_elements; i < num_elements; ++i) {
    const uint64_t unfored_value = encoded_integers[i] + frame_of_reference;
    int64_t signed_value;
    std::memcpy(&signed_value, &unfored_value, sizeof(int64_t));
    output[i] = static_cast<double>(signed_value) * int_factor * float_factor;
  }
}

inline void DecodeVectorNeonFloat(const uint32_t* encoded_integers, float* output,
                                  size_t num_elements, uint32_t frame_of_reference,
                                  int64_t int_factor, float float_factor) {
  // NEON: Process 4 floats at a time (128 bits)
  constexpr size_t kSimdWidth = 4;
  const size_t num_simd_elements = (num_elements / kSimdWidth) * kSimdWidth;

  // Broadcast values to all lanes
  const uint32x4_t for_vec = vdupq_n_u32(frame_of_reference);
  const float32x4_t int_factor_vec = vdupq_n_f32(static_cast<float>(int_factor));
  const float32x4_t float_factor_vec = vdupq_n_f32(float_factor);

  for (size_t i = 0; i < num_simd_elements; i += kSimdWidth) {
    // Load 4 uint32_t values
    uint32x4_t encoded = vld1q_u32(encoded_integers + i);

    // Add frame of reference
    uint32x4_t unfored = vaddq_u32(encoded, for_vec);

    // Reinterpret as signed and convert to float
    int32x4_t signed_vals = vreinterpretq_s32_u32(unfored);
    float32x4_t converted = vcvtq_f32_s32(signed_vals);

    // Two-step multiplication to match scalar precision
    float32x4_t step1 = vmulq_f32(converted, int_factor_vec);
    float32x4_t result = vmulq_f32(step1, float_factor_vec);

    // Store result
    vst1q_f32(output + i, result);
  }

  // Handle remaining elements with scalar code
  for (size_t i = num_simd_elements; i < num_elements; ++i) {
    const uint32_t unfored_value = encoded_integers[i] + frame_of_reference;
    int32_t signed_value;
    std::memcpy(&signed_value, &unfored_value, sizeof(int32_t));
    output[i] = static_cast<float>(signed_value) * int_factor * float_factor;
  }
}

#endif  // ARROW_HAVE_NEON

// ----------------------------------------------------------------------
// Dispatcher functions

/// \brief Dispatch to best available SIMD implementation for double decode
///
/// Automatically selects AVX2, NEON, or scalar implementation based on
/// compile-time feature detection.
/// \param[in] encoded_integers pointer to encoded uint64_t values
/// \param[out] output pointer to output double values
/// \param[in] num_elements number of elements to decode
/// \param[in] frame_of_reference frame of reference to add
/// \param[in] int_factor integer factor (10^factor)
/// \param[in] float_factor floating-point factor (10^-exponent)
inline void DecodeVectorSimdDouble(const uint64_t* encoded_integers, double* output,
                                   size_t num_elements, uint64_t frame_of_reference,
                                   int64_t int_factor, double float_factor) {
#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX2)
  DecodeVectorAvx2Double(encoded_integers, output, num_elements, frame_of_reference,
                         int_factor, float_factor);
#elif defined(ARROW_HAVE_NEON)
  DecodeVectorNeonDouble(encoded_integers, output, num_elements, frame_of_reference,
                         int_factor, float_factor);
#else
  DecodeVectorScalar<double, uint64_t, int64_t>(encoded_integers, output, num_elements,
                                                frame_of_reference, int_factor,
                                                float_factor);
#endif
}

/// \brief Dispatch to best available SIMD implementation for float decode
///
/// Automatically selects AVX2, NEON, or scalar implementation based on
/// compile-time feature detection.
/// \param[in] encoded_integers pointer to encoded uint32_t values
/// \param[out] output pointer to output float values
/// \param[in] num_elements number of elements to decode
/// \param[in] frame_of_reference frame of reference to add
/// \param[in] int_factor integer factor (10^factor)
/// \param[in] float_factor floating-point factor (10^-exponent)
inline void DecodeVectorSimdFloat(const uint32_t* encoded_integers, float* output,
                                  size_t num_elements, uint32_t frame_of_reference,
                                  int64_t int_factor, float float_factor) {
#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX2)
  DecodeVectorAvx2Float(encoded_integers, output, num_elements, frame_of_reference,
                        int_factor, float_factor);
#elif defined(ARROW_HAVE_NEON)
  DecodeVectorNeonFloat(encoded_integers, output, num_elements, frame_of_reference,
                        int_factor, float_factor);
#else
  DecodeVectorScalar<float, uint32_t, int32_t>(encoded_integers, output, num_elements,
                                               frame_of_reference, int_factor,
                                               float_factor);
#endif
}

}  // namespace arrow::util::alp::internal


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

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_basic_internal.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/align_util.h"
#include "arrow/util/simd.h"

TARGET_CODE_START_AVX2_BMI2
namespace arrow {
namespace compute {
namespace aggregate {

// ----------------------------------------------------------------------
// Sum implementation for AVX2

// Each m256 stream handle 4 double/int64 accumulator type, one batch has 4 streams.
static constexpr int kAvx2BatchStreams = 4;
static constexpr int kAvx2StreamSize = sizeof(__m256d) / sizeof(double);
static constexpr int kAvx2BatchSize = kAvx2BatchStreams * kAvx2StreamSize;
static constexpr int kAvx2BatchBytes = kAvx2BatchSize / 8;

// Default scalar version
template <typename T, typename SumT>
inline SumResult<SumT> SumDenseBatchAvx2(const T* values, int64_t num_batch) {
  SumResult<SumT> sum_result;
  SumT sum_streams[kAvx2BatchSize] = {0};

  // Add the results by streams
  for (int64_t batch = 0; batch < num_batch; batch++) {
    for (int i = 0; i < kAvx2BatchSize; i++) {
      sum_streams[i] += values[(batch * kAvx2BatchSize) + i];
    }
  }

  // Aggregate the result streams
  for (int i = 0; i < kAvx2BatchSize; i++) {
    sum_result.sum += sum_streams[i];
  }
  sum_result.count = num_batch * kAvx2BatchSize;
  return sum_result;
}

// Dense helper for accumulator type is same to data type
#define SUM_DENSE_BATCH_AVX2_DIRECT(Type, SumSimdType, SimdZeroFn, SimdLoadFn,      \
                                    SimdAddFn)                                      \
  template <>                                                                       \
  inline SumResult<Type> SumDenseBatchAvx2(const Type* values, int64_t num_batch) { \
    SumResult<Type> sum_result;                                                     \
    SumSimdType results_simd[kAvx2BatchStreams];                                    \
    for (int i = 0; i < kAvx2BatchStreams; i++) {                                   \
      results_simd[i] = SimdZeroFn();                                               \
    }                                                                               \
                                                                                    \
    /* Add the values to result streams */                                          \
    for (int64_t batch = 0; batch < num_batch; batch++) {                           \
      for (int i = 0; i < kAvx2BatchStreams; i++) {                                 \
        const auto src_simd = SimdLoadFn(&values[batch * kAvx2BatchSize + 4 * i]);  \
        results_simd[i] = SimdAddFn(src_simd, results_simd[i]);                     \
      }                                                                             \
    }                                                                               \
                                                                                    \
    const Type* results_scalar = reinterpret_cast<const Type*>(&results_simd);      \
    for (int stream = 0; stream < kAvx2BatchStreams; stream++) {                    \
      /* Each AVX2 stream has four accumulator type vaules */                       \
      for (int i = 0; i < kAvx2StreamSize; i++) {                                   \
        sum_result.sum += results_scalar[kAvx2StreamSize * stream + i];             \
      }                                                                             \
    }                                                                               \
    sum_result.count = num_batch * kAvx2BatchSize;                                  \
    return sum_result;                                                              \
  }

// Dense version for double
SUM_DENSE_BATCH_AVX2_DIRECT(double, __m256d, _mm256_setzero_pd, _mm256_load_pd,
                            _mm256_add_pd)
// Dense version for int64_t
SUM_DENSE_BATCH_AVX2_DIRECT(int64_t, __m256i, _mm256_setzero_si256, LOAD_SI256,
                            _mm256_add_epi64)
// Dense version for uint64_t
SUM_DENSE_BATCH_AVX2_DIRECT(uint64_t, __m256i, _mm256_setzero_si256, LOAD_SI256,
                            _mm256_add_epi64)

// Dense helper for which need a converter from data type to accumulator type
#define SUM_DENSE_BATCH_AVX2_CVT(Type, SumType, SumSimdType, SimdZeroFn, SimdLoadFn,   \
                                 SimdCvtFn, SimdAddFn)                                 \
  template <>                                                                          \
  inline SumResult<SumType> SumDenseBatchAvx2(const Type* values, int64_t num_batch) { \
    SumResult<SumType> sum_result;                                                     \
    SumSimdType results_simd[kAvx2BatchStreams];                                       \
    for (int i = 0; i < kAvx2BatchStreams; i++) {                                      \
      results_simd[i] = SimdZeroFn();                                                  \
    }                                                                                  \
                                                                                       \
    /* Covert to the target type, then add the values to result streams */             \
    for (int64_t batch = 0; batch < num_batch; batch++) {                              \
      for (int i = 0; i < kAvx2BatchStreams; i++) {                                    \
        const auto src_simd = SimdLoadFn(&values[batch * kAvx2BatchSize + 4 * i]);     \
        const auto cvt_simd = SimdCvtFn(src_simd);                                     \
        results_simd[i] = SimdAddFn(cvt_simd, results_simd[i]);                        \
      }                                                                                \
    }                                                                                  \
                                                                                       \
    const SumType* results_scalar = reinterpret_cast<const SumType*>(&results_simd);   \
    for (int stream = 0; stream < kAvx2BatchStreams; stream++) {                       \
      /* Each AVX2 stream has four accumulator type vaules */                          \
      for (int i = 0; i < kAvx2StreamSize; i++) {                                      \
        sum_result.sum += results_scalar[kAvx2StreamSize * stream + i];                \
      }                                                                                \
    }                                                                                  \
    sum_result.count = num_batch * kAvx2BatchSize;                                     \
    return sum_result;                                                                 \
  }

// Dense version for float
SUM_DENSE_BATCH_AVX2_CVT(float, double, __m256d, _mm256_setzero_pd, _mm_load_ps,
                         _mm256_cvtps_pd, _mm256_add_pd)
// Dense version for int32_t
SUM_DENSE_BATCH_AVX2_CVT(int32_t, int64_t, __m256i, _mm256_setzero_si256, LOAD_SI128,
                         _mm256_cvtepi32_epi64, _mm256_add_epi64)
// Dense version for uint32_t
SUM_DENSE_BATCH_AVX2_CVT(uint32_t, uint64_t, __m256i, _mm256_setzero_si256, LOAD_SI128,
                         _mm256_cvtepu32_epi64, _mm256_add_epi64)
// Dense version for int16_t
SUM_DENSE_BATCH_AVX2_CVT(int16_t, int64_t, __m256i, _mm256_setzero_si256, LOADU_SI128,
                         _mm256_cvtepi16_epi64, _mm256_add_epi64)
// Dense version for uint16_t
SUM_DENSE_BATCH_AVX2_CVT(uint16_t, uint64_t, __m256i, _mm256_setzero_si256, LOADU_SI128,
                         _mm256_cvtepu16_epi64, _mm256_add_epi64)
// Dense version for int8_t
SUM_DENSE_BATCH_AVX2_CVT(int8_t, int64_t, __m256i, _mm256_setzero_si256, LOADU_SI128,
                         _mm256_cvtepi8_epi64, _mm256_add_epi64)
// Dense version for uint8_t
SUM_DENSE_BATCH_AVX2_CVT(uint8_t, uint64_t, __m256i, _mm256_setzero_si256, LOADU_SI128,
                         _mm256_cvtepu8_epi64, _mm256_add_epi64)

template <typename T, typename SumT>
// Default version for sparse batch
inline SumResult<SumT> SumSparseBatchAvx2(const uint8_t* bitmap, const T* values,
                                          int64_t num_batch) {
  SumResult<SumT> sum_result;

  for (int64_t batch = 0; batch < num_batch; batch++) {
    for (int i = 0; i < kAvx2BatchBytes; i++) {
      SumResult<SumT> result = SumSparseByte<T, SumT>(
          bitmap[kAvx2BatchBytes * batch + i], &values[batch * kAvx2BatchSize + i * 8]);
      sum_result.sum += result.sum;
      sum_result.count += result.count;
    }
  }

  return sum_result;
}

// Get the m256i and mask for the valid bits of double/int64, bits should < (1 << 4)
inline __m256i And256iMaskForEpi64(uint8_t bits) {
  const __m256i target = _mm256_set1_epi64x(1);
  // Map the 4-bits mask to 256i by two steps, 4 bits -> 32 bits -> 256 bits
  const __m256i mask_256 =
      _mm256_cvtepu8_epi64(_mm_cvtsi32_si128(_pdep_u32(bits, 0x01010101)));
  // Generate the filter 256 mask by comparing with the target
  return _mm256_cmpeq_epi64(mask_256, target);
}

// Get the m256i and mask for the valid bits of float/int32
inline __m256i And256iMaskForEpi32(uint8_t bits) {
  const __m256i target = _mm256_set1_epi32(1);
  // Map the 8-bits mask to 256i by two steps, 8 bits -> 64 bits -> 256 bits
  const __m256i mask_256 =
      _mm256_cvtepu8_epi32(_mm_cvtsi64_si128(_pdep_u64(bits, 0x0101010101010101)));
  // Generate the filter 256 mask by comparing with the target 256i full valid mask
  return _mm256_cmpeq_epi32(mask_256, target);
}

#define SUM_SPARSE_BATCH_AVX2_START(Type, SimdType, SimdZeroFn) \
  SumResult<Type> sum_result;                                   \
  SimdType results_simd[kAvx2BatchStreams];                     \
  for (int i = 0; i < kAvx2BatchStreams; i++) {                 \
    results_simd[i] = SimdZeroFn();                             \
  }

#define SUM_SPARSE_BATCH_AVX2_END(Type)                                     \
  const auto results_scalar = reinterpret_cast<const Type*>(&results_simd); \
  for (int stream = 0; stream < kAvx2BatchStreams; stream++) {              \
    /* Each AVX2 stream has four accumulator type vaules */                 \
    for (int i = 0; i < kAvx2StreamSize; i++) {                             \
      sum_result.sum += results_scalar[kAvx2StreamSize * stream + i];       \
    }                                                                       \
  }

#define SUM_SPARSE_BATCH_AVX2_EPI64(Type, LoadSimd, AddSimd)                          \
  for (int64_t batch = 0; batch < num_batch; batch++) {                               \
    for (int i = 0; i < kAvx2BatchBytes; i++) {                                       \
      const uint8_t bits = bitmap[batch * kAvx2BatchBytes + i];                       \
      /* First handle the low 4 bits */                                               \
      __m256i and_mask = And256iMaskForEpi64(bits & 0x0F);                            \
      /* Load the data(4 doubles) and filter the invalid values to zero */            \
      __m256i src = _mm256_loadu_si256(                                               \
          reinterpret_cast<const __m256i*>(&values[batch * kAvx2BatchSize + i * 8])); \
      __m256i valid = _mm256_and_si256(src, and_mask);                                \
      auto valid_values = reinterpret_cast<Type*>(&valid);                            \
      results_simd[i * kAvx2BatchBytes] =                                             \
          AddSimd(LoadSimd(valid_values), results_simd[i * kAvx2BatchBytes]);         \
                                                                                      \
      /* Handle the high 4 bits */                                                    \
      and_mask = And256iMaskForEpi64((bits >> 4) & 0x0F);                             \
      src = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(                      \
          &values[batch * kAvx2BatchSize + i * 8 + 4]));                              \
      valid = _mm256_and_si256(src, and_mask);                                        \
      valid_values = reinterpret_cast<Type*>(&valid);                                 \
      results_simd[i * kAvx2BatchBytes + 1] =                                         \
          AddSimd(LoadSimd(valid_values), results_simd[i * kAvx2BatchBytes + 1]);     \
                                                                                      \
      sum_result.count += BitUtil::kBytePopcount[bits];                               \
    }                                                                                 \
  }

template <>
// Sparse version for double
inline SumResult<double> SumSparseBatchAvx2(const uint8_t* bitmap, const double* values,
                                            int64_t num_batch) {
  SUM_SPARSE_BATCH_AVX2_START(double, __m256d, _mm256_setzero_pd)
  SUM_SPARSE_BATCH_AVX2_EPI64(double, _mm256_load_pd, _mm256_add_pd)
  SUM_SPARSE_BATCH_AVX2_END(double)
  return sum_result;
}

template <>
// Sparse version for int64_t
inline SumResult<int64_t> SumSparseBatchAvx2(const uint8_t* bitmap, const int64_t* values,
                                             int64_t num_batch) {
  SUM_SPARSE_BATCH_AVX2_START(int64_t, __m256i, _mm256_setzero_si256)
  SUM_SPARSE_BATCH_AVX2_EPI64(int64_t, LOAD_SI256, _mm256_add_epi64)
  SUM_SPARSE_BATCH_AVX2_END(int64_t)
  return sum_result;
}

template <>
// Sparse version for uint64_t
inline SumResult<uint64_t> SumSparseBatchAvx2(const uint8_t* bitmap,
                                              const uint64_t* values, int64_t num_batch) {
  SUM_SPARSE_BATCH_AVX2_START(uint64_t, __m256i, _mm256_setzero_si256)
  SUM_SPARSE_BATCH_AVX2_EPI64(uint64_t, LOAD_SI256, _mm256_add_epi64)
  SUM_SPARSE_BATCH_AVX2_END(uint64_t)
  return sum_result;
}

#define SUM_SPARSE_BATCH_AVX2_EPI32(Type, LoadSimd, CvtSimd, AddSimd)                  \
  for (int64_t batch = 0; batch < num_batch; batch++) {                                \
    for (int i = 0; i < kAvx2BatchBytes; i++) {                                        \
      const uint8_t bits = bitmap[batch * kAvx2BatchBytes + i];                        \
      const __m256i and_mask = And256iMaskForEpi32(bits);                              \
      /* Load 8 float and replace the invalid values to zero */                        \
      const __m256i src = _mm256_loadu_si256(                                          \
          reinterpret_cast<const __m256i*>(&values[batch * kAvx2BatchSize + i * 8]));  \
      const __m256i valid = _mm256_and_si256(src, and_mask);                           \
      const auto valid_values = reinterpret_cast<const Type*>(&valid);                 \
                                                                                       \
      /* Convert and add the first 4 floats */                                         \
      results_simd[i * kAvx2BatchBytes] = AddSimd(CvtSimd(LoadSimd(&valid_values[0])), \
                                                  results_simd[i * kAvx2BatchBytes]);  \
      /* Convert and add the second 4 floats */                                        \
      results_simd[i * kAvx2BatchBytes + 1] = AddSimd(                                 \
          CvtSimd(LoadSimd(&valid_values[4])), results_simd[i * kAvx2BatchBytes + 1]); \
                                                                                       \
      sum_result.count += BitUtil::kBytePopcount[bits];                                \
    }                                                                                  \
  }

template <>
// Sparse version for float
inline SumResult<double> SumSparseBatchAvx2(const uint8_t* bitmap, const float* values,
                                            int64_t num_batch) {
  SUM_SPARSE_BATCH_AVX2_START(double, __m256d, _mm256_setzero_pd)
  SUM_SPARSE_BATCH_AVX2_EPI32(float, _mm_load_ps, _mm256_cvtps_pd, _mm256_add_pd)
  SUM_SPARSE_BATCH_AVX2_END(double)
  return sum_result;
}

template <>
// Sparse version for int32_t
inline SumResult<int64_t> SumSparseBatchAvx2(const uint8_t* bitmap, const int32_t* values,
                                             int64_t num_batch) {
  SUM_SPARSE_BATCH_AVX2_START(int64_t, __m256i, _mm256_setzero_si256)
  SUM_SPARSE_BATCH_AVX2_EPI32(int32_t, LOAD_SI128, _mm256_cvtepi32_epi64,
                              _mm256_add_epi64)
  SUM_SPARSE_BATCH_AVX2_END(int64_t)
  return sum_result;
}

template <>
// Sparse version for uint32_t
inline SumResult<uint64_t> SumSparseBatchAvx2(const uint8_t* bitmap,
                                              const uint32_t* values, int64_t num_batch) {
  SUM_SPARSE_BATCH_AVX2_START(uint64_t, __m256i, _mm256_setzero_si256)
  SUM_SPARSE_BATCH_AVX2_EPI32(uint32_t, LOAD_SI128, _mm256_cvtepu32_epi64,
                              _mm256_add_epi64)
  SUM_SPARSE_BATCH_AVX2_END(uint64_t)
  return sum_result;
}

template <typename ArrowType,
          typename SumType = typename FindAccumulatorType<ArrowType>::Type>
struct SumStateAvx2 {
  using ThisType = SumStateAvx2<ArrowType, SumType>;
  using T = typename TypeTraits<ArrowType>::CType;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using SumT = typename SumType::c_type;

  ThisType operator+(const ThisType& rhs) const {
    return ThisType(this->count + rhs.count, this->sum + rhs.sum);
  }

  ThisType& operator+=(const ThisType& rhs) {
    this->count += rhs.count;
    this->sum += rhs.sum;

    return *this;
  }

 public:
  void Consume(const Array& input) {
    const ArrayType& array = static_cast<const ArrayType&>(input);
    if (input.null_count() == 0) {
      (*this) += ConsumeDense(array);
    } else {
      (*this) += ConsumeSparse(array);
    }
  }

  size_t count = 0;
  SumT sum = 0;

 private:
  ThisType ConsumeDense(const ArrayType& array) const {
    ThisType local;
    const auto values = array.raw_values();
    const int64_t length = array.length();
    int64_t idx = 0;

    // For better performance, SIMD aligned(32-byte) load used for AVX2
    for (int64_t i = 0; i < length; ++i) {
      if (0 == (reinterpret_cast<int64_t>(&values[idx]) & 0xFF)) {
        // Address aligned to 32-byte already
        break;
      }

      local.sum += values[idx];
      idx++;
    }

    // Fix for AddressSanitizer: heap-buffer-overflow READ of size
    // Each batch handle 4 values, and the SIMD load size is 128 at least
    constexpr int64_t kSafeSimdLoadLen =
        (8 == sizeof(T)) ? 0 : (sizeof(__m128i) / sizeof(T)) - kAvx2StreamSize;
    // Parts can fill into batches
    if ((length - idx) > kSafeSimdLoadLen) {
      const int64_t length_batched =
          BitUtil::RoundDown(length - idx - kSafeSimdLoadLen, kAvx2BatchSize);
      SumResult<SumT> sum_result =
          SumDenseBatchAvx2<T, SumT>(&values[idx], length_batched / kAvx2BatchSize);

      local.sum += sum_result.sum;
      idx += sum_result.count;
    }

    // The trailing part
    for (; idx < length; idx++) {
      local.sum += values[idx];
    }

    local.count = idx;
    return local;
  }

  ThisType ConsumeSparse(const ArrayType& array) const {
    ThisType local;
    const uint8_t* valid_bits = array.null_bitmap_data();
    const int64_t offset = array.offset();
    const T* values = array.raw_values();
    const auto p = arrow::internal::BitmapWordAlign<kAvx2BatchBytes>(valid_bits, offset,
                                                                     array.length());

    // First handle the leading bits
    const int64_t leading_bits = p.leading_bits;
    if (leading_bits > 0) {
      SumResult<SumT> sum_result =
          SumSparseBits<T, SumT>(valid_bits, offset, values, leading_bits);
      local.sum += sum_result.sum;
      local.count += sum_result.count;
    }

    // The aligned parts
    const int64_t aligned_words = p.aligned_words;
    if (aligned_words > 0) {
      SumResult<SumT> sum_result = SumSparseBatchAvx2<T, SumT>(
          p.aligned_start, &values[leading_bits], aligned_words);
      local.sum += sum_result.sum;
      local.count += sum_result.count;
    }

    // The trailing bits
    const int64_t trailing_bits = p.trailing_bits;
    if (trailing_bits > 0) {
      SumResult<SumT> sum_result = SumSparseBits<T, SumT>(
          valid_bits, p.trailing_bit_offset,
          &values[leading_bits + aligned_words * kAvx2BatchSize], trailing_bits);
      local.sum += sum_result.sum;
      local.count += sum_result.count;
    }

    return local;
  }
};

template <typename ArrowType>
struct SumImplAvx2 : public ScalarAggregator {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ThisType = SumImplAvx2<ArrowType>;
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using OutputType = typename TypeTraits<SumType>::ScalarType;

  void Consume(KernelContext*, const ExecBatch& batch) override {
    this->state.Consume(ArrayType(batch[0].array()));
  }

  void MergeFrom(KernelContext*, const KernelState& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->state += other.state;
  }

  void Finalize(KernelContext*, Datum* out) override {
    if (state.count == 0) {
      out->value = std::make_shared<OutputType>();
    } else {
      out->value = MakeScalar(state.sum);
    }
  }

  SumStateAvx2<ArrowType> state;
};

template <typename ArrowType>
struct MeanImplAvx2 : public SumImplAvx2<ArrowType> {
  void Finalize(KernelContext*, Datum* out) override {
    const bool is_valid = this->state.count > 0;
    const double divisor = static_cast<double>(is_valid ? this->state.count : 1UL);
    const double mean = static_cast<double>(this->state.sum) / divisor;

    if (!is_valid) {
      out->value = std::make_shared<DoubleScalar>();
    } else {
      out->value = std::make_shared<DoubleScalar>(mean);
    }
  }
};

std::unique_ptr<KernelState> SumInitAvx2(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<SumImplAvx2> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

std::unique_ptr<KernelState> MeanInitAvx2(KernelContext* ctx,
                                          const KernelInitArgs& args) {
  SumLikeInit<MeanImplAvx2> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

}  // namespace aggregate

namespace internal {
using arrow::compute::aggregate::AddBasicAggKernels;
using arrow::compute::aggregate::MeanInitAvx2;
using arrow::compute::aggregate::SumInitAvx2;

void RegisterScalarAggregateBasicAvx2(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary());
  AddBasicAggKernels(SumInitAvx2, SignedIntTypes(), int64(), func.get());
  AddBasicAggKernels(SumInitAvx2, UnsignedIntTypes(), uint64(), func.get());
  AddBasicAggKernels(SumInitAvx2, FloatingPointTypes(), float64(), func.get());
  // Overwrite the default scalar version
  DCHECK_OK(registry->AddFunction(std::move(func), true));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary());
  AddBasicAggKernels(MeanInitAvx2, NumericTypes(), float64(), func.get());
  // Overwrite the default scalar version
  DCHECK_OK(registry->AddFunction(std::move(func), true));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
TARGET_CODE_STOP

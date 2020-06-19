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

TARGET_CODE_START_AVX512
namespace arrow {
namespace compute {
namespace aggregate {

// ----------------------------------------------------------------------
// Sum implementation for AVX512

// Each m512 stream handle 8 double/int64 accumulator type, one batch has 4 streams.
static constexpr int kAvx512BatchStreams = 4;
static constexpr int kAvx512StreamSize = sizeof(__m512d) / sizeof(double);
static constexpr int kAvx512BatchSize = kAvx512BatchStreams * kAvx512StreamSize;
static constexpr int kAvx512BatchBytes = kAvx512BatchSize / 8;

// Default scalar version
template <typename T, typename SumT>
inline SumResult<SumT> SumDenseBatchAvx512(const T* values, int64_t num_batch) {
  SumResult<SumT> sum_result;
  SumT sum_streams[kAvx512BatchSize] = {0};

  // Add the results by streams
  for (int64_t batch = 0; batch < num_batch; batch++) {
    for (int i = 0; i < kAvx512BatchSize; i++) {
      sum_streams[i] += values[(batch * kAvx512BatchSize) + i];
    }
  }

  // Aggregate the result streams
  for (int i = 0; i < kAvx512BatchSize; i++) {
    sum_result.sum += sum_streams[i];
  }
  sum_result.count = num_batch * kAvx512BatchSize;
  return sum_result;
}

// Dense helper for accumulator type is same to data type
#define SUM_DENSE_BATCH_AVX512_DIRECT(Type, SumSimdType, SimdZeroFn, SimdLoadFn,      \
                                      SimdAddFn)                                      \
  template <>                                                                         \
  inline SumResult<Type> SumDenseBatchAvx512(const Type* values, int64_t num_batch) { \
    SumResult<Type> sum_result;                                                       \
    SumSimdType results_simd[kAvx512BatchStreams];                                    \
    for (int i = 0; i < kAvx512BatchStreams; i++) {                                   \
      results_simd[i] = SimdZeroFn();                                                 \
    }                                                                                 \
                                                                                      \
    /* Add the values to result streams */                                            \
    for (int64_t batch = 0; batch < num_batch; batch++) {                             \
      for (int i = 0; i < kAvx512BatchStreams; i++) {                                 \
        const auto src_simd =                                                         \
            SimdLoadFn(&values[batch * kAvx512BatchSize + kAvx512StreamSize * i]);    \
        results_simd[i] = SimdAddFn(src_simd, results_simd[i]);                       \
      }                                                                               \
    }                                                                                 \
                                                                                      \
    const Type* results_scalar = reinterpret_cast<const Type*>(&results_simd);        \
    for (int stream = 0; stream < kAvx512BatchStreams; stream++) {                    \
      /* Each AVX512 stream has 8 accumulator type vaules */                          \
      for (int i = 0; i < kAvx512StreamSize; i++) {                                   \
        sum_result.sum += results_scalar[kAvx512StreamSize * stream + i];             \
      }                                                                               \
    }                                                                                 \
    sum_result.count = num_batch * kAvx512BatchSize;                                  \
    return sum_result;                                                                \
  }

// Dense version for double
SUM_DENSE_BATCH_AVX512_DIRECT(double, __m512d, _mm512_setzero_pd, _mm512_load_pd,
                              _mm512_add_pd)
// Dense version for int64_t
SUM_DENSE_BATCH_AVX512_DIRECT(int64_t, __m512i, _mm512_setzero_si512, _mm512_load_si512,
                              _mm512_add_epi64)
// Dense version for uint64_t
SUM_DENSE_BATCH_AVX512_DIRECT(uint64_t, __m512i, _mm512_setzero_si512, _mm512_load_si512,
                              _mm512_add_epi64)

// Dense helper for which need a converter from data type to accumulator type
#define SUM_DENSE_BATCH_AVX512_CVT(Type, SumType, SumSimdType, SimdZeroFn, SimdLoadFn,   \
                                   SimdCvtFn, SimdAddFn)                                 \
  template <>                                                                            \
  inline SumResult<SumType> SumDenseBatchAvx512(const Type* values, int64_t num_batch) { \
    SumResult<SumType> sum_result;                                                       \
    SumSimdType results_simd[kAvx512BatchStreams];                                       \
    for (int i = 0; i < kAvx512BatchStreams; i++) {                                      \
      results_simd[i] = SimdZeroFn();                                                    \
    }                                                                                    \
                                                                                         \
    /* Covert to the target type, then add the values to result streams */               \
    for (int64_t batch = 0; batch < num_batch; batch++) {                                \
      for (int i = 0; i < kAvx512BatchStreams; i++) {                                    \
        const auto src_simd =                                                            \
            SimdLoadFn(&values[batch * kAvx512BatchSize + kAvx512StreamSize * i]);       \
        const auto cvt_simd = SimdCvtFn(src_simd);                                       \
        results_simd[i] = SimdAddFn(cvt_simd, results_simd[i]);                          \
      }                                                                                  \
    }                                                                                    \
                                                                                         \
    const SumType* results_scalar = reinterpret_cast<const SumType*>(&results_simd);     \
    for (int stream = 0; stream < kAvx512BatchStreams; stream++) {                       \
      /* Each AVX512 stream has 8 SumType values */                                      \
      for (int i = 0; i < kAvx512StreamSize; i++) {                                      \
        sum_result.sum += results_scalar[kAvx512StreamSize * stream + i];                \
      }                                                                                  \
    }                                                                                    \
    sum_result.count = num_batch * kAvx512BatchSize;                                     \
    return sum_result;                                                                   \
  }

// Dense version for float
SUM_DENSE_BATCH_AVX512_CVT(float, double, __m512d, _mm512_setzero_pd, _mm256_load_ps,
                           _mm512_cvtps_pd, _mm512_add_pd)
// Dense version for int32_t
SUM_DENSE_BATCH_AVX512_CVT(int32_t, int64_t, __m512i, _mm512_setzero_si512, LOAD_SI256,
                           _mm512_cvtepi32_epi64, _mm512_add_epi64)
// Dense version for uint32_t
SUM_DENSE_BATCH_AVX512_CVT(uint32_t, uint64_t, __m512i, _mm512_setzero_si512, LOAD_SI256,
                           _mm512_cvtepu32_epi64, _mm512_add_epi64)
// Dense version for int16_t
SUM_DENSE_BATCH_AVX512_CVT(int16_t, int64_t, __m512i, _mm512_setzero_si512, LOAD_SI128,
                           _mm512_cvtepi16_epi64, _mm512_add_epi64)
// Dense version for uint16_t
SUM_DENSE_BATCH_AVX512_CVT(uint16_t, uint64_t, __m512i, _mm512_setzero_si512, LOAD_SI128,
                           _mm512_cvtepu16_epi64, _mm512_add_epi64)
// Dense version for int8_t
SUM_DENSE_BATCH_AVX512_CVT(int8_t, int64_t, __m512i, _mm512_setzero_si512, LOAD_SI64,
                           _mm512_cvtepi8_epi64, _mm512_add_epi64)
// Dense version for uint8_t
SUM_DENSE_BATCH_AVX512_CVT(uint8_t, uint64_t, __m512i, _mm512_setzero_si512, LOAD_SI64,
                           _mm512_cvtepu8_epi64, _mm512_add_epi64)

template <typename T, typename SumT>
// Default version for sparse batch
inline SumResult<SumT> SumSparseBatchAvx512(const uint8_t* bitmap, const T* values,
                                            int64_t num_batch) {
  SumResult<SumT> sum_result;

  for (int64_t batch = 0; batch < num_batch; batch++) {
    for (int i = 0; i < kAvx512BatchBytes; i++) {
      SumResult<SumT> result =
          SumSparseByte<T, SumT>(bitmap[kAvx512BatchBytes * batch + i],
                                 &values[batch * kAvx512BatchSize + i * 8]);
      sum_result.sum += result.sum;
      sum_result.count += result.count;
    }
  }

  return sum_result;
}

// Sparse helper for accumulator type is same to data type
#define SUM_SPARSE_BATCH_AVX512_DIRECT(Type, SumSimdType, SimdZeroFn, SimdLoadFn,        \
                                       SimdMaskAddFn)                                    \
  template <>                                                                            \
  inline SumResult<Type> SumSparseBatchAvx512(const uint8_t* bitmap, const Type* values, \
                                              int64_t num_batch) {                       \
    SumResult<Type> sum_result;                                                          \
    SumSimdType results_simd[kAvx512BatchStreams];                                       \
    for (int i = 0; i < kAvx512BatchStreams; i++) {                                      \
      results_simd[i] = SimdZeroFn();                                                    \
    }                                                                                    \
                                                                                         \
    /* Add the values to result streams */                                               \
    for (int64_t batch = 0; batch < num_batch; batch++) {                                \
      for (int i = 0; i < kAvx512BatchStreams; i++) {                                    \
        const uint8_t bits = bitmap[batch * kAvx512BatchBytes + i];                      \
        const auto src_simd =                                                            \
            SimdLoadFn(&values[batch * kAvx512BatchSize + kAvx512StreamSize * i]);       \
        results_simd[i] =                                                                \
            SimdMaskAddFn(results_simd[i], bits, src_simd, results_simd[i]);             \
        sum_result.count += BitUtil::kBytePopcount[bits];                                \
      }                                                                                  \
    }                                                                                    \
                                                                                         \
    const Type* results_scalar = reinterpret_cast<const Type*>(&results_simd);           \
    for (int stream = 0; stream < kAvx512BatchStreams; stream++) {                       \
      /* Each AVX512 stream has 8 accumulator type vaules */                             \
      for (int i = 0; i < kAvx512StreamSize; i++) {                                      \
        sum_result.sum += results_scalar[kAvx512StreamSize * stream + i];                \
      }                                                                                  \
    }                                                                                    \
    return sum_result;                                                                   \
  }

// Sparse version for double
SUM_SPARSE_BATCH_AVX512_DIRECT(double, __m512d, _mm512_setzero_pd, _mm512_load_pd,
                               _mm512_mask_add_pd)
// Sparse version for int64_t
SUM_SPARSE_BATCH_AVX512_DIRECT(int64_t, __m512i, _mm512_setzero_si512, _mm512_load_si512,
                               _mm512_mask_add_epi64)
// Sparse version for uint64_t
SUM_SPARSE_BATCH_AVX512_DIRECT(uint64_t, __m512i, _mm512_setzero_si512, _mm512_load_si512,
                               _mm512_mask_add_epi64)

// Sparse helper for which need a converter from data type to accumulator type
#define SUM_SPARSE_BATCH_AVX512_CVT(Type, SumType, SumSimdType, SimdZeroFn, SimdLoadFn, \
                                    SimdCvtFn, SimdMaskAddFn)                           \
  template <>                                                                           \
  inline SumResult<SumType> SumSparseBatchAvx512(                                       \
      const uint8_t* bitmap, const Type* values, int64_t num_batch) {                   \
    SumResult<SumType> sum_result;                                                      \
    SumSimdType results_simd[kAvx512BatchStreams];                                      \
    for (int i = 0; i < kAvx512BatchStreams; i++) {                                     \
      results_simd[i] = SimdZeroFn();                                                   \
    }                                                                                   \
                                                                                        \
    /* Covert to the target type, then add the values to result streams */              \
    for (int64_t batch = 0; batch < num_batch; batch++) {                               \
      for (int i = 0; i < kAvx512BatchStreams; i++) {                                   \
        const uint8_t bits = bitmap[batch * kAvx512BatchBytes + i];                     \
        const auto src_simd =                                                           \
            SimdLoadFn(&values[batch * kAvx512BatchSize + kAvx512StreamSize * i]);      \
        const auto cvt_simd = SimdCvtFn(src_simd);                                      \
        results_simd[i] =                                                               \
            SimdMaskAddFn(results_simd[i], bits, cvt_simd, results_simd[i]);            \
        sum_result.count += BitUtil::kBytePopcount[bits];                               \
      }                                                                                 \
    }                                                                                   \
                                                                                        \
    const SumType* results_scalar = reinterpret_cast<const SumType*>(&results_simd);    \
    for (int stream = 0; stream < kAvx512BatchStreams; stream++) {                      \
      /* Each AVX512 stream has 8 SumType values */                                     \
      for (int i = 0; i < kAvx512StreamSize; i++) {                                     \
        sum_result.sum += results_scalar[kAvx512StreamSize * stream + i];               \
      }                                                                                 \
    }                                                                                   \
    return sum_result;                                                                  \
  }

// Sparse version for float
SUM_SPARSE_BATCH_AVX512_CVT(float, double, __m512d, _mm512_setzero_pd, _mm256_load_ps,
                            _mm512_cvtps_pd, _mm512_mask_add_pd)
// Sparse version for int32_t
SUM_SPARSE_BATCH_AVX512_CVT(int32_t, int64_t, __m512i, _mm512_setzero_si512, LOAD_SI256,
                            _mm512_cvtepi32_epi64, _mm512_mask_add_epi64)
// Sparse version for uint32_t
SUM_SPARSE_BATCH_AVX512_CVT(uint32_t, uint64_t, __m512i, _mm512_setzero_si512, LOAD_SI256,
                            _mm512_cvtepu32_epi64, _mm512_mask_add_epi64)
// Sparse version for int16_t
SUM_SPARSE_BATCH_AVX512_CVT(int16_t, int64_t, __m512i, _mm512_setzero_si512, LOAD_SI128,
                            _mm512_cvtepi16_epi64, _mm512_mask_add_epi64)
// Sparse version for uint16_t
SUM_SPARSE_BATCH_AVX512_CVT(uint16_t, uint64_t, __m512i, _mm512_setzero_si512, LOAD_SI128,
                            _mm512_cvtepu16_epi64, _mm512_mask_add_epi64)
// Sparse version for int8_t
SUM_SPARSE_BATCH_AVX512_CVT(int8_t, int64_t, __m512i, _mm512_setzero_si512, LOAD_SI64,
                            _mm512_cvtepi8_epi64, _mm512_mask_add_epi64)
// Sparse version for uint8_t
SUM_SPARSE_BATCH_AVX512_CVT(uint8_t, uint64_t, __m512i, _mm512_setzero_si512, LOAD_SI64,
                            _mm512_cvtepu8_epi64, _mm512_mask_add_epi64)

template <typename ArrowType,
          typename SumType = typename FindAccumulatorType<ArrowType>::Type>
struct SumStateAvx512 {
  using ThisType = SumStateAvx512<ArrowType, SumType>;
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

    // For better performance, SIMD aligned(64-byte) load used for AVX512
    for (int64_t i = 0; i < length; ++i) {
      if (0 == (reinterpret_cast<int64_t>(&values[idx]) & 0x1FF)) {
        // Address aligned to 64-byte already
        break;
      }

      local.sum += values[idx];
      idx++;
    }

    // Parts can fill into batches
    const int64_t length_batched = BitUtil::RoundDown(length - idx, kAvx512BatchSize);
    SumResult<SumT> sum_result =
        SumDenseBatchAvx512<T, SumT>(&values[idx], length_batched / kAvx512BatchSize);
    local.sum += sum_result.sum;
    idx += sum_result.count;

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
    const auto p = arrow::internal::BitmapWordAlign<kAvx512BatchBytes>(valid_bits, offset,
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
      SumResult<SumT> sum_result = SumSparseBatchAvx512<T, SumT>(
          p.aligned_start, &values[leading_bits], aligned_words);
      local.sum += sum_result.sum;
      local.count += sum_result.count;
    }

    // The trailing bits
    const int64_t trailing_bits = p.trailing_bits;
    if (trailing_bits > 0) {
      SumResult<SumT> sum_result = SumSparseBits<T, SumT>(
          valid_bits, p.trailing_bit_offset,
          &values[leading_bits + aligned_words * kAvx512BatchSize], trailing_bits);
      local.sum += sum_result.sum;
      local.count += sum_result.count;
    }

    return local;
  }
};

template <typename ArrowType>
struct SumImplAvx512 : public ScalarAggregator {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ThisType = SumImplAvx512<ArrowType>;
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

  SumStateAvx512<ArrowType> state;
};

template <typename ArrowType>
struct MeanImplAvx512 : public SumImplAvx512<ArrowType> {
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

std::unique_ptr<KernelState> SumInitAvx512(KernelContext* ctx,
                                           const KernelInitArgs& args) {
  SumLikeInit<SumImplAvx512> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

std::unique_ptr<KernelState> MeanInitAvx512(KernelContext* ctx,
                                            const KernelInitArgs& args) {
  SumLikeInit<MeanImplAvx512> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

}  // namespace aggregate

namespace internal {
using arrow::compute::aggregate::AddBasicAggKernels;
using arrow::compute::aggregate::MeanInitAvx512;
using arrow::compute::aggregate::SumInitAvx512;

void RegisterScalarAggregateBasicAvx512(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary());
  AddBasicAggKernels(SumInitAvx512, SignedIntTypes(), int64(), func.get());
  AddBasicAggKernels(SumInitAvx512, UnsignedIntTypes(), uint64(), func.get());
  AddBasicAggKernels(SumInitAvx512, FloatingPointTypes(), float64(), func.get());
  // Register the override AVX512 version
  DCHECK_OK(registry->AddFunction(std::move(func), true));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary());
  AddBasicAggKernels(MeanInitAvx512, NumericTypes(), float64(), func.get());
  // Register the override AVX512 version
  DCHECK_OK(registry->AddFunction(std::move(func), true));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
TARGET_CODE_STOP

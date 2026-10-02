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

#include "arrow/util/alp/alp_sampler_internal.h"

#include <algorithm>
#include <span>
#include <utility>

#include "arrow/util/alp/alp_compression_internal.h"
#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"

namespace arrow::util::alp {

namespace {

struct AlpSamplingParameters {
  /// Prefix length examined in the chunk.
  int64_t values_to_examine;
  /// Stride between sampled values.
  int64_t sample_stride;
};

// The interval holds 30 4096-element chunks. Round up the jump so about eight
// chunks per interval are sampled instead of ten under integer truncation.
constexpr int64_t kSampledChunkStride = bit_util::CeilDiv(
    bit_util::CeilDiv(AlpSamplingOptions::kIntervalSize, AlpSamplingOptions::kChunkSize),
    AlpSamplingOptions::kSampleChunksPerInterval);
static_assert(kSampledChunkStride > 0, "sample jump must be positive");

// Caps the lookup window and chooses a stride that yields at most one sample per
// kSamplesPerChunk values.
AlpSamplingParameters GetSamplingParameters(int64_t chunk_size) {
  const int64_t values_to_examine =
      std::min(chunk_size, static_cast<int64_t>(AlpFormatConstants::kDefaultVectorSize));
  const int64_t sample_stride = std::max<int64_t>(
      1, bit_util::CeilDiv(values_to_examine, AlpSamplingOptions::kSamplesPerChunk));
  return AlpSamplingParameters{values_to_examine, sample_stride};
}

bool ShouldSkipChunk(const int64_t chunk_index, const int64_t sampled_chunk_count,
                     const int64_t chunk_size) {
  if ((chunk_index % kSampledChunkStride) != 0) {
    return true;
  }

  // Skip short chunks unless nothing has been sampled yet: for inputs shorter
  // than a full sample window, a short chunk is the only sample available.
  return chunk_size < AlpSamplingOptions::kSamplesPerChunk && sampled_chunk_count != 0;
}

}  // namespace

template <AlpFloatingType T>
void AlpSampler<T>::AddSample(std::span<const T> input) {
  const int64_t input_size = static_cast<int64_t>(input.size());
  for (int64_t i = 0; i < input_size; i += AlpSamplingOptions::kChunkSize) {
    const int64_t elements = std::min(input_size - i, AlpSamplingOptions::kChunkSize);
    AddSampleChunk({input.data() + i, static_cast<size_t>(elements)});
  }
}

template <AlpFloatingType T>
void AlpSampler<T>::AddSampleChunk(std::span<const T> input) {
  if (input.empty()) {
    return;
  }

  const int64_t input_size = static_cast<int64_t>(input.size());
  const bool should_skip =
      ShouldSkipChunk(chunks_processed_, chunks_sampled_, input_size);

  chunks_processed_ += 1;
  values_processed_ += input_size;
  if (should_skip) {
    return;
  }

  const AlpSamplingParameters sampling_params = GetSamplingParameters(input_size);
  const int64_t sample_count =
      bit_util::CeilDiv(sampling_params.values_to_examine, sampling_params.sample_stride);
  ARROW_CHECK_LE(sample_count, AlpSamplingOptions::kSamplesPerChunk)
      << "ALP sampler produced too many values for one chunk: " << sample_count << " > "
      << AlpSamplingOptions::kSamplesPerChunk;

  std::vector<T> chunk_sample;
  chunk_sample.reserve(static_cast<size_t>(sample_count));
  for (int64_t i = 0; i < sampling_params.values_to_examine;
       i += sampling_params.sample_stride) {
    chunk_sample.push_back(input[static_cast<size_t>(i)]);
  }
  sampled_values_count_ += static_cast<int64_t>(chunk_sample.size());

  chunk_samples_.push_back(std::move(chunk_sample));
  chunks_sampled_++;
}

template <AlpFloatingType T>
AlpEncodingPreset AlpSampler<T>::MakePreset() const {
  ARROW_LOG(DEBUG) << "AlpSampler create preset: chunksSampled=" << chunks_sampled_ << "/"
                   << chunks_processed_ << " total"
                   << ", valuesSampled=" << sampled_values_count_ << "/"
                   << values_processed_ << " total";

  AlpEncodingPreset preset = AlpCompression<T>::MakePreset(chunk_samples_);

  ARROW_LOG(DEBUG) << "AlpSampler preset: " << preset.combinations.size()
                   << " exponent/factor combinations"
                   << ", estimatedSize=" << preset.estimated_compressed_size_bytes
                   << " bytes";

  return preset;
}

template class AlpSampler<float>;
template class AlpSampler<double>;

}  // namespace arrow::util::alp

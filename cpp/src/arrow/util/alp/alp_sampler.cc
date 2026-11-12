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
#include <cstdint>
#include <span>
#include <utility>

#include "arrow/util/alp/alp_compression_internal.h"
#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"

namespace arrow::util::alp {

namespace {

// splitmix64, a small fixed-seed generator: the same input always yields the same
// sample.
class SampleRng {
 public:
  explicit SampleRng(uint64_t seed) : state_(0x9E3779B97F4A7C15ULL ^ seed) {}

  uint64_t Next() {
    uint64_t z = (state_ += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
  }

 private:
  uint64_t state_;
};

}  // namespace

std::vector<int64_t> GetSampledVectorOffsets(int64_t num_values) {
  constexpr int64_t kVectorSize = AlpFormatConstants::kDefaultVectorSize;
  constexpr int64_t kWindowSize = AlpSamplingOptions::kSampleWindowSize;
  static_assert(kWindowSize % kVectorSize == 0, "windows must hold whole vectors");

  std::vector<int64_t> offsets;
  for (int64_t window = 0; window < num_values; window += kWindowSize) {
    const int64_t num_vectors =
        bit_util::CeilDiv(std::min(kWindowSize, num_values - window), kVectorSize);
    const int64_t num_sampled =
        std::min(num_vectors, AlpSamplingOptions::kSampledVectors);
    for (int64_t i = 0; i < num_sampled; ++i) {
      offsets.push_back(window + i * num_vectors / num_sampled * kVectorSize);
    }
  }
  return offsets;
}

template <AlpFloatingType T>
void AlpSampler<T>::AddSample(std::span<const T> input) {
  const int64_t input_size = static_cast<int64_t>(input.size());
  values_processed_ += input_size;
  for (const int64_t offset : GetSampledVectorOffsets(input_size)) {
    const int64_t length =
        std::min<int64_t>(AlpFormatConstants::kDefaultVectorSize, input_size - offset);
    AddVectorSample(
        input.subspan(static_cast<size_t>(offset), static_cast<size_t>(length)), offset);
  }
}

template <AlpFloatingType T>
void AlpSampler<T>::AddVectorSample(std::span<const T> vector, int64_t offset) {
  const size_t stride = static_cast<size_t>(bit_util::CeilDiv(
      static_cast<int64_t>(vector.size()), AlpSamplingOptions::kSamplesPerVector));

  // One value from each run of `stride` values, at a pseudo-random position within
  // the run, so the sample covers the whole vector without lining up with data
  // that repeats with a period dividing the stride.
  SampleRng rng(static_cast<uint64_t>(offset) * 0xD1B54A32D192ED03ULL);
  std::vector<T> sample;
  sample.reserve(static_cast<size_t>(AlpSamplingOptions::kSamplesPerVector));
  for (size_t i = 0; i < vector.size(); i += stride) {
    const size_t run = std::min(stride, vector.size() - i);
    sample.push_back(vector[i + static_cast<size_t>(rng.Next() % run)]);
  }
  sampled_values_count_ += static_cast<int64_t>(sample.size());
  vector_samples_.push_back(std::move(sample));
}

template <AlpFloatingType T>
AlpEncodingPreset AlpSampler<T>::MakePreset() const {
  ARROW_LOG(DEBUG) << "AlpSampler create preset: vectorsSampled="
                   << vector_samples_.size()
                   << ", valuesSampled=" << sampled_values_count_ << "/"
                   << values_processed_ << " total";

  AlpEncodingPreset preset = AlpCompression<T>::MakePreset(vector_samples_);

  ARROW_LOG(DEBUG) << "AlpSampler preset: " << preset.combinations.size()
                   << " exponent/factor combinations"
                   << ", estimatedSize=" << preset.estimated_compressed_size_bytes
                   << " bytes";

  return preset;
}

template class AlpSampler<float>;
template class AlpSampler<double>;

}  // namespace arrow::util::alp

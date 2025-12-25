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

#include "arrow/util/alp/alp_sampler.h"

#include <cmath>

#include "arrow/util/alp/alp.h"
#include "arrow/util/alp/alp_constants.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// AlpSampler implementation

template <typename T>
AlpSampler<T>::AlpSampler()
    : sample_vector_size_(AlpConstants::kSamplerVectorSize),
      rowgroup_size_(AlpConstants::kSamplerRowgroupSize),
      samples_per_vector_(AlpConstants::kSamplerSamplesPerVector),
      sample_vectors_per_rowgroup_(AlpConstants::kSamplerSampleVectorsPerRowgroup),
      rowgroup_sample_jump_((rowgroup_size_ / sample_vectors_per_rowgroup_) /
                            sample_vector_size_) {}

template <typename T>
void AlpSampler<T>::AddSample(arrow::util::span<const T> input) {
  for (uint64_t i = 0; i < input.size(); i += sample_vector_size_) {
    const uint64_t elements = std::min(input.size() - i, sample_vector_size_);
    AddSampleVector({input.data() + i, elements});
  }
}

template <typename T>
void AlpSampler<T>::AddSampleVector(arrow::util::span<const T> input) {
  const bool must_skip_current_vector =
      MustSkipSamplingFromCurrentVector(vectors_count_, vectors_sampled_count_,
                                        input.size());

  vectors_count_ += 1;
  total_values_count_ += input.size();
  if (must_skip_current_vector) {
    return;
  }

  const AlpSamplingParameters sampling_params = GetAlpSamplingParameters(input.size());

  // Slice: take first num_lookup_value elements.
  std::vector<T> current_vector_values(
      input.begin(),
      input.begin() + std::min<size_t>(sampling_params.num_lookup_value, input.size()));

  // Stride: take every num_sampled_increments-th element.
  std::vector<T> current_vector_sample;
  for (size_t i = 0; i < current_vector_values.size();
       i += sampling_params.num_sampled_increments) {
    current_vector_sample.push_back(current_vector_values[i]);
  }
  sample_stored_ += current_vector_sample.size();

  complete_vectors_sampled_.push_back(std::move(current_vector_values));
  rowgroup_sample_.push_back(std::move(current_vector_sample));
  vectors_sampled_count_++;
}

template <typename T>
typename AlpSampler<T>::AlpSamplerResult AlpSampler<T>::Finalize() {
  ARROW_LOG(DEBUG) << "AlpSampler finalized: vectorsSampled=" << vectors_sampled_count_
                   << "/" << vectors_count_ << " total"
                   << ", valuesSampled=" << sample_stored_ << "/" << total_values_count_
                   << " total";

  AlpSamplerResult result;
  result.alp_preset = AlpCompression<T>::CreateEncodingPreset(rowgroup_sample_);

  ARROW_LOG(DEBUG) << "AlpSampler preset: " << result.alp_preset.combinations.size()
                   << " exponent/factor combinations"
                   << ", estimatedSize=" << result.alp_preset.best_compressed_size
                   << " bytes";

  return result;
}

template <typename T>
typename AlpSampler<T>::AlpSamplingParameters AlpSampler<T>::GetAlpSamplingParameters(
    uint64_t num_current_vector_values) {
  const uint64_t num_lookup_values =
      std::min(num_current_vector_values,
               static_cast<uint64_t>(AlpConstants::kAlpVectorSize));
  // Sample equidistant values within a vector; jump a fixed number of values.
  const uint64_t num_sampled_increments =
      std::max(uint64_t{1}, static_cast<uint64_t>(std::ceil(
                                static_cast<double>(num_lookup_values) /
                                samples_per_vector_)));
  const uint64_t num_sampled_values =
      std::ceil(static_cast<double>(num_lookup_values) / num_sampled_increments);

  ARROW_CHECK(num_sampled_values < AlpConstants::kAlpVectorSize) << "alp_sample_too_large";

  return AlpSamplingParameters{num_lookup_values, num_sampled_increments,
                               num_sampled_values};
}

template <typename T>
bool AlpSampler<T>::MustSkipSamplingFromCurrentVector(
    const uint64_t vectors_count, const uint64_t vectors_sampled_count,
    const uint64_t current_vector_n_values) {
  // Sample equidistant vectors; skip a fixed number of vectors.
  const bool must_select_rowgroup_samples = (vectors_count % rowgroup_sample_jump_) == 0;

  // If we are not in the correct jump, do not take sample from this vector.
  if (!must_select_rowgroup_samples) {
    return true;
  }

  // Do not take samples of non-complete vectors (usually the last one),
  // except in the case of too little data.
  if (current_vector_n_values < AlpConstants::kSamplerSamplesPerVector &&
      vectors_sampled_count != 0) {
    return true;
  }
  return false;
}

// ----------------------------------------------------------------------
// Template instantiations

template class AlpSampler<float>;
template class AlpSampler<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow

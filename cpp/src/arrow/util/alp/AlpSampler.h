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

// ALP sampler for collecting samples and creating encoding presets

#pragma once

#include <optional>
#include <vector>

#include "arrow/util/alp/Alp.h"
#include "arrow/util/span.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// AlpSampler

/// \class AlpSampler
/// \brief Collects samples from data to be compressed with ALP
///
/// Usage: Call AddSample() or AddSampleVector() multiple times to collect
/// samples, then call Finalize() to retrieve the resulting preset.
///
/// \tparam T the floating point type (float or double) to sample
template <typename T>
class AlpSampler {
 public:
  /// \brief Default constructor
  AlpSampler();

  /// \brief Helper struct containing the preset for ALP compression
  struct AlpSamplerResult {
    AlpEncodingPreset alp_preset;
  };

  /// \brief Add a sample of arbitrary size
  ///
  /// The sample is internally separated into vectors on which AddSampleVector()
  /// is called.
  ///
  /// \param[in] input the input data to sample from
  void AddSample(arrow::util::span<const T> input);

  /// \brief Add a single vector as a sample
  ///
  /// \param[in] input the input vector to add.
  ///            Size should be <= AlpConstants::kAlpVectorSize.
  void AddSampleVector(arrow::util::span<const T> input);

  /// \brief Finalize sampling and generate the encoding preset
  ///
  /// \return an AlpSamplerResult containing the generated encoding preset
  AlpSamplerResult Finalize();

 private:
  /// \brief Helper struct to encapsulate settings used for sampling
  struct AlpSamplingParameters {
    uint64_t num_lookup_value;
    uint64_t num_sampled_increments;
    uint64_t num_sampled_values;
  };

  /// \brief Calculate sampling parameters for the current vector
  ///
  /// \param[in] num_current_vector_values number of values in current vector
  /// \return the sampling parameters to use
  AlpSamplingParameters GetAlpSamplingParameters(uint64_t num_current_vector_values);

  /// \brief Check if the current vector must be ignored for sampling
  ///
  /// \param[in] vectors_count the total number of vectors processed so far
  /// \param[in] vectors_sampled_count the number of vectors sampled so far
  /// \param[in] num_current_vector_values number of values in current vector
  /// \return true if the current vector should be skipped, false otherwise
  bool MustSkipSamplingFromCurrentVector(uint64_t vectors_count,
                                         uint64_t vectors_sampled_count,
                                         uint64_t num_current_vector_values);

  /// Count of vectors that have been sampled
  uint64_t vectors_sampled_count_ = 0;
  /// Total count of values processed
  uint64_t total_values_count_ = 0;
  /// Total count of vectors processed
  uint64_t vectors_count_ = 0;
  /// Number of samples stored
  uint64_t sample_stored_ = 0;
  /// Samples collected from current rowgroup
  std::vector<std::vector<T>> rowgroup_sample_;

  /// Complete vectors sampled
  std::vector<std::vector<T>> complete_vectors_sampled_;
  /// Size of each sample vector
  const uint64_t sample_vector_size_;
  /// Size of each rowgroup
  const uint64_t rowgroup_size_;
  /// Number of samples to take per vector
  const uint64_t samples_per_vector_;
  /// Number of vectors to sample per rowgroup
  const uint64_t sample_vectors_per_rowgroup_;
  /// Jump interval for rowgroup sampling
  const uint64_t rowgroup_sample_jump_;
};

}  // namespace alp
}  // namespace util
}  // namespace arrow

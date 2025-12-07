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
/// Usage: Call addSample() or addSampleVector() multiple times to collect samples,
/// then call finalize() to retrieve the resulting preset.
///
/// \tparam T the floating point type (float or double) to sample
template <typename T>
class AlpSampler {
 public:
  /// \brief Default constructor
  AlpSampler();

  /// \brief Helper struct containing the preset for ALP compression
  struct AlpSamplerResult {
    AlpEncodingPreset alpPreset;
  };

  /// \brief Add a sample of arbitrary size
  ///
  /// The sample is internally separated into vectors on which addSampleVector() is called.
  ///
  /// \param[in] input the input data to sample from
  void addSample(arrow::util::span<const T> input);

  /// \brief Add a single vector as a sample
  ///
  /// \param[in] input the input vector to add. Size should be <= AlpConstants::kAlpVectorSize.
  void addSampleVector(arrow::util::span<const T> input);

  /// \brief Finalize sampling and generate the encoding preset
  ///
  /// \return an AlpSamplerResult containing the generated encoding preset
  AlpSamplerResult finalize();

 private:
  /// \brief Helper struct to encapsulate settings used for sampling
  struct AlpSamplingParameters {
    uint64_t numLookupValue;
    uint64_t numSampledIncrements;
    uint64_t numSampledValues;
  };

  /// \brief Calculate sampling parameters for the current vector
  ///
  /// \param[in] numCurrentVectorValues the number of values in the current vector
  /// \return the sampling parameters to use
  AlpSamplingParameters getAlpSamplingParameters(uint64_t numCurrentVectorValues);

  /// \brief Check if the current vector must be ignored for sampling
  ///
  /// \param[in] vectorsCount the total number of vectors processed so far
  /// \param[in] vectorsSampledCount the number of vectors that have been sampled so far
  /// \param[in] numCurrentVectorValues the number of values in the current vector
  /// \return true if the current vector should be skipped, false otherwise
  bool mustSkipSamplingFromCurrentVector(uint64_t vectorsCount, uint64_t vectorsSampledCount,
                                         uint64_t numCurrentVectorValues);

  /// Count of vectors that have been sampled
  uint64_t m_vectorsSampledCount = 0;
  /// Total count of values processed
  uint64_t m_totalValuesCount = 0;
  /// Total count of vectors processed
  uint64_t m_vectorsCount = 0;
  /// Number of samples stored
  uint64_t m_sampleStored = 0;
  /// Samples collected from current rowgroup
  std::vector<std::vector<T>> m_rowgroupSample;

  /// Complete vectors sampled
  std::vector<std::vector<T>> m_completeVectorsSampled;
  /// Size of each sample vector
  const uint64_t m_sampleVectorSize;
  /// Size of each rowgroup
  const uint64_t m_rowgroupSize;
  /// Number of samples to take per vector
  const uint64_t m_samplesPerVector;
  /// Number of vectors to sample per rowgroup
  const uint64_t m_sampleVectorsPerRowgroup;
  /// Jump interval for rowgroup sampling
  const uint64_t m_rowgroupSampleJump;
};

}  // namespace alp
}  // namespace util
}  // namespace arrow

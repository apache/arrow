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

#include <cstdint>
#include <span>
#include <vector>

#include "arrow/util/alp/alp_compression_internal.h"
#include "arrow/util/visibility.h"

namespace arrow::util::alp {

/// Returns the offsets of the vectors AlpSampler samples from an input of
/// `num_values` values. Each window of AlpSamplingOptions::kSampleWindowSize
/// values, or the whole input if shorter, contributes up to
/// AlpSamplingOptions::kSampledVectors vectors of
/// AlpFormatConstants::kDefaultVectorSize values spaced evenly across it.
ARROW_EXPORT std::vector<int64_t> GetSampledVectorOffsets(int64_t num_values);

/// Collects samples and builds an ALP encoding preset.
///
/// Each AddSample() call samples the vectors given by GetSampledVectorOffsets().
/// Call AddSample() one or more times, then MakePreset().
template <AlpFloatingType T>
class ARROW_EXPORT AlpSampler {
 public:
  AlpSampler() = default;

  /// Adds input values to the sample set.
  void AddSample(std::span<const T> input);

  /// Builds a preset from the collected samples.
  ///
  /// Does not consume or clear the samples, so it may be called repeatedly.
  AlpEncodingPreset MakePreset() const;

 private:
  /// Adds a sample of at most AlpSamplingOptions::kSamplesPerVector values of the
  /// vector at `offset` in the input.
  void AddVectorSample(std::span<const T> vector, int64_t offset);

  /// Number of values passed to AddSample().
  int64_t values_processed_{0};
  /// Number of values retained in vector_samples_.
  int64_t sampled_values_count_{0};
  /// The sample of each sampled vector. MakePreset() only reads these.
  std::vector<std::vector<T>> vector_samples_;
};

}  // namespace arrow::util::alp

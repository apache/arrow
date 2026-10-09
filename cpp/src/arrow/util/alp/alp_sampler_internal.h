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

#include <span>
#include <vector>

#include "arrow/util/alp/alp_compression_internal.h"
#include "arrow/util/visibility.h"

namespace arrow::util::alp {

/// Collects samples and builds an ALP encoding preset.
///
/// Input is split into fixed-size chunks. Each sampled chunk contributes one
/// strided sample. Call AddSample() one or more times, then MakePreset().
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
  /// Adds one sampling chunk. Only the first
  /// AlpFormatConstants::kDefaultVectorSize elements are examined.
  void AddSampleChunk(std::span<const T> input);

  /// Number of chunks that have contributed a sample.
  int64_t chunks_sampled_{0};
  /// Number of chunks processed.
  int64_t chunks_processed_{0};
  /// Number of values processed.
  int64_t values_processed_{0};
  /// Number of values retained in chunk_samples_.
  int64_t sampled_values_count_{0};
  /// One strided sample per sampled chunk. MakePreset() only reads these.
  std::vector<std::vector<T>> chunk_samples_;
};

}  // namespace arrow::util::alp

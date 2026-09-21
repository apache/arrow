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

// ALP compression and decompression primitives.

#pragma once

#include <concepts>
#include <cstdint>
#include <span>
#include <vector>

#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/alp/alp_metadata_internal.h"
#include "arrow/util/visibility.h"

namespace arrow::util::alp {

template <typename T, typename TargetType>
concept AlpDecodeTarget = std::same_as<T, TargetType> ||
                          (std::same_as<T, float> && std::same_as<TargetType, double>);

/// Preset used while choosing per-vector ALP parameters.
struct AlpEncodingPreset {
  /// Candidate exponent/factor pairs. Consumers must reject an empty vector.
  std::vector<AlpExponentAndFactor> combinations;
  /// Minimum estimated compressed size across sampled vectors, in bytes.
  int64_t estimated_compressed_size_bytes{0};
  /// Integer encoding applied after decimal encoding.
  AlpIntegerEncoding integer_encoding{AlpIntegerEncoding::kForBitPack};

  /// Creates the identity preset for inputs without samples.
  static AlpEncodingPreset MakeDefault() {
    return {std::vector<AlpExponentAndFactor>{AlpExponentAndFactor{0, 0}}, 0};
  }
};

/// ALP compression and decompression primitives for float and double.
template <AlpFloatingType T>
class ARROW_EXPORT AlpCompression {
 public:
  AlpCompression() = delete;

  using Constants = AlpTypedConstants<T>;
  using EncodedUnsigned = typename Constants::EncodedUnsigned;
  using EncodedSigned = typename Constants::EncodedSigned;
  static constexpr uint8_t kEncodedBitSize = sizeof(EncodedUnsigned) * 8;

  /// Compresses one vector using the supplied preset.
  static AlpEncodedVector<T> Compress(std::span<const T> inputs,
                                      const AlpEncodingPreset& preset);

  /// Creates an encoding preset from the collected samples.
  ///
  /// Each element is one strided sample from a sampling chunk.
  static AlpEncodingPreset MakePreset(const std::vector<std::vector<T>>& samples);

  /// Decompresses a view into caller-provided outputs and reusable scratch space.
  ///
  /// `outputs` and `integer_scratch` must each hold at least
  /// `encoded_view.num_elements()` elements.
  template <typename TargetType>
    requires AlpDecodeTarget<T, TargetType>
  static void Decompress(const AlpEncodedVectorView<T>& encoded_view,
                         std::span<TargetType> outputs,
                         std::span<EncodedUnsigned> integer_scratch);

 private:
  static std::vector<T> CreateSample(std::span<const T> inputs);

  static AlpExponentAndFactor FindBestExponentAndFactor(
      std::span<const T> inputs, const std::vector<AlpExponentAndFactor>& combinations);

  struct EncodingResult {
    std::vector<EncodedUnsigned> for_deltas;
    std::vector<AlpFormatConstants::PositionType> exception_positions;
    std::vector<T> exceptions;
    EncodedUnsigned min_max_diff{0};
    EncodedUnsigned frame_of_reference{0};
  };

  static EncodingResult EncodeVector(std::span<const T> inputs,
                                     AlpExponentAndFactor exponent_and_factor);

  struct BitPackingResult {
    std::vector<uint8_t> packed_integers;
    uint8_t bit_width{0};
  };

  static BitPackingResult BitPackIntegers(std::span<const EncodedUnsigned> for_deltas,
                                          EncodedUnsigned min_max_diff);

  static void BitUnpackIntegers(std::span<const uint8_t> packed_integers,
                                const AlpForInfo<T>& for_info,
                                std::span<EncodedUnsigned> outputs);

  /// Overwrites outputs at exception_positions with the original exception values.
  template <typename TargetType>
    requires AlpDecodeTarget<T, TargetType>
  static void PatchExceptions(
      std::span<const T> exceptions,
      std::span<const AlpFormatConstants::PositionType> exception_positions,
      std::span<TargetType> outputs);
};

// The marker on a class template does not reach its member templates: each is
// instantiated on its own, so its instantiations need their own marker to land in
// the shared library's export table. The definitions are in the implementation
// file. Wider output than T is allowed, narrower is not, which is why float ->
// float, float -> double and double -> double are the only three.
extern template ARROW_TEMPLATE_EXPORT void AlpCompression<float>::Decompress(
    const AlpEncodedVectorView<float>&, std::span<float>,
    std::span<AlpCompression<float>::EncodedUnsigned>);
extern template ARROW_TEMPLATE_EXPORT void AlpCompression<float>::Decompress(
    const AlpEncodedVectorView<float>&, std::span<double>,
    std::span<AlpCompression<float>::EncodedUnsigned>);
extern template ARROW_TEMPLATE_EXPORT void AlpCompression<double>::Decompress(
    const AlpEncodedVectorView<double>&, std::span<double>,
    std::span<AlpCompression<double>::EncodedUnsigned>);

}  // namespace arrow::util::alp

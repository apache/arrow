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

#pragma once

#include <cstdint>
#include <span>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/alp/alp_compression_internal.h"
#include "arrow/util/alp/alp_metadata_internal.h"
#include "arrow/util/visibility.h"

namespace arrow::util::alp {

/// ALP compression and decompression codec.
template <AlpFloatingType T>
class ARROW_EXPORT AlpCodec {
 public:
  AlpCodec() = delete;

  /// Creates an encoding preset by sampling `inputs`.
  static Result<AlpEncodingPreset> MakePreset(std::span<const T> inputs);

  /// Encodes `inputs` using a pre-computed preset.
  static Result<int64_t> Encode(std::span<const T> inputs,
                                const AlpEncodingPreset& preset, int32_t vector_size,
                                std::span<uint8_t> output);

  /// Samples a preset and encodes `inputs`.
  static Result<int64_t> Encode(std::span<const T> inputs, int32_t vector_size,
                                std::span<uint8_t> output);

  /// Returns the maximum encoded size in bytes.
  static Result<int64_t> GetMaxCompressedSize(int64_t num_elements, int32_t vector_size);
};

/// Random access to the vectors of one compressed ALP page.
template <AlpFloatingType T>
class ARROW_EXPORT AlpVectorReader {
 public:
  /// Opens a page and validates its header and offset table.
  ///
  /// The buffer referenced by `input` must outlive the reader.
  static Result<AlpVectorReader> Open(std::span<const uint8_t> input, MemoryPool* pool);

  /// Number of values the header declares.
  int32_t num_elements() const { return num_elements_; }

  /// Number of values in every vector but the last.
  int32_t vector_size() const { return vector_size_; }

  /// Number of vectors the buffer holds.
  int32_t num_vectors() const { return static_cast<int32_t>(vector_offsets_.size()); }

  /// Returns the number of values in `vector_index`.
  Result<int32_t> VectorLength(int32_t vector_index) const;

  /// Decodes one vector.
  ///
  /// `output` must contain exactly `VectorLength(vector_index)` elements.
  /// `TargetType` may be wider than `T`, but not narrower.
  template <typename TargetType>
    requires AlpDecodeTarget<T, TargetType>
  Status Decode(int32_t vector_index, std::span<TargetType> output);

 private:
  explicit AlpVectorReader(MemoryPool* pool)
      : vector_offsets_(AlpAllocator<AlpFormatConstants::OffsetType>(pool)),
        decode_view_(pool),
        unpacked_integers_(
            AlpAllocator<typename AlpTypedConstants<T>::EncodedUnsigned>(pool)) {}

  /// Metadata and data bounds for one vector.
  struct VectorLayout {
    AlpInfo alp_info;
    AlpForInfo<T> for_info;
    /// First byte of the bit-packed values, after both metadata blocks.
    const uint8_t* data{nullptr};
    int64_t data_size{0};
    int32_t num_elements{0};
  };

  /// Reads and bounds-checks one vector's metadata.
  Result<VectorLayout> LoadVectorLayout(int32_t vector_index) const;

  /// Returns the total encoded size of one vector.
  Result<int64_t> VectorSizeInBytes(int32_t vector_index) const;

  /// Start of the offset table; vector offsets are relative to it.
  const uint8_t* body_{nullptr};
  int64_t body_size_{0};
  int32_t num_elements_{0};
  int32_t vector_size_{0};
  /// Byte offset of each vector.
  AlpVector<AlpFormatConstants::OffsetType> vector_offsets_;
  /// Reused exception arrays for vector decoding.
  AlpEncodedVectorView<T> decode_view_;
  /// Reused scratch space for unpacked integers.
  AlpVector<typename AlpTypedConstants<T>::EncodedUnsigned> unpacked_integers_;
};

// Member templates need explicit export instantiations.
extern template ARROW_TEMPLATE_EXPORT Status
AlpVectorReader<float>::Decode(int32_t, std::span<float>);
extern template ARROW_TEMPLATE_EXPORT Status
AlpVectorReader<float>::Decode(int32_t, std::span<double>);
extern template ARROW_TEMPLATE_EXPORT Status
AlpVectorReader<double>::Decode(int32_t, std::span<double>);

}  // namespace arrow::util::alp

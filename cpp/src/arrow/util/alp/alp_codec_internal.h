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

// High-level codec interface for ALP compression

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/alp/alp_internal.h"
#include "arrow/util/alp/alp_sampler_internal.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// AlpCodec

/// \class AlpCodec
/// \brief High-level interface for ALP compression
///
/// AlpCodec is an interface for Adaptive Lossless floating-Point Compression
/// (ALP) (https://dl.acm.org/doi/10.1145/3626717). For encoding, it samples
/// the data and applies decimal compression (Alp) to floating point values.
/// This class acts as a wrapper around the vector-based interfaces of
/// AlpSampler and Alp.
///
/// \tparam T the floating point type (float or double)
template <typename T>
class AlpCodec {
 public:
  /// Type alias for the sampler result containing encoding presets
  using AlpSamplerResult = typename AlpSampler<T>::AlpSamplerResult;

  /// \brief Create a sampling preset from input data
  ///
  /// This samples the input data and generates an encoding preset that can be
  /// reused for encoding. Pre-computing the preset lets you keep sampling out
  /// of a benchmark loop, or encode multiple batches with one preset.
  ///
  /// \param[in] input pointer to the input data to sample
  /// \param[in] num_elements number of elements to sample
  /// \return the sampling result containing the encoding preset, or
  ///         Status::Invalid if `num_elements < 0`.
  static Result<AlpSamplerResult> CreateSamplingPreset(const T* input,
                                                       int64_t num_elements);

  /// \brief Encode floating point values using a pre-computed preset
  ///
  /// This encodes the data using a preset that was previously computed via
  /// CreateSamplingPreset(). This avoids the sampling overhead during encoding.
  ///
  /// \param[in] input pointer to the input to encode
  /// \param[in] num_elements number of elements to encode
  /// \param[in] preset the pre-computed sampling result from CreateSamplingPreset()
  /// \param[in] vector_size number of elements per vector (must be a power of 2
  ///            in the inclusive range [2^kMinLogVectorSize, 2^kMaxLogVectorSize],
  ///            i.e. 8 to 32768)
  /// \param[out] output pointer to the memory region we will encode into.
  ///             Must be at least GetMaxCompressedSize(num_elements, vector_size) bytes.
  ///             Behavior is undefined if `output` is smaller.
  /// \param[in,out] output_size on input, the size of `output` in bytes; on output,
  ///                the actual size of the encoded data. Must satisfy
  ///                `*output_size >= GetMaxCompressedSize(num_elements, vector_size)`;
  ///                undersizing leads to undefined behavior (a partial write to
  ///                `output` and an out-of-bounds write of the header).
  /// \return Status::OK on success, or Status::Invalid if any precondition is
  ///         violated: `num_elements >= 0`, `num_elements <= INT32_MAX`, and
  ///         `vector_size` a power of two in
  ///         `[2^kMinLogVectorSize, 2^kMaxLogVectorSize]`.
  static Status EncodeWithPreset(const T* input, int64_t num_elements,
                                 const AlpSamplerResult& preset, int32_t vector_size,
                                 uint8_t* output, int64_t* output_size);

  /// \brief Encode floating point values using ALP decimal compression
  ///
  /// \param[in] input pointer to the input to encode
  /// \param[in] num_elements number of elements to encode
  /// \param[in] vector_size number of elements per vector (must be a power of 2
  ///            in the inclusive range [2^kMinLogVectorSize, 2^kMaxLogVectorSize],
  ///            i.e. 8 to 32768)
  /// \param[out] output pointer to the memory region we will encode into.
  ///             Must be at least GetMaxCompressedSize(num_elements, vector_size) bytes.
  ///             Behavior is undefined if `output` is smaller.
  /// \param[in,out] output_size on input, the size of `output` in bytes; on output,
  ///                the actual size of the encoded data. Must satisfy
  ///                `*output_size >= GetMaxCompressedSize(num_elements, vector_size)`;
  ///                undersizing leads to undefined behavior (a partial write to
  ///                `output` and an out-of-bounds write of the header).
  /// \return Status::OK on success, or Status::Invalid if any precondition is
  ///         violated: `num_elements >= 0`, `num_elements <= INT32_MAX`, and
  ///         `vector_size` a power of two in
  ///         `[2^kMinLogVectorSize, 2^kMaxLogVectorSize]`.
  static Status Encode(const T* input, int64_t num_elements, int32_t vector_size,
                       uint8_t* output, int64_t* output_size);

  /// \brief Convenience overload with default vector_size = kAlpVectorSize.
  ///        Same preconditions and error returns as the four-argument overload.
  static Status Encode(const T* input, int64_t num_elements, uint8_t* output,
                       int64_t* output_size);

  /// \brief Decode floating point values
  ///
  /// \param[in] num_elements number of elements the caller expects, which is
  ///            also the capacity of `output` in elements. The ALP header
  ///            embedded in `input` carries its own count, and decoding fails
  ///            unless the two are equal: a smaller header count would leave
  ///            part of `output` unwritten, and a larger one would overrun it.
  /// \param[in] input pointer to the compressed data
  /// \param[in] input_size size of the compressed data in bytes
  /// \param[out] output pointer to the memory region we will decode into.
  ///             The caller is responsible for ensuring this is big enough
  ///             to hold num_elements values.
  /// \return Status::OK on success, or an error if the compressed data is malformed
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static Status Decode(int32_t num_elements, const uint8_t* input, int64_t input_size,
                       TargetType* output);

  /// \brief Read the number of values the page's ALP header declares
  ///
  /// A Parquet page header counts nulls, while an ALP payload holds only the
  /// non-null values, so a decoder needs this to learn how many values the page
  /// actually carries before decoding it.
  ///
  /// \param[in] input pointer to the compressed data, starting at the header
  /// \param[in] input_size size of the compressed data in bytes
  /// \return the declared element count, or an error if the header is malformed
  static Result<int32_t> DecodeElementCount(const uint8_t* input, int64_t input_size);

  /// \brief Get the maximum compressed size for a given number of elements
  ///
  /// \param[in] num_elements number of elements to compress
  /// \param[in] vector_size number of elements per vector (must be a power of 2
  ///            in the inclusive range [2^kMinLogVectorSize, 2^kMaxLogVectorSize],
  ///            i.e. 8 to 32768)
  /// \return the maximum size of the compressed buffer in bytes, or
  ///         Status::Invalid if `num_elements < 0` or `vector_size` is not a
  ///         power of two in `[2^kMinLogVectorSize, 2^kMaxLogVectorSize]`.
  static Result<int64_t> GetMaxCompressedSize(int64_t num_elements, int32_t vector_size);

  /// \brief Random access to the vectors of one compressed buffer
  ///
  /// `Open` reads the header and validates the whole offset chain once. A caller
  /// can then decode any single vector on its own, so serving a batch of values
  /// costs work in proportion to the batch rather than to the buffer, and needs
  /// scratch for one vector rather than for the whole buffer.
  class VectorReader {
   public:
    /// \brief Read the header and validate the offset chain
    ///
    /// \param[in] input pointer to the compressed data, starting at the header
    /// \param[in] input_size size of the compressed data in bytes
    /// \return a reader over `input`, which it does not own and which must
    ///         outlive it, or an error if the buffer is malformed
    static Result<VectorReader> Open(const uint8_t* input, int64_t input_size);

    /// Number of values the header declares.
    int32_t num_elements() const { return num_elements_; }

    /// Number of values in every vector but the last.
    int32_t vector_size() const { return vector_size_; }

    /// Number of vectors the buffer holds.
    int32_t num_vectors() const { return static_cast<int32_t>(vector_offsets_.size()); }

    /// \brief Number of values in one vector
    ///
    /// Every vector but the last holds `vector_size()` values.
    ///
    /// \param[in] vector_index index of the vector, in `[0, num_vectors())`
    int32_t VectorLength(int32_t vector_index) const;

    /// \brief Decode one vector
    ///
    /// \param[in] vector_index index of the vector, in `[0, num_vectors())`
    /// \param[out] output room for `VectorLength(vector_index)` values
    /// \return Status::OK on success, or an error if the vector is malformed
    /// \tparam TargetType the type that is used to store the output.
    ///         May not be a narrowing conversion from T.
    template <typename TargetType>
    Status DecodeVector(int32_t vector_index, TargetType* output) const;

   private:
    /// \brief Where one vector's metadata and data sit in the buffer
    struct VectorLayout {
      AlpEncodedVectorInfo alp_info;
      AlpEncodedForVectorInfo<T> for_info;
      /// First byte of the bit-packed values, after both metadata blocks.
      const uint8_t* data = nullptr;
      int64_t data_size = 0;
      int32_t num_elements = 0;
    };

    /// \brief Read one vector's metadata and bound-check its data
    ///
    /// \param[in] vector_index index of the vector, already known to be in range
    Result<VectorLayout> LoadVectorLayout(int32_t vector_index) const;

    /// \brief Total bytes one vector occupies, metadata included
    ///
    /// \param[in] vector_index index of the vector, already known to be in range
    Result<int64_t> VectorSizeInBytes(int32_t vector_index) const;

    /// First byte after the header, which every offset is relative to.
    const uint8_t* body_ = nullptr;
    int64_t body_size_ = 0;
    int32_t num_elements_ = 0;
    int32_t vector_size_ = 0;
    AlpIntegerEncoding integer_encoding_ = AlpIntegerEncoding::kForBitPack;
    /// Byte offset of each vector, checked against the chain rule by `Open`.
    std::vector<AlpConstants::OffsetType> vector_offsets_;
  };

 private:
  struct AlpHeader;

  /// \brief Tracks the progress of a compression operation
  ///
  /// Used to report how much data was consumed and produced during encoding.
  struct CompressionProgress {
    /// Number of compressed bytes written to output
    int64_t num_compressed_bytes_produced = 0;
    /// Number of input elements consumed
    int64_t num_uncompressed_elements_taken = 0;
  };

  /// \brief Compress a buffer using ALP
  ///
  /// \param[in] input array of floating point numbers to compress
  /// \param[in] element_count the number of floating point numbers
  /// \param[in] preset the encoding preset to use (contains the candidate
  ///            exponent/factor combinations, best estimated size, and the
  ///            integer encoding to apply)
  /// \param[in] vector_size number of elements per vector
  /// \param[out] output the buffer to be compressed into
  /// \param[in] output_size the size of the compression buffer
  /// \return the compression progress
  static CompressionProgress EncodeAlp(const T* input, int64_t element_count,
                                       const AlpEncodingParameters& preset,
                                       int32_t vector_size, uint8_t* output,
                                       int64_t output_size);

  /// \brief Load the AlpHeader from compressed data
  ///
  /// \param[in] input the compressed buffer
  /// \param[in] input_size the size of the compressed data
  /// \return the AlpHeader, or an error if the buffer is too small
  static Result<AlpHeader> LoadHeader(const uint8_t* input, int64_t input_size);
};

}  // namespace alp
}  // namespace util
}  // namespace arrow

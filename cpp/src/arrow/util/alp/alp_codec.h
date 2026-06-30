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
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/alp/alp.h"
#include "arrow/util/alp/alp_sampler.h"

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
  /// reused for encoding. This is useful when you want to pre-compute the preset
  /// outside of the benchmark loop or encode multiple batches with the same preset.
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
  /// \param[in] input pointer to the input that is to be encoded
  /// \param[in] num_elements number of elements to encode
  /// \param[in] preset the pre-computed sampling result from CreateSamplingPreset()
  /// \param[in] vector_size number of elements per vector (must be a power of 2,
  ///            at most 2^kMaxLogVectorSize)
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
  ///         `vector_size` a positive power of two no larger than
  ///         `2^kMaxLogVectorSize`.
  static Status EncodeWithPreset(const T* input, int64_t num_elements,
                                 const AlpSamplerResult& preset,
                                 int32_t vector_size,
                                 uint8_t* output, int64_t* output_size);

  /// \brief Encode floating point values using ALP decimal compression
  ///
  /// \param[in] input pointer to the input that is to be encoded
  /// \param[in] num_elements number of elements to encode
  /// \param[in] vector_size number of elements per vector (must be a power of 2,
  ///            at most 2^kMaxLogVectorSize)
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
  ///         `vector_size` a positive power of two no larger than
  ///         `2^kMaxLogVectorSize`.
  static Status Encode(const T* input, int64_t num_elements,
                       int32_t vector_size,
                       uint8_t* output, int64_t* output_size);

  /// \brief Convenience overload with default vector_size = kAlpVectorSize.
  ///        Same preconditions and error returns as the four-argument overload.
  static Status Encode(const T* input, int64_t num_elements,
                       uint8_t* output, int64_t* output_size);

  /// \brief Decode floating point values
  ///
  /// \param[in] num_elements number of elements to decode (from page header)
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

  /// \brief Get the maximum compressed size for a given number of elements
  ///
  /// \param[in] num_elements number of elements to compress
  /// \param[in] vector_size number of elements per vector (must be a positive
  ///            power of 2, at most 2^kMaxLogVectorSize)
  /// \return the maximum size of the compressed buffer in bytes, or
  ///         Status::Invalid if `num_elements < 0` or `vector_size` is not a
  ///         positive power of two no larger than `2^kMaxLogVectorSize`.
  static Result<int64_t> GetMaxCompressedSize(
      int64_t num_elements,
      int32_t vector_size = AlpConstants::kAlpVectorSize);

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

  /// \brief Tracks the progress of a decompression operation
  ///
  /// Used to report how much data was consumed and produced during decoding.
  struct DecompressionProgress {
    /// Number of decompressed elements written
    int64_t num_decompressed_elements_produced = 0;
    /// Number of compressed bytes consumed
    int64_t num_compressed_bytes_taken = 0;
  };

  /// \brief Compress a buffer using ALP
  ///
  /// \param[in] input array of floating point numbers to compress
  /// \param[in] element_count the number of floating point numbers
  /// \param[out] output the buffer to be compressed into
  /// \param[in] output_size the size of the compression buffer
  /// \param[in] preset the encoding preset to use (contains the candidate
  ///            exponent/factor combinations, best estimated size, and the
  ///            integer encoding to apply)
  /// \return the compression progress
  static CompressionProgress EncodeAlp(const T* input, int64_t element_count,
                                       uint8_t* output, int64_t output_size,
                                       const AlpEncodingParameters& preset,
                                       int32_t vector_size);

  /// \brief Decompress a buffer using ALP
  ///
  /// \param[in] num_elements the number of elements to decompress
  /// \param[in] input the compressed buffer
  /// \param[in] input_size the size of the compressed data in bytes
  /// \param[in] integer_encoding the bit packing layout used
  /// \param[in] vector_size the number of elements per vector (from header)
  /// \param[in] total_elements the total number of elements in the page (from header)
  /// \param[out] output the buffer to decompress into
  /// \return the decompression progress, or an error if the compressed data is malformed
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static Result<DecompressionProgress> DecodeAlp(int64_t num_elements,
                                                  const uint8_t* input, int64_t input_size,
                                                  AlpIntegerEncoding integer_encoding,
                                                  int32_t vector_size,
                                                  int32_t total_elements,
                                                  TargetType* output);

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

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

// High-level wrapper interface for ALP compression

#pragma once

#include <cstddef>
#include <optional>

#include "arrow/util/alp/Alp.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// AlpWrapper

/// \class AlpWrapper
/// \brief High-level interface for ALP compression
///
/// AlpWrapper is an interface for Adaptive Lossless floating-Point Compression (ALP)
/// (https://dl.acm.org/doi/10.1145/3626717). For encoding, it samples the data and applies
/// decimal compression (Alp) to floating point values.
/// This class acts as a wrapper around the vector-based interfaces of AlpSampler and Alp.
///
/// \tparam T the floating point type (float or double)
template <typename T>
class AlpWrapper {
 public:
  /// \brief Encode floating point values using ALP decimal compression
  ///
  /// \param[in] decomp pointer to the input that is to be encoded
  /// \param[in] decompSize size of decomp in bytes. This needs to be a multiple of sizeof(T).
  /// \param[out] comp pointer to the memory region we will encode into.
  ///             The caller is responsible for ensuring this is big enough.
  /// \param[in,out] compSize the actual size of the encoded data in bytes,
  ///                expects the size of comp as input. If this is too small,
  ///                this is set to 0 and we bail out.
  /// \param[in] enforceMode reserved for future use. Currently only AlpMode::kAlp is supported.
  static void encode(const T* decomp, size_t decompSize, char* comp, size_t* compSize,
                     std::optional<AlpMode> enforceMode = std::nullopt);

  /// \brief Decode floating point values
  ///
  /// \param[out] decomp pointer to the memory region we will decode into.
  ///             The caller is responsible for ensuring this is big enough.
  /// \param[in,out] decompSize the actual size of decoded data in bytes,
  ///                expects the decomp size as input.
  /// \param[in] comp pointer to the input that is to be decoded
  /// \param[in] compSize size of the input in bytes.
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void decode(TargetType* decomp, size_t* decompSize, const char* comp, size_t compSize);

  /// \brief Get the decompressed size of a compression block
  ///
  /// Get the size of a compression block encoded previously with AlpWrapper::encode().
  ///
  /// \param[in] comp start of the memory region containing the compression block
  /// \param[in] compSize size of the compression block
  /// \return the decompressed size of the block, in bytes
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static uint64_t getDecompressedSize(const char* comp, uint64_t compSize);

  /// \brief Get the maximum compressed size of an uncompressed buffer
  ///
  /// \param[in] decompSize the size of the uncompressed buffer in bytes
  /// \return the maximum size of the compressed buffer
  static uint64_t getMaxCompressedSize(uint64_t decompSize);

 private:
  struct CompressionBlockHeader;

  /// \brief Tracks the progress of a compression operation
  ///
  /// Used to report how much data was consumed and produced during encoding.
  struct CompressionProgress {
    /// Number of compressed bytes written to output
    uint64_t numCompressedBytesProduced = 0;
    /// Number of input elements consumed
    uint64_t numUncompressedElementsTaken = 0;
  };

  /// \brief Tracks the progress of a decompression operation
  ///
  /// Used to report how much data was consumed and produced during decoding.
  struct DecompressionProgress {
    /// Number of decompressed elements written
    uint64_t numDecompressedElementsProduced = 0;
    /// Number of compressed bytes consumed
    uint64_t numCompressedBytesTaken = 0;
  };

  /// \brief Compress a buffer using the ALP variant
  ///
  /// \param[in] decomp an array of floating point numbers containing the uncompressed data
  /// \param[in] elementCount the number of floating point numbers to be compressed
  /// \param[out] comp the buffer to be compressed into
  /// \param[in] compSize the size of the compression buffer
  /// \param[in] combinations the encoding preset to use
  /// \return the compression progress
  static CompressionProgress encodeAlp(const T* decomp, uint64_t elementCount, char* comp,
                                       size_t compSize, const AlpEncodingPreset& combinations);

  /// \brief Decompress a buffer using the ALP variant
  ///
  /// \param[out] decomp the buffer to be decompressed into
  /// \param[in] decompElementCount the number of floating point numbers to be decompressed
  /// \param[in] comp the compressed buffer to be decompressed
  /// \param[in] compSize the size of the compressed data in the compressed buffer
  /// \param[in] bitPackLayout the bit packing layout used
  /// \return the decompression progress
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static DecompressionProgress decodeAlp(TargetType* decomp, size_t decompElementCount,
                                         const char* comp, size_t compSize,
                                         const AlpBitPackLayout bitPackLayout);

  /// \brief Load the CompressionBlockHeader from compressed data
  ///
  /// \param[in] comp the compressed buffer
  /// \param[in] compSize the size of the compressed data
  /// \return the CompressionBlockHeader from comp
  static CompressionBlockHeader loadHeader(const char* comp, size_t compSize);
};

}  // namespace alp
}  // namespace util
}  // namespace arrow

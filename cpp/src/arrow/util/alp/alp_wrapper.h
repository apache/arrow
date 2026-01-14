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

#include "arrow/util/alp/alp.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// AlpWrapper

/// \class AlpWrapper
/// \brief High-level interface for ALP compression
///
/// AlpWrapper is an interface for Adaptive Lossless floating-Point Compression
/// (ALP) (https://dl.acm.org/doi/10.1145/3626717). For encoding, it samples
/// the data and applies decimal compression (Alp) to floating point values.
/// This class acts as a wrapper around the vector-based interfaces of
/// AlpSampler and Alp.
///
/// \tparam T the floating point type (float or double)
template <typename T>
class AlpWrapper {
 public:
  /// \brief Encode floating point values using ALP decimal compression
  ///
  /// \param[in] decomp pointer to the input that is to be encoded
  /// \param[in] decomp_size size of decomp in bytes.
  ///            This needs to be a multiple of sizeof(T).
  /// \param[out] comp pointer to the memory region we will encode into.
  ///             The caller is responsible for ensuring this is big enough.
  /// \param[in,out] comp_size the actual size of the encoded data in bytes,
  ///                expects the size of comp as input. If this is too small,
  ///                this is set to 0 and we bail out.
  /// \param[in] enforce_mode reserved for future use.
  ///            Currently only AlpMode::kAlp is supported.
  static void Encode(const T* decomp, size_t decomp_size, char* comp,
                     size_t* comp_size,
                     std::optional<AlpMode> enforce_mode = std::nullopt);

  /// \brief Decode floating point values
  ///
  /// \param[out] decomp pointer to the memory region we will decode into.
  ///             The caller is responsible for ensuring this is big enough
  ///             to hold num_elements values.
  /// \param[in] num_elements number of elements to decode (from page header).
  ///            Uses uint32_t since Parquet page headers use i32 for num_values.
  /// \param[in] comp pointer to the input that is to be decoded
  /// \param[in] comp_size size of the input in bytes (from page header)
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void Decode(TargetType* decomp, uint32_t num_elements, const char* comp,
                     size_t comp_size);

  /// \brief Get the maximum compressed size of an uncompressed buffer
  ///
  /// \param[in] decomp_size the size of the uncompressed buffer in bytes
  /// \return the maximum size of the compressed buffer
  static uint64_t GetMaxCompressedSize(uint64_t decomp_size);

 private:
  struct AlpHeader;

  /// \brief Tracks the progress of a compression operation
  ///
  /// Used to report how much data was consumed and produced during encoding.
  struct CompressionProgress {
    /// Number of compressed bytes written to output
    uint64_t num_compressed_bytes_produced = 0;
    /// Number of input elements consumed
    uint64_t num_uncompressed_elements_taken = 0;
  };

  /// \brief Tracks the progress of a decompression operation
  ///
  /// Used to report how much data was consumed and produced during decoding.
  struct DecompressionProgress {
    /// Number of decompressed elements written
    uint64_t num_decompressed_elements_produced = 0;
    /// Number of compressed bytes consumed
    uint64_t num_compressed_bytes_taken = 0;
  };

  /// \brief Compress a buffer using the ALP variant
  ///
  /// \param[in] decomp array of floating point numbers to compress
  /// \param[in] element_count the number of floating point numbers
  /// \param[out] comp the buffer to be compressed into
  /// \param[in] comp_size the size of the compression buffer
  /// \param[in] combinations the encoding preset to use
  /// \return the compression progress
  static CompressionProgress EncodeAlp(const T* decomp, uint64_t element_count,
                                       char* comp, size_t comp_size,
                                       const AlpEncodingPreset& combinations);

  /// \brief Decompress a buffer using the ALP variant
  ///
  /// \param[out] decomp the buffer to be decompressed into
  /// \param[in] decomp_element_count the number of floats to decompress
  /// \param[in] comp the compressed buffer to be decompressed
  /// \param[in] comp_size the size of the compressed data
  /// \param[in] integer_encoding the bit packing layout used
  /// \param[in] vector_size the number of elements per vector (from header)
  /// \param[in] total_elements the total number of elements in the page (from header).
  ///            Uses uint32_t since Parquet page headers use i32 for num_values.
  /// \return the decompression progress
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static DecompressionProgress DecodeAlp(TargetType* decomp, size_t decomp_element_count,
                                         const char* comp, size_t comp_size,
                                         AlpIntegerEncoding integer_encoding,
                                         uint32_t vector_size, uint32_t total_elements);

  /// \brief Load the AlpHeader from compressed data
  ///
  /// \param[in] comp the compressed buffer
  /// \param[in] comp_size the size of the compressed data
  /// \return the AlpHeader from comp
  static AlpHeader LoadHeader(const char* comp, size_t comp_size);
};

}  // namespace alp
}  // namespace util
}  // namespace arrow

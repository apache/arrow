#pragma once
#include <cstddef>
#include <optional>

#include "arrow/util/alp/Alp.h"

namespace arrow {
namespace util {
namespace alp {

/**
 * AlpWrapper is an interface for Adaptive Lossless floating-Point Compression (ALP)
 * (https://dl.acm.org/doi/10.1145/3626717). For encoding, it samples the data and applies
 * decimal compression (Alp) to floating point values.
 * This class acts as a wrapper around the vector-based interfaces of AlpSampler and Alp.
 */
template <typename T>
class AlpWrapper {
 public:
  /**
   * Entry point for encoding floating point values using Alp decimal compression.
   * @param decomp [IN]
   *  Pointer to the input that is to be encoded
   * @param decompSize [IN]
   *  Size of decomp in bytes. This needs to be a multiple of sizeof(T).
   * @param comp [OUT]
   *  Pointer to the memory region we will encode into. The caller is responsible for ensuring this
   *  is big enough.
   * @param compSize [IN/OUT]
   *  The actual size of the encoded data in bytes, expects the size of comp as input. If this is
   *  too small, this is set to 0 and we bail out.
   * @param enforceMode [IN]
   *  Reserved for future use. Currently only AlpMode::kAlp is supported.
   */
  static void encode(const T* decomp, size_t decompSize, char* comp, size_t* compSize,
                     std::optional<AlpMode> enforceMode = std::nullopt);

  /**
   * Entry point for decoding floating point values.
   * @param decomp [OUT]
   *  Pointer to the memory region we will decode into. The caller is responsible
   *  for ensuring this is big enough.
   * @param decompSize [IN/OUT]
   *  The actual size of decoded data in bytes, expects the decomp size as input.
   * @param comp [IN]
   *  Pointer to the input that is to be decoded
   * @param compSize [IN]
   *  Size of the input in bytes.
   * @tparam TargetType
   *  The type that is used to store the output. May not be a narrowing conversion from T.
   */
  template <typename TargetType>
  static void decode(TargetType* decomp, size_t* decompSize, const char* comp, size_t compSize);

  /**
   * Get the size of a compression block encoded previously with AlpWrapper::encode().
   * @param comp [IN]
   *  Start of the memory region containg the compression block.
   * @param compSize [IN]
   *  Size of the compression block.
   * @return
   *  The decompressed size of the block, in bytes.
   * @tparam TargetType
   *  The type that is used to store the output. May not be a narrowing conversion from T.
   */
  template <typename TargetType>
  static uint64_t getDecompressedSize(const char* comp, uint64_t compSize);

  /**
   * Get the maximum compressed size of a uncompressed buffer.
   * @param decompSize
   *  The size of the uncompressed buffer in bytes.
   * @return
   * The maximum size of the compressed buffer.
   */
  static uint64_t getMaxCompressedSize(uint64_t decompSize);

 private:
  struct CompressionBlockHeader;

  /**
   * Tracks the progress of a compression operation.
   * Used to report how much data was consumed and produced during encoding.
   */
  struct CompressionProgress {
    /// Number of compressed bytes written to output.
    uint64_t numCompressedBytesProduced = 0;
    /// Number of input elements consumed.
    uint64_t numUncompressedElementsTaken = 0;
  };

  /**
   * Tracks the progress of a decompression operation.
   * Used to report how much data was consumed and produced during decoding.
   */
  struct DecompressionProgress {
    /// Number of decompressed elements written.
    uint64_t numDecompressedElementsProduced = 0;
    /// Number of compressed bytes consumed.
    uint64_t numCompressedBytesTaken = 0;
  };

  /**
   * Compress a buffer using the Alp variant.
   * @param decomp
   *  An array of floating point numbers containing the uncompressed data.
   * @param elementCount
   *  The number of of floating point numbers to be compressed.
   * @param comp
   *  The buffer to be compressed into.
   * @param compSize
   *  The size of the compression buffer.
   * @return
   *  The compressed size, in bytes.
   */
  static CompressionProgress encodeAlp(const T* decomp, uint64_t elementCount, char* comp,
                                       size_t compSize, const AlpEncodingPreset& combinations);

  /**
   * Decompress a buffer using the Alp variant.
   * @param decomp
   *  The buffer to be decompressed into.
   * @param decompElementCount
   *  The number of of floating point numbers to be decompressed.
   * @param comp
   *  The compressed buffer to be decompressed.
   * @param compSize
   *  The size of the compressed data in the compressed buffer.
   * @return
   *  The number of decompressed elements.
   * @tparam TargetType
   *  The type that is used to store the output. May not be a narrowing conversion from T.
   */
  template <typename TargetType>
  static DecompressionProgress decodeAlp(TargetType* decomp, size_t decompElementCount,
                                         const char* comp, size_t compSize,
                                         const AlpBitPackLayout bitPackLayout);

  /**
   * Helper function to load the CompressionBlockHeader.
   * @param comp
   *  The compressed buffer to be decompressed.
   * @param compSize
   *  The size of the compressed data in the compressed buffer.
   * @return
   *  The CompressionBlockHeader from comp.
   */
  static CompressionBlockHeader loadHeader(const char* comp, size_t compSize);
};

}  // namespace alp
}  // namespace util
}  // namespace arrow

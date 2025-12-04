#include "arrow/util/alp/AlpWrapper.hpp"

#include <cmath>
#include <optional>

#include "arrow/util/alp/Alp.hpp"
#include "arrow/util/alp/AlpConstants.hpp"
#include "arrow/util/alp/AlpSampler.hpp"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

/**
 * Header structure for ALP compression blocks.
 * Contains metadata required to decompress the data.
 *
 * Serialization format (version 1):
 *
 *   ┌───────────────────────────────────────────────────┐
 *   │  CompressionBlockHeader (40 bytes)                │
 *   ├───────────────────────────────────────────────────┤
 *   │  Offset │ Field               │ Size              │
 *   ├─────────┼─────────────────────┼───────────────────┤
 *   │    0    │ version             │ 8 bytes (uint64)  │
 *   │    8    │ compressedSize      │ 8 bytes (uint64)  │
 *   │   16    │ numElements         │ 8 bytes (uint64)  │
 *   │   24    │ vectorSize          │ 8 bytes (uint64)  │
 *   │   32    │ compressionMode     │ 4 bytes (enum)    │
 *   │   36    │ bitPackLayout       │ 4 bytes (enum)    │
 *   └───────────────────────────────────────────────────┘
 *
 * Note: version must remain the first field to allow reading the rest
 * of the header based on version number.
 */
template <typename T>
struct AlpWrapper<T>::CompressionBlockHeader {
  /// Version number. Must remain the first field for version-based parsing.
  uint64_t version = 0;
  /// Size of the compressed data in bytes (includes header).
  uint64_t compressedSize = 0;
  /// Number of elements in the compressed data.
  uint64_t numElements = 0;
  /// Vector size used for compression. Must be AlpConstants::kAlpVectorSize for decompression.
  uint64_t vectorSize = 0;
  /// Compression mode (currently only kAlp is supported).
  AlpMode compressionMode = AlpMode::kAlp;
  /// Bit packing layout used for bitpacking.
  AlpBitPackLayout bitPackLayout = AlpBitPackLayout::kNormal;

  /**
   * Return the size in bytes of the CompressionBlockHeader based on the provided version.
   * @param v Version
   */
  static size_t getSizeForVersion(const uint64_t v) {
    size_t size;
    if (v == 1) {
      size = sizeof(version) + sizeof(compressedSize) + sizeof(numElements) + sizeof(vectorSize) +
             sizeof(compressionMode) + sizeof(bitPackLayout);
    } else {
      ARROW_CHECK(false) << "unknown_version: " << v;
    }
    return size;
  }

  /**
   * Check whether the given version v is valid and return it. Otherwise assert.
   * @param v Version
   */
  static uint64_t isValidVersion(const uint64_t v) {
    if (v == 1) {
      return v;
    }
    ARROW_CHECK(false) << "invalid_version: " << v;
    return 0;  // Unreachable, but silences warning.
  }
};

template <typename T>
typename AlpWrapper<T>::CompressionBlockHeader AlpWrapper<T>::loadHeader(const char* comp,
                                                                         size_t compSize) {
  CompressionBlockHeader header{};
  ARROW_CHECK(compSize > sizeof(header.version))
      << "alp_loadHeader_compSize_too_small_for_header_version";
  uint64_t version;
  std::memcpy(&version, comp, sizeof(header.version));
  CompressionBlockHeader::isValidVersion(version);
  ARROW_CHECK(compSize >= CompressionBlockHeader::getSizeForVersion(version))
      << "alp_loadHeader_compSize_too_small";
  std::memcpy(&header, comp, CompressionBlockHeader::getSizeForVersion(version));
  return header;
}

template <typename T>
void AlpWrapper<T>::encode(const T* decomp, const size_t decompSize, char* comp, size_t* compSize,
                           std::optional<AlpMode> enforceMode) {
  ARROW_CHECK(decompSize % sizeof(T) == 0) << "alp_encode_input_must_be_multiple_of_T";
  const uint64_t elementCount = decompSize / sizeof(T);
  const uint64_t version = CompressionBlockHeader::isValidVersion(AlpConstants::kAlpVersion);

  AlpSampler<T> sampler;
  sampler.addSample({decomp, elementCount});
  auto samplingResult = sampler.finalize();

  // Make room to store header afterwards.
  char* encodedHeader = comp;
  comp += CompressionBlockHeader::getSizeForVersion(version);
  const uint64_t remainingCompressedSize =
      *compSize - CompressionBlockHeader::getSizeForVersion(version);

  const CompressionProgress compressionProgress =
      encodeAlp(decomp, elementCount, comp, remainingCompressedSize, samplingResult.alpPreset);

  CompressionBlockHeader header{};
  header.version = version;
  header.compressedSize =
      header.getSizeForVersion(version) + compressionProgress.numCompressedBytesProduced;
  header.numElements = decompSize / sizeof(T);
  header.vectorSize = AlpConstants::kAlpVectorSize;
  header.compressionMode = AlpMode::kAlp;
  header.bitPackLayout = AlpBitPackLayout::kNormal;

  std::memcpy(encodedHeader, &header, CompressionBlockHeader::getSizeForVersion(version));
  *compSize = header.compressedSize;
}

template <typename T>
template <typename TargetType>
void AlpWrapper<T>::decode(TargetType* decomp, size_t* decompSize, const char* comp,
                           const size_t compSize) {
  const CompressionBlockHeader header = loadHeader(comp, compSize);
  ARROW_CHECK(header.vectorSize == AlpConstants::kAlpVectorSize)
      << "unsupported_vector_size: " << header.vectorSize;

  if (header.numElements * sizeof(TargetType) > *decompSize) {
    *decompSize = 0;
    return;
  }

  const uint64_t elementsToDecode = header.numElements;
  const char* compressionBody = comp + CompressionBlockHeader::getSizeForVersion(header.version);
  const uint64_t compressionBodySize =
      compSize - CompressionBlockHeader::getSizeForVersion(header.version);

  ARROW_CHECK(header.compressionMode == AlpMode::kAlp) << "alp_decode_unsupported_mode";

  uint64_t elementsDecoded = decodeAlp(decomp, elementsToDecode, compressionBody,
                                       compressionBodySize, header.bitPackLayout)
                                 .numDecompressedElementsProduced;
  *decompSize = elementsDecoded * sizeof(TargetType);
}

template void AlpWrapper<float>::decode(float* decomp, size_t* decompSize, const char* comp,
                                        const size_t compSize);
template void AlpWrapper<float>::decode(double* decomp, size_t* decompSize, const char* comp,
                                        const size_t compSize);
template void AlpWrapper<double>::decode(double* decomp, size_t* decompSize, const char* comp,
                                         const size_t compSize);

template <typename T>
template <typename TargetType>
uint64_t AlpWrapper<T>::getDecompressedSize(const char* comp, uint64_t compSize) {
  const CompressionBlockHeader header = loadHeader(comp, compSize);
  return header.numElements * sizeof(TargetType);
}

template uint64_t AlpWrapper<float>::getDecompressedSize<float>(const char* comp,
                                                                uint64_t compSize);
template uint64_t AlpWrapper<float>::getDecompressedSize<double>(const char* comp,
                                                                 uint64_t compSize);
template uint64_t AlpWrapper<double>::getDecompressedSize<double>(const char* comp,
                                                                  uint64_t compSize);

template <typename T>
uint64_t AlpWrapper<T>::getMaxCompressedSize(const uint64_t decompSize) {
  ARROW_CHECK(decompSize % sizeof(T) == 0) << "alp_decompressed_size_not_multiple_of_T";
  const uint64_t elementCount = decompSize / sizeof(T);
  const uint64_t version = CompressionBlockHeader::isValidVersion(AlpConstants::kAlpVersion);
  uint64_t maxAlpSize = CompressionBlockHeader::getSizeForVersion(version);
  // Add header sizes.
  maxAlpSize += sizeof(AlpEncodedVectorInfo) *
                std::ceil(static_cast<double>(elementCount) / AlpConstants::kAlpVectorSize);
  // Worst case: everything is an exception, except two values that are chosen with large
  // difference to make FOR encoding for the placeholders impossible.
  // Values/placeholders.
  maxAlpSize += elementCount * sizeof(T);
  // Exceptions.
  maxAlpSize += elementCount * sizeof(T);
  // Exception positions.
  maxAlpSize += elementCount * sizeof(AlpConstants::PositionType);

  return maxAlpSize;
}

template <typename T>
auto AlpWrapper<T>::encodeAlp(const T* decomp, uint64_t elementCount, char* comp, size_t compSize,
                              const AlpEncodingPreset& combinations) -> CompressionProgress {
  uint64_t outputOffset = 0;
  uint64_t inputOffset = 0;
  uint64_t remainingOutputSize = compSize;

  for (uint64_t remainingElements = elementCount; remainingElements > 0;
       remainingElements -= std::min(AlpConstants::kAlpVectorSize, remainingElements)) {
    const uint64_t elementsToEncode = std::min(AlpConstants::kAlpVectorSize, remainingElements);
    const AlpEncodedVector<T> encodedVector =
        AlpCompression<T>::compressVector(decomp + inputOffset, elementsToEncode, combinations);

    const uint64_t compressedVectorSize = encodedVector.getStoredSize();
    if (compressedVectorSize == 0 || compressedVectorSize > remainingOutputSize) {
      return CompressionProgress{0, 0};
    }

    ARROW_CHECK(encodedVector.getStoredSize() <= remainingOutputSize)
        << "alp_encode_cannot_store_compressed_vector";

    encodedVector.store({comp + outputOffset, remainingOutputSize});

    remainingOutputSize -= compressedVectorSize;
    outputOffset += compressedVectorSize;
    inputOffset += elementsToEncode;
  }
  return CompressionProgress{outputOffset, inputOffset};
}

template <typename T>
template <typename TargetType>
auto AlpWrapper<T>::decodeAlp(TargetType* decomp, const size_t decompElementCount, const char* comp,
                              const size_t compSize, const AlpBitPackLayout bitPackLayout)
    -> DecompressionProgress {
  uint64_t inputOffset = 0;
  uint64_t outputOffset = 0;
  while (inputOffset < compSize && outputOffset < decompElementCount) {
    const AlpEncodedVector<T> encodedVector =
        AlpEncodedVector<T>::load({comp + inputOffset, compSize});
    const uint64_t compressedSize = encodedVector.getStoredSize();
    const uint64_t elementCount = encodedVector.vectorInfo.numElements;

    ARROW_CHECK(outputOffset + elementCount <= decompElementCount)
        << "alp_decode_output_too_small: " << outputOffset << " vs " << elementCount << " vs "
        << decompElementCount;

    AlpCompression<T>::decompressVector(encodedVector, bitPackLayout, decomp + outputOffset);

    inputOffset += compressedSize;
    outputOffset += elementCount;
  }

  return DecompressionProgress{outputOffset, inputOffset};
}

template class AlpWrapper<float>;
template class AlpWrapper<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow

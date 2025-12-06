#pragma once
#include <optional>
#include <vector>

#include "arrow/util/span.h"
#include "arrow/util/alp/Alp.h"

namespace arrow {
namespace util {
namespace alp {

/**
 * Collects samples from data to be compressed with ALP and creates an encoding preset.
 * Usage: Call addSample() or addSampleVector() multiple times to collect samples,
 * then call finalize() to retrieve the resulting preset.
 * @tparam T
 *  The floating point type (float or double) to sample.
 */
template <typename T>
class AlpSampler {
 public:
  /// Default constructor.
  AlpSampler();

  /// Helper struct containing the preset for ALP compression.
  struct AlpSamplerResult {
    AlpEncodingPreset alpPreset;
  };

  /**
   * Add a sample of arbitrary size.
   * The sample is internally separated into vectors on which addSampleVector() is called.
   * @param input
   *  The input data to sample from.
   */
  void addSample(arrow::util::span<const T> input);

  /**
   * Add a single vector as a sample.
   * @param input
   *  The input vector to add. Size should be <= AlpConstants::kAlpVectorSize.
   */
  void addSampleVector(arrow::util::span<const T> input);

  /**
   * Finalize sampling and generate the encoding preset.
   * @return
   *  An AlpSamplerResult containing the generated encoding preset.
   */
  AlpSamplerResult finalize();

 private:
  /// Helper struct to encapsulate settings used for sampling.
  struct AlpSamplingParameters {
    uint64_t numLookupValue;
    uint64_t numSampledIncrements;
    uint64_t numSampledValues;
  };

  /**
   * Calculate sampling parameters for the current vector.
   * @param numCurrentVectorValues
   *  The number of values in the current vector.
   * @return
   *  The sampling parameters to use.
   */
  AlpSamplingParameters getAlpSamplingParameters(uint64_t numCurrentVectorValues);

  /**
   * Check if the current vector must be ignored for sampling.
   * @param vectorsCount
   *  The total number of vectors processed so far.
   * @param vectorsSampledCount
   *  The number of vectors that have been sampled so far.
   * @param numCurrentVectorValues
   *  The number of values in the current vector.
   * @return
   *  True if the current vector should be skipped, false otherwise.
   */
  bool mustSkipSamplingFromCurrentVector(uint64_t vectorsCount, uint64_t vectorsSampledCount,
                                         uint64_t numCurrentVectorValues);

  /// Count of vectors that have been sampled.
  uint64_t m_vectorsSampledCount = 0;
  /// Total count of values processed.
  uint64_t m_totalValuesCount = 0;
  /// Total count of vectors processed.
  uint64_t m_vectorsCount = 0;
  /// Number of samples stored.
  uint64_t m_sampleStored = 0;
  /// Samples collected from current rowgroup.
  std::vector<std::vector<T>> m_rowgroupSample;

  /// Complete vectors sampled.
  std::vector<std::vector<T>> m_completeVectorsSampled;
  /// Size of each sample vector.
  const uint64_t m_sampleVectorSize;
  /// Size of each rowgroup.
  const uint64_t m_rowgroupSize;
  /// Number of samples to take per vector.
  const uint64_t m_samplesPerVector;
  /// Number of vectors to sample per rowgroup.
  const uint64_t m_sampleVectorsPerRowgroup;
  /// Jump interval for rowgroup sampling.
  const uint64_t m_rowgroupSampleJump;
};

}  // namespace alp
}  // namespace util
}  // namespace arrow

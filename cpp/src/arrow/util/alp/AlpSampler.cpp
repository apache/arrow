#include "arrow/util/alp/AlpSampler.hpp"

#include <cmath>

#include "arrow/util/alp/Alp.hpp"
#include "arrow/util/alp/AlpConstants.hpp"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

template <typename T>
AlpSampler<T>::AlpSampler()
    : m_sampleVectorSize(AlpConstants::kSamplerVectorSize),
      m_rowgroupSize(AlpConstants::kSamplerRowgroupSize),
      m_samplesPerVector(AlpConstants::kSamplerSamplesPerVector),
      m_sampleVectorsPerRowgroup(AlpConstants::kSamplerSampleVectorsPerRowgroup),
      m_rowgroupSampleJump((m_rowgroupSize / m_sampleVectorsPerRowgroup) / m_sampleVectorSize) {}

template <typename T>
void AlpSampler<T>::addSample(arrow::util::span<const T> input) {
  for (uint64_t i = 0; i < input.size(); i += m_sampleVectorSize) {
    const uint64_t elements = std::min(input.size() - i, m_sampleVectorSize);
    addSampleVector({input.data() + i, elements});
  }
}

template <typename T>
void AlpSampler<T>::addSampleVector(arrow::util::span<const T> input) {
  const bool mustSkipCurrentVector =
      mustSkipSamplingFromCurrentVector(m_vectorsCount, m_vectorsSampledCount, input.size());

  m_vectorsCount += 1;
  m_totalValuesCount += input.size();
  if (mustSkipCurrentVector) {
    return;
  }

  const AlpSamplingParameters samplingParams = getAlpSamplingParameters(input.size());

  // Slice: take first numLookupValue elements.
  std::vector<T> currentVectorValues(
      input.begin(), input.begin() + std::min<size_t>(samplingParams.numLookupValue, input.size()));

  // Stride: take every numSampledIncrements-th element.
  std::vector<T> currentVectorSample;
  for (size_t i = 0; i < currentVectorValues.size(); i += samplingParams.numSampledIncrements) {
    currentVectorSample.push_back(currentVectorValues[i]);
  }
  m_sampleStored += currentVectorSample.size();

  m_completeVectorsSampled.push_back(std::move(currentVectorValues));
  m_rowgroupSample.push_back(std::move(currentVectorSample));
  m_vectorsSampledCount++;
}

template <typename T>
typename AlpSampler<T>::AlpSamplerResult AlpSampler<T>::finalize() {
  ARROW_LOG(DEBUG) << "AlpSampler finalized: vectorsSampled=" << m_vectorsSampledCount << "/"
                   << m_vectorsCount << " total"
                   << ", valuesSampled=" << m_sampleStored << "/" << m_totalValuesCount << " total";

  AlpSamplerResult result;
  result.alpPreset = AlpCompression<T>::createEncodingPreset(m_rowgroupSample);

  ARROW_LOG(DEBUG) << "AlpSampler preset: " << result.alpPreset.combinations.size()
                   << " exponent/factor combinations"
                   << ", estimatedSize=" << result.alpPreset.bestCompressedSize << " bytes";

  return result;
}

template <typename T>
typename AlpSampler<T>::AlpSamplingParameters AlpSampler<T>::getAlpSamplingParameters(
    uint64_t numCurrentVectorValues) {
  const uint64_t numLookupValues =
      std::min(numCurrentVectorValues, static_cast<uint64_t>(AlpConstants::kAlpVectorSize));
  // We sample equidistant values within a vector; to do this we jump a fixed number of values.
  const uint64_t numSampledIncrements = std::max(
      uint64_t{1},
      static_cast<uint64_t>(std::ceil(static_cast<double>(numLookupValues) / m_samplesPerVector)));
  const uint64_t numSampledValues =
      std::ceil(static_cast<double>(numLookupValues) / numSampledIncrements);

  ARROW_CHECK(numSampledValues < AlpConstants::kAlpVectorSize) << "alp_sample_too_large";

  return AlpSamplingParameters{numLookupValues, numSampledIncrements, numSampledValues};
}

template <typename T>
bool AlpSampler<T>::mustSkipSamplingFromCurrentVector(const uint64_t vectorsCount,
                                                      const uint64_t vectorsSampledCount,
                                                      const uint64_t currentVectorNValues) {
  // We sample equidistant vectors; to do this we skip a fixed number of vectors.
  const bool mustSelectRowgroupSamples = (vectorsCount % m_rowgroupSampleJump) == 0;

  // If we are not in the correct jump, we do not take sample from this vector.
  if (!mustSelectRowgroupSamples) {
    return true;
  }

  // We do not take samples of non-complete vectors (usually the last one),
  // except in the case of too little data.
  if (currentVectorNValues < AlpConstants::kSamplerSamplesPerVector && vectorsSampledCount != 0) {
    return true;
  }
  return false;
}

template class AlpSampler<float>;
template class AlpSampler<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow

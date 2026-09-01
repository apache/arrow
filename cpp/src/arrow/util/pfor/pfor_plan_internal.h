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

// PFOR encoder decision logic: what to do with one vector.
//
// The encoder chooses three things per vector, and this header holds the
// search over all three:
//
//   1. whether to difference the values first (the delta mode),
//   2. the frame of reference, which is not required to be the minimum,
//   3. the bit width, from the existing histogram cost model.
//
// Kept apart from pfor.h, and free of every Arrow dependency, so the same
// decision code can be linked into a standalone size study without building
// the library. Nothing here touches bytes on the wire; pfor.cc does that.

#pragma once

#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>

#include "arrow/util/pfor/pfor_constants_internal.h"

namespace arrow {
namespace util {
namespace pfor {

/// \brief Result of the optimal bit width search
struct BitWidthResult {
  uint8_t bit_width = 0;
  PforConstants::ExceptionCountType num_exceptions = 0;
};

/// \brief Bits an exception costs: its position plus a full-width value.
template <typename T>
constexpr int64_t ExceptionBits() {
  return static_cast<int64_t>(sizeof(PforConstants::PositionType)) * 8 +
         static_cast<int64_t>(sizeof(T)) * 8;
}

// ----------------------------------------------------------------------
// Bit width search

/// \brief Histogram of the bit widths needed by `values` reduced by `frame`.
///
/// out[b] counts the offsets that need exactly b bits. The subtraction is
/// modular in the unsigned type, so a value below the frame wraps to a huge
/// offset and lands in the top bin -- which is what makes a frame above the
/// minimum work at all: below-frame values are counted as exceptions here and
/// then patched on the way out, with no separate sign or direction to track.
template <typename T>
void BuildOffsetHistogram(const T* values, int32_t num_elements, T frame,
                          std::array<int32_t, 65>* out) {
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  const auto unsigned_frame = static_cast<UnsignedT>(frame);

  // Four independent accumulators so repeated bins do not serialize the
  // read-modify-write and the load->clz->bump chains overlap. This loop is
  // ~half of encode time; a single array runs scalar and stalls.
  std::array<int32_t, 65> h0{}, h1{}, h2{}, h3{};
  int32_t i = 0;
  for (; i + 4 <= num_elements; i += 4) {
    ++h0[PforTypeTraits<T>::BitsRequired(static_cast<UnsignedT>(values[i]) -
                                         unsigned_frame)];
    ++h1[PforTypeTraits<T>::BitsRequired(static_cast<UnsignedT>(values[i + 1]) -
                                         unsigned_frame)];
    ++h2[PforTypeTraits<T>::BitsRequired(static_cast<UnsignedT>(values[i + 2]) -
                                         unsigned_frame)];
    ++h3[PforTypeTraits<T>::BitsRequired(static_cast<UnsignedT>(values[i + 3]) -
                                         unsigned_frame)];
  }
  for (; i < num_elements; ++i) {
    ++h0[PforTypeTraits<T>::BitsRequired(static_cast<UnsignedT>(values[i]) -
                                         unsigned_frame)];
  }
  for (int b = 0; b <= 64; ++b) (*out)[b] = h0[b] + h1[b] + h2[b] + h3[b];
}

/// \brief Pick the width that minimises packed bits plus exception bits.
///
/// \param[in] histogram output of BuildOffsetHistogram
/// \param[in] num_elements number of elements the histogram covers
/// \param[out] cost_bits total cost of the winning width
template <typename T>
BitWidthResult BestWidthFromHistogram(const std::array<int32_t, 65>& histogram,
                                      int32_t num_elements, int64_t* cost_bits) {
  constexpr uint8_t max_bits = PforTypeTraits<T>::kMaxBitWidth;

  int64_t best_cost = std::numeric_limits<int64_t>::max();
  uint8_t best_bit_width = max_bits;
  PforConstants::ExceptionCountType best_num_exceptions = 0;

  int64_t exceptions_above = num_elements;
  for (uint8_t b = 0; b <= max_bits; ++b) {
    exceptions_above -= histogram[b];

    // A vector holds at most kMaxVectorSize elements, so that is also the most
    // exceptions one can name. Skipping the wider counts keeps the cast below
    // exact; the full-width candidate has none at all, so a best is always
    // found.
    if (exceptions_above > PforConstants::kMaxVectorSize) continue;

    const int64_t total_cost =
        static_cast<int64_t>(num_elements) * b + exceptions_above * ExceptionBits<T>();
    if (total_cost < best_cost) {
      best_cost = total_cost;
      best_bit_width = b;
      best_num_exceptions =
          static_cast<PforConstants::ExceptionCountType>(exceptions_above);
    }
  }

  *cost_bits = best_cost;
  return {best_bit_width, best_num_exceptions};
}

/// \brief Histogram over offsets that were already reduced by their frame.
template <typename T>
void BuildOffsetHistogram(const typename PforTypeTraits<T>::UnsignedType* offsets,
                          int32_t num_elements, std::array<int32_t, 65>* out) {
  // A pointer to the unsigned counterpart of T may be read as a pointer to T,
  // and BitsRequired only looks at the bits, so a frame of zero over the same
  // storage gives exactly the histogram of the offsets.
  BuildOffsetHistogram<T>(reinterpret_cast<const T*>(offsets), num_elements,
                          static_cast<T>(0), out);
}

// ----------------------------------------------------------------------
// Frame search

/// \brief A frame of reference together with the width that suits it.
template <typename T>
struct FrameChoice {
  T frame_of_reference = 0;
  uint8_t bit_width = 0;
  PforConstants::ExceptionCountType num_exceptions = 0;
  int64_t cost_bits = std::numeric_limits<int64_t>::max();
};

/// Buckets used by the frame search. 256 keeps the scan below (see
/// ChooseFrameAndWidth) at roughly two passes over a 1024-value vector.
constexpr int32_t kFrameSearchBuckets = 256;

/// \brief Choose a frame of reference and a bit width together.
///
/// The frame PFOR has always used is the minimum, which makes every exception
/// an overshoot: one value far below the cluster drags the whole packed window
/// down with it and nothing can patch it back. Treating the frame as a free
/// parameter instead -- any lower bound, not the lowest -- lets the window sit
/// where the values actually are and patch on both sides.
///
/// Storage is unaffected: the frame field already holds a full-width T, and the
/// decoder only ever adds it. The whole cost is this search.
///
/// The search is approximate by design. An exact answer needs the values
/// sorted; instead the range is bucketed with a shift, and for each candidate
/// width a window is slid over the bucket counts. Only whole buckets count as
/// covered, so the exception estimate is an upper bound -- never optimistic.
/// The winning frame is then re-costed exactly against a real histogram, and
/// the minimum-frame answer is always among the candidates, so the result can
/// never be worse than what the old cost model would have picked.
template <typename T>
FrameChoice<T> ChooseFrameAndWidth(const T* values, int32_t num_elements) {
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  constexpr uint8_t max_bits = PforTypeTraits<T>::kMaxBitWidth;

  T min_val = values[0];
  T max_val = values[0];
  for (int32_t i = 1; i < num_elements; ++i) {
    if (values[i] < min_val) min_val = values[i];
    if (values[i] > max_val) max_val = values[i];
  }

  // Candidate 0: the minimum, i.e. what PFOR has always done. Evaluated
  // unconditionally so the search cannot regress against it.
  std::array<int32_t, 65> histogram{};
  BuildOffsetHistogram<T>(values, num_elements, min_val, &histogram);
  FrameChoice<T> best;
  best.frame_of_reference = min_val;
  int64_t cost_bits = 0;
  BitWidthResult r = BestWidthFromHistogram<T>(histogram, num_elements, &cost_bits);
  best.bit_width = r.bit_width;
  best.num_exceptions = r.num_exceptions;
  best.cost_bits = cost_bits;

  // A constant vector has nothing left to improve. A vector that merely has no
  // exceptions does: the whole point of a frame above the minimum is to trade a
  // narrower width for a few patches, so an exception-free choice is a starting
  // point for the search, not a reason to skip it. The sawtooth is the case --
  // its differences pack at width 12 with no exceptions, or at width 0 with
  // five, and only the second is worth having.
  if (best.bit_width == 0) return best;

  const auto range = static_cast<UnsignedT>(max_val) - static_cast<UnsignedT>(min_val);
  const auto range_bits = PforTypeTraits<T>::BitsRequired(range);
  constexpr int32_t kBucketBits = 8;  // 1 << kBucketBits == kFrameSearchBuckets
  const int32_t shift = range_bits > kBucketBits ? range_bits - kBucketBits : 0;

  std::array<int32_t, kFrameSearchBuckets + 1> counts{};
  const auto unsigned_min = static_cast<UnsignedT>(min_val);
  for (int32_t i = 0; i < num_elements; ++i) {
    const UnsignedT offset = static_cast<UnsignedT>(values[i]) - unsigned_min;
    ++counts[static_cast<size_t>(offset >> shift)];
  }
  const int32_t num_buckets = static_cast<int32_t>(range >> shift) + 1;

  std::array<int32_t, kFrameSearchBuckets + 2> prefix{};
  for (int32_t b = 0; b < num_buckets; ++b) prefix[b + 1] = prefix[b] + counts[b];

  // Slide a 2^w-wide window over the buckets. Widths below the bucket size
  // cannot be scanned at this resolution, and once one window spans every
  // bucket there are no exceptions left to remove, so the loop is short: it
  // covers only the ~kBucketBits widths in between.
  T scan_frame = min_val;
  int64_t scan_cost = std::numeric_limits<int64_t>::max();
  for (int32_t w = shift; w <= max_bits; ++w) {
    const int64_t whole_buckets = static_cast<int64_t>(1) << (w - shift);
    if (whole_buckets <= 0) break;
    const int32_t k =
        static_cast<int32_t>(whole_buckets < num_buckets ? whole_buckets : num_buckets);
    for (int32_t s = 0; s + 1 <= num_buckets; ++s) {
      const int32_t end = s + k < num_buckets ? s + k : num_buckets;
      const int64_t covered = prefix[end] - prefix[s];
      const int64_t exceptions = num_elements - covered;
      if (exceptions > PforConstants::kMaxVectorSize) continue;
      const int64_t cost =
          static_cast<int64_t>(num_elements) * w + exceptions * ExceptionBits<T>();
      if (cost < scan_cost) {
        scan_cost = cost;
        scan_frame = static_cast<T>(unsigned_min + (static_cast<UnsignedT>(s) << shift));
      }
    }
    if (k >= num_buckets) break;  // one window already spans the data
  }

  if (scan_frame == min_val) return best;

  // Re-cost the winning frame exactly. The bucketed estimate over-counts
  // exceptions, so the real cost can only come out lower than `scan_cost`.
  BuildOffsetHistogram<T>(values, num_elements, scan_frame, &histogram);
  int64_t exact_cost = 0;
  r = BestWidthFromHistogram<T>(histogram, num_elements, &exact_cost);
  if (exact_cost < best.cost_bits) {
    best.frame_of_reference = scan_frame;
    best.bit_width = r.bit_width;
    best.num_exceptions = r.num_exceptions;
    best.cost_bits = exact_cost;
  }
  return best;
}

// ----------------------------------------------------------------------
// Whole-vector plan

/// \brief Everything the encoder decided about one vector.
template <typename T>
struct PforVectorPlan {
  /// Difference the values before framing and packing them.
  bool delta = false;
  /// Subtracted from every element before packing; added back on decode.
  T frame_of_reference = 0;
  /// First value of the vector, stored only when `delta` is set. It is what
  /// makes a delta vector decodable on its own, without the vector before it.
  T start_value = 0;
  uint8_t bit_width = 0;
  PforConstants::ExceptionCountType num_exceptions = 0;
  /// Packed bits, exception bits, and the start value if there is one. Excludes
  /// the fixed per-vector info, which every plan pays equally.
  int64_t cost_bits = 0;
};

/// \brief Fill `deltas` with the backward differences of `values`.
///
/// deltas[0] is 0: the first value travels in the plan's start_value, and
/// giving slot 0 a real delta would mean either a shorter packed run or a
/// value that is not a difference sitting in the width histogram. Zero costs
/// bit_width bits and distorts nothing.
///
/// Subtraction is done unsigned and copied across, because signed overflow is
/// undefined and a column that spans the type's range will overflow.
template <typename T>
void ComputeDeltas(const T* values, int32_t num_elements, T* deltas) {
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  deltas[0] = 0;
  for (int32_t i = 1; i < num_elements; ++i) {
    const UnsignedT d =
        static_cast<UnsignedT>(values[i]) - static_cast<UnsignedT>(values[i - 1]);
    std::memcpy(&deltas[i], &d, sizeof(T));
  }
}

/// \brief Decide how to encode one vector.
///
/// \param[in] values the vector
/// \param[in] num_elements element count, > 0
/// \param[out] delta_scratch scratch for num_elements differences; on return it
///             holds the differences if the plan chose the delta mode, and is
///             clobbered either way
///
/// Both transforms are costed with the same model and the cheaper one wins, so
/// the mode is a per-vector decision. It has to be: differencing loses 6-19% on
/// every unclustered draw, and a column is rarely all one shape.
template <typename T>
PforVectorPlan<T> ChooseVectorPlan(const T* values, int32_t num_elements,
                                   T* delta_scratch) {
  const FrameChoice<T> raw = ChooseFrameAndWidth<T>(values, num_elements);

  PforVectorPlan<T> plan;
  plan.delta = false;
  plan.frame_of_reference = raw.frame_of_reference;
  plan.bit_width = raw.bit_width;
  plan.num_exceptions = raw.num_exceptions;
  plan.cost_bits = raw.cost_bits;

  // One element has no difference to take, and a vector already packing at
  // width 0 cannot be improved on.
  if (num_elements < 2 || raw.bit_width == 0) return plan;

  ComputeDeltas<T>(values, num_elements, delta_scratch);

  // Both candidates get the full search. A cheaper gate on the spread of the
  // differences was tried first and had to go: the sawtooth is a tight cluster
  // of small positive differences with a handful of large negative ones, so its
  // span is as wide as the raw span while its cost is a fraction of it. Any
  // gate that reads a span rather than a distribution throws away the one shape
  // the mode is here for.
  const FrameChoice<T> delta = ChooseFrameAndWidth<T>(delta_scratch, num_elements);

  // A delta vector carries its own first value, so it starts one full-width
  // value behind.
  const int64_t delta_cost = delta.cost_bits + static_cast<int64_t>(sizeof(T)) * 8;
  if (delta_cost < plan.cost_bits) {
    plan.delta = true;
    plan.frame_of_reference = delta.frame_of_reference;
    plan.start_value = values[0];
    plan.bit_width = delta.bit_width;
    plan.num_exceptions = delta.num_exceptions;
    plan.cost_bits = delta_cost;
  }
  return plan;
}

}  // namespace pfor
}  // namespace util
}  // namespace arrow

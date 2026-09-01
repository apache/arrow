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

/// Bucket count used by the frame search, as a shift and as a count. 256 keeps
/// the scan in ChooseFrameAndWidth at roughly two passes' worth of work over a
/// 1024-value vector.
constexpr int32_t kFrameSearchBits = 8;
constexpr int32_t kFrameSearchBuckets = 1 << kFrameSearchBits;

/// \brief One walk producing both histograms the frame search needs.
///
/// The bit-width histogram costs the frame it is given; the bucket counts cost
/// every other frame. They are gathered together because each needs the same
/// offset, and computing that offset twice was measurably the larger half of
/// the search.
///
/// Bucketing is by shift rather than division: `1 << shift` values per bucket
/// keeps the count at or below kFrameSearchBuckets without a per-element divide.
template <typename T>
void BuildFrameSearchHistograms(const T* values, int32_t num_elements, T frame,
                                int32_t shift, std::array<int32_t, 65>* width_hist,
                                std::array<int32_t, kFrameSearchBuckets + 1>* buckets) {
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  const auto unsigned_frame = static_cast<UnsignedT>(frame);

  // Four accumulators, as in BuildOffsetHistogram: a column whose offsets share
  // a width -- which is most of them -- sends every element to one bin, and a
  // single accumulator turns that into a serial read-modify-write chain.
  std::array<std::array<int32_t, 65>, 4> h{};
  for (int32_t i = 0; i < num_elements; ++i) {
    const UnsignedT offset = static_cast<UnsignedT>(values[i]) - unsigned_frame;
    ++h[i & 3][PforTypeTraits<T>::BitsRequired(offset)];
    ++(*buckets)[static_cast<size_t>(offset >> shift)];
  }
  for (int b = 0; b <= 64; ++b) {
    (*width_hist)[b] = h[0][b] + h[1][b] + h[2][b] + h[3][b];
  }
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

/// \brief The extremes of a run of values.
template <typename T>
struct MinMax {
  T min = 0;
  T max = 0;
};

/// \brief A frame of reference together with the width that suits it.
template <typename T>
struct FrameChoice {
  T frame_of_reference = 0;
  uint8_t bit_width = 0;
  PforConstants::ExceptionCountType num_exceptions = 0;
  int64_t cost_bits = std::numeric_limits<int64_t>::max();
};

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
/// The minimum-frame answer is always among the candidates, and it alone is
/// costed from a real histogram, so the search can never do worse than what the
/// old cost model would have picked.
template <typename T>
FrameChoice<T> ChooseFrameAndWidth(const T* values, int32_t num_elements,
                                   MinMax<T> bounds) {
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  constexpr uint8_t max_bits = PforTypeTraits<T>::kMaxBitWidth;

  const T min_val = bounds.min;
  const T max_val = bounds.max;

  // A constant vector is already at the floor, and min/max has just proved it
  // constant, so it needs no histogram to find that out. Worth the special case
  // for its own sake: a run of equal values sends every element to one
  // histogram bin, where the read-modify-write serializes and the pass runs at
  // a fraction of its usual rate.
  const auto range = static_cast<UnsignedT>(max_val) - static_cast<UnsignedT>(min_val);
  if (range == 0) {
    FrameChoice<T> constant_choice;
    constant_choice.frame_of_reference = min_val;
    constant_choice.cost_bits = 0;
    return constant_choice;
  }

  const auto range_bits = PforTypeTraits<T>::BitsRequired(range);
  const int32_t shift = range_bits > kFrameSearchBits ? range_bits - kFrameSearchBits : 0;

  // One walk serves both halves of the search: the bit-width histogram costs
  // the minimum as a frame, the bucket counts cost every other frame.
  std::array<int32_t, 65> histogram{};
  std::array<int32_t, kFrameSearchBuckets + 1> counts{};
  BuildFrameSearchHistograms<T>(values, num_elements, min_val, shift, &histogram,
                                &counts);

  // Candidate 0: the minimum, i.e. what PFOR has always done. Costed
  // unconditionally, and from a real histogram, so the search cannot regress
  // against it.
  FrameChoice<T> best;
  best.frame_of_reference = min_val;
  int64_t cost_bits = 0;
  BitWidthResult r = BestWidthFromHistogram<T>(histogram, num_elements, &cost_bits);
  best.bit_width = r.bit_width;
  best.num_exceptions = r.num_exceptions;
  best.cost_bits = cost_bits;

  // Already at width 0, with a handful of patches carrying the rest. Nothing a
  // frame can do about it. Note this is not the same as having no exceptions:
  // the whole point of a frame above the minimum is to trade a narrower width
  // for a few patches, so an exception-free choice is where the search starts,
  // not a reason to skip it. The sawtooth is the example -- its differences pack
  // at width 12 with no exceptions, or at width 0 with five, and only the second
  // is worth having.
  if (best.bit_width == 0) return best;

  const int32_t num_buckets = static_cast<int32_t>(range >> shift) + 1;
  std::array<int32_t, kFrameSearchBuckets + 2> prefix{};
  for (int32_t b = 0; b < num_buckets; ++b) prefix[b + 1] = prefix[b] + counts[b];

  // Slide a 2^w-wide window over the buckets and keep the position that costs
  // least. Widths below the bucket size cannot be resolved at this granularity,
  // and once one window spans every bucket there are no exceptions left to
  // remove, so the loop covers only the ~kFrameSearchBits widths in between --
  // fixed work, and none of it touching the data again.
  //
  // What comes out of this is a frame, not a width. Only whole buckets count as
  // covered, so `w` here is an upper bound on the width the frame really needs,
  // and the exception count an upper bound too. The exact pass below is what
  // turns the frame into a plan.
  //
  // Seeded with the incumbent's cost, so a window only registers if it beats
  // the minimum as a frame. Everything after this loop -- the walk that lowers
  // the frame onto a real value, and the exact pass that turns it into a width
  // -- is then skipped entirely on a column the frame cannot help, which is
  // most of them. Seeding it costs the conservative direction: the scan
  // over-counts exceptions, so it can decline a frame whose exact cost would
  // have won, but it cannot accept one that loses.
  int32_t best_start = -1;
  int32_t best_end = 0;
  int64_t scan_cost = best.cost_bits;
  for (int32_t w = shift; w <= max_bits; ++w) {
    const int64_t whole_buckets = static_cast<int64_t>(1) << (w - shift);
    if (whole_buckets <= 0) break;
    const int32_t k =
        static_cast<int32_t>(whole_buckets < num_buckets ? whole_buckets : num_buckets);
    for (int32_t s = 0; s < num_buckets; ++s) {
      const int32_t end = s + k < num_buckets ? s + k : num_buckets;
      const int64_t exceptions = num_elements - (prefix[end] - prefix[s]);
      if (exceptions > PforConstants::kMaxVectorSize) continue;
      const int64_t cost =
          static_cast<int64_t>(num_elements) * w + exceptions * ExceptionBits<T>();
      if (cost < scan_cost) {
        scan_cost = cost;
        best_start = s;
        best_end = end;
      }
    }
    if (k >= num_buckets) break;  // one window already spans the data
  }

  if (best_start < 0) return best;

  // Lower the frame from the boundary of the winning window onto the smallest
  // value the window actually covers. Bucket boundaries stand 2^shift apart,
  // which on a wide column is thousands, and a cluster sitting just above one
  // would otherwise pay those bits for nothing.
  //
  // A walk of its own, rather than per-bucket minima kept by the pass above:
  // tracking them there costs every vector a compare and a store per element,
  // including the vectors where the scan finds nothing and the minima are
  // discarded. Over the distributions in pfor_benchmark.cc that cost about 30%
  // of encode throughput to improve one of them. Here only a vector the search has
  // already won pays, and it pays one traversal.
  const auto window_lo = static_cast<UnsignedT>(best_start) << shift;
  // A window reaching the last bucket has no upper edge to test against: the
  // edge would be num_buckets << shift, which is one past the representable
  // range whenever the offsets span the whole type.
  const bool bounded_above = best_end < num_buckets;
  const UnsignedT window_hi =
      bounded_above ? (static_cast<UnsignedT>(best_end) << shift) : UnsignedT{0};

  const auto unsigned_min = static_cast<UnsignedT>(min_val);
  UnsignedT frame_offset = 0;
  bool covers_anything = false;
  for (int32_t i = 0; i < num_elements; ++i) {
    const UnsignedT offset = static_cast<UnsignedT>(values[i]) - unsigned_min;
    if (offset < window_lo || (bounded_above && offset >= window_hi)) continue;
    if (!covers_anything || offset < frame_offset) {
      frame_offset = offset;
      covers_anything = true;
    }
  }
  if (!covers_anything) return best;

  const auto scan_frame = static_cast<T>(unsigned_min + frame_offset);
  if (scan_frame == min_val) return best;

  // Cost the winning frame exactly. This pass is not bookkeeping -- it is where
  // the width and the exception count are actually decided. The scan works at
  // bucket granularity and so cannot see a window narrower than one bucket,
  // which is exactly where the answers worth having tend to be: the sawtooth's
  // differences span 12 bits, so its buckets are 16 wide, and no scan over them
  // can resolve the 0-bit window that its five patches leave behind.
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

/// \brief Overload for callers that have not already seen the values.
template <typename T>
FrameChoice<T> ChooseFrameAndWidth(const T* values, int32_t num_elements) {
  MinMax<T> bounds{values[0], values[0]};
  for (int32_t i = 1; i < num_elements; ++i) {
    if (values[i] < bounds.min) bounds.min = values[i];
    if (values[i] > bounds.max) bounds.max = values[i];
  }
  return ChooseFrameAndWidth<T>(values, num_elements, bounds);
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
/// The bounds of the differences come back with them: this walk has just seen
/// every one, and the search needs them before it can do anything else.
template <typename T>
void ComputeDeltas(const T* values, int32_t num_elements, T* deltas, MinMax<T>* bounds) {
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  deltas[0] = 0;
  *bounds = MinMax<T>{0, 0};
  for (int32_t i = 1; i < num_elements; ++i) {
    const UnsignedT d =
        static_cast<UnsignedT>(values[i]) - static_cast<UnsignedT>(values[i - 1]);
    T signed_delta;
    std::memcpy(&signed_delta, &d, sizeof(T));
    deltas[i] = signed_delta;
    if (signed_delta < bounds->min) bounds->min = signed_delta;
    if (signed_delta > bounds->max) bounds->max = signed_delta;
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

  MinMax<T> delta_bounds;
  ComputeDeltas<T>(values, num_elements, delta_scratch, &delta_bounds);

  // Both candidates get the full search. A cheaper gate on the spread of the
  // differences was tried first and had to go: the sawtooth is a tight cluster
  // of small positive differences with a handful of large negative ones, so its
  // span is as wide as the raw span while its cost is a fraction of it. Any
  // gate that reads a span rather than a distribution throws away the one shape
  // the mode is here for.
  const FrameChoice<T> delta =
      ChooseFrameAndWidth<T>(delta_scratch, num_elements, delta_bounds);

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

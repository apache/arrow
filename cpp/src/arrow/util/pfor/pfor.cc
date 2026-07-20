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

// Core PFOR (Patched Frame of Reference) compression implementation
//
// Implementation notes:
//   - Vector size: 1024
//   - Max exceptions: int16
//   - Exception values: original integers (not FOR offsets)
//   - Bit packing: Arrow's BitWriter/unpack

#include "arrow/util/pfor/pfor.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <span>

#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/endian.h"
#include "arrow/util/fastlanes/fastlanes_kernels.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace pfor {

static_assert(ARROW_LITTLE_ENDIAN,
              "PFOR serialization assumes little-endian byte order");

// ----------------------------------------------------------------------
// FindOptimalBitWidth: histogram-based cost model

template <typename T>
BitWidthResult PforCompression<T>::FindOptimalBitWidth(const UnsignedT* deltas,
                                                       int32_t num_elements) {
  constexpr uint8_t max_bits = PforTypeTraits<T>::kMaxBitWidth;
  constexpr int32_t position_bits = 16;
  constexpr int32_t value_bits = sizeof(T) * 8;

  // Build histogram: histogram[b] = count of deltas requiring exactly b bits
  std::array<int32_t, 65> histogram{};
  for (int32_t i = 0; i < num_elements; ++i) {
    uint8_t bits = PforTypeTraits<T>::BitsRequired(deltas[i]);
    histogram[bits]++;
  }

  // Evaluate each candidate bit width
  int64_t best_cost = std::numeric_limits<int64_t>::max();
  uint8_t best_bit_width = max_bits;
  int16_t best_num_exceptions = 0;

  int64_t exceptions_above = num_elements;

  for (uint8_t b = 0; b <= max_bits; ++b) {
    exceptions_above -= histogram[b];

    if (exceptions_above > std::numeric_limits<int16_t>::max()) {
      continue;
    }

    int64_t packing_cost = static_cast<int64_t>(num_elements) * b;
    int64_t exception_cost = exceptions_above * (position_bits + value_bits);
    int64_t total_cost = packing_cost + exception_cost;

    if (total_cost < best_cost) {
      best_cost = total_cost;
      best_bit_width = b;
      best_num_exceptions = static_cast<int16_t>(exceptions_above);
    }
  }

  return {best_bit_width, best_num_exceptions};
}

namespace {

// Dispatch by runtime bit_width into the templated FastLanes pack/unpack
// kernels. Only used when use_fastlanes && num_elements == kPforVectorSize
// (the FastLanes block size matches PFOR's vector size of 1024).
inline void FastLanesPackBlockDispatch(uint8_t bit_width, const uint32_t* in,
                                       uint32_t* out) {
  switch (bit_width) {
#define CASE_W(N) \
  case N:         \
    fastlanes::PackBlock<N>(in, out); \
    break
    CASE_W(1);  CASE_W(2);  CASE_W(3);  CASE_W(4);  CASE_W(5);  CASE_W(6);
    CASE_W(7);  CASE_W(8);  CASE_W(9);  CASE_W(10); CASE_W(11); CASE_W(12);
    CASE_W(13); CASE_W(14); CASE_W(15); CASE_W(16); CASE_W(17); CASE_W(18);
    CASE_W(19); CASE_W(20); CASE_W(21); CASE_W(22); CASE_W(23); CASE_W(24);
    CASE_W(25); CASE_W(26); CASE_W(27); CASE_W(28); CASE_W(29); CASE_W(30);
    CASE_W(31); CASE_W(32);
#undef CASE_W
    default: break;
  }
}

inline void FastLanesUnpackBlockDispatch(uint8_t bit_width, const uint32_t* packed,
                                          uint32_t* out) {
  switch (bit_width) {
#define CASE_W(N) \
  case N:         \
    fastlanes::UnpackBlock<N>(packed, out); \
    break
    CASE_W(1);  CASE_W(2);  CASE_W(3);  CASE_W(4);  CASE_W(5);  CASE_W(6);
    CASE_W(7);  CASE_W(8);  CASE_W(9);  CASE_W(10); CASE_W(11); CASE_W(12);
    CASE_W(13); CASE_W(14); CASE_W(15); CASE_W(16); CASE_W(17); CASE_W(18);
    CASE_W(19); CASE_W(20); CASE_W(21); CASE_W(22); CASE_W(23); CASE_W(24);
    CASE_W(25); CASE_W(26); CASE_W(27); CASE_W(28); CASE_W(29); CASE_W(30);
    CASE_W(31); CASE_W(32);
#undef CASE_W
    default: break;
  }
}

}  // namespace

// ----------------------------------------------------------------------
// EncodeVector

template <typename T>
PforEncodedVector<T> PforCompression<T>::EncodeVector(const T* values,
                                                      int32_t num_elements,
                                                      PackingMode mode) {
  ARROW_DCHECK(num_elements > 0);

  // Step 1: Find min (frame of reference)
  T min_val = values[0];
  for (int32_t i = 1; i < num_elements; ++i) {
    if (values[i] < min_val) min_val = values[i];
  }

  // Step 2: Compute unsigned deltas
  const auto unsigned_min = static_cast<UnsignedT>(min_val);
  std::vector<UnsignedT> deltas(num_elements);
  for (int32_t i = 0; i < num_elements; ++i) {
    deltas[i] = static_cast<UnsignedT>(values[i]) - unsigned_min;
  }

  // Step 3: Find optimal bit width
  auto [bit_width, num_exceptions] =
      FindOptimalBitWidth(deltas.data(), num_elements);

  // FastLanes (either variant) only applies when the vector matches the
  // FastLanes block size (1024) and the type is 32-bit. For shorter tails or
  // 64-bit values, fall back to PackingMode::BitPack.
  const bool fastlanes_ok =
      (mode == PackingMode::FastLanes || mode == PackingMode::FastLanesOrdered) &&
      num_elements == static_cast<int32_t>(fastlanes::kBlockSize) && sizeof(T) == 4;
  const PackingMode effective_mode = fastlanes_ok ? mode : PackingMode::BitPack;

  // Step 4: Collect exceptions and replace with placeholder (0)
  PforEncodedVector<T> result;
  result.set_info(
      PforVectorInfo<T>(min_val, bit_width, num_exceptions, effective_mode));

  if (num_exceptions > 0) {
    result.mutable_exception_positions().reserve(num_exceptions);
    result.mutable_exception_values().reserve(num_exceptions);

    UnsignedT mask = (bit_width >= PforTypeTraits<T>::kMaxBitWidth)
                         ? static_cast<UnsignedT>(-1)
                         : (static_cast<UnsignedT>(1) << bit_width) - 1;

    for (int32_t i = 0; i < num_elements; ++i) {
      if (deltas[i] > mask) {
        result.mutable_exception_positions().push_back(static_cast<int16_t>(i));
        result.mutable_exception_values().push_back(values[i]);
        deltas[i] = 0;
      }
    }
  }

  // Step 5: Bit-pack the deltas
  if (bit_width > 0) {
    int64_t packed_size =
        bit_util::BytesForBits(static_cast<int64_t>(num_elements) * bit_width);
    result.mutable_packed_values().resize(static_cast<size_t>(packed_size), 0);

    if (effective_mode == PackingMode::FastLanes ||
        effective_mode == PackingMode::FastLanesOrdered) {
      // Both variants pack with FastLanes' lane-interleaved kernel (payload is
      // exactly 128 * bit_width bytes, same as the BitPack size). The only
      // difference is value placement: FastLanes applies the FL_ORDER reorder
      // (gather deltas[fromTransposed32(t)]); FastLanesOrdered keeps original
      // order (no gather), so decode returns flat output with no inverse gather.
      alignas(64) uint32_t block[fastlanes::kBlockSize];
      if (effective_mode == PackingMode::FastLanes) {
        for (size_t t = 0; t < fastlanes::kBlockSize; ++t) {
          block[t] = static_cast<uint32_t>(deltas[fastlanes::fromTransposed32(t)]);
        }
      } else {
        for (size_t i = 0; i < fastlanes::kBlockSize; ++i) {
          block[i] = static_cast<uint32_t>(deltas[i]);
        }
      }
      FastLanesPackBlockDispatch(
          bit_width, block,
          reinterpret_cast<uint32_t*>(result.mutable_packed_values().data()));
    } else {
      bit_util::BitWriter writer(result.mutable_packed_values().data(),
                                 static_cast<int>(packed_size));
      for (int32_t i = 0; i < num_elements; ++i) {
        writer.PutValue(static_cast<uint64_t>(deltas[i]), bit_width);
      }
      writer.Flush();
    }
  }

  return result;
}

// ----------------------------------------------------------------------
// DecodeVector

template <typename T>
Result<int64_t> PforCompression<T>::DecodeVector(T* values,
                                                  std::span<const uint8_t> data,
                                                  int32_t num_elements,
                                                  OutputOrder order) {
  // Step 1: Read vector info
  ARROW_ASSIGN_OR_RAISE(auto info, PforVectorInfo<T>::Load(data));
  const uint8_t* read_ptr = data.data() + PforVectorInfo<T>::kStoredSize;

  // OutputOrder::Transposed is only meaningful for FastLanes-encoded vectors.
  // BitPack vectors have no transposition to skip, so they always emit flat
  // output regardless of `order`.
  const bool emit_transposed =
      (order == OutputOrder::Transposed) &&
      (info.packing_mode() == PackingMode::FastLanes);

  // Step 2: Handle constant data (bit_width == 0, no exceptions)
  if (info.bit_width() == 0 && info.num_exceptions() == 0) {
    std::fill(values, values + num_elements, info.frame_of_reference());
    return PforVectorInfo<T>::kStoredSize;
  }

  // Step 3: Unpack bit-packed deltas and add FOR
  if (info.bit_width() > 0) {
    const auto unsigned_for = static_cast<UnsignedT>(info.frame_of_reference());

    const PackingMode mode = info.packing_mode();
    if (mode == PackingMode::FastLanes || mode == PackingMode::FastLanesOrdered) {
      // FastLanes-packed payload: 128 * bit_width bytes per 1024-block.
      // Unpack into lane-interleaved scratch.
      ARROW_DCHECK(num_elements ==
                   static_cast<int32_t>(fastlanes::kBlockSize));
      alignas(64) uint32_t scratch[fastlanes::kBlockSize];
      FastLanesUnpackBlockDispatch(
          info.bit_width(),
          reinterpret_cast<const uint32_t*>(read_ptr), scratch);

      if (mode == PackingMode::FastLanesOrdered) {
        // No FL_ORDER reorder was applied at encode: scratch[i] is already the
        // delta for original position i. Sequential read + sequential write,
        // flat (in-order) output at full unpack speed — no gather either side.
        for (size_t i = 0; i < fastlanes::kBlockSize; ++i) {
          values[i] = util::SafeCopy<T>(
              static_cast<UnsignedT>(scratch[i]) + unsigned_for);
        }
      } else if (emit_transposed) {
        // FastLanes, transposed output: write `values[t] = scratch[t] + FOR`
        // sequentially. Output is in FastLanes stream order, i.e. values[t]
        // corresponds to the original input at fromTransposed32(t).
        for (size_t t = 0; t < fastlanes::kBlockSize; ++t) {
          values[t] = util::SafeCopy<T>(
              static_cast<UnsignedT>(scratch[t]) + unsigned_for);
        }
      } else {
        // FastLanes, flat output: fused FL_ORDER inverse + FOR-add. The gather
        // index is toTransposed32(i) (inverse of the encode-side gather). This
        // gather is what FastLanesOrdered avoids.
        for (size_t i = 0; i < fastlanes::kBlockSize; ++i) {
          const UnsignedT v =
              static_cast<UnsignedT>(scratch[fastlanes::toTransposed32(i)]);
          values[i] = util::SafeCopy<T>(v + unsigned_for);
        }
      }
    } else {
      std::vector<UnsignedT> unsigned_values(num_elements);
      // Arrow's unpack handles arbitrary sizes: SIMD for complete batches,
      // then unpack_exact for the remainder.
      arrow::internal::unpack(
          read_ptr, unsigned_values.data(),
          arrow::internal::UnpackOptions{static_cast<int>(num_elements),
                                         info.bit_width()});

      // Add FOR and convert to signed output via SafeCopy
#pragma GCC unroll PforConstants::kLoopUnrolls
#pragma GCC ivdep
      for (int32_t i = 0; i < num_elements; ++i) {
        unsigned_values[i] += unsigned_for;
        values[i] = util::SafeCopy<T>(unsigned_values[i]);
      }
    }

    int64_t packed_size =
        bit_util::BytesForBits(static_cast<int64_t>(num_elements) * info.bit_width());
    read_ptr += packed_size;
  } else {
    // bit_width == 0 but has exceptions - fill with FOR
    std::fill(values, values + num_elements, info.frame_of_reference());
  }

  // Step 4: Patch exceptions (stored as original values at FLAT positions).
  // When emitting transposed output, redirect each patch to toTransposed32(pos).
  const int16_t num_exceptions = info.num_exceptions();
  if (num_exceptions > 0) {
    const uint8_t* positions_ptr = read_ptr;
    read_ptr += num_exceptions * sizeof(int16_t);

    const uint8_t* values_ptr = read_ptr;
    read_ptr += num_exceptions * sizeof(T);

#pragma GCC unroll PforConstants::kLoopUnrolls
#pragma GCC ivdep
    for (int16_t i = 0; i < num_exceptions; ++i) {
      int16_t pos = util::SafeLoadAs<int16_t>(positions_ptr + i * sizeof(int16_t));
      T value = util::SafeLoadAs<T>(values_ptr + i * sizeof(T));
      const size_t out_pos =
          emit_transposed ? fastlanes::toTransposed32(static_cast<size_t>(pos))
                          : static_cast<size_t>(pos);
      values[out_pos] = value;
    }
  }

  return static_cast<int64_t>(read_ptr - data.data());
}

// ----------------------------------------------------------------------
// Serialization helpers

// ----------------------------------------------------------------------
// PforEncodedVectorView::LoadView

template <typename T>
Result<PforEncodedVectorView<T>> PforEncodedVectorView<T>::LoadView(
    std::span<const uint8_t> data, int32_t num_elements) {
  ARROW_ASSIGN_OR_RAISE(auto info, PforVectorInfo<T>::Load(data));

  PforEncodedVectorView<T> view;
  view.set_info(info);
  view.set_num_elements(num_elements);

  const uint8_t* ptr = data.data() + PforVectorInfo<T>::kStoredSize;

  // packed_values: zero-copy span into the buffer
  int64_t packed_size = 0;
  if (info.bit_width() > 0) {
    packed_size =
        bit_util::BytesForBits(static_cast<int64_t>(num_elements) * info.bit_width());
    view.set_packed_values(std::span<const uint8_t>(ptr, packed_size));
    ptr += packed_size;
  }

  // Exception positions and values: copy into aligned storage
  if (info.num_exceptions() > 0) {
    view.mutable_exception_positions().resize(info.num_exceptions());
    std::memcpy(view.mutable_exception_positions().data(), ptr,
                info.num_exceptions() * sizeof(int16_t));
    ptr += info.num_exceptions() * sizeof(int16_t);

    view.mutable_exception_values().resize(info.num_exceptions());
    std::memcpy(view.mutable_exception_values().data(), ptr,
                info.num_exceptions() * sizeof(T));
  }

  return view;
}

template class PforEncodedVectorView<int32_t>;
template class PforEncodedVectorView<int64_t>;

// ----------------------------------------------------------------------
// Serialization helpers

template <typename T>
int64_t PforCompression<T>::SerializedVectorSize(const PforEncodedVector<T>& vec,
                                                  int32_t num_elements) {
  int64_t size = PforVectorInfo<T>::kStoredSize;
  if (vec.info().bit_width() > 0) {
    size += bit_util::BytesForBits(
        static_cast<int64_t>(num_elements) * vec.info().bit_width());
  }
  size += vec.info().num_exceptions() * static_cast<int64_t>(sizeof(int16_t));
  size += vec.info().num_exceptions() * static_cast<int64_t>(sizeof(T));
  return size;
}

template <typename T>
int64_t PforCompression<T>::SerializeVector(const PforEncodedVector<T>& vec,
                                            int32_t num_elements,
                                            std::span<uint8_t> dest) {
  uint8_t* write_ptr = dest.data();

  // Write vector info
  vec.info().Store(std::span<uint8_t>(write_ptr, PforVectorInfo<T>::kStoredSize));
  write_ptr += PforVectorInfo<T>::kStoredSize;

  // Write packed values
  if (vec.info().bit_width() > 0 && !vec.packed_values().empty()) {
    std::memcpy(write_ptr, vec.packed_values().data(), vec.packed_values().size());
    write_ptr += vec.packed_values().size();
  }

  // Write exception positions
  if (vec.info().num_exceptions() > 0) {
    std::memcpy(write_ptr, vec.exception_positions().data(),
                vec.info().num_exceptions() * sizeof(int16_t));
    write_ptr += vec.info().num_exceptions() * sizeof(int16_t);

    // Write exception values (original integers)
    std::memcpy(write_ptr, vec.exception_values().data(),
                vec.info().num_exceptions() * sizeof(T));
    write_ptr += vec.info().num_exceptions() * sizeof(T);
  }

  return static_cast<int64_t>(write_ptr - dest.data());
}

// Explicit template instantiations
template class PforCompression<int32_t>;
template class PforCompression<int64_t>;

}  // namespace pfor
}  // namespace util
}  // namespace arrow

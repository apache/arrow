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

  // Build histogram: histogram[b] = count of deltas requiring exactly b bits.
  // Use 4 independent accumulators so the read-modify-write doesn't serialize
  // on repeated bins and the four load->clz->bump chains overlap (this loop is
  // ~half of encode time; a single histogram array runs scalar and stalls).
  std::array<int32_t, 65> h0{}, h1{}, h2{}, h3{};
  int32_t i = 0;
  for (; i + 4 <= num_elements; i += 4) {
    ++h0[PforTypeTraits<T>::BitsRequired(deltas[i])];
    ++h1[PforTypeTraits<T>::BitsRequired(deltas[i + 1])];
    ++h2[PforTypeTraits<T>::BitsRequired(deltas[i + 2])];
    ++h3[PforTypeTraits<T>::BitsRequired(deltas[i + 3])];
  }
  for (; i < num_elements; ++i) ++h0[PforTypeTraits<T>::BitsRequired(deltas[i])];
  std::array<int32_t, 65> histogram{};
  for (int b = 0; b <= 64; ++b) histogram[b] = h0[b] + h1[b] + h2[b] + h3[b];

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

// ----------------------------------------------------------------------
// EncodeVector

template <typename T>
PforEncodedVector<T> PforCompression<T>::EncodeVector(const T* values,
                                                      int32_t num_elements) {
  ARROW_DCHECK(num_elements > 0);

  // Step 1: Find min (frame of reference)
  T min_val = values[0];
  for (int32_t i = 1; i < num_elements; ++i) {
    if (values[i] < min_val) min_val = values[i];
  }

  // Step 2: Compute unsigned deltas. Use a stack scratch for the common
  // (<=vector-size) case to avoid a per-vector heap alloc + zero-init.
  const auto unsigned_min = static_cast<UnsignedT>(min_val);
  constexpr int32_t kFull = static_cast<int32_t>(PforConstants::kPforVectorSize);
  UnsignedT stack_deltas[kFull];
  std::vector<UnsignedT> heap_deltas;
  UnsignedT* deltas = stack_deltas;
  if (num_elements > kFull) {
    heap_deltas.resize(num_elements);
    deltas = heap_deltas.data();
  }
  for (int32_t i = 0; i < num_elements; ++i) {
    deltas[i] = static_cast<UnsignedT>(values[i]) - unsigned_min;
  }

  // Step 3: Find optimal bit width
  auto [bit_width, num_exceptions] = FindOptimalBitWidth(deltas, num_elements);

  // Step 4: Collect exceptions and replace with placeholder (0)
  PforEncodedVector<T> result;
  result.set_info(PforVectorInfo<T>(min_val, bit_width, num_exceptions));

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

    bit_util::BitWriter writer(result.mutable_packed_values().data(),
                               static_cast<int>(packed_size));
    for (int32_t i = 0; i < num_elements; ++i) {
      writer.PutValue(static_cast<uint64_t>(deltas[i]), bit_width);
    }
    writer.Flush();
  }

  return result;
}

// ----------------------------------------------------------------------
// DecodeVector

template <typename T>
Result<int64_t> PforCompression<T>::DecodeVector(T* values,
                                                  std::span<const uint8_t> data,
                                                  int32_t num_elements) {
  // Step 1: Read vector info
  ARROW_ASSIGN_OR_RAISE(auto info, PforVectorInfo<T>::Load(data));
  const uint8_t* read_ptr = data.data() + PforVectorInfo<T>::kStoredSize;

  // Step 2: Handle constant data (bit_width == 0, no exceptions)
  if (info.bit_width() == 0 && info.num_exceptions() == 0) {
    std::fill(values, values + num_elements, info.frame_of_reference());
    return PforVectorInfo<T>::kStoredSize;
  }

  // Step 3: Unpack bit-packed deltas and add FOR
  if (info.bit_width() > 0) {
    const auto unsigned_for = static_cast<UnsignedT>(info.frame_of_reference());

    if (unsigned_for == 0) {
      // FOR is zero: there is no bias to add, so unpack straight into the
      // output. T and UnsignedT are the same width, so the unsigned bits the
      // unpacker writes ARE the signed values — no scratch buffer and no
      // second (add-FOR) pass. This is the common case (any column whose
      // minimum is 0) and decodes at the raw unpack speed. Exceptions are
      // still patched below in Step 4.
      arrow::internal::unpack(
          read_ptr, reinterpret_cast<UnsignedT*>(values),
          arrow::internal::UnpackOptions{static_cast<int>(num_elements),
                                         info.bit_width()});
    } else {
      // FOR is non-zero: hand it to the unpacker as a bias, so the add happens
      // inside the kernel before its store and the output is traversed once.
      // The obvious alternative — unpack, then a second pass adding FOR — is
      // what this code used to do, and that pass measured 1.47x-2.40x the cost
      // of the unpack it followed (median 1.68x). A pass that only copies costs
      // the same as one that adds, so what is paid for is the extra traversal,
      // not the arithmetic; keeping the scratch buffer small enough to stay in
      // L1 (it was 4 KB on the stack) did not avoid it.
      //
      // The add is modular in UnsignedT, so the bits the unpacker stores ARE the
      // signed values, exactly as in the FOR==0 case above — no cast pass, no
      // scratch, and no aliasing question. Exceptions are patched in Step 4.
      arrow::internal::unpack_bias(read_ptr, reinterpret_cast<UnsignedT*>(values),
                                   arrow::internal::UnpackOptions{
                                       static_cast<int>(num_elements), info.bit_width()},
                                   unsigned_for);
    }

    int64_t packed_size =
        bit_util::BytesForBits(static_cast<int64_t>(num_elements) * info.bit_width());
    read_ptr += packed_size;
  } else {
    // bit_width == 0 but has exceptions - fill with FOR
    std::fill(values, values + num_elements, info.frame_of_reference());
  }

  // Step 4: Patch exceptions (stored as original values at their positions).
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
      values[static_cast<size_t>(pos)] = value;
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

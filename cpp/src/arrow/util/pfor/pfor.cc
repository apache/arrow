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
//   - Max exceptions: PforConstants::ExceptionCountType
//   - Exception values: original integers (not FOR offsets)
//   - Bit packing: Arrow's BitWriter/unpack

#include "arrow/util/pfor/pfor_internal.h"

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

namespace {

// The PFOR wire format is little-endian, so every multi-byte field converts on the
// way in and out. The bit-packed deltas need no conversion of their own:
// bit_util::BitWriter writes them little-endian and arrow::internal::unpack reads
// them back the same way, so those bytes are copied verbatim.
//
// On a little-endian host both helpers below are a plain memcpy.
template <typename T>
void StoreLittleEndianArray(const T* values, int64_t num_values, uint8_t* output) {
  if constexpr (ARROW_LITTLE_ENDIAN == 1) {
    std::memcpy(output, values, static_cast<size_t>(num_values) * sizeof(T));
  } else {
    for (int64_t i = 0; i < num_values; ++i) {
      util::SafeStore(output + i * sizeof(T), bit_util::ToLittleEndian(values[i]));
    }
  }
}

}  // namespace

// ----------------------------------------------------------------------
// FindOptimalBitWidth

template <typename T>
BitWidthResult PforCompression<T>::FindOptimalBitWidth(const UnsignedT* deltas,
                                                       int32_t num_elements) {
  std::array<int32_t, 65> histogram{};
  BuildOffsetHistogram<T>(deltas, num_elements, &histogram);
  int64_t cost_bits = 0;
  return BestWidthFromHistogram<T>(histogram, num_elements, &cost_bits);
}

// ----------------------------------------------------------------------
// EncodeVector

template <typename T>
PforEncodedVector<T> PforCompression<T>::EncodeVector(const T* values,
                                                      int32_t num_elements,
                                                      const PforEncodeOptions& options) {
  ARROW_DCHECK(num_elements > 0);

  // One scratch buffer serves both stages. ChooseVectorPlan fills it with the
  // differences, and if the plan takes them it is also the source the offsets
  // are computed from -- read at index i, written at index i, so the offsets
  // can go back into the same storage. A stack buffer for the common
  // (<=vector-size) case avoids a per-vector heap alloc and zero-init.
  constexpr int32_t kFull = static_cast<int32_t>(PforConstants::kPforVectorSize);
  T stack_scratch[kFull];
  std::vector<T> heap_scratch;
  T* scratch = stack_scratch;
  if (num_elements > kFull) {
    heap_scratch.resize(num_elements);
    scratch = heap_scratch.data();
  }

  const PforVectorPlan<T> plan =
      ChooseVectorPlan<T>(values, num_elements, scratch, options.delta_enabled);
  const T* source = plan.delta ? scratch : values;

  PforEncodedVector<T> result;
  result.set_info(PforVectorInfo<T>(plan.frame_of_reference, plan.bit_width,
                                    plan.num_exceptions, plan.delta));
  result.set_start_value(plan.start_value);

  // Step 1: reduce by the frame, collecting whatever will not fit.
  //
  // The comparison is against the packed mask in the unsigned domain, which is
  // what lets the frame sit above the minimum: a source value below the frame
  // wraps to a huge offset, fails the same test as one above the window, and is
  // patched from the exception list like any other. No second test, no sign.
  auto* offsets = reinterpret_cast<UnsignedT*>(scratch);
  const auto unsigned_frame = static_cast<UnsignedT>(plan.frame_of_reference);
  const uint8_t bit_width = plan.bit_width;

  if (plan.num_exceptions > 0) {
    result.mutable_exception_positions().reserve(plan.num_exceptions);
    result.mutable_exception_values().reserve(plan.num_exceptions);

    const UnsignedT mask = (bit_width >= PforTypeTraits<T>::kMaxBitWidth)
                               ? static_cast<UnsignedT>(-1)
                               : (static_cast<UnsignedT>(1) << bit_width) - 1;

    for (int32_t i = 0; i < num_elements; ++i) {
      const T value = source[i];
      UnsignedT offset = static_cast<UnsignedT>(value) - unsigned_frame;
      if (offset > mask) {
        result.mutable_exception_positions().push_back(
            static_cast<PforConstants::PositionType>(i));
        // The exception carries whatever the packed stream carries: a value in
        // a plain vector, a difference in a delta vector. The decoder patches
        // it in before the running sum, so a patched difference is summed like
        // any other.
        result.mutable_exception_values().push_back(value);
        offset = 0;
      }
      offsets[i] = offset;
    }
  } else {
    for (int32_t i = 0; i < num_elements; ++i) {
      offsets[i] = static_cast<UnsignedT>(source[i]) - unsigned_frame;
    }
  }

  // Step 2: bit-pack the offsets
  if (bit_width > 0) {
    int64_t packed_size =
        bit_util::BytesForBits(static_cast<int64_t>(num_elements) * bit_width);
    result.mutable_packed_values().resize(static_cast<size_t>(packed_size), 0);

    bit_util::BitWriter writer(result.mutable_packed_values().data(),
                               static_cast<int>(packed_size));
    for (int32_t i = 0; i < num_elements; ++i) {
      writer.PutValue(static_cast<uint64_t>(offsets[i]), bit_width);
    }
    writer.Flush();
  }

  return result;
}

// ----------------------------------------------------------------------
// DecodeVector

template <typename T>
Result<int64_t> PforCompression<T>::DecodeVector(std::span<const uint8_t> data,
                                                 int32_t num_elements, T* values) {
  // Step 1: Read vector info
  ARROW_ASSIGN_OR_RAISE(auto info, PforVectorInfo<T>::Load(data));
  const int64_t info_bytes = info.stored_bytes();
  if (info_bytes > static_cast<int64_t>(data.size())) {
    return Status::Invalid("PFOR delta vector needs ", info_bytes,
                           " bytes of metadata but only ", data.size(), " remain");
  }
  const uint8_t* read_ptr = data.data() + PforVectorInfo<T>::kStoredSize;

  // A delta vector stores its own first value, which is what lets it decode
  // without the vector before it -- the property the whole mode exists for.
  T start_value = 0;
  if (info.is_delta()) {
    start_value = util::SafeLoadAs<T>(read_ptr);
    read_ptr += sizeof(T);
  }

  // `values` holds num_elements slots, and everything below is sized by fields
  // that came off the wire, so check them against it before reading or writing.
  if (info.num_exceptions() > num_elements) {
    return Status::Invalid("PFOR vector has ", info.num_exceptions(),
                           " exceptions but only ", num_elements, " elements");
  }
  const int64_t packed_bytes =
      bit_util::BytesForBits(static_cast<int64_t>(num_elements) * info.bit_width());
  const int64_t exception_bytes =
      info.num_exceptions() *
      (static_cast<int64_t>(sizeof(PforConstants::PositionType)) + sizeof(T));
  if (info_bytes + packed_bytes + exception_bytes > static_cast<int64_t>(data.size())) {
    return Status::Invalid("PFOR vector needs ",
                           info_bytes + packed_bytes + exception_bytes,
                           " bytes but only ", data.size(), " remain");
  }

  // Step 2: Handle constant data (bit_width == 0, no exceptions)
  if (info.bit_width() == 0 && info.num_exceptions() == 0) {
    if (info.is_delta()) {
      // Every difference is the frame, so the values step by it from the start
      // value. Slot 0 always holds a difference of zero (see ComputeDeltas), so
      // a frame other than zero cannot occur here, but the running sum below
      // reconstructs the sequence either way rather than assuming it.
      const auto step = static_cast<UnsignedT>(info.frame_of_reference());
      auto acc = static_cast<UnsignedT>(start_value);
      for (int32_t i = 0; i < num_elements; ++i) {
        if (i > 0) acc += step;
        std::memcpy(&values[i], &acc, sizeof(T));
      }
    } else {
      std::fill(values, values + num_elements, info.frame_of_reference());
    }
    return info_bytes;
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
      arrow::internal::unpack(read_ptr, reinterpret_cast<UnsignedT*>(values),
                              arrow::internal::UnpackOptions{
                                  static_cast<int>(num_elements), info.bit_width()});
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

    read_ptr += packed_bytes;
  } else {
    // bit_width == 0 but has exceptions - fill with FOR
    std::fill(values, values + num_elements, info.frame_of_reference());
  }

  // Step 4: Patch exceptions (stored as original values at their positions).
  const PforConstants::ExceptionCountType num_exceptions = info.num_exceptions();
  if (num_exceptions > 0) {
    const uint8_t* positions_ptr = read_ptr;
    read_ptr += num_exceptions * sizeof(PforConstants::PositionType);

    const uint8_t* values_ptr = read_ptr;
    read_ptr += num_exceptions * sizeof(T);

    // Every position indexes `values`, so one past the end is an out-of-bounds
    // write. Take the maximum first: a reduction still vectorizes, where a
    // bounds check with an early return inside the patch loop would not.
    PforConstants::PositionType max_position = 0;
    for (PforConstants::ExceptionCountType i = 0; i < num_exceptions; ++i) {
      max_position = std::max(
          max_position,
          bit_util::FromLittleEndian(util::SafeLoadAs<PforConstants::PositionType>(
              positions_ptr + i * sizeof(PforConstants::PositionType))));
    }
    if (max_position >= num_elements) {
      return Status::Invalid("PFOR exception position ", max_position,
                             " is outside a vector of ", num_elements, " elements");
    }

    for (PforConstants::ExceptionCountType i = 0; i < num_exceptions; ++i) {
      PforConstants::PositionType pos =
          bit_util::FromLittleEndian(util::SafeLoadAs<PforConstants::PositionType>(
              positions_ptr + i * sizeof(PforConstants::PositionType)));
      T value =
          bit_util::FromLittleEndian(util::SafeLoadAs<T>(values_ptr + i * sizeof(T)));
      values[static_cast<size_t>(pos)] = value;
    }
  }

  // Step 5: In a delta vector, everything above produced differences. Sum them.
  //
  // This has to come after the patch: an exception in a delta vector is a
  // difference too, and summing before patching would carry the placeholder
  // zero into every value that follows.
  //
  // The sum runs in the unsigned type. Signed overflow is undefined, and a
  // column that spans the type's range will overflow -- the encoder took the
  // differences the same way, so the bits round-trip exactly.
  if (info.is_delta()) {
    auto acc = static_cast<UnsignedT>(start_value);
    for (int32_t i = 0; i < num_elements; ++i) {
      acc += static_cast<UnsignedT>(values[i]);
      std::memcpy(&values[i], &acc, sizeof(T));
    }
  }

  return static_cast<int64_t>(read_ptr - data.data());
}

// ----------------------------------------------------------------------
// Serialization helpers

template <typename T>
int64_t PforCompression<T>::SerializedVectorSize(const PforEncodedVector<T>& vec,
                                                 int32_t num_elements) {
  int64_t size = vec.info().stored_bytes();
  if (vec.info().bit_width() > 0) {
    size += bit_util::BytesForBits(static_cast<int64_t>(num_elements) *
                                   vec.info().bit_width());
  }
  size += vec.info().num_exceptions() *
          static_cast<int64_t>(sizeof(PforConstants::PositionType));
  size += vec.info().num_exceptions() * static_cast<int64_t>(sizeof(T));
  return size;
}

template <typename T>
Result<int64_t> PforCompression<T>::SerializeVector(const PforEncodedVector<T>& vec,
                                                    int32_t num_elements,
                                                    std::span<uint8_t> dest) {
  const int64_t needed = SerializedVectorSize(vec, num_elements);
  if (static_cast<int64_t>(dest.size()) < needed) {
    return Status::Invalid("PFOR vector needs ", needed, " bytes to serialize but only ",
                           dest.size(), " remain");
  }

  // `needed` is computed from bit_width and num_exceptions, while the copies
  // below take their lengths from the sections themselves, so a vector whose
  // info disagrees with its sections would write past `needed` bytes.
  const int64_t expected_packed_bytes =
      vec.info().bit_width() > 0
          ? bit_util::BytesForBits(static_cast<int64_t>(num_elements) *
                                   vec.info().bit_width())
          : 0;
  if (static_cast<int64_t>(vec.packed_values().size()) != expected_packed_bytes) {
    return Status::Invalid("PFOR vector has ", vec.packed_values().size(),
                           " packed bytes but bit_width ",
                           static_cast<int>(vec.info().bit_width()), " over ",
                           num_elements, " elements needs ", expected_packed_bytes);
  }
  if (vec.exception_positions().size() != vec.info().num_exceptions() ||
      vec.exception_values().size() != vec.info().num_exceptions()) {
    return Status::Invalid("PFOR vector claims ", vec.info().num_exceptions(),
                           " exceptions but carries ", vec.exception_positions().size(),
                           " positions and ", vec.exception_values().size(), " values");
  }

  uint8_t* write_ptr = dest.data();

  // Write vector info
  vec.info().Store(std::span<uint8_t>(write_ptr, PforVectorInfo<T>::kStoredSize));
  write_ptr += PforVectorInfo<T>::kStoredSize;

  // Write the start value of a delta vector
  if (vec.info().is_delta()) {
    util::SafeStore(write_ptr, vec.start_value());
    write_ptr += sizeof(T);
  }

  // Write packed values
  if (vec.info().bit_width() > 0) {
    std::memcpy(write_ptr, vec.packed_values().data(), vec.packed_values().size());
    write_ptr += vec.packed_values().size();
  }

  // Write exception positions
  if (vec.info().num_exceptions() > 0) {
    StoreLittleEndianArray(vec.exception_positions().data(), vec.info().num_exceptions(),
                           write_ptr);
    write_ptr += vec.info().num_exceptions() * sizeof(PforConstants::PositionType);

    // Write exception values (original integers)
    StoreLittleEndianArray(vec.exception_values().data(), vec.info().num_exceptions(),
                           write_ptr);
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

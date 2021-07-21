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

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <type_traits>
#include <utility>

#include "arrow/array/data.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/table.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/optional.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

// Visit all physical types for which sorting is implemented.
#define VISIT_PHYSICAL_TYPES(VISIT) \
  VISIT(BooleanType)                \
  VISIT(Int8Type)                   \
  VISIT(Int16Type)                  \
  VISIT(Int32Type)                  \
  VISIT(Int64Type)                  \
  VISIT(UInt8Type)                  \
  VISIT(UInt16Type)                 \
  VISIT(UInt32Type)                 \
  VISIT(UInt64Type)                 \
  VISIT(FloatType)                  \
  VISIT(DoubleType)                 \
  VISIT(BinaryType)                 \
  VISIT(LargeBinaryType)            \
  VISIT(FixedSizeBinaryType)        \
  VISIT(Decimal128Type)             \
  VISIT(Decimal256Type)

namespace {

// The target chunk in a chunked array.
template <typename ArrayType>
struct ResolvedChunk {
  using V = GetViewType<typename ArrayType::TypeClass>;
  using LogicalValueType = typename V::T;

  // The target array in chunked array.
  const ArrayType* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const ArrayType* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->IsNull(index); }

  LogicalValueType Value() const { return V::LogicalValue(array->GetView(index)); }
};

// ResolvedChunk specialization for untyped arrays when all is needed is null lookup
template <>
struct ResolvedChunk<Array> {
  // The target array in chunked array.
  const Array* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const Array* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->IsNull(index); }
};

// An object that resolves an array chunk depending on the index.
struct ChunkedArrayResolver {
  explicit ChunkedArrayResolver(const std::vector<const Array*>& chunks)
      : num_chunks_(static_cast<int64_t>(chunks.size())),
        chunks_(chunks.data()),
        offsets_(MakeEndOffsets(chunks)),
        cached_chunk_(0) {}

  template <typename ArrayType>
  ResolvedChunk<ArrayType> Resolve(int64_t index) const {
    // It is common for the algorithms below to make consecutive accesses at
    // a relatively small distance from each other, hence often falling in
    // the same chunk.
    // This is trivial when merging (assuming each side of the merge uses
    // its own resolver), but also in the inner recursive invocations of
    // partitioning.
    const bool cache_hit =
        (index >= offsets_[cached_chunk_] && index < offsets_[cached_chunk_ + 1]);
    if (ARROW_PREDICT_TRUE(cache_hit)) {
      return ResolvedChunk<ArrayType>(
          checked_cast<const ArrayType*>(chunks_[cached_chunk_]),
          index - offsets_[cached_chunk_]);
    } else {
      return ResolveMissBisect<ArrayType>(index);
    }
  }

 private:
  template <typename ArrayType>
  ResolvedChunk<ArrayType> ResolveMissBisect(int64_t index) const {
    // Like std::upper_bound(), but hand-written as it can help the compiler.
    const int64_t* raw_offsets = offsets_.data();
    // Search [lo, lo + n)
    int64_t lo = 0, n = num_chunks_;
    while (n > 1) {
      int64_t m = n >> 1;
      int64_t mid = lo + m;
      if (index >= raw_offsets[mid]) {
        lo = mid;
        n -= m;
      } else {
        n = m;
      }
    }
    cached_chunk_ = lo;
    return ResolvedChunk<ArrayType>(checked_cast<const ArrayType*>(chunks_[lo]),
                                    index - offsets_[lo]);
  }

  static std::vector<int64_t> MakeEndOffsets(const std::vector<const Array*>& chunks) {
    std::vector<int64_t> end_offsets(chunks.size() + 1);
    int64_t offset = 0;
    end_offsets[0] = 0;
    std::transform(chunks.begin(), chunks.end(), end_offsets.begin() + 1,
                   [&](const Array* chunk) {
                     offset += chunk->length();
                     return offset;
                   });
    return end_offsets;
  }

  int64_t num_chunks_;
  const Array* const* chunks_;
  std::vector<int64_t> offsets_;

  mutable int64_t cached_chunk_;
};

// We could try to reproduce the concrete Array classes' facilities
// (such as cached raw values pointer) in a separate hierarchy of
// physical accessors, but doing so ends up too cumbersome.
// Instead, we simply create the desired concrete Array objects.
std::shared_ptr<Array> GetPhysicalArray(const Array& array,
                                        const std::shared_ptr<DataType>& physical_type) {
  auto new_data = array.data()->Copy();
  new_data->type = physical_type;
  return MakeArray(std::move(new_data));
}

ArrayVector GetPhysicalChunks(const ChunkedArray& chunked_array,
                              const std::shared_ptr<DataType>& physical_type) {
  const auto& chunks = chunked_array.chunks();
  ArrayVector physical(chunks.size());
  std::transform(chunks.begin(), chunks.end(), physical.begin(),
                 [&](const std::shared_ptr<Array>& array) {
                   return GetPhysicalArray(*array, physical_type);
                 });
  return physical;
}

std::vector<const Array*> GetArrayPointers(const ArrayVector& arrays) {
  std::vector<const Array*> pointers(arrays.size());
  std::transform(arrays.begin(), arrays.end(), pointers.begin(),
                 [&](const std::shared_ptr<Array>& array) { return array.get(); });
  return pointers;
}

// NOTE: std::partition is usually faster than std::stable_partition.

struct NonStablePartitioner {
  template <typename Predicate>
  uint64_t* operator()(uint64_t* indices_begin, uint64_t* indices_end, Predicate&& pred) {
    return std::partition(indices_begin, indices_end, std::forward<Predicate>(pred));
  }
};

struct StablePartitioner {
  template <typename Predicate>
  uint64_t* operator()(uint64_t* indices_begin, uint64_t* indices_end, Predicate&& pred) {
    return std::stable_partition(indices_begin, indices_end,
                                 std::forward<Predicate>(pred));
  }
};

// TODO factor out value comparison and NaN checking?

template <typename TypeClass, typename Enable = void>
struct NullTraits {
  static constexpr bool has_null_like_values = false;
};

template <typename TypeClass>
struct NullTraits<TypeClass, enable_if_floating_point<TypeClass>> {
  static constexpr bool has_null_like_values = true;
};

// Move nulls (not null-like values) to end of array. Return where null starts.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename Partitioner>
uint64_t* PartitionNullsOnly(uint64_t* indices_begin, uint64_t* indices_end,
                             const Array& values, int64_t offset) {
  if (values.null_count() == 0) {
    return indices_end;
  }
  Partitioner partitioner;
  return partitioner(indices_begin, indices_end, [&values, &offset](uint64_t ind) {
    return !values.IsNull(ind - offset);
  });
}

// For chunked array.
template <typename Partitioner>
uint64_t* PartitionNullsOnly(uint64_t* indices_begin, uint64_t* indices_end,
                             const std::vector<const Array*>& arrays,
                             int64_t null_count) {
  if (null_count == 0) {
    return indices_end;
  }
  ChunkedArrayResolver resolver(arrays);
  Partitioner partitioner;
  return partitioner(indices_begin, indices_end, [&](uint64_t ind) {
    const auto chunk = resolver.Resolve<Array>(ind);
    return !chunk.IsNull();
  });
}

// Move non-null null-like values to end of array. Return where null-like starts.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename ArrayType, typename Partitioner>
enable_if_t<!is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const ArrayType& values, int64_t offset) {
  return indices_end;
}

// For chunked array.
template <typename ArrayType, typename Partitioner>
enable_if_t<!is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const std::vector<const Array*>& arrays, int64_t null_count) {
  return indices_end;
}

template <typename ArrayType, typename Partitioner>
enable_if_t<is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const ArrayType& values, int64_t offset) {
  Partitioner partitioner;
  return partitioner(indices_begin, indices_end, [&values, &offset](uint64_t ind) {
    return !std::isnan(values.GetView(ind - offset));
  });
}

template <typename ArrayType, typename Partitioner>
enable_if_t<is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const std::vector<const Array*>& arrays, int64_t null_count) {
  Partitioner partitioner;
  ChunkedArrayResolver resolver(arrays);
  return partitioner(indices_begin, indices_end, [&](uint64_t ind) {
    const auto chunk = resolver.Resolve<ArrayType>(ind);
    return !std::isnan(chunk.Value());
  });
}

// Move nulls to end of array. Return where null starts.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename ArrayType, typename Partitioner>
uint64_t* PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end,
                         const ArrayType& values, int64_t offset) {
  // Partition nulls at end, and null-like values just before
  uint64_t* nulls_begin =
      PartitionNullsOnly<Partitioner>(indices_begin, indices_end, values, offset);
  return PartitionNullLikes<ArrayType, Partitioner>(indices_begin, nulls_begin, values,
                                                    offset);
}

// For chunked array.
template <typename ArrayType, typename Partitioner>
uint64_t* PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end,
                         const std::vector<const Array*>& arrays, int64_t null_count) {
  // Partition nulls at end, and null-like values just before
  uint64_t* nulls_begin =
      PartitionNullsOnly<Partitioner>(indices_begin, indices_end, arrays, null_count);
  return PartitionNullLikes<ArrayType, Partitioner>(indices_begin, nulls_begin, arrays,
                                                    null_count);
}

// ----------------------------------------------------------------------
// partition_nth_indices implementation

// We need to preserve the options
using PartitionNthToIndicesState = internal::OptionsWrapper<PartitionNthOptions>;

template <typename OutType, typename InType>
struct PartitionNthToIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    using GetView = GetViewType<InType>;

    if (ctx->state() == nullptr) {
      return Status::Invalid("NthToIndices requires PartitionNthOptions");
    }

    ArrayType arr(batch[0].array());

    int64_t pivot = PartitionNthToIndicesState::Get(ctx).pivot;
    if (pivot > arr.length()) {
      return Status::IndexError("NthToIndices index out of bound");
    }
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();
    std::iota(out_begin, out_end, 0);
    if (pivot == arr.length()) {
      return Status::OK();
    }
    auto nulls_begin =
        PartitionNulls<ArrayType, NonStablePartitioner>(out_begin, out_end, arr, 0);
    auto nth_begin = out_begin + pivot;
    if (nth_begin < nulls_begin) {
      std::nth_element(out_begin, nth_begin, nulls_begin,
                       [&arr](uint64_t left, uint64_t right) {
                         const auto lval = GetView::LogicalValue(arr.GetView(left));
                         const auto rval = GetView::LogicalValue(arr.GetView(right));
                         return lval < rval;
                       });
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Array sorting implementations

template <typename ArrayType, typename VisitorNotNull, typename VisitorNull>
inline void VisitRawValuesInline(const ArrayType& values,
                                 VisitorNotNull&& visitor_not_null,
                                 VisitorNull&& visitor_null) {
  const auto data = values.raw_values();
  VisitBitBlocksVoid(
      values.null_bitmap(), values.offset(), values.length(),
      [&](int64_t i) { visitor_not_null(data[i]); }, [&]() { visitor_null(); });
}

template <typename VisitorNotNull, typename VisitorNull>
inline void VisitRawValuesInline(const BooleanArray& values,
                                 VisitorNotNull&& visitor_not_null,
                                 VisitorNull&& visitor_null) {
  if (values.null_count() != 0) {
    const uint8_t* data = values.data()->GetValues<uint8_t>(1, 0);
    VisitBitBlocksVoid(
        values.null_bitmap(), values.offset(), values.length(),
        [&](int64_t i) { visitor_not_null(BitUtil::GetBit(data, values.offset() + i)); },
        [&]() { visitor_null(); });
  } else {
    // Can avoid GetBit() overhead in the no-nulls case
    VisitBitBlocksVoid(
        values.data()->buffers[1], values.offset(), values.length(),
        [&](int64_t i) { visitor_not_null(true); }, [&]() { visitor_not_null(false); });
  }
}

template <typename ArrowType>
class ArrayCompareSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using GetView = GetViewType<ArrowType>;

 public:
  // Returns where null starts.
  //
  // `offset` is used when this is called on a chunk of a chunked array
  uint64_t* Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values,
                 int64_t offset, const ArraySortOptions& options) {
    auto nulls_begin = PartitionNulls<ArrayType, StablePartitioner>(
        indices_begin, indices_end, values, offset);
    if (options.order == SortOrder::Ascending) {
      std::stable_sort(
          indices_begin, nulls_begin, [&values, &offset](uint64_t left, uint64_t right) {
            const auto lhs = GetView::LogicalValue(values.GetView(left - offset));
            const auto rhs = GetView::LogicalValue(values.GetView(right - offset));
            return lhs < rhs;
          });
    } else {
      std::stable_sort(
          indices_begin, nulls_begin, [&values, &offset](uint64_t left, uint64_t right) {
            const auto lhs = GetView::LogicalValue(values.GetView(left - offset));
            const auto rhs = GetView::LogicalValue(values.GetView(right - offset));
            // We don't use 'left > right' here to reduce required operator.
            // If we use 'right < left' here, '<' is only required.
            return rhs < lhs;
          });
    }
    return nulls_begin;
  }
};

template <typename ArrowType>
class ArrayCountSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using c_type = typename ArrowType::c_type;

 public:
  ArrayCountSorter() = default;

  explicit ArrayCountSorter(c_type min, c_type max) { SetMinMax(min, max); }

  // Assume: max >= min && (max - min) < 4Gi
  void SetMinMax(c_type min, c_type max) {
    min_ = min;
    value_range_ = static_cast<uint32_t>(max - min) + 1;
  }

  // Returns where null starts.
  uint64_t* Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values,
                 int64_t offset, const ArraySortOptions& options) {
    // 32bit counter performs much better than 64bit one
    if (values.length() < (1LL << 32)) {
      return SortInternal<uint32_t>(indices_begin, indices_end, values, offset, options);
    } else {
      return SortInternal<uint64_t>(indices_begin, indices_end, values, offset, options);
    }
  }

 private:
  c_type min_{0};
  uint32_t value_range_{0};

  // Returns where null starts.
  //
  // `offset` is used when this is called on a chunk of a chunked array
  template <typename CounterType>
  uint64_t* SortInternal(uint64_t* indices_begin, uint64_t* indices_end,
                         const ArrayType& values, int64_t offset,
                         const ArraySortOptions& options) {
    const uint32_t value_range = value_range_;

    // first slot reserved for prefix sum
    std::vector<CounterType> counts(1 + value_range);

    if (options.order == SortOrder::Ascending) {
      VisitRawValuesInline(
          values, [&](c_type v) { ++counts[v - min_ + 1]; }, []() {});
      for (uint32_t i = 1; i <= value_range; ++i) {
        counts[i] += counts[i - 1];
      }
      auto null_position = counts[value_range];
      auto nulls_begin = indices_begin + null_position;
      int64_t index = offset;
      VisitRawValuesInline(
          values, [&](c_type v) { indices_begin[counts[v - min_]++] = index++; },
          [&]() { indices_begin[null_position++] = index++; });
      return nulls_begin;
    } else {
      VisitRawValuesInline(
          values, [&](c_type v) { ++counts[v - min_]; }, []() {});
      for (uint32_t i = value_range; i >= 1; --i) {
        counts[i - 1] += counts[i];
      }
      auto null_position = counts[0];
      auto nulls_begin = indices_begin + null_position;
      int64_t index = offset;
      VisitRawValuesInline(
          values, [&](c_type v) { indices_begin[counts[v - min_ + 1]++] = index++; },
          [&]() { indices_begin[null_position++] = index++; });
      return nulls_begin;
    }
  }
};

using ::arrow::internal::Bitmap;

template <>
class ArrayCountSorter<BooleanType> {
 public:
  ArrayCountSorter() = default;

  // Returns where null starts.
  // `offset` is used when this is called on a chunk of a chunked array
  uint64_t* Sort(uint64_t* indices_begin, uint64_t* indices_end,
                 const BooleanArray& values, int64_t offset,
                 const ArraySortOptions& options) {
    std::array<int64_t, 2> counts{0, 0};

    const int64_t nulls = values.null_count();
    const int64_t ones = values.true_count();
    const int64_t zeros = values.length() - ones - nulls;

    int64_t null_position = values.length() - nulls;
    int64_t index = offset;
    const auto nulls_begin = indices_begin + null_position;

    if (options.order == SortOrder::Ascending) {
      // ones start after zeros
      counts[1] = zeros;
    } else {
      // zeros start after ones
      counts[0] = ones;
    }
    VisitRawValuesInline(
        values, [&](bool v) { indices_begin[counts[v]++] = index++; },
        [&]() { indices_begin[null_position++] = index++; });
    return nulls_begin;
  }
};

// Sort integers with counting sort or comparison based sorting algorithm
// - Use O(n) counting sort if values are in a small range
// - Use O(nlogn) std::stable_sort otherwise
template <typename ArrowType>
class ArrayCountOrCompareSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using c_type = typename ArrowType::c_type;

 public:
  // Returns where null starts.
  //
  // `offset` is used when this is called on a chunk of a chunked array
  uint64_t* Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values,
                 int64_t offset, const ArraySortOptions& options) {
    if (values.length() >= countsort_min_len_ && values.length() > values.null_count()) {
      c_type min, max;
      std::tie(min, max) = GetMinMax<c_type>(*values.data());

      // For signed int32/64, (max - min) may overflow and trigger UBSAN.
      // Cast to largest unsigned type(uint64_t) before subtraction.
      if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <=
          countsort_max_range_) {
        count_sorter_.SetMinMax(min, max);
        return count_sorter_.Sort(indices_begin, indices_end, values, offset, options);
      }
    }

    return compare_sorter_.Sort(indices_begin, indices_end, values, offset, options);
  }

 private:
  ArrayCompareSorter<ArrowType> compare_sorter_;
  ArrayCountSorter<ArrowType> count_sorter_;

  // Cross point to prefer counting sort than stl::stable_sort(merge sort)
  // - array to be sorted is longer than "count_min_len_"
  // - value range (max-min) is within "count_max_range_"
  //
  // The optimal setting depends heavily on running CPU. Below setting is
  // conservative to adapt to various hardware and keep code simple.
  // It's possible to decrease array-len and/or increase value-range to cover
  // more cases, or setup a table for best array-len/value-range combinations.
  // See https://issues.apache.org/jira/browse/ARROW-1571 for detailed analysis.
  static const uint32_t countsort_min_len_ = 1024;
  static const uint32_t countsort_max_range_ = 4096;
};

template <typename Type, typename Enable = void>
struct ArraySorter;

template <>
struct ArraySorter<BooleanType> {
  ArrayCountSorter<BooleanType> impl;
};

template <>
struct ArraySorter<UInt8Type> {
  ArrayCountSorter<UInt8Type> impl;
  ArraySorter() : impl(0, 255) {}
};

template <>
struct ArraySorter<Int8Type> {
  ArrayCountSorter<Int8Type> impl;
  ArraySorter() : impl(-128, 127) {}
};

template <typename Type>
struct ArraySorter<Type, enable_if_t<(is_integer_type<Type>::value &&
                                      (sizeof(typename Type::c_type) > 1)) ||
                                     is_temporal_type<Type>::value>> {
  ArrayCountOrCompareSorter<Type> impl;
};

template <typename Type>
struct ArraySorter<
    Type, enable_if_t<is_floating_type<Type>::value || is_base_binary_type<Type>::value ||
                      is_fixed_size_binary_type<Type>::value>> {
  ArrayCompareSorter<Type> impl;
};

using ArraySortIndicesState = internal::OptionsWrapper<ArraySortOptions>;

template <typename OutType, typename InType>
struct ArraySortIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = ArraySortIndicesState::Get(ctx);

    ArrayType arr(batch[0].array());
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();
    std::iota(out_begin, out_end, 0);

    ArraySorter<InType> sorter;
    sorter.impl.Sort(out_begin, out_end, arr, 0, options);

    return Status::OK();
  }
};

// Sort indices kernels implemented for
//
// * Boolean type
// * Number types
// * Base binary types

template <template <typename...> class ExecTemplate>
void AddSortingKernels(VectorKernel base, VectorFunction* func) {
  // bool type
  base.signature = KernelSignature::Make({InputType::Array(boolean())}, uint64());
  base.exec = ExecTemplate<UInt64Type, BooleanType>::Exec;
  DCHECK_OK(func->AddKernel(base));

  for (const auto& ty : NumericTypes()) {
    auto physical_type = GetPhysicalType(ty);
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = GenerateNumeric<ExecTemplate, UInt64Type>(*physical_type);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto& ty : TemporalTypes()) {
    auto physical_type = GetPhysicalType(ty);
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = GenerateNumeric<ExecTemplate, UInt64Type>(*physical_type);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto id : DecimalTypeIds()) {
    base.signature = KernelSignature::Make({InputType::Array(id)}, uint64());
    base.exec = GenerateDecimal<ExecTemplate, UInt64Type>(id);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto& ty : BaseBinaryTypes()) {
    auto physical_type = GetPhysicalType(ty);
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = GenerateVarBinaryBase<ExecTemplate, UInt64Type>(*physical_type);
    DCHECK_OK(func->AddKernel(base));
  }
  base.signature =
      KernelSignature::Make({InputType::Array(Type::FIXED_SIZE_BINARY)}, uint64());
  base.exec = ExecTemplate<UInt64Type, FixedSizeBinaryType>::Exec;
  DCHECK_OK(func->AddKernel(base));
}

// ----------------------------------------------------------------------
// ChunkedArray sorting implementations

// Sort a chunked array directly without sorting each array in the
// chunked array. This is used for processing the second and following
// sort keys in TableRadixSorter.
//
// This uses the same algorithm as ArrayCompareSorter.
template <typename Type>
class ChunkedArrayCompareSorter {
  using ArrayType = typename TypeTraits<Type>::ArrayType;

 public:
  // Returns where null starts.
  uint64_t* Sort(uint64_t* indices_begin, uint64_t* indices_end,
                 const std::vector<const Array*>& arrays, int64_t null_count,
                 const ArraySortOptions& options) {
    auto nulls_begin = PartitionNulls<ArrayType, StablePartitioner>(
        indices_begin, indices_end, arrays, null_count);
    ChunkedArrayResolver resolver(arrays);
    if (options.order == SortOrder::Ascending) {
      std::stable_sort(indices_begin, nulls_begin, [&](uint64_t left, uint64_t right) {
        const auto chunk_left = resolver.Resolve<ArrayType>(left);
        const auto chunk_right = resolver.Resolve<ArrayType>(right);
        return chunk_left.Value() < chunk_right.Value();
      });
    } else {
      std::stable_sort(indices_begin, nulls_begin, [&](uint64_t left, uint64_t right) {
        const auto chunk_left = resolver.Resolve<ArrayType>(left);
        const auto chunk_right = resolver.Resolve<ArrayType>(right);
        // We don't use 'left > right' here to reduce required operator.
        // If we use 'right < left' here, '<' is only required.
        return chunk_right.Value() < chunk_left.Value();
      });
    }
    return nulls_begin;
  }
};

// Sort a chunked array by sorting each array in the chunked array.
//
// TODO: This is a naive implementation. We'll be able to improve
// performance of this. For example, we'll be able to use threads for
// sorting each array.
class ChunkedArraySorter : public TypeVisitor {
 public:
  ChunkedArraySorter(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
                     const ChunkedArray& chunked_array, const SortOrder order,
                     bool can_use_array_sorter = true)
      : TypeVisitor(),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        chunked_array_(chunked_array),
        physical_type_(GetPhysicalType(chunked_array.type())),
        physical_chunks_(GetPhysicalChunks(chunked_array_, physical_type_)),
        order_(order),
        can_use_array_sorter_(can_use_array_sorter),
        ctx_(ctx) {}

  Status Sort() { return physical_type_->Accept(this); }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) override { return SortInternal<TYPE>(); }

  VISIT_PHYSICAL_TYPES(VISIT)

#undef VISIT

 private:
  template <typename Type>
  Status SortInternal() {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    ArraySortOptions options(order_);
    const auto num_chunks = chunked_array_.num_chunks();
    if (num_chunks == 0) {
      return Status::OK();
    }
    const auto arrays = GetArrayPointers(physical_chunks_);
    if (can_use_array_sorter_) {
      // Sort each chunk independently and merge to sorted indices.
      // This is a serial implementation.
      ArraySorter<Type> sorter;
      struct SortedChunk {
        int64_t begin_offset;
        int64_t end_offset;
        int64_t nulls_offset;
      };
      std::vector<SortedChunk> sorted(num_chunks);

      // First sort all individual chunks
      int64_t begin_offset = 0;
      int64_t end_offset = 0;
      int64_t null_count = 0;
      for (int i = 0; i < num_chunks; ++i) {
        const auto array = checked_cast<const ArrayType*>(arrays[i]);
        end_offset += array->length();
        null_count += array->null_count();
        uint64_t* nulls_begin =
            sorter.impl.Sort(indices_begin_ + begin_offset, indices_begin_ + end_offset,
                             *array, begin_offset, options);
        sorted[i] = {begin_offset, end_offset, nulls_begin - indices_begin_};
        begin_offset = end_offset;
      }
      DCHECK_EQ(end_offset, indices_end_ - indices_begin_);

      std::unique_ptr<Buffer> temp_buffer;
      uint64_t* temp_indices = nullptr;
      if (sorted.size() > 1) {
        ARROW_ASSIGN_OR_RAISE(
            temp_buffer,
            AllocateBuffer(sizeof(int64_t) * (indices_end_ - indices_begin_ - null_count),
                           ctx_->memory_pool()));
        temp_indices = reinterpret_cast<uint64_t*>(temp_buffer->mutable_data());
      }

      // Then merge them by pairs, recursively
      while (sorted.size() > 1) {
        auto out_it = sorted.begin();
        auto it = sorted.begin();
        while (it < sorted.end() - 1) {
          const auto& left = *it++;
          const auto& right = *it++;
          DCHECK_EQ(left.end_offset, right.begin_offset);
          DCHECK_GE(left.nulls_offset, left.begin_offset);
          DCHECK_LE(left.nulls_offset, left.end_offset);
          DCHECK_GE(right.nulls_offset, right.begin_offset);
          DCHECK_LE(right.nulls_offset, right.end_offset);
          uint64_t* nulls_begin = Merge<ArrayType>(
              indices_begin_ + left.begin_offset, indices_begin_ + left.end_offset,
              indices_begin_ + right.end_offset, indices_begin_ + left.nulls_offset,
              indices_begin_ + right.nulls_offset, arrays, null_count, order_,
              temp_indices);
          *out_it++ = {left.begin_offset, right.end_offset, nulls_begin - indices_begin_};
        }
        if (it < sorted.end()) {
          *out_it++ = *it++;
        }
        sorted.erase(out_it, sorted.end());
      }
      DCHECK_EQ(sorted.size(), 1);
      DCHECK_EQ(sorted[0].begin_offset, 0);
      DCHECK_EQ(sorted[0].end_offset, chunked_array_.length());
      // Note that "nulls" can also include NaNs, hence the >= check
      DCHECK_GE(chunked_array_.length() - sorted[0].nulls_offset, null_count);
    } else {
      // Sort the chunked array directory.
      ChunkedArrayCompareSorter<Type> sorter;
      sorter.Sort(indices_begin_, indices_end_, arrays, chunked_array_.null_count(),
                  options);
    }
    return Status::OK();
  }

  // Merges two sorted indices arrays and returns where nulls starts.
  // Where nulls starts is used when the next merge to detect the
  // sorted indices locations.
  template <typename ArrayType>
  uint64_t* Merge(uint64_t* indices_begin, uint64_t* indices_middle,
                  uint64_t* indices_end, uint64_t* left_nulls_begin,
                  uint64_t* right_nulls_begin, const std::vector<const Array*>& arrays,
                  int64_t null_count, const SortOrder order, uint64_t* temp_indices) {
    // Input layout:
    // [left non-nulls .... left nulls .... right non-nulls .... right nulls]
    //  ^                   ^               ^                    ^
    //  |                   |               |                    |
    //  indices_begin   left_nulls_begin   indices_middle     right_nulls_begin
    auto left_num_non_nulls = left_nulls_begin - indices_begin;
    auto right_num_non_nulls = right_nulls_begin - indices_middle;

    // Mutate the input, stably, to obtain the following layout:
    // [left non-nulls .... right non-nulls .... left nulls .... right nulls]
    //  ^                   ^                    ^                    ^
    //  |                   |                    |                    |
    //  indices_begin   indices_middle        nulls_begin     right_nulls_begin
    std::rotate(left_nulls_begin, indices_middle, right_nulls_begin);
    auto nulls_begin = indices_begin + left_num_non_nulls + right_num_non_nulls;
    // If the type has null-like values (such as NaN), ensure those plus regular
    // nulls are partitioned in the right order.  Note this assumes that all
    // null-like values (e.g. NaN) are ordered equally.
    if (NullTraits<typename ArrayType::TypeClass>::has_null_like_values) {
      PartitionNullsOnly<StablePartitioner>(nulls_begin, indices_end, arrays, null_count);
    }

    // Merge the non-null values into temp area
    indices_middle = indices_begin + left_num_non_nulls;
    indices_end = indices_middle + right_num_non_nulls;
    const ChunkedArrayResolver left_resolver(arrays);
    const ChunkedArrayResolver right_resolver(arrays);
    if (order == SortOrder::Ascending) {
      std::merge(indices_begin, indices_middle, indices_middle, indices_end, temp_indices,
                 [&](uint64_t left, uint64_t right) {
                   const auto chunk_left = left_resolver.Resolve<ArrayType>(left);
                   const auto chunk_right = right_resolver.Resolve<ArrayType>(right);
                   return chunk_left.Value() < chunk_right.Value();
                 });
    } else {
      std::merge(indices_begin, indices_middle, indices_middle, indices_end, temp_indices,
                 [&](uint64_t left, uint64_t right) {
                   const auto chunk_left = left_resolver.Resolve<ArrayType>(left);
                   const auto chunk_right = right_resolver.Resolve<ArrayType>(right);
                   // We don't use 'left > right' here to reduce required
                   // operator. If we use 'right < left' here, '<' is only
                   // required.
                   return chunk_right.Value() < chunk_left.Value();
                 });
    }
    // Copy back temp area into main buffer
    std::copy(temp_indices, temp_indices + (nulls_begin - indices_begin), indices_begin);
    return nulls_begin;
  }

  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  const ChunkedArray& chunked_array_;
  const std::shared_ptr<DataType> physical_type_;
  const ArrayVector physical_chunks_;
  const SortOrder order_;
  const bool can_use_array_sorter_;
  ExecContext* ctx_;
};

// ----------------------------------------------------------------------
// Record batch sorting implementation(s)

// Visit contiguous ranges of equal values.  All entries are assumed
// to be non-null.
template <typename ArrayType, typename Visitor>
void VisitConstantRanges(const ArrayType& array, uint64_t* indices_begin,
                         uint64_t* indices_end, Visitor&& visit) {
  using GetView = GetViewType<typename ArrayType::TypeClass>;

  if (indices_begin == indices_end) {
    return;
  }
  auto range_start = indices_begin;
  auto range_cur = range_start;
  auto last_value = GetView::LogicalValue(array.GetView(*range_cur));
  while (++range_cur != indices_end) {
    auto v = GetView::LogicalValue(array.GetView(*range_cur));
    if (v != last_value) {
      visit(range_start, range_cur);
      range_start = range_cur;
      last_value = v;
    }
  }
  if (range_start != range_cur) {
    visit(range_start, range_cur);
  }
}

// A sorter for a single column of a RecordBatch, deferring to the next column
// for ranges of equal values.
class RecordBatchColumnSorter {
 public:
  explicit RecordBatchColumnSorter(RecordBatchColumnSorter* next_column = nullptr)
      : next_column_(next_column) {}
  virtual ~RecordBatchColumnSorter() {}

  virtual void SortRange(uint64_t* indices_begin, uint64_t* indices_end) = 0;

 protected:
  RecordBatchColumnSorter* next_column_;
};

template <typename Type>
class ConcreteRecordBatchColumnSorter : public RecordBatchColumnSorter {
 public:
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  ConcreteRecordBatchColumnSorter(std::shared_ptr<Array> array, SortOrder order,
                                  RecordBatchColumnSorter* next_column = nullptr)
      : RecordBatchColumnSorter(next_column),
        owned_array_(std::move(array)),
        array_(checked_cast<const ArrayType&>(*owned_array_)),
        order_(order),
        null_count_(array_.null_count()) {}

  void SortRange(uint64_t* indices_begin, uint64_t* indices_end) {
    using GetView = GetViewType<Type>;

    constexpr int64_t offset = 0;
    uint64_t* nulls_begin;
    if (null_count_ == 0) {
      nulls_begin = indices_end;
    } else {
      // NOTE that null_count_ is merely an upper bound on the number of nulls
      // in this particular range.
      nulls_begin = PartitionNullsOnly<StablePartitioner>(indices_begin, indices_end,
                                                          array_, offset);
      DCHECK_LE(indices_end - nulls_begin, null_count_);
    }
    uint64_t* null_likes_begin = PartitionNullLikes<ArrayType, StablePartitioner>(
        indices_begin, nulls_begin, array_, offset);

    // TODO This is roughly the same as ArrayCompareSorter.
    // Also, we would like to use a counting sort if possible.  This requires
    // a counting sort compatible with indirect indexing.
    if (order_ == SortOrder::Ascending) {
      std::stable_sort(
          indices_begin, null_likes_begin, [&](uint64_t left, uint64_t right) {
            const auto lhs = GetView::LogicalValue(array_.GetView(left - offset));
            const auto rhs = GetView::LogicalValue(array_.GetView(right - offset));
            return lhs < rhs;
          });
    } else {
      std::stable_sort(
          indices_begin, null_likes_begin, [&](uint64_t left, uint64_t right) {
            // We don't use 'left > right' here to reduce required operator.
            // If we use 'right < left' here, '<' is only required.
            const auto lhs = GetView::LogicalValue(array_.GetView(left - offset));
            const auto rhs = GetView::LogicalValue(array_.GetView(right - offset));
            return lhs > rhs;
          });
    }

    if (next_column_ != nullptr) {
      // Visit all ranges of equal values in this column and sort them on
      // the next column.
      SortNextColumn(null_likes_begin, nulls_begin);
      SortNextColumn(nulls_begin, indices_end);
      VisitConstantRanges(array_, indices_begin, null_likes_begin,
                          [&](uint64_t* range_start, uint64_t* range_end) {
                            SortNextColumn(range_start, range_end);
                          });
    }
  }

  void SortNextColumn(uint64_t* indices_begin, uint64_t* indices_end) {
    // Avoid the cost of a virtual method call in trivial cases
    if (indices_end - indices_begin > 1) {
      next_column_->SortRange(indices_begin, indices_end);
    }
  }

 protected:
  const std::shared_ptr<Array> owned_array_;
  const ArrayType& array_;
  const SortOrder order_;
  const int64_t null_count_;
};

// Sort a batch using a single-pass left-to-right radix sort.
class RadixRecordBatchSorter {
 public:
  RadixRecordBatchSorter(uint64_t* indices_begin, uint64_t* indices_end,
                         const RecordBatch& batch, const SortOptions& options)
      : batch_(batch),
        options_(options),
        indices_begin_(indices_begin),
        indices_end_(indices_end) {}

  Status Sort() {
    ARROW_ASSIGN_OR_RAISE(const auto sort_keys,
                          ResolveSortKeys(batch_, options_.sort_keys));

    // Create column sorters from right to left
    std::vector<std::unique_ptr<RecordBatchColumnSorter>> column_sorts(sort_keys.size());
    RecordBatchColumnSorter* next_column = nullptr;
    for (int64_t i = static_cast<int64_t>(sort_keys.size() - 1); i >= 0; --i) {
      ColumnSortFactory factory(sort_keys[i], next_column);
      ARROW_ASSIGN_OR_RAISE(column_sorts[i], factory.MakeColumnSort());
      next_column = column_sorts[i].get();
    }

    // Sort from left to right
    column_sorts.front()->SortRange(indices_begin_, indices_end_);
    return Status::OK();
  }

 protected:
  struct ResolvedSortKey {
    std::shared_ptr<Array> array;
    SortOrder order;
  };

  struct ColumnSortFactory {
    ColumnSortFactory(const ResolvedSortKey& sort_key,
                      RecordBatchColumnSorter* next_column)
        : physical_type(GetPhysicalType(sort_key.array->type())),
          array(GetPhysicalArray(*sort_key.array, physical_type)),
          order(sort_key.order),
          next_column(next_column) {}

    Result<std::unique_ptr<RecordBatchColumnSorter>> MakeColumnSort() {
      RETURN_NOT_OK(VisitTypeInline(*physical_type, this));
      DCHECK_NE(result, nullptr);
      return std::move(result);
    }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return VisitGeneric(type); }

    VISIT_PHYSICAL_TYPES(VISIT)

#undef VISIT

    Status Visit(const DataType& type) {
      return Status::TypeError("Unsupported type for RecordBatch sorting: ",
                               type.ToString());
    }

    template <typename Type>
    Status VisitGeneric(const Type&) {
      result.reset(new ConcreteRecordBatchColumnSorter<Type>(array, order, next_column));
      return Status::OK();
    }

    std::shared_ptr<DataType> physical_type;
    std::shared_ptr<Array> array;
    SortOrder order;
    RecordBatchColumnSorter* next_column;
    std::unique_ptr<RecordBatchColumnSorter> result;
  };

  static Result<std::vector<ResolvedSortKey>> ResolveSortKeys(
      const RecordBatch& batch, const std::vector<SortKey>& sort_keys) {
    std::vector<ResolvedSortKey> resolved;
    resolved.reserve(sort_keys.size());
    for (const auto& sort_key : sort_keys) {
      auto array = batch.GetColumnByName(sort_key.name);
      if (!array) {
        return Status::Invalid("Nonexistent sort key column: ", sort_key.name);
      }
      resolved.push_back({std::move(array), sort_key.order});
    }
    return resolved;
  }

  const RecordBatch& batch_;
  const SortOptions& options_;
  uint64_t* indices_begin_;
  uint64_t* indices_end_;
};

// Compare two records in the same RecordBatch or Table
// (indexing is handled through ResolvedSortKey)
template <typename ResolvedSortKey>
class MultipleKeyComparator {
 public:
  explicit MultipleKeyComparator(const std::vector<ResolvedSortKey>& sort_keys)
      : sort_keys_(sort_keys) {}

  Status status() const { return status_; }

  // Returns true if the left-th value should be ordered before the
  // right-th value, false otherwise. The start_sort_key_index-th
  // sort key and subsequent sort keys are used for comparison.
  bool Compare(uint64_t left, uint64_t right, size_t start_sort_key_index) {
    current_left_ = left;
    current_right_ = right;
    current_compared_ = 0;
    auto num_sort_keys = sort_keys_.size();
    for (size_t i = start_sort_key_index; i < num_sort_keys; ++i) {
      current_sort_key_index_ = i;
      status_ = VisitTypeInline(*sort_keys_[i].type, this);
      // If the left value equals to the right value, we need to
      // continue to sort.
      if (current_compared_ != 0) {
        break;
      }
    }
    return current_compared_ < 0;
  }

#define VISIT(TYPE)                          \
  Status Visit(const TYPE& type) {           \
    current_compared_ = CompareType<TYPE>(); \
    return Status::OK();                     \
  }

  VISIT_PHYSICAL_TYPES(VISIT)

#undef VISIT

  Status Visit(const DataType& type) {
    return Status::TypeError("Unsupported type for RecordBatch sorting: ",
                             type.ToString());
  }

 private:
  // Compares two records in the same table and returns -1, 0 or 1.
  //
  // -1: The left is less than the right.
  // 0: The left equals to the right.
  // 1: The left is greater than the right.
  //
  // This supports null and NaN. Null is processed in this and NaN
  // is processed in CompareTypeValue().
  template <typename Type>
  int32_t CompareType() {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    const auto& sort_key = sort_keys_[current_sort_key_index_];
    auto order = sort_key.order;
    const auto chunk_left = sort_key.template GetChunk<ArrayType>(current_left_);
    const auto chunk_right = sort_key.template GetChunk<ArrayType>(current_right_);
    if (sort_key.null_count > 0) {
      auto is_null_left = chunk_left.IsNull();
      auto is_null_right = chunk_right.IsNull();
      if (is_null_left && is_null_right) {
        return 0;
      } else if (is_null_left) {
        return 1;
      } else if (is_null_right) {
        return -1;
      }
    }
    return CompareTypeValue<Type>(chunk_left, chunk_right, order);
  }

  // For non-float types. Value is never NaN.
  template <typename Type>
  enable_if_t<!is_floating_type<Type>::value, int32_t> CompareTypeValue(
      const ResolvedChunk<typename TypeTraits<Type>::ArrayType>& chunk_left,
      const ResolvedChunk<typename TypeTraits<Type>::ArrayType>& chunk_right,
      const SortOrder order) {
    const auto left = chunk_left.Value();
    const auto right = chunk_right.Value();
    int32_t compared;
    if (left == right) {
      compared = 0;
    } else if (left > right) {
      compared = 1;
    } else {
      compared = -1;
    }
    if (order == SortOrder::Descending) {
      compared = -compared;
    }
    return compared;
  }

  // For float types. Value may be NaN.
  template <typename Type>
  enable_if_t<is_floating_type<Type>::value, int32_t> CompareTypeValue(
      const ResolvedChunk<typename TypeTraits<Type>::ArrayType>& chunk_left,
      const ResolvedChunk<typename TypeTraits<Type>::ArrayType>& chunk_right,
      const SortOrder order) {
    const auto left = chunk_left.Value();
    const auto right = chunk_right.Value();
    auto is_nan_left = std::isnan(left);
    auto is_nan_right = std::isnan(right);
    if (is_nan_left && is_nan_right) {
      return 0;
    } else if (is_nan_left) {
      return 1;
    } else if (is_nan_right) {
      return -1;
    }
    int32_t compared;
    if (left == right) {
      compared = 0;
    } else if (left > right) {
      compared = 1;
    } else {
      compared = -1;
    }
    if (order == SortOrder::Descending) {
      compared = -compared;
    }
    return compared;
  }

  const std::vector<ResolvedSortKey>& sort_keys_;
  Status status_;
  int64_t current_left_;
  int64_t current_right_;
  size_t current_sort_key_index_;
  int32_t current_compared_;
};

// Sort a batch using a single sort and multiple-key comparisons.
class MultipleKeyRecordBatchSorter : public TypeVisitor {
 private:
  // Preprocessed sort key.
  struct ResolvedSortKey {
    ResolvedSortKey(const std::shared_ptr<Array>& array, const SortOrder order)
        : type(GetPhysicalType(array->type())),
          owned_array(GetPhysicalArray(*array, type)),
          array(*owned_array),
          order(order),
          null_count(array->null_count()) {}

    template <typename ArrayType>
    ResolvedChunk<ArrayType> GetChunk(int64_t index) const {
      return {&checked_cast<const ArrayType&>(array), index};
    }

    const std::shared_ptr<DataType> type;
    std::shared_ptr<Array> owned_array;
    const Array& array;
    SortOrder order;
    int64_t null_count;
  };

  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  MultipleKeyRecordBatchSorter(uint64_t* indices_begin, uint64_t* indices_end,
                               const RecordBatch& batch, const SortOptions& options)
      : indices_begin_(indices_begin),
        indices_end_(indices_end),
        sort_keys_(ResolveSortKeys(batch, options.sort_keys, &status_)),
        comparator_(sort_keys_) {}

  // This is optimized for the first sort key. The first sort key sort
  // is processed in this class. The second and following sort keys
  // are processed in Comparator.
  Status Sort() {
    RETURN_NOT_OK(status_);
    return sort_keys_[0].type->Accept(this);
  }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) override { return SortInternal<TYPE>(); }

  VISIT_PHYSICAL_TYPES(VISIT)

#undef VISIT

 private:
  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const RecordBatch& batch, const std::vector<SortKey>& sort_keys, Status* status) {
    std::vector<ResolvedSortKey> resolved;
    for (const auto& sort_key : sort_keys) {
      auto array = batch.GetColumnByName(sort_key.name);
      if (!array) {
        *status = Status::Invalid("Nonexistent sort key column: ", sort_key.name);
        break;
      }
      resolved.emplace_back(array, sort_key.order);
    }
    return resolved;
  }

  template <typename Type>
  Status SortInternal() {
    using ArrayType = typename TypeTraits<Type>::ArrayType;

    auto& comparator = comparator_;
    const auto& first_sort_key = sort_keys_[0];
    const ArrayType& array = checked_cast<const ArrayType&>(first_sort_key.array);
    auto nulls_begin = indices_end_;
    nulls_begin = PartitionNullsInternal<Type>(first_sort_key);
    // Sort first-key non-nulls
    std::stable_sort(indices_begin_, nulls_begin, [&](uint64_t left, uint64_t right) {
      // Both values are never null nor NaN
      // (otherwise they've been partitioned away above).
      const auto value_left = array.GetView(left);
      const auto value_right = array.GetView(right);
      if (value_left != value_right) {
        bool compared = value_left < value_right;
        if (first_sort_key.order == SortOrder::Ascending) {
          return compared;
        } else {
          return !compared;
        }
      }
      // If the left value equals to the right value,
      // we need to compare the second and following
      // sort keys.
      return comparator.Compare(left, right, 1);
    });
    return comparator_.status();
  }

  // Behaves like PatitionNulls() but this supports multiple sort keys.
  //
  // For non-float types.
  template <typename Type>
  enable_if_t<!is_floating_type<Type>::value, uint64_t*> PartitionNullsInternal(
      const ResolvedSortKey& first_sort_key) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    if (first_sort_key.null_count == 0) {
      return indices_end_;
    }
    const ArrayType& array = checked_cast<const ArrayType&>(first_sort_key.array);
    StablePartitioner partitioner;
    auto nulls_begin = partitioner(indices_begin_, indices_end_,
                                   [&](uint64_t index) { return !array.IsNull(index); });
    // Sort all nulls by second and following sort keys
    // TODO: could we instead run an independent sort from the second key on
    // this slice?
    if (nulls_begin != indices_end_) {
      auto& comparator = comparator_;
      std::stable_sort(nulls_begin, indices_end_,
                       [&comparator](uint64_t left, uint64_t right) {
                         return comparator.Compare(left, right, 1);
                       });
    }
    return nulls_begin;
  }

  // Behaves like PatitionNulls() but this supports multiple sort keys.
  //
  // For float types.
  template <typename Type>
  enable_if_t<is_floating_type<Type>::value, uint64_t*> PartitionNullsInternal(
      const ResolvedSortKey& first_sort_key) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    const ArrayType& array = checked_cast<const ArrayType&>(first_sort_key.array);
    StablePartitioner partitioner;
    uint64_t* nulls_begin;
    if (first_sort_key.null_count == 0) {
      nulls_begin = indices_end_;
    } else {
      nulls_begin = partitioner(indices_begin_, indices_end_,
                                [&](uint64_t index) { return !array.IsNull(index); });
    }
    uint64_t* nans_and_nulls_begin =
        partitioner(indices_begin_, nulls_begin,
                    [&](uint64_t index) { return !std::isnan(array.GetView(index)); });
    auto& comparator = comparator_;
    if (nans_and_nulls_begin != nulls_begin) {
      // Sort all NaNs by the second and following sort keys.
      // TODO: could we instead run an independent sort from the second key on
      // this slice?
      std::stable_sort(nans_and_nulls_begin, nulls_begin,
                       [&comparator](uint64_t left, uint64_t right) {
                         return comparator.Compare(left, right, 1);
                       });
    }
    if (nulls_begin != indices_end_) {
      // Sort all nulls by the second and following sort keys.
      // TODO: could we instead run an independent sort from the second key on
      // this slice?
      std::stable_sort(nulls_begin, indices_end_,
                       [&comparator](uint64_t left, uint64_t right) {
                         return comparator.Compare(left, right, 1);
                       });
    }
    return nans_and_nulls_begin;
  }

  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  Status status_;
  std::vector<ResolvedSortKey> sort_keys_;
  Comparator comparator_;
};

// ----------------------------------------------------------------------
// Table sorting implementations

// Sort a table using a radix sort-like algorithm.
// A distinct stable sort is called for each sort key, from the last key to the first.
class TableRadixSorter {
 public:
  Status Sort(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
              const Table& table, const SortOptions& options) {
    for (auto i = options.sort_keys.size(); i > 0; --i) {
      const auto& sort_key = options.sort_keys[i - 1];
      const auto& chunked_array = table.GetColumnByName(sort_key.name);
      if (!chunked_array) {
        return Status::Invalid("Nonexistent sort key column: ", sort_key.name);
      }
      // We can use ArraySorter only for the sort key that is
      // processed first because ArraySorter doesn't care about
      // existing indices.
      const auto can_use_array_sorter = (i == 0);
      ChunkedArraySorter sorter(ctx, indices_begin, indices_end, *chunked_array.get(),
                                sort_key.order, can_use_array_sorter);
      ARROW_RETURN_NOT_OK(sorter.Sort());
    }
    return Status::OK();
  }
};

// Sort a table using a single sort and multiple-key comparisons.
class MultipleKeyTableSorter : public TypeVisitor {
 private:
  // TODO instead of resolving chunks for each column independently, we could
  // split the table into RecordBatches and pay the cost of chunked indexing
  // at the first column only.

  // Preprocessed sort key.
  struct ResolvedSortKey {
    ResolvedSortKey(const ChunkedArray& chunked_array, const SortOrder order)
        : order(order),
          type(GetPhysicalType(chunked_array.type())),
          chunks(GetPhysicalChunks(chunked_array, type)),
          chunk_pointers(GetArrayPointers(chunks)),
          null_count(chunked_array.null_count()),
          num_chunks(chunked_array.num_chunks()),
          resolver(chunk_pointers) {}

    // Finds the target chunk and index in the target chunk from an
    // index in chunked array.
    template <typename ArrayType>
    ResolvedChunk<ArrayType> GetChunk(int64_t index) const {
      return resolver.Resolve<ArrayType>(index);
    }

    const SortOrder order;
    const std::shared_ptr<DataType> type;
    const ArrayVector chunks;
    const std::vector<const Array*> chunk_pointers;
    const int64_t null_count;
    const int num_chunks;
    const ChunkedArrayResolver resolver;
  };

  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  MultipleKeyTableSorter(uint64_t* indices_begin, uint64_t* indices_end,
                         const Table& table, const SortOptions& options)
      : indices_begin_(indices_begin),
        indices_end_(indices_end),
        sort_keys_(ResolveSortKeys(table, options.sort_keys, &status_)),
        comparator_(sort_keys_) {}

  // This is optimized for the first sort key. The first sort key sort
  // is processed in this class. The second and following sort keys
  // are processed in Comparator.
  Status Sort() {
    ARROW_RETURN_NOT_OK(status_);
    return sort_keys_[0].type->Accept(this);
  }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) override { return SortInternal<TYPE>(); }

  VISIT_PHYSICAL_TYPES(VISIT)

#undef VISIT

 private:
  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const Table& table, const std::vector<SortKey>& sort_keys, Status* status) {
    std::vector<ResolvedSortKey> resolved;
    resolved.reserve(sort_keys.size());
    for (const auto& sort_key : sort_keys) {
      const auto& chunked_array = table.GetColumnByName(sort_key.name);
      if (!chunked_array) {
        *status = Status::Invalid("Nonexistent sort key column: ", sort_key.name);
        break;
      }
      resolved.emplace_back(*chunked_array, sort_key.order);
    }
    return resolved;
  }

  template <typename Type>
  Status SortInternal() {
    using ArrayType = typename TypeTraits<Type>::ArrayType;

    auto& comparator = comparator_;
    const auto& first_sort_key = sort_keys_[0];
    auto nulls_begin = indices_end_;
    nulls_begin = PartitionNullsInternal<Type>(first_sort_key);
    std::stable_sort(indices_begin_, nulls_begin, [&](uint64_t left, uint64_t right) {
      // Both values are never null nor NaN.
      auto chunk_left = first_sort_key.GetChunk<ArrayType>(left);
      auto chunk_right = first_sort_key.GetChunk<ArrayType>(right);
      auto value_left = chunk_left.Value();
      auto value_right = chunk_right.Value();
      if (value_left == value_right) {
        // If the left value equals to the right value,
        // we need to compare the second and following
        // sort keys.
        return comparator.Compare(left, right, 1);
      } else {
        auto compared = value_left < value_right;
        if (first_sort_key.order == SortOrder::Ascending) {
          return compared;
        } else {
          return !compared;
        }
      }
    });
    return comparator_.status();
  }

  // Behaves like PatitionNulls() but this supports multiple sort keys.
  //
  // For non-float types.
  template <typename Type>
  enable_if_t<!is_floating_type<Type>::value, uint64_t*> PartitionNullsInternal(
      const ResolvedSortKey& first_sort_key) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    if (first_sort_key.null_count == 0) {
      return indices_end_;
    }
    StablePartitioner partitioner;
    auto nulls_begin =
        partitioner(indices_begin_, indices_end_, [&first_sort_key](uint64_t index) {
          const auto chunk = first_sort_key.GetChunk<ArrayType>(index);
          return !chunk.IsNull();
        });
    DCHECK_EQ(indices_end_ - nulls_begin, first_sort_key.null_count);
    auto& comparator = comparator_;
    std::stable_sort(nulls_begin, indices_end_, [&](uint64_t left, uint64_t right) {
      return comparator.Compare(left, right, 1);
    });
    return nulls_begin;
  }

  // Behaves like PatitionNulls() but this supports multiple sort keys.
  //
  // For float types.
  template <typename Type>
  enable_if_t<is_floating_type<Type>::value, uint64_t*> PartitionNullsInternal(
      const ResolvedSortKey& first_sort_key) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    StablePartitioner partitioner;
    uint64_t* nulls_begin;
    if (first_sort_key.null_count == 0) {
      nulls_begin = indices_end_;
    } else {
      nulls_begin = partitioner(indices_begin_, indices_end_, [&](uint64_t index) {
        const auto chunk = first_sort_key.GetChunk<ArrayType>(index);
        return !chunk.IsNull();
      });
    }
    DCHECK_EQ(indices_end_ - nulls_begin, first_sort_key.null_count);
    uint64_t* nans_begin = partitioner(indices_begin_, nulls_begin, [&](uint64_t index) {
      const auto chunk = first_sort_key.GetChunk<ArrayType>(index);
      return !std::isnan(chunk.Value());
    });
    auto& comparator = comparator_;
    // Sort all NaNs by the second and following sort keys.
    std::stable_sort(nans_begin, nulls_begin, [&](uint64_t left, uint64_t right) {
      return comparator.Compare(left, right, 1);
    });
    // Sort all nulls by the second and following sort keys.
    std::stable_sort(nulls_begin, indices_end_, [&](uint64_t left, uint64_t right) {
      return comparator.Compare(left, right, 1);
    });
    return nans_begin;
  }

  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  Status status_;
  std::vector<ResolvedSortKey> sort_keys_;
  Comparator comparator_;
};

// ----------------------------------------------------------------------
// Top-level sort functions

const auto kDefaultSortOptions = SortOptions::Defaults();

const FunctionDoc sort_indices_doc(
    "Return the indices that would sort an array, record batch or table",
    ("This function computes an array of indices that define a stable sort\n"
     "of the input array, record batch or table.  Null values are considered\n"
     "greater than any other value and are therefore sorted at the end of the\n"
     "input. For floating-point types, NaNs are considered greater than any\n"
     "other non-null value, but smaller than null values."),
    {"input"}, "SortOptions");

class SortIndicesMetaFunction : public MetaFunction {
 public:
  SortIndicesMetaFunction()
      : MetaFunction("sort_indices", Arity::Unary(), &sort_indices_doc,
                     &kDefaultSortOptions) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    const SortOptions& sort_options = static_cast<const SortOptions&>(*options);
    switch (args[0].kind()) {
      case Datum::ARRAY:
        return SortIndices(*args[0].make_array(), sort_options, ctx);
        break;
      case Datum::CHUNKED_ARRAY:
        return SortIndices(*args[0].chunked_array(), sort_options, ctx);
        break;
      case Datum::RECORD_BATCH: {
        return SortIndices(*args[0].record_batch(), sort_options, ctx);
      } break;
      case Datum::TABLE:
        return SortIndices(*args[0].table(), sort_options, ctx);
        break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for sort_indices operation: "
        "values=",
        args[0].ToString());
  }

 private:
  Result<Datum> SortIndices(const Array& values, const SortOptions& options,
                            ExecContext* ctx) const {
    SortOrder order = SortOrder::Ascending;
    if (!options.sort_keys.empty()) {
      order = options.sort_keys[0].order;
    }
    ArraySortOptions array_options(order);
    return CallFunction("array_sort_indices", {values}, &array_options, ctx);
  }

  Result<Datum> SortIndices(const ChunkedArray& chunked_array, const SortOptions& options,
                            ExecContext* ctx) const {
    SortOrder order = SortOrder::Ascending;
    if (!options.sort_keys.empty()) {
      order = options.sort_keys[0].order;
    }

    auto out_type = uint64();
    auto length = chunked_array.length();
    auto buffer_size = BitUtil::BytesForBits(
        length * std::static_pointer_cast<UInt64Type>(out_type)->bit_width());
    std::vector<std::shared_ptr<Buffer>> buffers(2);
    ARROW_ASSIGN_OR_RAISE(buffers[1],
                          AllocateResizableBuffer(buffer_size, ctx->memory_pool()));
    auto out = std::make_shared<ArrayData>(out_type, length, buffers, 0);
    auto out_begin = out->GetMutableValues<uint64_t>(1);
    auto out_end = out_begin + length;
    std::iota(out_begin, out_end, 0);

    ChunkedArraySorter sorter(ctx, out_begin, out_end, chunked_array, order);
    ARROW_RETURN_NOT_OK(sorter.Sort());
    return Datum(out);
  }

  Result<Datum> SortIndices(const RecordBatch& batch, const SortOptions& options,
                            ExecContext* ctx) const {
    auto n_sort_keys = options.sort_keys.size();
    if (n_sort_keys == 0) {
      return Status::Invalid("Must specify one or more sort keys");
    }
    if (n_sort_keys == 1) {
      auto array = batch.GetColumnByName(options.sort_keys[0].name);
      if (!array) {
        return Status::Invalid("Nonexistent sort key column: ",
                               options.sort_keys[0].name);
      }
      return SortIndices(*array, options, ctx);
    }

    auto out_type = uint64();
    auto length = batch.num_rows();
    auto buffer_size = BitUtil::BytesForBits(
        length * std::static_pointer_cast<UInt64Type>(out_type)->bit_width());
    BufferVector buffers(2);
    ARROW_ASSIGN_OR_RAISE(buffers[1],
                          AllocateResizableBuffer(buffer_size, ctx->memory_pool()));
    auto out = std::make_shared<ArrayData>(out_type, length, buffers, 0);
    auto out_begin = out->GetMutableValues<uint64_t>(1);
    auto out_end = out_begin + length;
    std::iota(out_begin, out_end, 0);

    // Radix sorting is consistently faster except when there is a large number
    // of sort keys, in which case it can end up degrading catastrophically.
    // Cut off above 8 sort keys.
    if (n_sort_keys <= 8) {
      RadixRecordBatchSorter sorter(out_begin, out_end, batch, options);
      ARROW_RETURN_NOT_OK(sorter.Sort());
    } else {
      MultipleKeyRecordBatchSorter sorter(out_begin, out_end, batch, options);
      ARROW_RETURN_NOT_OK(sorter.Sort());
    }
    return Datum(out);
  }

  Result<Datum> SortIndices(const Table& table, const SortOptions& options,
                            ExecContext* ctx) const {
    auto n_sort_keys = options.sort_keys.size();
    if (n_sort_keys == 0) {
      return Status::Invalid("Must specify one or more sort keys");
    }
    if (n_sort_keys == 1) {
      auto chunked_array = table.GetColumnByName(options.sort_keys[0].name);
      if (!chunked_array) {
        return Status::Invalid("Nonexistent sort key column: ",
                               options.sort_keys[0].name);
      }
      return SortIndices(*chunked_array, options, ctx);
    }

    auto out_type = uint64();
    auto length = table.num_rows();
    auto buffer_size = BitUtil::BytesForBits(
        length * std::static_pointer_cast<UInt64Type>(out_type)->bit_width());
    std::vector<std::shared_ptr<Buffer>> buffers(2);
    ARROW_ASSIGN_OR_RAISE(buffers[1],
                          AllocateResizableBuffer(buffer_size, ctx->memory_pool()));
    auto out = std::make_shared<ArrayData>(out_type, length, buffers, 0);
    auto out_begin = out->GetMutableValues<uint64_t>(1);
    auto out_end = out_begin + length;
    std::iota(out_begin, out_end, 0);

    // TODO: We should choose suitable sort implementation
    // automatically. The current TableRadixSorter implementation is
    // faster than MultipleKeyTableSorter only when the number of
    // sort keys is 2 and counting sort is used. So we always
    // MultipleKeyTableSorter for now.
    //
    // TableRadixSorter sorter;
    // ARROW_RETURN_NOT_OK(sorter.Sort(ctx, out_begin, out_end, table, options));
    MultipleKeyTableSorter sorter(out_begin, out_end, table, options);
    ARROW_RETURN_NOT_OK(sorter.Sort());
    return Datum(out);
  }
};

const auto kDefaultArraySortOptions = ArraySortOptions::Defaults();

const FunctionDoc array_sort_indices_doc(
    "Return the indices that would sort an array",
    ("This function computes an array of indices that define a stable sort\n"
     "of the input array.  Null values are considered greater than any\n"
     "other value and are therefore sorted at the end of the array.\n"
     "For floating-point types, NaNs are considered greater than any\n"
     "other non-null value, but smaller than null values."),
    {"array"}, "ArraySortOptions");

const FunctionDoc partition_nth_indices_doc(
    "Return the indices that would partition an array around a pivot",
    ("This functions computes an array of indices that define a non-stable\n"
     "partial sort of the input array.\n"
     "\n"
     "The output is such that the `N`'th index points to the `N`'th element\n"
     "of the input in sorted order, and all indices before the `N`'th point\n"
     "to elements in the input less or equal to elements at or after the `N`'th.\n"
     "\n"
     "Null values are considered greater than any other value and are\n"
     "therefore partitioned towards the end of the array.\n"
     "For floating-point types, NaNs are considered greater than any\n"
     "other non-null value, but smaller than null values.\n"
     "\n"
     "The pivot index `N` must be given in PartitionNthOptions."),
    {"array"}, "PartitionNthOptions");

}  // namespace

void RegisterVectorSort(FunctionRegistry* registry) {
  // The kernel outputs into preallocated memory and is never null
  VectorKernel base;
  base.mem_allocation = MemAllocation::PREALLOCATE;
  base.null_handling = NullHandling::OUTPUT_NOT_NULL;

  auto array_sort_indices = std::make_shared<VectorFunction>(
      "array_sort_indices", Arity::Unary(), &array_sort_indices_doc,
      &kDefaultArraySortOptions);
  base.init = ArraySortIndicesState::Init;
  AddSortingKernels<ArraySortIndices>(base, array_sort_indices.get());
  DCHECK_OK(registry->AddFunction(std::move(array_sort_indices)));

  DCHECK_OK(registry->AddFunction(std::make_shared<SortIndicesMetaFunction>()));

  // partition_nth_indices has a parameter so needs its init function
  auto part_indices = std::make_shared<VectorFunction>(
      "partition_nth_indices", Arity::Unary(), &partition_nth_indices_doc);
  base.init = PartitionNthToIndicesState::Init;
  AddSortingKernels<PartitionNthToIndices>(base, part_indices.get());
  DCHECK_OK(registry->AddFunction(std::move(part_indices)));
}

#undef VISIT_PHYSICAL_TYPES

}  // namespace internal
}  // namespace compute
}  // namespace arrow

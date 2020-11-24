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
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/optional.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

namespace {

// The target chunk in chunked array.
template <typename ArrayType>
struct ResolvedChunk {
  using ViewType = decltype(std::declval<ArrayType>().GetView(0));

  // The target array in chunked array.
  const ArrayType* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const ArrayType* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->IsNull(index); }

  ViewType GetView() const { return array->GetView(index); }
};

// Finds the target chunk and index in the target chunk from an index
// in chunked array. `chunks` is not shared array of
// ChunkedArray::chunks() for performance.
template <typename ArrayType>
ResolvedChunk<ArrayType> ResolveChunk(const std::vector<const Array*>& chunks,
                                      int64_t index) {
  const auto num_chunks = chunks.size();
  int64_t offset = 0;
  for (size_t i = 0; i < num_chunks; ++i) {
    if (index < offset + chunks[i]->length()) {
      return ResolvedChunk<ArrayType>(checked_cast<const ArrayType*>(chunks[i]),
                                      index - offset);
    }
    offset += chunks[i]->length();
  }
  // Never reach here. `index` must be validated in caller.
  return ResolvedChunk<ArrayType>(nullptr, 0);
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

// Move nulls to end of array. Return where null starts.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename ArrayType, typename Partitioner>
enable_if_t<!is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values,
               int64_t offset) {
  if (values.null_count() == 0) {
    return indices_end;
  }
  Partitioner partitioner;
  return partitioner(indices_begin, indices_end, [&values, &offset](uint64_t ind) {
    return !values.IsNull(ind - offset);
  });
}

// For chunked array.
template <typename ArrayType, typename Partitioner>
enable_if_t<!is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end,
               const std::vector<const Array*>& arrays, int64_t null_count) {
  if (null_count == 0) {
    return indices_end;
  }
  Partitioner partitioner;
  return partitioner(indices_begin, indices_end, [&arrays](uint64_t ind) {
    const auto chunk = ResolveChunk<ArrayType>(arrays, ind);
    return !chunk.IsNull();
  });
}

// Move NaNs and nulls to end of array, nulls after NaN. Return where
// NaN/null starts.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename ArrayType, typename Partitioner>
enable_if_t<is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values,
               int64_t offset) {
  Partitioner partitioner;
  if (values.null_count() == 0) {
    return partitioner(indices_begin, indices_end, [&values, &offset](uint64_t ind) {
      return !std::isnan(values.GetView(ind - offset));
    });
  }
  uint64_t* nulls_begin =
      partitioner(indices_begin, indices_end, [&values, &offset](uint64_t ind) {
        return !values.IsNull(ind - offset) && !std::isnan(values.GetView(ind - offset));
      });
  // move nulls after NaN
  if (values.null_count() < static_cast<int64_t>(indices_end - nulls_begin)) {
    partitioner(nulls_begin, indices_end, [&values, &offset](uint64_t ind) {
      return !values.IsNull(ind - offset);
    });
  }
  return nulls_begin;
}

// For chunked array.
template <typename ArrayType, typename Partitioner>
enable_if_t<is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end,
               const std::vector<const Array*>& arrays, int64_t null_count) {
  Partitioner partitioner;
  if (null_count == 0) {
    return partitioner(indices_begin, indices_end, [&arrays](uint64_t ind) {
      const auto chunk = ResolveChunk<ArrayType>(arrays, ind);
      return !std::isnan(chunk.GetView());
    });
  }
  uint64_t* nulls_begin =
      partitioner(indices_begin, indices_end, [&arrays](uint64_t ind) {
        const auto chunk = ResolveChunk<ArrayType>(arrays, ind);
        return !chunk.IsNull() && !std::isnan(chunk.GetView());
      });
  // move nulls after NaN
  if (null_count < static_cast<int64_t>(indices_end - nulls_begin)) {
    partitioner(nulls_begin, indices_end, [&arrays](uint64_t ind) {
      const auto chunk = ResolveChunk<ArrayType>(arrays, ind);
      return !chunk.IsNull();
    });
  }
  return nulls_begin;
}

// ----------------------------------------------------------------------
// partition_nth_indices implementation

// We need to preserve the options
using PartitionNthToIndicesState = internal::OptionsWrapper<PartitionNthOptions>;

Status GetPhysicalView(const std::shared_ptr<ArrayData>& arr,
                       const std::shared_ptr<DataType>& type,
                       std::shared_ptr<ArrayData>* out) {
  if (!arr->type->Equals(*type)) {
    return ::arrow::internal::GetArrayView(arr, type).Value(out);
  } else {
    *out = arr;
    return Status::OK();
  }
}

template <typename OutType, typename InType>
struct PartitionNthToIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (ctx->state() == nullptr) {
      ctx->SetStatus(Status::Invalid("NthToIndices requires PartitionNthOptions"));
      return;
    }

    std::shared_ptr<ArrayData> arg0;
    KERNEL_RETURN_IF_ERROR(
        ctx,
        GetPhysicalView(batch[0].array(), TypeTraits<InType>::type_singleton(), &arg0));
    ArrayType arr(arg0);

    int64_t pivot = PartitionNthToIndicesState::Get(ctx).pivot;
    if (pivot > arr.length()) {
      ctx->SetStatus(Status::IndexError("NthToIndices index out of bound"));
      return;
    }
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();
    std::iota(out_begin, out_end, 0);
    if (pivot == arr.length()) {
      return;
    }
    auto nulls_begin =
        PartitionNulls<ArrayType, NonStablePartitioner>(out_begin, out_end, arr, 0);
    auto nth_begin = out_begin + pivot;
    if (nth_begin < nulls_begin) {
      std::nth_element(out_begin, nth_begin, nulls_begin,
                       [&arr](uint64_t left, uint64_t right) {
                         return arr.GetView(left) < arr.GetView(right);
                       });
    }
  }
};

template <typename ArrayType, typename VisitorNotNull, typename VisitorNull>
inline void VisitRawValuesInline(const ArrayType& values,
                                 VisitorNotNull&& visitor_not_null,
                                 VisitorNull&& visitor_null) {
  const auto data = values.raw_values();
  if (values.null_count() > 0) {
    BitmapReader reader(values.null_bitmap_data(), values.offset(), values.length());
    for (int64_t i = 0; i < values.length(); ++i) {
      if (reader.IsSet()) {
        visitor_not_null(data[i]);
      } else {
        visitor_null();
      }
      reader.Next();
    }
  } else {
    for (int64_t i = 0; i < values.length(); ++i) {
      visitor_not_null(data[i]);
    }
  }
}

template <typename ArrowType>
class ArrayCompareSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

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
            return values.GetView(left - offset) < values.GetView(right - offset);
          });
    } else {
      std::stable_sort(
          indices_begin, nulls_begin, [&values, &offset](uint64_t left, uint64_t right) {
            // We don't use 'left > right' here to reduce required operator.
            // If we use 'right < left' here, '<' is only required.
            return values.GetView(right - offset) < values.GetView(left - offset);
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
      c_type min{std::numeric_limits<c_type>::max()};
      c_type max{std::numeric_limits<c_type>::min()};

      VisitRawValuesInline(
          values,
          [&](c_type v) {
            min = std::min(min, v);
            max = std::max(max, v);
          },
          []() {});

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
struct ArraySorter<Type, enable_if_t<is_integer_type<Type>::value &&
                                     (sizeof(typename Type::c_type) > 1)>> {
  ArrayCountOrCompareSorter<Type> impl;
};

template <typename Type>
struct ArraySorter<Type, enable_if_t<is_floating_type<Type>::value ||
                                     is_base_binary_type<Type>::value>> {
  ArrayCompareSorter<Type> impl;
};

using ArraySortIndicesState = internal::OptionsWrapper<ArraySortOptions>;

template <typename OutType, typename InType>
struct ArraySortIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = ArraySortIndicesState::Get(ctx);

    std::shared_ptr<ArrayData> arg0;
    KERNEL_RETURN_IF_ERROR(
        ctx,
        GetPhysicalView(batch[0].array(), TypeTraits<InType>::type_singleton(), &arg0));
    ArrayType arr(arg0);
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();
    std::iota(out_begin, out_end, 0);

    ArraySorter<InType> sorter;
    sorter.impl.Sort(out_begin, out_end, arr, 0, options);
  }
};

// Sort indices kernels implemented for
//
// * Number types
// * Base binary types

template <template <typename...> class ExecTemplate>
void AddSortingKernels(VectorKernel base, VectorFunction* func) {
  for (const auto& ty : NumericTypes()) {
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = GenerateNumeric<ExecTemplate, UInt64Type>(*ty);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto& ty : BaseBinaryTypes()) {
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = GenerateVarBinaryBase<ExecTemplate, UInt64Type>(*ty);
    DCHECK_OK(func->AddKernel(base));
  }
}

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
    if (options.order == SortOrder::Ascending) {
      std::stable_sort(indices_begin, nulls_begin,
                       [&arrays](uint64_t left, uint64_t right) {
                         const auto chunk_left = ResolveChunk<ArrayType>(arrays, left);
                         const auto chunk_right = ResolveChunk<ArrayType>(arrays, right);
                         return chunk_left.GetView() < chunk_right.GetView();
                       });
    } else {
      std::stable_sort(indices_begin, nulls_begin,
                       [&arrays](uint64_t left, uint64_t right) {
                         const auto chunk_left = ResolveChunk<ArrayType>(arrays, left);
                         const auto chunk_right = ResolveChunk<ArrayType>(arrays, right);
                         // We don't use 'left > right' here to reduce required operator.
                         // If we use 'right < left' here, '<' is only required.
                         return chunk_right.GetView() < chunk_left.GetView();
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
  ChunkedArraySorter(uint64_t* indices_begin, uint64_t* indices_end,
                     const ChunkedArray& chunked_array, const SortOrder order,
                     bool can_use_array_sorter = true)
      : TypeVisitor(),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        chunked_array_(chunked_array),
        order_(order),
        can_use_array_sorter_(can_use_array_sorter) {}

  Status Sort() { return chunked_array_.type()->Accept(this); }

#define VISIT(TYPE) \
  Status Visit(const TYPE##Type& type) override { return SortInternal<TYPE##Type>(); }

  VISIT(Int8)
  VISIT(Int16)
  VISIT(Int32)
  VISIT(Int64)
  VISIT(UInt8)
  VISIT(UInt16)
  VISIT(UInt32)
  VISIT(UInt64)
  VISIT(Float)
  VISIT(Double)
  VISIT(String)
  VISIT(Binary)
  VISIT(LargeString)
  VISIT(LargeBinary)

#undef VISIT

 private:
  template <typename Type>
  Status SortInternal() {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    ArraySortOptions options(order_);
    const auto num_chunks = chunked_array_.num_chunks();
    const auto& shared_arrays = chunked_array_.chunks();
    std::vector<const Array*> arrays(num_chunks);
    for (int i = 0; i < num_chunks; ++i) {
      const auto& array = shared_arrays[i];
      arrays[i] = array.get();
    }
    if (can_use_array_sorter_) {
      // Sort each chunk from the beginning and merge to sorted indices.
      // This is a naive implementation.
      ArraySorter<Type> sorter;
      int64_t begin_offset = 0;
      int64_t end_offset = 0;
      int64_t null_count = 0;
      uint64_t* left_nulls_begin = indices_begin_;
      for (int i = 0; i < num_chunks; ++i) {
        const auto array = checked_cast<const ArrayType*>(arrays[i]);
        end_offset += array->length();
        null_count += array->null_count();
        uint64_t* right_nulls_begin;
        right_nulls_begin =
            sorter.impl.Sort(indices_begin_ + begin_offset, indices_begin_ + end_offset,
                             *array, begin_offset, options);
        if (i > 0) {
          left_nulls_begin = Merge<ArrayType>(
              indices_begin_, indices_begin_ + begin_offset, indices_begin_ + end_offset,
              left_nulls_begin, right_nulls_begin, arrays, null_count, order_);
        } else {
          left_nulls_begin = right_nulls_begin;
        }
        begin_offset = end_offset;
      }
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
                  int64_t null_count, const SortOrder order) {
    auto left_num_non_nulls = left_nulls_begin - indices_begin;
    auto right_num_non_nulls = right_nulls_begin - indices_middle;
    auto nulls_begin = PartitionNulls<ArrayType, StablePartitioner>(
        indices_begin, indices_end, arrays, null_count);
    indices_middle = indices_begin + left_num_non_nulls;
    indices_end = indices_middle + right_num_non_nulls;
    if (order == SortOrder::Ascending) {
      std::inplace_merge(indices_begin, indices_middle, indices_end,
                         [&arrays](uint64_t left, uint64_t right) {
                           const auto chunk_left = ResolveChunk<ArrayType>(arrays, left);
                           const auto chunk_right =
                               ResolveChunk<ArrayType>(arrays, right);
                           return chunk_left.GetView() < chunk_right.GetView();
                         });
    } else {
      std::inplace_merge(indices_begin, indices_middle, indices_end,
                         [&arrays](uint64_t left, uint64_t right) {
                           const auto chunk_left = ResolveChunk<ArrayType>(arrays, left);
                           const auto chunk_right =
                               ResolveChunk<ArrayType>(arrays, right);
                           // We don't use 'left > right' here to reduce required
                           // operator. If we use 'right < left' here, '<' is only
                           // required.
                           return chunk_right.GetView() < chunk_left.GetView();
                         });
    }
    return nulls_begin;
  }

  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  const ChunkedArray& chunked_array_;
  const SortOrder order_;
  const bool can_use_array_sorter_;
};

// Sort a table using a radix sort-like algorithm.
// A distinct stable sort is called for each sort key, from the last key to the first.
class TableRadixSorter {
 public:
  Status Sort(uint64_t* indices_begin, uint64_t* indices_end, const Table& table,
              const SortOptions& options) {
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
      ChunkedArraySorter sorter(indices_begin, indices_end, *chunked_array.get(),
                                sort_key.order, can_use_array_sorter);
      ARROW_RETURN_NOT_OK(sorter.Sort());
    }
    return Status::OK();
  }
};

// Sort a table using a single sort and multiple-key comparisons.
class MultipleKeyTableSorter : public TypeVisitor {
 private:
  // Preprocessed sort key.
  struct ResolvedSortKey {
    ResolvedSortKey(const ChunkedArray& chunked_array, const SortOrder order)
        : order(order) {
      type = chunked_array.type().get();
      null_count = chunked_array.null_count();
      num_chunks = chunked_array.num_chunks();
      for (const auto& chunk : chunked_array.chunks()) {
        chunks.push_back(chunk.get());
      }
    }

    // Finds the target chunk and index in the target chunk from an
    // index in chunked array.
    template <typename ArrayType>
    ResolvedChunk<ArrayType> GetChunk(int64_t index) const {
      return ResolveChunk<ArrayType>(chunks, index);
    }

    SortOrder order;
    DataType* type;
    int64_t null_count;
    int num_chunks;
    std::vector<const Array*> chunks;
  };

  // Compare two records in the same table.
  class Comparer : public TypeVisitor {
   public:
    Comparer(const Table& table, const std::vector<SortKey>& sort_keys)
        : TypeVisitor(),
          status_(Status::OK()),
          sort_keys_(ResolveSortKeys(table, sort_keys, &status_)) {}

    Status status() { return status_; }

    const std::vector<ResolvedSortKey>& sort_keys() { return sort_keys_; }

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
        status_ = sort_keys_[i].type->Accept(this);
        // If the left value equals to the right value, we need to
        // continue to sort.
        if (current_compared_ != 0) {
          break;
        }
      }
      return current_compared_ < 0;
    }

#define VISIT(TYPE)                                \
  Status Visit(const TYPE##Type& type) override {  \
    current_compared_ = CompareType<TYPE##Type>(); \
    return Status::OK();                           \
  }

    VISIT(Int8)
    VISIT(Int16)
    VISIT(Int32)
    VISIT(Int64)
    VISIT(UInt8)
    VISIT(UInt16)
    VISIT(UInt32)
    VISIT(UInt64)
    VISIT(Float)
    VISIT(Double)
    VISIT(String)
    VISIT(Binary)
    VISIT(LargeString)
    VISIT(LargeBinary)

#undef VISIT

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
      const auto chunk_left = sort_key.GetChunk<ArrayType>(current_left_);
      const auto chunk_right = sort_key.GetChunk<ArrayType>(current_right_);
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
      const auto left = chunk_left.GetView();
      const auto right = chunk_right.GetView();
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
      const auto left = chunk_left.GetView();
      const auto right = chunk_right.GetView();
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

    static std::vector<ResolvedSortKey> ResolveSortKeys(
        const Table& table, const std::vector<SortKey>& sort_keys, Status* status) {
      std::vector<ResolvedSortKey> resolved;
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

    Status status_;
    const std::vector<ResolvedSortKey> sort_keys_;
    int64_t current_left_;
    int64_t current_right_;
    size_t current_sort_key_index_;
    int32_t current_compared_;
  };

 public:
  MultipleKeyTableSorter(uint64_t* indices_begin, uint64_t* indices_end,
                         const Table& table, const SortOptions& options)
      : indices_begin_(indices_begin),
        indices_end_(indices_end),
        comparer_(table, options.sort_keys) {}

  // This is optimized for the first sort key. The first sort key sort
  // is processed in this class. The second and following sort keys
  // are processed in Comparer.
  Status Sort() {
    ARROW_RETURN_NOT_OK(comparer_.status());
    return comparer_.sort_keys()[0].type->Accept(this);
  }

#define VISIT(TYPE) \
  Status Visit(const TYPE##Type& type) override { return SortInternal<TYPE##Type>(); }

  VISIT(Int8)
  VISIT(Int16)
  VISIT(Int32)
  VISIT(Int64)
  VISIT(UInt8)
  VISIT(UInt16)
  VISIT(UInt32)
  VISIT(UInt64)
  VISIT(Float)
  VISIT(Double)
  VISIT(String)
  VISIT(Binary)
  VISIT(LargeString)
  VISIT(LargeBinary)

#undef VISIT

 private:
  template <typename Type>
  Status SortInternal() {
    using ArrayType = typename TypeTraits<Type>::ArrayType;

    auto& comparer = comparer_;
    const auto& first_sort_key = comparer.sort_keys()[0];
    auto nulls_begin = indices_end_;
    nulls_begin = PartitionNullsInternal<Type>(first_sort_key);
    std::stable_sort(indices_begin_, nulls_begin,
                     [&first_sort_key, &comparer](uint64_t left, uint64_t right) {
                       // Both values are never null nor NaN.
                       auto chunk_left = first_sort_key.GetChunk<ArrayType>(left);
                       auto chunk_right = first_sort_key.GetChunk<ArrayType>(right);
                       auto value_left = chunk_left.GetView();
                       auto value_right = chunk_right.GetView();
                       if (value_left == value_right) {
                         // If the left value equals to the right value,
                         // we need to compare the second and following
                         // sort keys.
                         return comparer.Compare(left, right, 1);
                       } else {
                         auto compared = value_left < value_right;
                         if (first_sort_key.order == SortOrder::Ascending) {
                           return compared;
                         } else {
                           return !compared;
                         }
                       }
                     });
    return Status::OK();
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
    auto& comparer = comparer_;
    std::stable_sort(nulls_begin, indices_end_,
                     [&comparer](uint64_t left, uint64_t right) {
                       return comparer.Compare(left, right, 1);
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
    if (first_sort_key.null_count == 0) {
      return partitioner(indices_begin_, indices_end_, [&first_sort_key](uint64_t index) {
        const auto chunk = first_sort_key.GetChunk<ArrayType>(index);
        return !std::isnan(chunk.GetView());
      });
    }
    auto nans_and_nulls_begin =
        partitioner(indices_begin_, indices_end_, [&first_sort_key](uint64_t index) {
          const auto chunk = first_sort_key.GetChunk<ArrayType>(index);
          return !chunk.IsNull() && !std::isnan(chunk.GetView());
        });
    auto nulls_begin = nans_and_nulls_begin;
    if (first_sort_key.null_count < static_cast<int64_t>(indices_end_ - nulls_begin)) {
      // move nulls after NaN
      nulls_begin = partitioner(
          nans_and_nulls_begin, indices_end_, [&first_sort_key](uint64_t index) {
            const auto chunk = first_sort_key.GetChunk<ArrayType>(index);
            return !chunk.IsNull();
          });
    }
    auto& comparer = comparer_;
    if (nans_and_nulls_begin != nulls_begin) {
      // Sort all NaNs by the second and following sort keys.
      std::stable_sort(nans_and_nulls_begin, nulls_begin,
                       [&comparer](uint64_t left, uint64_t right) {
                         return comparer.Compare(left, right, 1);
                       });
    }
    // Sort all nulls by the second and following sort keys.
    std::stable_sort(nulls_begin, indices_end_,
                     [&comparer](uint64_t left, uint64_t right) {
                       return comparer.Compare(left, right, 1);
                     });
    return nans_and_nulls_begin;
  }

  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  Comparer comparer_;
};

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
      : MetaFunction("sort_indices", Arity::Unary(), &sort_indices_doc) {}

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
        ARROW_ASSIGN_OR_RAISE(auto table,
                              Table::FromRecordBatches({args[0].record_batch()}));
        return SortIndices(*table, sort_options, ctx);
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

    ChunkedArraySorter sorter(out_begin, out_end, chunked_array, order);
    ARROW_RETURN_NOT_OK(sorter.Sort());
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
    // ARROW_RETURN_NOT_OK(sorter.Sort(out_begin, out_end, table, options));
    MultipleKeyTableSorter sorter(out_begin, out_end, table, options);
    ARROW_RETURN_NOT_OK(sorter.Sort());
    return Datum(out);
  }
};

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
      "array_sort_indices", Arity::Unary(), &array_sort_indices_doc);
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

}  // namespace internal
}  // namespace compute
}  // namespace arrow

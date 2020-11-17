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
#include <utility>

#include "arrow/array/concatenate.h"
#include "arrow/array/data.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/table.h"
#include "arrow/util/optional.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

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

// Move Nulls to end of array. Return where Null starts.
template <typename ArrayType, typename Partitioner>
enable_if_t<!is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values) {
  if (values.null_count() == 0) {
    return indices_end;
  }
  Partitioner partitioner;
  return partitioner(indices_begin, indices_end,
                     [&values](uint64_t ind) { return !values.IsNull(ind); });
}

// Move NaNs and Nulls to end of array, Nulls after NaN.
// Return where NaN/Null starts.
template <typename ArrayType, typename Partitioner>
enable_if_t<is_floating_type<typename ArrayType::TypeClass>::value, uint64_t*>
PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values) {
  Partitioner partitioner;
  if (values.null_count() == 0) {
    return partitioner(indices_begin, indices_end, [&values](uint64_t ind) {
      return !std::isnan(values.GetView(ind));
    });
  }
  uint64_t* nulls_begin =
      partitioner(indices_begin, indices_end, [&values](uint64_t ind) {
        return !values.IsNull(ind) && !std::isnan(values.GetView(ind));
      });
  // move Nulls after NaN
  if (values.null_count() < static_cast<int64_t>(indices_end - nulls_begin)) {
    partitioner(nulls_begin, indices_end,
                [&values](uint64_t ind) { return !values.IsNull(ind); });
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
        PartitionNulls<ArrayType, NonStablePartitioner>(out_begin, out_end, arr);
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
  void Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values,
            const ArraySortOptions& options) {
    std::iota(indices_begin, indices_end, 0);
    auto nulls_begin =
        PartitionNulls<ArrayType, StablePartitioner>(indices_begin, indices_end, values);
    if (options.order == SortOrder::Ascending) {
      std::stable_sort(indices_begin, nulls_begin,
                       [&values](uint64_t left, uint64_t right) {
                         return values.GetView(left) < values.GetView(right);
                       });
    } else {
      std::stable_sort(indices_begin, nulls_begin,
                       [&values](uint64_t left, uint64_t right) {
                         return values.GetView(right) < values.GetView(left);
                       });
    }
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

  void Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values,
            const ArraySortOptions& options) {
    // 32bit counter performs much better than 64bit one
    if (values.length() < (1LL << 32)) {
      SortInternal<uint32_t>(indices_begin, indices_end, values, options);
    } else {
      SortInternal<uint64_t>(indices_begin, indices_end, values, options);
    }
  }

 private:
  c_type min_{0};
  uint32_t value_range_{0};

  template <typename CounterType>
  void SortInternal(uint64_t* indices_begin, uint64_t* indices_end,
                    const ArrayType& values, const ArraySortOptions& options) {
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
      int64_t index = 0;
      VisitRawValuesInline(
          values, [&](c_type v) { indices_begin[counts[v - min_]++] = index++; },
          [&]() { indices_begin[null_position++] = index++; });
    } else {
      VisitRawValuesInline(
          values, [&](c_type v) { ++counts[v - min_]; }, []() {});
      for (uint32_t i = value_range; i >= 1; --i) {
        counts[i - 1] += counts[i];
      }
      auto null_position = counts[0];
      int64_t index = 0;
      VisitRawValuesInline(
          values, [&](c_type v) { indices_begin[counts[v - min_ + 1]++] = index++; },
          [&]() { indices_begin[null_position++] = index++; });
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
  void Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values,
            const ArraySortOptions& options) {
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
        count_sorter_.Sort(indices_begin, indices_end, values, options);
        return;
      }
    }

    compare_sorter_.Sort(indices_begin, indices_end, values, options);
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
    auto& options = ArraySortIndicesState::Get(ctx);

    std::shared_ptr<ArrayData> arg0;
    KERNEL_RETURN_IF_ERROR(
        ctx,
        GetPhysicalView(batch[0].array(), TypeTraits<InType>::type_singleton(), &arg0));
    ArrayType arr(arg0);
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();

    ArraySorter<InType> sorter;
    sorter.impl.Sort(out_begin, out_end, arr, options);
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

class TableSorter : public TypeVisitor {
 private:
  // Finds the target chunk and index in the target chunk from an
  // index in ChunkedArray.
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
    // index in ChunkedArray.
    template <typename ArrayType>
    std::pair<ArrayType*, int64_t> ResolveChunk(int64_t index) const {
      using Chunk = std::pair<ArrayType*, int64_t>;
      if (num_chunks == 1) {
        return Chunk(static_cast<ArrayType*>(chunks[0]), index);
      } else {
        int64_t offset = 0;
        for (size_t i = 0; i < num_chunks; ++i) {
          if (index < offset + chunks[i]->length()) {
            return Chunk(static_cast<ArrayType*>(chunks[i]), index - offset);
          }
          offset += chunks[i]->length();
        }
        return Chunk(nullptr, 0);
      }
    }

    SortOrder order;
    DataType* type;
    int64_t null_count;
    size_t num_chunks;
    std::vector<Array*> chunks;
  };

  class Comparer : public TypeVisitor {
   public:
    Comparer(const Table& table, const std::vector<SortKey>& sort_keys)
        : TypeVisitor(), status_(Status::OK()) {
      for (const auto& sort_key : sort_keys) {
        const auto& chunked_array = table.GetColumnByName(sort_key.name);
        if (!chunked_array) {
          status_ = Status::Invalid("Nonexistent sort key column: ", sort_key.name);
          return;
        }
        sort_keys_.emplace_back(*chunked_array, sort_key.order);
      }
    }

    Status status() { return status_; }

    const std::vector<ResolvedSortKey>& sort_keys() { return sort_keys_; }

    bool Compare(uint64_t left, uint64_t right, size_t start_sort_key_index) {
      current_left_ = left;
      current_right_ = right;
      current_compared_ = 0;
      auto num_sort_keys = sort_keys_.size();
      for (size_t i = start_sort_key_index; i < num_sort_keys; ++i) {
        current_sort_key_index_ = i;
        status_ = sort_keys_[i].type->Accept(this);
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
    template <typename Type>
    int32_t CompareType() {
      using ArrayType = typename TypeTraits<Type>::ArrayType;
      const auto& sort_key = sort_keys_[current_sort_key_index_];
      auto order = sort_key.order;
      auto chunk_left = sort_key.ResolveChunk<ArrayType>(current_left_);
      auto array_left = chunk_left.first;
      auto index_left = chunk_left.second;
      auto chunk_right = sort_key.ResolveChunk<ArrayType>(current_right_);
      auto array_right = chunk_right.first;
      auto index_right = chunk_right.second;
      if (sort_key.null_count > 0) {
        auto is_null_left = array_left->IsNull(index_left);
        auto is_null_right = array_right->IsNull(index_right);
        if (is_null_left && is_null_right) {
          return 0;
        } else if (is_null_left) {
          return 1;
        } else if (is_null_right) {
          return -1;
        }
      }
      auto left = array_left->GetView(index_left);
      auto right = array_right->GetView(index_right);
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

    Status status_;
    std::vector<ResolvedSortKey> sort_keys_;
    int64_t current_left_;
    int64_t current_right_;
    size_t current_sort_key_index_;
    int32_t current_compared_;
  };

 public:
  TableSorter(uint64_t* indices_begin, uint64_t* indices_end, const Table& table,
              const SortOptions& options)
      : indices_begin_(indices_begin),
        indices_end_(indices_end),
        comparer_(table, options.sort_keys) {}

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
    std::iota(indices_begin_, indices_end_, 0);

    auto& comparer = comparer_;
    const auto& first_sort_key = comparer.sort_keys()[0];
    auto nulls_begin = indices_end_;
    nulls_begin = PartitionNullsInternal<Type>(first_sort_key);
    std::stable_sort(indices_begin_, nulls_begin,
                     [&first_sort_key, &comparer](uint64_t left, uint64_t right) {
                       auto chunk_left = first_sort_key.ResolveChunk<ArrayType>(left);
                       auto array_left = chunk_left.first;
                       auto index_left = chunk_left.second;
                       auto chunk_right = first_sort_key.ResolveChunk<ArrayType>(right);
                       auto array_right = chunk_right.first;
                       auto index_right = chunk_right.second;
                       auto value_left = array_left->GetView(index_left);
                       auto value_right = array_right->GetView(index_right);
                       if (value_left == value_right) {
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
          auto chunk = first_sort_key.ResolveChunk<ArrayType>(index);
          auto array = chunk.first;
          auto index_array = chunk.second;
          return !array->IsNull(index_array);
        });
    auto& comparer = comparer_;
    std::stable_sort(nulls_begin, indices_end_,
                     [&comparer](uint64_t left, uint64_t right) {
                       return comparer.Compare(left, right, 1);
                     });
    return nulls_begin;
  }

  template <typename Type>
  enable_if_t<is_floating_type<Type>::value, uint64_t*> PartitionNullsInternal(
      const ResolvedSortKey& first_sort_key) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    StablePartitioner partitioner;
    if (first_sort_key.null_count == 0) {
      return partitioner(indices_begin_, indices_end_, [&first_sort_key](uint64_t index) {
        auto chunk = first_sort_key.ResolveChunk<ArrayType>(index);
        auto array = chunk.first;
        auto index_array = chunk.second;
        return !std::isnan(array->GetView(index_array));
      });
    }
    auto nans_and_nulls_begin =
        partitioner(indices_begin_, indices_end_, [&first_sort_key](uint64_t index) {
          auto chunk = first_sort_key.ResolveChunk<ArrayType>(index);
          auto array = chunk.first;
          auto index_array = chunk.second;
          return !array->IsNull(index_array) && !std::isnan(array->GetView(index_array));
        });
    auto nulls_begin = nans_and_nulls_begin;
    if (first_sort_key.null_count < static_cast<int64_t>(indices_end_ - nulls_begin)) {
      // move Nulls after NaN
      nulls_begin = partitioner(
          nans_and_nulls_begin, indices_end_, [&first_sort_key](uint64_t index) {
            auto chunk = first_sort_key.ResolveChunk<ArrayType>(index);
            auto array = chunk.first;
            auto index_array = chunk.second;
            return !array->IsNull(index_array);
          });
    }
    auto& comparer = comparer_;
    if (nans_and_nulls_begin != nulls_begin) {
      std::stable_sort(nans_and_nulls_begin, nulls_begin,
                       [&comparer](uint64_t left, uint64_t right) {
                         return comparer.Compare(left, right, 1);
                       });
    }
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
  Result<std::shared_ptr<Array>> SortIndices(const Array& values,
                                             const SortOptions& options,
                                             ExecContext* ctx) const {
    SortOrder order = SortOrder::Ascending;
    if (!options.sort_keys.empty()) {
      order = options.sort_keys[0].order;
    }
    ArraySortOptions array_options(order);
    ARROW_ASSIGN_OR_RAISE(
        Datum result, CallFunction("array_sort_indices", {values}, &array_options, ctx));
    return result.make_array();
  }

  Result<std::shared_ptr<Array>> SortIndices(const ChunkedArray& values,
                                             const SortOptions& options,
                                             ExecContext* ctx) const {
    SortOrder order = SortOrder::Ascending;
    if (!options.sort_keys.empty()) {
      order = options.sort_keys[0].order;
    }
    ArraySortOptions array_options(order);

    std::shared_ptr<Array> array_values;
    if (values.num_chunks() == 1) {
      array_values = values.chunk(0);
    } else {
      ARROW_ASSIGN_OR_RAISE(array_values,
                            Concatenate(values.chunks(), ctx->memory_pool()));
    }
    ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("array_sort_indices", {array_values},
                                                     &array_options, ctx));
    return result.make_array();
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

    TableSorter sorter(out_begin, out_end, table, options);
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

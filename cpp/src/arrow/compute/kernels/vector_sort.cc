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
#include <limits>
#include <numeric>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/optional.h"

namespace arrow {
namespace compute {

namespace {

// ----------------------------------------------------------------------
// partition_indices implementation

// We need to preserve the options
struct PartitionIndicesState : public KernelState {
  explicit PartitionIndicesState(int64_t pivot) : pivot(pivot) {}
  int64_t pivot;
};

template <typename OutType, typename InType>
struct PartitionIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayType arr(batch[0].array());

    int64_t pivot = checked_cast<const PartitionIndicesState&>(*ctx->state()).pivot;
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
    uint64_t* nulls_begin = out_end;
    if (arr.null_count()) {
      nulls_begin = std::stable_partition(
          out_begin, out_end, [&arr](uint64_t ind) { return !arr.IsNull(ind); });
    }
    auto nth_begin = out_begin + pivot;
    if (nth_begin < nulls_begin) {
      std::nth_element(out_begin, nth_begin, nulls_begin,
                       [&arr](uint64_t left, uint64_t right) {
                         return arr.GetView(left) < arr.GetView(right);
                       });
    }
  }
};

}  // namespace

template <typename ArrowType>
class CompareSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 public:
  void Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values) {
    std::iota(indices_begin, indices_end, 0);

    auto nulls_begin = indices_end;
    if (values.null_count()) {
      nulls_begin =
          std::stable_partition(indices_begin, indices_end,
                                [&values](uint64_t ind) { return !values.IsNull(ind); });
    }
    std::stable_sort(indices_begin, nulls_begin,
                     [&values](uint64_t left, uint64_t right) {
                       return values.GetView(left) < values.GetView(right);
                     });
  }
};

template <typename ArrowType>
class CountSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using c_type = typename ArrowType::c_type;

 public:
  CountSorter() = default;

  explicit CountSorter(c_type min, c_type max) { SetMinMax(min, max); }

  // Assume: max >= min && (max - min) < 4Gi
  void SetMinMax(c_type min, c_type max) {
    min_ = min;
    value_range_ = static_cast<uint32_t>(max - min) + 1;
  }

  void Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values) {
    // 32bit counter performs much better than 64bit one
    if (values.length() < (1LL << 32)) {
      SortInternal<uint32_t>(indices_begin, indices_end, values);
    } else {
      SortInternal<uint64_t>(indices_begin, indices_end, values);
    }
  }

 private:
  c_type min_{0};
  uint32_t value_range_{0};

  template <typename CounterType>
  void SortInternal(uint64_t* indices_begin, uint64_t* indices_end,
                    const ArrayType& values) {
    const uint32_t value_range = value_range_;

    // first slot reserved for prefix sum, last slot for null value
    std::vector<CounterType> counts(1 + value_range + 1);

    auto update_counts = [&](util::optional<c_type> v) {
      if (v.has_value()) {
        ++counts[*v - min_ + 1];
      } else {
        ++counts[value_range + 1];
      }
    };
    VisitArrayDataInline<ArrowType>(*values.data(), std::move(update_counts));

    for (uint32_t i = 1; i <= value_range; ++i) {
      counts[i] += counts[i - 1];
    }

    int64_t index = 0;
    auto write_index = [&](util::optional<c_type> v) {
      if (v.has_value()) {
        indices_begin[counts[*v - min_]++] = index++;
      } else {
        indices_begin[counts[value_range]++] = index++;
      }
    };
    VisitArrayDataInline<ArrowType>(*values.data(), std::move(write_index));
  }
};

// Sort integers with counting sort or comparison based sorting algorithm
// - Use O(n) counting sort if values are in a small range
// - Use O(nlogn) std::stable_sort otherwise
template <typename ArrowType>
class CountOrCompareSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using c_type = typename ArrowType::c_type;

 public:
  void Sort(uint64_t* indices_begin, uint64_t* indices_end, const ArrayType& values) {
    if (values.length() >= countsort_min_len_ && values.length() > values.null_count()) {
      c_type min{std::numeric_limits<c_type>::max()};
      c_type max{std::numeric_limits<c_type>::min()};

      auto update_minmax = [&min, &max](util::optional<c_type> v) {
        if (v.has_value()) {
          min = std::min(min, *v);
          max = std::max(max, *v);
        }
      };
      VisitArrayDataInline<ArrowType>(*values.data(), std::move(update_minmax));
      // For signed int32/64, (max - min) may overflow and trigger UBSAN.
      // Cast to largest unsigned type(uint64_t) before subtraction.
      if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <=
          countsort_max_range_) {
        count_sorter_.SetMinMax(min, max);
        count_sorter_.Sort(indices_begin, indices_end, values);
        return;
      }
    }

    compare_sorter_.Sort(indices_begin, indices_end, values);
  }

 private:
  CompareSorter<ArrowType> compare_sorter_;
  CountSorter<ArrowType> count_sorter_;

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
struct Sorter;

template <>
struct Sorter<UInt8Type> {
  CountSorter<UInt8Type> impl;
  Sorter() : impl(0, 255) {}
};

template <>
struct Sorter<Int8Type> {
  CountSorter<Int8Type> impl;
  Sorter() : impl(-128, 127) {}
};

template <typename Type>
struct Sorter<Type, enable_if_t<is_integer_type<Type>::value &&
                                (sizeof(typename Type::c_type) > 1)>> {
  CountOrCompareSorter<Type> impl;
};

template <typename Type>
struct Sorter<Type, enable_if_t<is_floating_type<Type>::value ||
                                is_base_binary_type<Type>::value>> {
  CompareSorter<Type> impl;
};

template <typename OutType, typename InType>
struct SortIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayType arr(batch[0].array());
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();

    Sorter<InType> sorter;
    sorter.impl.Sort(out_begin, out_end, arr);
  }
};

namespace internal {

// Sort indices kernels implemented for
//
// * Number types
// * Base binary types

std::unique_ptr<KernelState> InitPartitionIndices(KernelContext*,
                                                  const KernelInitArgs& args) {
  int64_t pivot = static_cast<const PartitionOptions*>(args.options)->pivot;
  return std::unique_ptr<KernelState>(new PartitionIndicesState(pivot));
}

template <template <typename...> class ExecTemplate>
void AddSortingKernels(VectorKernel base, VectorFunction* func) {
  for (const auto& ty : NumericTypes()) {
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = codegen::Numeric<ExecTemplate, UInt64Type>(*ty);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto& ty : BaseBinaryTypes()) {
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = codegen::BaseBinary<ExecTemplate, UInt64Type>(*ty);
    DCHECK_OK(func->AddKernel(base));
  }
}

void RegisterVectorSort(FunctionRegistry* registry) {
  // The kernel outputs into preallocated memory and is never null
  VectorKernel base;
  base.mem_allocation = MemAllocation::PREALLOCATE;
  base.null_handling = NullHandling::OUTPUT_NOT_NULL;

  auto sort_indices = std::make_shared<VectorFunction>("sort_indices", Arity::Unary());
  AddSortingKernels<SortIndices>(base, sort_indices.get());
  DCHECK_OK(registry->AddFunction(std::move(sort_indices)));

  // partition_indices has a parameter so needs its init function
  auto part_indices =
      std::make_shared<VectorFunction>("partition_indices", Arity::Unary());
  base.init = InitPartitionIndices;
  AddSortingKernels<PartitionIndices>(base, part_indices.get());
  DCHECK_OK(registry->AddFunction(std::move(part_indices)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

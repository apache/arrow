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

#include "arrow/compute/kernels/sort_to_indices.h"

#include <algorithm>
#include <limits>
#include <numeric>
#include <utility>
#include <vector>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/logical_type.h"
#include "arrow/type_traits.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class Array;

namespace compute {

/// \brief UnaryKernel implementing SortToIndices operation
class ARROW_EXPORT SortToIndicesKernel : public UnaryKernel {
 protected:
  std::shared_ptr<DataType> type_;

 public:
  /// \brief UnaryKernel interface
  ///
  /// delegates to subclasses via SortToIndices()
  Status Call(FunctionContext* ctx, const Datum& values, Datum* offsets) override = 0;

  /// \brief output type of this kernel
  std::shared_ptr<DataType> out_type() const override { return uint64(); }

  /// \brief single-array implementation
  virtual Status SortToIndices(FunctionContext* ctx, const std::shared_ptr<Array>& values,
                               std::shared_ptr<Array>* offsets) = 0;

  /// \brief factory for SortToIndicesKernel
  ///
  /// \param[in] value_type constructed SortToIndicesKernel will support sorting
  ///            values of this type
  /// \param[out] out created kernel
  static Status Make(const std::shared_ptr<DataType>& value_type,
                     std::unique_ptr<SortToIndicesKernel>* out);
};

template <typename ArrowType>
class CompareSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 public:
  void Sort(int64_t* indices_begin, int64_t* indices_end, const ArrayType& values) {
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

  void Sort(int64_t* indices_begin, int64_t* indices_end, const ArrayType& values) {
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
  void SortInternal(int64_t* indices_begin, int64_t* indices_end,
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
  void Sort(int64_t* indices_begin, int64_t* indices_end, const ArrayType& values) {
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
      // Cast to largest unsigned type(uint64_t) before substraction.
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

template <typename ArrowType, typename Sorter>
class SortToIndicesKernelImpl : public SortToIndicesKernel {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 public:
  explicit SortToIndicesKernelImpl(Sorter sorter) : sorter_(sorter) {}

  Status SortToIndices(FunctionContext* ctx, const std::shared_ptr<Array>& values,
                       std::shared_ptr<Array>* offsets) {
    return SortToIndicesImpl(ctx, std::static_pointer_cast<ArrayType>(values), offsets);
  }

  Status Call(FunctionContext* ctx, const Datum& values, Datum* offsets) {
    if (!values.is_array()) {
      return Status::Invalid("SortToIndicesKernel expects array values");
    }
    auto values_array = values.make_array();
    std::shared_ptr<Array> offsets_array;
    RETURN_NOT_OK(this->SortToIndices(ctx, values_array, &offsets_array));
    *offsets = offsets_array;
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const { return type_; }

 private:
  Sorter sorter_;

  Status SortToIndicesImpl(FunctionContext* ctx, const std::shared_ptr<ArrayType>& values,
                           std::shared_ptr<Array>* offsets) {
    int64_t buf_size = values->length() * sizeof(uint64_t);
    ARROW_ASSIGN_OR_RAISE(auto indices_buf, AllocateBuffer(buf_size, ctx->memory_pool()));

    int64_t* indices_begin = reinterpret_cast<int64_t*>(indices_buf->mutable_data());
    int64_t* indices_end = indices_begin + values->length();

    sorter_.Sort(indices_begin, indices_end, *values.get());
    *offsets = std::make_shared<UInt64Array>(values->length(), std::move(indices_buf));
    return Status::OK();
  }
};

template <typename ArrowType, typename Sorter = CompareSorter<ArrowType>>
static SortToIndicesKernelImpl<ArrowType, Sorter>* MakeCompareKernel() {
  return new SortToIndicesKernelImpl<ArrowType, Sorter>(Sorter());
}

template <typename ArrowType, typename Sorter = CountSorter<ArrowType>>
static SortToIndicesKernelImpl<ArrowType, Sorter>* MakeCountKernel(int min, int max) {
  return new SortToIndicesKernelImpl<ArrowType, Sorter>(Sorter(min, max));
}

template <typename ArrowType, typename Sorter = CountOrCompareSorter<ArrowType>>
static SortToIndicesKernelImpl<ArrowType, Sorter>* MakeCountOrCompareKernel() {
  return new SortToIndicesKernelImpl<ArrowType, Sorter>(Sorter());
}

Status SortToIndicesKernel::Make(const std::shared_ptr<DataType>& value_type,
                                 std::unique_ptr<SortToIndicesKernel>* out) {
  SortToIndicesKernel* kernel;
  switch (value_type->id()) {
    case Type::UINT8:
      kernel = MakeCountKernel<UInt8Type>(0, 255);
      break;
    case Type::INT8:
      kernel = MakeCountKernel<Int8Type>(-128, 127);
      break;
    case Type::UINT16:
      kernel = MakeCountOrCompareKernel<UInt16Type>();
      break;
    case Type::INT16:
      kernel = MakeCountOrCompareKernel<Int16Type>();
      break;
    case Type::UINT32:
      kernel = MakeCountOrCompareKernel<UInt32Type>();
      break;
    case Type::INT32:
      kernel = MakeCountOrCompareKernel<Int32Type>();
      break;
    case Type::UINT64:
      kernel = MakeCountOrCompareKernel<UInt64Type>();
      break;
    case Type::INT64:
      kernel = MakeCountOrCompareKernel<Int64Type>();
      break;
    case Type::FLOAT:
      kernel = MakeCompareKernel<FloatType>();
      break;
    case Type::DOUBLE:
      kernel = MakeCompareKernel<DoubleType>();
      break;
    case Type::BINARY:
      kernel = MakeCompareKernel<BinaryType>();
      break;
    case Type::STRING:
      kernel = MakeCompareKernel<StringType>();
      break;
    default:
      return Status::NotImplemented("Sorting of ", *value_type, " arrays");
  }
  out->reset(kernel);
  return Status::OK();
}

static Status SortToIndices(FunctionContext* ctx, const Datum& values, Datum* offsets) {
  std::unique_ptr<SortToIndicesKernel> kernel;
  RETURN_NOT_OK(SortToIndicesKernel::Make(values.type(), &kernel));
  return kernel->Call(ctx, values, offsets);
}

Status SortToIndices(FunctionContext* ctx, const Array& values,
                     std::shared_ptr<Array>* offsets) {
  Datum offsets_datum;
  RETURN_NOT_OK(SortToIndices(ctx, Datum(values.data()), &offsets_datum));
  *offsets = offsets_datum.make_array();
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

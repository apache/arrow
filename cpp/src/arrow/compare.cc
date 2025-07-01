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

// Functions for comparing Arrow data structures

#include "arrow/compare.h"

#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/statistics.h"
#include "arrow/buffer.h"
#include "arrow/compare_internal.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/macros.h"
#include "arrow/util/unreachable.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Public method implementations

namespace {

class TypeEqualsVisitor {
 public:
  explicit TypeEqualsVisitor(const DataType& right, bool check_metadata)
      : right_(right), check_metadata_(check_metadata), result_(false) {}

  bool MetadataEqual(const Field& left, const Field& right) {
    if (left.HasMetadata() && right.HasMetadata()) {
      return left.metadata()->Equals(*right.metadata());
    } else {
      return !left.HasMetadata() && !right.HasMetadata();
    }
  }

  Status VisitChildren(const DataType& left) {
    if (left.num_fields() != right_.num_fields()) {
      result_ = false;
      return Status::OK();
    }

    for (int i = 0; i < left.num_fields(); ++i) {
      if (!left.field(i)->Equals(right_.field(i), check_metadata_)) {
        result_ = false;
        return Status::OK();
      }
    }
    result_ = true;
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_null_type<T>::value || is_primitive_ctype<T>::value ||
                  is_base_binary_type<T>::value,
              Status>
  Visit(const T&) {
    result_ = true;
    return Status::OK();
  }

  Status Visit(const BinaryViewType&) {
    result_ = true;
    return Status::OK();
  }

  template <typename T>
  enable_if_interval<T, Status> Visit(const T& left) {
    const auto& right = checked_cast<const IntervalType&>(right_);
    result_ = right.interval_type() == left.interval_type();
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_time_type<T>::value || is_date_type<T>::value ||
                  is_duration_type<T>::value,
              Status>
  Visit(const T& left) {
    const auto& right = checked_cast<const T&>(right_);
    result_ = left.unit() == right.unit();
    return Status::OK();
  }

  Status Visit(const TimestampType& left) {
    const auto& right = checked_cast<const TimestampType&>(right_);
    result_ = left.unit() == right.unit() && left.timezone() == right.timezone();
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType& left) {
    const auto& right = checked_cast<const FixedSizeBinaryType&>(right_);
    result_ = left.byte_width() == right.byte_width();
    return Status::OK();
  }

  Status Visit(const DecimalType& left) {
    const auto& right = checked_cast<const DecimalType&>(right_);
    result_ = left.byte_width() == right.byte_width() &&
              left.precision() == right.precision() && left.scale() == right.scale();
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_list_type<T>::value || is_list_view_type<T>::value, Status> Visit(
      const T& left) {
    std::shared_ptr<Field> left_field = left.field(0);
    std::shared_ptr<Field> right_field = checked_cast<const T&>(right_).field(0);
    bool equal_names = !check_metadata_ || (left_field->name() == right_field->name());
    bool equal_metadata = !check_metadata_ || MetadataEqual(*left_field, *right_field);

    result_ = equal_names && equal_metadata &&
              (left_field->nullable() == right_field->nullable()) &&
              left_field->type()->Equals(*right_field->type(), check_metadata_);

    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_struct_type<T>::value, Status> Visit(const T& left) {
    return VisitChildren(left);
  }

  Status Visit(const MapType& left) {
    const auto& right = checked_cast<const MapType&>(right_);
    if (left.keys_sorted() != right.keys_sorted()) {
      result_ = false;
      return Status::OK();
    }
    if (check_metadata_ && (left.item_field()->name() != right.item_field()->name() ||
                            left.key_field()->name() != right.key_field()->name() ||
                            left.value_field()->name() != right.value_field()->name())) {
      result_ = false;
      return Status::OK();
    }
    if (check_metadata_ && !(MetadataEqual(*left.item_field(), *right.item_field()) &&
                             MetadataEqual(*left.key_field(), *right.key_field()) &&
                             MetadataEqual(*left.value_field(), *right.value_field()))) {
      result_ = false;
      return Status::OK();
    }
    result_ = left.key_type()->Equals(*right.key_type(), check_metadata_) &&
              left.item_type()->Equals(*right.item_type(), check_metadata_);
    return Status::OK();
  }

  Status Visit(const UnionType& left) {
    const auto& right = checked_cast<const UnionType&>(right_);

    if (left.mode() != right.mode() || left.type_codes() != right.type_codes()) {
      result_ = false;
      return Status::OK();
    }

    result_ = std::equal(
        left.fields().begin(), left.fields().end(), right.fields().begin(),
        [this](const std::shared_ptr<Field>& l, const std::shared_ptr<Field>& r) {
          return l->Equals(r, check_metadata_);
        });
    return Status::OK();
  }

  Status Visit(const DictionaryType& left) {
    const auto& right = checked_cast<const DictionaryType&>(right_);
    result_ = left.index_type()->Equals(right.index_type()) &&
              left.value_type()->Equals(right.value_type()) &&
              (left.ordered() == right.ordered());
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& left) {
    const auto& right = checked_cast<const RunEndEncodedType&>(right_);
    result_ = left.value_type()->Equals(right.value_type()) &&
              left.run_end_type()->Equals(right.run_end_type());
    return Status::OK();
  }

  Status Visit(const ExtensionType& left) {
    result_ = left.ExtensionEquals(static_cast<const ExtensionType&>(right_));
    return Status::OK();
  }

  bool result() const { return result_; }

 protected:
  const DataType& right_;
  bool check_metadata_;
  bool result_;
};
}  // namespace

bool ArrayRangeEquals(const Array& left, const Array& right, int64_t left_start_idx,
                      int64_t left_end_idx, int64_t right_start_idx,
                      const EqualOptions& options) {
  return ArrayRangeEquals(left, right, left_start_idx, left_end_idx, right_start_idx,
                          options, options.use_atol());
}

bool ArrayRangeApproxEquals(const Array& left, const Array& right, int64_t left_start_idx,
                            int64_t left_end_idx, int64_t right_start_idx,
                            const EqualOptions& options) {
  const bool floating_approximate = true;
  return ArrayRangeEquals(left, right, left_start_idx, left_end_idx, right_start_idx,
                          options, floating_approximate);
}

bool ArrayEquals(const Array& left, const Array& right, const EqualOptions& opts) {
  return ArrayEquals(left, right, opts, opts.use_atol());
}

bool ArrayApproxEquals(const Array& left, const Array& right, const EqualOptions& opts) {
  const bool floating_approximate = true;
  return ArrayEquals(left, right, opts, floating_approximate);
}

bool ArrayDataEquals(const ArrayData& left, const ArrayData& right,
                     const EqualOptions& opts) {
  const bool floating_approximate = false;
  return CompareArrayRanges(left, right, 0, left.length, 0, opts, floating_approximate);
}

bool ScalarEquals(const Scalar& left, const Scalar& right, const EqualOptions& options) {
  return ScalarEquals(left, right, options, options.use_atol());
}

bool ScalarApproxEquals(const Scalar& left, const Scalar& right,
                        const EqualOptions& options) {
  const bool floating_approximate = true;
  return ScalarEquals(left, right, options, floating_approximate);
}

namespace {

bool StridedIntegerTensorContentEquals(const int dim_index, int64_t left_offset,
                                       int64_t right_offset, int elem_size,
                                       const Tensor& left, const Tensor& right) {
  const auto n = left.shape()[dim_index];
  const auto left_stride = left.strides()[dim_index];
  const auto right_stride = right.strides()[dim_index];
  if (dim_index == left.ndim() - 1) {
    for (int64_t i = 0; i < n; ++i) {
      if (memcmp(left.raw_data() + left_offset + i * left_stride,
                 right.raw_data() + right_offset + i * right_stride, elem_size) != 0) {
        return false;
      }
    }
    return true;
  }
  for (int64_t i = 0; i < n; ++i) {
    if (!StridedIntegerTensorContentEquals(dim_index + 1, left_offset, right_offset,
                                           elem_size, left, right)) {
      return false;
    }
    left_offset += left_stride;
    right_offset += right_stride;
  }
  return true;
}

bool IntegerTensorEquals(const Tensor& left, const Tensor& right) {
  bool are_equal;
  // The arrays are the same object
  if (&left == &right) {
    are_equal = true;
  } else {
    const bool left_row_major_p = left.is_row_major();
    const bool left_column_major_p = left.is_column_major();
    const bool right_row_major_p = right.is_row_major();
    const bool right_column_major_p = right.is_column_major();

    if (!(left_row_major_p && right_row_major_p) &&
        !(left_column_major_p && right_column_major_p)) {
      const auto& type = checked_cast<const FixedWidthType&>(*left.type());
      are_equal =
          StridedIntegerTensorContentEquals(0, 0, 0, type.byte_width(), left, right);
    } else {
      const int byte_width = left.type()->byte_width();
      DCHECK_GT(byte_width, 0);

      const uint8_t* left_data = left.data()->data();
      const uint8_t* right_data = right.data()->data();

      are_equal = memcmp(left_data, right_data,
                         static_cast<size_t>(byte_width * left.size())) == 0;
    }
  }
  return are_equal;
}

template <typename DataType>
bool StridedFloatTensorContentEquals(const int dim_index, int64_t left_offset,
                                     int64_t right_offset, const Tensor& left,
                                     const Tensor& right, const EqualOptions& opts) {
  using c_type = typename DataType::c_type;
  static_assert(std::is_floating_point<c_type>::value,
                "DataType must be a floating point type");

  const auto n = left.shape()[dim_index];
  const auto left_stride = left.strides()[dim_index];
  const auto right_stride = right.strides()[dim_index];
  if (dim_index == left.ndim() - 1) {
    // Leaf dimension, compare values
    auto left_data = left.raw_data();
    auto right_data = right.raw_data();
    bool result = true;

    auto visitor = [&](auto&& compare_func) {
      for (int64_t i = 0; i < n; ++i) {
        c_type left_value =
            *reinterpret_cast<const c_type*>(left_data + left_offset + i * left_stride);
        c_type right_value = *reinterpret_cast<const c_type*>(right_data + right_offset +
                                                              i * right_stride);
        if (!compare_func(left_value, right_value)) {
          result = false;
          return;
        }
      }
    };

    VisitFloatingEquality<c_type>(opts, /*floating_approximate=*/false,
                                  std::move(visitor));
    return result;
  }

  // Outer dimension, recurse into inner
  for (int64_t i = 0; i < n; ++i) {
    if (!StridedFloatTensorContentEquals<DataType>(dim_index + 1, left_offset,
                                                   right_offset, left, right, opts)) {
      return false;
    }
    left_offset += left_stride;
    right_offset += right_stride;
  }
  return true;
}

template <typename DataType>
bool FloatTensorEquals(const Tensor& left, const Tensor& right,
                       const EqualOptions& opts) {
  return StridedFloatTensorContentEquals<DataType>(0, 0, 0, left, right, opts);
}

}  // namespace

bool TensorEquals(const Tensor& left, const Tensor& right, const EqualOptions& opts) {
  if (left.type_id() != right.type_id()) {
    return false;
  } else if (left.size() == 0 && right.size() == 0) {
    return true;
  } else if (left.shape() != right.shape()) {
    return false;
  }

  switch (left.type_id()) {
    // TODO: Support half-float tensors
    // case Type::HALF_FLOAT:
    case Type::FLOAT:
      return FloatTensorEquals<FloatType>(left, right, opts);

    case Type::DOUBLE:
      return FloatTensorEquals<DoubleType>(left, right, opts);

    default:
      return IntegerTensorEquals(left, right);
  }
}

namespace {

template <typename LeftSparseIndexType, typename RightSparseIndexType>
struct SparseTensorEqualsImpl {
  static bool Compare(const SparseTensorImpl<LeftSparseIndexType>& left,
                      const SparseTensorImpl<RightSparseIndexType>& right,
                      const EqualOptions&) {
    // TODO(mrkn): should we support the equality among different formats?
    return false;
  }
};

bool IntegerSparseTensorDataEquals(const uint8_t* left_data, const uint8_t* right_data,
                                   const int byte_width, const int64_t length) {
  if (left_data == right_data) {
    return true;
  }
  return memcmp(left_data, right_data, static_cast<size_t>(byte_width * length)) == 0;
}

template <typename DataType>
bool FloatSparseTensorDataEquals(const typename DataType::c_type* left_data,
                                 const typename DataType::c_type* right_data,
                                 const int64_t length, const EqualOptions& opts) {
  using c_type = typename DataType::c_type;
  static_assert(std::is_floating_point<c_type>::value,
                "DataType must be a floating point type");
  if (opts.nans_equal()) {
    if (left_data == right_data) {
      return true;
    }

    for (int64_t i = 0; i < length; ++i) {
      const auto left = left_data[i];
      const auto right = right_data[i];
      if (left != right && !(std::isnan(left) && std::isnan(right))) {
        return false;
      }
    }
  } else {
    for (int64_t i = 0; i < length; ++i) {
      if (left_data[i] != right_data[i]) {
        return false;
      }
    }
  }
  return true;
}

template <typename SparseIndexType>
struct SparseTensorEqualsImpl<SparseIndexType, SparseIndexType> {
  static bool Compare(const SparseTensorImpl<SparseIndexType>& left,
                      const SparseTensorImpl<SparseIndexType>& right,
                      const EqualOptions& opts) {
    DCHECK(left.type()->id() == right.type()->id());
    DCHECK(left.shape() == right.shape());

    const auto length = left.non_zero_length();
    DCHECK(length == right.non_zero_length());

    const auto& left_index = checked_cast<const SparseIndexType&>(*left.sparse_index());
    const auto& right_index = checked_cast<const SparseIndexType&>(*right.sparse_index());

    if (!left_index.Equals(right_index)) {
      return false;
    }

    const int byte_width = left.type()->byte_width();
    DCHECK_GT(byte_width, 0);

    const uint8_t* left_data = left.data()->data();
    const uint8_t* right_data = right.data()->data();
    switch (left.type()->id()) {
      // TODO: Support half-float tensors
      // case Type::HALF_FLOAT:
      case Type::FLOAT:
        return FloatSparseTensorDataEquals<FloatType>(
            reinterpret_cast<const float*>(left_data),
            reinterpret_cast<const float*>(right_data), length, opts);

      case Type::DOUBLE:
        return FloatSparseTensorDataEquals<DoubleType>(
            reinterpret_cast<const double*>(left_data),
            reinterpret_cast<const double*>(right_data), length, opts);

      default:  // Integer cases
        return IntegerSparseTensorDataEquals(left_data, right_data, byte_width, length);
    }
  }
};

template <typename SparseIndexType>
inline bool SparseTensorEqualsImplDispatch(const SparseTensorImpl<SparseIndexType>& left,
                                           const SparseTensor& right,
                                           const EqualOptions& opts) {
  switch (right.format_id()) {
    case SparseTensorFormat::COO: {
      const auto& right_coo =
          checked_cast<const SparseTensorImpl<SparseCOOIndex>&>(right);
      return SparseTensorEqualsImpl<SparseIndexType, SparseCOOIndex>::Compare(
          left, right_coo, opts);
    }

    case SparseTensorFormat::CSR: {
      const auto& right_csr =
          checked_cast<const SparseTensorImpl<SparseCSRIndex>&>(right);
      return SparseTensorEqualsImpl<SparseIndexType, SparseCSRIndex>::Compare(
          left, right_csr, opts);
    }

    case SparseTensorFormat::CSC: {
      const auto& right_csc =
          checked_cast<const SparseTensorImpl<SparseCSCIndex>&>(right);
      return SparseTensorEqualsImpl<SparseIndexType, SparseCSCIndex>::Compare(
          left, right_csc, opts);
    }

    case SparseTensorFormat::CSF: {
      const auto& right_csf =
          checked_cast<const SparseTensorImpl<SparseCSFIndex>&>(right);
      return SparseTensorEqualsImpl<SparseIndexType, SparseCSFIndex>::Compare(
          left, right_csf, opts);
    }

    default:
      return false;
  }
}

}  // namespace

bool SparseTensorEquals(const SparseTensor& left, const SparseTensor& right,
                        const EqualOptions& opts) {
  if (left.type()->id() != right.type()->id()) {
    return false;
  } else if (left.size() == 0 && right.size() == 0) {
    return true;
  } else if (left.shape() != right.shape()) {
    return false;
  } else if (left.non_zero_length() != right.non_zero_length()) {
    return false;
  }

  switch (left.format_id()) {
    case SparseTensorFormat::COO: {
      const auto& left_coo = checked_cast<const SparseTensorImpl<SparseCOOIndex>&>(left);
      return SparseTensorEqualsImplDispatch(left_coo, right, opts);
    }

    case SparseTensorFormat::CSR: {
      const auto& left_csr = checked_cast<const SparseTensorImpl<SparseCSRIndex>&>(left);
      return SparseTensorEqualsImplDispatch(left_csr, right, opts);
    }

    case SparseTensorFormat::CSC: {
      const auto& left_csc = checked_cast<const SparseTensorImpl<SparseCSCIndex>&>(left);
      return SparseTensorEqualsImplDispatch(left_csc, right, opts);
    }

    case SparseTensorFormat::CSF: {
      const auto& left_csf = checked_cast<const SparseTensorImpl<SparseCSFIndex>&>(left);
      return SparseTensorEqualsImplDispatch(left_csf, right, opts);
    }

    default:
      return false;
  }
}

bool TypeEquals(const DataType& left, const DataType& right, bool check_metadata) {
  // The arrays are the same object
  if (&left == &right) {
    return true;
  } else if (left.id() != right.id()) {
    return false;
  } else {
    // First try to compute fingerprints
    if (check_metadata) {
      const auto& left_metadata_fp = left.metadata_fingerprint();
      const auto& right_metadata_fp = right.metadata_fingerprint();
      if (left_metadata_fp != right_metadata_fp) {
        return false;
      }
    }

    const auto& left_fp = left.fingerprint();
    const auto& right_fp = right.fingerprint();
    if (!left_fp.empty() && !right_fp.empty()) {
      return left_fp == right_fp;
    }

    // TODO remove check_metadata here?
    TypeEqualsVisitor visitor(right, check_metadata);
    auto error = VisitTypeInline(left, &visitor);
    if (!error.ok()) {
      DCHECK(false) << "Types are not comparable: " << error.ToString();
    }
    return visitor.result();
  }
}

namespace {

bool DoubleEquals(const double& left, const double& right, const EqualOptions& options) {
  bool result;
  auto visitor = [&](auto&& compare_func) { result = compare_func(left, right); };
  VisitFloatingEquality<double>(options, options.use_atol(), std::move(visitor));
  return result;
}

template <typename Type>
bool ArrayStatisticsOptionalValueEquals(const std::optional<Type>& left,
                                        const std::optional<Type>& right,
                                        const EqualOptions& options) {
  if (!left.has_value() || !right.has_value()) {
    return left.has_value() == right.has_value();
  } else if constexpr (std::is_same_v<Type, double>) {
    return DoubleEquals(left.value(), right.value(), options);
  } else if (left->index() != right->index()) {
    return false;
  } else {
    auto EqualsVisitor = [&](const auto& v1, const auto& v2) {
      using type_1 = std::decay_t<decltype(v1)>;
      using type_2 = std::decay_t<decltype(v2)>;
      if constexpr (std::conjunction_v<std::is_same<type_1, double>,
                                       std::is_same<type_2, double>>) {
        return DoubleEquals(v1, v2, options);
      } else if constexpr (std::is_same_v<type_1, type_2>) {
        return v1 == v2;
      }
      Unreachable("The types are different.");
      return false;
    };
    return std::visit(EqualsVisitor, left.value(), right.value());
  }
}

bool ArrayStatisticsEqualsImpl(const ArrayStatistics& left, const ArrayStatistics& right,
                               const EqualOptions& equal_options) {
  return left.null_count == right.null_count &&
         ArrayStatisticsOptionalValueEquals(left.distinct_count, right.distinct_count,
                                            equal_options) &&
         left.is_average_byte_width_exact == right.is_average_byte_width_exact &&
         left.is_min_exact == right.is_min_exact &&
         left.is_max_exact == right.is_max_exact &&
         ArrayStatisticsOptionalValueEquals(left.average_byte_width,
                                            right.average_byte_width, equal_options) &&
         ArrayStatisticsOptionalValueEquals(left.min, right.min, equal_options) &&
         ArrayStatisticsOptionalValueEquals(left.max, right.max, equal_options);
}

}  // namespace

bool ArrayStatisticsEquals(const ArrayStatistics& left, const ArrayStatistics& right,
                           const EqualOptions& options) {
  return ArrayStatisticsEqualsImpl(left, right, options);
}

}  // namespace arrow

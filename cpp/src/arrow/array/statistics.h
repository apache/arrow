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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include "arrow/compare.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \class ArrayStatistics
/// \brief Statistics for an Array
///
/// Apache Arrow format doesn't have statistics but data source such
/// as Apache Parquet may have statistics. Statistics associated with
/// data source can be read unified API via this class.
struct ARROW_EXPORT ArrayStatistics {
  /// \brief The type for maximum and minimum values. If the target
  /// value exists, one of them is used. `std::nullopt` is used
  /// otherwise.
  using ValueType =
      std::variant<bool, int64_t, uint64_t, double, std::string, std::shared_ptr<Scalar>>;

  static Result<std::shared_ptr<DataType>> ValueToArrowType(
      const std::optional<ValueType>& value, const std::shared_ptr<DataType>& array_type);

  /// \brief The number of null values, may not be set
  std::optional<int64_t> null_count = std::nullopt;

  /// \brief The number of distinct values, may not be set
  std::optional<int64_t> distinct_count = std::nullopt;

  /// \brief The minimum value, may not be set
  std::optional<ValueType> min = std::nullopt;

  /// \brief Compute Arrow type of the minimum value.
  ///
  /// If \ref ValueType is `std::string`, `array_type` may be
  /// used. If `array_type` is a binary-like type such as \ref
  /// arrow::binary and \ref arrow::large_utf8, `array_type` is
  /// returned. \ref arrow::utf8 is returned otherwise.
  ///
  /// If \ref ValueType isn't `std::string`, `array_type` isn't used.
  ///
  /// \param array_type The Arrow type of the associated array.
  ///
  /// \return \ref arrow::null if the minimum value is `std::nullopt`,
  ///         Arrow type based on \ref ValueType of the \ref min
  ///         otherwise.
  Result<std::shared_ptr<DataType>> MinArrowType(
      const std::shared_ptr<DataType>& array_type) {
    return ValueToArrowType(min, array_type);
  }

  /// \brief Whether the minimum value is exact or not
  bool is_min_exact = false;

  /// \brief The maximum value, may not be set
  std::optional<ValueType> max = std::nullopt;

  /// \brief Compute Arrow type of the maximum value.
  ///
  /// If \ref ValueType is `std::string`, `array_type` may be
  /// used. If `array_type` is a binary-like type such as \ref
  /// arrow::binary and \ref arrow::large_utf8, `array_type` is
  /// returned. \ref arrow::utf8 is returned otherwise.
  ///
  /// If \ref ValueType isn't `std::string`, `array_type` isn't used.
  ///
  /// \param array_type The Arrow type of the associated array.
  ///
  /// \return \ref arrow::null if the maximum value is `std::nullopt`,
  ///         Arrow type based on \ref ValueType of the \ref max
  ///         otherwise.
  Result<std::shared_ptr<DataType>> MaxArrowType(
      const std::shared_ptr<DataType>& array_type) {
    return ValueToArrowType(max, array_type);
  }

  /// \brief Whether the maximum value is exact or not
  bool is_max_exact = false;

  /// \brief Check two statistics for equality
  ///
  /// \param[in] other the ArrayStatistics to compare with
  /// \param[in] options the options for equality comparisons of Scalars
  ///
  /// \return true if  statistics are equal
  bool Equals(const ArrayStatistics& other,
              const EqualOptions& options = EqualOptions::Defaults()) const;

  /// \brief Check two statistics for equality
  bool operator==(const ArrayStatistics& other) const { return Equals(other); }

  /// \brief Check two statistics for not equality
  bool operator!=(const ArrayStatistics& other) const { return !Equals(other); }
};

namespace internal {
/// \brief Extract all nested arrays from an array as \ref ArrayDataVector.
///
/// Returns all nested arrays within the given array, up to the specified
/// maximum nesting depth, as a vector of ArrayData.
///
/// \param[in] array_data The input array_data from which nested array_data will be
/// extracted.
/// \param[in] max_nesting_depth The maximum depth of nested arrays to extract.
///
/// \return A vector of \ref ArrayData .
ARROW_EXPORT
Result<ArrayDataVector> ExtractColumnsToArrayData(
    const std::shared_ptr<ArrayData>& array_data, const int32_t& max_nesting_depth);
/// \brief Collect Statistics From ArrayDataVector and turns into an Array
///
/// \param[in] memory_pool the memory pool to allocate memory from
/// \param extracted_array_data_vector The input ArrayData from which statistics array is
/// extracted
///
/// \param num_rows The input specifies the number of rows an object(which uses for
/// record batch), if it  is set to std::nullopt(which uses for array),
/// the number of rows is deduced from the
/// length of the first array
///
/// \return A statistics array.
ARROW_EXPORT
Result<std::shared_ptr<Array>> MakeStatisticsArray(
    MemoryPool* memory_pool, const ArrayDataVector& extracted_array_data_vector,
    std::optional<int64_t> num_rows = std::nullopt);
}  // namespace internal

}  // namespace arrow

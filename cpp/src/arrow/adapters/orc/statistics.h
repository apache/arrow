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

#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace orc {
class ColumnStatistics;
class DateColumnStatistics;
class DecimalColumnStatistics;
class DoubleColumnStatistics;
class IntegerColumnStatistics;
class Reader;
class Statistics;
class StringColumnStatistics;
class TimestampColumnStatistics;
class Type;
}  // namespace orc

namespace arrow {
class KeyValueMetadata;

namespace adapters {
namespace orc {

class Statistics;
class ColumnMetaData;
class StripeMetaData;

/// \brief Scalar materialization of ORC column statistics.
struct ARROW_EXPORT ColumnStatisticsAsScalars {
  /// \brief Whether the column contains null values.
  bool has_null;
  /// \brief Number of non-null values in the column.
  int64_t num_values;
  /// \brief Whether min/max statistics are available and valid.
  bool has_min_max;
  /// \brief Minimum value (nullptr if unavailable).
  std::shared_ptr<Scalar> min;
  /// \brief Maximum value (nullptr if unavailable).
  std::shared_ptr<Scalar> max;
};

/// \brief File-level ORC metadata container.
class ARROW_EXPORT FileMetaData {
 public:
  FileMetaData() = default;
  FileMetaData(std::shared_ptr<const ::orc::Reader> reader,
               std::shared_ptr<const ::orc::Statistics> file_statistics)
      : reader_(std::move(reader)), file_statistics_(std::move(file_statistics)) {}

  bool valid() const { return reader_ != nullptr && file_statistics_ != nullptr; }
  int num_columns() const;
  int num_stripes() const;
  int64_t num_rows() const;
  Result<StripeMetaData> Stripe(int stripe_index) const;
  Result<ColumnMetaData> Column(int column_index) const;
  Result<std::shared_ptr<const KeyValueMetadata>> key_value_metadata() const;
  const ::orc::Type& schema_root() const;

 private:
  std::shared_ptr<const ::orc::Reader> reader_;
  std::shared_ptr<const ::orc::Statistics> file_statistics_;
};

/// \brief Stripe-level ORC metadata container.
class ARROW_EXPORT StripeMetaData {
 public:
  StripeMetaData() = default;
  StripeMetaData(int64_t stripe_index,
                 int64_t num_rows,
                 std::shared_ptr<const ::orc::Statistics> stripe_statistics)
      : stripe_index_(stripe_index),
        num_rows_(num_rows),
        stripe_statistics_(std::move(stripe_statistics)) {}

  bool valid() const { return stripe_statistics_ != nullptr; }
  int64_t stripe_index() const { return stripe_index_; }
  int64_t num_rows() const { return num_rows_; }
  int num_columns() const;
  Result<ColumnMetaData> Column(int column_index) const;

 private:
  int64_t stripe_index_ = -1;
  int64_t num_rows_ = 0;
  std::shared_ptr<const ::orc::Statistics> stripe_statistics_;
};

/// \brief Thin wrapper over liborc column statistics.
///
/// Keeps liborc type dispatch in one place and provides a stable API surface
/// for downstream consumers.
class ARROW_EXPORT Statistics {
 public:
  Statistics() = default;
  Statistics(std::shared_ptr<const ::orc::Statistics> owner,
             const ::orc::ColumnStatistics* column_statistics)
      : owner_(std::move(owner)), column_statistics_(column_statistics) {}
  explicit Statistics(const ::orc::ColumnStatistics* column_statistics)
      : column_statistics_(column_statistics) {}

  bool valid() const { return column_statistics_ != nullptr; }
  bool has_null() const;
  std::optional<int64_t> null_count() const;
  int64_t num_values() const;
  bool HasMinMax() const;

  const ::orc::ColumnStatistics* raw() const { return column_statistics_; }
  const ::orc::IntegerColumnStatistics* integer() const;
  const ::orc::DoubleColumnStatistics* floating_point() const;
  const ::orc::StringColumnStatistics* string() const;
  const ::orc::DateColumnStatistics* date() const;
  const ::orc::TimestampColumnStatistics* timestamp() const;
  const ::orc::DecimalColumnStatistics* decimal() const;

 private:
  std::shared_ptr<const ::orc::Statistics> owner_;
  const ::orc::ColumnStatistics* column_statistics_ = nullptr;
};

/// \brief Column-level metadata container exposing statistics.
class ARROW_EXPORT ColumnMetaData {
 public:
  ColumnMetaData() = default;
  ColumnMetaData(int column_index, Statistics statistics)
      : column_index_(column_index), statistics_(std::move(statistics)) {}

  bool valid() const { return statistics_.valid(); }
  int column_index() const { return column_index_; }
  Result<Statistics> statistics() const;

 private:
  int column_index_ = -1;
  Statistics statistics_;
};

ARROW_EXPORT Result<ColumnStatisticsAsScalars> StatisticsAsScalars(
    const Statistics& statistics);

}  // namespace orc
}  // namespace adapters
}  // namespace arrow

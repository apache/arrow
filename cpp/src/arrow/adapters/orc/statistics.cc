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

#include "arrow/adapters/orc/statistics.h"

#include <cmath>
#include <limits>

#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/key_value_metadata.h"
#include "orc/Reader.hh"
#include "orc/Statistics.hh"

namespace liborc = orc;

namespace arrow {
namespace adapters {
namespace orc {
namespace {

constexpr int64_t kMaxMillisForNanos = std::numeric_limits<int64_t>::max() / 1000000LL;
constexpr int64_t kMinMillisForNanos = std::numeric_limits<int64_t>::lowest() / 1000000LL;

bool MillisFitInNanos(int64_t millis) {
  return millis >= kMinMillisForNanos && millis <= kMaxMillisForNanos;
}

}  // namespace

bool Statistics::has_null() const { return column_statistics_->hasNull(); }

int FileMetaData::num_columns() const {
  return static_cast<int>(file_statistics_->getNumberOfColumns());
}

int FileMetaData::num_stripes() const {
  return static_cast<int>(reader_->getNumberOfStripes());
}

int64_t FileMetaData::num_rows() const {
  return static_cast<int64_t>(reader_->getNumberOfRows());
}

Result<StripeMetaData> FileMetaData::Stripe(int stripe_index) const {
  if (!valid()) {
    return Status::Invalid("ORC file metadata is not initialized");
  }
  if (stripe_index < 0 || stripe_index >= num_stripes()) {
    return Status::Invalid("Stripe index ", stripe_index, " out of range [0, ",
                           num_stripes(), ")");
  }

  auto stripe_stats = std::shared_ptr<const liborc::Statistics>(
      reader_->getStripeStatistics(static_cast<uint64_t>(stripe_index)).release());
  auto stripe_info = reader_->getStripe(static_cast<uint64_t>(stripe_index));
  return StripeMetaData(stripe_index, static_cast<int64_t>(stripe_info->getNumberOfRows()),
                        std::move(stripe_stats));
}

Result<ColumnMetaData> FileMetaData::Column(int column_index) const {
  if (!valid()) {
    return Status::Invalid("ORC file metadata is not initialized");
  }
  if (column_index < 0 || static_cast<uint32_t>(column_index) >=
                              file_statistics_->getNumberOfColumns()) {
    return Status::Invalid("Column index ", column_index, " out of range [0, ",
                           file_statistics_->getNumberOfColumns(), ")");
  }

  const liborc::ColumnStatistics* col_stats =
      file_statistics_->getColumnStatistics(static_cast<uint32_t>(column_index));
  return ColumnMetaData(column_index, Statistics(file_statistics_, col_stats));
}

Result<std::shared_ptr<const KeyValueMetadata>> FileMetaData::key_value_metadata() const {
  if (!valid()) {
    return Status::Invalid("ORC file metadata is not initialized");
  }

  auto metadata = std::make_shared<KeyValueMetadata>();
  const std::list<std::string> keys = reader_->getMetadataKeys();
  for (const auto& key : keys) {
    metadata->Append(key, reader_->getMetadataValue(key));
  }
  return std::const_pointer_cast<const KeyValueMetadata>(metadata);
}

const ::orc::Type& FileMetaData::schema_root() const { return reader_->getType(); }

Result<Statistics> ColumnMetaData::statistics() const {
  if (!valid()) {
    return Status::Invalid("ORC column metadata is not initialized");
  }
  return statistics_;
}

int StripeMetaData::num_columns() const {
  return static_cast<int>(stripe_statistics_->getNumberOfColumns());
}

Result<ColumnMetaData> StripeMetaData::Column(int column_index) const {
  if (!valid()) {
    return Status::Invalid("ORC stripe metadata is not initialized");
  }
  if (column_index < 0 || static_cast<uint32_t>(column_index) >=
                              stripe_statistics_->getNumberOfColumns()) {
    return Status::Invalid("Column index ", column_index, " out of range [0, ",
                           stripe_statistics_->getNumberOfColumns(), ")");
  }

  const liborc::ColumnStatistics* col_stats =
      stripe_statistics_->getColumnStatistics(static_cast<uint32_t>(column_index));
  return ColumnMetaData(column_index, Statistics(stripe_statistics_, col_stats));
}

std::optional<int64_t> Statistics::null_count() const {
  // liborc doesn't expose null_count on ColumnStatistics.
  return std::nullopt;
}

int64_t Statistics::num_values() const {
  return static_cast<int64_t>(column_statistics_->getNumberOfValues());
}

const liborc::IntegerColumnStatistics* Statistics::integer() const {
  return dynamic_cast<const liborc::IntegerColumnStatistics*>(column_statistics_);
}

const liborc::DoubleColumnStatistics* Statistics::floating_point() const {
  return dynamic_cast<const liborc::DoubleColumnStatistics*>(column_statistics_);
}

const liborc::StringColumnStatistics* Statistics::string() const {
  return dynamic_cast<const liborc::StringColumnStatistics*>(column_statistics_);
}

const liborc::DateColumnStatistics* Statistics::date() const {
  return dynamic_cast<const liborc::DateColumnStatistics*>(column_statistics_);
}

const liborc::TimestampColumnStatistics* Statistics::timestamp() const {
  return dynamic_cast<const liborc::TimestampColumnStatistics*>(column_statistics_);
}

const liborc::DecimalColumnStatistics* Statistics::decimal() const {
  return dynamic_cast<const liborc::DecimalColumnStatistics*>(column_statistics_);
}

bool Statistics::HasMinMax() const {
  if (!valid()) {
    return false;
  }

  if (const auto* int_stats = integer()) {
    return int_stats->hasMinimum() && int_stats->hasMaximum();
  }
  if (const auto* double_stats = floating_point()) {
    if (!double_stats->hasMinimum() || !double_stats->hasMaximum()) {
      return false;
    }
    return !std::isnan(double_stats->getMinimum()) && !std::isnan(double_stats->getMaximum());
  }
  if (const auto* string_stats = string()) {
    return string_stats->hasMinimum() && string_stats->hasMaximum();
  }
  if (const auto* date_stats = date()) {
    return date_stats->hasMinimum() && date_stats->hasMaximum();
  }
  if (const auto* ts_stats = timestamp()) {
    if (!ts_stats->hasMinimum() || !ts_stats->hasMaximum()) {
      return false;
    }
    return MillisFitInNanos(ts_stats->getMinimum()) &&
           MillisFitInNanos(ts_stats->getMaximum());
  }
  if (const auto* decimal_stats = decimal()) {
    if (!decimal_stats->hasMinimum() || !decimal_stats->hasMaximum()) {
      return false;
    }
    liborc::Decimal min_dec = decimal_stats->getMinimum();
    liborc::Decimal max_dec = decimal_stats->getMaximum();
    return min_dec.scale == max_dec.scale;
  }
  return false;
}

Result<ColumnStatisticsAsScalars> StatisticsAsScalars(const Statistics& statistics) {
  if (!statistics.valid()) {
    return Status::Invalid("ORC statistics wrapper is not initialized");
  }

  ColumnStatisticsAsScalars converted;
  converted.has_null = statistics.has_null();
  converted.num_values = statistics.num_values();
  converted.has_min_max = false;
  converted.min = nullptr;
  converted.max = nullptr;

  if (!statistics.HasMinMax()) {
    return converted;
  }

  converted.has_min_max = true;
  if (const auto* int_stats = statistics.integer()) {
    converted.min = std::make_shared<Int64Scalar>(int_stats->getMinimum());
    converted.max = std::make_shared<Int64Scalar>(int_stats->getMaximum());
    return converted;
  }
  if (const auto* double_stats = statistics.floating_point()) {
    converted.min = std::make_shared<DoubleScalar>(double_stats->getMinimum());
    converted.max = std::make_shared<DoubleScalar>(double_stats->getMaximum());
    return converted;
  }
  if (const auto* string_stats = statistics.string()) {
    converted.min = std::make_shared<StringScalar>(string_stats->getMinimum());
    converted.max = std::make_shared<StringScalar>(string_stats->getMaximum());
    return converted;
  }
  if (const auto* date_stats = statistics.date()) {
    converted.min = std::make_shared<Date32Scalar>(date_stats->getMinimum());
    converted.max = std::make_shared<Date32Scalar>(date_stats->getMaximum());
    return converted;
  }
  if (const auto* ts_stats = statistics.timestamp()) {
    auto ts_type = timestamp(TimeUnit::NANO);
    converted.min = std::make_shared<TimestampScalar>(
        ts_stats->getMinimum() * 1000000LL + ts_stats->getMinimumNanos(), ts_type);
    converted.max = std::make_shared<TimestampScalar>(
        ts_stats->getMaximum() * 1000000LL + ts_stats->getMaximumNanos(), ts_type);
    return converted;
  }
  if (const auto* decimal_stats = statistics.decimal()) {
    liborc::Decimal min_dec = decimal_stats->getMinimum();
    liborc::Decimal max_dec = decimal_stats->getMaximum();

    Decimal128 min_d128(min_dec.value.getHighBits(), min_dec.value.getLowBits());
    Decimal128 max_d128(max_dec.value.getHighBits(), max_dec.value.getLowBits());
    auto dec_type = decimal128(38, min_dec.scale);

    converted.min = std::make_shared<Decimal128Scalar>(min_d128, dec_type);
    converted.max = std::make_shared<Decimal128Scalar>(max_d128, dec_type);
    return converted;
  }

  converted.has_min_max = false;
  converted.min = nullptr;
  converted.max = nullptr;
  return converted;
}

}  // namespace orc
}  // namespace adapters
}  // namespace arrow

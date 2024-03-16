// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliancec
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

#include "parquet/size_statistics.h"

#include <algorithm>

#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/visit_data_inline.h"
#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/thrift_internal.h"
#include "parquet/types.h"

namespace parquet {

class SizeStatistics::SizeStatisticsImpl {
 public:
  SizeStatisticsImpl() = default;

  SizeStatisticsImpl(const format::SizeStatistics* size_stats,
                     const ColumnDescriptor* descr)
      : rep_level_histogram_(size_stats->repetition_level_histogram),
        def_level_histogram_(size_stats->definition_level_histogram) {
    if (descr->physical_type() == Type::BYTE_ARRAY &&
        size_stats->__isset.unencoded_byte_array_data_bytes) {
      unencoded_byte_array_data_bytes_ = size_stats->unencoded_byte_array_data_bytes;
    }
  }

  const std::vector<int64_t>& repetition_level_histogram() const {
    return rep_level_histogram_;
  }

  const std::vector<int64_t>& definition_level_histogram() const {
    return def_level_histogram_;
  }

  std::optional<int64_t> unencoded_byte_array_data_bytes() const {
    return unencoded_byte_array_data_bytes_;
  }

  void Merge(const SizeStatistics& other) {
    if (rep_level_histogram_.size() != other.repetition_level_histogram().size() ||
        def_level_histogram_.size() != other.definition_level_histogram().size() ||
        unencoded_byte_array_data_bytes_.has_value() !=
            other.unencoded_byte_array_data_bytes().has_value()) {
      throw ParquetException("Cannot merge incompatible SizeStatistics");
    }

    std::transform(rep_level_histogram_.begin(), rep_level_histogram_.end(),
                   other.repetition_level_histogram().begin(),
                   rep_level_histogram_.begin(), std::plus<>());

    std::transform(def_level_histogram_.begin(), def_level_histogram_.end(),
                   other.definition_level_histogram().begin(),
                   def_level_histogram_.begin(), std::plus<>());
    if (unencoded_byte_array_data_bytes_.has_value()) {
      unencoded_byte_array_data_bytes_ = unencoded_byte_array_data_bytes_.value() +
                                         other.unencoded_byte_array_data_bytes().value();
    }
  }

 private:
  friend class SizeStatisticsBuilder;
  std::vector<int64_t> rep_level_histogram_;
  std::vector<int64_t> def_level_histogram_;
  std::optional<int64_t> unencoded_byte_array_data_bytes_;
};

const std::vector<int64_t>& SizeStatistics::repetition_level_histogram() const {
  return impl_->repetition_level_histogram();
}

const std::vector<int64_t>& SizeStatistics::definition_level_histogram() const {
  return impl_->definition_level_histogram();
}

std::optional<int64_t> SizeStatistics::unencoded_byte_array_data_bytes() const {
  return impl_->unencoded_byte_array_data_bytes();
}

void SizeStatistics::Merge(const SizeStatistics& other) { return impl_->Merge(other); }

SizeStatistics::SizeStatistics(const void* size_statistics, const ColumnDescriptor* descr)
    : impl_(std::make_unique<SizeStatisticsImpl>(
          reinterpret_cast<const format::SizeStatistics*>(size_statistics), descr)) {}

SizeStatistics::SizeStatistics() : impl_(std::make_unique<SizeStatisticsImpl>()) {}

SizeStatistics::~SizeStatistics() = default;

std::unique_ptr<SizeStatistics> SizeStatistics::Make(const void* size_statistics,
                                                     const ColumnDescriptor* descr) {
  return std::unique_ptr<SizeStatistics>(new SizeStatistics(size_statistics, descr));
}

class SizeStatisticsBuilder::SizeStatisticsBuilderImpl {
 public:
  SizeStatisticsBuilderImpl(const ColumnDescriptor* descr)
      : rep_level_histogram_(descr->max_repetition_level() + 1, 0),
        def_level_histogram_(descr->max_definition_level() + 1, 0) {
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      unencoded_byte_array_data_bytes_ = 0;
    }
  }

  void WriteRepetitionLevels(int64_t num_levels, const int16_t* rep_levels) {
    for (int64_t i = 0; i < num_levels; ++i) {
      ARROW_DCHECK_LT(rep_levels[i], static_cast<int16_t>(rep_level_histogram_.size()));
      rep_level_histogram_[rep_levels[i]]++;
    }
  }

  void WriteDefinitionLevels(int64_t num_levels, const int16_t* def_levels) {
    for (int64_t i = 0; i < num_levels; ++i) {
      ARROW_DCHECK_LT(def_levels[i], static_cast<int16_t>(def_level_histogram_.size()));
      def_level_histogram_[def_levels[i]]++;
    }
  }

  void WriteRepetitionLevel(int64_t num_levels, int16_t rep_level) {
    ARROW_DCHECK_LT(rep_level, static_cast<int16_t>(rep_level_histogram_.size()));
    rep_level_histogram_[rep_level] += num_levels;
  }

  void WriteDefinitionLevel(int64_t num_levels, int16_t def_level) {
    ARROW_DCHECK_LT(def_level, static_cast<int16_t>(def_level_histogram_.size()));
    def_level_histogram_[def_level] += num_levels;
  }

  void WriteValuesSpaced(const ByteArray* values, const uint8_t* valid_bits,
                         int64_t valid_bits_offset, int64_t num_spaced_values) {
    int64_t total_bytes = 0;
    ::arrow::internal::VisitSetBitRunsVoid(valid_bits, valid_bits_offset,
                                           num_spaced_values,
                                           [&](int64_t pos, int64_t length) {
                                             for (int64_t i = 0; i < length; i++) {
                                               // Don't bother to check unlikely overflow.
                                               total_bytes += values[i + pos].len;
                                             }
                                           });
    IncrementUnencodedByteArrayDataBytes(total_bytes);
  }

  void WriteValues(const ByteArray* values, int64_t num_values) {
    int64_t total_bytes = 0;
    std::for_each(values, values + num_values,
                  [&](const ByteArray& value) { total_bytes += values->len; });
    IncrementUnencodedByteArrayDataBytes(total_bytes);
  }

  void WriteValues(const ::arrow::Array& values) {
    int64_t total_bytes = 0;
    const auto valid_func = [&](ByteArray val) { total_bytes += val.len; };
    const auto null_func = [&]() {};

    if (::arrow::is_binary_like(values.type_id())) {
      ::arrow::VisitArraySpanInline<::arrow::BinaryType>(
          *values.data(), std::move(valid_func), std::move(null_func));
    } else if (::arrow::is_large_binary_like(values.type_id())) {
      ::arrow::VisitArraySpanInline<::arrow::LargeBinaryType>(
          *values.data(), std::move(valid_func), std::move(null_func));
    } else {
      throw ParquetException("Unsupported type: " + values.type()->ToString());
    }

    IncrementUnencodedByteArrayDataBytes(total_bytes);
  }

  std::unique_ptr<SizeStatistics> Build() {
    auto stats = std::unique_ptr<SizeStatistics>(new SizeStatistics());
    stats->impl_->rep_level_histogram_ = rep_level_histogram_;
    stats->impl_->def_level_histogram_ = def_level_histogram_;
    stats->impl_->unencoded_byte_array_data_bytes_ = unencoded_byte_array_data_bytes_;
    return stats;
  }

  void Reset() {
    rep_level_histogram_.assign(rep_level_histogram_.size(), 0);
    def_level_histogram_.assign(def_level_histogram_.size(), 0);
    if (unencoded_byte_array_data_bytes_.has_value()) {
      unencoded_byte_array_data_bytes_ = 0;
    }
  }

 private:
  void IncrementUnencodedByteArrayDataBytes(int64_t total_bytes) {
    ARROW_DCHECK(unencoded_byte_array_data_bytes_.has_value());
    if (::arrow::internal::AddWithOverflow(
            total_bytes, unencoded_byte_array_data_bytes_.value(), &total_bytes)) {
      throw ParquetException("unencoded byte array data bytes overflows to INT64_MAX");
    }
    unencoded_byte_array_data_bytes_ = total_bytes;
  }

 private:
  std::vector<int64_t> rep_level_histogram_;
  std::vector<int64_t> def_level_histogram_;
  std::optional<int64_t> unencoded_byte_array_data_bytes_;
};

void SizeStatisticsBuilder::WriteRepetitionLevels(int64_t num_levels,
                                                  const int16_t* rep_levels) {
  impl_->WriteRepetitionLevels(num_levels, rep_levels);
}

void SizeStatisticsBuilder::WriteDefinitionLevels(int64_t num_levels,
                                                  const int16_t* def_levels) {
  impl_->WriteDefinitionLevels(num_levels, def_levels);
}

void SizeStatisticsBuilder::WriteRepetitionLevel(int64_t num_levels, int16_t rep_level) {
  impl_->WriteRepetitionLevel(num_levels, rep_level);
}

void SizeStatisticsBuilder::WriteDefinitionLevel(int64_t num_levels, int16_t def_level) {
  impl_->WriteDefinitionLevel(num_levels, def_level);
}

void SizeStatisticsBuilder::WriteValuesSpaced(const ByteArray* values,
                                              const uint8_t* valid_bits,
                                              int64_t valid_bits_offset,
                                              int64_t num_spaced_values) {
  impl_->WriteValuesSpaced(values, valid_bits, valid_bits_offset, num_spaced_values);
}

void SizeStatisticsBuilder::WriteValues(const ByteArray* values, int64_t num_values) {
  impl_->WriteValues(values, num_values);
}

void SizeStatisticsBuilder::WriteValues(const ::arrow::Array& values) {
  impl_->WriteValues(values);
}

std::unique_ptr<SizeStatistics> SizeStatisticsBuilder::Build() { return impl_->Build(); }

void SizeStatisticsBuilder::Reset() { return impl_->Reset(); }

SizeStatisticsBuilder::SizeStatisticsBuilder(const ColumnDescriptor* descr)
    : impl_(std::make_unique<SizeStatisticsBuilderImpl>(descr)) {}

SizeStatisticsBuilder::~SizeStatisticsBuilder() = default;

std::unique_ptr<SizeStatisticsBuilder> SizeStatisticsBuilder::Make(
    const ColumnDescriptor* descr) {
  return std::unique_ptr<SizeStatisticsBuilder>(new SizeStatisticsBuilder(descr));
}

}  // namespace parquet

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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>

#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/macros.h"
#include "parquet/util/memory.h"
#include "parquet/util/visibility.h"

namespace parquet {

// ----------------------------------------------------------------------
// Value comparator interfaces

/// \brief Base class for value comparators. Use with
/// TypedComparator<T>
class PARQUET_EXPORT Comparator {
 public:
  virtual ~Comparator() {}
  static std::shared_ptr<Comparator> Make(const ColumnDescriptor* descr);
};

/// \brief Interface for comparison of physical types according to the
/// semantics of a particular logical type
template <typename DType>
class TypedComparator : public Comparator {
 public:
  using T = typename DType::c_type;

  /// \brief Scalar comparison of two elements, return true if first
  /// is strictly less than the second
  virtual bool Compare(const T& a, const T& b) = 0;
};

// ----------------------------------------------------------------------

class PARQUET_EXPORT EncodedStatistics {
  std::shared_ptr<std::string> max_, min_;

 public:
  EncodedStatistics()
      : max_(std::make_shared<std::string>()), min_(std::make_shared<std::string>()) {}

  const std::string& max() const { return *max_; }
  const std::string& min() const { return *min_; }

  int64_t null_count = 0;
  int64_t distinct_count = 0;

  bool has_min = false;
  bool has_max = false;
  bool has_null_count = false;
  bool has_distinct_count = false;

  inline bool is_set() const {
    return has_min || has_max || has_null_count || has_distinct_count;
  }

  // larger of the max_ and min_ stat values
  inline size_t max_stat_length() { return std::max(max_->length(), min_->length()); }

  inline EncodedStatistics& set_max(const std::string& value) {
    *max_ = value;
    has_max = true;
    return *this;
  }

  inline EncodedStatistics& set_min(const std::string& value) {
    *min_ = value;
    has_min = true;
    return *this;
  }

  inline EncodedStatistics& set_null_count(int64_t value) {
    null_count = value;
    has_null_count = true;
    return *this;
  }

  inline EncodedStatistics& set_distinct_count(int64_t value) {
    distinct_count = value;
    has_distinct_count = true;
    return *this;
  }
};

class PARQUET_EXPORT Statistics
    : public std::enable_shared_from_this<Statistics> {
 public:
  static std::shared_ptr<Statistics> Make(
      const ColumnDescriptor* schema,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  static  std::shared_ptr<Statistics> Make(
      const ColumnDescriptor* schema,
      const std::string& encoded_min,
      const std::string& encoded_max, int64_t num_values,
      int64_t null_count, int64_t distinct_count,
      bool has_min_max,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  int64_t null_count() const { return statistics_.null_count; }
  int64_t distinct_count() const { return statistics_.distinct_count; }
  int64_t num_values() const { return num_values_; }

  virtual bool HasMinMax() const = 0;
  virtual void Reset() = 0;

  // Plain-encoded minimum value
  virtual std::string EncodeMin() = 0;

  // Plain-encoded maximum value
  virtual std::string EncodeMax() = 0;

  virtual EncodedStatistics Encode() = 0;

  virtual ~Statistics() {}

  Type::type physical_type() const { return descr_->physical_type(); }

 protected:
  const ColumnDescriptor* descr() const { return descr_; }

  void IncrementNullCount(int64_t n) { statistics_.null_count += n; }

  void IncrementNumValues(int64_t n) { num_values_ += n; }

  void IncrementDistinctCount(int64_t n) { statistics_.distinct_count += n; }

  void MergeCounts(const Statistics& other) {
    this->statistics_.null_count += other.statistics_.null_count;
    this->statistics_.distinct_count += other.statistics_.distinct_count;
    this->num_values_ += other.num_values_;
  }

  void ResetCounts() {
    this->statistics_.null_count = 0;
    this->statistics_.distinct_count = 0;
    this->num_values_ = 0;
  }

  const ColumnDescriptor* descr_ = NULLPTR;
  int64_t num_values_ = 0;
  EncodedStatistics statistics_;
};

template <typename DType>
class TypedStatistics : public Statistics {
 public:
  using T = typename DType::c_type;

  void Update(const T* values, int64_t num_not_null, int64_t num_null) = 0;
  void UpdateSpaced(const T* values, const uint8_t* valid_bits, int64_t valid_bits_spaced,
                    int64_t num_not_null, int64_t num_null) = 0;
  void SetMinMax(const T& min, const T& max) = 0;
};

using BoolStatistics = TypedStatistics<BooleanType>;
using Int32Statistics = TypedStatistics<Int32Type>;
using Int64Statistics = TypedStatistics<Int64Type>;
using Int96Statistics = TypedStatistics<Int96Type>;
using FloatStatistics = TypedStatistics<FloatType>;
using DoubleStatistics = TypedStatistics<DoubleType>;
using ByteArrayStatistics = TypedStatistics<ByteArrayType>;
using FLBAStatistics = TypedStatistics<FLBAType>;

}  // namespace parquet

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

#include "arrow/util/logging.h"
#include "parquet/exception.h"
#include "parquet/schema.h"

namespace parquet {

void SizeStatistics::Merge(const SizeStatistics& other) {
  if (repetition_level_histogram.size() != other.repetition_level_histogram.size()) {
    throw ParquetException("Repetition level histogram size mismatch");
  }
  if (definition_level_histogram.size() != other.definition_level_histogram.size()) {
    throw ParquetException("Definition level histogram size mismatch");
  }
  if (unencoded_byte_array_data_bytes.has_value() !=
      other.unencoded_byte_array_data_bytes.has_value()) {
    throw ParquetException("Unencoded byte array data bytes are not consistent");
  }
  std::transform(repetition_level_histogram.begin(), repetition_level_histogram.end(),
                 other.repetition_level_histogram.begin(),
                 repetition_level_histogram.begin(), std::plus<>());
  std::transform(definition_level_histogram.begin(), definition_level_histogram.end(),
                 other.definition_level_histogram.begin(),
                 definition_level_histogram.begin(), std::plus<>());
  if (unencoded_byte_array_data_bytes.has_value()) {
    unencoded_byte_array_data_bytes = unencoded_byte_array_data_bytes.value() +
                                      other.unencoded_byte_array_data_bytes.value();
  }
}

void SizeStatistics::IncrementUnencodedByteArrayDataBytes(int64_t value) {
  ARROW_CHECK(unencoded_byte_array_data_bytes.has_value());
  unencoded_byte_array_data_bytes = unencoded_byte_array_data_bytes.value() + value;
}

void SizeStatistics::Validate(const ColumnDescriptor* descr) const {
  auto validate_histogram = [](const std::vector<int64_t>& histogram, int16_t max_level,
                               const std::string& name) {
    if (histogram.empty()) {
      // A levels histogram is always allowed to be missing.
      return;
    }
    if (histogram.size() != static_cast<size_t>(max_level + 1)) {
      std::stringstream ss;
      ss << name << " level histogram size mismatch, size: " << histogram.size()
         << ", expected: " << (max_level + 1);
      throw ParquetException(ss.str());
    }
  };
  validate_histogram(repetition_level_histogram, descr->max_repetition_level(),
                     "Repetition");
  validate_histogram(definition_level_histogram, descr->max_definition_level(),
                     "Definition");
  if (unencoded_byte_array_data_bytes.has_value() &&
      descr->physical_type() != Type::BYTE_ARRAY) {
    throw ParquetException("Unencoded byte array data bytes does not support " +
                           TypeToString(descr->physical_type()));
  }
}

void SizeStatistics::Reset() {
  repetition_level_histogram.assign(repetition_level_histogram.size(), 0);
  definition_level_histogram.assign(definition_level_histogram.size(), 0);
  if (unencoded_byte_array_data_bytes.has_value()) {
    unencoded_byte_array_data_bytes = 0;
  }
}

std::unique_ptr<SizeStatistics> SizeStatistics::Make(const ColumnDescriptor* descr) {
  auto size_stats = std::make_unique<SizeStatistics>();
  // If the max level is 0, the level histogram can be omitted because it contains
  // only single level (a.k.a. 0) and its count is equivalent to `num_values` of the
  // column chunk or data page.
  if (descr->max_repetition_level() != 0) {
    size_stats->repetition_level_histogram.resize(descr->max_repetition_level() + 1, 0);
  }
  if (descr->max_definition_level() != 0) {
    size_stats->definition_level_histogram.resize(descr->max_definition_level() + 1, 0);
  }
  if (descr->physical_type() == Type::BYTE_ARRAY) {
    size_stats->unencoded_byte_array_data_bytes = 0;
  }
  return size_stats;
}

}  // namespace parquet

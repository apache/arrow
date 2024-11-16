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

void SizeStatistics::Merge(const SizeStatistics& other) {
  ARROW_CHECK_EQ(repetition_level_histogram.size(),
                 other.repetition_level_histogram.size());
  ARROW_CHECK_EQ(definition_level_histogram.size(),
                 other.definition_level_histogram.size());
  ARROW_CHECK_EQ(unencoded_byte_array_data_bytes.has_value(),
                 other.unencoded_byte_array_data_bytes.has_value());
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

void SizeStatistics::Reset() {
  repetition_level_histogram.assign(repetition_level_histogram.size(), 0);
  definition_level_histogram.assign(definition_level_histogram.size(), 0);
  if (unencoded_byte_array_data_bytes.has_value()) {
    unencoded_byte_array_data_bytes = 0;
  }
}

std::unique_ptr<SizeStatistics> MakeSizeStatistics(const ColumnDescriptor* descr) {
  auto size_stats = std::make_unique<SizeStatistics>();
  size_stats->repetition_level_histogram.resize(descr->max_repetition_level() + 1, 0);
  size_stats->definition_level_histogram.resize(descr->max_definition_level() + 1, 0);
  if (descr->physical_type() == Type::BYTE_ARRAY) {
    size_stats->unencoded_byte_array_data_bytes = 0;
  }
  return size_stats;
}

}  // namespace parquet

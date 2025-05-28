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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#include "parquet/bloom_filter_builder_internal.h"

#include <map>
#include <utility>
#include <vector>

#include "arrow/io/interfaces.h"
#include "arrow/util/logging.h"

#include "parquet/bloom_filter.h"
#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"

namespace parquet {

namespace {
/// BloomFilterBuilderImpl is the implementation of BloomFilterBuilder.
///
/// Note: Column encryption for bloom filter is not implemented yet.
class BloomFilterBuilderImpl : public BloomFilterBuilder {
 public:
  explicit BloomFilterBuilderImpl(const SchemaDescriptor* schema,
                                  const WriterProperties* properties)
      : schema_(schema), properties_(properties) {}
  BloomFilterBuilderImpl(const BloomFilterBuilderImpl&) = delete;
  BloomFilterBuilderImpl(BloomFilterBuilderImpl&&) = default;

  /// Append a new row group to host all incoming bloom filters.
  void AppendRowGroup() override;

  BloomFilter* GetOrCreateBloomFilter(int32_t column_ordinal) override;

  /// Serialize all bloom filters with header and bitset in the order of row group and
  /// column id. The side effect is that it deletes all bloom filters after they have
  /// been flushed.
  void WriteTo(::arrow::io::OutputStream* sink, BloomFilterLocation* location) override;

 private:
  /// Make sure column ordinal is not out of bound and the builder is in good state.
  void CheckState(int32_t column_ordinal) const {
    if (finished_) {
      throw ParquetException("BloomFilterBuilder is already finished.");
    }
    if (column_ordinal < 0 || column_ordinal >= schema_->num_columns()) {
      throw ParquetException("Invalid column ordinal: ", column_ordinal);
    }
    if (file_bloom_filters_.empty()) {
      throw ParquetException("No row group appended to BloomFilterBuilder.");
    }
    if (schema_->Column(column_ordinal)->physical_type() == Type::BOOLEAN) {
      throw ParquetException("BloomFilterBuilder does not support boolean type.");
    }
  }

  const SchemaDescriptor* schema_;
  const WriterProperties* properties_;
  bool finished_ = false;

  using RowGroupBloomFilters = std::map<int32_t, std::unique_ptr<BloomFilter>>;
  // Using unique_ptr because the `RowGroupBloomFilters` is not copyable.
  // MSVC has the issue below: https://github.com/microsoft/STL/issues/1036
  // So we use `std::unique_ptr<std::map<>>` to avoid the issue.
  std::vector<std::unique_ptr<RowGroupBloomFilters>> file_bloom_filters_;
};

void BloomFilterBuilderImpl::AppendRowGroup() {
  if (finished_) {
    throw ParquetException("Cannot append to a finished BloomFilterBuilder");
  }
  file_bloom_filters_.emplace_back(std::make_unique<RowGroupBloomFilters>());
}

BloomFilter* BloomFilterBuilderImpl::GetOrCreateBloomFilter(int32_t column_ordinal) {
  CheckState(column_ordinal);
  const ColumnDescriptor* column_descr = schema_->Column(column_ordinal);
  // Bloom filter does not support boolean type, and this should be checked in
  // CheckState() already.
  ARROW_DCHECK_NE(column_descr->physical_type(), Type::BOOLEAN);
  auto bloom_filter_options_opt = properties_->bloom_filter_options(column_descr->path());
  if (bloom_filter_options_opt == std::nullopt) {
    return nullptr;
  }
  const BloomFilterOptions& bloom_filter_options = *bloom_filter_options_opt;
  // CheckState() should have checked that file_bloom_filters_ is not empty.
  RowGroupBloomFilters& row_group_bloom_filter = *file_bloom_filters_.back();
  auto iter = row_group_bloom_filter.find(column_ordinal);
  if (iter == row_group_bloom_filter.end()) {
    auto block_split_bloom_filter =
        std::make_unique<BlockSplitBloomFilter>(properties_->memory_pool());
    block_split_bloom_filter->Init(BlockSplitBloomFilter::OptimalNumOfBytes(
        bloom_filter_options.ndv, bloom_filter_options.fpp));
    auto insert_result = row_group_bloom_filter.emplace(
        column_ordinal, std::move(block_split_bloom_filter));
    iter = insert_result.first;
  }
  if (iter->second == nullptr) {
    throw ParquetException("Bloom filter should not be null for column ",
                           column_descr->path());
  }
  return iter->second.get();
}

void BloomFilterBuilderImpl::WriteTo(::arrow::io::OutputStream* sink,
                                     BloomFilterLocation* location) {
  if (finished_) {
    throw ParquetException("Cannot write a finished BloomFilterBuilder");
  }
  finished_ = true;

  for (size_t row_group_ordinal = 0; row_group_ordinal < file_bloom_filters_.size();
       ++row_group_ordinal) {
    RowGroupBloomFilters& row_group_bloom_filters =
        *file_bloom_filters_[row_group_ordinal];
    // the whole row group has no bloom filter
    if (row_group_bloom_filters.empty()) {
      continue;
    }
    int num_columns = schema_->num_columns();
    RowGroupBloomFilterLocation locations;

    // serialize bloom filter in ascending order of column id
    for (auto& [column_id, filter] : row_group_bloom_filters) {
      if (ARROW_PREDICT_FALSE(filter == nullptr)) {
        throw ParquetException("Bloom filter is null for column ", column_id);
      }
      if (ARROW_PREDICT_FALSE(column_id < 0 || column_id >= num_columns)) {
        throw ParquetException("Invalid column ordinal when serializing bloom filter: ",
                               column_id);
      }
      PARQUET_ASSIGN_OR_THROW(int64_t offset, sink->Tell());
      // TODO(GH-43138): Estimate the quality of the bloom filter before writing it.
      filter->WriteTo(sink);
      PARQUET_ASSIGN_OR_THROW(int64_t pos, sink->Tell());
      if (pos - offset > std::numeric_limits<int32_t>::max()) {
        throw ParquetException("Bloom filter is too large to be serialized, size: ",
                               pos - offset, " for column ", column_id);
      }
      locations[column_id] = IndexLocation{offset, static_cast<int32_t>(pos - offset)};
    }
    location->bloom_filter_location.emplace(row_group_ordinal, std::move(locations));
  }
}
}  // namespace

std::unique_ptr<BloomFilterBuilder> BloomFilterBuilder::Make(
    const SchemaDescriptor* schema, const WriterProperties* properties) {
  return std::make_unique<BloomFilterBuilderImpl>(schema, properties);
}

}  // namespace parquet

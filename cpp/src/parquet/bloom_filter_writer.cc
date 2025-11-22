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

#include "parquet/bloom_filter_writer.h"

#include <map>
#include <utility>

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/unreachable.h"

#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

constexpr int64_t kHashBatchSize = 256;

template <typename ParquetType>
BloomFilterWriter<ParquetType>::BloomFilterWriter(const ColumnDescriptor* descr,
                                                  BloomFilter* bloom_filter)
    : descr_(descr), bloom_filter_(bloom_filter) {}

template <typename ParquetType>
bool BloomFilterWriter<ParquetType>::bloom_filter_enabled() const {
  return bloom_filter_ != nullptr;
}

template <typename ParquetType>
void BloomFilterWriter<ParquetType>::Update(const T* values, int64_t num_values) {
  if (!bloom_filter_enabled()) {
    return;
  }

  if constexpr (std::is_same_v<ParquetType, BooleanType>) {
    throw ParquetException("Bloom filter is not supported for boolean type");
  }

  std::array<uint64_t, kHashBatchSize> hashes;
  for (int64_t i = 0; i < num_values; i += kHashBatchSize) {
    auto batch_size = static_cast<int>(std::min(kHashBatchSize, num_values - i));
    if constexpr (std::is_same_v<ParquetType, FLBAType>) {
      bloom_filter_->Hashes(values, descr_->type_length(), batch_size, hashes.data());
    } else {
      bloom_filter_->Hashes(values, batch_size, hashes.data());
    }
    bloom_filter_->InsertHashes(hashes.data(), batch_size);
  }
}

template <>
void BloomFilterWriter<BooleanType>::Update(const bool*, int64_t) {
  if (!bloom_filter_enabled()) {
    return;
  }
  throw ParquetException("Bloom filter is not supported for boolean type");
}

template <typename ParquetType>
void BloomFilterWriter<ParquetType>::UpdateSpaced(const T* values, int64_t num_values,
                                                  const uint8_t* valid_bits,
                                                  int64_t valid_bits_offset) {
  if (!bloom_filter_enabled()) {
    return;
  }

  std::array<uint64_t, kHashBatchSize> hashes;
  ::arrow::internal::VisitSetBitRunsVoid(
      valid_bits, valid_bits_offset, num_values, [&](int64_t position, int64_t length) {
        for (int64_t i = 0; i < length; i += kHashBatchSize) {
          auto batch_size = static_cast<int>(std::min(kHashBatchSize, length - i));
          if constexpr (std::is_same_v<ParquetType, FLBAType>) {
            bloom_filter_->Hashes(values + i + position, descr_->type_length(),
                                  batch_size, hashes.data());
          } else {
            bloom_filter_->Hashes(values + i + position, batch_size, hashes.data());
          }
          bloom_filter_->InsertHashes(hashes.data(), batch_size);
        }
      });
}

template <>
void BloomFilterWriter<BooleanType>::UpdateSpaced(const bool*, int64_t, const uint8_t*,
                                                  int64_t) {
  if (!bloom_filter_enabled()) {
    return;
  }
  throw ParquetException("Bloom filter is not supported for boolean type");
}

template <typename ParquetType>
void BloomFilterWriter<ParquetType>::Update(const ::arrow::Array& values) {
  ::arrow::Unreachable("Update for non-ByteArray type should be unreachable");
}

namespace {

template <typename ArrayType>
void UpdateBinaryBloomFilter(BloomFilter& bloom_filter, const ArrayType& array) {
  // Using a small batch size because an extra `byte_arrays` is used.
  constexpr int64_t kBinaryHashBatchSize = 64;
  std::array<ByteArray, kBinaryHashBatchSize> byte_arrays;
  std::array<uint64_t, kBinaryHashBatchSize> hashes;

  auto batch_insert_hashes = [&](int count) {
    if (count > 0) {
      bloom_filter.Hashes(byte_arrays.data(), count, hashes.data());
      bloom_filter.InsertHashes(hashes.data(), count);
    }
  };

  int batch_idx = 0;
  ::arrow::internal::VisitSetBitRunsVoid(
      array.null_bitmap_data(), array.offset(), array.length(),
      [&](int64_t position, int64_t run_length) {
        for (int64_t i = 0; i < run_length; ++i) {
          byte_arrays[batch_idx++] = array.GetView(position + i);
          if (batch_idx == kBinaryHashBatchSize) {
            batch_insert_hashes(batch_idx);
            batch_idx = 0;
          }
        }
      });
  batch_insert_hashes(batch_idx);
}

}  // namespace

template <>
void BloomFilterWriter<ByteArrayType>::Update(const ::arrow::Array& values) {
  if (!bloom_filter_enabled()) {
    return;
  }

  if (::arrow::is_binary_view_like(values.type_id())) {
    UpdateBinaryBloomFilter(
        *bloom_filter_,
        ::arrow::internal::checked_cast<const ::arrow::BinaryViewArray&>(values));
  } else if (::arrow::is_binary_like(values.type_id())) {
    UpdateBinaryBloomFilter(
        *bloom_filter_,
        ::arrow::internal::checked_cast<const ::arrow::BinaryArray&>(values));
  } else if (::arrow::is_large_binary_like(values.type_id())) {
    UpdateBinaryBloomFilter(
        *bloom_filter_,
        ::arrow::internal::checked_cast<const ::arrow::LargeBinaryArray&>(values));
  } else {
    ParquetException::NYI("Bloom filter is not supported for this Arrow type: " +
                          values.type()->ToString());
  }
}

template class BloomFilterWriter<BooleanType>;
template class BloomFilterWriter<Int32Type>;
template class BloomFilterWriter<Int64Type>;
template class BloomFilterWriter<Int96Type>;
template class BloomFilterWriter<FloatType>;
template class BloomFilterWriter<DoubleType>;
template class BloomFilterWriter<ByteArrayType>;
template class BloomFilterWriter<FLBAType>;

namespace {

/// \brief A concrete implementation of BloomFilterBuilder.
///
/// \note Column encryption for bloom filter is not implemented yet.
class BloomFilterBuilderImpl : public BloomFilterBuilder {
 public:
  BloomFilterBuilderImpl(const SchemaDescriptor* schema,
                         const WriterProperties* properties)
      : schema_(schema), properties_(properties) {}

  void AppendRowGroup() override;

  BloomFilter* GetOrCreateBloomFilter(int32_t column_ordinal) override;

  void WriteTo(::arrow::io::OutputStream* sink, IndexLocations* location) override;

 private:
  /// Make sure column ordinal is not out of bound and the builder is in good state.
  void CheckState(int32_t column_ordinal) const {
    if (finished_) {
      throw ParquetException("BloomFilterBuilder is already finished.");
    }
    if (bloom_filters_.empty()) {
      throw ParquetException("No row group appended to BloomFilterBuilder");
    }
    if (column_ordinal < 0 || column_ordinal >= schema_->num_columns()) {
      throw ParquetException("Invalid column ordinal: " + std::to_string(column_ordinal));
    }
    if (schema_->Column(column_ordinal)->physical_type() == Type::BOOLEAN) {
      throw ParquetException("BloomFilterBuilder does not support boolean type.");
    }
  }

  const SchemaDescriptor* schema_;
  const WriterProperties* properties_;
  bool finished_ = false;

  using RowGroupBloomFilters = std::map<int32_t, std::shared_ptr<BloomFilter>>;
  std::map<int32_t, RowGroupBloomFilters> bloom_filters_;
};

void BloomFilterBuilderImpl::AppendRowGroup() {
  if (finished_) {
    throw ParquetException(
        "Cannot append a new row group to a finished BloomFilterBuilder");
  }
  bloom_filters_.emplace(bloom_filters_.size(), RowGroupBloomFilters());
}

BloomFilter* BloomFilterBuilderImpl::GetOrCreateBloomFilter(int32_t column_ordinal) {
  CheckState(column_ordinal);

  const auto bloom_filter_options =
      properties_->bloom_filter_options(schema_->Column(column_ordinal)->path());
  if (!bloom_filter_options.has_value()) {
    return nullptr;
  }

  auto& row_group_bloom_filters = bloom_filters_.rbegin()->second;

  // Get or create bloom filter of the column.
  auto bloom_filter_iter = row_group_bloom_filters.find(column_ordinal);
  if (bloom_filter_iter == row_group_bloom_filters.cend()) {
    auto bloom_filter =
        std::make_unique<BlockSplitBloomFilter>(properties_->memory_pool());
    bloom_filter->Init(BlockSplitBloomFilter::OptimalNumOfBytes(
        bloom_filter_options->ndv, bloom_filter_options->fpp));
    bloom_filter_iter =
        row_group_bloom_filters.emplace(column_ordinal, std::move(bloom_filter)).first;
  }

  return bloom_filter_iter->second.get();
}

void BloomFilterBuilderImpl::WriteTo(::arrow::io::OutputStream* sink,
                                     IndexLocations* location) {
  if (finished_) {
    throw ParquetException("Cannot write a finished BloomFilterBuilder");
  }
  finished_ = true;

  location->type = IndexLocations::IndexType::kBloomFilter;
  location->locations.clear();

  for (const auto& [row_group_ordinal, row_group_bloom_filters] : bloom_filters_) {
    std::map<size_t, IndexLocation> column_id_to_location;
    for (const auto& [column_id, filter] : row_group_bloom_filters) {
      if (ARROW_PREDICT_FALSE(filter == nullptr)) {
        throw ParquetException("Bloom filter cannot be null");
      }

      // TODO(GH-43138): Determine the quality of bloom filter before writing it.
      PARQUET_ASSIGN_OR_THROW(int64_t offset, sink->Tell());
      filter->WriteTo(sink);
      PARQUET_ASSIGN_OR_THROW(int64_t pos, sink->Tell());

      if (pos - offset > std::numeric_limits<int32_t>::max()) {
        throw ParquetException(
            "Bloom filter size is too large, size: " + std::to_string(pos - offset) +
            ", column: " + std::to_string(column_id) +
            ", row group: " + std::to_string(row_group_ordinal));
      }

      column_id_to_location.emplace(
          column_id, IndexLocation{offset, static_cast<int32_t>(pos - offset)});
    }

    if (!column_id_to_location.empty()) {
      location->locations.emplace(row_group_ordinal, std::move(column_id_to_location));
    }
  }
}

}  // namespace

std::unique_ptr<BloomFilterBuilder> BloomFilterBuilder::Make(
    const SchemaDescriptor* schema, const WriterProperties* properties) {
  return std::make_unique<BloomFilterBuilderImpl>(schema, properties);
}

}  // namespace parquet

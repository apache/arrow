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

#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

constexpr int64_t kHashBatchSize = 256;

template <typename ParquetType>
TypedBloomFilterWriter<ParquetType>::TypedBloomFilterWriter(const ColumnDescriptor* descr,
                                                            BloomFilter* bloom_filter)
    : descr_(descr), bloom_filter_(bloom_filter) {}

template <typename ParquetType>
void TypedBloomFilterWriter<ParquetType>::Update(const T* values, int64_t num_values) {
  ARROW_DCHECK(bloom_filter_ != nullptr);
  std::array<uint64_t, kHashBatchSize> hashes;
  for (int64_t i = 0; i < num_values; i += kHashBatchSize) {
    auto batch_size = static_cast<int>(std::min(kHashBatchSize, num_values - i));
    if constexpr (std::is_same_v<ParquetType, FLBAType>) {
      bloom_filter_->Hashes(values + i, descr_->type_length(), batch_size, hashes.data());
    } else {
      bloom_filter_->Hashes(values + i, batch_size, hashes.data());
    }
    bloom_filter_->InsertHashes(hashes.data(), batch_size);
  }
}

template <>
void TypedBloomFilterWriter<BooleanType>::Update(const bool*, int64_t) {
  throw ParquetException("Bloom filter is not supported for boolean type");
}

template <typename ParquetType>
void TypedBloomFilterWriter<ParquetType>::UpdateSpaced(const T* values,
                                                       int64_t num_values,
                                                       const uint8_t* valid_bits,
                                                       int64_t valid_bits_offset) {
  ARROW_DCHECK(bloom_filter_ != nullptr);
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
void TypedBloomFilterWriter<BooleanType>::UpdateSpaced(const bool*, int64_t,
                                                       const uint8_t*, int64_t) {
  throw ParquetException("Bloom filter is not supported for boolean type");
}

template <typename ParquetType>
void TypedBloomFilterWriter<ParquetType>::Update(const ::arrow::Array& values) {
  ParquetException::NYI("Updating bloom filter is not implemented for array of type: " +
                        values.type()->ToString());
}

namespace {

template <typename ArrayType>
void UpdateBinaryBloomFilter(BloomFilter& bloom_filter, const ArrayType& array) {
  std::array<ByteArray, kHashBatchSize> byte_arrays;
  std::array<uint64_t, kHashBatchSize> hashes;
  ::arrow::internal::VisitSetBitRunsVoid(
      array.null_bitmap_data(), array.offset(), array.length(),
      [&](int64_t position, int64_t length) {
        for (int64_t i = 0; i < length; i += kHashBatchSize) {
          auto batch_size = static_cast<int>(std::min(kHashBatchSize, length - i));
          for (int j = 0; j < batch_size; j++) {
            byte_arrays[j] = array.GetView(position + i + j);
          }
          bloom_filter.Hashes(byte_arrays.data(), batch_size, hashes.data());
          bloom_filter.InsertHashes(hashes.data(), batch_size);
        }
      });
}

}  // namespace

template <>
void TypedBloomFilterWriter<ByteArrayType>::Update(const ::arrow::Array& values) {
  ARROW_DCHECK(bloom_filter_ != nullptr);
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

template class TypedBloomFilterWriter<BooleanType>;
template class TypedBloomFilterWriter<Int32Type>;
template class TypedBloomFilterWriter<Int64Type>;
template class TypedBloomFilterWriter<Int96Type>;
template class TypedBloomFilterWriter<FloatType>;
template class TypedBloomFilterWriter<DoubleType>;
template class TypedBloomFilterWriter<ByteArrayType>;
template class TypedBloomFilterWriter<FLBAType>;

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

  BloomFilter* CreateBloomFilter(int32_t column_ordinal) override;

  IndexLocations WriteTo(::arrow::io::OutputStream* sink) override;

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

  using RowGroupBloomFilters =
      std::map</*column_id=*/int32_t, std::shared_ptr<BloomFilter>>;
  std::vector<RowGroupBloomFilters> bloom_filters_;  // indexed by row group ordinal
};

void BloomFilterBuilderImpl::AppendRowGroup() {
  if (finished_) {
    throw ParquetException(
        "Cannot append a new row group to a finished BloomFilterBuilder");
  }
  bloom_filters_.emplace_back();
}

BloomFilter* BloomFilterBuilderImpl::CreateBloomFilter(int32_t column_ordinal) {
  CheckState(column_ordinal);

  auto opts = properties_->bloom_filter_options(schema_->Column(column_ordinal)->path());
  if (!opts.has_value()) {
    return nullptr;
  }

  auto& curr_rg_bfs = *bloom_filters_.rbegin();
  if (curr_rg_bfs.find(column_ordinal) != curr_rg_bfs.cend()) {
    std::stringstream ss;
    ss << "Bloom filter already exists for column: " << column_ordinal
       << ", row group: " << (bloom_filters_.size() - 1);
    throw ParquetException(ss.str());
  }

  auto bf = std::make_unique<BlockSplitBloomFilter>(properties_->memory_pool());
  bf->Init(BlockSplitBloomFilter::OptimalNumOfBytes(opts->ndv, opts->fpp));
  return curr_rg_bfs.emplace(column_ordinal, std::move(bf)).first->second.get();
}

IndexLocations BloomFilterBuilderImpl::WriteTo(::arrow::io::OutputStream* sink) {
  if (finished_) {
    throw ParquetException("Cannot write a finished BloomFilterBuilder");
  }
  finished_ = true;

  IndexLocations locations;

  for (size_t i = 0; i != bloom_filters_.size(); ++i) {
    auto& row_group_bloom_filters = bloom_filters_[i];
    for (const auto& [column_id, filter] : row_group_bloom_filters) {
      // TODO(GH-43138): Determine the quality of bloom filter before writing it.
      PARQUET_ASSIGN_OR_THROW(int64_t offset, sink->Tell());
      filter->WriteTo(sink);
      PARQUET_ASSIGN_OR_THROW(int64_t pos, sink->Tell());

      if (pos - offset > std::numeric_limits<int32_t>::max()) {
        throw ParquetException(
            "Bloom filter size is too large, size: " + std::to_string(pos - offset) +
            ", column: " + std::to_string(column_id) +
            ", row group: " + std::to_string(i));
      }

      locations.emplace_back(ColumnChunkId{static_cast<int32_t>(i), column_id},
                             IndexLocation{offset, static_cast<int32_t>(pos - offset)});
    }
  }

  return locations;
}

}  // namespace

std::unique_ptr<BloomFilterBuilder> BloomFilterBuilder::Make(
    const SchemaDescriptor* schema, const WriterProperties* properties) {
  return std::make_unique<BloomFilterBuilderImpl>(schema, properties);
}

}  // namespace parquet

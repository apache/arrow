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

#include "parquet/page_index.h"
#include "parquet/encoding.h"
#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/thrift_internal.h"

#include "arrow/util/unreachable.h"

#include <limits>
#include <numeric>

namespace parquet {

namespace {

template <typename DType>
void Decode(std::unique_ptr<typename EncodingTraits<DType>::Decoder>& decoder,
            const std::string& input, std::vector<typename DType::c_type>* output,
            size_t output_index) {
  if (ARROW_PREDICT_FALSE(output_index >= output->size())) {
    throw ParquetException("Index out of bound");
  }

  decoder->SetData(/*num_values=*/1, reinterpret_cast<const uint8_t*>(input.c_str()),
                   static_cast<int>(input.size()));
  const auto num_values = decoder->Decode(&output->at(output_index), /*max_values=*/1);
  if (ARROW_PREDICT_FALSE(num_values != 1)) {
    throw ParquetException("Could not decode statistics value");
  }
}

template <>
void Decode<BooleanType>(std::unique_ptr<BooleanDecoder>& decoder,
                         const std::string& input, std::vector<bool>* output,
                         size_t output_index) {
  if (ARROW_PREDICT_FALSE(output_index >= output->size())) {
    throw ParquetException("Index out of bound");
  }

  bool value;
  decoder->SetData(/*num_values=*/1, reinterpret_cast<const uint8_t*>(input.c_str()),
                   static_cast<int>(input.size()));
  const auto num_values = decoder->Decode(&value, /*max_values=*/1);
  if (ARROW_PREDICT_FALSE(num_values != 1)) {
    throw ParquetException("Could not decode statistics value");
  }
  output->at(output_index) = value;
}

template <>
void Decode<ByteArrayType>(std::unique_ptr<ByteArrayDecoder>&, const std::string& input,
                           std::vector<ByteArray>* output, size_t output_index) {
  if (ARROW_PREDICT_FALSE(output_index >= output->size())) {
    throw ParquetException("Index out of bound");
  }

  if (ARROW_PREDICT_FALSE(input.size() >
                          static_cast<size_t>(std::numeric_limits<uint32_t>::max()))) {
    throw ParquetException("Invalid encoded byte array length");
  }

  output->at(output_index) = {/*len=*/static_cast<uint32_t>(input.size()),
                              /*ptr=*/reinterpret_cast<const uint8_t*>(input.data())};
}

template <typename DType>
class TypedColumnIndexImpl : public TypedColumnIndex<DType> {
 public:
  using T = typename DType::c_type;

  TypedColumnIndexImpl(const ColumnDescriptor& descr,
                       const format::ColumnIndex& column_index)
      : column_index_(column_index) {
    // Make sure the number of pages is valid and it does not overflow to int32_t.
    const size_t num_pages = column_index_.null_pages.size();
    if (num_pages >= static_cast<size_t>(std::numeric_limits<int32_t>::max()) ||
        column_index_.min_values.size() != num_pages ||
        column_index_.max_values.size() != num_pages ||
        (column_index_.__isset.null_counts &&
         column_index_.null_counts.size() != num_pages)) {
      throw ParquetException("Invalid column index");
    }

    const size_t num_non_null_pages = static_cast<size_t>(std::accumulate(
        column_index_.null_pages.cbegin(), column_index_.null_pages.cend(), 0,
        [](int32_t num_non_null_pages, bool null_page) {
          return num_non_null_pages + (null_page ? 0 : 1);
        }));
    DCHECK_LE(num_non_null_pages, num_pages);

    // Allocate slots for decoded values.
    min_values_.resize(num_pages);
    max_values_.resize(num_pages);
    non_null_page_indices_.reserve(num_non_null_pages);

    // Decode min and max values according to the physical type.
    // Note that null page are skipped.
    auto plain_decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, &descr);
    for (size_t i = 0; i < num_pages; ++i) {
      if (!column_index_.null_pages[i]) {
        // The check on `num_pages` has guaranteed the cast below is safe.
        non_null_page_indices_.emplace_back(static_cast<int32_t>(i));
        Decode<DType>(plain_decoder, column_index_.min_values[i], &min_values_, i);
        Decode<DType>(plain_decoder, column_index_.max_values[i], &max_values_, i);
      }
    }
    DCHECK_EQ(num_non_null_pages, non_null_page_indices_.size());
  }

  const std::vector<bool>& null_pages() const override {
    return column_index_.null_pages;
  }

  const std::vector<std::string>& encoded_min_values() const override {
    return column_index_.min_values;
  }

  const std::vector<std::string>& encoded_max_values() const override {
    return column_index_.max_values;
  }

  BoundaryOrder::type boundary_order() const override {
    return LoadEnumSafe(&column_index_.boundary_order);
  }

  bool has_null_counts() const override { return column_index_.__isset.null_counts; }

  const std::vector<int64_t>& null_counts() const override {
    return column_index_.null_counts;
  }

  const std::vector<int32_t>& non_null_page_indices() const override {
    return non_null_page_indices_;
  }

  const std::vector<T>& min_values() const override { return min_values_; }

  const std::vector<T>& max_values() const override { return max_values_; }

 private:
  /// Wrapped thrift column index.
  const format::ColumnIndex column_index_;
  /// Decoded typed min/max values. Undefined for null pages.
  std::vector<T> min_values_;
  std::vector<T> max_values_;
  /// A list of page indices for non-null pages.
  std::vector<int32_t> non_null_page_indices_;
};

class OffsetIndexImpl : public OffsetIndex {
 public:
  explicit OffsetIndexImpl(const format::OffsetIndex& offset_index) {
    page_locations_.reserve(offset_index.page_locations.size());
    for (const auto& page_location : offset_index.page_locations) {
      page_locations_.emplace_back(PageLocation{page_location.offset,
                                                page_location.compressed_page_size,
                                                page_location.first_row_index});
    }
  }

  const std::vector<PageLocation>& page_locations() const override {
    return page_locations_;
  }

 private:
  std::vector<PageLocation> page_locations_;
};

class RowGroupPageIndexReaderImpl : public RowGroupPageIndexReader {
 public:
  RowGroupPageIndexReaderImpl(::arrow::io::RandomAccessFile* input,
                              std::shared_ptr<RowGroupMetaData> row_group_metadata,
                              const ReaderProperties& properties,
                              int32_t row_group_ordinal,
                              std::shared_ptr<InternalFileDecryptor> file_decryptor)
      : input_(input),
        row_group_metadata_(std::move(row_group_metadata)),
        properties_(properties),
        file_decryptor_(std::move(file_decryptor)),
        index_read_range_(
            PageIndexReader::DeterminePageIndexRangesInRowGroup(*row_group_metadata_)) {}

  /// Read column index of a column chunk.
  std::shared_ptr<ColumnIndex> GetColumnIndex(int32_t i) override {
    if (i < 0 || i >= row_group_metadata_->num_columns()) {
      throw ParquetException("Invalid column {} to get column index", i);
    }

    auto col_chunk = row_group_metadata_->ColumnChunk(i);

    std::unique_ptr<ColumnCryptoMetaData> crypto_metadata = col_chunk->crypto_metadata();
    if (crypto_metadata != nullptr && file_decryptor_ == nullptr) {
      ParquetException::NYI("Cannot read encrypted column index yet");
    }

    auto column_index_location = col_chunk->GetColumnIndexLocation();
    if (!column_index_location.has_value()) {
      return nullptr;
    }

    if (!index_read_range_.column_index.has_value()) {
      throw ParquetException("Missing column index read range");
    }

    if (column_index_buffer_ == nullptr) {
      PARQUET_ASSIGN_OR_THROW(column_index_buffer_,
                              input_->ReadAt(index_read_range_.column_index->offset,
                                             index_read_range_.column_index->length));
    }

    auto buffer = column_index_buffer_.get();
    int64_t buffer_offset =
        column_index_location->offset - index_read_range_.column_index->offset;
    uint32_t length = static_cast<uint32_t>(column_index_location->length);
    DCHECK_GE(buffer_offset, 0);
    DCHECK_LE(buffer_offset + length, index_read_range_.column_index->length);

    auto descr = row_group_metadata_->schema()->Column(i);
    std::shared_ptr<ColumnIndex> column_index;
    try {
      column_index =
          ColumnIndex::Make(*descr, buffer->data() + buffer_offset, length, properties_);
    } catch (...) {
      throw ParquetException("Cannot deserialize column index for column {}", i);
    }
    return column_index;
  }

  /// Read offset index of a column chunk.
  std::shared_ptr<OffsetIndex> GetOffsetIndex(int32_t i) override {
    if (i < 0 || i >= row_group_metadata_->num_columns()) {
      throw ParquetException("Invalid column {} to get offset index", i);
    }

    auto col_chunk = row_group_metadata_->ColumnChunk(i);

    std::unique_ptr<ColumnCryptoMetaData> crypto_metadata = col_chunk->crypto_metadata();
    if (crypto_metadata != nullptr && file_decryptor_ == nullptr) {
      ParquetException::NYI("Cannot read encrypted offset index yet");
    }

    auto offset_index_location = col_chunk->GetOffsetIndexLocation();
    if (!offset_index_location.has_value()) {
      return nullptr;
    }

    if (!index_read_range_.offset_index.has_value()) {
      throw ParquetException("Missing column index read range");
    }

    if (offset_index_buffer_ == nullptr) {
      PARQUET_ASSIGN_OR_THROW(offset_index_buffer_,
                              input_->ReadAt(index_read_range_.offset_index->offset,
                                             index_read_range_.offset_index->length));
    }

    auto buffer = offset_index_buffer_.get();
    int64_t buffer_offset =
        offset_index_location->offset - index_read_range_.offset_index->offset;
    uint32_t length = static_cast<uint32_t>(offset_index_location->length);
    DCHECK_GE(buffer_offset, 0);
    DCHECK_LE(buffer_offset + length, index_read_range_.offset_index->length);

    std::shared_ptr<OffsetIndex> offset_index;
    try {
      offset_index =
          OffsetIndex::Make(buffer->data() + buffer_offset, length, properties_);
    } catch (...) {
      throw ParquetException("Cannot deserialize offset index for column {}", i);
    }
    return offset_index;
  }

 private:
  /// The input stream that can perform random access read.
  ::arrow::io::RandomAccessFile* input_;

  /// The row group metadata to get column chunk metadata.
  std::shared_ptr<RowGroupMetaData> row_group_metadata_;

  /// Reader properties used to deserialize thrift object.
  const ReaderProperties& properties_;

  /// File-level decryptor.
  std::shared_ptr<InternalFileDecryptor> file_decryptor_;

  /// File offsets and sizes of the page Index of all column chunks in the row group.
  RowGroupIndexReadRange index_read_range_;

  /// Buffer to hold the raw bytes of the page index.
  /// Will be set lazily when the corresponding page index is accessed for the 1st time.
  std::shared_ptr<::arrow::Buffer> column_index_buffer_;
  std::shared_ptr<::arrow::Buffer> offset_index_buffer_;
};

class PageIndexReaderImpl : public PageIndexReader {
 public:
  PageIndexReaderImpl(::arrow::io::RandomAccessFile* input,
                      std::shared_ptr<FileMetaData> file_metadata,
                      const ReaderProperties& properties,
                      std::shared_ptr<InternalFileDecryptor> file_decryptor)
      : input_(input),
        file_metadata_(std::move(file_metadata)),
        properties_(properties),
        file_decryptor_(std::move(file_decryptor)) {}

  std::shared_ptr<RowGroupPageIndexReader> RowGroup(int i) override {
    if (i < 0 || i >= file_metadata_->num_row_groups()) {
      throw ParquetException("Invalid row group ordinal {}", i);
    }
    return std::make_shared<RowGroupPageIndexReaderImpl>(
        input_, file_metadata_->RowGroup(i), properties_, i, file_decryptor_);
  }

  void WillNeed(const std::vector<int32_t>& row_group_indices,
                IndexSelection index_selection) override {
    std::vector<::arrow::io::ReadRange> read_ranges;
    for (int32_t row_group_ordinal : row_group_indices) {
      auto read_range = PageIndexReader::DeterminePageIndexRangesInRowGroup(
          *file_metadata_->RowGroup(row_group_ordinal));
      if (index_selection.column_index && read_range.column_index.has_value()) {
        read_ranges.emplace_back(::arrow::io::ReadRange{read_range.column_index->offset,
                                                        read_range.column_index->length});
      }
      if (index_selection.offset_index && read_range.offset_index.has_value()) {
        read_ranges.emplace_back(::arrow::io::ReadRange{read_range.offset_index->offset,
                                                        read_range.offset_index->length});
      }
    }
    PARQUET_IGNORE_NOT_OK(input_->WillNeed(read_ranges));
  }

  void WillNotNeed(const std::vector<int32_t>& row_group_indices) override {
    // No-op for now.
  }

 private:
  /// The input stream that can perform random read.
  ::arrow::io::RandomAccessFile* input_;

  /// The file metadata to get row group metadata.
  std::shared_ptr<FileMetaData> file_metadata_;

  /// Reader properties used to deserialize thrift object.
  const ReaderProperties& properties_;

  /// File-level decrypter.
  std::shared_ptr<InternalFileDecryptor> file_decryptor_;
};

}  // namespace

RowGroupIndexReadRange PageIndexReader::DeterminePageIndexRangesInRowGroup(
    const RowGroupMetaData& row_group_metadata) {
  int64_t ci_start = std::numeric_limits<int64_t>::max();
  int64_t oi_start = std::numeric_limits<int64_t>::max();
  int64_t ci_end = -1;
  int64_t oi_end = -1;

  auto merge_range = [](const std::optional<IndexLocation>& index_location,
                        int64_t* start, int64_t* end) {
    if (index_location.has_value()) {
      *start = std::min(*start, index_location->offset);
      *end = std::max(*end, index_location->offset + index_location->length);
    }
  };

  for (int i = 0; i < row_group_metadata.num_columns(); ++i) {
    auto col_chunk = row_group_metadata.ColumnChunk(i);
    merge_range(col_chunk->GetColumnIndexLocation(), &ci_start, &ci_end);
    merge_range(col_chunk->GetOffsetIndexLocation(), &oi_start, &oi_end);
  }

  RowGroupIndexReadRange read_range;
  if (ci_end != -1) {
    read_range.column_index =
        RowGroupIndexReadRange::ReadRange{ci_start, ci_end - ci_start};
  }
  if (oi_end != -1) {
    read_range.offset_index =
        RowGroupIndexReadRange::ReadRange{oi_start, oi_end - oi_start};
  }
  return read_range;
}

// ----------------------------------------------------------------------
// Public factory functions

std::unique_ptr<ColumnIndex> ColumnIndex::Make(const ColumnDescriptor& descr,
                                               const void* serialized_index,
                                               uint32_t index_len,
                                               const ReaderProperties& properties) {
  format::ColumnIndex column_index;
  ThriftDeserializer deserializer(properties);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(serialized_index),
                                  &index_len, &column_index);
  switch (descr.physical_type()) {
    case Type::BOOLEAN:
      return std::make_unique<TypedColumnIndexImpl<BooleanType>>(descr, column_index);
    case Type::INT32:
      return std::make_unique<TypedColumnIndexImpl<Int32Type>>(descr, column_index);
    case Type::INT64:
      return std::make_unique<TypedColumnIndexImpl<Int64Type>>(descr, column_index);
    case Type::INT96:
      return std::make_unique<TypedColumnIndexImpl<Int96Type>>(descr, column_index);
    case Type::FLOAT:
      return std::make_unique<TypedColumnIndexImpl<FloatType>>(descr, column_index);
    case Type::DOUBLE:
      return std::make_unique<TypedColumnIndexImpl<DoubleType>>(descr, column_index);
    case Type::BYTE_ARRAY:
      return std::make_unique<TypedColumnIndexImpl<ByteArrayType>>(descr, column_index);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_unique<TypedColumnIndexImpl<FLBAType>>(descr, column_index);
    case Type::UNDEFINED:
      return nullptr;
  }
  ::arrow::Unreachable("Cannot make ColumnIndex of an unknown type");
  return nullptr;
}

std::unique_ptr<OffsetIndex> OffsetIndex::Make(const void* serialized_index,
                                               uint32_t index_len,
                                               const ReaderProperties& properties) {
  format::OffsetIndex offset_index;
  ThriftDeserializer deserializer(properties);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(serialized_index),
                                  &index_len, &offset_index);
  return std::make_unique<OffsetIndexImpl>(offset_index);
}

std::shared_ptr<PageIndexReader> PageIndexReader::Make(
    ::arrow::io::RandomAccessFile* input, std::shared_ptr<FileMetaData> file_metadata,
    const ReaderProperties& properties,
    std::shared_ptr<InternalFileDecryptor> file_decryptor) {
  return std::make_shared<PageIndexReaderImpl>(input, file_metadata, properties,
                                               std::move(file_decryptor));
}

}  // namespace parquet

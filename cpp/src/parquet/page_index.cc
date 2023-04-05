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

#include "arrow/util/int_util_overflow.h"
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
                              const RowGroupIndexReadRange& index_read_range,
                              std::shared_ptr<InternalFileDecryptor> file_decryptor)
      : input_(input),
        row_group_metadata_(std::move(row_group_metadata)),
        properties_(properties),
        row_group_ordinal_(row_group_ordinal),
        index_read_range_(index_read_range),
        file_decryptor_(std::move(file_decryptor)) {}

  /// Read column index of a column chunk.
  std::shared_ptr<ColumnIndex> GetColumnIndex(int32_t i) override {
    if (i < 0 || i >= row_group_metadata_->num_columns()) {
      throw ParquetException("Invalid column index at column ordinal ", i);
    }

    auto col_chunk = row_group_metadata_->ColumnChunk(i);
    std::unique_ptr<ColumnCryptoMetaData> crypto_metadata = col_chunk->crypto_metadata();
    if (crypto_metadata != nullptr) {
      ParquetException::NYI("Cannot read encrypted column index yet");
    }

    auto column_index_location = col_chunk->GetColumnIndexLocation();
    if (!column_index_location.has_value()) {
      return nullptr;
    }

    CheckReadRangeOrThrow(*column_index_location, index_read_range_.column_index,
                          row_group_ordinal_);

    if (column_index_buffer_ == nullptr) {
      PARQUET_ASSIGN_OR_THROW(column_index_buffer_,
                              input_->ReadAt(index_read_range_.column_index->offset,
                                             index_read_range_.column_index->length));
    }

    int64_t buffer_offset =
        column_index_location->offset - index_read_range_.column_index->offset;
    // ColumnIndex::Make() requires the type of serialized thrift message to be
    // uint32_t
    uint32_t length = static_cast<uint32_t>(column_index_location->length);
    auto descr = row_group_metadata_->schema()->Column(i);
    return ColumnIndex::Make(*descr, column_index_buffer_->data() + buffer_offset, length,
                             properties_);
  }

  /// Read offset index of a column chunk.
  std::shared_ptr<OffsetIndex> GetOffsetIndex(int32_t i) override {
    if (i < 0 || i >= row_group_metadata_->num_columns()) {
      throw ParquetException("Invalid offset index at column ordinal ", i);
    }

    auto col_chunk = row_group_metadata_->ColumnChunk(i);
    std::unique_ptr<ColumnCryptoMetaData> crypto_metadata = col_chunk->crypto_metadata();
    if (crypto_metadata != nullptr) {
      ParquetException::NYI("Cannot read encrypted offset index yet");
    }

    auto offset_index_location = col_chunk->GetOffsetIndexLocation();
    if (!offset_index_location.has_value()) {
      return nullptr;
    }

    CheckReadRangeOrThrow(*offset_index_location, index_read_range_.offset_index,
                          row_group_ordinal_);

    if (offset_index_buffer_ == nullptr) {
      PARQUET_ASSIGN_OR_THROW(offset_index_buffer_,
                              input_->ReadAt(index_read_range_.offset_index->offset,
                                             index_read_range_.offset_index->length));
    }

    int64_t buffer_offset =
        offset_index_location->offset - index_read_range_.offset_index->offset;
    // OffsetIndex::Make() requires the type of serialized thrift message to be
    // uint32_t
    uint32_t length = static_cast<uint32_t>(offset_index_location->length);
    return OffsetIndex::Make(offset_index_buffer_->data() + buffer_offset, length,
                             properties_);
  }

 private:
  static void CheckReadRangeOrThrow(
      const IndexLocation& index_location,
      const std::optional<::arrow::io::ReadRange>& index_read_range,
      int32_t row_group_ordinal) {
    if (!index_read_range.has_value()) {
      throw ParquetException("Missing page index read range of row group ",
                             row_group_ordinal,
                             ", it may not exist or has not been requested");
    }

    /// The coalesced read range is invalid.
    if (index_read_range->offset < 0 || index_read_range->length <= 0) {
      throw ParquetException("Invalid page index read range: offset ",
                             index_read_range->offset, " length ",
                             index_read_range->length);
    }

    /// The location to page index itself is corrupted.
    if (index_location.offset < 0 || index_location.length <= 0) {
      throw ParquetException("Invalid page index location: offset ",
                             index_location.offset, " length ", index_location.length);
    }

    /// Page index location must be within the range of the read range.
    if (index_location.offset < index_read_range->offset ||
        index_location.offset + index_location.length >
            index_read_range->offset + index_read_range->length) {
      throw ParquetException("Page index location [offset:", index_location.offset,
                             ",length:", index_location.length,
                             "] is out of range from previous WillNeed request [offset:",
                             index_read_range->offset,
                             ",length:", index_read_range->length,
                             "], row group: ", row_group_ordinal);
    }
  }

 private:
  /// The input stream that can perform random access read.
  ::arrow::io::RandomAccessFile* input_;

  /// The row group metadata to get column chunk metadata.
  std::shared_ptr<RowGroupMetaData> row_group_metadata_;

  /// Reader properties used to deserialize thrift object.
  const ReaderProperties& properties_;

  /// The ordinal of the row group in the file.
  int32_t row_group_ordinal_;

  /// File offsets and sizes of the page Index of all column chunks in the row group.
  RowGroupIndexReadRange index_read_range_;

  /// File-level decryptor.
  std::shared_ptr<InternalFileDecryptor> file_decryptor_;

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
      throw ParquetException("Invalid row group ordinal: ", i);
    }

    auto row_group_metadata = file_metadata_->RowGroup(i);

    // Find the read range of the page index of the row group if provided by WillNeed()
    RowGroupIndexReadRange index_read_range;
    auto iter = index_read_ranges_.find(i);
    if (iter != index_read_ranges_.cend()) {
      /// This row group has been requested by WillNeed(). Only column index and/or
      /// offset index of requested columns can be read.
      index_read_range = iter->second;
    } else {
      /// If the row group has not been requested by WillNeed(), by default both column
      /// index and offset index of all column chunks for the row group can be read.
      index_read_range =
          PageIndexReader::DeterminePageIndexRangesInRowGroup(*row_group_metadata, {});
    }

    if (index_read_range.column_index.has_value() ||
        index_read_range.offset_index.has_value()) {
      return std::make_shared<RowGroupPageIndexReaderImpl>(
          input_, std::move(row_group_metadata), properties_, i, index_read_range,
          file_decryptor_);
    }

    /// The row group does not has page index or has not been requested by WillNeed().
    /// Simply returns nullptr.
    return nullptr;
  }

  void WillNeed(const std::vector<int32_t>& row_group_indices,
                const std::vector<int32_t>& column_indices,
                const PageIndexSelection& selection) override {
    std::vector<::arrow::io::ReadRange> read_ranges;
    for (int32_t row_group_ordinal : row_group_indices) {
      auto read_range = PageIndexReader::DeterminePageIndexRangesInRowGroup(
          *file_metadata_->RowGroup(row_group_ordinal), column_indices);
      if (selection.column_index && read_range.column_index.has_value()) {
        read_ranges.push_back(*read_range.column_index);
      } else {
        // Mark the column index as not requested.
        read_range.column_index = std::nullopt;
      }
      if (selection.offset_index && read_range.offset_index.has_value()) {
        read_ranges.push_back(*read_range.offset_index);
      } else {
        // Mark the offset index as not requested.
        read_range.offset_index = std::nullopt;
      }
      index_read_ranges_.emplace(row_group_ordinal, std::move(read_range));
    }
    PARQUET_THROW_NOT_OK(input_->WillNeed(read_ranges));
  }

  void WillNotNeed(const std::vector<int32_t>& row_group_indices) override {
    for (int32_t row_group_ordinal : row_group_indices) {
      index_read_ranges_.erase(row_group_ordinal);
    }
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

  /// Coalesced read ranges of page index of row groups that have been suggested by
  /// WillNeed(). Key is the row group ordinal.
  std::unordered_map<int32_t, RowGroupIndexReadRange> index_read_ranges_;
};

}  // namespace

RowGroupIndexReadRange PageIndexReader::DeterminePageIndexRangesInRowGroup(
    const RowGroupMetaData& row_group_metadata, const std::vector<int32_t>& columns) {
  int64_t ci_start = std::numeric_limits<int64_t>::max();
  int64_t oi_start = std::numeric_limits<int64_t>::max();
  int64_t ci_end = -1;
  int64_t oi_end = -1;

  auto merge_range = [](const std::optional<IndexLocation>& index_location,
                        int64_t* start, int64_t* end) {
    if (index_location.has_value()) {
      int64_t index_end = 0;
      if (index_location->offset < 0 || index_location->length <= 0 ||
          ::arrow::internal::AddWithOverflow(index_location->offset,
                                             index_location->length, &index_end)) {
        throw ParquetException("Invalid page index location: offset ",
                               index_location->offset, " length ",
                               index_location->length);
      }
      *start = std::min(*start, index_location->offset);
      *end = std::max(*end, index_end);
    }
  };

  if (columns.empty()) {
    for (int32_t i = 0; i < row_group_metadata.num_columns(); ++i) {
      auto col_chunk = row_group_metadata.ColumnChunk(i);
      merge_range(col_chunk->GetColumnIndexLocation(), &ci_start, &ci_end);
      merge_range(col_chunk->GetOffsetIndexLocation(), &oi_start, &oi_end);
    }
  } else {
    for (int32_t i : columns) {
      if (i < 0 || i >= row_group_metadata.num_columns()) {
        throw ParquetException("Invalid column ordinal ", i);
      }
      auto col_chunk = row_group_metadata.ColumnChunk(i);
      merge_range(col_chunk->GetColumnIndexLocation(), &ci_start, &ci_end);
      merge_range(col_chunk->GetOffsetIndexLocation(), &oi_start, &oi_end);
    }
  }

  RowGroupIndexReadRange read_range;
  if (ci_end != -1) {
    read_range.column_index = {ci_start, ci_end - ci_start};
  }
  if (oi_end != -1) {
    read_range.offset_index = {oi_start, oi_end - oi_start};
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

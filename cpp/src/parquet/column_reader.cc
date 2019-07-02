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

#include "parquet/column_reader.h"

#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/util/bit-stream-utils.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle-encoding.h"
#include "arrow/util/ubsan.h"

#include "parquet/column_page.h"
#include "parquet/encoding.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"
#include "parquet/thrift.h"

using arrow::MemoryPool;

namespace parquet {

LevelDecoder::LevelDecoder() : num_values_remaining_(0) {}

LevelDecoder::~LevelDecoder() {}

int LevelDecoder::SetData(Encoding::type encoding, int16_t max_level,
                          int num_buffered_values, const uint8_t* data) {
  int32_t num_bytes = 0;
  encoding_ = encoding;
  num_values_remaining_ = num_buffered_values;
  bit_width_ = BitUtil::Log2(max_level + 1);
  switch (encoding) {
    case Encoding::RLE: {
      num_bytes = arrow::util::SafeLoadAs<int32_t>(data);
      const uint8_t* decoder_data = data + sizeof(int32_t);
      if (!rle_decoder_) {
        rle_decoder_.reset(
            new ::arrow::util::RleDecoder(decoder_data, num_bytes, bit_width_));
      } else {
        rle_decoder_->Reset(decoder_data, num_bytes, bit_width_);
      }
      return static_cast<int>(sizeof(int32_t)) + num_bytes;
    }
    case Encoding::BIT_PACKED: {
      num_bytes =
          static_cast<int32_t>(BitUtil::BytesForBits(num_buffered_values * bit_width_));
      if (!bit_packed_decoder_) {
        bit_packed_decoder_.reset(new ::arrow::BitUtil::BitReader(data, num_bytes));
      } else {
        bit_packed_decoder_->Reset(data, num_bytes);
      }
      return num_bytes;
    }
    default:
      throw ParquetException("Unknown encoding type for levels.");
  }
  return -1;
}

int LevelDecoder::Decode(int batch_size, int16_t* levels) {
  int num_decoded = 0;

  int num_values = std::min(num_values_remaining_, batch_size);
  if (encoding_ == Encoding::RLE) {
    num_decoded = rle_decoder_->GetBatch(levels, num_values);
  } else {
    num_decoded = bit_packed_decoder_->GetBatch(bit_width_, levels, num_values);
  }
  num_values_remaining_ -= num_decoded;
  return num_decoded;
}

ReaderProperties default_reader_properties() {
  static ReaderProperties default_reader_properties;
  return default_reader_properties;
}

// ----------------------------------------------------------------------
// SerializedPageReader deserializes Thrift metadata and pages that have been
// assembled in a serialized stream for storing in a Parquet files

// This subclass delimits pages appearing in a serialized stream, each preceded
// by a serialized Thrift format::PageHeader indicating the type of each page
// and the page metadata.
class SerializedPageReader : public PageReader {
 public:
  SerializedPageReader(const std::shared_ptr<ArrowInputStream>& stream,
                       int64_t total_num_rows, Compression::type codec,
                       ::arrow::MemoryPool* pool)
      : stream_(stream),
        decompression_buffer_(AllocateBuffer(pool, 0)),
        seen_num_rows_(0),
        total_num_rows_(total_num_rows) {
    max_page_header_size_ = kDefaultMaxPageHeaderSize;
    decompressor_ = GetCodecFromArrow(codec);
  }

  // Implement the PageReader interface
  std::shared_ptr<Page> NextPage() override;

  void set_max_page_header_size(uint32_t size) override { max_page_header_size_ = size; }

 private:
  std::shared_ptr<ArrowInputStream> stream_;

  format::PageHeader current_page_header_;
  std::shared_ptr<Page> current_page_;

  // Compression codec to use.
  std::unique_ptr<::arrow::util::Codec> decompressor_;
  std::shared_ptr<ResizableBuffer> decompression_buffer_;

  // Maximum allowed page size
  uint32_t max_page_header_size_;

  // Number of rows read in data pages so far
  int64_t seen_num_rows_;

  // Number of rows in all the data pages
  int64_t total_num_rows_;
};

std::shared_ptr<Page> SerializedPageReader::NextPage() {
  // Loop here because there may be unhandled page types that we skip until
  // finding a page that we do know what to do with
  while (seen_num_rows_ < total_num_rows_) {
    uint32_t header_size = 0;
    uint32_t allowed_page_size = kDefaultPageHeaderSize;

    // Page headers can be very large because of page statistics
    // We try to deserialize a larger buffer progressively
    // until a maximum allowed header limit
    while (true) {
      string_view buffer;
      PARQUET_THROW_NOT_OK(stream_->Peek(allowed_page_size, &buffer));
      if (buffer.size() == 0) {
        return std::shared_ptr<Page>(nullptr);
      }

      // This gets used, then set by DeserializeThriftMsg
      header_size = static_cast<uint32_t>(buffer.size());
      try {
        DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(buffer.data()),
                             &header_size, &current_page_header_);
        break;
      } catch (std::exception& e) {
        // Failed to deserialize. Double the allowed page header size and try again
        std::stringstream ss;
        ss << e.what();
        allowed_page_size *= 2;
        if (allowed_page_size > max_page_header_size_) {
          ss << "Deserializing page header failed.\n";
          throw ParquetException(ss.str());
        }
      }
    }
    // Advance the stream offset
    PARQUET_THROW_NOT_OK(stream_->Advance(header_size));

    int compressed_len = current_page_header_.compressed_page_size;
    int uncompressed_len = current_page_header_.uncompressed_page_size;

    // Read the compressed data page.
    std::shared_ptr<Buffer> page_buffer;
    PARQUET_THROW_NOT_OK(stream_->Read(compressed_len, &page_buffer));
    if (page_buffer->size() != compressed_len) {
      std::stringstream ss;
      ss << "Page was smaller (" << page_buffer->size() << ") than expected ("
         << compressed_len << ")";
      ParquetException::EofException(ss.str());
    }

    // Uncompress it if we need to
    if (decompressor_ != nullptr) {
      // Grow the uncompressed buffer if we need to.
      if (uncompressed_len > static_cast<int>(decompression_buffer_->size())) {
        PARQUET_THROW_NOT_OK(decompression_buffer_->Resize(uncompressed_len, false));
      }
      PARQUET_THROW_NOT_OK(
          decompressor_->Decompress(compressed_len, page_buffer->data(), uncompressed_len,
                                    decompression_buffer_->mutable_data()));
      page_buffer = decompression_buffer_;
    }

    if (current_page_header_.type == format::PageType::DICTIONARY_PAGE) {
      const format::DictionaryPageHeader& dict_header =
          current_page_header_.dictionary_page_header;

      bool is_sorted = dict_header.__isset.is_sorted ? dict_header.is_sorted : false;

      return std::make_shared<DictionaryPage>(page_buffer, dict_header.num_values,
                                              FromThrift(dict_header.encoding),
                                              is_sorted);
    } else if (current_page_header_.type == format::PageType::DATA_PAGE) {
      const format::DataPageHeader& header = current_page_header_.data_page_header;

      EncodedStatistics page_statistics;
      if (header.__isset.statistics) {
        const format::Statistics& stats = header.statistics;
        if (stats.__isset.max) {
          page_statistics.set_max(stats.max);
        }
        if (stats.__isset.min) {
          page_statistics.set_min(stats.min);
        }
        if (stats.__isset.null_count) {
          page_statistics.set_null_count(stats.null_count);
        }
        if (stats.__isset.distinct_count) {
          page_statistics.set_distinct_count(stats.distinct_count);
        }
      }

      seen_num_rows_ += header.num_values;

      return std::make_shared<DataPageV1>(
          page_buffer, header.num_values, FromThrift(header.encoding),
          FromThrift(header.definition_level_encoding),
          FromThrift(header.repetition_level_encoding), page_statistics);
    } else if (current_page_header_.type == format::PageType::DATA_PAGE_V2) {
      const format::DataPageHeaderV2& header = current_page_header_.data_page_header_v2;
      bool is_compressed = header.__isset.is_compressed ? header.is_compressed : false;

      seen_num_rows_ += header.num_values;

      return std::make_shared<DataPageV2>(
          page_buffer, header.num_values, header.num_nulls, header.num_rows,
          FromThrift(header.encoding), header.definition_levels_byte_length,
          header.repetition_levels_byte_length, is_compressed);
    } else {
      // We don't know what this page type is. We're allowed to skip non-data
      // pages.
      continue;
    }
  }
  return std::shared_ptr<Page>(nullptr);
}

std::unique_ptr<PageReader> PageReader::Open(
    const std::shared_ptr<ArrowInputStream>& stream, int64_t total_num_rows,
    Compression::type codec, ::arrow::MemoryPool* pool) {
  return std::unique_ptr<PageReader>(
      new SerializedPageReader(stream, total_num_rows, codec, pool));
}

// ----------------------------------------------------------------------
// TypedColumnReader implementations

template <typename DType>
class TypedColumnReaderImpl : public TypedColumnReader<DType> {
 public:
  using T = typename DType::c_type;

  TypedColumnReaderImpl(const ColumnDescriptor* descr, std::unique_ptr<PageReader> pager,
                        ::arrow::MemoryPool* pool)
      : descr_(descr),
        pager_(std::move(pager)),
        num_buffered_values_(0),
        num_decoded_values_(0),
        pool_(pool),
        current_decoder_(NULLPTR) {}

  int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                    T* values, int64_t* values_read) override;

  int64_t ReadBatchSpaced(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                          T* values, uint8_t* valid_bits, int64_t valid_bits_offset,
                          int64_t* levels_read, int64_t* values_read,
                          int64_t* null_count) override;

  int64_t Skip(int64_t num_rows_to_skip) override;

  bool HasNext() override {
    // Either there is no data page available yet, or the data page has been
    // exhausted
    if (num_buffered_values_ == 0 || num_decoded_values_ == num_buffered_values_) {
      if (!ReadNewPage() || num_buffered_values_ == 0) {
        return false;
      }
    }
    return true;
  }

  Type::type type() const override { return descr_->physical_type(); }

  const ColumnDescriptor* descr() const override { return descr_; }

 protected:
  using DecoderType = TypedDecoder<DType>;

  // Advance to the next data page
  bool ReadNewPage();

  // Read multiple definition levels into preallocated memory
  //
  // Returns the number of decoded definition levels
  int64_t ReadDefinitionLevels(int64_t batch_size, int16_t* levels) {
    if (descr_->max_definition_level() == 0) {
      return 0;
    }
    return definition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
  }

  // Read multiple repetition levels into preallocated memory
  // Returns the number of decoded repetition levels
  int64_t ReadRepetitionLevels(int64_t batch_size, int16_t* levels) {
    if (descr_->max_repetition_level() == 0) {
      return 0;
    }
    return repetition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
  }

  int64_t available_values_current_page() const {
    return num_buffered_values_ - num_decoded_values_;
  }

  void ConsumeBufferedValues(int64_t num_values) { num_decoded_values_ += num_values; }

  const ColumnDescriptor* descr_;

  std::unique_ptr<PageReader> pager_;
  std::shared_ptr<Page> current_page_;

  // Not set if full schema for this field has no optional or repeated elements
  LevelDecoder definition_level_decoder_;

  // Not set for flat schemas.
  LevelDecoder repetition_level_decoder_;

  // The total number of values stored in the data page. This is the maximum of
  // the number of encoded definition levels or encoded values. For
  // non-repeated, required columns, this is equal to the number of encoded
  // values. For repeated or optional values, there may be fewer data values
  // than levels, and this tells you how many encoded levels there are in that
  // case.
  int64_t num_buffered_values_;

  // The number of values from the current data page that have been decoded
  // into memory
  int64_t num_decoded_values_;

  ::arrow::MemoryPool* pool_;

  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValues(int64_t batch_size, T* out);

  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*, leaving spaces for null entries according
  // to the def_levels.
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValuesSpaced(int64_t batch_size, T* out, int64_t null_count,
                           uint8_t* valid_bits, int64_t valid_bits_offset);

  // Map of encoding type to the respective decoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::unique_ptr<DecoderType>> decoders_;

  void ConfigureDictionary(const DictionaryPage* page);
  DecoderType* current_decoder_;
};

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::ReadValues(int64_t batch_size, T* out) {
  int64_t num_decoded = current_decoder_->Decode(out, static_cast<int>(batch_size));
  return num_decoded;
}

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::ReadValuesSpaced(int64_t batch_size, T* out,
                                                       int64_t null_count,
                                                       uint8_t* valid_bits,
                                                       int64_t valid_bits_offset) {
  return current_decoder_->DecodeSpaced(out, static_cast<int>(batch_size),
                                        static_cast<int>(null_count), valid_bits,
                                        valid_bits_offset);
}

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::ReadBatch(int64_t batch_size, int16_t* def_levels,
                                                int16_t* rep_levels, T* values,
                                                int64_t* values_read) {
  // HasNext invokes ReadNewPage
  if (!HasNext()) {
    *values_read = 0;
    return 0;
  }

  // TODO(wesm): keep reading data pages until batch_size is reached, or the
  // row group is finished
  batch_size = std::min(batch_size, num_buffered_values_ - num_decoded_values_);

  int64_t num_def_levels = 0;
  int64_t num_rep_levels = 0;

  int64_t values_to_read = 0;

  // If the field is required and non-repeated, there are no definition levels
  if (descr_->max_definition_level() > 0 && def_levels) {
    num_def_levels = ReadDefinitionLevels(batch_size, def_levels);
    // TODO(wesm): this tallying of values-to-decode can be performed with better
    // cache-efficiency if fused with the level decoding.
    for (int64_t i = 0; i < num_def_levels; ++i) {
      if (def_levels[i] == descr_->max_definition_level()) {
        ++values_to_read;
      }
    }
  } else {
    // Required field, read all values
    values_to_read = batch_size;
  }

  // Not present for non-repeated fields
  if (descr_->max_repetition_level() > 0 && rep_levels) {
    num_rep_levels = ReadRepetitionLevels(batch_size, rep_levels);
    if (def_levels && num_def_levels != num_rep_levels) {
      throw ParquetException("Number of decoded rep / def levels did not match");
    }
  }

  *values_read = ReadValues(values_to_read, values);
  int64_t total_values = std::max(num_def_levels, *values_read);
  ConsumeBufferedValues(total_values);

  return total_values;
}

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::ReadBatchSpaced(
    int64_t batch_size, int16_t* def_levels, int16_t* rep_levels, T* values,
    uint8_t* valid_bits, int64_t valid_bits_offset, int64_t* levels_read,
    int64_t* values_read, int64_t* null_count_out) {
  // HasNext invokes ReadNewPage
  if (!HasNext()) {
    *levels_read = 0;
    *values_read = 0;
    *null_count_out = 0;
    return 0;
  }

  int64_t total_values;
  // TODO(wesm): keep reading data pages until batch_size is reached, or the
  // row group is finished
  batch_size = std::min(batch_size, num_buffered_values_ - num_decoded_values_);

  // If the field is required and non-repeated, there are no definition levels
  if (descr_->max_definition_level() > 0) {
    int64_t num_def_levels = ReadDefinitionLevels(batch_size, def_levels);

    // Not present for non-repeated fields
    if (descr_->max_repetition_level() > 0) {
      int64_t num_rep_levels = ReadRepetitionLevels(batch_size, rep_levels);
      if (num_def_levels != num_rep_levels) {
        throw ParquetException("Number of decoded rep / def levels did not match");
      }
    }

    const bool has_spaced_values = internal::HasSpacedValues(descr_);

    int64_t null_count = 0;
    if (!has_spaced_values) {
      int values_to_read = 0;
      for (int64_t i = 0; i < num_def_levels; ++i) {
        if (def_levels[i] == descr_->max_definition_level()) {
          ++values_to_read;
        }
      }
      total_values = ReadValues(values_to_read, values);
      for (int64_t i = 0; i < total_values; i++) {
        ::arrow::BitUtil::SetBit(valid_bits, valid_bits_offset + i);
      }
      *values_read = total_values;
    } else {
      int16_t max_definition_level = descr_->max_definition_level();
      int16_t max_repetition_level = descr_->max_repetition_level();
      internal::DefinitionLevelsToBitmap(def_levels, num_def_levels, max_definition_level,
                                         max_repetition_level, values_read, &null_count,
                                         valid_bits, valid_bits_offset);
      total_values = ReadValuesSpaced(*values_read, values, static_cast<int>(null_count),
                                      valid_bits, valid_bits_offset);
    }
    *levels_read = num_def_levels;
    *null_count_out = null_count;

  } else {
    // Required field, read all values
    total_values = ReadValues(batch_size, values);
    for (int64_t i = 0; i < total_values; i++) {
      ::arrow::BitUtil::SetBit(valid_bits, valid_bits_offset + i);
    }
    *null_count_out = 0;
    *levels_read = total_values;
  }

  ConsumeBufferedValues(*levels_read);
  return total_values;
}

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::Skip(int64_t num_rows_to_skip) {
  int64_t rows_to_skip = num_rows_to_skip;
  while (HasNext() && rows_to_skip > 0) {
    // If the number of rows to skip is more than the number of undecoded values, skip the
    // Page.
    if (rows_to_skip > (num_buffered_values_ - num_decoded_values_)) {
      rows_to_skip -= num_buffered_values_ - num_decoded_values_;
      num_decoded_values_ = num_buffered_values_;
    } else {
      // We need to read this Page
      // Jump to the right offset in the Page
      int64_t batch_size = 1024;  // ReadBatch with a smaller memory footprint
      int64_t values_read = 0;

      std::shared_ptr<ResizableBuffer> vals = AllocateBuffer(
          this->pool_, batch_size * type_traits<DType::type_num>::value_byte_size);
      std::shared_ptr<ResizableBuffer> def_levels =
          AllocateBuffer(this->pool_, batch_size * sizeof(int16_t));

      std::shared_ptr<ResizableBuffer> rep_levels =
          AllocateBuffer(this->pool_, batch_size * sizeof(int16_t));

      do {
        batch_size = std::min(batch_size, rows_to_skip);
        values_read = ReadBatch(static_cast<int>(batch_size),
                                reinterpret_cast<int16_t*>(def_levels->mutable_data()),
                                reinterpret_cast<int16_t*>(rep_levels->mutable_data()),
                                reinterpret_cast<T*>(vals->mutable_data()), &values_read);
        rows_to_skip -= values_read;
      } while (values_read > 0 && rows_to_skip > 0);
    }
  }
  return num_rows_to_skip - rows_to_skip;
}

template <typename DType>
void TypedColumnReaderImpl<DType>::ConfigureDictionary(const DictionaryPage* page) {
  int encoding = static_cast<int>(page->encoding());
  if (page->encoding() == Encoding::PLAIN_DICTIONARY ||
      page->encoding() == Encoding::PLAIN) {
    encoding = static_cast<int>(Encoding::RLE_DICTIONARY);
  }

  auto it = decoders_.find(encoding);
  if (it != decoders_.end()) {
    throw ParquetException("Column cannot have more than one dictionary.");
  }

  if (page->encoding() == Encoding::PLAIN_DICTIONARY ||
      page->encoding() == Encoding::PLAIN) {
    auto dictionary = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
    dictionary->SetData(page->num_values(), page->data(), page->size());

    // The dictionary is fully decoded during SetData, so the
    // DictionaryPage buffer is no longer required after this step
    //
    // TODO(wesm): investigate whether this all-or-nothing decoding of the
    // dictionary makes sense and whether performance can be improved
    auto decoder = MakeDictDecoder<DType>(descr_, pool_);
    decoder->SetDict(dictionary.get());
    decoders_[encoding] = std::move(decoder);
  } else {
    ParquetException::NYI("only plain dictionary encoding has been implemented");
  }

  current_decoder_ = decoders_[encoding].get();
}

// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
static bool IsDictionaryIndexEncoding(const Encoding::type& e) {
  return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
}

template <typename DType>
bool TypedColumnReaderImpl<DType>::ReadNewPage() {
  // Loop until we find the next data page.
  const uint8_t* buffer;

  while (true) {
    current_page_ = pager_->NextPage();
    if (!current_page_) {
      // EOS
      return false;
    }

    if (current_page_->type() == PageType::DICTIONARY_PAGE) {
      ConfigureDictionary(static_cast<const DictionaryPage*>(current_page_.get()));
      continue;
    } else if (current_page_->type() == PageType::DATA_PAGE) {
      const DataPageV1& page = static_cast<const DataPageV1&>(*current_page_);

      // Read a data page.
      num_buffered_values_ = page.num_values();

      // Have not decoded any values from the data page yet
      num_decoded_values_ = 0;

      buffer = page.data();

      // If the data page includes repetition and definition levels, we
      // initialize the level decoder and subtract the encoded level bytes from
      // the page size to determine the number of bytes in the encoded data.
      int64_t data_size = page.size();

      // Data page Layout: Repetition Levels - Definition Levels - encoded values.
      // Levels are encoded as rle or bit-packed.
      // Init repetition levels
      if (descr_->max_repetition_level() > 0) {
        int64_t rep_levels_bytes = repetition_level_decoder_.SetData(
            page.repetition_level_encoding(), descr_->max_repetition_level(),
            static_cast<int>(num_buffered_values_), buffer);
        buffer += rep_levels_bytes;
        data_size -= rep_levels_bytes;
      }
      // TODO figure a way to set max_definition_level_ to 0
      // if the initial value is invalid

      // Init definition levels
      if (descr_->max_definition_level() > 0) {
        int64_t def_levels_bytes = definition_level_decoder_.SetData(
            page.definition_level_encoding(), descr_->max_definition_level(),
            static_cast<int>(num_buffered_values_), buffer);
        buffer += def_levels_bytes;
        data_size -= def_levels_bytes;
      }

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      Encoding::type encoding = page.encoding();

      if (IsDictionaryIndexEncoding(encoding)) {
        encoding = Encoding::RLE_DICTIONARY;
      }

      auto it = decoders_.find(static_cast<int>(encoding));
      if (it != decoders_.end()) {
        if (encoding == Encoding::RLE_DICTIONARY) {
          DCHECK(current_decoder_->encoding() == Encoding::RLE_DICTIONARY);
        }
        current_decoder_ = it->second.get();
      } else {
        switch (encoding) {
          case Encoding::PLAIN: {
            auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
            current_decoder_ = decoder.get();
            decoders_[static_cast<int>(encoding)] = std::move(decoder);
            break;
          }
          case Encoding::RLE_DICTIONARY:
            throw ParquetException("Dictionary page must be before data page.");

          case Encoding::DELTA_BINARY_PACKED:
          case Encoding::DELTA_LENGTH_BYTE_ARRAY:
          case Encoding::DELTA_BYTE_ARRAY:
            ParquetException::NYI("Unsupported encoding");

          default:
            throw ParquetException("Unknown encoding type.");
        }
      }
      current_decoder_->SetData(static_cast<int>(num_buffered_values_), buffer,
                                static_cast<int>(data_size));
      return true;
    } else {
      // We don't know what this page type is. We're allowed to skip non-data
      // pages.
      continue;
    }
  }
  return true;
}

// ----------------------------------------------------------------------
// Dynamic column reader constructor

std::shared_ptr<ColumnReader> ColumnReader::Make(const ColumnDescriptor* descr,
                                                 std::unique_ptr<PageReader> pager,
                                                 MemoryPool* pool) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::make_shared<TypedColumnReaderImpl<BooleanType>>(descr, std::move(pager),
                                                                  pool);
    case Type::INT32:
      return std::make_shared<TypedColumnReaderImpl<Int32Type>>(descr, std::move(pager),
                                                                pool);
    case Type::INT64:
      return std::make_shared<TypedColumnReaderImpl<Int64Type>>(descr, std::move(pager),
                                                                pool);
    case Type::INT96:
      return std::make_shared<TypedColumnReaderImpl<Int96Type>>(descr, std::move(pager),
                                                                pool);
    case Type::FLOAT:
      return std::make_shared<TypedColumnReaderImpl<FloatType>>(descr, std::move(pager),
                                                                pool);
    case Type::DOUBLE:
      return std::make_shared<TypedColumnReaderImpl<DoubleType>>(descr, std::move(pager),
                                                                 pool);
    case Type::BYTE_ARRAY:
      return std::make_shared<TypedColumnReaderImpl<ByteArrayType>>(
          descr, std::move(pager), pool);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<TypedColumnReaderImpl<FLBAType>>(descr, std::move(pager),
                                                               pool);
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::shared_ptr<ColumnReader>(nullptr);
}

}  // namespace parquet

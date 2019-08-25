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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <utility>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_encoding.h"

#include "parquet/column_page.h"
#include "parquet/encoding.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"
#include "parquet/thrift.h"  // IWYU pragma: keep

using arrow::MemoryPool;
using arrow::internal::checked_cast;

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
    decompressor_ = GetCodec(codec);
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
// Impl base class for TypedColumnReader and RecordReader

// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
static bool IsDictionaryIndexEncoding(const Encoding::type& e) {
  return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
}

template <typename DType>
class ColumnReaderImplBase {
 public:
  using T = typename DType::c_type;

  ColumnReaderImplBase(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool)
      : descr_(descr),
        max_def_level_(descr->max_definition_level()),
        max_rep_level_(descr->max_repetition_level()),
        num_buffered_values_(0),
        num_decoded_values_(0),
        pool_(pool),
        current_decoder_(nullptr),
        current_encoding_(Encoding::UNKNOWN) {}

  virtual ~ColumnReaderImplBase() = default;

 protected:
  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValues(int64_t batch_size, T* out) {
    int64_t num_decoded = current_decoder_->Decode(out, static_cast<int>(batch_size));
    return num_decoded;
  }

  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*, leaving spaces for null entries according
  // to the def_levels.
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValuesSpaced(int64_t batch_size, T* out, int64_t null_count,
                           uint8_t* valid_bits, int64_t valid_bits_offset) {
    return current_decoder_->DecodeSpaced(out, static_cast<int>(batch_size),
                                          static_cast<int>(null_count), valid_bits,
                                          valid_bits_offset);
  }

  // Read multiple definition levels into preallocated memory
  //
  // Returns the number of decoded definition levels
  int64_t ReadDefinitionLevels(int64_t batch_size, int16_t* levels) {
    if (max_def_level_ == 0) {
      return 0;
    }
    return definition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
  }

  bool HasNextInternal() {
    // Either there is no data page available yet, or the data page has been
    // exhausted
    if (num_buffered_values_ == 0 || num_decoded_values_ == num_buffered_values_) {
      if (!ReadNewPage() || num_buffered_values_ == 0) {
        return false;
      }
    }
    return true;
  }

  // Read multiple repetition levels into preallocated memory
  // Returns the number of decoded repetition levels
  int64_t ReadRepetitionLevels(int64_t batch_size, int16_t* levels) {
    if (max_rep_level_ == 0) {
      return 0;
    }
    return repetition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
  }

  // Advance to the next data page
  bool ReadNewPage() {
    // Loop until we find the next data page.
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
        const auto page = std::static_pointer_cast<DataPageV1>(current_page_);
        const int64_t levels_byte_size = InitializeLevelDecoders(
            *page, page->repetition_level_encoding(), page->definition_level_encoding());
        InitializeDataDecoder(*page, levels_byte_size);
        return true;
      } else if (current_page_->type() == PageType::DATA_PAGE_V2) {
        const auto page = std::static_pointer_cast<DataPageV2>(current_page_);
        // Repetition and definition levels are always encoded using RLE encoding
        // in the DataPageV2 format.
        const int64_t levels_byte_size =
            InitializeLevelDecoders(*page, Encoding::RLE, Encoding::RLE);
        InitializeDataDecoder(*page, levels_byte_size);
        return true;
      } else {
        // We don't know what this page type is. We're allowed to skip non-data
        // pages.
        continue;
      }
    }
    return true;
  }

  void ConfigureDictionary(const DictionaryPage* page) {
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

      // The dictionary is fully decoded during DictionaryDecoder::Init, so the
      // DictionaryPage buffer is no longer required after this step
      //
      // TODO(wesm): investigate whether this all-or-nothing decoding of the
      // dictionary makes sense and whether performance can be improved

      std::unique_ptr<DictDecoder<DType>> decoder = MakeDictDecoder<DType>(descr_, pool_);
      decoder->SetDict(dictionary.get());
      decoders_[encoding] =
          std::unique_ptr<DecoderType>(dynamic_cast<DecoderType*>(decoder.release()));
    } else {
      ParquetException::NYI("only plain dictionary encoding has been implemented");
    }

    new_dictionary_ = true;
    current_decoder_ = decoders_[encoding].get();
    DCHECK(current_decoder_);
  }

  // Initialize repetition and definition level decoders on the next data page.

  // If the data page includes repetition and definition levels, we
  // initialize the level decoders and return the number of encoded level bytes.
  // The return value helps determine the number of bytes in the encoded data.
  int64_t InitializeLevelDecoders(const DataPage& page,
                                  Encoding::type repetition_level_encoding,
                                  Encoding::type definition_level_encoding) {
    // Read a data page.
    num_buffered_values_ = page.num_values();

    // Have not decoded any values from the data page yet
    num_decoded_values_ = 0;

    const uint8_t* buffer = page.data();
    int64_t levels_byte_size = 0;

    // Data page Layout: Repetition Levels - Definition Levels - encoded values.
    // Levels are encoded as rle or bit-packed.
    // Init repetition levels
    if (max_rep_level_ > 0) {
      int64_t rep_levels_bytes = repetition_level_decoder_.SetData(
          repetition_level_encoding, max_rep_level_,
          static_cast<int>(num_buffered_values_), buffer);
      buffer += rep_levels_bytes;
      levels_byte_size += rep_levels_bytes;
    }
    // TODO figure a way to set max_def_level_ to 0
    // if the initial value is invalid

    // Init definition levels
    if (max_def_level_ > 0) {
      int64_t def_levels_bytes = definition_level_decoder_.SetData(
          definition_level_encoding, max_def_level_,
          static_cast<int>(num_buffered_values_), buffer);
      levels_byte_size += def_levels_bytes;
    }

    return levels_byte_size;
  }

  // Get a decoder object for this page or create a new decoder if this is the
  // first page with this encoding.
  void InitializeDataDecoder(const DataPage& page, int64_t levels_byte_size) {
    const uint8_t* buffer = page.data() + levels_byte_size;
    const int64_t data_size = page.size() - levels_byte_size;

    Encoding::type encoding = page.encoding();

    if (IsDictionaryIndexEncoding(encoding)) {
      encoding = Encoding::RLE_DICTIONARY;
    }

    auto it = decoders_.find(static_cast<int>(encoding));
    if (it != decoders_.end()) {
      DCHECK(it->second.get() != nullptr);
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
    current_encoding_ = encoding;
    current_decoder_->SetData(static_cast<int>(num_buffered_values_), buffer,
                              static_cast<int>(data_size));
  }

  const ColumnDescriptor* descr_;
  const int16_t max_def_level_;
  const int16_t max_rep_level_;

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

  using DecoderType = typename EncodingTraits<DType>::Decoder;
  DecoderType* current_decoder_;
  Encoding::type current_encoding_;

  /// Flag to signal when a new dictionary has been set, for the benefit of
  /// DictionaryRecordReader
  bool new_dictionary_;

  // Map of encoding type to the respective decoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::unique_ptr<DecoderType>> decoders_;

  void ConsumeBufferedValues(int64_t num_values) { num_decoded_values_ += num_values; }
};

// ----------------------------------------------------------------------
// TypedColumnReader implementations

template <typename DType>
class TypedColumnReaderImpl : public TypedColumnReader<DType>,
                              public ColumnReaderImplBase<DType> {
 public:
  using T = typename DType::c_type;

  TypedColumnReaderImpl(const ColumnDescriptor* descr, std::unique_ptr<PageReader> pager,
                        ::arrow::MemoryPool* pool)
      : ColumnReaderImplBase<DType>(descr, pool) {
    this->pager_ = std::move(pager);
  }

  bool HasNext() override { return this->HasNextInternal(); }

  int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                    T* values, int64_t* values_read) override;

  int64_t ReadBatchSpaced(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                          T* values, uint8_t* valid_bits, int64_t valid_bits_offset,
                          int64_t* levels_read, int64_t* values_read,
                          int64_t* null_count) override;

  int64_t Skip(int64_t num_rows_to_skip) override;

  Type::type type() const override { return this->descr_->physical_type(); }

  const ColumnDescriptor* descr() const override { return this->descr_; }
};

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
  batch_size =
      std::min(batch_size, this->num_buffered_values_ - this->num_decoded_values_);

  int64_t num_def_levels = 0;
  int64_t num_rep_levels = 0;

  int64_t values_to_read = 0;

  // If the field is required and non-repeated, there are no definition levels
  if (this->max_def_level_ > 0 && def_levels) {
    num_def_levels = this->ReadDefinitionLevels(batch_size, def_levels);
    // TODO(wesm): this tallying of values-to-decode can be performed with better
    // cache-efficiency if fused with the level decoding.
    for (int64_t i = 0; i < num_def_levels; ++i) {
      if (def_levels[i] == this->max_def_level_) {
        ++values_to_read;
      }
    }
  } else {
    // Required field, read all values
    values_to_read = batch_size;
  }

  // Not present for non-repeated fields
  if (this->max_rep_level_ > 0 && rep_levels) {
    num_rep_levels = this->ReadRepetitionLevels(batch_size, rep_levels);
    if (def_levels && num_def_levels != num_rep_levels) {
      throw ParquetException("Number of decoded rep / def levels did not match");
    }
  }

  *values_read = this->ReadValues(values_to_read, values);
  int64_t total_values = std::max(num_def_levels, *values_read);
  this->ConsumeBufferedValues(total_values);

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
  batch_size =
      std::min(batch_size, this->num_buffered_values_ - this->num_decoded_values_);

  // If the field is required and non-repeated, there are no definition levels
  if (this->max_def_level_ > 0) {
    int64_t num_def_levels = this->ReadDefinitionLevels(batch_size, def_levels);

    // Not present for non-repeated fields
    if (this->max_rep_level_ > 0) {
      int64_t num_rep_levels = this->ReadRepetitionLevels(batch_size, rep_levels);
      if (num_def_levels != num_rep_levels) {
        throw ParquetException("Number of decoded rep / def levels did not match");
      }
    }

    const bool has_spaced_values = internal::HasSpacedValues(this->descr_);

    int64_t null_count = 0;
    if (!has_spaced_values) {
      int values_to_read = 0;
      for (int64_t i = 0; i < num_def_levels; ++i) {
        if (def_levels[i] == this->max_def_level_) {
          ++values_to_read;
        }
      }
      total_values = this->ReadValues(values_to_read, values);
      for (int64_t i = 0; i < total_values; i++) {
        ::arrow::BitUtil::SetBit(valid_bits, valid_bits_offset + i);
      }
      *values_read = total_values;
    } else {
      internal::DefinitionLevelsToBitmap(def_levels, num_def_levels, this->max_def_level_,
                                         this->max_rep_level_, values_read, &null_count,
                                         valid_bits, valid_bits_offset);
      total_values =
          this->ReadValuesSpaced(*values_read, values, static_cast<int>(null_count),
                                 valid_bits, valid_bits_offset);
    }
    *levels_read = num_def_levels;
    *null_count_out = null_count;

  } else {
    // Required field, read all values
    total_values = this->ReadValues(batch_size, values);
    for (int64_t i = 0; i < total_values; i++) {
      ::arrow::BitUtil::SetBit(valid_bits, valid_bits_offset + i);
    }
    *null_count_out = 0;
    *levels_read = total_values;
  }

  this->ConsumeBufferedValues(*levels_read);
  return total_values;
}

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::Skip(int64_t num_rows_to_skip) {
  int64_t rows_to_skip = num_rows_to_skip;
  while (HasNext() && rows_to_skip > 0) {
    // If the number of rows to skip is more than the number of undecoded values, skip the
    // Page.
    if (rows_to_skip > (this->num_buffered_values_ - this->num_decoded_values_)) {
      rows_to_skip -= this->num_buffered_values_ - this->num_decoded_values_;
      this->num_decoded_values_ = this->num_buffered_values_;
    } else {
      // We need to read this Page
      // Jump to the right offset in the Page
      int64_t batch_size = 1024;  // ReadBatch with a smaller memory footprint
      int64_t values_read = 0;

      // This will be enough scratch space to accommodate 16-bit levels or any
      // value type
      std::shared_ptr<ResizableBuffer> scratch = AllocateBuffer(
          this->pool_, batch_size * type_traits<DType::type_num>::value_byte_size);

      do {
        batch_size = std::min(batch_size, rows_to_skip);
        values_read =
            ReadBatch(static_cast<int>(batch_size),
                      reinterpret_cast<int16_t*>(scratch->mutable_data()),
                      reinterpret_cast<int16_t*>(scratch->mutable_data()),
                      reinterpret_cast<T*>(scratch->mutable_data()), &values_read);
        rows_to_skip -= values_read;
      } while (values_read > 0 && rows_to_skip > 0);
    }
  }
  return num_rows_to_skip - rows_to_skip;
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

// ----------------------------------------------------------------------
// RecordReader

namespace internal {

// The minimum number of repetition/definition levels to decode at a time, for
// better vectorized performance when doing many smaller record reads
constexpr int64_t kMinLevelBatchSize = 1024;

template <typename DType>
class TypedRecordReader : public ColumnReaderImplBase<DType>,
                          virtual public RecordReader {
 public:
  using T = typename DType::c_type;
  using BASE = ColumnReaderImplBase<DType>;
  TypedRecordReader(const ColumnDescriptor* descr, MemoryPool* pool) : BASE(descr, pool) {
    nullable_values_ = internal::HasSpacedValues(descr);
    at_record_start_ = true;
    records_read_ = 0;
    values_written_ = 0;
    values_capacity_ = 0;
    null_count_ = 0;
    levels_written_ = 0;
    levels_position_ = 0;
    levels_capacity_ = 0;
    uses_values_ = !(descr->physical_type() == Type::BYTE_ARRAY);

    if (uses_values_) {
      values_ = AllocateBuffer(pool);
    }
    valid_bits_ = AllocateBuffer(pool);
    def_levels_ = AllocateBuffer(pool);
    rep_levels_ = AllocateBuffer(pool);
    Reset();
  }

  int64_t available_values_current_page() const {
    return this->num_buffered_values_ - this->num_decoded_values_;
  }

  int64_t ReadRecords(int64_t num_records) override {
    // Delimit records, then read values at the end
    int64_t records_read = 0;

    if (levels_position_ < levels_written_) {
      records_read += ReadRecordData(num_records);
    }

    int64_t level_batch_size = std::max(kMinLevelBatchSize, num_records);

    // If we are in the middle of a record, we continue until reaching the
    // desired number of records or the end of the current record if we've found
    // enough records
    while (!at_record_start_ || records_read < num_records) {
      // Is there more data to read in this row group?
      if (!this->HasNextInternal()) {
        if (!at_record_start_) {
          // We ended the row group while inside a record that we haven't seen
          // the end of yet. So increment the record count for the last record in
          // the row group
          ++records_read;
          at_record_start_ = true;
        }
        break;
      }

      /// We perform multiple batch reads until we either exhaust the row group
      /// or observe the desired number of records
      int64_t batch_size = std::min(level_batch_size, available_values_current_page());

      // No more data in column
      if (batch_size == 0) {
        break;
      }

      if (this->max_def_level_ > 0) {
        ReserveLevels(batch_size);

        int16_t* def_levels = this->def_levels() + levels_written_;
        int16_t* rep_levels = this->rep_levels() + levels_written_;

        // Not present for non-repeated fields
        int64_t levels_read = 0;
        if (this->max_rep_level_ > 0) {
          levels_read = this->ReadDefinitionLevels(batch_size, def_levels);
          if (this->ReadRepetitionLevels(batch_size, rep_levels) != levels_read) {
            throw ParquetException("Number of decoded rep / def levels did not match");
          }
        } else if (this->max_def_level_ > 0) {
          levels_read = this->ReadDefinitionLevels(batch_size, def_levels);
        }

        // Exhausted column chunk
        if (levels_read == 0) {
          break;
        }

        levels_written_ += levels_read;
        records_read += ReadRecordData(num_records - records_read);
      } else {
        // No repetition or definition levels
        batch_size = std::min(num_records - records_read, batch_size);
        records_read += ReadRecordData(batch_size);
      }
    }

    return records_read;
  }

  // We may outwardly have the appearance of having exhausted a column chunk
  // when in fact we are in the middle of processing the last batch
  bool has_values_to_process() const { return levels_position_ < levels_written_; }

  std::shared_ptr<ResizableBuffer> ReleaseValues() override {
    if (uses_values_) {
      auto result = values_;
      values_ = AllocateBuffer(this->pool_);
      return result;
    } else {
      return nullptr;
    }
  }

  std::shared_ptr<ResizableBuffer> ReleaseIsValid() override {
    if (nullable_values_) {
      auto result = valid_bits_;
      valid_bits_ = AllocateBuffer(this->pool_);
      return result;
    } else {
      return nullptr;
    }
  }

  // Process written repetition/definition levels to reach the end of
  // records. Process no more levels than necessary to delimit the indicated
  // number of logical records. Updates internal state of RecordReader
  //
  // \return Number of records delimited
  int64_t DelimitRecords(int64_t num_records, int64_t* values_seen) {
    int64_t values_to_read = 0;
    int64_t records_read = 0;

    const int16_t* def_levels = this->def_levels() + levels_position_;
    const int16_t* rep_levels = this->rep_levels() + levels_position_;

    DCHECK_GT(this->max_rep_level_, 0);

    // Count logical records and number of values to read
    while (levels_position_ < levels_written_) {
      if (*rep_levels++ == 0) {
        // If at_record_start_ is true, we are seeing the start of a record
        // for the second time, such as after repeated calls to
        // DelimitRecords. In this case we must continue until we find
        // another record start or exhausting the ColumnChunk
        if (!at_record_start_) {
          // We've reached the end of a record; increment the record count.
          ++records_read;
          if (records_read == num_records) {
            // We've found the number of records we were looking for. Set
            // at_record_start_ to true and break
            at_record_start_ = true;
            break;
          }
        }
      }

      // We have decided to consume the level at this position; therefore we
      // must advance until we find another record boundary
      at_record_start_ = false;

      if (*def_levels++ == this->max_def_level_) {
        ++values_to_read;
      }
      ++levels_position_;
    }
    *values_seen = values_to_read;
    return records_read;
  }

  void Reserve(int64_t capacity) override {
    ReserveLevels(capacity);
    ReserveValues(capacity);
  }

  void ReserveLevels(int64_t capacity) {
    if (this->max_def_level_ > 0 && (levels_written_ + capacity > levels_capacity_)) {
      int64_t new_levels_capacity = BitUtil::NextPower2(levels_capacity_ + 1);
      while (levels_written_ + capacity > new_levels_capacity) {
        new_levels_capacity = BitUtil::NextPower2(new_levels_capacity + 1);
      }
      PARQUET_THROW_NOT_OK(
          def_levels_->Resize(new_levels_capacity * sizeof(int16_t), false));
      if (this->max_rep_level_ > 0) {
        PARQUET_THROW_NOT_OK(
            rep_levels_->Resize(new_levels_capacity * sizeof(int16_t), false));
      }
      levels_capacity_ = new_levels_capacity;
    }
  }

  void ReserveValues(int64_t capacity) {
    if (values_written_ + capacity > values_capacity_) {
      int64_t new_values_capacity = BitUtil::NextPower2(values_capacity_ + 1);
      while (values_written_ + capacity > new_values_capacity) {
        new_values_capacity = BitUtil::NextPower2(new_values_capacity + 1);
      }

      int type_size = GetTypeByteSize(this->descr_->physical_type());

      // XXX(wesm): A hack to avoid memory allocation when reading directly
      // into builder classes
      if (uses_values_) {
        PARQUET_THROW_NOT_OK(values_->Resize(new_values_capacity * type_size, false));
      }

      values_capacity_ = new_values_capacity;
    }
    if (nullable_values_) {
      int64_t valid_bytes_new = BitUtil::BytesForBits(values_capacity_);
      if (valid_bits_->size() < valid_bytes_new) {
        int64_t valid_bytes_old = BitUtil::BytesForBits(values_written_);
        PARQUET_THROW_NOT_OK(valid_bits_->Resize(valid_bytes_new, false));

        // Avoid valgrind warnings
        memset(valid_bits_->mutable_data() + valid_bytes_old, 0,
               valid_bytes_new - valid_bytes_old);
      }
    }
  }

  void Reset() override {
    ResetValues();

    if (levels_written_ > 0) {
      const int64_t levels_remaining = levels_written_ - levels_position_;
      // Shift remaining levels to beginning of buffer and trim to only the number
      // of decoded levels remaining
      int16_t* def_data = def_levels();
      int16_t* rep_data = rep_levels();

      std::copy(def_data + levels_position_, def_data + levels_written_, def_data);
      PARQUET_THROW_NOT_OK(
          def_levels_->Resize(levels_remaining * sizeof(int16_t), false));

      if (this->max_rep_level_ > 0) {
        std::copy(rep_data + levels_position_, rep_data + levels_written_, rep_data);
        PARQUET_THROW_NOT_OK(
            rep_levels_->Resize(levels_remaining * sizeof(int16_t), false));
      }

      levels_written_ -= levels_position_;
      levels_position_ = 0;
      levels_capacity_ = levels_remaining;
    }

    records_read_ = 0;

    // Call Finish on the binary builders to reset them
  }

  void SetPageReader(std::unique_ptr<PageReader> reader) override {
    at_record_start_ = true;
    this->pager_ = std::move(reader);
    ResetDecoders();
  }

  bool HasMoreData() const override { return this->pager_ != nullptr; }

  // Dictionary decoders must be reset when advancing row groups
  void ResetDecoders() { this->decoders_.clear(); }

  virtual void ReadValuesSpaced(int64_t values_with_nulls, int64_t null_count) {
    uint8_t* valid_bits = valid_bits_->mutable_data();
    const int64_t valid_bits_offset = values_written_;

    int64_t num_decoded = this->current_decoder_->DecodeSpaced(
        ValuesHead<T>(), static_cast<int>(values_with_nulls),
        static_cast<int>(null_count), valid_bits, valid_bits_offset);
    DCHECK_EQ(num_decoded, values_with_nulls);
  }

  virtual void ReadValuesDense(int64_t values_to_read) {
    int64_t num_decoded =
        this->current_decoder_->Decode(ValuesHead<T>(), static_cast<int>(values_to_read));
    DCHECK_EQ(num_decoded, values_to_read);
  }

  // Return number of logical records read
  int64_t ReadRecordData(int64_t num_records) {
    // Conservative upper bound
    const int64_t possible_num_values =
        std::max(num_records, levels_written_ - levels_position_);
    ReserveValues(possible_num_values);

    const int64_t start_levels_position = levels_position_;

    int64_t values_to_read = 0;
    int64_t records_read = 0;
    if (this->max_rep_level_ > 0) {
      records_read = DelimitRecords(num_records, &values_to_read);
    } else if (this->max_def_level_ > 0) {
      // No repetition levels, skip delimiting logic. Each level represents a
      // null or not null entry
      records_read = std::min(levels_written_ - levels_position_, num_records);

      // This is advanced by DelimitRecords, which we skipped
      levels_position_ += records_read;
    } else {
      records_read = values_to_read = num_records;
    }

    int64_t null_count = 0;
    if (nullable_values_) {
      int64_t values_with_nulls = 0;
      internal::DefinitionLevelsToBitmap(
          def_levels() + start_levels_position, levels_position_ - start_levels_position,
          this->max_def_level_, this->max_rep_level_, &values_with_nulls, &null_count,
          valid_bits_->mutable_data(), values_written_);
      values_to_read = values_with_nulls - null_count;
      ReadValuesSpaced(values_with_nulls, null_count);
      this->ConsumeBufferedValues(levels_position_ - start_levels_position);
    } else {
      ReadValuesDense(values_to_read);
      this->ConsumeBufferedValues(values_to_read);
    }
    // Total values, including null spaces, if any
    values_written_ += values_to_read + null_count;
    null_count_ += null_count;

    return records_read;
  }

  void DebugPrintState() override {
    const int16_t* def_levels = this->def_levels();
    const int16_t* rep_levels = this->rep_levels();
    const int64_t total_levels_read = levels_position_;

    const T* vals = reinterpret_cast<const T*>(this->values());

    std::cout << "def levels: ";
    for (int64_t i = 0; i < total_levels_read; ++i) {
      std::cout << def_levels[i] << " ";
    }
    std::cout << std::endl;

    std::cout << "rep levels: ";
    for (int64_t i = 0; i < total_levels_read; ++i) {
      std::cout << rep_levels[i] << " ";
    }
    std::cout << std::endl;

    std::cout << "values: ";
    for (int64_t i = 0; i < this->values_written(); ++i) {
      std::cout << vals[i] << " ";
    }
    std::cout << std::endl;
  }

  void ResetValues() {
    if (values_written_ > 0) {
      // Resize to 0, but do not shrink to fit
      if (uses_values_) {
        PARQUET_THROW_NOT_OK(values_->Resize(0, false));
      }
      PARQUET_THROW_NOT_OK(valid_bits_->Resize(0, false));
      values_written_ = 0;
      values_capacity_ = 0;
      null_count_ = 0;
    }
  }

 protected:
  template <typename T>
  T* ValuesHead() {
    return reinterpret_cast<T*>(values_->mutable_data()) + values_written_;
  }
};

class FLBARecordReader : public TypedRecordReader<FLBAType>,
                         virtual public BinaryRecordReader {
 public:
  FLBARecordReader(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool)
      : TypedRecordReader<FLBAType>(descr, pool), builder_(nullptr) {
    DCHECK_EQ(descr_->physical_type(), Type::FIXED_LEN_BYTE_ARRAY);
    int byte_width = descr_->type_length();
    std::shared_ptr<::arrow::DataType> type = ::arrow::fixed_size_binary(byte_width);
    builder_.reset(new ::arrow::FixedSizeBinaryBuilder(type, this->pool_));
  }

  ::arrow::ArrayVector GetBuilderChunks() override {
    std::shared_ptr<::arrow::Array> chunk;
    PARQUET_THROW_NOT_OK(builder_->Finish(&chunk));
    return ::arrow::ArrayVector({chunk});
  }

  void ReadValuesDense(int64_t values_to_read) override {
    auto values = ValuesHead<FLBA>();
    int64_t num_decoded =
        this->current_decoder_->Decode(values, static_cast<int>(values_to_read));
    DCHECK_EQ(num_decoded, values_to_read);

    for (int64_t i = 0; i < num_decoded; i++) {
      PARQUET_THROW_NOT_OK(builder_->Append(values[i].ptr));
    }
    ResetValues();
  }

  void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
    uint8_t* valid_bits = valid_bits_->mutable_data();
    const int64_t valid_bits_offset = values_written_;
    auto values = ValuesHead<FLBA>();

    int64_t num_decoded = this->current_decoder_->DecodeSpaced(
        values, static_cast<int>(values_to_read), static_cast<int>(null_count),
        valid_bits, valid_bits_offset);
    DCHECK_EQ(num_decoded, values_to_read);

    for (int64_t i = 0; i < num_decoded; i++) {
      if (::arrow::BitUtil::GetBit(valid_bits, valid_bits_offset + i)) {
        PARQUET_THROW_NOT_OK(builder_->Append(values[i].ptr));
      } else {
        PARQUET_THROW_NOT_OK(builder_->AppendNull());
      }
    }
    ResetValues();
  }

 private:
  std::unique_ptr<::arrow::FixedSizeBinaryBuilder> builder_;
};

class ByteArrayChunkedRecordReader : public TypedRecordReader<ByteArrayType>,
                                     virtual public BinaryRecordReader {
 public:
  ByteArrayChunkedRecordReader(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool)
      : TypedRecordReader<ByteArrayType>(descr, pool), builder_(nullptr) {
    // ARROW-4688(wesm): Using 2^31 - 1 chunks for now
    constexpr int32_t kBinaryChunksize = 2147483647;
    DCHECK_EQ(descr_->physical_type(), Type::BYTE_ARRAY);
    builder_.reset(
        new ::arrow::internal::ChunkedBinaryBuilder(kBinaryChunksize, this->pool_));
  }

  ::arrow::ArrayVector GetBuilderChunks() override {
    ::arrow::ArrayVector chunks;
    PARQUET_THROW_NOT_OK(builder_->Finish(&chunks));
    return chunks;
  }

  void ReadValuesDense(int64_t values_to_read) override {
    int64_t num_decoded = this->current_decoder_->DecodeArrowNonNull(
        static_cast<int>(values_to_read), builder_.get());
    DCHECK_EQ(num_decoded, values_to_read);
    ResetValues();
  }

  void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
    int64_t num_decoded = this->current_decoder_->DecodeArrow(
        static_cast<int>(values_to_read), static_cast<int>(null_count),
        valid_bits_->mutable_data(), values_written_, builder_.get());
    DCHECK_EQ(num_decoded, values_to_read - null_count);
    ResetValues();
  }

 private:
  std::unique_ptr<::arrow::internal::ChunkedBinaryBuilder> builder_;
};

class ByteArrayDictionaryRecordReader : public TypedRecordReader<ByteArrayType>,
                                        virtual public DictionaryRecordReader {
 public:
  ByteArrayDictionaryRecordReader(const ColumnDescriptor* descr,
                                  ::arrow::MemoryPool* pool)
      : TypedRecordReader<ByteArrayType>(descr, pool), builder_(pool) {
    this->read_dictionary_ = true;
  }

  std::shared_ptr<::arrow::ChunkedArray> GetResult() override {
    FlushBuilder();
    return std::make_shared<::arrow::ChunkedArray>(result_chunks_);
  }

  void FlushBuilder() {
    if (builder_.length() > 0) {
      std::shared_ptr<::arrow::Array> chunk;
      PARQUET_THROW_NOT_OK(builder_.Finish(&chunk));
      result_chunks_.emplace_back(std::move(chunk));

      // Reset clears the dictionary memo table
      builder_.Reset();
    }
  }

  void MaybeWriteNewDictionary() {
    if (this->new_dictionary_) {
      /// If there is a new dictionary, we may need to flush the builder, then
      /// insert the new dictionary values
      FlushBuilder();
      auto decoder = dynamic_cast<BinaryDictDecoder*>(this->current_decoder_);
      decoder->InsertDictionary(&builder_);
      this->new_dictionary_ = false;
    }
  }

  void ReadValuesDense(int64_t values_to_read) override {
    int64_t num_decoded = 0;
    if (current_encoding_ == Encoding::RLE_DICTIONARY) {
      MaybeWriteNewDictionary();
      auto decoder = dynamic_cast<BinaryDictDecoder*>(this->current_decoder_);
      num_decoded = decoder->DecodeIndices(static_cast<int>(values_to_read), &builder_);
    } else {
      num_decoded = this->current_decoder_->DecodeArrowNonNull(
          static_cast<int>(values_to_read), &builder_);

      /// Flush values since they have been copied into the builder
      ResetValues();
    }
    DCHECK_EQ(num_decoded, values_to_read);
  }

  void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
    int64_t num_decoded = 0;
    if (current_encoding_ == Encoding::RLE_DICTIONARY) {
      MaybeWriteNewDictionary();
      auto decoder = dynamic_cast<BinaryDictDecoder*>(this->current_decoder_);
      num_decoded = decoder->DecodeIndicesSpaced(
          static_cast<int>(values_to_read), static_cast<int>(null_count),
          valid_bits_->mutable_data(), values_written_, &builder_);
    } else {
      num_decoded = this->current_decoder_->DecodeArrow(
          static_cast<int>(values_to_read), static_cast<int>(null_count),
          valid_bits_->mutable_data(), values_written_, &builder_);

      /// Flush values since they have been copied into the builder
      ResetValues();
    }
    DCHECK_EQ(num_decoded, values_to_read - null_count);
  }

 private:
  using BinaryDictDecoder = DictDecoder<ByteArrayType>;

  ::arrow::BinaryDictionary32Builder builder_;
  std::vector<std::shared_ptr<::arrow::Array>> result_chunks_;
};

// TODO(wesm): Implement these to some satisfaction
template <>
void TypedRecordReader<Int96Type>::DebugPrintState() {}

template <>
void TypedRecordReader<ByteArrayType>::DebugPrintState() {}

template <>
void TypedRecordReader<FLBAType>::DebugPrintState() {}

std::shared_ptr<RecordReader> MakeByteArrayRecordReader(const ColumnDescriptor* descr,
                                                        arrow::MemoryPool* pool,
                                                        bool read_dictionary) {
  if (read_dictionary) {
    return std::make_shared<ByteArrayDictionaryRecordReader>(descr, pool);
  } else {
    return std::make_shared<ByteArrayChunkedRecordReader>(descr, pool);
  }
}

std::shared_ptr<RecordReader> RecordReader::Make(const ColumnDescriptor* descr,
                                                 MemoryPool* pool,
                                                 const bool read_dictionary) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::make_shared<TypedRecordReader<BooleanType>>(descr, pool);
    case Type::INT32:
      return std::make_shared<TypedRecordReader<Int32Type>>(descr, pool);
    case Type::INT64:
      return std::make_shared<TypedRecordReader<Int64Type>>(descr, pool);
    case Type::INT96:
      return std::make_shared<TypedRecordReader<Int96Type>>(descr, pool);
    case Type::FLOAT:
      return std::make_shared<TypedRecordReader<FloatType>>(descr, pool);
    case Type::DOUBLE:
      return std::make_shared<TypedRecordReader<DoubleType>>(descr, pool);
    case Type::BYTE_ARRAY:
      return MakeByteArrayRecordReader(descr, pool, read_dictionary);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FLBARecordReader>(descr, pool);
    default: {
      // PARQUET-1481: This can occur if the file is corrupt
      std::stringstream ss;
      ss << "Invalid physical column type: " << static_cast<int>(descr->physical_type());
      throw ParquetException(ss.str());
    }
  }
  // Unreachable code, but supress compiler warning
  return nullptr;
}

}  // namespace internal
}  // namespace parquet

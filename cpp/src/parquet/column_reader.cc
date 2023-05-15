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
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/chunked_array.h"
#include "arrow/type.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/crc32.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_encoding.h"
#include "parquet/column_page.h"
#include "parquet/encoding.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/level_comparison.h"
#include "parquet/level_conversion.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"
#include "parquet/thrift_internal.h"  // IWYU pragma: keep
#include "parquet/windows_fixup.h"    // for OPTIONAL

using arrow::MemoryPool;
using arrow::internal::AddWithOverflow;
using arrow::internal::checked_cast;
using arrow::internal::MultiplyWithOverflow;

namespace bit_util = arrow::bit_util;

namespace parquet {

namespace {

// The minimum number of repetition/definition levels to decode at a time, for
// better vectorized performance when doing many smaller record reads
constexpr int64_t kMinLevelBatchSize = 1024;

// Batch size for reading and throwing away values during skip.
// Both RecordReader and the ColumnReader use this for skipping.
constexpr int64_t kSkipScratchBatchSize = 1024;

inline bool HasSpacedValues(const ColumnDescriptor* descr) {
  if (descr->max_repetition_level() > 0) {
    // repeated+flat case
    return !descr->schema_node()->is_required();
  } else {
    // non-repeated+nested case
    // Find if a node forces nulls in the lowest level along the hierarchy
    const schema::Node* node = descr->schema_node().get();
    while (node) {
      if (node->is_optional()) {
        return true;
      }
      node = node->parent();
    }
    return false;
  }
}

// Throws exception if number_decoded does not match expected.
inline void CheckNumberDecoded(int64_t number_decoded, int64_t expected) {
  if (ARROW_PREDICT_FALSE(number_decoded != expected)) {
    ParquetException::EofException("Decoded values " + std::to_string(number_decoded) +
                                   " does not match expected " +
                                   std::to_string(expected));
  }
}
}  // namespace

LevelDecoder::LevelDecoder() : num_values_remaining_(0) {}

LevelDecoder::~LevelDecoder() {}

int LevelDecoder::SetData(Encoding::type encoding, int16_t max_level,
                          int num_buffered_values, const uint8_t* data,
                          int32_t data_size) {
  max_level_ = max_level;
  int32_t num_bytes = 0;
  encoding_ = encoding;
  num_values_remaining_ = num_buffered_values;
  bit_width_ = bit_util::Log2(max_level + 1);
  switch (encoding) {
    case Encoding::RLE: {
      if (data_size < 4) {
        throw ParquetException("Received invalid levels (corrupt data page?)");
      }
      num_bytes = ::arrow::util::SafeLoadAs<int32_t>(data);
      if (num_bytes < 0 || num_bytes > data_size - 4) {
        throw ParquetException("Received invalid number of bytes (corrupt data page?)");
      }
      const uint8_t* decoder_data = data + 4;
      if (!rle_decoder_) {
        rle_decoder_ = std::make_unique<::arrow::util::RleDecoder>(decoder_data,
                                                                   num_bytes, bit_width_);
      } else {
        rle_decoder_->Reset(decoder_data, num_bytes, bit_width_);
      }
      return 4 + num_bytes;
    }
    case Encoding::BIT_PACKED: {
      int num_bits = 0;
      if (MultiplyWithOverflow(num_buffered_values, bit_width_, &num_bits)) {
        throw ParquetException(
            "Number of buffered values too large (corrupt data page?)");
      }
      num_bytes = static_cast<int32_t>(bit_util::BytesForBits(num_bits));
      if (num_bytes < 0 || num_bytes > data_size - 4) {
        throw ParquetException("Received invalid number of bytes (corrupt data page?)");
      }
      if (!bit_packed_decoder_) {
        bit_packed_decoder_ =
            std::make_unique<::arrow::bit_util::BitReader>(data, num_bytes);
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

void LevelDecoder::SetDataV2(int32_t num_bytes, int16_t max_level,
                             int num_buffered_values, const uint8_t* data) {
  max_level_ = max_level;
  // Repetition and definition levels always uses RLE encoding
  // in the DataPageV2 format.
  if (num_bytes < 0) {
    throw ParquetException("Invalid page header (corrupt data page?)");
  }
  encoding_ = Encoding::RLE;
  num_values_remaining_ = num_buffered_values;
  bit_width_ = bit_util::Log2(max_level + 1);

  if (!rle_decoder_) {
    rle_decoder_ =
        std::make_unique<::arrow::util::RleDecoder>(data, num_bytes, bit_width_);
  } else {
    rle_decoder_->Reset(data, num_bytes, bit_width_);
  }
}

int LevelDecoder::Decode(int batch_size, int16_t* levels) {
  int num_decoded = 0;

  int num_values = std::min(num_values_remaining_, batch_size);
  if (encoding_ == Encoding::RLE) {
    num_decoded = rle_decoder_->GetBatch(levels, num_values);
  } else {
    num_decoded = bit_packed_decoder_->GetBatch(bit_width_, levels, num_values);
  }
  if (num_decoded > 0) {
    internal::MinMax min_max = internal::FindMinMax(levels, num_decoded);
    if (ARROW_PREDICT_FALSE(min_max.min < 0 || min_max.max > max_level_)) {
      std::stringstream ss;
      ss << "Malformed levels. min: " << min_max.min << " max: " << min_max.max
         << " out of range.  Max Level: " << max_level_;
      throw ParquetException(ss.str());
    }
  }
  num_values_remaining_ -= num_decoded;
  return num_decoded;
}

ReaderProperties default_reader_properties() {
  static ReaderProperties default_reader_properties;
  return default_reader_properties;
}

namespace {

// Extracts encoded statistics from V1 and V2 data page headers
template <typename H>
EncodedStatistics ExtractStatsFromHeader(const H& header) {
  EncodedStatistics page_statistics;
  if (!header.__isset.statistics) {
    return page_statistics;
  }
  const format::Statistics& stats = header.statistics;
  // Use the new V2 min-max statistics over the former one if it is filled
  if (stats.__isset.max_value || stats.__isset.min_value) {
    // TODO: check if the column_order is TYPE_DEFINED_ORDER.
    if (stats.__isset.max_value) {
      page_statistics.set_max(stats.max_value);
    }
    if (stats.__isset.min_value) {
      page_statistics.set_min(stats.min_value);
    }
  } else if (stats.__isset.max || stats.__isset.min) {
    // TODO: check created_by to see if it is corrupted for some types.
    // TODO: check if the sort_order is SIGNED.
    if (stats.__isset.max) {
      page_statistics.set_max(stats.max);
    }
    if (stats.__isset.min) {
      page_statistics.set_min(stats.min);
    }
  }
  if (stats.__isset.null_count) {
    page_statistics.set_null_count(stats.null_count);
  }
  if (stats.__isset.distinct_count) {
    page_statistics.set_distinct_count(stats.distinct_count);
  }
  return page_statistics;
}

void CheckNumValuesInHeader(int num_values) {
  if (num_values < 0) {
    throw ParquetException("Invalid page header (negative number of values)");
  }
}

// ----------------------------------------------------------------------
// SerializedPageReader deserializes Thrift metadata and pages that have been
// assembled in a serialized stream for storing in a Parquet files

// This subclass delimits pages appearing in a serialized stream, each preceded
// by a serialized Thrift format::PageHeader indicating the type of each page
// and the page metadata.
class SerializedPageReader : public PageReader {
 public:
  SerializedPageReader(std::shared_ptr<ArrowInputStream> stream, int64_t total_num_values,
                       Compression::type codec, const ReaderProperties& properties,
                       const CryptoContext* crypto_ctx, bool always_compressed)
      : properties_(properties),
        stream_(std::move(stream)),
        decompression_buffer_(AllocateBuffer(properties_.memory_pool(), 0)),
        page_ordinal_(0),
        seen_num_values_(0),
        total_num_values_(total_num_values),
        decryption_buffer_(AllocateBuffer(properties_.memory_pool(), 0)) {
    if (crypto_ctx != nullptr) {
      crypto_ctx_ = *crypto_ctx;
      InitDecryption();
    }
    max_page_header_size_ = kDefaultMaxPageHeaderSize;
    decompressor_ = GetCodec(codec);
    always_compressed_ = always_compressed;
  }

  // Implement the PageReader interface
  //
  // The returned Page contains references that aren't guaranteed to live
  // beyond the next call to NextPage(). SerializedPageReader reuses the
  // decryption and decompression buffers internally, so if NextPage() is
  // called then the content of previous page might be invalidated.
  std::shared_ptr<Page> NextPage() override;

  void set_max_page_header_size(uint32_t size) override { max_page_header_size_ = size; }

 private:
  void UpdateDecryption(const std::shared_ptr<Decryptor>& decryptor, int8_t module_type,
                        std::string* page_aad);

  void InitDecryption();

  std::shared_ptr<Buffer> DecompressIfNeeded(std::shared_ptr<Buffer> page_buffer,
                                             int compressed_len, int uncompressed_len,
                                             int levels_byte_len = 0);

  // Returns true for non-data pages, and if we should skip based on
  // data_page_filter_. Performs basic checks on values in the page header.
  // Fills in data_page_statistics.
  bool ShouldSkipPage(EncodedStatistics* data_page_statistics);

  const ReaderProperties properties_;
  std::shared_ptr<ArrowInputStream> stream_;

  format::PageHeader current_page_header_;
  std::shared_ptr<Page> current_page_;

  // Compression codec to use.
  std::unique_ptr<::arrow::util::Codec> decompressor_;
  std::shared_ptr<ResizableBuffer> decompression_buffer_;

  bool always_compressed_;

  // The fields below are used for calculation of AAD (additional authenticated data)
  // suffix which is part of the Parquet Modular Encryption.
  // The AAD suffix for a parquet module is built internally by
  // concatenating different parts some of which include
  // the row group ordinal, column ordinal and page ordinal.
  // Please refer to the encryption specification for more details:
  // https://github.com/apache/parquet-format/blob/encryption/Encryption.md#44-additional-authenticated-data

  // The ordinal fields in the context below are used for AAD suffix calculation.
  CryptoContext crypto_ctx_;
  int32_t page_ordinal_;  // page ordinal does not count the dictionary page

  // Maximum allowed page size
  uint32_t max_page_header_size_;

  // Number of values read in data pages so far
  int64_t seen_num_values_;

  // Number of values in all the data pages
  int64_t total_num_values_;

  // data_page_aad_ and data_page_header_aad_ contain the AAD for data page and data page
  // header in a single column respectively.
  // While calculating AAD for different pages in a single column the pages AAD is
  // updated by only the page ordinal.
  std::string data_page_aad_;
  std::string data_page_header_aad_;
  // Encryption
  std::shared_ptr<ResizableBuffer> decryption_buffer_;
};

void SerializedPageReader::InitDecryption() {
  // Prepare the AAD for quick update later.
  if (crypto_ctx_.data_decryptor != nullptr) {
    ARROW_DCHECK(!crypto_ctx_.data_decryptor->file_aad().empty());
    data_page_aad_ = encryption::CreateModuleAad(
        crypto_ctx_.data_decryptor->file_aad(), encryption::kDataPage,
        crypto_ctx_.row_group_ordinal, crypto_ctx_.column_ordinal, kNonPageOrdinal);
  }
  if (crypto_ctx_.meta_decryptor != nullptr) {
    ARROW_DCHECK(!crypto_ctx_.meta_decryptor->file_aad().empty());
    data_page_header_aad_ = encryption::CreateModuleAad(
        crypto_ctx_.meta_decryptor->file_aad(), encryption::kDataPageHeader,
        crypto_ctx_.row_group_ordinal, crypto_ctx_.column_ordinal, kNonPageOrdinal);
  }
}

void SerializedPageReader::UpdateDecryption(const std::shared_ptr<Decryptor>& decryptor,
                                            int8_t module_type, std::string* page_aad) {
  ARROW_DCHECK(decryptor != nullptr);
  if (crypto_ctx_.start_decrypt_with_dictionary_page) {
    std::string aad = encryption::CreateModuleAad(
        decryptor->file_aad(), module_type, crypto_ctx_.row_group_ordinal,
        crypto_ctx_.column_ordinal, kNonPageOrdinal);
    decryptor->UpdateAad(aad);
  } else {
    encryption::QuickUpdatePageAad(page_ordinal_, page_aad);
    decryptor->UpdateAad(*page_aad);
  }
}

bool SerializedPageReader::ShouldSkipPage(EncodedStatistics* data_page_statistics) {
  const PageType::type page_type = LoadEnumSafe(&current_page_header_.type);
  if (page_type == PageType::DATA_PAGE) {
    const format::DataPageHeader& header = current_page_header_.data_page_header;
    CheckNumValuesInHeader(header.num_values);
    *data_page_statistics = ExtractStatsFromHeader(header);
    seen_num_values_ += header.num_values;
    if (data_page_filter_) {
      const EncodedStatistics* filter_statistics =
          data_page_statistics->is_set() ? data_page_statistics : nullptr;
      DataPageStats data_page_stats(filter_statistics, header.num_values,
                                    /*num_rows=*/std::nullopt);
      if (data_page_filter_(data_page_stats)) {
        return true;
      }
    }
  } else if (page_type == PageType::DATA_PAGE_V2) {
    const format::DataPageHeaderV2& header = current_page_header_.data_page_header_v2;
    CheckNumValuesInHeader(header.num_values);
    if (header.num_rows < 0) {
      throw ParquetException("Invalid page header (negative number of rows)");
    }
    if (header.definition_levels_byte_length < 0 ||
        header.repetition_levels_byte_length < 0) {
      throw ParquetException("Invalid page header (negative levels byte length)");
    }
    *data_page_statistics = ExtractStatsFromHeader(header);
    seen_num_values_ += header.num_values;
    if (data_page_filter_) {
      const EncodedStatistics* filter_statistics =
          data_page_statistics->is_set() ? data_page_statistics : nullptr;
      DataPageStats data_page_stats(filter_statistics, header.num_values,
                                    header.num_rows);
      if (data_page_filter_(data_page_stats)) {
        return true;
      }
    }
  } else if (page_type == PageType::DICTIONARY_PAGE) {
    const format::DictionaryPageHeader& dict_header =
        current_page_header_.dictionary_page_header;
    CheckNumValuesInHeader(dict_header.num_values);
  } else {
    // We don't know what this page type is. We're allowed to skip non-data
    // pages.
    return true;
  }
  return false;
}

std::shared_ptr<Page> SerializedPageReader::NextPage() {
  ThriftDeserializer deserializer(properties_);

  // Loop here because there may be unhandled page types that we skip until
  // finding a page that we do know what to do with
  while (seen_num_values_ < total_num_values_) {
    uint32_t header_size = 0;
    uint32_t allowed_page_size = kDefaultPageHeaderSize;

    // Page headers can be very large because of page statistics
    // We try to deserialize a larger buffer progressively
    // until a maximum allowed header limit
    while (true) {
      PARQUET_ASSIGN_OR_THROW(auto view, stream_->Peek(allowed_page_size));
      if (view.size() == 0) {
        return std::shared_ptr<Page>(nullptr);
      }

      // This gets used, then set by DeserializeThriftMsg
      header_size = static_cast<uint32_t>(view.size());
      try {
        if (crypto_ctx_.meta_decryptor != nullptr) {
          UpdateDecryption(crypto_ctx_.meta_decryptor, encryption::kDictionaryPageHeader,
                           &data_page_header_aad_);
        }
        // Reset current page header to avoid unclearing the __isset flag.
        current_page_header_ = format::PageHeader();
        deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(view.data()),
                                        &header_size, &current_page_header_,
                                        crypto_ctx_.meta_decryptor);
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
    if (compressed_len < 0 || uncompressed_len < 0) {
      throw ParquetException("Invalid page header");
    }

    EncodedStatistics data_page_statistics;
    if (ShouldSkipPage(&data_page_statistics)) {
      PARQUET_THROW_NOT_OK(stream_->Advance(compressed_len));
      continue;
    }

    if (crypto_ctx_.data_decryptor != nullptr) {
      UpdateDecryption(crypto_ctx_.data_decryptor, encryption::kDictionaryPage,
                       &data_page_aad_);
    }

    // Read the compressed data page.
    PARQUET_ASSIGN_OR_THROW(auto page_buffer, stream_->Read(compressed_len));
    if (page_buffer->size() != compressed_len) {
      std::stringstream ss;
      ss << "Page was smaller (" << page_buffer->size() << ") than expected ("
         << compressed_len << ")";
      ParquetException::EofException(ss.str());
    }

    const PageType::type page_type = LoadEnumSafe(&current_page_header_.type);

    if (properties_.page_checksum_verification() && current_page_header_.__isset.crc &&
        PageCanUseChecksum(page_type)) {
      // verify crc
      uint32_t checksum =
          ::arrow::internal::crc32(/* prev */ 0, page_buffer->data(), compressed_len);
      if (static_cast<int32_t>(checksum) != current_page_header_.crc) {
        throw ParquetException(
            "could not verify page integrity, CRC checksum verification failed for "
            "page_ordinal " +
            std::to_string(page_ordinal_));
      }
    }

    // Decrypt it if we need to
    if (crypto_ctx_.data_decryptor != nullptr) {
      PARQUET_THROW_NOT_OK(decryption_buffer_->Resize(
          compressed_len - crypto_ctx_.data_decryptor->CiphertextSizeDelta(),
          /*shrink_to_fit=*/false));
      compressed_len = crypto_ctx_.data_decryptor->Decrypt(
          page_buffer->data(), compressed_len, decryption_buffer_->mutable_data());

      page_buffer = decryption_buffer_;
    }

    if (page_type == PageType::DICTIONARY_PAGE) {
      crypto_ctx_.start_decrypt_with_dictionary_page = false;
      const format::DictionaryPageHeader& dict_header =
          current_page_header_.dictionary_page_header;
      bool is_sorted = dict_header.__isset.is_sorted ? dict_header.is_sorted : false;

      page_buffer =
          DecompressIfNeeded(std::move(page_buffer), compressed_len, uncompressed_len);

      return std::make_shared<DictionaryPage>(page_buffer, dict_header.num_values,
                                              LoadEnumSafe(&dict_header.encoding),
                                              is_sorted);
    } else if (page_type == PageType::DATA_PAGE) {
      ++page_ordinal_;
      const format::DataPageHeader& header = current_page_header_.data_page_header;
      page_buffer =
          DecompressIfNeeded(std::move(page_buffer), compressed_len, uncompressed_len);

      return std::make_shared<DataPageV1>(page_buffer, header.num_values,
                                          LoadEnumSafe(&header.encoding),
                                          LoadEnumSafe(&header.definition_level_encoding),
                                          LoadEnumSafe(&header.repetition_level_encoding),
                                          uncompressed_len, data_page_statistics);
    } else if (page_type == PageType::DATA_PAGE_V2) {
      ++page_ordinal_;
      const format::DataPageHeaderV2& header = current_page_header_.data_page_header_v2;

      // Arrow prior to 3.0.0 set is_compressed to false but still compressed.
      bool is_compressed =
          (header.__isset.is_compressed ? header.is_compressed : false) ||
          always_compressed_;

      // Uncompress if needed
      int levels_byte_len;
      if (AddWithOverflow(header.definition_levels_byte_length,
                          header.repetition_levels_byte_length, &levels_byte_len)) {
        throw ParquetException("Levels size too large (corrupt file?)");
      }
      // DecompressIfNeeded doesn't take `is_compressed` into account as
      // it's page type-agnostic.
      if (is_compressed) {
        page_buffer = DecompressIfNeeded(std::move(page_buffer), compressed_len,
                                         uncompressed_len, levels_byte_len);
      }

      return std::make_shared<DataPageV2>(
          page_buffer, header.num_values, header.num_nulls, header.num_rows,
          LoadEnumSafe(&header.encoding), header.definition_levels_byte_length,
          header.repetition_levels_byte_length, uncompressed_len, is_compressed,
          data_page_statistics);
    } else {
      throw ParquetException(
          "Internal error, we have already skipped non-data pages in ShouldSkipPage()");
    }
  }
  return std::shared_ptr<Page>(nullptr);
}

std::shared_ptr<Buffer> SerializedPageReader::DecompressIfNeeded(
    std::shared_ptr<Buffer> page_buffer, int compressed_len, int uncompressed_len,
    int levels_byte_len) {
  if (decompressor_ == nullptr) {
    return page_buffer;
  }
  if (compressed_len < levels_byte_len || uncompressed_len < levels_byte_len) {
    throw ParquetException("Invalid page header");
  }

  // Grow the uncompressed buffer if we need to.
  PARQUET_THROW_NOT_OK(
      decompression_buffer_->Resize(uncompressed_len, /*shrink_to_fit=*/false));

  if (levels_byte_len > 0) {
    // First copy the levels as-is
    uint8_t* decompressed = decompression_buffer_->mutable_data();
    memcpy(decompressed, page_buffer->data(), levels_byte_len);
  }

  // Decompress the values
  PARQUET_THROW_NOT_OK(decompressor_->Decompress(
      compressed_len - levels_byte_len, page_buffer->data() + levels_byte_len,
      uncompressed_len - levels_byte_len,
      decompression_buffer_->mutable_data() + levels_byte_len));

  return decompression_buffer_;
}

}  // namespace

std::unique_ptr<PageReader> PageReader::Open(std::shared_ptr<ArrowInputStream> stream,
                                             int64_t total_num_values,
                                             Compression::type codec,
                                             const ReaderProperties& properties,
                                             bool always_compressed,
                                             const CryptoContext* ctx) {
  return std::unique_ptr<PageReader>(new SerializedPageReader(
      std::move(stream), total_num_values, codec, properties, ctx, always_compressed));
}

std::unique_ptr<PageReader> PageReader::Open(std::shared_ptr<ArrowInputStream> stream,
                                             int64_t total_num_values,
                                             Compression::type codec,
                                             bool always_compressed,
                                             ::arrow::MemoryPool* pool,
                                             const CryptoContext* ctx) {
  return std::unique_ptr<PageReader>(
      new SerializedPageReader(std::move(stream), total_num_values, codec,
                               ReaderProperties(pool), ctx, always_compressed));
}

namespace {

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
        int64_t levels_byte_size = InitializeLevelDecodersV2(*page);
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
    ARROW_DCHECK(current_decoder_);
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
    int32_t levels_byte_size = 0;
    int32_t max_size = page.size();

    // Data page Layout: Repetition Levels - Definition Levels - encoded values.
    // Levels are encoded as rle or bit-packed.
    // Init repetition levels
    if (max_rep_level_ > 0) {
      int32_t rep_levels_bytes = repetition_level_decoder_.SetData(
          repetition_level_encoding, max_rep_level_,
          static_cast<int>(num_buffered_values_), buffer, max_size);
      buffer += rep_levels_bytes;
      levels_byte_size += rep_levels_bytes;
      max_size -= rep_levels_bytes;
    }
    // TODO figure a way to set max_def_level_ to 0
    // if the initial value is invalid

    // Init definition levels
    if (max_def_level_ > 0) {
      int32_t def_levels_bytes = definition_level_decoder_.SetData(
          definition_level_encoding, max_def_level_,
          static_cast<int>(num_buffered_values_), buffer, max_size);
      levels_byte_size += def_levels_bytes;
      max_size -= def_levels_bytes;
    }

    return levels_byte_size;
  }

  int64_t InitializeLevelDecodersV2(const DataPageV2& page) {
    // Read a data page.
    num_buffered_values_ = page.num_values();

    // Have not decoded any values from the data page yet
    num_decoded_values_ = 0;
    const uint8_t* buffer = page.data();

    const int64_t total_levels_length =
        static_cast<int64_t>(page.repetition_levels_byte_length()) +
        page.definition_levels_byte_length();

    if (total_levels_length > page.size()) {
      throw ParquetException("Data page too small for levels (corrupt header?)");
    }

    if (max_rep_level_ > 0) {
      repetition_level_decoder_.SetDataV2(page.repetition_levels_byte_length(),
                                          max_rep_level_,
                                          static_cast<int>(num_buffered_values_), buffer);
    }
    // ARROW-17453: Even if max_rep_level_ is 0, there may still be
    // repetition level bytes written and/or reported in the header by
    // some writers (e.g. Athena)
    buffer += page.repetition_levels_byte_length();

    if (max_def_level_ > 0) {
      definition_level_decoder_.SetDataV2(page.definition_levels_byte_length(),
                                          max_def_level_,
                                          static_cast<int>(num_buffered_values_), buffer);
    }

    return total_levels_length;
  }

  // Get a decoder object for this page or create a new decoder if this is the
  // first page with this encoding.
  void InitializeDataDecoder(const DataPage& page, int64_t levels_byte_size) {
    const uint8_t* buffer = page.data() + levels_byte_size;
    const int64_t data_size = page.size() - levels_byte_size;

    if (data_size < 0) {
      throw ParquetException("Page smaller than size of encoded levels");
    }

    Encoding::type encoding = page.encoding();

    if (IsDictionaryIndexEncoding(encoding)) {
      encoding = Encoding::RLE_DICTIONARY;
    }

    auto it = decoders_.find(static_cast<int>(encoding));
    if (it != decoders_.end()) {
      ARROW_DCHECK(it->second.get() != nullptr);
      current_decoder_ = it->second.get();
    } else {
      switch (encoding) {
        case Encoding::PLAIN: {
          auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
          current_decoder_ = decoder.get();
          decoders_[static_cast<int>(encoding)] = std::move(decoder);
          break;
        }
        case Encoding::BYTE_STREAM_SPLIT: {
          auto decoder = MakeTypedDecoder<DType>(Encoding::BYTE_STREAM_SPLIT, descr_);
          current_decoder_ = decoder.get();
          decoders_[static_cast<int>(encoding)] = std::move(decoder);
          break;
        }
        case Encoding::RLE: {
          auto decoder = MakeTypedDecoder<DType>(Encoding::RLE, descr_);
          current_decoder_ = decoder.get();
          decoders_[static_cast<int>(encoding)] = std::move(decoder);
          break;
        }
        case Encoding::RLE_DICTIONARY:
          throw ParquetException("Dictionary page must be before data page.");

        case Encoding::DELTA_BINARY_PACKED: {
          auto decoder = MakeTypedDecoder<DType>(Encoding::DELTA_BINARY_PACKED, descr_);
          current_decoder_ = decoder.get();
          decoders_[static_cast<int>(encoding)] = std::move(decoder);
          break;
        }
        case Encoding::DELTA_BYTE_ARRAY: {
          auto decoder = MakeTypedDecoder<DType>(Encoding::DELTA_BYTE_ARRAY, descr_);
          current_decoder_ = decoder.get();
          decoders_[static_cast<int>(encoding)] = std::move(decoder);
          break;
        }
        case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
          auto decoder =
              MakeTypedDecoder<DType>(Encoding::DELTA_LENGTH_BYTE_ARRAY, descr_);
          current_decoder_ = decoder.get();
          decoders_[static_cast<int>(encoding)] = std::move(decoder);
          break;
        }

        default:
          throw ParquetException("Unknown encoding type.");
      }
    }
    current_encoding_ = encoding;
    current_decoder_->SetData(static_cast<int>(num_buffered_values_), buffer,
                              static_cast<int>(data_size));
  }

  int64_t available_values_current_page() const {
    return num_buffered_values_ - num_decoded_values_;
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

  using DecoderType = TypedDecoder<DType>;
  DecoderType* current_decoder_;
  Encoding::type current_encoding_;

  /// Flag to signal when a new dictionary has been set, for the benefit of
  /// DictionaryRecordReader
  bool new_dictionary_;

  // The exposed encoding
  ExposedEncoding exposed_encoding_ = ExposedEncoding::NO_ENCODING;

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

  int64_t Skip(int64_t num_values_to_skip) override;

  Type::type type() const override { return this->descr_->physical_type(); }

  const ColumnDescriptor* descr() const override { return this->descr_; }

  ExposedEncoding GetExposedEncoding() override { return this->exposed_encoding_; };

  int64_t ReadBatchWithDictionary(int64_t batch_size, int16_t* def_levels,
                                  int16_t* rep_levels, int32_t* indices,
                                  int64_t* indices_read, const T** dict,
                                  int32_t* dict_len) override;

 protected:
  void SetExposedEncoding(ExposedEncoding encoding) override {
    this->exposed_encoding_ = encoding;
  }

  // Allocate enough scratch space to accommodate skipping 16-bit levels or any
  // value type.
  void InitScratchForSkip();

  // Scratch space for reading and throwing away rep/def levels and values when
  // skipping.
  std::shared_ptr<ResizableBuffer> scratch_for_skip_;

 private:
  // Read dictionary indices. Similar to ReadValues but decode data to dictionary indices.
  // This function is called only by ReadBatchWithDictionary().
  int64_t ReadDictionaryIndices(int64_t indices_to_read, int32_t* indices) {
    auto decoder = dynamic_cast<DictDecoder<DType>*>(this->current_decoder_);
    return decoder->DecodeIndices(static_cast<int>(indices_to_read), indices);
  }

  // Get dictionary. The dictionary should have been set by SetDict(). The dictionary is
  // owned by the internal decoder and is destroyed when the reader is destroyed. This
  // function is called only by ReadBatchWithDictionary() after dictionary is configured.
  void GetDictionary(const T** dictionary, int32_t* dictionary_length) {
    auto decoder = dynamic_cast<DictDecoder<DType>*>(this->current_decoder_);
    decoder->GetDictionary(dictionary, dictionary_length);
  }

  // Read definition and repetition levels. Also return the number of definition levels
  // and number of values to read. This function is called before reading values.
  void ReadLevels(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                  int64_t* num_def_levels, int64_t* values_to_read) {
    batch_size =
        std::min(batch_size, this->num_buffered_values_ - this->num_decoded_values_);

    // If the field is required and non-repeated, there are no definition levels
    if (this->max_def_level_ > 0 && def_levels != nullptr) {
      *num_def_levels = this->ReadDefinitionLevels(batch_size, def_levels);
      // TODO(wesm): this tallying of values-to-decode can be performed with better
      // cache-efficiency if fused with the level decoding.
      for (int64_t i = 0; i < *num_def_levels; ++i) {
        if (def_levels[i] == this->max_def_level_) {
          ++(*values_to_read);
        }
      }
    } else {
      // Required field, read all values
      *values_to_read = batch_size;
    }

    // Not present for non-repeated fields
    if (this->max_rep_level_ > 0 && rep_levels != nullptr) {
      int64_t num_rep_levels = this->ReadRepetitionLevels(batch_size, rep_levels);
      if (def_levels != nullptr && *num_def_levels != num_rep_levels) {
        throw ParquetException("Number of decoded rep / def levels did not match");
      }
    }
  }
};

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::ReadBatchWithDictionary(
    int64_t batch_size, int16_t* def_levels, int16_t* rep_levels, int32_t* indices,
    int64_t* indices_read, const T** dict, int32_t* dict_len) {
  bool has_dict_output = dict != nullptr && dict_len != nullptr;
  // Similar logic as ReadValues to get pages.
  if (!HasNext()) {
    *indices_read = 0;
    if (has_dict_output) {
      *dict = nullptr;
      *dict_len = 0;
    }
    return 0;
  }

  // Verify the current data page is dictionary encoded.
  if (this->current_encoding_ != Encoding::RLE_DICTIONARY) {
    std::stringstream ss;
    ss << "Data page is not dictionary encoded. Encoding: "
       << EncodingToString(this->current_encoding_);
    throw ParquetException(ss.str());
  }

  // Get dictionary pointer and length.
  if (has_dict_output) {
    GetDictionary(dict, dict_len);
  }

  // Similar logic as ReadValues to get def levels and rep levels.
  int64_t num_def_levels = 0;
  int64_t indices_to_read = 0;
  ReadLevels(batch_size, def_levels, rep_levels, &num_def_levels, &indices_to_read);

  // Read dictionary indices.
  *indices_read = ReadDictionaryIndices(indices_to_read, indices);
  int64_t total_indices = std::max<int64_t>(num_def_levels, *indices_read);
  // Some callers use a batch size of 0 just to get the dictionary.
  int64_t expected_values =
      std::min(batch_size, this->num_buffered_values_ - this->num_decoded_values_);
  if (total_indices == 0 && expected_values > 0) {
    std::stringstream ss;
    ss << "Read 0 values, expected " << expected_values;
    ParquetException::EofException(ss.str());
  }
  this->ConsumeBufferedValues(total_indices);

  return total_indices;
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
  int64_t num_def_levels = 0;
  int64_t values_to_read = 0;
  ReadLevels(batch_size, def_levels, rep_levels, &num_def_levels, &values_to_read);

  *values_read = this->ReadValues(values_to_read, values);
  int64_t total_values = std::max<int64_t>(num_def_levels, *values_read);
  int64_t expected_values =
      std::min(batch_size, this->num_buffered_values_ - this->num_decoded_values_);
  if (total_values == 0 && expected_values > 0) {
    std::stringstream ss;
    ss << "Read 0 values, expected " << expected_values;
    ParquetException::EofException(ss.str());
  }
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

    const bool has_spaced_values = HasSpacedValues(this->descr_);
    int64_t null_count = 0;
    if (!has_spaced_values) {
      int values_to_read = 0;
      for (int64_t i = 0; i < num_def_levels; ++i) {
        if (def_levels[i] == this->max_def_level_) {
          ++values_to_read;
        }
      }
      total_values = this->ReadValues(values_to_read, values);
      ::arrow::bit_util::SetBitsTo(valid_bits, valid_bits_offset,
                                   /*length=*/total_values,
                                   /*bits_are_set=*/true);
      *values_read = total_values;
    } else {
      internal::LevelInfo info;
      info.repeated_ancestor_def_level = this->max_def_level_ - 1;
      info.def_level = this->max_def_level_;
      info.rep_level = this->max_rep_level_;
      internal::ValidityBitmapInputOutput validity_io;
      validity_io.values_read_upper_bound = num_def_levels;
      validity_io.valid_bits = valid_bits;
      validity_io.valid_bits_offset = valid_bits_offset;
      validity_io.null_count = null_count;
      validity_io.values_read = *values_read;

      internal::DefLevelsToBitmap(def_levels, num_def_levels, info, &validity_io);
      null_count = validity_io.null_count;
      *values_read = validity_io.values_read;

      total_values =
          this->ReadValuesSpaced(*values_read, values, static_cast<int>(null_count),
                                 valid_bits, valid_bits_offset);
    }
    *levels_read = num_def_levels;
    *null_count_out = null_count;

  } else {
    // Required field, read all values
    total_values = this->ReadValues(batch_size, values);
    ::arrow::bit_util::SetBitsTo(valid_bits, valid_bits_offset,
                                 /*length=*/total_values,
                                 /*bits_are_set=*/true);
    *null_count_out = 0;
    *values_read = total_values;
    *levels_read = total_values;
  }

  this->ConsumeBufferedValues(*levels_read);
  return total_values;
}

template <typename DType>
void TypedColumnReaderImpl<DType>::InitScratchForSkip() {
  if (this->scratch_for_skip_ == nullptr) {
    int value_size = type_traits<DType::type_num>::value_byte_size;
    this->scratch_for_skip_ = AllocateBuffer(
        this->pool_, kSkipScratchBatchSize * std::max<int>(sizeof(int16_t), value_size));
  }
}

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::Skip(int64_t num_values_to_skip) {
  int64_t values_to_skip = num_values_to_skip;
  // Optimization: Do not call HasNext() when values_to_skip == 0.
  while (values_to_skip > 0 && HasNext()) {
    // If the number of values to skip is more than the number of undecoded values, skip
    // the Page.
    const int64_t available_values = this->available_values_current_page();
    if (values_to_skip >= available_values) {
      values_to_skip -= available_values;
      this->ConsumeBufferedValues(available_values);
    } else {
      // We need to read this Page
      // Jump to the right offset in the Page
      int64_t values_read = 0;
      InitScratchForSkip();
      ARROW_DCHECK_NE(this->scratch_for_skip_, nullptr);
      do {
        int64_t batch_size = std::min(kSkipScratchBatchSize, values_to_skip);
        values_read = ReadBatch(
            static_cast<int>(batch_size),
            reinterpret_cast<int16_t*>(this->scratch_for_skip_->mutable_data()),
            reinterpret_cast<int16_t*>(this->scratch_for_skip_->mutable_data()),
            reinterpret_cast<T*>(this->scratch_for_skip_->mutable_data()), &values_read);
        values_to_skip -= values_read;
      } while (values_read > 0 && values_to_skip > 0);
    }
  }
  return num_values_to_skip - values_to_skip;
}

}  // namespace

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
  // Unreachable code, but suppress compiler warning
  return std::shared_ptr<ColumnReader>(nullptr);
}

// ----------------------------------------------------------------------
// RecordReader

namespace internal {

namespace {

template <typename DType>
class TypedRecordReader : public TypedColumnReaderImpl<DType>,
                          virtual public RecordReader {
 public:
  using T = typename DType::c_type;
  using BASE = TypedColumnReaderImpl<DType>;
  TypedRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info, MemoryPool* pool,
                    bool read_dense_for_nullable)
      // Pager must be set using SetPageReader.
      : BASE(descr, /* pager = */ nullptr, pool) {
    leaf_info_ = leaf_info;
    nullable_values_ = leaf_info_.HasNullableValues();
    at_record_start_ = true;
    values_written_ = 0;
    null_count_ = 0;
    values_capacity_ = 0;
    levels_written_ = 0;
    levels_position_ = 0;
    levels_capacity_ = 0;
    read_dense_for_nullable_ = read_dense_for_nullable;
    uses_values_ = !(descr->physical_type() == Type::BYTE_ARRAY);

    if (uses_values_) {
      values_ = AllocateBuffer(pool);
    }
    valid_bits_ = AllocateBuffer(pool);
    def_levels_ = AllocateBuffer(pool);
    rep_levels_ = AllocateBuffer(pool);
    TypedRecordReader::Reset();
  }

  // Compute the values capacity in bytes for the given number of elements
  int64_t bytes_for_values(int64_t nitems) const {
    int64_t type_size = GetTypeByteSize(this->descr_->physical_type());
    int64_t bytes_for_values = -1;
    if (MultiplyWithOverflow(nitems, type_size, &bytes_for_values)) {
      throw ParquetException("Total size of items too large");
    }
    return bytes_for_values;
  }

  int64_t ReadRecords(int64_t num_records) override {
    if (num_records == 0) return 0;
    // Delimit records, then read values at the end
    int64_t records_read = 0;

    if (has_values_to_process()) {
      records_read += ReadRecordData(num_records);
    }

    int64_t level_batch_size = std::max<int64_t>(kMinLevelBatchSize, num_records);

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
      int64_t batch_size =
          std::min(level_batch_size, this->available_values_current_page());

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

  // Throw away levels from start_levels_position to levels_position_.
  // Will update levels_position_, levels_written_, and levels_capacity_
  // accordingly and move the levels to left to fill in the gap.
  // It will resize the buffer without releasing the memory allocation.
  void ThrowAwayLevels(int64_t start_levels_position) {
    ARROW_DCHECK_LE(levels_position_, levels_written_);
    ARROW_DCHECK_LE(start_levels_position, levels_position_);
    ARROW_DCHECK_GT(this->max_def_level_, 0);
    ARROW_DCHECK_NE(def_levels_, nullptr);

    int64_t gap = levels_position_ - start_levels_position;
    if (gap == 0) return;

    int64_t levels_remaining = levels_written_ - gap;

    auto left_shift = [&](::arrow::ResizableBuffer* buffer) {
      int16_t* data = reinterpret_cast<int16_t*>(buffer->mutable_data());
      std::copy(data + levels_position_, data + levels_written_,
                data + start_levels_position);
      PARQUET_THROW_NOT_OK(buffer->Resize(levels_remaining * sizeof(int16_t),
                                          /*shrink_to_fit=*/false));
    };

    left_shift(def_levels_.get());

    if (this->max_rep_level_ > 0) {
      ARROW_DCHECK_NE(rep_levels_, nullptr);
      left_shift(rep_levels_.get());
    }

    levels_written_ -= gap;
    levels_position_ -= gap;
    levels_capacity_ -= gap;
  }

  // Skip records that we have in our buffer. This function is only for
  // non-repeated fields.
  int64_t SkipRecordsInBufferNonRepeated(int64_t num_records) {
    ARROW_DCHECK_EQ(this->max_rep_level_, 0);
    if (!this->has_values_to_process() || num_records == 0) return 0;

    int64_t remaining_records = levels_written_ - levels_position_;
    int64_t skipped_records = std::min(num_records, remaining_records);
    int64_t start_levels_position = levels_position_;
    // Since there is no repetition, number of levels equals number of records.
    levels_position_ += skipped_records;

    // We skipped the levels by incrementing 'levels_position_'. For values
    // we do not have a buffer, so we need to read them and throw them away.
    // First we need to figure out how many present/not-null values there are.
    std::shared_ptr<::arrow::ResizableBuffer> valid_bits;
    valid_bits = AllocateBuffer(this->pool_);
    PARQUET_THROW_NOT_OK(valid_bits->Resize(bit_util::BytesForBits(skipped_records),
                                            /*shrink_to_fit=*/true));
    ValidityBitmapInputOutput validity_io;
    validity_io.values_read_upper_bound = skipped_records;
    validity_io.valid_bits = valid_bits->mutable_data();
    validity_io.valid_bits_offset = 0;
    DefLevelsToBitmap(def_levels() + start_levels_position, skipped_records,
                      this->leaf_info_, &validity_io);
    int64_t values_to_read = validity_io.values_read - validity_io.null_count;

    // Now that we have figured out number of values to read, we do not need
    // these levels anymore. We will remove these values from the buffer.
    // This requires shifting the levels in the buffer to left. So this will
    // update levels_position_ and levels_written_.
    ThrowAwayLevels(start_levels_position);
    // For values, we do not have them in buffer, so we will read them and
    // throw them away.
    ReadAndThrowAwayValues(values_to_read);

    // Mark the levels as read in the underlying column reader.
    this->ConsumeBufferedValues(skipped_records);

    return skipped_records;
  }

  // Attempts to skip num_records from the buffer. Will throw away levels
  // and corresponding values for the records it skipped and consumes them from the
  // underlying decoder. Will advance levels_position_ and update
  // at_record_start_.
  // Returns how many records were skipped.
  int64_t DelimitAndSkipRecordsInBuffer(int64_t num_records) {
    if (num_records == 0) return 0;
    // Look at the buffered levels, delimit them based on
    // (rep_level == 0), report back how many records are in there, and
    // fill in how many not-null values (def_level == max_def_level_).
    // DelimitRecords updates levels_position_.
    int64_t start_levels_position = levels_position_;
    int64_t values_seen = 0;
    int64_t skipped_records = DelimitRecords(num_records, &values_seen);
    ReadAndThrowAwayValues(values_seen);
    // Mark those levels and values as consumed in the underlying page.
    // This must be done before we throw away levels since it updates
    // levels_position_ and levels_written_.
    this->ConsumeBufferedValues(levels_position_ - start_levels_position);
    // Updated levels_position_ and levels_written_.
    ThrowAwayLevels(start_levels_position);
    return skipped_records;
  }

  // Skip records for repeated fields. For repeated fields, we are technically
  // reading and throwing away the levels and values since we do not know the record
  // boundaries in advance. Keep filling the buffer and skipping until we reach the
  // desired number of records or we run out of values in the column chunk.
  // Returns number of skipped records.
  int64_t SkipRecordsRepeated(int64_t num_records) {
    ARROW_DCHECK_GT(this->max_rep_level_, 0);
    int64_t skipped_records = 0;

    // First consume what is in the buffer.
    if (levels_position_ < levels_written_) {
      // This updates at_record_start_.
      skipped_records = DelimitAndSkipRecordsInBuffer(num_records);
    }

    int64_t level_batch_size =
        std::max<int64_t>(kMinLevelBatchSize, num_records - skipped_records);

    // If 'at_record_start_' is false, but (skipped_records == num_records), it
    // means that for the last record that was counted, we have not seen all
    // of its values yet.
    while (!at_record_start_ || skipped_records < num_records) {
      // Is there more data to read in this row group?
      // HasNextInternal() will advance to the next page if necessary.
      if (!this->HasNextInternal()) {
        if (!at_record_start_) {
          // We ended the row group while inside a record that we haven't seen
          // the end of yet. So increment the record count for the last record
          // in the row group
          ++skipped_records;
          at_record_start_ = true;
        }
        break;
      }

      // Read some more levels.
      int64_t batch_size =
          std::min(level_batch_size, this->available_values_current_page());
      // No more data in column. This must be an empty page.
      // If we had exhausted the last page, HasNextInternal() must have advanced
      // to the next page. So there must be available values to process.
      if (batch_size == 0) {
        break;
      }

      // For skipping we will read the levels and append them to the end
      // of the def_levels and rep_levels just like for read.
      ReserveLevels(batch_size);

      int16_t* def_levels = this->def_levels() + levels_written_;
      int16_t* rep_levels = this->rep_levels() + levels_written_;

      int64_t levels_read = 0;
      levels_read = this->ReadDefinitionLevels(batch_size, def_levels);
      if (this->ReadRepetitionLevels(batch_size, rep_levels) != levels_read) {
        throw ParquetException("Number of decoded rep / def levels did not match");
      }

      levels_written_ += levels_read;
      int64_t remaining_records = num_records - skipped_records;
      // This updates at_record_start_.
      skipped_records += DelimitAndSkipRecordsInBuffer(remaining_records);
    }

    return skipped_records;
  }

  // Read 'num_values' values and throw them away.
  // Throws an error if it could not read 'num_values'.
  void ReadAndThrowAwayValues(int64_t num_values) {
    int64_t values_left = num_values;
    int64_t values_read = 0;

    // Allocate enough scratch space to accommodate 16-bit levels or any
    // value type
    this->InitScratchForSkip();
    ARROW_DCHECK_NE(this->scratch_for_skip_, nullptr);
    do {
      int64_t batch_size = std::min<int64_t>(kSkipScratchBatchSize, values_left);
      values_read = this->ReadValues(
          batch_size, reinterpret_cast<T*>(this->scratch_for_skip_->mutable_data()));
      values_left -= values_read;
    } while (values_read > 0 && values_left > 0);
    if (values_left > 0) {
      std::stringstream ss;
      ss << "Could not read and throw away " << num_values << " values";
      throw ParquetException(ss.str());
    }
  }

  int64_t SkipRecords(int64_t num_records) override {
    if (num_records == 0) return 0;

    // Top level required field. Number of records equals to number of levels,
    // and there is not read-ahead for levels.
    if (this->max_rep_level_ == 0 && this->max_def_level_ == 0) {
      return this->Skip(num_records);
    }
    int64_t skipped_records = 0;
    if (this->max_rep_level_ == 0) {
      // Non-repeated optional field.
      // First consume whatever is in the buffer.
      skipped_records = SkipRecordsInBufferNonRepeated(num_records);

      ARROW_DCHECK_LE(skipped_records, num_records);

      // For records that we have not buffered, we will use the column
      // reader's Skip to do the remaining Skip. Since the field is not
      // repeated number of levels to skip is the same as number of records
      // to skip.
      skipped_records += this->Skip(num_records - skipped_records);
    } else {
      skipped_records += this->SkipRecordsRepeated(num_records);
    }
    return skipped_records;
  }

  // We may outwardly have the appearance of having exhausted a column chunk
  // when in fact we are in the middle of processing the last batch
  bool has_values_to_process() const { return levels_position_ < levels_written_; }

  std::shared_ptr<ResizableBuffer> ReleaseValues() override {
    if (uses_values_) {
      auto result = values_;
      PARQUET_THROW_NOT_OK(
          result->Resize(bytes_for_values(values_written_), /*shrink_to_fit=*/true));
      values_ = AllocateBuffer(this->pool_);
      values_capacity_ = 0;
      return result;
    } else {
      return nullptr;
    }
  }

  std::shared_ptr<ResizableBuffer> ReleaseIsValid() override {
    if (nullable_values()) {
      auto result = valid_bits_;
      PARQUET_THROW_NOT_OK(result->Resize(bit_util::BytesForBits(values_written_),
                                          /*shrink_to_fit=*/true));
      valid_bits_ = AllocateBuffer(this->pool_);
      return result;
    } else {
      return nullptr;
    }
  }

  // Process written repetition/definition levels to reach the end of
  // records. Only used for repeated fields.
  // Process no more levels than necessary to delimit the indicated
  // number of logical records. Updates internal state of RecordReader
  //
  // \return Number of records delimited
  int64_t DelimitRecords(int64_t num_records, int64_t* values_seen) {
    int64_t values_to_read = 0;
    int64_t records_read = 0;

    const int16_t* def_levels = this->def_levels() + levels_position_;
    const int16_t* rep_levels = this->rep_levels() + levels_position_;

    ARROW_DCHECK_GT(this->max_rep_level_, 0);

    // Count logical records and number of values to read
    while (levels_position_ < levels_written_) {
      const int16_t rep_level = *rep_levels++;
      if (rep_level == 0) {
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

      const int16_t def_level = *def_levels++;
      if (def_level == this->max_def_level_) {
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

  int64_t UpdateCapacity(int64_t capacity, int64_t size, int64_t extra_size) {
    if (extra_size < 0) {
      throw ParquetException("Negative size (corrupt file?)");
    }
    int64_t target_size = -1;
    if (AddWithOverflow(size, extra_size, &target_size)) {
      throw ParquetException("Allocation size too large (corrupt file?)");
    }
    if (target_size >= (1LL << 62)) {
      throw ParquetException("Allocation size too large (corrupt file?)");
    }
    if (capacity >= target_size) {
      return capacity;
    }
    return bit_util::NextPower2(target_size);
  }

  void ReserveLevels(int64_t extra_levels) {
    if (this->max_def_level_ > 0) {
      const int64_t new_levels_capacity =
          UpdateCapacity(levels_capacity_, levels_written_, extra_levels);
      if (new_levels_capacity > levels_capacity_) {
        constexpr auto kItemSize = static_cast<int64_t>(sizeof(int16_t));
        int64_t capacity_in_bytes = -1;
        if (MultiplyWithOverflow(new_levels_capacity, kItemSize, &capacity_in_bytes)) {
          throw ParquetException("Allocation size too large (corrupt file?)");
        }
        PARQUET_THROW_NOT_OK(
            def_levels_->Resize(capacity_in_bytes, /*shrink_to_fit=*/false));
        if (this->max_rep_level_ > 0) {
          PARQUET_THROW_NOT_OK(
              rep_levels_->Resize(capacity_in_bytes, /*shrink_to_fit=*/false));
        }
        levels_capacity_ = new_levels_capacity;
      }
    }
  }

  void ReserveValues(int64_t extra_values) {
    const int64_t new_values_capacity =
        UpdateCapacity(values_capacity_, values_written_, extra_values);
    if (new_values_capacity > values_capacity_) {
      // XXX(wesm): A hack to avoid memory allocation when reading directly
      // into builder classes
      if (uses_values_) {
        PARQUET_THROW_NOT_OK(values_->Resize(bytes_for_values(new_values_capacity),
                                             /*shrink_to_fit=*/false));
      }
      values_capacity_ = new_values_capacity;
    }
    if (nullable_values() && !read_dense_for_nullable_) {
      int64_t valid_bytes_new = bit_util::BytesForBits(values_capacity_);
      if (valid_bits_->size() < valid_bytes_new) {
        int64_t valid_bytes_old = bit_util::BytesForBits(values_written_);
        PARQUET_THROW_NOT_OK(
            valid_bits_->Resize(valid_bytes_new, /*shrink_to_fit=*/false));

        // Avoid valgrind warnings
        memset(valid_bits_->mutable_data() + valid_bytes_old, 0,
               valid_bytes_new - valid_bytes_old);
      }
    }
  }

  void Reset() override {
    ResetValues();

    if (levels_written_ > 0) {
      // Throw away levels from 0 to levels_position_.
      ThrowAwayLevels(0);
    }

    // Call Finish on the binary builders to reset them
  }

  void SetPageReader(std::unique_ptr<PageReader> reader) override {
    at_record_start_ = true;
    this->pager_ = std::move(reader);
    ResetDecoders();
  }

  bool HasMoreData() const override { return this->pager_ != nullptr; }

  const ColumnDescriptor* descr() const override { return this->descr_; }

  // Dictionary decoders must be reset when advancing row groups
  void ResetDecoders() { this->decoders_.clear(); }

  virtual void ReadValuesSpaced(int64_t values_with_nulls, int64_t null_count) {
    uint8_t* valid_bits = valid_bits_->mutable_data();
    const int64_t valid_bits_offset = values_written_;

    int64_t num_decoded = this->current_decoder_->DecodeSpaced(
        ValuesHead<T>(), static_cast<int>(values_with_nulls),
        static_cast<int>(null_count), valid_bits, valid_bits_offset);
    CheckNumberDecoded(num_decoded, values_with_nulls);
  }

  virtual void ReadValuesDense(int64_t values_to_read) {
    int64_t num_decoded =
        this->current_decoder_->Decode(ValuesHead<T>(), static_cast<int>(values_to_read));
    CheckNumberDecoded(num_decoded, values_to_read);
  }

  // Reads repeated records and returns number of records read. Fills in
  // values_to_read and null_count.
  int64_t ReadRepeatedRecords(int64_t num_records, int64_t* values_to_read,
                              int64_t* null_count) {
    const int64_t start_levels_position = levels_position_;
    // Note that repeated records may be required or nullable. If they have
    // an optional parent in the path, they will be nullable, otherwise,
    // they are required. We use leaf_info_->HasNullableValues() that looks
    // at repeated_ancestor_def_level to determine if it is required or
    // nullable. Even if they are required, we may have to read ahead and
    // delimit the records to get the right number of values and they will
    // have associated levels.
    int64_t records_read = DelimitRecords(num_records, values_to_read);
    if (!nullable_values() || read_dense_for_nullable_) {
      ReadValuesDense(*values_to_read);
      // null_count is always 0 for required.
      ARROW_DCHECK_EQ(*null_count, 0);
    } else {
      ReadSpacedForOptionalOrRepeated(start_levels_position, values_to_read, null_count);
    }
    return records_read;
  }

  // Reads optional records and returns number of records read. Fills in
  // values_to_read and null_count.
  int64_t ReadOptionalRecords(int64_t num_records, int64_t* values_to_read,
                              int64_t* null_count) {
    const int64_t start_levels_position = levels_position_;
    // No repetition levels, skip delimiting logic. Each level represents a
    // null or not null entry
    int64_t records_read =
        std::min<int64_t>(levels_written_ - levels_position_, num_records);
    // This is advanced by DelimitRecords for the repeated field case above.
    levels_position_ += records_read;

    // Optional fields are always nullable.
    if (read_dense_for_nullable_) {
      ReadDenseForOptional(start_levels_position, values_to_read);
      // We don't need to update null_count when reading dense. It should be
      // already set to 0.
      ARROW_DCHECK_EQ(*null_count, 0);
    } else {
      ReadSpacedForOptionalOrRepeated(start_levels_position, values_to_read, null_count);
    }
    return records_read;
  }

  // Reads required records and returns number of records read. Fills in
  // values_to_read.
  int64_t ReadRequiredRecords(int64_t num_records, int64_t* values_to_read) {
    *values_to_read = num_records;
    ReadValuesDense(*values_to_read);
    return num_records;
  }

  // Reads dense for optional records. First it figures out how many values to
  // read.
  void ReadDenseForOptional(int64_t start_levels_position, int64_t* values_to_read) {
    // levels_position_ must already be incremented based on number of records
    // read.
    ARROW_DCHECK_GE(levels_position_, start_levels_position);

    // When reading dense we need to figure out number of values to read.
    const int16_t* def_levels = this->def_levels();
    for (int64_t i = start_levels_position; i < levels_position_; ++i) {
      if (def_levels[i] == this->max_def_level_) {
        ++(*values_to_read);
      }
    }
    ReadValuesDense(*values_to_read);
  }

  // Reads spaced for optional or repeated fields.
  void ReadSpacedForOptionalOrRepeated(int64_t start_levels_position,
                                       int64_t* values_to_read, int64_t* null_count) {
    // levels_position_ must already be incremented based on number of records
    // read.
    ARROW_DCHECK_GE(levels_position_, start_levels_position);
    ValidityBitmapInputOutput validity_io;
    validity_io.values_read_upper_bound = levels_position_ - start_levels_position;
    validity_io.valid_bits = valid_bits_->mutable_data();
    validity_io.valid_bits_offset = values_written_;

    DefLevelsToBitmap(def_levels() + start_levels_position,
                      levels_position_ - start_levels_position, leaf_info_, &validity_io);
    *values_to_read = validity_io.values_read - validity_io.null_count;
    *null_count = validity_io.null_count;
    ARROW_DCHECK_GE(*values_to_read, 0);
    ARROW_DCHECK_GE(*null_count, 0);
    ReadValuesSpaced(validity_io.values_read, *null_count);
  }

  // Return number of logical records read.
  // Updates levels_position_, values_written_, and null_count_.
  int64_t ReadRecordData(int64_t num_records) {
    // Conservative upper bound
    const int64_t possible_num_values =
        std::max<int64_t>(num_records, levels_written_ - levels_position_);
    ReserveValues(possible_num_values);

    const int64_t start_levels_position = levels_position_;

    // To be updated by the function calls below for each of the repetition
    // types.
    int64_t records_read = 0;
    int64_t values_to_read = 0;
    int64_t null_count = 0;
    if (this->max_rep_level_ > 0) {
      // Repeated fields may be nullable or not.
      // This call updates levels_position_.
      records_read = ReadRepeatedRecords(num_records, &values_to_read, &null_count);
    } else if (this->max_def_level_ > 0) {
      // Non-repeated optional values are always nullable.
      // This call updates levels_position_.
      ARROW_DCHECK(nullable_values());
      records_read = ReadOptionalRecords(num_records, &values_to_read, &null_count);
    } else {
      ARROW_DCHECK(!nullable_values());
      records_read = ReadRequiredRecords(num_records, &values_to_read);
      // We don't need to update null_count, since it is 0.
    }

    ARROW_DCHECK_GE(records_read, 0);
    ARROW_DCHECK_GE(values_to_read, 0);
    ARROW_DCHECK_GE(null_count, 0);

    if (read_dense_for_nullable_) {
      values_written_ += values_to_read;
      ARROW_DCHECK_EQ(null_count, 0);
    } else {
      values_written_ += values_to_read + null_count;
      null_count_ += null_count;
    }
    // Total values, including null spaces, if any
    if (this->max_def_level_ > 0) {
      // Optional, repeated, or some mix thereof
      this->ConsumeBufferedValues(levels_position_ - start_levels_position);
    } else {
      // Flat, non-repeated
      this->ConsumeBufferedValues(values_to_read);
    }

    return records_read;
  }

  void DebugPrintState() override {
    const int16_t* def_levels = this->def_levels();
    const int16_t* rep_levels = this->rep_levels();
    const int64_t total_levels_read = levels_position_;

    const T* vals = reinterpret_cast<const T*>(this->values());

    if (leaf_info_.def_level > 0) {
      std::cout << "def levels: ";
      for (int64_t i = 0; i < total_levels_read; ++i) {
        std::cout << def_levels[i] << " ";
      }
      std::cout << std::endl;
    }

    if (leaf_info_.rep_level > 0) {
      std::cout << "rep levels: ";
      for (int64_t i = 0; i < total_levels_read; ++i) {
        std::cout << rep_levels[i] << " ";
      }
      std::cout << std::endl;
    }

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
        PARQUET_THROW_NOT_OK(values_->Resize(0, /*shrink_to_fit=*/false));
      }
      PARQUET_THROW_NOT_OK(valid_bits_->Resize(0, /*shrink_to_fit=*/false));
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
  LevelInfo leaf_info_;
};

class FLBARecordReader : public TypedRecordReader<FLBAType>,
                         virtual public BinaryRecordReader {
 public:
  FLBARecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                   ::arrow::MemoryPool* pool, bool read_dense_for_nullable)
      : TypedRecordReader<FLBAType>(descr, leaf_info, pool, read_dense_for_nullable),
        builder_(nullptr) {
    ARROW_DCHECK_EQ(descr_->physical_type(), Type::FIXED_LEN_BYTE_ARRAY);
    int byte_width = descr_->type_length();
    std::shared_ptr<::arrow::DataType> type = ::arrow::fixed_size_binary(byte_width);
    builder_ = std::make_unique<::arrow::FixedSizeBinaryBuilder>(type, this->pool_);
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
    CheckNumberDecoded(num_decoded, values_to_read);

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
    ARROW_DCHECK_EQ(num_decoded, values_to_read);

    for (int64_t i = 0; i < num_decoded; i++) {
      if (::arrow::bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
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
  ByteArrayChunkedRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                               ::arrow::MemoryPool* pool, bool read_dense_for_nullable)
      : TypedRecordReader<ByteArrayType>(descr, leaf_info, pool,
                                         read_dense_for_nullable) {
    ARROW_DCHECK_EQ(descr_->physical_type(), Type::BYTE_ARRAY);
    accumulator_.builder = std::make_unique<::arrow::BinaryBuilder>(pool);
  }

  ::arrow::ArrayVector GetBuilderChunks() override {
    ::arrow::ArrayVector result = accumulator_.chunks;
    if (result.size() == 0 || accumulator_.builder->length() > 0) {
      std::shared_ptr<::arrow::Array> last_chunk;
      PARQUET_THROW_NOT_OK(accumulator_.builder->Finish(&last_chunk));
      result.push_back(std::move(last_chunk));
    }
    accumulator_.chunks = {};
    return result;
  }

  void ReadValuesDense(int64_t values_to_read) override {
    int64_t num_decoded = this->current_decoder_->DecodeArrowNonNull(
        static_cast<int>(values_to_read), &accumulator_);
    CheckNumberDecoded(num_decoded, values_to_read);
    ResetValues();
  }

  void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
    int64_t num_decoded = this->current_decoder_->DecodeArrow(
        static_cast<int>(values_to_read), static_cast<int>(null_count),
        valid_bits_->mutable_data(), values_written_, &accumulator_);
    CheckNumberDecoded(num_decoded, values_to_read - null_count);
    ResetValues();
  }

 private:
  // Helper data structure for accumulating builder chunks
  typename EncodingTraits<ByteArrayType>::Accumulator accumulator_;
};

class ByteArrayDictionaryRecordReader : public TypedRecordReader<ByteArrayType>,
                                        virtual public DictionaryRecordReader {
 public:
  ByteArrayDictionaryRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                                  ::arrow::MemoryPool* pool, bool read_dense_for_nullable)
      : TypedRecordReader<ByteArrayType>(descr, leaf_info, pool, read_dense_for_nullable),
        builder_(pool) {
    this->read_dictionary_ = true;
  }

  std::shared_ptr<::arrow::ChunkedArray> GetResult() override {
    FlushBuilder();
    std::vector<std::shared_ptr<::arrow::Array>> result;
    std::swap(result, result_chunks_);
    return std::make_shared<::arrow::ChunkedArray>(std::move(result), builder_.type());
  }

  void FlushBuilder() {
    if (builder_.length() > 0) {
      std::shared_ptr<::arrow::Array> chunk;
      PARQUET_THROW_NOT_OK(builder_.Finish(&chunk));
      result_chunks_.emplace_back(std::move(chunk));

      // Also clears the dictionary memo table
      builder_.Reset();
    }
  }

  void MaybeWriteNewDictionary() {
    if (this->new_dictionary_) {
      /// If there is a new dictionary, we may need to flush the builder, then
      /// insert the new dictionary values
      FlushBuilder();
      builder_.ResetFull();
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
    CheckNumberDecoded(num_decoded, values_to_read);
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
    ARROW_DCHECK_EQ(num_decoded, values_to_read - null_count);
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
                                                        LevelInfo leaf_info,
                                                        ::arrow::MemoryPool* pool,
                                                        bool read_dictionary,
                                                        bool read_dense_for_nullable) {
  if (read_dictionary) {
    return std::make_shared<ByteArrayDictionaryRecordReader>(descr, leaf_info, pool,
                                                             read_dense_for_nullable);
  } else {
    return std::make_shared<ByteArrayChunkedRecordReader>(descr, leaf_info, pool,
                                                          read_dense_for_nullable);
  }
}

}  // namespace

std::shared_ptr<RecordReader> RecordReader::Make(const ColumnDescriptor* descr,
                                                 LevelInfo leaf_info, MemoryPool* pool,
                                                 bool read_dictionary,
                                                 bool read_dense_for_nullable) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::make_shared<TypedRecordReader<BooleanType>>(descr, leaf_info, pool,
                                                              read_dense_for_nullable);
    case Type::INT32:
      return std::make_shared<TypedRecordReader<Int32Type>>(descr, leaf_info, pool,
                                                            read_dense_for_nullable);
    case Type::INT64:
      return std::make_shared<TypedRecordReader<Int64Type>>(descr, leaf_info, pool,
                                                            read_dense_for_nullable);
    case Type::INT96:
      return std::make_shared<TypedRecordReader<Int96Type>>(descr, leaf_info, pool,
                                                            read_dense_for_nullable);
    case Type::FLOAT:
      return std::make_shared<TypedRecordReader<FloatType>>(descr, leaf_info, pool,
                                                            read_dense_for_nullable);
    case Type::DOUBLE:
      return std::make_shared<TypedRecordReader<DoubleType>>(descr, leaf_info, pool,
                                                             read_dense_for_nullable);
    case Type::BYTE_ARRAY: {
      return MakeByteArrayRecordReader(descr, leaf_info, pool, read_dictionary,
                                       read_dense_for_nullable);
    }
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FLBARecordReader>(descr, leaf_info, pool,
                                                read_dense_for_nullable);
    default: {
      // PARQUET-1481: This can occur if the file is corrupt
      std::stringstream ss;
      ss << "Invalid physical column type: " << static_cast<int>(descr->physical_type());
      throw ParquetException(ss.str());
    }
  }
  // Unreachable code, but suppress compiler warning
  return nullptr;
}

}  // namespace internal
}  // namespace parquet

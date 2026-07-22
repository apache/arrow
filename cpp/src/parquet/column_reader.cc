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
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/chunked_array.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/crc32.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_bitmap_internal.h"
#include "arrow/util/rle_encoding_internal.h"
#include "arrow/util/unreachable.h"
#include "parquet/column_page.h"
#include "parquet/encoding.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/exception.h"
#include "parquet/level_comparison.h"
#include "parquet/level_conversion.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"
#include "parquet/thrift_internal.h"  // IWYU pragma: keep
#include "parquet/windows_fixup.h"    // for OPTIONAL

#ifdef _MSC_VER
// disable warning about inheritance via dominance in the diamond pattern
#  pragma warning(disable : 4250)
#endif

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

// Throws exception if number_decoded does not match expected.
inline void CheckNumberDecoded(int64_t number_decoded, int64_t expected) {
  if (ARROW_PREDICT_FALSE(number_decoded != expected)) {
    auto msg = std::format("Decoded values {} does not match expected {}", number_decoded,
                           expected);
    ParquetException::EofException(msg);
  }
}

constexpr std::string_view kErrorRepDefLevelNotMatchesNumValues =
    "Number of decoded rep / def levels do not match num_values in page header";

template <typename T, typename U>
constexpr T clamp_to(U val) {
  constexpr U kMax = std::numeric_limits<T>::max();
  constexpr U kMin = std::numeric_limits<T>::min();
  return static_cast<T>(std::clamp<U>(val, kMin, kMax));
}

}  // namespace

/******************
 *  LevelDecoder  *
 ******************/

struct LevelDecoder::Impl {
  using RleBitPackedDecoder = ::arrow::util::RleBitPackedDecoder<int16_t>;
  using BitPackedDecoder = ::arrow::util::BitPackedDecoder<int16_t>;

  std::variant<RleBitPackedDecoder, BitPackedDecoder> decoder = {};

  [[nodiscard]] int32_t GetBatch(int16_t* out, int32_t batch_size) {
    return std::visit([&](auto& dec) { return dec.GetBatch(out, batch_size); }, decoder);
  }

  [[nodiscard]] int32_t Advance(int32_t batch_size) {
    return std::visit([&](auto& dec) { return dec.Advance(batch_size); }, decoder);
  }

  auto CountUpTo(int16_t value, int32_t batch_size) {
    return std::visit([&](auto& dec) { return dec.CountUpTo(value, batch_size); },
                      decoder);
  }
};

LevelDecoder::LevelDecoder(int16_t max_level)
    : impl_(std::make_unique<Impl>()), max_level_(max_level) {}

LevelDecoder::~LevelDecoder() = default;

int32_t LevelDecoder::SetData(Encoding::type encoding, int16_t max_level,
                              int32_t num_buffered_values, const uint8_t* data,
                              int32_t data_size) {
  max_level_ = max_level;
  num_values_remaining_ = num_buffered_values;
  const int32_t value_bit_width = bit_util::Log2(max_level + 1);

  switch (encoding) {
    case Encoding::RLE: {
      if (data_size < 4) {
        throw ParquetException("Received invalid levels (corrupt data page?)");
      }
      const auto num_bytes = ::arrow::util::SafeLoadAs<int32_t>(data);
      if (num_bytes < 0 || num_bytes > data_size - 4) {
        throw ParquetException("Received invalid number of bytes (corrupt data page?)");
      }
      this->impl_->decoder = Impl::RleBitPackedDecoder(  //
          /* data= */ data + 4,
          /* data_size =*/num_bytes,
          /* value_bit_width= */ value_bit_width);
      return 4 + num_bytes;
    }
    case Encoding::BIT_PACKED: {
      int32_t num_bits = 0;
      if (MultiplyWithOverflow(num_buffered_values, value_bit_width, &num_bits)) {
        throw ParquetException(
            "Number of buffered values too large (corrupt data page?)");
      }
      const auto num_bytes = static_cast<int32_t>(bit_util::BytesForBits(num_bits));
      if (num_bytes < 0 || num_bytes > data_size) {
        throw ParquetException("Received invalid number of bytes (corrupt data page?)");
      }
      // Also adding `value_count` so that the decoder also works with zero-width runs.
      this->impl_->decoder = Impl::BitPackedDecoder(  //
          /* data= */ data,
          /* data_size =*/num_bytes,
          /* value_bit_width= */ value_bit_width,
          /* value_count= */ num_buffered_values);
      return num_bytes;
    }
    default:
      throw ParquetException("Unknown encoding type for levels.");
  }
  return -1;
}

void LevelDecoder::SetDataV2(int32_t num_bytes, int16_t max_level,
                             int32_t num_buffered_values, const uint8_t* data) {
  max_level_ = max_level;
  // Repetition and definition levels always uses RLE encoding
  // in the DataPageV2 format.
  if (num_bytes < 0) {
    throw ParquetException("Invalid page header (corrupt data page?)");
  }
  num_values_remaining_ = num_buffered_values;

  this->impl_->decoder = Impl::RleBitPackedDecoder(  //
      /* data= */ data,
      /* data_size =*/num_bytes,
      /* value_bit_width= */ bit_util::Log2(max_level + 1));
}

int32_t LevelDecoder::Decode(int32_t batch_size, int16_t* levels) {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const int32_t num_decoded = impl_->GetBatch(levels, num_values);
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

int32_t LevelDecoder::Skip(int32_t batch_size) {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const int32_t num_advanced = impl_->Advance(num_values);
  ARROW_DCHECK_EQ(num_values, num_advanced);
  num_values_remaining_ -= num_advanced;
  return num_advanced;
}

auto LevelDecoder::CountUpTo(int16_t value, int32_t batch_size) -> CountUpToResult {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const auto result = impl_->CountUpTo(value, num_values);
  ARROW_DCHECK_EQ(num_values, result.processed_count);
  num_values_remaining_ -= result.processed_count;
  return {
      .matching_count = result.matching_count,
      .processed_count = result.processed_count,
  };
}

/**************************
 *  LevelToBitmapDecoder  *
 **************************/

/// Decoder for definition levels that writes directly into a validity bitmap.
///
/// This is the bitmap counterpart of ``LevelDecoder``, specialized for levels
/// encoded on a single bit (a max level of 1), such as the definition levels of a
/// flat, nullable column. Rather than decoding into an ``int16_t`` array and
/// re-encoding into an Arrow validity bitmap, it decodes straight into the bitmap.
///
/// @see LevelDecoder
class LevelToBitmapDecoder {
 public:
  using BitmapSpanMut = ::arrow::util::BitmapSpanMut;
  using RleBitPackedDecoder = ::arrow::util::RleBitPackedToBitmapDecoder;
  using BitPackedDecoder = ::arrow::util::BitPackedToBitmapDecoder;
  using CountUpToResult = LevelDecoder::CountUpToResult;

  // TODO we should factor this with the LevelDecoder

  LevelToBitmapDecoder() = default;

  /// Initialize the decoder state with new data from a legacy (V1) page.
  ///
  /// @return the number of bytes consumed
  int32_t SetData(Encoding::type encoding, int16_t max_level, int32_t num_buffered_values,
                  const uint8_t* data, int32_t data_size);

  /// Initialize the decoder state with new data from a V2 page.
  ///
  /// Repetition and definition levels in V2 pages are always RLE encoded.
  void SetDataV2(int32_t num_bytes, int16_t max_level, int32_t num_buffered_values,
                 const uint8_t* data);

  /// Decode a batch of levels into `out` and return the number of levels decoded.
  int32_t Decode(int32_t batch_size, BitmapSpanMut out);

  /// Advance the decoder and throw away decoded levels.
  int32_t Skip(int32_t batch_size);

  /// Advance and count the number of occurrences of `value`.
  ///
  /// The count is limited to at most the next `batch_size` items.
  /// @return The matching value count and number of elements that were processed.
  CountUpToResult CountUpTo(bool value, int32_t batch_size);

  /// Return the max level used in this decoder.
  int32_t max_level() const { return max_level_; }

  /// Return the number of values left to be decoded.
  int32_t remaining() const { return num_values_remaining_; }

 private:
  static void CheckMaxLevel(int16_t max_level);

  std::variant<RleBitPackedDecoder, BitPackedDecoder> decoder_ = {};
  /// Number of values remaining. The underlying decoder zero pads bit packed values
  /// up to a multiple of 8 so it cannot know the exact number of remaining values.
  int32_t num_values_remaining_ = 0;
  int16_t max_level_ = 0;
};

void LevelToBitmapDecoder::CheckMaxLevel(int16_t max_level) {
  if (ARROW_PREDICT_FALSE(max_level != 1)) {
    throw ParquetException(
        "LevelToBitmapDecoder only supports levels with a max level of 1.");
  }
}

int32_t LevelToBitmapDecoder::SetData(Encoding::type encoding, int16_t max_level,
                                      int32_t num_buffered_values, const uint8_t* data,
                                      int32_t data_size) {
  CheckMaxLevel(max_level);
  max_level_ = max_level;
  num_values_remaining_ = num_buffered_values;
  // Levels with a max level of 1 are encoded on a single bit.
  constexpr int32_t value_bit_width = 1;

  switch (encoding) {
    case Encoding::RLE: {
      if (data_size < 4) {
        throw ParquetException("Received invalid levels (corrupt data page?)");
      }
      const auto num_bytes = ::arrow::util::SafeLoadAs<int32_t>(data);
      if (num_bytes < 0 || num_bytes > data_size - 4) {
        throw ParquetException("Received invalid number of bytes (corrupt data page?)");
      }
      decoder_ = RleBitPackedDecoder(
          /* data= */ data + 4,
          /* data_size= */ num_bytes);
      return 4 + num_bytes;
    }
    case Encoding::BIT_PACKED: {
      int32_t num_bits = 0;
      if (MultiplyWithOverflow(num_buffered_values, value_bit_width, &num_bits)) {
        throw ParquetException(
            "Number of buffered values too large (corrupt data page?)");
      }
      const auto num_bytes = static_cast<int32_t>(bit_util::BytesForBits(num_bits));
      if (num_bytes < 0 || num_bytes > data_size) {
        throw ParquetException("Received invalid number of bytes (corrupt data page?)");
      }
      // Also passing `value_count` so that the decoder works with zero-width runs.
      decoder_ = BitPackedDecoder(
          /* data= */ data,
          /* data_size= */ num_bytes,
          /* value_count= */ num_buffered_values);
      return num_bytes;
    }
    default:
      throw ParquetException("Unknown encoding type for levels.");
  }
  return -1;
}

void LevelToBitmapDecoder::SetDataV2(int32_t num_bytes, int16_t max_level,
                                     int32_t num_buffered_values, const uint8_t* data) {
  CheckMaxLevel(max_level);
  if (num_bytes < 0) {
    throw ParquetException("Invalid page header (corrupt data page?)");
  }
  max_level_ = max_level;
  num_values_remaining_ = num_buffered_values;
  decoder_ = RleBitPackedDecoder(
      /* data= */ data,
      /* data_size= */ num_bytes);
}

int32_t LevelToBitmapDecoder::Decode(int32_t batch_size, BitmapSpanMut out) {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const int32_t num_decoded =
      std::visit([&](auto& dec) { return dec.GetBatch(out, num_values); }, decoder_);
  num_values_remaining_ -= num_decoded;
  return num_decoded;
}

int32_t LevelToBitmapDecoder::Skip(int32_t batch_size) {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const int32_t num_advanced =
      std::visit([&](auto& dec) { return dec.Advance(num_values); }, decoder_);
  ARROW_DCHECK_EQ(num_values, num_advanced);
  num_values_remaining_ -= num_advanced;
  return num_advanced;
}

auto LevelToBitmapDecoder::CountUpTo(bool value, int32_t batch_size) -> CountUpToResult {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const auto result =
      std::visit([&](auto& dec) { return dec.CountUpTo(value, num_values); }, decoder_);
  ARROW_DCHECK_EQ(num_values, result.processed_count);
  num_values_remaining_ -= result.processed_count;
  return {
      .matching_count = result.matching_count,
      .processed_count = result.processed_count,
  };
}

ReaderProperties default_reader_properties() {
  static ReaderProperties default_reader_properties;
  return default_reader_properties;
}

namespace {

// Extracts encoded statistics from V1 and V2 data page headers
template <typename H>
EncodedStatistics ExtractStatsFromHeader(const H& header, StatisticsMinMaxField min_max) {
  EncodedStatistics page_statistics;
  if (header.__isset.statistics) {
    page_statistics = FromThrift(header.statistics, min_max);
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
                       const CryptoContext* crypto_ctx, bool always_compressed,
                       StatisticsMinMaxField stats_min_max_field)
      : properties_(properties),
        stream_(std::move(stream)),
        decompression_buffer_(AllocateBuffer(properties_.memory_pool(), 0)),
        page_ordinal_(0),
        seen_num_values_(0),
        total_num_values_(total_num_values),
        stats_min_max_field_(stats_min_max_field) {
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
  // decompression buffer internally, so if NextPage() is
  // called then the content of previous page might be invalidated.
  std::shared_ptr<Page> NextPage() override;

  void set_max_page_header_size(uint32_t size) override { max_page_header_size_ = size; }

 private:
  void UpdateDecryption(Decryptor* decryptor, int8_t module_type, std::string* page_aad);

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

  // The CryptoContext used by this PageReader.
  CryptoContext crypto_ctx_;
  // This PageReader has its own Decryptor instances in order to be thread-safe.
  std::unique_ptr<Decryptor> meta_decryptor_;
  std::unique_ptr<Decryptor> data_decryptor_;

  // The ordinal fields in the context below are used for AAD suffix calculation.
  int32_t page_ordinal_;  // page ordinal does not count the dictionary page

  // Maximum allowed page size
  uint32_t max_page_header_size_;

  // Number of values read in data pages so far
  int64_t seen_num_values_;

  // Number of values in all the data pages
  int64_t total_num_values_;

  StatisticsMinMaxField stats_min_max_field_;

  // data_page_aad_ and data_page_header_aad_ contain the AAD for data page and data page
  // header in a single column respectively.
  // While calculating AAD for different pages in a single column the pages AAD is
  // updated by only the page ordinal.
  std::string data_page_aad_;
  std::string data_page_header_aad_;
};

void SerializedPageReader::InitDecryption() {
  // Prepare the AAD for quick update later.
  if (crypto_ctx_.data_decryptor_factory) {
    data_decryptor_ = crypto_ctx_.data_decryptor_factory();
    if (data_decryptor_) {
      ARROW_DCHECK(!data_decryptor_->file_aad().empty());
      data_page_aad_ = encryption::CreateModuleAad(
          data_decryptor_->file_aad(), encryption::kDataPage,
          crypto_ctx_.row_group_ordinal, crypto_ctx_.column_ordinal, kNonPageOrdinal);
    }
  }
  if (crypto_ctx_.meta_decryptor_factory) {
    meta_decryptor_ = crypto_ctx_.meta_decryptor_factory();
    if (meta_decryptor_) {
      ARROW_DCHECK(!meta_decryptor_->file_aad().empty());
      data_page_header_aad_ = encryption::CreateModuleAad(
          meta_decryptor_->file_aad(), encryption::kDataPageHeader,
          crypto_ctx_.row_group_ordinal, crypto_ctx_.column_ordinal, kNonPageOrdinal);
    }
  }
}

void SerializedPageReader::UpdateDecryption(Decryptor* decryptor, int8_t module_type,
                                            std::string* page_aad) {
  ARROW_DCHECK(decryptor != nullptr);
  if (crypto_ctx_.start_decrypt_with_dictionary_page) {
    UpdateDecryptor(decryptor, crypto_ctx_.row_group_ordinal, crypto_ctx_.column_ordinal,
                    module_type);
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
    *data_page_statistics = ExtractStatsFromHeader(header, stats_min_max_field_);
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
    *data_page_statistics = ExtractStatsFromHeader(header, stats_min_max_field_);
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
      if (view.size() == 0) return nullptr;

      // This gets used, then set by DeserializeThriftMsg
      header_size = static_cast<uint32_t>(view.size());
      try {
        if (meta_decryptor_ != nullptr) {
          UpdateDecryption(meta_decryptor_.get(), encryption::kDictionaryPageHeader,
                           &data_page_header_aad_);
        }
        // Reset current page header to avoid unclearing the __isset flag.
        current_page_header_ = format::PageHeader();
        deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(view.data()),
                                        &header_size, &current_page_header_,
                                        meta_decryptor_.get());
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

    int32_t compressed_len = current_page_header_.compressed_page_size;
    int32_t uncompressed_len = current_page_header_.uncompressed_page_size;
    if (compressed_len < 0 || uncompressed_len < 0) {
      throw ParquetException("Invalid page header");
    }

    EncodedStatistics data_page_statistics;
    if (ShouldSkipPage(&data_page_statistics)) {
      PARQUET_THROW_NOT_OK(stream_->Advance(compressed_len));
      continue;
    }

    if (data_decryptor_ != nullptr) {
      UpdateDecryption(data_decryptor_.get(), encryption::kDictionaryPage,
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
    if (data_decryptor_ != nullptr) {
      auto decryption_buffer = AllocateBuffer(
          properties_.memory_pool(), data_decryptor_->PlaintextLength(compressed_len));
      compressed_len = data_decryptor_->Decrypt(
          page_buffer->span_as<uint8_t>(), decryption_buffer->mutable_span_as<uint8_t>());

      page_buffer = decryption_buffer;
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

      return std::make_shared<DataPageV1>(
          page_buffer, header.num_values, LoadEnumSafe(&header.encoding),
          LoadEnumSafe(&header.definition_level_encoding),
          LoadEnumSafe(&header.repetition_level_encoding), uncompressed_len,
          std::move(data_page_statistics));
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
          std::move(data_page_statistics));
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

  // GH-31992: DataPageV2 may store only levels and no values when all
  // values are null. In this case, Parquet java is known to produce a
  // 0-len compressed area (which is invalid compressed input).
  // See https://github.com/apache/parquet-java/issues/3122
  int64_t decompressed_len = 0;
  if (uncompressed_len - levels_byte_len != 0) {
    // Decompress the values
    PARQUET_ASSIGN_OR_THROW(
        decompressed_len,
        decompressor_->Decompress(
            compressed_len - levels_byte_len, page_buffer->data() + levels_byte_len,
            uncompressed_len - levels_byte_len,
            decompression_buffer_->mutable_data() + levels_byte_len));
  }

  if (decompressed_len != uncompressed_len - levels_byte_len) {
    throw ParquetException("Page didn't decompress to expected size, expected: " +
                           std::to_string(uncompressed_len - levels_byte_len) +
                           ", but got:" + std::to_string(decompressed_len));
  }

  return decompression_buffer_;
}

}  // namespace

std::unique_ptr<PageReader> PageReader::Open(
    std::shared_ptr<ArrowInputStream> stream, int64_t total_num_values,
    Compression::type codec, const ReaderProperties& properties,
    const ColumnDescriptor& descr, bool always_compressed, const CryptoContext* ctx) {
  const auto stats_min_max_field = GetStatisticsMinMaxField(descr);
  return std::unique_ptr<PageReader>(
      new SerializedPageReader(std::move(stream), total_num_values, codec, properties,
                               ctx, always_compressed, stats_min_max_field));
}

std::unique_ptr<PageReader> PageReader::Open(std::shared_ptr<ArrowInputStream> stream,
                                             int64_t total_num_values,
                                             Compression::type codec,
                                             const ReaderProperties& properties,
                                             bool always_compressed,
                                             const CryptoContext* ctx) {
  return std::unique_ptr<PageReader>(new SerializedPageReader(
      std::move(stream), total_num_values, codec, properties, ctx, always_compressed,
      StatisticsMinMaxField::kMinValueMaxValue));
}

std::unique_ptr<PageReader> PageReader::Open(std::shared_ptr<ArrowInputStream> stream,
                                             int64_t total_num_values,
                                             Compression::type codec,
                                             bool always_compressed,
                                             ::arrow::MemoryPool* pool,
                                             const CryptoContext* ctx) {
  return std::unique_ptr<PageReader>(new SerializedPageReader(
      std::move(stream), total_num_values, codec, ReaderProperties(pool), ctx,
      always_compressed, StatisticsMinMaxField::kMinValueMaxValue));
}

namespace {

/***************************
 *  SkippableTypedDecoder  *
 ***************************/

/// Wrapper around a `TypedDecoder` pointer to skip values.
///
/// Use a scratch buffer to decode values into that buffer.
/// This was migrated here from a historical implementation.
/// Ideally all decoders would implement a `Skip` functionality that would at best
/// avoid decoding, and at worst, decode without intermediary allocation.
///
/// @todo GH-50453
template <typename DType, int64_t kScratchValueCount>
class SkippableTypedDecoder {
 public:
  using Decoder = TypedDecoder<DType>;
  using T = typename Decoder::T;

  static constexpr int64_t kValueByteSize = type_traits<DType::type_num>::value_byte_size;
  static constexpr int64_t kScratchByteSize = kScratchValueCount * kValueByteSize;

  explicit SkippableTypedDecoder(::arrow::MemoryPool* pool = nullptr) : pool_(pool) {}

  explicit SkippableTypedDecoder(Decoder* decoder) : decoder_(decoder) {}

  void SetDecoder(Decoder* decoder) { decoder_ = decoder; }

  const Decoder* get() const { return decoder_; }

  Decoder* get() { return decoder_; }

  const Decoder* operator->() const { return decoder_; }

  Decoder* operator->() { return decoder_; }

  explicit operator bool() const { return decoder_ != nullptr; }

  int64_t Skip(int64_t num_values) {
    EnsureScratch();

    int64_t total_read = 0;
    int iter_read = 0;
    do {
      static_assert(kScratchValueCount <= std::numeric_limits<int>::max());
      const int batch_size =
          static_cast<int>(std::min(kScratchValueCount, num_values - total_read));

      iter_read = get()->Decode(scratch_->mutable_data_as<T>(), batch_size);
      total_read += iter_read;
    } while (iter_read > 0 && total_read < num_values);

    return total_read;
  }

 private:
  /// Scratch space to skip decode values that need skipping.
  /// We actually do not need the whole shared_ptr machinery but it was historically
  /// chosen for ease of use with ``AllocateBuffer`` and migrated here.
  std::shared_ptr<ResizableBuffer> scratch_ = nullptr;
  Decoder* decoder_ = nullptr;
  ::arrow::MemoryPool* pool_ = nullptr;

  void EnsureScratch() {
    if (this->scratch_ == nullptr) {
      this->scratch_ = AllocateBuffer(pool_, kScratchByteSize);
    }
    ARROW_DCHECK_NE(this->scratch_, nullptr);
  }
};

/*********************
 *  ValueSinkCursor  *
 *********************/

inline int64_t compute_capacity_pow2(int64_t capacity, int64_t size, int64_t extra_size) {
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

class ValueSinkCursor {
 public:
  int64_t capacity() const { return capacity_; }

  int64_t values_count() const { return values_count_; }

  void set_values_count(int64_t vals) { values_count_ = vals; }

  int64_t fit_capacity_for_extra(int64_t extra_values) {
    auto new_capacity = compute_capacity_pow2(capacity_, values_count_, extra_values);
    ARROW_DCHECK_GE(new_capacity, capacity());
    return std::exchange(capacity_, new_capacity);
  }

  int64_t reset_capacity() { return std::exchange(capacity_, 0); }

 private:
  int64_t values_count_ = 0;
  int64_t capacity_ = 0;
};

/*********************
 *  ValueSinkBuffer  *
 *********************/

template <typename T>
class ValueSinkBuffer : private ValueSinkCursor {
 public:
  using value_type = T;

  using ValueSinkCursor::capacity;
  using ValueSinkCursor::values_count;

  explicit ValueSinkBuffer(MemoryPool* pool) : values_(AllocateBuffer(pool)) {}

  value_type* data() const { return values_->mutable_data_as<value_type>(); }

  void OnNewDictionary(auto& /* decoder */) {}

  [[nodiscard]] auto ReadValuesDense(auto& decoder, int32_t batch_size) {
    const auto decoded = decoder.Decode(write_start(), batch_size);
    set_values_count(values_count() + batch_size);
    return decoded;
  }

  [[nodiscard]] auto ReadValuesSpaced(auto& decoder, int32_t batch_size,
                                      int32_t null_count, const uint8_t* valid_bits,
                                      int64_t valid_bits_offset) {
    const auto decoded = decoder.DecodeSpaced(write_start(), batch_size, null_count,
                                              valid_bits, valid_bits_offset);
    set_values_count(values_count() + batch_size);
    return decoded;
  }

  std::shared_ptr<ResizableBuffer> ReleaseValues(MemoryPool* pool) {
    // TODO should we set values_written to zero?
    auto result = values_;
    const auto byte_count = bytes_for_values(values_count());
    PARQUET_THROW_NOT_OK(result->Resize(byte_count, /*shrink_to_fit=*/true));
    values_ = AllocateBuffer(pool);
    reset_capacity();
    return result;
  }

  void ReserveValues(int64_t extra_values) {
    const auto old_capacity = fit_capacity_for_extra(extra_values);
    if (capacity() > old_capacity) {
      const auto byte_count = bytes_for_values(capacity());
      PARQUET_THROW_NOT_OK(values_->Resize(byte_count, /*shrink_to_fit=*/false));
    }
  }

  void ResetValues() {
    if (values_count() > 0) {
      PARQUET_THROW_NOT_OK(values_->Resize(0, /*shrink_to_fit=*/false));
      ValueSinkCursor::operator=({});
    }
  }

 private:
  std::shared_ptr<::arrow::ResizableBuffer> values_;

  static int64_t bytes_for_values(int64_t nitems) {
    constexpr auto kValueByteSize = static_cast<int64_t>(sizeof(value_type));
    int64_t bytes = -1;
    if (MultiplyWithOverflow(nitems, kValueByteSize, &bytes)) {
      throw ParquetException("Total size of items too large");
    }
    return bytes;
  }

  value_type* write_start() { return data() + values_count(); }
};

/************************
 *  ValiditySinkBuffer  *
 ************************/

class ValiditySinkBuffer : private ValueSinkCursor {
 public:
  using ValueSinkCursor::capacity;
  using ValueSinkCursor::values_count;

  ValiditySinkBuffer() = default;

  explicit ValiditySinkBuffer(MemoryPool* pool) : values_(AllocateBuffer(pool)) {}

  uint8_t* data() const { return values_->mutable_data_as<uint8_t>(); }

  struct ReadResult {
    int64_t values_read = 0;
    int64_t null_count = 0;
  };

  ReadResult ReadFromLevels(const int16_t* def_levels, int64_t num_def_levels,
                            const internal::LevelInfo& level_info) {
    internal::ValidityBitmapInputOutput validity_io{};
    validity_io.values_read_upper_bound = num_def_levels;
    validity_io.valid_bits = data();
    validity_io.valid_bits_offset = values_count();
    DefLevelsToBitmap(def_levels, num_def_levels, level_info, &validity_io);
    ARROW_DCHECK_GE(validity_io.values_read, 0);
    ARROW_DCHECK_GE(validity_io.null_count, 0);

    // Advance by the number of leaf values (one validity bit each), not by the
    // number of definition levels: for repeated/nested columns some def levels
    // describe empty or absent lists that produce no leaf value, so
    // values_read <= num_def_levels.
    set_values_count(values_count() + validity_io.values_read);
    return {
        .values_read = validity_io.values_read,
        .null_count = validity_io.null_count,
    };
  }

  std::shared_ptr<ResizableBuffer> ReleaseValues(MemoryPool* pool) {
    // TODO should we set values_written to zero?
    auto result = values_;
    const auto byte_count = bytes_for_values(values_count());
    PARQUET_THROW_NOT_OK(result->Resize(byte_count, /*shrink_to_fit=*/true));
    values_ = AllocateBuffer(pool);
    reset_capacity();
    return result;
  }

  void ReserveValues(int64_t extra_values) {
    const auto old_capacity = fit_capacity_for_extra(extra_values);
    if (capacity() > old_capacity) {
      const auto byte_count = bytes_for_values(capacity());
      PARQUET_THROW_NOT_OK(values_->Resize(byte_count, /*shrink_to_fit=*/false));
      // Zero the newly grown region so that appending bits at a non-byte-aligned
      // offset never reads uninitialized memory (avoids Valgrind/MSAN warnings).
      const auto old_byte_count = bytes_for_values(old_capacity);
      std::memset(data() + old_byte_count, 0,
                  static_cast<std::size_t>(byte_count - old_byte_count));
    }
  }

  void ResetValues() {
    if (values_count() > 0) {
      PARQUET_THROW_NOT_OK(values_->Resize(0, /*shrink_to_fit=*/false));
      ValueSinkCursor::operator=({});
    }
  }

 private:
  std::shared_ptr<::arrow::ResizableBuffer> values_;

  static int64_t bytes_for_values(int64_t num_values) {
    return bit_util::BytesForBits(num_values);
  }
};

/***********************
 *  ColumnChunkReader  *
 ***********************/

/// Initialize repetition and definition level decoders on the given data page.
///
/// If the data page includes repetition and definition levels, we initialize the level
/// decoders and return the number of encoded level bytes.
/// The return value helps determine the number of bytes in the encoded data.
template <typename LvlDec>
int64_t InitializeV1Levels(const DataPageV1& page, LvlDec& def_dec, LvlDec& rep_dec) {
  const auto num_values = static_cast<int>(page.num_values());

  const uint8_t* buffer = page.data();
  int32_t levels_byte_size = 0;
  int32_t max_size = page.size();

  if (const auto max_rep_lvl = rep_dec.max_level(); max_rep_lvl > 0) {
    const int32_t rep_levels_bytes = rep_dec.SetData(
        page.repetition_level_encoding(), max_rep_lvl, num_values, buffer, max_size);
    buffer += rep_levels_bytes;
    levels_byte_size += rep_levels_bytes;
    max_size -= rep_levels_bytes;
  }

  if (const auto max_def_lvl = def_dec.max_level(); max_def_lvl > 0) {
    const int32_t def_levels_bytes = def_dec.SetData(
        page.definition_level_encoding(), max_def_lvl, num_values, buffer, max_size);
    levels_byte_size += def_levels_bytes;
    max_size -= def_levels_bytes;
  }

  return levels_byte_size;
}

/// Initialize repetition and definition level decoders on the given data page.
///
/// If the data page includes repetition and definition levels, we initialize the level
/// decoders and return the number of encoded level bytes.
/// The return value helps determine the number of bytes in the encoded data.
template <typename LvlDec>
int64_t InitializeV2Levels(const DataPageV2& page, LvlDec& def_dec, LvlDec& rep_dec) {
  const auto num_values = static_cast<int>(page.num_values());

  const int64_t total_levels_length =
      static_cast<int64_t>(page.repetition_levels_byte_length()) +
      page.definition_levels_byte_length();
  if (total_levels_length > page.size()) {
    throw ParquetException("Data page too small for levels (corrupt header?)");
  }

  const uint8_t* buffer = page.data();

  if (const auto max_rep_lvl = rep_dec.max_level(); max_rep_lvl > 0) {
    rep_dec.SetDataV2(page.repetition_levels_byte_length(), max_rep_lvl, num_values,
                      buffer);
  }
  // ARROW-17453: Even if max_rep_level_ is 0, there may still be
  // repetition level bytes written and/or reported in the header by
  // some writers (e.g. Athena)
  buffer += page.repetition_levels_byte_length();

  if (const auto max_def_lvl = def_dec.max_level(); max_def_lvl > 0) {
    def_dec.SetDataV2(page.definition_levels_byte_length(), max_def_lvl, num_values,
                      buffer);
  }

  return total_levels_length;
}

/// Traits of a concrete column chunk reader, for use by `ColumnChunkReader`.
template <typename Derived>
struct reader_trait;

/// Read through the multiple pages of a column chunk.
template <typename Derived>
class ColumnChunkReader {
 public:
  using DType = typename reader_trait<Derived>::DType;
  using LvlDec = typename reader_trait<Derived>::level_decoder;
  using value_type = typename DType::c_type;

  ColumnChunkReader(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool,
                    LvlDec def_levels_decoder, LvlDec rep_levels_decoder)
      : descr_(descr),
        pool_(pool),
        current_decoder_(pool),
        def_levels_decoder_(std::move(def_levels_decoder)),
        rep_levels_decoder_(std::move(rep_levels_decoder)) {}

  void SetPageReader(std::unique_ptr<PageReader> reader) {
    pager_ = std::move(reader);
    // Dictionary decoders must be reset when advancing row groups
    decoders_.clear();
  }

  bool HasPageReader() const { return pager_ != nullptr; }

  /// Return true if there is more data.
  ///
  /// If the current page is exhausted, it will process more pages until some data
  /// page is found.
  bool ProcessToMoreData();

  /// Check the encoding of the current page or throw an exception.
  void CheckEncodingIs(Encoding::type encoding);

 private:
  // Declared here because `decoders_` below refers to it.
  using DecoderType = TypedDecoder<DType>;

  Derived& derived() { return static_cast<Derived&>(*this); }
  const Derived& derived() const { return static_cast<const Derived&>(*this); }

 protected:
  int32_t ReadDefinitionLevels(int32_t batch_size, int16_t* levels) {
    if (max_def_level() == 0) {
      return 0;
    }
    return def_levels_decoder_.Decode(batch_size, levels);
  }

  int32_t ReadRepetitionLevels(int32_t batch_size, int16_t* levels) {
    if (max_rep_level() == 0) {
      return 0;
    }
    return rep_levels_decoder_.Decode(batch_size, levels);
  }

  const ColumnDescriptor* descr_;
  ::arrow::MemoryPool* pool_;
  SkippableTypedDecoder<DType, kSkipScratchBatchSize> current_decoder_;

  // Available values in the current data page, value includes repeated values and nulls.
  int32_t available_values_current_page() const {
    const int32_t out = num_buffered_values_ - num_decoded_values_;
    ARROW_DCHECK_GE(out, 0);
    return out;
  }

  int16_t max_def_level() const;

  int16_t max_rep_level() const;

  void ConsumeBufferedValues(int64_t num_values) { num_decoded_values_ += num_values; }

  int64_t Skip(int64_t num_values_to_skip);

 private:
  LvlDec def_levels_decoder_;
  LvlDec rep_levels_decoder_;
  // Map of encoding type to the respective decoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::unique_ptr<DecoderType>> decoders_;
  std::unique_ptr<PageReader> pager_;
  std::shared_ptr<Page> current_page_;
  // The total number of values stored in the data page. This is the maximum of
  // the number of encoded definition levels or encoded values. For
  // non-repeated, required columns, this is equal to the number of encoded
  // values. For repeated or optional values, there may be fewer data values
  // than levels, and this tells you how many encoded levels there are in that
  // case.
  int32_t num_buffered_values_ = 0;
  // The number of values from the current data page that have been decoded
  // into memory or skipped over.
  int32_t num_decoded_values_ = 0;
  Encoding::type current_encoding_ = Encoding::UNKNOWN;

  // Advance to the next data page
  bool ReadNewPage();

  void ConfigureDictionary(const DictionaryPage* page);

  // Get a decoder object for this page or create a new decoder if this is the
  // first page with this encoding.
  void InitializeDataDecoder(const DataPage& page, int64_t levels_byte_size);

  /// Advance both level decoders by num_levels, without materializing them.
  ///
  /// @return The number of values present (non-null) among them, which is the
  ///         number of values to skip in the data decoder.
  int32_t AdvanceLevels(int32_t num_levels);
};

/**************************************
 *  ColumnChunkReader Implementation  *
 **************************************/

template <typename Derived>
void ColumnChunkReader<Derived>::CheckEncodingIs(Encoding::type encoding) {
  if (current_encoding_ != encoding) {
    auto msg =
        std::format("Unexpected data page encoding. Expected {}, got {}",
                    EncodingToString(encoding), EncodingToString(current_encoding_));
    throw ParquetException(msg);
  }
}

template <typename Derived>
bool ColumnChunkReader<Derived>::ProcessToMoreData() {
  // Either there is no data page available yet, or the data page has been
  // exhausted
  if (num_buffered_values_ == 0 || num_decoded_values_ == num_buffered_values_) {
    if (!ReadNewPage() || num_buffered_values_ == 0) {
      return false;
    }
  }
  return true;
}

template <typename Derived>
bool ColumnChunkReader<Derived>::ReadNewPage() {
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
      const auto& page = static_cast<const DataPageV1&>(*current_page_);
      const int64_t levels_byte_size =
          InitializeV1Levels(page, def_levels_decoder_, rep_levels_decoder_);
      num_buffered_values_ = page.num_values();
      num_decoded_values_ = 0;
      InitializeDataDecoder(page, levels_byte_size);
      return true;
    } else if (current_page_->type() == PageType::DATA_PAGE_V2) {
      const auto& page = static_cast<const DataPageV2&>(*current_page_);
      const int64_t levels_byte_size =
          InitializeV2Levels(page, def_levels_decoder_, rep_levels_decoder_);
      num_buffered_values_ = page.num_values();
      num_buffered_values_ = page.num_values();
      num_decoded_values_ = 0;
      InitializeDataDecoder(page, levels_byte_size);
      return true;
    } else {
      // We don't know what this page type is. We're allowed to skip non-data
      // pages.
      continue;
    }
  }
  return true;
}

template <typename Derived>
void ColumnChunkReader<Derived>::ConfigureDictionary(const DictionaryPage* page) {
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
    auto dictionary = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_, pool_);
    dictionary->SetData(page->num_values(), page->data(), page->size());

    // The dictionary is fully decoded during DictionaryDecoder::Init, so the
    // DictionaryPage buffer is no longer required after this step
    //
    // TODO(wesm): investigate whether this all-or-nothing decoding of the
    // dictionary makes sense and whether performance can be improved

    std::unique_ptr<DictDecoder<DType>> decoder = MakeDictDecoder<DType>(descr_, pool_);
    decoder->SetDict(dictionary.get());
    derived().OnNewDictionary(*decoder);
    decoders_[encoding] =
        std::unique_ptr<DecoderType>(dynamic_cast<DecoderType*>(decoder.release()));
  } else {
    ParquetException::NYI("only plain dictionary encoding has been implemented");
  }

  current_decoder_.SetDecoder(decoders_[encoding].get());
  ARROW_DCHECK(current_decoder_);
}

template <typename Derived>
void ColumnChunkReader<Derived>::InitializeDataDecoder(const DataPage& page,
                                                       int64_t levels_byte_size) {
  const uint8_t* buffer = page.data() + levels_byte_size;
  const int64_t data_size = page.size() - levels_byte_size;

  if (data_size < 0) {
    throw ParquetException("Page smaller than size of encoded levels");
  }

  Encoding::type encoding = page.encoding();
  if (IsDictionaryIndexEncoding(encoding)) {
    // Normalizing the PLAIN_DICTIONARY to RLE_DICTIONARY encoding
    // in decoder.
    encoding = Encoding::RLE_DICTIONARY;
  }

  auto it = decoders_.find(static_cast<int>(encoding));
  if (it != decoders_.end()) {
    ARROW_DCHECK(it->second.get() != nullptr);
    current_decoder_.SetDecoder(it->second.get());
  } else {
    switch (encoding) {
      case Encoding::PLAIN:
      case Encoding::BYTE_STREAM_SPLIT:
      case Encoding::RLE:
      case Encoding::DELTA_BINARY_PACKED:
      case Encoding::DELTA_BYTE_ARRAY:
      case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
        auto decoder = MakeTypedDecoder<DType>(encoding, descr_, pool_);
        current_decoder_.SetDecoder(decoder.get());
        decoders_[static_cast<int>(encoding)] = std::move(decoder);
        break;
      }

      case Encoding::RLE_DICTIONARY:
        throw ParquetException("Dictionary page must be before data page.");

      default:
        throw ParquetException("Unknown encoding type.");
    }
  }
  current_encoding_ = encoding;
  current_decoder_->SetData(static_cast<int>(num_buffered_values_), buffer,
                            static_cast<int>(data_size));
}

template <typename Derived>
int16_t ColumnChunkReader<Derived>::max_def_level() const {
  return def_levels_decoder_.max_level();
}

template <typename Derived>
int16_t ColumnChunkReader<Derived>::max_rep_level() const {
  return rep_levels_decoder_.max_level();
}

template <typename Derived>
int32_t ColumnChunkReader<Derived>::AdvanceLevels(int32_t num_levels) {
  int max_count = num_levels;
  // Advance the definition levels, counting how many correspond to present
  // (non-null) values that must be skipped in the data decoder.
  if (this->max_def_level() > 0) {
    const auto count = def_levels_decoder_.CountUpTo(this->max_def_level(), num_levels);
    max_count = count.matching_count;
    ARROW_DCHECK_EQ(count.processed_count, num_levels);
  }
  // Advance the repetition levels; their values are not needed.
  if (this->max_rep_level() > 0) {
    rep_levels_decoder_.Skip(num_levels);
  }
  return max_count;
}

template <typename Derived>
int64_t ColumnChunkReader<Derived>::Skip(int64_t num_values_to_skip) {
  int64_t values_to_skip = num_values_to_skip;
  // Optimization: Do not call HasNext() when values_to_skip == 0.
  while (values_to_skip > 0 && ProcessToMoreData()) {
    // If the number of values to skip is more than the number of undecoded values, skip
    // the whole Page without decoding levels or values.
    const int64_t available_values = this->available_values_current_page();
    if (values_to_skip >= available_values) {
      values_to_skip -= available_values;
      this->ConsumeBufferedValues(available_values);
    } else {
      // Skip within the current Page. Since `values_to_skip < available_values`, the
      // whole batch fits inside this Page and no page boundary is crossed.
      const int batch_size = static_cast<int>(values_to_skip);

      int64_t non_null = AdvanceLevels(batch_size);
      // Skip the corresponding data values.
      this->current_decoder_.Skip(non_null);

      this->ConsumeBufferedValues(batch_size);
      values_to_skip -= batch_size;
    }
  }
  return num_values_to_skip - values_to_skip;
}

/***************************
 *  TypedColumnReaderImpl  *
 ***************************/

template <typename DType>
class TypedColumnReaderImpl;

template <typename D>
struct reader_trait<TypedColumnReaderImpl<D>> {
  using DType = D;
  using level_decoder = LevelDecoder;
};

template <typename DType>
class TypedColumnReaderImpl : public TypedColumnReader<DType>,
                              public ColumnChunkReader<TypedColumnReaderImpl<DType>> {
 public:
  using T = typename DType::c_type;

  TypedColumnReaderImpl(const ColumnDescriptor* descr, std::unique_ptr<PageReader> pager,
                        ::arrow::MemoryPool* pool)
      : ColumnChunkReader<TypedColumnReaderImpl<DType>>(
            descr, pool, LevelDecoder(descr->max_definition_level()),
            LevelDecoder(descr->max_repetition_level())) {
    this->SetPageReader(std::move(pager));
  }

  bool HasNext() override { return this->ProcessToMoreData(); }

  /// Called by ColumnChunkReader.
  void OnNewDictionary(DictDecoder<DType>& /* decoder */) {}

  int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                    T* values, int64_t* values_read) override;

  int64_t Skip(int64_t num_values_to_skip) override {
    return ColumnChunkReader<TypedColumnReaderImpl<DType>>::Skip(num_values_to_skip);
  }

  Type::type type() const override { return this->descr_->physical_type(); }

  const ColumnDescriptor* descr() const override { return this->descr_; }

  ExposedEncoding GetExposedEncoding() override { return exposed_encoding_; };

  int64_t ReadBatchWithDictionary(int64_t batch_size, int16_t* def_levels,
                                  int16_t* rep_levels, int32_t* indices,
                                  int64_t* indices_read, const T** dict,
                                  int32_t* dict_len) override;

 protected:
  void SetExposedEncoding(ExposedEncoding encoding) override {
    exposed_encoding_ = encoding;
  }

 private:
  // The exposed encoding
  ExposedEncoding exposed_encoding_ = ExposedEncoding::NO_ENCODING;

  // Read dictionary indices. Similar to ReadValues but decode data to dictionary indices.
  // This function is called only by ReadBatchWithDictionary().
  int64_t ReadDictionaryIndices(int64_t indices_to_read, int32_t* indices) {
    auto decoder = dynamic_cast<DictDecoder<DType>*>(this->current_decoder_.get());
    return decoder->DecodeIndices(static_cast<int>(indices_to_read), indices);
  }

  // Get dictionary. The dictionary should have been set by SetDict(). The dictionary is
  // owned by the internal decoder and is destroyed when the reader is destroyed. This
  // function is called only by ReadBatchWithDictionary() after dictionary is configured.
  void GetDictionary(const T** dictionary, int32_t* dictionary_length) {
    auto decoder = dynamic_cast<DictDecoder<DType>*>(this->current_decoder_.get());
    decoder->GetDictionary(dictionary, dictionary_length);
  }

  // Read definition and repetition levels. Also return the number of definition levels
  // and number of values to read. This function is called before reading values.
  //
  // ReadLevelsInCurrentPage will throw exception when any num-levels read is not
  // equal to the number of the levels can be read.
  void ReadLevelsInCurrentPage(int32_t batch_size, int16_t* def_levels,
                               int16_t* rep_levels, int32_t* num_def_levels,
                               int32_t* non_null_values_to_read) {
    batch_size = std::min(batch_size, this->available_values_current_page());
    // If the field is required and non-repeated, there are no definition levels
    if (this->max_def_level() > 0 && def_levels != nullptr) {
      *num_def_levels = this->ReadDefinitionLevels(batch_size, def_levels);
      if (ARROW_PREDICT_FALSE(*num_def_levels != batch_size)) {
        throw ParquetException(kErrorRepDefLevelNotMatchesNumValues);
      }
      // TODO(wesm): this tallying of values-to-decode can be performed with better
      // cache-efficiency if fused with the level decoding.
      *non_null_values_to_read +=
          std::count(def_levels, def_levels + *num_def_levels, this->max_def_level());
    } else {
      // Required field, read all values
      if (num_def_levels != nullptr) {
        *num_def_levels = 0;
      }
      *non_null_values_to_read = batch_size;
    }

    // Not present for non-repeated fields
    if (this->max_rep_level() > 0 && rep_levels != nullptr) {
      int64_t num_rep_levels = this->ReadRepetitionLevels(batch_size, rep_levels);
      if (batch_size != num_rep_levels) {
        throw ParquetException(kErrorRepDefLevelNotMatchesNumValues);
      }
    }
  }
};

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::ReadBatchWithDictionary(
    int64_t batch_size_64, int16_t* def_levels, int16_t* rep_levels, int32_t* indices,
    int64_t* indices_read, const T** dict, int32_t* dict_len) {
  // This is only reading in the current page so this fits in an int32.
  const auto batch_size = clamp_to<int32_t>(batch_size_64);

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
  this->CheckEncodingIs(Encoding::RLE_DICTIONARY);

  // Get dictionary pointer and length.
  if (has_dict_output) {
    GetDictionary(dict, dict_len);
  }

  // Similar logic as ReadValues to get def levels and rep levels.
  int32_t num_def_levels = 0;
  int32_t indices_to_read = 0;
  ReadLevelsInCurrentPage(batch_size, def_levels, rep_levels, &num_def_levels,
                          &indices_to_read);

  // Read dictionary indices.
  *indices_read = ReadDictionaryIndices(indices_to_read, indices);
  int64_t total_indices = std::max<int64_t>(num_def_levels, *indices_read);
  // Some callers use a batch size of 0 just to get the dictionary.
  int64_t expected_values = std::min(batch_size, this->available_values_current_page());
  if (total_indices == 0 && expected_values > 0) {
    std::stringstream ss;
    ss << "Read 0 values, expected " << expected_values;
    ParquetException::EofException(ss.str());
  }
  this->ConsumeBufferedValues(total_indices);

  return total_indices;
}

template <typename DType>
int64_t TypedColumnReaderImpl<DType>::ReadBatch(int64_t batch_size_64,
                                                int16_t* def_levels, int16_t* rep_levels,
                                                T* values, int64_t* values_read) {
  // HasNext might invoke ReadNewPage until a data page with
  // `available_values_current_page() > 0` is found.
  if (!HasNext()) {
    *values_read = 0;
    return 0;
  }

  // This is only reading in the current page so this fits in an int32.
  const auto batch_size = clamp_to<int32_t>(batch_size_64);

  // TODO(wesm): keep reading data pages until batch_size is reached, or the
  // row group is finished
  int32_t num_def_levels = 0;
  // Number of non-null values to read within `num_def_levels`.
  int32_t non_null_values_to_read = 0;
  ReadLevelsInCurrentPage(batch_size, def_levels, rep_levels, &num_def_levels,
                          &non_null_values_to_read);
  // Should not return more values than available in the current data page,
  // since currently, ReadLevelsInCurrentPage would only consume levels from current
  // data page.
  if (ARROW_PREDICT_FALSE(num_def_levels > this->available_values_current_page())) {
    throw ParquetException(kErrorRepDefLevelNotMatchesNumValues);
  }
  if (non_null_values_to_read != 0) {
    *values_read = this->current_decoder_->Decode(values, non_null_values_to_read);
  } else {
    *values_read = 0;
  }
  // Adjust total_values, since if max_def_level_ == 0, num_def_levels would
  // be 0 and `values_read` would adjust to `available_values_current_page()`.
  int64_t total_values = std::max<int64_t>(num_def_levels, *values_read);
  int64_t expected_values = std::min(batch_size, this->available_values_current_page());
  if (total_values == 0 && expected_values > 0) {
    std::stringstream ss;
    ss << "Read 0 values, expected " << expected_values;
    ParquetException::EofException(ss.str());
  }
  this->ConsumeBufferedValues(total_values);
  return total_values;
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
  ::arrow::Unreachable();
}

// ----------------------------------------------------------------------
// RecordReader

namespace internal {

namespace {

/// Whether values of type T can be printed to `std::cout`.
template <typename T>
concept can_cout = requires(std::ostream& os, const T& value) {
  os << value;
};  // NOLINT(readability/braces)

/***********************
 *  TypedRecordReader  *
 ***********************/

template <typename DType, typename ValueSink, bool kReadDictionary>
class TypedRecordReader;

// `reader_trait` can only be specialized in the namespace it is declared in.
}  // namespace
}  // namespace internal

namespace {
template <typename D, typename ValueSink, bool kReadDictionary>
struct reader_trait<internal::TypedRecordReader<D, ValueSink, kReadDictionary>> {
  using DType = D;
  using level_decoder = LevelDecoder;
};
}  // namespace

namespace internal {
namespace {

template <typename DType, typename ValueSink, bool kReadDictionary>
class TypedRecordReader
    : public ColumnChunkReader<TypedRecordReader<DType, ValueSink, kReadDictionary>>,
      virtual public RecordReader {
 public:
  using T = typename DType::c_type;
  using Base = ColumnChunkReader<TypedRecordReader<DType, ValueSink, kReadDictionary>>;

  TypedRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info, MemoryPool* pool,
                    bool read_dense_for_nullable, ValueSink value_sink)
      : Base(descr, pool, LevelDecoder(descr->max_definition_level()),
             LevelDecoder(descr->max_repetition_level())),
        value_sink_(std::move(value_sink)),
        leaf_info_(leaf_info),
        def_levels_(AllocateBuffer(pool)),
        rep_levels_(AllocateBuffer(pool)),
        read_dense_for_nullable_(read_dense_for_nullable) {
    TypedRecordReader::Reset();
  }

  int16_t* def_levels() const final {
    return reinterpret_cast<int16_t*>(def_levels_->mutable_data());
  }

  int16_t* rep_levels() const final {
    return reinterpret_cast<int16_t*>(rep_levels_->mutable_data());
  }

  int64_t levels_position() const final { return levels_position_; }

  int64_t levels_written() const final { return levels_written_; }

  int64_t null_count() const final { return null_count_; }

  /// \brief Indicates if we can have nullable values. Note that repeated fields
  /// may or may not be nullable.
  bool nullable_values() const final { return leaf_info_.HasNullableValues(); }

  bool read_dictionary() const final { return kReadDictionary; }

  bool read_dense_for_nullable() const final { return read_dense_for_nullable_; }

  uint8_t* values() const final { return reinterpret_cast<uint8_t*>(value_sink_.data()); }

  int64_t values_written() const final { return value_sink_.values_count(); }

  const void* ReadDictionary(int32_t* dictionary_length) override;

  int64_t ReadRecords(int64_t num_records) override;

  // Throw away levels from start_levels_position to levels_position_.
  // Will update levels_position_, levels_written_, and levels_capacity_
  // accordingly and move the levels to left to fill in the gap.
  // It will resize the buffer without releasing the memory allocation.
  void ThrowAwayLevels(int64_t start_levels_position);

  // Skip records that we have in our buffer. This function is only for
  // non-repeated fields.
  int64_t SkipRecordsInBufferNonRepeated(int64_t num_records);

  // Attempts to skip num_records from the buffer. Will throw away levels
  // and corresponding values for the records it skipped and consumes them from the
  // underlying decoder. Will advance levels_position_ and update
  // at_record_start_.
  // Returns how many records were skipped.
  int64_t DelimitAndSkipRecordsInBuffer(int64_t num_records);

  // Skip records for repeated fields. For repeated fields, we are technically
  // reading and throwing away the levels and values since we do not know the record
  // boundaries in advance. Keep filling the buffer and skipping until we reach the
  // desired number of records or we run out of values in the column chunk.
  // Returns number of skipped records.
  int64_t SkipRecordsRepeated(int64_t num_records);

  // Read 'num_values' values and throw them away.
  // Throws an error if it could not read 'num_values'.
  void ReadAndThrowAwayValues(int64_t num_values);

  int64_t SkipRecords(int64_t num_records) override;

  // We may outwardly have the appearance of having exhausted a column chunk
  // when in fact we are in the middle of processing the last batch
  bool has_values_to_process() const { return levels_position_ < levels_written_; }

  std::shared_ptr<ResizableBuffer> ReleaseValues() override {
    return value_sink_.ReleaseValues(this->pool_);
  }

  std::shared_ptr<ResizableBuffer> ReleaseIsValid() override;

  // Process written repetition/definition levels to reach the end of
  // records. Only used for repeated fields.
  // Process no more levels than necessary to delimit the indicated
  // number of logical records. Updates internal state of RecordReader
  //
  // \return Number of records delimited
  int64_t DelimitRecords(int64_t num_records, int64_t* values_seen);

  void Reserve(int64_t capacity) override;

  void ReserveLevels(int64_t extra_levels);

  void ReserveValues(int64_t extra_values) { value_sink_.ReserveValues(extra_values); }

  void ReserveIsValid(int64_t extra_values);

  void Reset() override;

  void SetPageReader(std::unique_ptr<PageReader> reader) override;

  bool HasMoreData() const override {
    return Base::HasPageReader();  // Surprising legacy behaviour
  }

  const ColumnDescriptor* descr() const override { return this->descr_; }

  void OnNewDictionary(DictDecoder<DType>& decoder) {
    value_sink_.OnNewDictionary(decoder);
  }

  void ReadValuesSpaced(int64_t valid_bits_offset, int32_t values_with_nulls,
                        int32_t null_count);

  void ReadValuesDense(int32_t values_to_read);

  // Reads repeated records and returns number of records read. Fills in
  // values_to_read and null_count.
  int64_t ReadRepeatedRecords(int64_t num_records, int64_t* values_to_read,
                              int64_t* null_count);

  // Reads optional records and returns number of records read. Fills in
  // values_to_read and null_count.
  int64_t ReadOptionalRecords(int64_t num_records, int64_t* values_to_read,
                              int64_t* null_count);

  // Reads dense for optional records. First it figures out how many values to
  // read.
  void ReadDenseForOptional(int64_t start_levels_position, int64_t* values_to_read);

  // Reads spaced for optional or repeated fields.
  void ReadSpacedForOptionalOrRepeated(int64_t start_levels_position,
                                       int64_t* values_to_read, int64_t* null_count);

  // Return number of logical records read.
  // Updates levels_position_, values_written_, and null_count_.
  int64_t ReadRecordData(int64_t num_records);

  void DebugPrintState() override;

  void ResetValues();

 protected:
  auto value_sink() -> ValueSink& { return value_sink_; }
  auto value_sink() const -> const ValueSink& { return value_sink_; }

 private:
  ValueSink value_sink_;
  /// \brief Each bit corresponds to one element in 'values_' and specifies if it
  /// is null or not null.
  ///
  /// Not set if leaf type is not nullable or read_dense_for_nullable_ is true.
  // TODO make optional where abscense means read_dense_for_optional
  ValiditySinkBuffer valid_bits_;
  LevelInfo leaf_info_;

  /// \brief Buffer for definition levels. May contain more levels than
  /// is actually read. This is because we read levels ahead to
  /// figure out record boundaries for repeated fields.
  /// For flat required fields, 'def_levels_' and 'rep_levels_' are not
  ///  populated. For non-repeated fields 'rep_levels_' is not populated.
  /// 'def_levels_' and 'rep_levels_' must be of the same size if present.
  std::shared_ptr<::arrow::ResizableBuffer> def_levels_;
  /// \brief Buffer for repetition levels. Only populated for repeated
  /// fields.
  std::shared_ptr<::arrow::ResizableBuffer> rep_levels_;

  int64_t records_read_;

  int64_t null_count_ = 0;

  /// \brief Number of definition / repetition levels that have been written
  /// internally in the reader. This may be larger than values_written() since
  /// for repeated fields we need to look at the levels in advance to figure out
  /// the record boundaries.
  int64_t levels_written_ = 0;
  /// \brief Position of the next level that should be consumed.
  int64_t levels_position_ = 0;
  int64_t levels_capacity_ = 0;

  bool at_record_start_ = true;

  // If true, we will not leave any space for the null values in the values_
  // vector or fill nulls values in BinaryRecordReader/DictionaryRecordReader.
  //
  // If read_dense_for_nullable_ is true, the BinaryRecordReader/DictionaryRecordReader
  // might still populate the validity bitmap buffer.
  bool read_dense_for_nullable_ = false;
};

/**************************************
 *  TypedRecordReader Implementation  *
 **************************************/

template <typename DT, typename VS, bool kDic>
const void* TypedRecordReader<DT, VS, kDic>::ReadDictionary(int32_t* dictionary_length) {
  if (!this->current_decoder_ && !this->ProcessToMoreData()) {
    *dictionary_length = 0;
    return nullptr;
  }
  // Verify the current data page is dictionary encoded.
  this->CheckEncodingIs(Encoding::RLE_DICTIONARY);
  auto decoder = dynamic_cast<DictDecoder<DT>*>(this->current_decoder_.get());
  const T* dictionary = nullptr;
  decoder->GetDictionary(&dictionary, dictionary_length);
  return reinterpret_cast<const void*>(dictionary);
}

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::ReadRecords(int64_t num_records) {
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
    if (!this->ProcessToMoreData()) {
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
    const int64_t batch_size_64 =
        std::min<int64_t>(level_batch_size, this->available_values_current_page());
    // available_values_current_page fits in int32_t
    const auto batch_size = static_cast<int32_t>(batch_size_64);

    // No more data in column
    if (batch_size == 0) {
      break;
    }

    if (this->max_def_level() > 0) {
      ReserveLevels(batch_size);

      int16_t* def_levels = this->def_levels() + levels_written_;
      int16_t* rep_levels = this->rep_levels() + levels_written_;

      if (ARROW_PREDICT_FALSE(this->ReadDefinitionLevels(batch_size, def_levels) !=
                              batch_size)) {
        throw ParquetException(kErrorRepDefLevelNotMatchesNumValues);
      }
      if (this->max_rep_level() > 0) {
        int64_t rep_levels_read = this->ReadRepetitionLevels(batch_size, rep_levels);
        if (ARROW_PREDICT_FALSE(rep_levels_read != batch_size)) {
          throw ParquetException(kErrorRepDefLevelNotMatchesNumValues);
        }
      }

      levels_written_ += batch_size;
      records_read += ReadRecordData(num_records - records_read);
    } else {
      // No repetition and definition levels, we can read values directly
      const auto batch_size = std::min(num_records - records_read, batch_size_64);
      records_read += ReadRecordData(batch_size);
    }
  }

  return records_read;
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ThrowAwayLevels(int64_t start_levels_position) {
  ARROW_DCHECK_LE(levels_position_, levels_written_);
  ARROW_DCHECK_LE(start_levels_position, levels_position_);
  ARROW_DCHECK_GT(this->max_def_level(), 0);
  ARROW_DCHECK_NE(def_levels_, nullptr);

  int64_t gap = levels_position_ - start_levels_position;
  if (gap == 0) return;

  int64_t levels_remaining = levels_written_ - gap;

  auto left_shift = [&](::arrow::ResizableBuffer* buffer) {
    auto* data = buffer->mutable_data_as<int16_t>();
    std::copy(data + levels_position_, data + levels_written_,
              data + start_levels_position);
    PARQUET_THROW_NOT_OK(buffer->Resize(levels_remaining * sizeof(int16_t),
                                        /*shrink_to_fit=*/false));
  };

  left_shift(def_levels_.get());

  if (this->max_rep_level() > 0) {
    ARROW_DCHECK_NE(rep_levels_, nullptr);
    left_shift(rep_levels_.get());
  }

  levels_written_ -= gap;
  levels_position_ -= gap;
  levels_capacity_ -= gap;
}

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::SkipRecordsInBufferNonRepeated(
    int64_t num_records) {
  ARROW_DCHECK_EQ(this->max_rep_level(), 0);
  if (!this->has_values_to_process() || num_records == 0) return 0;

  int64_t remaining_records = levels_written_ - levels_position_;
  int64_t skipped_records = std::min(num_records, remaining_records);
  int64_t start_levels_position = levels_position_;
  // Since there is no repetition, number of levels equals number of records.
  levels_position_ += skipped_records;

  // We skipped the levels by incrementing 'levels_position_'. For values
  // we do not have a buffer, so we need to read them and throw them away.
  // First we need to figure out how many present/not-null values there are.
  int64_t values_to_read =
      std::count(def_levels() + start_levels_position, def_levels() + levels_position_,
                 this->max_def_level());

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

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::DelimitAndSkipRecordsInBuffer(
    int64_t num_records) {
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

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::SkipRecordsRepeated(int64_t num_records) {
  ARROW_DCHECK_GT(this->max_rep_level(), 0);
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
    if (!this->ProcessToMoreData()) {
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
    const int64_t batch_size_64 =
        std::min<int64_t>(level_batch_size, this->available_values_current_page());
    // available_values_current_page fits in int32_t
    const auto batch_size = static_cast<int32_t>(batch_size_64);

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

    if (this->ReadDefinitionLevels(batch_size, def_levels) != batch_size) {
      throw ParquetException(kErrorRepDefLevelNotMatchesNumValues);
    }
    if (this->ReadRepetitionLevels(batch_size, rep_levels) != batch_size) {
      throw ParquetException(kErrorRepDefLevelNotMatchesNumValues);
    }

    levels_written_ += batch_size;
    int64_t remaining_records = num_records - skipped_records;
    // This updates at_record_start_.
    skipped_records += DelimitAndSkipRecordsInBuffer(remaining_records);
  }

  return skipped_records;
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ReadAndThrowAwayValues(int64_t num_values) {
  const int64_t values_read = this->current_decoder_.Skip(num_values);
  if (values_read < num_values) {
    std::stringstream ss;
    ss << "Could not read and throw away " << num_values << " values";
    throw ParquetException(ss.str());
  }
}

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::SkipRecords(int64_t num_records) {
  if (num_records == 0) return 0;

  // Top level required field. Number of records equals to number of levels,
  // and there is not read-ahead for levels.
  if (this->max_rep_level() == 0 && this->max_def_level() == 0) {
    return this->Skip(num_records);
  }
  int64_t skipped_records = 0;
  if (this->max_rep_level() == 0) {
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

template <typename DT, typename VS, bool kDic>
std::shared_ptr<ResizableBuffer> TypedRecordReader<DT, VS, kDic>::ReleaseIsValid() {
  if (nullable_values()) {
    return valid_bits_.ReleaseValues(this->pool_);
  } else {
    return nullptr;
  }
}

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::DelimitRecords(int64_t num_records,
                                                        int64_t* values_seen) {
  if (ARROW_PREDICT_FALSE(num_records == 0 || levels_position_ == levels_written_)) {
    *values_seen = 0;
    return 0;
  }
  int64_t records_read = 0;
  const int16_t* const rep_levels = this->rep_levels();
  const int16_t* const def_levels = this->def_levels();
  ARROW_DCHECK_GT(this->max_rep_level(), 0);
  // If at_record_start_ is true, we are seeing the start of a record
  // for the second time, such as after repeated calls to
  // DelimitRecords. In this case we must continue until we find
  // another record start or exhausting the ColumnChunk
  int64_t level = levels_position_;
  if (at_record_start_) {
    if (ARROW_PREDICT_FALSE(rep_levels[levels_position_] != 0)) {
      std::stringstream ss;
      ss << "The repetition level at the start of a record must be 0 but got "
         << rep_levels[levels_position_];
      throw ParquetException(ss.str());
    }
    ++levels_position_;
    // We have decided to consume the level at this position; therefore we
    // must advance until we find another record boundary
    at_record_start_ = false;
  }

  // Count logical records and number of non-null values to read
  ARROW_DCHECK(!at_record_start_);
  // Scan repetition levels to find record end
  while (levels_position_ < levels_written_) {
    // We use an estimated batch size to simplify branching and
    // improve performance in the common case. This might slow
    // things down a bit if a single long record remains, though.
    int64_t stride =
        std::min(levels_written_ - levels_position_, num_records - records_read);
    const int64_t position_end = levels_position_ + stride;
    for (int64_t i = levels_position_; i < position_end; ++i) {
      records_read += rep_levels[i] == 0;
    }
    levels_position_ = position_end;
    if (records_read == num_records) {
      // Check last rep_level reaches the boundary and
      // pop the last level.
      ARROW_CHECK_EQ(rep_levels[levels_position_ - 1], 0);
      --levels_position_;
      // We've found the number of records we were looking for. Set
      // at_record_start_ to true and break
      at_record_start_ = true;
      break;
    }
  }
  // Scan definition levels to find number of physical values
  *values_seen = std::count(def_levels + level, def_levels + levels_position_,
                            this->max_def_level());
  return records_read;
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::Reserve(int64_t extra_values) {
  ReserveLevels(extra_values);
  ReserveValues(extra_values);
  ReserveIsValid(extra_values);
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ReserveLevels(int64_t extra_levels) {
  if (this->max_def_level() > 0) {
    const int64_t new_levels_capacity =
        compute_capacity_pow2(levels_capacity_, levels_written_, extra_levels);
    if (new_levels_capacity > levels_capacity_) {
      constexpr auto kItemSize = static_cast<int64_t>(sizeof(int16_t));
      int64_t capacity_in_bytes = -1;
      if (MultiplyWithOverflow(new_levels_capacity, kItemSize, &capacity_in_bytes)) {
        throw ParquetException("Allocation size too large (corrupt file?)");
      }
      PARQUET_THROW_NOT_OK(
          def_levels_->Resize(capacity_in_bytes, /*shrink_to_fit=*/false));
      if (this->max_rep_level() > 0) {
        PARQUET_THROW_NOT_OK(
            rep_levels_->Resize(capacity_in_bytes, /*shrink_to_fit=*/false));
      }
      levels_capacity_ = new_levels_capacity;
    }
  }
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ReserveIsValid(int64_t extra_values) {
  if (nullable_values() && !read_dense_for_nullable_) {
    valid_bits_.ReserveValues(extra_values);
  }
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::Reset() {
  ResetValues();
  if (!read_dense_for_nullable()) {
    valid_bits_ = ValiditySinkBuffer(this->pool_);
  }

  if (levels_written_ > 0) {
    // Throw away levels from 0 to levels_position_.
    ThrowAwayLevels(0);
  }

  // Call Finish on the binary builders to reset them
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::SetPageReader(std::unique_ptr<PageReader> reader) {
  at_record_start_ = true;
  Base::SetPageReader(std::move(reader));
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ReadValuesSpaced(int64_t valid_bits_offset,
                                                       int32_t values_with_nulls,
                                                       int32_t null_count) {
  const auto decoded = value_sink_.ReadValuesSpaced(
      *this->current_decoder_.get(), values_with_nulls, null_count,
      /* valid_bits= */ valid_bits_.data(),
      /* valid_bits_offset= */ valid_bits_offset);
  CheckNumberDecoded(decoded, values_with_nulls);
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ReadValuesDense(int32_t batch_size) {
  const auto decoded =
      value_sink_.ReadValuesDense(*this->current_decoder_.get(), batch_size);
  CheckNumberDecoded(decoded, batch_size);
}

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::ReadRepeatedRecords(int64_t num_records,
                                                             int64_t* values_to_read,
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
    // This is only reading in the current page so this fits in an int32.
    ReadValuesDense(clamp_to<int32_t>(*values_to_read));
    // null_count is always 0 for required.
    ARROW_DCHECK_EQ(*null_count, 0);
  } else {
    ReadSpacedForOptionalOrRepeated(start_levels_position, values_to_read, null_count);
  }
  return records_read;
}

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::ReadOptionalRecords(int64_t num_records,
                                                             int64_t* values_to_read,
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

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ReadDenseForOptional(int64_t start_levels_position,
                                                           int64_t* values_to_read) {
  // levels_position_ must already be incremented based on number of records
  // read.
  ARROW_DCHECK_GE(levels_position_, start_levels_position);

  // When reading dense we need to figure out number of values to read.
  const int16_t* def_levels = this->def_levels();
  *values_to_read += std::count(def_levels + start_levels_position,
                                def_levels + levels_position_, this->max_def_level());
  // This is only reading in the current page so this fits in an int32.
  ReadValuesDense(clamp_to<int32_t>(*values_to_read));
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ReadSpacedForOptionalOrRepeated(
    int64_t start_levels_position, int64_t* values_to_read, int64_t* null_count) {
  // levels_position_ must already be incremented based on number of records
  // read.
  const int64_t valid_bits_offset = valid_bits_.values_count();
  ARROW_DCHECK_EQ(values_written(), valid_bits_offset);
  const auto result = valid_bits_.ReadFromLevels(  //
      def_levels() + start_levels_position, levels_position_ - start_levels_position,
      leaf_info_);

  *values_to_read = result.values_read - result.null_count;
  *null_count = result.null_count;

  // This is only reading in the current page so this fits in an int32.
  ReadValuesSpaced(valid_bits_offset, clamp_to<int32_t>(result.values_read),
                   clamp_to<int32_t>(*null_count));
}

template <typename DT, typename VS, bool kDic>
int64_t TypedRecordReader<DT, VS, kDic>::ReadRecordData(int64_t num_records) {
  // Conservative upper bound
  const int64_t possible_num_values =
      std::max<int64_t>(num_records, levels_written_ - levels_position_);
  ReserveValues(possible_num_values);
  ReserveIsValid(possible_num_values);

  const int64_t start_levels_position = levels_position_;

  // To be updated by the function calls below for each of the repetition
  // types.
  int64_t records_read = 0;
  int64_t values_to_read = 0;
  int64_t null_count = 0;
  if (this->max_rep_level() > 0) {
    // Repeated fields may be nullable or not.
    // This call updates levels_position_.
    records_read = ReadRepeatedRecords(num_records, &values_to_read, &null_count);
  } else if (this->max_def_level() > 0) {
    // Non-repeated optional values are always nullable.
    // This call updates levels_position_.
    ARROW_DCHECK(nullable_values());
    records_read = ReadOptionalRecords(num_records, &values_to_read, &null_count);
  } else {
    ARROW_DCHECK(!nullable_values());
    values_to_read = num_records;
    // This is only reading in the current page so this fits in an int32.
    ReadValuesDense(clamp_to<int32_t>(values_to_read));
    records_read = num_records;
    // We don't need to update null_count, since it is 0.
  }

  ARROW_DCHECK_GE(records_read, 0);
  ARROW_DCHECK_GE(values_to_read, 0);
  ARROW_DCHECK_GE(null_count, 0);

  // The values have already been accounted for by ReadValuesDense/Spaced.
  if (read_dense_for_nullable_) {
    ARROW_DCHECK_EQ(null_count, 0);
  } else {
    null_count_ += null_count;
  }
  // Total values, including null spaces, if any
  if (this->max_def_level() > 0) {
    // Optional, repeated, or some mix thereof
    this->ConsumeBufferedValues(levels_position_ - start_levels_position);
  } else {
    // Flat, non-repeated
    this->ConsumeBufferedValues(values_to_read);
  }

  return records_read;
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::DebugPrintState() {
  const int16_t* def_levels = this->def_levels();
  const int16_t* rep_levels = this->rep_levels();
  const int64_t total_levels_read = levels_position_;

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
  if constexpr (can_cout<T>) {
    const T* vals = reinterpret_cast<const T*>(this->values());
    for (int64_t i = 0; i < this->values_written(); ++i) {
      std::cout << vals[i] << " ";
    }
  } else {
    for (int64_t i = 0; i < this->values_written(); ++i) {
      std::cout << "? ";
    }
  }
  std::cout << std::endl;
}

template <typename DT, typename VS, bool kDic>
void TypedRecordReader<DT, VS, kDic>::ResetValues() {
  if (values_written() > 0) {
    value_sink_.ResetValues();
    valid_bits_.ResetValues();
    null_count_ = 0;
  }
}

/*******************************
 *  RequiredTypedRecordReader  *
 *******************************/

// TODO can we reduce some code share with TypedRecordREader ?
template <typename DType, typename ValueSink, bool kReadDictionary>
class RequiredTypedRecordReader;

// `reader_trait` can only be specialized in the namespace it is declared in.
}  // namespace
}  // namespace internal

namespace {
template <typename D, typename ValueSink, bool kReadDictionary>
struct reader_trait<internal::RequiredTypedRecordReader<D, ValueSink, kReadDictionary>> {
  using DType = D;
  using level_decoder = LevelDecoder;
};
}  // namespace

namespace internal {
namespace {

template <typename DType, typename ValueSink = ValueSinkBuffer<typename DType::c_type>,
          bool kReadDictionary = false>
class RequiredTypedRecordReader
    : public ColumnChunkReader<
          RequiredTypedRecordReader<DType, ValueSink, kReadDictionary>>,
      virtual public RecordReader {
 public:
  using T = typename DType::c_type;
  using Base =
      ColumnChunkReader<RequiredTypedRecordReader<DType, ValueSink, kReadDictionary>>;

  RequiredTypedRecordReader(const ColumnDescriptor* descr, MemoryPool* pool,
                            ValueSink value_sink)
      : Base(descr, pool, LevelDecoder(descr->max_definition_level()),
             LevelDecoder(descr->max_repetition_level())),
        value_sink_(std::move(value_sink)) {
    RequiredTypedRecordReader::Reset();
    ARROW_DCHECK_EQ(descr->max_definition_level(), 0);
    ARROW_DCHECK_EQ(descr->max_repetition_level(), 0);
  }

  uint8_t* values() const final { return reinterpret_cast<uint8_t*>(value_sink_.data()); }

  int64_t values_written() const final { return value_sink_.values_count(); }

  int16_t* def_levels() const final { return nullptr; }

  int16_t* rep_levels() const final { return nullptr; }

  int64_t levels_position() const final { return 0; }

  int64_t levels_written() const final { return 0; }

  int64_t null_count() const final { return 0; }

  bool nullable_values() const final { return false; }

  bool read_dictionary() const final { return kReadDictionary; }

  bool read_dense_for_nullable() const final { return false; }

  const void* ReadDictionary(int32_t* dictionary_length) final;

  int64_t ReadRecords(int64_t num_records) final;

  int64_t SkipRecords(int64_t num_records) final { return this->Skip(num_records); }

  std::shared_ptr<ResizableBuffer> ReleaseValues() final {
    return value_sink_.ReleaseValues(this->pool_);
  }

  std::shared_ptr<ResizableBuffer> ReleaseIsValid() final { return nullptr; }

  void Reserve(int64_t extra_values) final { ReserveValues(extra_values); }

  void Reset() final;

  void SetPageReader(std::unique_ptr<PageReader> reader) final {
    return Base::SetPageReader(std::move(reader));
  }

  bool HasMoreData() const override {
    return Base::HasPageReader();  // Surprising legacy behaviour
  }

  const ColumnDescriptor* descr() const final { return this->descr_; }

  void OnNewDictionary(DictDecoder<DType>& decoder) {
    value_sink_.OnNewDictionary(decoder);
  }

  void DebugPrintState() final;

 protected:
  auto value_sink() -> ValueSink& { return value_sink_; }
  auto value_sink() const -> const ValueSink& { return value_sink_; }

 private:
  ValueSink value_sink_;

  void ReserveValues(int64_t extra_values) { value_sink_.ReserveValues(extra_values); }

  void ReadValuesDense(int32_t values_to_read);
};

/**********************************************
 *  RequiredTypedRecordReader Implementation  *
 **********************************************/

template <typename DT, typename VS, bool kDic>
const void* RequiredTypedRecordReader<DT, VS, kDic>::ReadDictionary(
    int32_t* dictionary_length) {
  if (!this->current_decoder_ && !this->ProcessToMoreData()) {
    *dictionary_length = 0;
    return nullptr;
  }
  // Verify the current data page is dictionary encoded.
  this->CheckEncodingIs(Encoding::RLE_DICTIONARY);
  auto decoder = dynamic_cast<DictDecoder<DT>*>(this->current_decoder_.get());
  const T* dictionary = nullptr;
  decoder->GetDictionary(&dictionary, dictionary_length);
  return reinterpret_cast<const void*>(dictionary);
}

template <typename DT, typename VS, bool kDic>
int64_t RequiredTypedRecordReader<DT, VS, kDic>::ReadRecords(int64_t num_records) {
  if (num_records <= 0) {
    return 0;
  }

  Reserve(num_records);

  int64_t records_read = 0;

  do {
    // Is there more data to read in this row group?
    if (!this->ProcessToMoreData()) {
      break;
    }

    const int32_t batch_size =
        std::min<int32_t>(clamp_to<int32_t>(num_records - records_read),
                          this->available_values_current_page());
    ReadValuesDense(batch_size);
    this->ConsumeBufferedValues(batch_size);

    records_read += batch_size;
  } while (records_read < num_records);

  return records_read;
}

template <typename DT, typename VS, bool kDic>
void RequiredTypedRecordReader<DT, VS, kDic>::ReadValuesDense(int32_t batch_size) {
  const auto decoded =
      value_sink_.ReadValuesDense(*this->current_decoder_.get(), batch_size);
  CheckNumberDecoded(decoded, batch_size);
}

template <typename DT, typename VS, bool kDic>
void RequiredTypedRecordReader<DT, VS, kDic>::Reset() {
  if (values_written() > 0) {
    value_sink_.ResetValues();
  }
}

template <typename DT, typename VS, bool kDic>
void RequiredTypedRecordReader<DT, VS, kDic>::DebugPrintState() {
  std::cout << "values: ";
  if constexpr (can_cout<T>) {
    const T* vals = reinterpret_cast<const T*>(this->values());
    for (int64_t i = 0; i < this->values_written(); ++i) {
      std::cout << vals[i] << " ";
    }
  } else {
    for (int64_t i = 0; i < this->values_written(); ++i) {
      std::cout << "? ";
    }
  }
  std::cout << std::endl;
}

/*********************
 *  ArrayValuesSink  *
 *********************/

template <typename T, typename BuilderType>
class ArrayValuesSink : private ValueSinkCursor {
 public:
  using value_type = T;

  using ValueSinkCursor::capacity;
  using ValueSinkCursor::values_count;

  explicit ArrayValuesSink(BuilderType builder) : builder_{std::move(builder)} {}

  value_type* data() const { return nullptr; }

  void mark_values_as_written(int64_t extra_values) {
    set_values_count(values_count() + extra_values);
  }

  std::shared_ptr<ResizableBuffer> ReleaseValues(MemoryPool* /* pool */) {
    return nullptr;
  }

  void ReserveValues(int64_t extra_values) {
    fit_capacity_for_extra(extra_values);
    // The chunked accumulator is not an ArrayBuilder and cannot be reserved.
    if constexpr (requires { builder_.Reserve(extra_values); }) {
      PARQUET_THROW_NOT_OK(builder_.Reserve(extra_values));
    }
  }

  void ResetValues() { ValueSinkCursor::operator=({}); }

  void OnNewDictionary(auto& /* decoder */) {}

  [[nodiscard]] auto ReadValuesDense(auto& decoder, int32_t batch_size) {
    // TODO once we have a validity sink: we can reset the validity to save some space
    const int64_t decoded = decoder.DecodeArrowNonNull(batch_size, &builder_);
    mark_values_as_written(batch_size);
    return decoded;
  }

  [[nodiscard]] auto ReadValuesSpaced(auto& decoder, int32_t batch_size,
                                      int32_t null_count, const uint8_t* valid_bits,
                                      int64_t valid_bits_offset) {
    // TODO once we have a validity sink: we can reset the validity to save some space
    const int64_t decoded = decoder.DecodeArrow(batch_size, null_count, valid_bits,
                                                valid_bits_offset, &builder_);
    mark_values_as_written(batch_size);
    // `DecodeArrow` only counts the non-null values, but the caller expects the
    // number of values written, nulls included.
    ARROW_DCHECK_EQ(decoded, batch_size - null_count);
    return decoded + null_count;
  }

  auto builder() -> BuilderType& { return builder_; }
  auto builder() const -> const BuilderType& { return builder_; }

 private:
  BuilderType builder_;
};

/**********************
 *  FLBARecordReader  *
 **********************/

template <typename DType, typename ValueSink, bool kRequired, bool kReadDictionary>
using record_reader_base_t =
    std::conditional_t<kRequired,
                       RequiredTypedRecordReader<DType, ValueSink, kReadDictionary>,
                       TypedRecordReader<DType, ValueSink, kReadDictionary>>;

template <bool kRequired>
struct flba_record_reader_base {
  using DType = FLBAType;
  using c_type = typename DType::c_type;
  using Builder = ::arrow::FixedSizeBinaryBuilder;
  using ValueSink = ArrayValuesSink<c_type, Builder>;
  using type = record_reader_base_t<DType, ValueSink, kRequired, false>;
};

template <bool kRequired>
using flba_record_reader_base_t = typename flba_record_reader_base<kRequired>::type;

/// Reads fixed length byte array values into a FixedSizeBinaryBuilder.
///
/// `kRequired` selects the base class: RequiredTypedRecordReader for
/// required (non-nullable, non-repeated) columns, TypedRecordReader
/// otherwise.
///
/// Values are decoded directly into `array_builder_`; the `values_` buffer
/// of the base class is a ReadValuesNoBuffer and only tracks the number of
/// values written. The `valid_bits_` buffer, if any, is consumed by each
/// spaced decode.
template <bool kRequired>
class FLBARecordReader final : public flba_record_reader_base_t<kRequired>,
                               virtual public BinaryRecordReader {
 public:
  using Base = flba_record_reader_base_t<kRequired>;

  FLBARecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                   ::arrow::MemoryPool* pool, bool read_dense_for_nullable)
    requires(!kRequired)
      : Base(descr, leaf_info, pool, read_dense_for_nullable, MakeSink(descr, pool)) {
    ARROW_DCHECK_EQ(descr->physical_type(), Type::FIXED_LEN_BYTE_ARRAY);
  }

  FLBARecordReader(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool)
    requires(kRequired)
      : Base(descr, pool, MakeSink(descr, pool)) {
    ARROW_DCHECK_EQ(descr->physical_type(), Type::FIXED_LEN_BYTE_ARRAY);
  }

  ::arrow::ArrayVector GetBuilderChunks() override {
    PARQUET_ASSIGN_OR_THROW(auto chunk, builder().Finish());
    return ::arrow::ArrayVector{std::move(chunk)};
  }

 private:
  using Builder = typename flba_record_reader_base<kRequired>::Builder;
  using ValueSink = typename flba_record_reader_base<kRequired>::ValueSink;

  static auto MakeSink(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool) {
    return ValueSink(Builder(::arrow::fixed_size_binary(descr->type_length()), pool));
  }

  auto builder() -> Builder& { return this->value_sink().builder(); }
};

/***************************
 *  ByteArrayRecordReader  *
 ***************************/

/// Create the Arrow builder for reading a Parquet BYTE_ARRAY column as the
/// given Arrow type (BINARY by default).
std::unique_ptr<::arrow::ArrayBuilder> MakeByteArrayBuilder(::arrow::DataType* arrow_type,
                                                            ::arrow::MemoryPool* pool) {
  auto arrow_binary_type = arrow_type ? arrow_type->id() : ::arrow::Type::BINARY;
  switch (arrow_binary_type) {
    case ::arrow::Type::BINARY:
      return std::make_unique<::arrow::BinaryBuilder>(pool);
    case ::arrow::Type::STRING:
      return std::make_unique<::arrow::StringBuilder>(pool);
    case ::arrow::Type::LARGE_BINARY:
      return std::make_unique<::arrow::LargeBinaryBuilder>(pool);
    case ::arrow::Type::LARGE_STRING:
      return std::make_unique<::arrow::LargeStringBuilder>(pool);
    case ::arrow::Type::BINARY_VIEW:
      return std::make_unique<::arrow::BinaryViewBuilder>(pool);
    case ::arrow::Type::STRING_VIEW:
      return std::make_unique<::arrow::StringViewBuilder>(pool);
    default:
      throw ParquetException("cannot read Parquet BYTE_ARRAY as Arrow " +
                             arrow_type->ToString());
  }
}

template <bool kRequired>
struct byte_array_chunked_record_reader {
  using DType = ByteArrayType;
  using c_type = typename DType::c_type;
  using Builder = typename EncodingTraits<ByteArrayType>::Accumulator;
  using ValueSink = ArrayValuesSink<c_type, Builder>;
  using type = record_reader_base_t<DType, ValueSink, kRequired, false>;
};

template <bool kRequired>
using byte_array_chunked_record_reader_t =
    typename byte_array_chunked_record_reader<kRequired>::type;

/// Reads variable length byte array values into a chunked binary builder.
///
/// `kRequired` selects the base class: RequiredTypedRecordReader for
/// required (non-nullable, non-repeated) columns, TypedRecordReader
/// otherwise.
///
/// It only calls `DecodeArrowNonNull` and `DecodeArrow` to read values, and
/// `Decode` and `DecodeSpaced` are not used.
///
/// The `values_` buffers are never used, and the `accumulator_`
/// is used to store the values.
template <bool kRequired>
class ByteArrayChunkedRecordReader final
    : public byte_array_chunked_record_reader_t<kRequired>,
      virtual public BinaryRecordReader {
 public:
  using Base = byte_array_chunked_record_reader_t<kRequired>;

  ByteArrayChunkedRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                               ::arrow::MemoryPool* pool, bool read_dense_for_nullable,
                               const std::shared_ptr<::arrow::DataType>& arrow_type)
    requires(!kRequired)
      : Base(descr, leaf_info, pool, read_dense_for_nullable,
             MakeSink(pool, arrow_type)) {
    ARROW_DCHECK_EQ(descr->physical_type(), Type::BYTE_ARRAY);
  }

  ByteArrayChunkedRecordReader(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool,
                               const std::shared_ptr<::arrow::DataType>& arrow_type)
    requires(kRequired)
      : Base(descr, pool, MakeSink(pool, arrow_type)) {
    ARROW_DCHECK_EQ(descr->physical_type(), Type::BYTE_ARRAY);
  }

  ::arrow::ArrayVector GetBuilderChunks() override {
    ::arrow::ArrayVector result = accumulator().chunks;
    if (result.empty() || accumulator().builder->length() > 0) {
      std::shared_ptr<::arrow::Array> last_chunk;
      PARQUET_THROW_NOT_OK(accumulator().builder->Finish(&last_chunk));
      result.push_back(std::move(last_chunk));
    }
    accumulator().chunks = {};
    return result;
  }

 private:
  using Builder = typename byte_array_chunked_record_reader<kRequired>::Builder;
  using ValueSink = typename byte_array_chunked_record_reader<kRequired>::ValueSink;

  static auto MakeSink(::arrow::MemoryPool* pool,
                       const std::shared_ptr<::arrow::DataType>& arrow_type) {
    Builder accumulator = {};
    accumulator.builder = MakeByteArrayBuilder(arrow_type.get(), pool);
    return ValueSink(std::move(accumulator));
  }

  auto accumulator() -> Builder& { return this->value_sink().builder(); }
};

/// Decodes byte array values into a ::arrow::BinaryDictionary32Builder.
///
/// If the current decoder is dictionary encoded, the dictionary indices are
/// decoded directly, otherwise the values are decoded and looked up in the
/// builder's memo table.
class ByteArrayDictionaryValuesSink : private ValueSinkCursor {
 public:
  using value_type = ByteArray;
  using Builder = ::arrow::BinaryDictionary32Builder;

  using ValueSinkCursor::capacity;
  using ValueSinkCursor::values_count;

  explicit ByteArrayDictionaryValuesSink(::arrow::MemoryPool* pool)
      : builder_(std::make_unique<Builder>(pool)) {}

  value_type* data() const { return nullptr; }

  void mark_values_as_written(int64_t extra_values) {}

  std::shared_ptr<ResizableBuffer> ReleaseValues(MemoryPool* /* pool */) {
    return nullptr;
  }

  void ReserveValues(int64_t extra_values) { fit_capacity_for_extra(extra_values); }

  void ResetValues() { ValueSinkCursor::operator=({}); }

  [[nodiscard]] int64_t ReadValuesDense(auto& decoder, int32_t batch_size) {
    // TODO once we have a validity sink: we can reset the validity to save some space
    const int64_t decoded = [&]() -> int64_t {
      if (auto* dict_decoder = dynamic_cast<BinaryDictDecoder*>(&decoder)) {
        return dict_decoder->DecodeIndices(batch_size, builder_.get());
      }
      return decoder.DecodeArrowNonNull(batch_size, builder_.get());
    }();
    set_values_count(values_count() + batch_size);
    return decoded;
  }

  [[nodiscard]] int64_t ReadValuesSpaced(auto& decoder, int32_t batch_size,
                                         int32_t null_count, const uint8_t* valid_bits,
                                         int64_t valid_bits_offset) {
    // TODO once we have a validity sink: we can reset the validity to save some space
    const int64_t decoded = [&]() -> int64_t {
      if (auto* dict_decoder = dynamic_cast<BinaryDictDecoder*>(&decoder)) {
        return dict_decoder->DecodeIndicesSpaced(batch_size, null_count, valid_bits,
                                                 valid_bits_offset, builder_.get());
      }
      return decoder.DecodeArrow(batch_size, null_count, valid_bits, valid_bits_offset,
                                 builder_.get());
    }();
    set_values_count(values_count() + batch_size);
    // The decoders only count the non-null values, but the caller expects the
    // number of values written, nulls included.
    ARROW_DCHECK_EQ(decoded, batch_size - null_count);
    return decoded + null_count;
  }

  void OnNewDictionary(auto& decoder) {
    // The indices accumulated so far refer to the previous dictionary
    FlushBuilder();
    builder_->ResetFull();
    decoder.InsertDictionary(builder_.get());
  }

  /// Finish the builder and return all the chunks accumulated so far.
  std::shared_ptr<::arrow::ChunkedArray> FlushChunks() {
    FlushBuilder();
    return std::make_shared<::arrow::ChunkedArray>(std::exchange(result_chunks_, {}),
                                                   builder_->type());
  }

 private:
  using BinaryDictDecoder = DictDecoder<ByteArrayType>;

  std::unique_ptr<Builder> builder_;
  std::vector<std::shared_ptr<::arrow::Array>> result_chunks_;

  void FlushBuilder() {
    if (builder_->length() > 0) {
      std::shared_ptr<::arrow::Array> chunk;
      PARQUET_THROW_NOT_OK(builder_->Finish(&chunk));
      result_chunks_.emplace_back(std::move(chunk));

      // Partial reset: the dictionary memo table is kept, so that the indices
      // appended to the next chunk keep referring to the same values.
      builder_->Reset();
    }
  }
};

template <bool kRequired>
struct byte_array_dictionary_record_reader {
  using DType = ByteArrayType;
  using ValueSink = ByteArrayDictionaryValuesSink;
  using type =
      record_reader_base_t<DType, ValueSink, kRequired, /*kReadDictionary=*/true>;
};

template <bool kRequired>
using byte_array_dictionary_record_reader_t =
    typename byte_array_dictionary_record_reader<kRequired>::type;

/// Reads byte array values into ::arrow::dictionary(index: int32, values: binary).
///
/// `kRequired` selects the base class: RequiredTypedRecordReader for
/// required (non-nullable, non-repeated) columns, TypedRecordReader
/// otherwise.
///
/// The `values_` buffers are never used, the values are stored in the
/// dictionary builder held by the value sink.
template <bool kRequired>
class ByteArrayDictionaryRecordReader final
    : public byte_array_dictionary_record_reader_t<kRequired>,
      virtual public DictionaryRecordReader {
 public:
  using Base = byte_array_dictionary_record_reader_t<kRequired>;

  ByteArrayDictionaryRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info,
                                  ::arrow::MemoryPool* pool, bool read_dense_for_nullable)
    requires(!kRequired)
      : Base(descr, leaf_info, pool, read_dense_for_nullable, ValueSink(pool)) {
    ARROW_DCHECK_EQ(descr->physical_type(), Type::BYTE_ARRAY);
  }

  ByteArrayDictionaryRecordReader(const ColumnDescriptor* descr,
                                  ::arrow::MemoryPool* pool)
    requires(kRequired)
      : Base(descr, pool, ValueSink(pool)) {
    ARROW_DCHECK_EQ(descr->physical_type(), Type::BYTE_ARRAY);
    ARROW_DCHECK_EQ(descr->max_definition_level(), 0);
    ARROW_DCHECK_EQ(descr->max_repetition_level(), 0);
  }

  std::shared_ptr<::arrow::ChunkedArray> GetResult() override {
    return this->value_sink().FlushChunks();
  }

 private:
  using ValueSink = typename byte_array_dictionary_record_reader<kRequired>::ValueSink;
};

std::shared_ptr<RecordReader> MakeByteArrayRecordReader(
    const ColumnDescriptor* descr, LevelInfo leaf_info, ::arrow::MemoryPool* pool,
    bool read_dictionary, bool read_dense_for_nullable,
    const std::shared_ptr<::arrow::DataType>& arrow_type) {
  const bool required =
      descr->max_definition_level() == 0 && descr->max_repetition_level() == 0;
  if (read_dictionary) {
    if (required) {
      using RequiredReader = ByteArrayDictionaryRecordReader</*kRequired=*/true>;

      return std::make_shared<RequiredReader>(descr, pool);
    }
    using Reader = ByteArrayDictionaryRecordReader</*kRequired=*/false>;
    return std::make_shared<Reader>(descr, leaf_info, pool, read_dense_for_nullable);
  } else {
    if (required) {
      using RequiredReader = ByteArrayChunkedRecordReader</*kRequired=*/true>;

      return std::make_shared<RequiredReader>(descr, pool, arrow_type);
    }
    using Reader = ByteArrayChunkedRecordReader</*kRequired=*/false>;
    return std::make_shared<Reader>(descr, leaf_info, pool, read_dense_for_nullable,
                                    arrow_type);
  }
}

}  // namespace

namespace {
template <typename DType>
std::shared_ptr<RecordReader> DispatchTypedRecordReader(const ColumnDescriptor* descr,
                                                        LevelInfo leaf_info,
                                                        MemoryPool* pool,
                                                        bool read_dense_for_nullable) {
  if (descr->max_definition_level() == 0 && descr->max_repetition_level() == 0) {
    if constexpr (std::is_same_v<DType, FLBAType>) {
      using FLBAReader = FLBARecordReader</*kRequired=*/true>;
      return std::make_shared<FLBAReader>(descr, pool);
    } else {
      using c_type = typename DType::c_type;
      using ValueSink = ValueSinkBuffer<c_type>;
      using Reader = RequiredTypedRecordReader<DType, ValueSink, false>;
      return std::make_shared<Reader>(descr, pool, ValueSink(pool));
    }
  }
  if constexpr (std::is_same_v<DType, FLBAType>) {
    using FLBAReader = FLBARecordReader</*kRequired=*/false>;
    return std::make_shared<FLBAReader>(descr, leaf_info, pool, read_dense_for_nullable);
  } else {
    using c_type = typename DType::c_type;
    using ValueSink = ValueSinkBuffer<c_type>;
    using Reader = TypedRecordReader<DType, ValueSink, false>;
    return std::make_shared<Reader>(descr, leaf_info, pool, read_dense_for_nullable,
                                    ValueSink(pool));
  }
}
}  // namespace

std::shared_ptr<RecordReader> RecordReader::Make(
    const ColumnDescriptor* descr, LevelInfo leaf_info, MemoryPool* pool,
    bool read_dictionary, bool read_dense_for_nullable,
    const std::shared_ptr<::arrow::DataType>& arrow_type) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return DispatchTypedRecordReader<BooleanType>(descr, leaf_info, pool,
                                                    read_dense_for_nullable);
    case Type::INT32:
      return DispatchTypedRecordReader<Int32Type>(descr, leaf_info, pool,
                                                  read_dense_for_nullable);
    case Type::INT64:
      return DispatchTypedRecordReader<Int64Type>(descr, leaf_info, pool,
                                                  read_dense_for_nullable);
    case Type::INT96:
      return DispatchTypedRecordReader<Int96Type>(descr, leaf_info, pool,
                                                  read_dense_for_nullable);
    case Type::FLOAT:
      return DispatchTypedRecordReader<FloatType>(descr, leaf_info, pool,
                                                  read_dense_for_nullable);
    case Type::DOUBLE:
      return DispatchTypedRecordReader<DoubleType>(descr, leaf_info, pool,
                                                   read_dense_for_nullable);
    case Type::BYTE_ARRAY: {
      return MakeByteArrayRecordReader(descr, leaf_info, pool, read_dictionary,
                                       read_dense_for_nullable, arrow_type);
    }
    case Type::FIXED_LEN_BYTE_ARRAY:
      return DispatchTypedRecordReader<FLBAType>(descr, leaf_info, pool,
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

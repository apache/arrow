// licensed to the Apache Software Foundation (ASF) under one
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

#include "parquet/arrow/record_reader.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sstream>

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/util/bit-util.h>
#include <arrow/util/rle-encoding.h>

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/encoding-internal.h"
#include "parquet/exception.h"
#include "parquet/properties.h"

using arrow::MemoryPool;

namespace parquet {
namespace internal {

namespace BitUtil = ::arrow::BitUtil;

template <typename DType>
class TypedRecordReader;

// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
static bool IsDictionaryIndexEncoding(const Encoding::type& e) {
  return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
}

class RecordReader::RecordReaderImpl {
 public:
  RecordReaderImpl(const ColumnDescriptor* descr, MemoryPool* pool)
      : descr_(descr),
        pool_(pool),
        num_buffered_values_(0),
        num_decoded_values_(0),
        max_def_level_(descr->max_definition_level()),
        max_rep_level_(descr->max_repetition_level()),
        at_record_start_(false),
        records_read_(0),
        values_written_(0),
        values_capacity_(0),
        null_count_(0),
        levels_written_(0),
        levels_position_(0),
        levels_capacity_(0) {
    nullable_values_ = internal::HasSpacedValues(descr);
    values_ = std::make_shared<PoolBuffer>(pool);
    valid_bits_ = std::make_shared<PoolBuffer>(pool);
    def_levels_ = std::make_shared<PoolBuffer>(pool);
    rep_levels_ = std::make_shared<PoolBuffer>(pool);

    if (descr->physical_type() == Type::BYTE_ARRAY) {
      builder_.reset(new ::arrow::BinaryBuilder(pool));
    } else if (descr->physical_type() == Type::FIXED_LEN_BYTE_ARRAY) {
      int byte_width = descr->type_length();
      std::shared_ptr<::arrow::DataType> type = ::arrow::fixed_size_binary(byte_width);
      builder_.reset(new ::arrow::FixedSizeBinaryBuilder(type, pool));
    }
    Reset();
  }

  virtual ~RecordReaderImpl() = default;

  virtual int64_t ReadRecords(int64_t num_records) = 0;

  // Dictionary decoders must be reset when advancing row groups
  virtual void ResetDecoders() = 0;

  void SetPageReader(std::unique_ptr<PageReader> reader) {
    pager_ = std::move(reader);
    ResetDecoders();
  }

  bool HasMoreData() const { return pager_ != nullptr; }

  int16_t* def_levels() const {
    return reinterpret_cast<int16_t*>(def_levels_->mutable_data());
  }

  int16_t* rep_levels() {
    return reinterpret_cast<int16_t*>(rep_levels_->mutable_data());
  }

  uint8_t* values() const { return values_->mutable_data(); }

  /// \brief Number of values written including nulls (if any)
  int64_t values_written() const { return values_written_; }

  int64_t levels_position() const { return levels_position_; }
  int64_t levels_written() const { return levels_written_; }

  // We may outwardly have the appearance of having exhausted a column chunk
  // when in fact we are in the middle of processing the last batch
  bool has_values_to_process() const { return levels_position_ < levels_written_; }

  int64_t null_count() const { return null_count_; }

  bool nullable_values() const { return nullable_values_; }

  std::shared_ptr<PoolBuffer> ReleaseValues() {
    auto result = values_;
    values_ = std::make_shared<PoolBuffer>(pool_);
    return result;
  }

  std::shared_ptr<PoolBuffer> ReleaseIsValid() {
    auto result = valid_bits_;
    valid_bits_ = std::make_shared<PoolBuffer>(pool_);
    return result;
  }

  ::arrow::ArrayBuilder* builder() { return builder_.get(); }

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

    DCHECK_GT(max_rep_level_, 0);

    // Count logical records and number of values to read
    while (levels_position_ < levels_written_) {
      if (*rep_levels++ == 0) {
        at_record_start_ = true;
        if (records_read == num_records) {
          // We've found the number of records we were looking for
          break;
        } else {
          // Continue
          ++records_read;
        }
      } else {
        at_record_start_ = false;
      }
      if (*def_levels++ == max_def_level_) {
        ++values_to_read;
      }
      ++levels_position_;
    }
    *values_seen = values_to_read;
    return records_read;
  }

  // Read multiple definition levels into preallocated memory
  //
  // Returns the number of decoded definition levels
  int64_t ReadDefinitionLevels(int64_t batch_size, int16_t* levels) {
    if (descr_->max_definition_level() == 0) {
      return 0;
    }
    return definition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
  }

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

  Type::type type() const { return descr_->physical_type(); }

  const ColumnDescriptor* descr() const { return descr_; }

  void Reserve(int64_t capacity) {
    ReserveLevels(capacity);
    ReserveValues(capacity);
  }

  void ReserveLevels(int64_t capacity) {
    if (descr_->max_definition_level() > 0 &&
        (levels_written_ + capacity > levels_capacity_)) {
      int64_t new_levels_capacity = BitUtil::NextPower2(levels_capacity_ + 1);
      while (levels_written_ + capacity > new_levels_capacity) {
        new_levels_capacity = BitUtil::NextPower2(new_levels_capacity + 1);
      }
      PARQUET_THROW_NOT_OK(
          def_levels_->Resize(new_levels_capacity * sizeof(int16_t), false));
      if (descr_->max_repetition_level() > 0) {
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

      int type_size = GetTypeByteSize(descr_->physical_type());
      PARQUET_THROW_NOT_OK(values_->Resize(new_values_capacity * type_size, false));
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

  void Reset() {
    ResetValues();

    if (levels_written_ > 0) {
      const int64_t levels_remaining = levels_written_ - levels_position_;
      // Shift remaining levels to beginning of buffer and trim to only the number
      // of decoded levels remaining
      int16_t* def_data = def_levels();
      int16_t* rep_data = rep_levels();

      std::copy(def_data + levels_position_, def_data + levels_written_, def_data);
      std::copy(rep_data + levels_position_, rep_data + levels_written_, rep_data);

      PARQUET_THROW_NOT_OK(
          def_levels_->Resize(levels_remaining * sizeof(int16_t), false));
      PARQUET_THROW_NOT_OK(
          rep_levels_->Resize(levels_remaining * sizeof(int16_t), false));

      levels_written_ -= levels_position_;
      levels_position_ = 0;
      levels_capacity_ = levels_remaining;
    }

    records_read_ = 0;

    // Calling Finish on the builders also resets them
  }

  void ResetValues() {
    if (values_written_ > 0) {
      // Resize to 0, but do not shrink to fit
      PARQUET_THROW_NOT_OK(values_->Resize(0, false));
      PARQUET_THROW_NOT_OK(valid_bits_->Resize(0, false));
      values_written_ = 0;
      values_capacity_ = 0;
      null_count_ = 0;
    }
  }

 protected:
  const ColumnDescriptor* descr_;
  ::arrow::MemoryPool* pool_;

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

  const int16_t max_def_level_;
  const int16_t max_rep_level_;

  bool nullable_values_;

  bool at_record_start_;
  int64_t records_read_;

  int64_t values_written_;
  int64_t values_capacity_;
  int64_t null_count_;

  int64_t levels_written_;
  int64_t levels_position_;
  int64_t levels_capacity_;

  // TODO(wesm): ByteArray / FixedLenByteArray types
  std::unique_ptr<::arrow::ArrayBuilder> builder_;

  std::shared_ptr<::arrow::PoolBuffer> values_;

  template <typename T>
  T* ValuesHead() {
    return reinterpret_cast<T*>(values_->mutable_data()) + values_written_;
  }

  std::shared_ptr<::arrow::PoolBuffer> valid_bits_;
  std::shared_ptr<::arrow::PoolBuffer> def_levels_;
  std::shared_ptr<::arrow::PoolBuffer> rep_levels_;
};

// The minimum number of repetition/definition levels to decode at a time, for
// better vectorized performance when doing many smaller record reads
constexpr int64_t kMinLevelBatchSize = 1024;

template <typename DType>
class TypedRecordReader : public RecordReader::RecordReaderImpl {
 public:
  typedef typename DType::c_type T;

  ~TypedRecordReader() {}

  TypedRecordReader(const ColumnDescriptor* schema, ::arrow::MemoryPool* pool)
      : RecordReader::RecordReaderImpl(schema, pool), current_decoder_(nullptr) {}

  void ResetDecoders() override { decoders_.clear(); }

  inline void ReadValuesSpaced(int64_t values_with_nulls, int64_t null_count) {
    uint8_t* valid_bits = valid_bits_->mutable_data();
    const int64_t valid_bits_offset = values_written_;

    int64_t num_decoded = current_decoder_->DecodeSpaced(
        ValuesHead<T>(), static_cast<int>(values_with_nulls),
        static_cast<int>(null_count), valid_bits, valid_bits_offset);
    DCHECK_EQ(num_decoded, values_with_nulls);
  }

  inline void ReadValuesDense(int64_t values_to_read) {
    int64_t num_decoded =
        current_decoder_->Decode(ValuesHead<T>(), static_cast<int>(values_to_read));
    DCHECK_EQ(num_decoded, values_to_read);
  }

  // Return number of logical records read
  int64_t ReadRecordData(const int64_t num_records) {
    // Conservative upper bound
    const int64_t possible_num_values =
        std::max(num_records, levels_written_ - levels_position_);
    ReserveValues(possible_num_values);

    const int64_t start_levels_position = levels_position_;

    int64_t values_to_read = 0;
    int64_t records_read = 0;
    if (max_rep_level_ > 0) {
      records_read = DelimitRecords(num_records, &values_to_read);
    } else if (max_def_level_ > 0) {
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
          max_def_level_, max_rep_level_, &values_with_nulls, &null_count,
          valid_bits_->mutable_data(), values_written_);
      values_to_read = values_with_nulls - null_count;
      ReadValuesSpaced(values_with_nulls, null_count);
      ConsumeBufferedValues(levels_position_ - start_levels_position);
    } else {
      ReadValuesDense(values_to_read);
      ConsumeBufferedValues(values_to_read);
    }
    // Total values, including null spaces, if any
    values_written_ += values_to_read + null_count;
    null_count_ += null_count;

    return records_read;
  }

  // Returns true if there are still values in this column.
  bool HasNext() {
    // Either there is no data page available yet, or the data page has been
    // exhausted
    if (num_buffered_values_ == 0 || num_decoded_values_ == num_buffered_values_) {
      if (!ReadNewPage() || num_buffered_values_ == 0) {
        return false;
      }
    }
    return true;
  }

  int64_t ReadRecords(int64_t num_records) override {
    // Delimit records, then read values at the end
    int64_t records_read = 0;

    if (levels_position_ < levels_written_) {
      records_read += ReadRecordData(num_records);
    }

    // HasNext invokes ReadNewPage
    if (records_read == 0 && !HasNext()) {
      return 0;
    }

    int64_t level_batch_size = std::max(kMinLevelBatchSize, num_records);

    // If we are in the middle of a record, we continue until reaching the
    // desired number of records or the end of the current record if we've found
    // enough records
    while (!at_record_start_ || records_read < num_records) {
      // Is there more data to read in this row group?
      if (!HasNext()) {
        break;
      }

      /// We perform multiple batch reads until we either exhaust the row group
      /// or observe the desired number of records
      int64_t batch_size = std::min(level_batch_size, available_values_current_page());

      // No more data in column
      if (batch_size == 0) {
        break;
      }

      if (max_def_level_ > 0) {
        ReserveLevels(batch_size);

        int16_t* def_levels = this->def_levels() + levels_written_;
        int16_t* rep_levels = this->rep_levels() + levels_written_;

        // Not present for non-repeated fields
        int64_t levels_read = 0;
        if (max_rep_level_ > 0) {
          levels_read = ReadDefinitionLevels(batch_size, def_levels);
          if (ReadRepetitionLevels(batch_size, rep_levels) != levels_read) {
            throw ParquetException("Number of decoded rep / def levels did not match");
          }
        } else if (max_def_level_ > 0) {
          levels_read = ReadDefinitionLevels(batch_size, def_levels);
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

 private:
  typedef Decoder<DType> DecoderType;

  // Map of encoding type to the respective decoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::shared_ptr<DecoderType>> decoders_;

  DecoderType* current_decoder_;

  // Advance to the next data page
  bool ReadNewPage();

  void ConfigureDictionary(const DictionaryPage* page);
};

template <>
inline void TypedRecordReader<ByteArrayType>::ReadValuesDense(int64_t values_to_read) {
  auto values = ValuesHead<ByteArray>();
  int64_t num_decoded =
      current_decoder_->Decode(values, static_cast<int>(values_to_read));
  DCHECK_EQ(num_decoded, values_to_read);

  auto builder = static_cast<::arrow::BinaryBuilder*>(builder_.get());
  for (int64_t i = 0; i < num_decoded; i++) {
    PARQUET_THROW_NOT_OK(
        builder->Append(values[i].ptr, static_cast<int64_t>(values[i].len)));
  }
  ResetValues();
}

template <>
inline void TypedRecordReader<FLBAType>::ReadValuesDense(int64_t values_to_read) {
  auto values = ValuesHead<FLBA>();
  int64_t num_decoded =
      current_decoder_->Decode(values, static_cast<int>(values_to_read));
  DCHECK_EQ(num_decoded, values_to_read);

  auto builder = static_cast<::arrow::FixedSizeBinaryBuilder*>(builder_.get());
  for (int64_t i = 0; i < num_decoded; i++) {
    PARQUET_THROW_NOT_OK(builder->Append(values[i].ptr));
  }
  ResetValues();
}

template <>
inline void TypedRecordReader<ByteArrayType>::ReadValuesSpaced(int64_t values_to_read,
                                                               int64_t null_count) {
  uint8_t* valid_bits = valid_bits_->mutable_data();
  const int64_t valid_bits_offset = values_written_;
  auto values = ValuesHead<ByteArray>();

  int64_t num_decoded = current_decoder_->DecodeSpaced(
      values, static_cast<int>(values_to_read), static_cast<int>(null_count), valid_bits,
      valid_bits_offset);
  DCHECK_EQ(num_decoded, values_to_read);

  auto builder = static_cast<::arrow::BinaryBuilder*>(builder_.get());

  for (int64_t i = 0; i < num_decoded; i++) {
    if (::arrow::BitUtil::GetBit(valid_bits, valid_bits_offset + i)) {
      PARQUET_THROW_NOT_OK(
          builder->Append(values[i].ptr, static_cast<int64_t>(values[i].len)));
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  }
  ResetValues();
}

template <>
inline void TypedRecordReader<FLBAType>::ReadValuesSpaced(int64_t values_to_read,
                                                          int64_t null_count) {
  uint8_t* valid_bits = valid_bits_->mutable_data();
  const int64_t valid_bits_offset = values_written_;
  auto values = ValuesHead<FLBA>();

  int64_t num_decoded = current_decoder_->DecodeSpaced(
      values, static_cast<int>(values_to_read), static_cast<int>(null_count), valid_bits,
      valid_bits_offset);
  DCHECK_EQ(num_decoded, values_to_read);

  auto builder = static_cast<::arrow::FixedSizeBinaryBuilder*>(builder_.get());
  for (int64_t i = 0; i < num_decoded; i++) {
    if (::arrow::BitUtil::GetBit(valid_bits, valid_bits_offset + i)) {
      PARQUET_THROW_NOT_OK(builder->Append(values[i].ptr));
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  }
  ResetValues();
}

template <typename DType>
inline void TypedRecordReader<DType>::ConfigureDictionary(const DictionaryPage* page) {
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
    PlainDecoder<DType> dictionary(descr_);
    dictionary.SetData(page->num_values(), page->data(), page->size());

    // The dictionary is fully decoded during DictionaryDecoder::Init, so the
    // DictionaryPage buffer is no longer required after this step
    //
    // TODO(wesm): investigate whether this all-or-nothing decoding of the
    // dictionary makes sense and whether performance can be improved

    auto decoder = std::make_shared<DictionaryDecoder<DType>>(descr_, pool_);
    decoder->SetDict(&dictionary);
    decoders_[encoding] = decoder;
  } else {
    ParquetException::NYI("only plain dictionary encoding has been implemented");
  }

  current_decoder_ = decoders_[encoding].get();
}

template <typename DType>
bool TypedRecordReader<DType>::ReadNewPage() {
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
      const DataPage* page = static_cast<const DataPage*>(current_page_.get());

      // Read a data page.
      num_buffered_values_ = page->num_values();

      // Have not decoded any values from the data page yet
      num_decoded_values_ = 0;

      buffer = page->data();

      // If the data page includes repetition and definition levels, we
      // initialize the level decoder and subtract the encoded level bytes from
      // the page size to determine the number of bytes in the encoded data.
      int64_t data_size = page->size();

      // Data page Layout: Repetition Levels - Definition Levels - encoded values.
      // Levels are encoded as rle or bit-packed.
      // Init repetition levels
      if (descr_->max_repetition_level() > 0) {
        int64_t rep_levels_bytes = repetition_level_decoder_.SetData(
            page->repetition_level_encoding(), descr_->max_repetition_level(),
            static_cast<int>(num_buffered_values_), buffer);
        buffer += rep_levels_bytes;
        data_size -= rep_levels_bytes;
      }
      // TODO figure a way to set max_definition_level_ to 0
      // if the initial value is invalid

      // Init definition levels
      if (descr_->max_definition_level() > 0) {
        int64_t def_levels_bytes = definition_level_decoder_.SetData(
            page->definition_level_encoding(), descr_->max_definition_level(),
            static_cast<int>(num_buffered_values_), buffer);
        buffer += def_levels_bytes;
        data_size -= def_levels_bytes;
      }

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      Encoding::type encoding = page->encoding();

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
            std::shared_ptr<DecoderType> decoder(new PlainDecoder<DType>(descr_));
            decoders_[static_cast<int>(encoding)] = decoder;
            current_decoder_ = decoder.get();
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

std::shared_ptr<RecordReader> RecordReader::Make(const ColumnDescriptor* descr,
                                                 MemoryPool* pool) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::shared_ptr<RecordReader>(
          new RecordReader(new TypedRecordReader<BooleanType>(descr, pool)));
    case Type::INT32:
      return std::shared_ptr<RecordReader>(
          new RecordReader(new TypedRecordReader<Int32Type>(descr, pool)));
    case Type::INT64:
      return std::shared_ptr<RecordReader>(
          new RecordReader(new TypedRecordReader<Int64Type>(descr, pool)));
    case Type::INT96:
      return std::shared_ptr<RecordReader>(
          new RecordReader(new TypedRecordReader<Int96Type>(descr, pool)));
    case Type::FLOAT:
      return std::shared_ptr<RecordReader>(
          new RecordReader(new TypedRecordReader<FloatType>(descr, pool)));
    case Type::DOUBLE:
      return std::shared_ptr<RecordReader>(
          new RecordReader(new TypedRecordReader<DoubleType>(descr, pool)));
    case Type::BYTE_ARRAY:
      return std::shared_ptr<RecordReader>(
          new RecordReader(new TypedRecordReader<ByteArrayType>(descr, pool)));
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::shared_ptr<RecordReader>(
          new RecordReader(new TypedRecordReader<FLBAType>(descr, pool)));
    default:
      DCHECK(false);
  }
  // Unreachable code, but supress compiler warning
  return nullptr;
}

// ----------------------------------------------------------------------
// Implement public API

RecordReader::RecordReader(RecordReaderImpl* impl) { impl_.reset(impl); }

RecordReader::~RecordReader() {}

int64_t RecordReader::ReadRecords(int64_t num_records) {
  return impl_->ReadRecords(num_records);
}

void RecordReader::Reset() { return impl_->Reset(); }

void RecordReader::Reserve(int64_t num_values) { impl_->Reserve(num_values); }

const int16_t* RecordReader::def_levels() const { return impl_->def_levels(); }

const int16_t* RecordReader::rep_levels() const { return impl_->rep_levels(); }

const uint8_t* RecordReader::values() const { return impl_->values(); }

std::shared_ptr<PoolBuffer> RecordReader::ReleaseValues() {
  return impl_->ReleaseValues();
}

std::shared_ptr<PoolBuffer> RecordReader::ReleaseIsValid() {
  return impl_->ReleaseIsValid();
}

::arrow::ArrayBuilder* RecordReader::builder() { return impl_->builder(); }

int64_t RecordReader::values_written() const { return impl_->values_written(); }

int64_t RecordReader::levels_position() const { return impl_->levels_position(); }

int64_t RecordReader::levels_written() const { return impl_->levels_written(); }

int64_t RecordReader::null_count() const { return impl_->null_count(); }

bool RecordReader::nullable_values() const { return impl_->nullable_values(); }

bool RecordReader::HasMoreData() const { return impl_->HasMoreData(); }

void RecordReader::SetPageReader(std::unique_ptr<PageReader> reader) {
  impl_->SetPageReader(std::move(reader));
}

}  // namespace internal
}  // namespace parquet

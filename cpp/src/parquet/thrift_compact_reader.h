// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "generated/parquet_types.h"
#include "parquet/direct_metadata.h"
#include "parquet/exception.h"

namespace parquet {
namespace internal {

enum class CompactType : uint8_t {
  Stop = 0,
  BoolTrue = 1,
  BoolFalse = 2,
  Byte = 3,
  I16 = 4,
  I32 = 5,
  I64 = 6,
  Double = 7,
  Binary = 8,
  List = 9,
  Set = 10,
  Map = 11,
  Struct = 12,
  Uuid = 13,
};

struct CompactField {
  CompactType type;
  int16_t id;
  bool bool_value;
};

class CompactInput {
 public:
  CompactInput(const uint8_t* data, int64_t length, bool skip_column_stats = false,
               bool skip_encoding_stats = false, bool skip_size_stats = false,
               const std::vector<int32_t>* statistics_columns = nullptr,
               bool skip_page_stats = false, bool encoding_stats_as_mask = false)
      : begin_(data),
        current_(data),
        end_(data == nullptr ? data : data + length),
        skip_column_stats_(skip_column_stats),
        skip_encoding_stats_(skip_encoding_stats),
        skip_size_stats_(skip_size_stats),
        statistics_columns_(statistics_columns),
        skip_page_stats_(skip_page_stats),
        encoding_stats_as_mask_(encoding_stats_as_mask) {
    if (length < 0 || (length != 0 && data == nullptr)) {
      throw ParquetException("Invalid Thrift compact buffer");
    }
  }

  int64_t position() const { return current_ - begin_; }
  bool skip_column_stats() const { return skip_column_stats_; }
  bool skip_encoding_stats() const { return skip_encoding_stats_; }
  bool skip_size_stats() const { return skip_size_stats_; }
  bool skip_page_stats() const { return skip_page_stats_; }
  bool encoding_stats_as_mask() const { return encoding_stats_as_mask_; }
  void SetColumnIndex(int32_t index) { column_index_ = index; }
  bool skip_for_column() const {
    return statistics_columns_ != nullptr &&
           !std::binary_search(statistics_columns_->begin(), statistics_columns_->end(),
                               column_index_);
  }

  uint64_t ReadListSize(CompactType expected) {
    const uint8_t header = ReadByte();
    const auto actual = static_cast<CompactType>(header & 0x0f);
    if (actual != expected &&
        !(expected == CompactType::BoolTrue && actual == CompactType::BoolFalse)) {
      throw ParquetException("Unexpected Thrift compact list element type");
    }
    uint64_t size = header >> 4;
    if (size == 15) size = ReadVarint();
    if (size > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
      throw ParquetException("Thrift compact list is too large");
    }
    return size;
  }

  uint8_t ReadByte() {
    if (current_ == end_) throw ParquetException("Truncated Thrift compact message");
    return *current_++;
  }

  uint64_t ReadVarint() {
    // Most compact-protocol integers fit in one byte. Keep the common path
    // branch-light and avoid repeated ReadByte() calls; the slow path retains
    // the same truncation and overflow checks for malformed input.
    if (current_ == end_) throw ParquetException("Truncated Thrift compact message");
    uint8_t byte = *current_++;
    if ((byte & 0x80) == 0) return byte;
    uint64_t value = byte & 0x7f;
    for (int shift = 7; shift < 64; shift += 7) {
      if (current_ == end_) throw ParquetException("Truncated Thrift compact message");
      byte = *current_++;
      if (shift == 63 && (byte & 0x7e) != 0) {
        throw ParquetException("Thrift compact varint overflow");
      }
      value |= static_cast<uint64_t>(byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) return value;
    }
    throw ParquetException("Invalid Thrift compact varint");
  }

  int64_t ReadZigZag() {
    const uint64_t value = ReadVarint();
    return static_cast<int64_t>((value >> 1) ^
                                static_cast<uint64_t>(-static_cast<int64_t>(value & 1)));
  }

  int8_t ReadI8() { return static_cast<int8_t>(ReadByte()); }
  bool ReadBoolElement() {
    const uint8_t value = ReadByte();
    if (value == 1) return true;
    if (value == 0 || value == 2) return false;
    throw ParquetException("Invalid Thrift compact boolean");
  }
  int16_t ReadI16() { return CheckedCast<int16_t>(ReadZigZag()); }
  int32_t ReadI32() { return CheckedCast<int32_t>(ReadZigZag()); }
  int64_t ReadI64() { return ReadZigZag(); }

  double ReadDouble() {
    uint64_t bits = 0;
    for (int i = 0; i < 8; ++i) bits |= static_cast<uint64_t>(ReadByte()) << (8 * i);
    double value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
  }

  std::string ReadBinary() {
    const uint64_t size = ReadVarint();
    if (size > static_cast<uint64_t>(end_ - current_)) {
      throw ParquetException("Truncated Thrift compact binary");
    }
    std::string value(reinterpret_cast<const char*>(current_), static_cast<size_t>(size));
    current_ += size;
    return value;
  }

  std::string_view ReadBinaryView() {
    const uint64_t size = ReadVarint();
    if (size > static_cast<uint64_t>(end_ - current_)) {
      throw ParquetException("Truncated Thrift compact binary");
    }
    const char* data = reinterpret_cast<const char*>(current_);
    current_ += size;
    return std::string_view(data, static_cast<size_t>(size));
  }

  CompactField ReadField(int16_t last_field_id) {
    const uint8_t header = ReadByte();
    const auto type = static_cast<CompactType>(header & 0x0f);
    if (type == CompactType::Stop) return {type, 0, false};
    const uint8_t delta = header >> 4;
    const int16_t id =
        delta == 0 ? ReadI16() : CheckedCast<int16_t>(last_field_id + delta);
    return {type, id, type == CompactType::BoolTrue};
  }

  template <typename Fn>
  void ReadStruct(Fn&& fn) {
    int16_t last_field_id = 0;
    while (true) {
      const CompactField field = ReadField(last_field_id);
      if (field.type == CompactType::Stop) return;
      fn(field);
      last_field_id = field.id;
    }
  }

  template <typename T, typename Fn>
  std::vector<T> ReadList(CompactType expected, Fn&& fn) {
    const uint64_t size = ReadListSize(expected);
    std::vector<T> result;
    result.reserve(static_cast<size_t>(size));
    for (uint64_t i = 0; i < size; ++i) result.emplace_back(fn(expected));
    return result;
  }

  void Skip(CompactType type, int depth = 64) {
    if (depth == 0) throw ParquetException("Thrift compact nesting is too deep");
    switch (type) {
      case CompactType::BoolTrue:
      case CompactType::BoolFalse:
        return;
      case CompactType::Byte:
        ReadByte();
        return;
      case CompactType::I16:
      case CompactType::I32:
      case CompactType::I64:
        ReadVarint();
        return;
      case CompactType::Double:
        for (int i = 0; i < 8; ++i) ReadByte();
        return;
      case CompactType::Binary:
        ReadBinary();
        return;
      case CompactType::Struct:
        ReadStruct([&](const CompactField& field) { Skip(field.type, depth - 1); });
        return;
      case CompactType::List:
      case CompactType::Set: {
        const uint8_t header = ReadByte();
        const auto element_type = static_cast<CompactType>(header & 0x0f);
        uint64_t size = header >> 4;
        if (size == 15) size = ReadVarint();
        if (size > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
          throw ParquetException("Thrift compact list is too large");
        }
        for (uint64_t i = 0; i < size; ++i) Skip(element_type, depth - 1);
        return;
      }
      case CompactType::Map: {
        const uint64_t size = ReadVarint();
        if (size == 0) return;
        if (size > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
          throw ParquetException("Thrift compact map is too large");
        }
        const uint8_t types = ReadByte();
        const auto key_type = static_cast<CompactType>(types >> 4);
        const auto value_type = static_cast<CompactType>(types & 0x0f);
        for (uint64_t i = 0; i < size; ++i) {
          Skip(key_type, depth - 1);
          Skip(value_type, depth - 1);
        }
        return;
      }
      case CompactType::Uuid:
        for (int i = 0; i < 16; ++i) ReadByte();
        return;
      default:
        throw ParquetException("Unknown Thrift compact type");
    }
  }

  void ReadEmptyStruct() {
    if (ReadByte() != 0) throw ParquetException("Expected an empty Thrift struct");
  }

 private:
  template <typename T>
  static T CheckedCast(int64_t value) {
    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
      throw ParquetException("Thrift compact integer overflow");
    }
    return static_cast<T>(value);
  }

  const uint8_t* begin_;
  const uint8_t* current_;
  const uint8_t* end_;
  bool skip_column_stats_ = false;
  bool skip_encoding_stats_ = false;
  bool skip_size_stats_ = false;
  const std::vector<int32_t>* statistics_columns_ = nullptr;
  int32_t column_index_ = -1;
  bool skip_page_stats_ = false;
  bool encoding_stats_as_mask_ = false;
};

inline void Expect(CompactType actual, CompactType expected) {
  if (actual != expected &&
      !(expected == CompactType::BoolTrue &&
        (actual == CompactType::BoolFalse || actual == CompactType::BoolTrue))) {
    throw ParquetException("Unexpected Thrift compact field type");
  }
}

template <typename Enum>
Enum ReadEnum(CompactInput& input) {
  return static_cast<Enum>(input.ReadI32());
}

void ReadSizeStatistics(CompactInput&, format::SizeStatistics*);
void ReadBoundingBox(CompactInput&, format::BoundingBox*);
void ReadGeospatialStatistics(CompactInput&, format::GeospatialStatistics*);
void ReadStatistics(CompactInput&, format::Statistics*);
void ReadLogicalType(CompactInput&, format::LogicalType*);
void ReadSchemaElement(CompactInput&, format::SchemaElement*);
void ReadKeyValue(CompactInput&, format::KeyValue*);
void ReadSortingColumn(CompactInput&, format::SortingColumn*);
void ReadPageEncodingStats(CompactInput&, format::PageEncodingStats*);
void ReadColumnMetaData(CompactInput&, format::ColumnMetaData*);
void ReadColumnCryptoMetaData(CompactInput&, format::ColumnCryptoMetaData*);
void ReadColumnChunk(CompactInput&, format::ColumnChunk*);
void ReadRowGroup(CompactInput&, format::RowGroup*);
void ReadColumnOrder(CompactInput&, format::ColumnOrder*);
void ReadEncryptionAlgorithm(CompactInput&, format::EncryptionAlgorithm*);
void ReadFileMetaData(CompactInput&, format::FileMetaData*);
void ReadPageLocation(CompactInput&, format::PageLocation*);
void ReadOffsetIndex(CompactInput&, format::OffsetIndex*);
void ReadColumnIndex(CompactInput&, format::ColumnIndex*);
void ReadDataPageHeader(CompactInput&, format::DataPageHeader*);
void ReadDataPageHeaderV2(CompactInput&, format::DataPageHeaderV2*);
void ReadDictionaryPageHeader(CompactInput&, format::DictionaryPageHeader*);
void ReadPageHeader(CompactInput&, format::PageHeader*);
void ReadBloomFilterHeader(CompactInput&, format::BloomFilterHeader*);
void ReadDirectFileMetaData(CompactInput&, DirectFileMetaData*);
void ReadFastFileMetaData(CompactInput&, FastMetadataSummary*);

}  // namespace internal
}  // namespace parquet

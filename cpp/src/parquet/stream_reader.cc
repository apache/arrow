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

#include "parquet/stream_reader.h"

#include <utility>

namespace parquet {

StreamReader::StreamReader(std::unique_ptr<ParquetFileReader> reader)
    : file_reader_{std::move(reader)} {
  file_metadata_ = file_reader_->metadata();

  auto schema = file_metadata_->schema();
  auto group_node = schema->group_node();

  nodes_.resize(schema->num_columns());

  for (auto i = 0; i < schema->num_columns(); ++i) {
    nodes_[i] = std::static_pointer_cast<schema::PrimitiveNode>(group_node->field(i));
  }
  NextRowGroup();
}

StreamReader& StreamReader::operator>>(bool& v) {
  CheckColumn(Type::BOOLEAN, ConvertedType::NONE);
  Read<BoolReader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(int8_t& v) {
  CheckColumn(Type::INT32, ConvertedType::INT_8);
  int32_t tmp;
  Read<Int32Reader>(&tmp);
  v = static_cast<int8_t>(tmp);
  return *this;
}

StreamReader& StreamReader::operator>>(uint8_t& v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_8);
  int32_t tmp;
  Read<Int32Reader>(&tmp);
  v = static_cast<uint8_t>(tmp);
  return *this;
}

StreamReader& StreamReader::operator>>(int16_t& v) {
  CheckColumn(Type::INT32, ConvertedType::INT_16);
  int32_t tmp;
  Read<Int32Reader>(&tmp);
  v = static_cast<int16_t>(tmp);
  return *this;
}

StreamReader& StreamReader::operator>>(uint16_t& v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_16);
  int32_t tmp;
  Read<Int32Reader>(&tmp);
  v = static_cast<uint16_t>(tmp);
  return *this;
}

StreamReader& StreamReader::operator>>(int32_t& v) {
  CheckColumn(Type::INT32, ConvertedType::INT_32);
  Read<Int32Reader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(uint32_t& v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_32);
  Read<Int32Reader>(reinterpret_cast<int32_t*>(&v));
  return *this;
}

StreamReader& StreamReader::operator>>(int64_t& v) {
  CheckColumn(Type::INT64, ConvertedType::INT_64);
  Read<Int64Reader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(uint64_t& v) {
  CheckColumn(Type::INT64, ConvertedType::UINT_64);
  Read<Int64Reader>(reinterpret_cast<int64_t*>(&v));
  return *this;
}

StreamReader& StreamReader::operator>>(std::chrono::milliseconds& v) {
  CheckColumn(Type::INT64, ConvertedType::TIMESTAMP_MILLIS);
  int64_t tmp;
  Read<Int64Reader>(&tmp);
  v = std::chrono::milliseconds{tmp};
  return *this;
}

StreamReader& StreamReader::operator>>(std::chrono::microseconds& v) {
  CheckColumn(Type::INT64, ConvertedType::TIMESTAMP_MICROS);
  int64_t tmp;
  Read<Int64Reader>(&tmp);
  v = std::chrono::microseconds{tmp};
  return *this;
}

StreamReader& StreamReader::operator>>(float& v) {
  CheckColumn(Type::FLOAT, ConvertedType::NONE);
  Read<FloatReader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(double& v) {
  CheckColumn(Type::DOUBLE, ConvertedType::NONE);
  Read<DoubleReader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(char& v) {
  CheckColumn(Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE, 1);
  FixedLenByteArray flba;
  Read(&flba);
  v = static_cast<char>(flba.ptr[0]);
  return *this;
}

StreamReader& StreamReader::operator>>(std::string& v) {
  CheckColumn(Type::BYTE_ARRAY, ConvertedType::UTF8);
  ByteArray ba;
  Read(&ba);
  v = std::string(reinterpret_cast<const char*>(ba.ptr), ba.len);
  return *this;
}

void StreamReader::ReadFixedLength(char* ptr, int len) {
  CheckColumn(Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE, len);
  FixedLenByteArray flba;
  Read(&flba);
  std::memcpy(ptr, flba.ptr, len);
}

void StreamReader::Read(ByteArray* v) {
  const auto& node = nodes_[column_index_];
  auto reader = static_cast<ByteArrayReader*>(column_readers_[column_index_++].get());
  int64_t values_read;

  reader->ReadBatch(1, nullptr, nullptr, v, &values_read);

  if (values_read != 1) {
    throw ParquetException("Failed to read value for column '" + node->name() + "'");
  }
}

void StreamReader::Read(FixedLenByteArray* v) {
  const auto& node = nodes_[column_index_];
  auto reader =
      static_cast<FixedLenByteArrayReader*>(column_readers_[column_index_++].get());
  int64_t values_read;

  reader->ReadBatch(1, nullptr, nullptr, v, &values_read);

  if (values_read != 1) {
    throw ParquetException("Failed to read value for column '" + node->name() + "'");
  }
}

void StreamReader::EndRow() {
  if (!file_reader_) {
    throw ParquetException("StreamReader not initialized");
  }
  if (static_cast<std::size_t>(column_index_) < nodes_.size()) {
    throw ParquetException("Cannot end row with " + std::to_string(column_index_) +
                           " of " + std::to_string(nodes_.size()) + " columns read");
  }
  column_index_ = 0;

  if (column_readers_[0]->HasNext()) {
    return;
  }
  NextRowGroup();
}

void StreamReader::NextRowGroup() {
  // Find next none-empty row group
  while (row_group_index_ < file_metadata_->num_row_groups()) {
    row_group_reader_ = file_reader_->RowGroup(row_group_index_);
    ++row_group_index_;

    column_readers_.resize(file_metadata_->num_columns());

    for (int i = 0; i < file_metadata_->num_columns(); ++i) {
      column_readers_[i] = row_group_reader_->Column(i);
    }
    if (column_readers_[0]->HasNext()) {
      return;
    }
  }
  // No more row groups found.
  eof_ = true;
  file_reader_.reset();
  file_metadata_.reset();
  row_group_reader_.reset();
  column_readers_.clear();
  nodes_.clear();
}

void StreamReader::CheckColumn(Type::type physical_type,
                               ConvertedType::type converted_type, int length) {
  if (static_cast<std::size_t>(column_index_) >= nodes_.size()) {
    if (eof_) {
      throw ParquetException("EOF reached");
    }
    throw ParquetException("Column index out-of-bounds.  Index " +
                           std::to_string(column_index_) + " is invalid for " +
                           std::to_string(nodes_.size()) + " columns");
  }
  const auto& node = nodes_[column_index_];

  if (physical_type != node->physical_type()) {
    throw ParquetException("Column physical type mismatch.  Column '" + node->name() +
                           "' has physical type '" + TypeToString(node->physical_type()) +
                           "' not '" + TypeToString(physical_type) + "'");
  }
  if (converted_type != node->converted_type()) {
    throw ParquetException("Column converted type mismatch.  Column '" + node->name() +
                           "' has converted type '" +
                           ConvertedTypeToString(node->converted_type()) + "' not '" +
                           ConvertedTypeToString(converted_type) + "'");
  }
  // Length must be exact.
  if (length != node->type_length()) {
    throw ParquetException("Column length mismatch.  Column '" + node->name() +
                           "' has length " + std::to_string(node->type_length()) +
                           "] not " + std::to_string(length));
  }
}

StreamReader& operator>>(StreamReader& os, EndRowType) {
  os.EndRow();
  return os;
}

}  // namespace parquet

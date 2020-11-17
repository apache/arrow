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

#include "parquet/stream_writer.h"

#include <utility>

namespace parquet {

int64_t StreamWriter::default_row_group_size_{512 * 1024 * 1024};  // 512MB

constexpr int16_t StreamWriter::kDefLevelZero;
constexpr int16_t StreamWriter::kDefLevelOne;
constexpr int16_t StreamWriter::kRepLevelZero;
constexpr int64_t StreamWriter::kBatchSizeOne;

StreamWriter::FixedStringView::FixedStringView(const char* data_ptr)
    : data{data_ptr}, size{std::strlen(data_ptr)} {}

StreamWriter::FixedStringView::FixedStringView(const char* data_ptr, std::size_t data_len)
    : data{data_ptr}, size{data_len} {}

StreamWriter::StreamWriter(std::unique_ptr<ParquetFileWriter> writer)
    : file_writer_{std::move(writer)},
      row_group_writer_{file_writer_->AppendBufferedRowGroup()} {
  auto schema = file_writer_->schema();
  auto group_node = schema->group_node();

  nodes_.resize(schema->num_columns());

  for (auto i = 0; i < schema->num_columns(); ++i) {
    nodes_[i] = std::static_pointer_cast<schema::PrimitiveNode>(group_node->field(i));
  }
}

void StreamWriter::SetDefaultMaxRowGroupSize(int64_t max_size) {
  default_row_group_size_ = max_size;
}

void StreamWriter::SetMaxRowGroupSize(int64_t max_size) {
  max_row_group_size_ = max_size;
}

int StreamWriter::num_columns() const { return static_cast<int>(nodes_.size()); }

StreamWriter& StreamWriter::operator<<(bool v) {
  CheckColumn(Type::BOOLEAN, ConvertedType::NONE);
  return Write<BoolWriter>(v);
}

StreamWriter& StreamWriter::operator<<(int8_t v) {
  CheckColumn(Type::INT32, ConvertedType::INT_8);
  return Write<Int32Writer>(static_cast<int32_t>(v));
}

StreamWriter& StreamWriter::operator<<(uint8_t v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_8);
  return Write<Int32Writer>(static_cast<int32_t>(v));
}

StreamWriter& StreamWriter::operator<<(int16_t v) {
  CheckColumn(Type::INT32, ConvertedType::INT_16);
  return Write<Int32Writer>(static_cast<int32_t>(v));
}

StreamWriter& StreamWriter::operator<<(uint16_t v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_16);
  return Write<Int32Writer>(static_cast<int32_t>(v));
}

StreamWriter& StreamWriter::operator<<(int32_t v) {
  CheckColumn(Type::INT32, ConvertedType::INT_32);
  return Write<Int32Writer>(v);
}

StreamWriter& StreamWriter::operator<<(uint32_t v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_32);
  return Write<Int32Writer>(static_cast<int32_t>(v));
}

StreamWriter& StreamWriter::operator<<(int64_t v) {
  CheckColumn(Type::INT64, ConvertedType::INT_64);
  return Write<Int64Writer>(v);
}

StreamWriter& StreamWriter::operator<<(uint64_t v) {
  CheckColumn(Type::INT64, ConvertedType::UINT_64);
  return Write<Int64Writer>(static_cast<int64_t>(v));
}

StreamWriter& StreamWriter::operator<<(const std::chrono::milliseconds& v) {
  CheckColumn(Type::INT64, ConvertedType::TIMESTAMP_MILLIS);
  return Write<Int64Writer>(static_cast<int64_t>(v.count()));
}

StreamWriter& StreamWriter::operator<<(const std::chrono::microseconds& v) {
  CheckColumn(Type::INT64, ConvertedType::TIMESTAMP_MICROS);
  return Write<Int64Writer>(static_cast<int64_t>(v.count()));
}

StreamWriter& StreamWriter::operator<<(float v) {
  CheckColumn(Type::FLOAT, ConvertedType::NONE);
  return Write<FloatWriter>(v);
}

StreamWriter& StreamWriter::operator<<(double v) {
  CheckColumn(Type::DOUBLE, ConvertedType::NONE);
  return Write<DoubleWriter>(v);
}

StreamWriter& StreamWriter::operator<<(char v) { return WriteFixedLength(&v, 1); }

StreamWriter& StreamWriter::operator<<(FixedStringView v) {
  return WriteFixedLength(v.data, v.size);
}

StreamWriter& StreamWriter::operator<<(const char* v) {
  return WriteVariableLength(v, std::strlen(v));
}

StreamWriter& StreamWriter::operator<<(const std::string& v) {
  return WriteVariableLength(v.data(), v.size());
}

StreamWriter& StreamWriter::operator<<(::arrow::util::string_view v) {
  return WriteVariableLength(v.data(), v.size());
}

StreamWriter& StreamWriter::WriteVariableLength(const char* data_ptr,
                                                std::size_t data_len) {
  CheckColumn(Type::BYTE_ARRAY, ConvertedType::UTF8);

  auto writer = static_cast<ByteArrayWriter*>(row_group_writer_->column(column_index_++));

  if (data_ptr != nullptr) {
    ByteArray ba_value;

    ba_value.ptr = reinterpret_cast<const uint8_t*>(data_ptr);
    ba_value.len = static_cast<uint32_t>(data_len);

    writer->WriteBatch(kBatchSizeOne, &kDefLevelOne, &kRepLevelZero, &ba_value);
  } else {
    writer->WriteBatch(kBatchSizeOne, &kDefLevelZero, &kRepLevelZero, nullptr);
  }
  if (max_row_group_size_ > 0) {
    row_group_size_ += writer->EstimatedBufferedValueBytes();
  }
  return *this;
}

StreamWriter& StreamWriter::WriteFixedLength(const char* data_ptr, std::size_t data_len) {
  CheckColumn(Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE,
              static_cast<int>(data_len));

  auto writer =
      static_cast<FixedLenByteArrayWriter*>(row_group_writer_->column(column_index_++));

  if (data_ptr != nullptr) {
    FixedLenByteArray flba_value;

    flba_value.ptr = reinterpret_cast<const uint8_t*>(data_ptr);
    writer->WriteBatch(kBatchSizeOne, &kDefLevelOne, &kRepLevelZero, &flba_value);
  } else {
    writer->WriteBatch(kBatchSizeOne, &kDefLevelZero, &kRepLevelZero, nullptr);
  }
  if (max_row_group_size_ > 0) {
    row_group_size_ += writer->EstimatedBufferedValueBytes();
  }
  return *this;
}

void StreamWriter::CheckColumn(Type::type physical_type,
                               ConvertedType::type converted_type, int length) {
  if (static_cast<std::size_t>(column_index_) >= nodes_.size()) {
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
                           "' has converted type[" +
                           ConvertedTypeToString(node->converted_type()) + "] not '" +
                           ConvertedTypeToString(converted_type) + "'");
  }
  // Length must be exact.
  // A shorter length fixed array is not acceptable as it would
  // result in array bound read errors.
  //
  if (length != node->type_length()) {
    throw ParquetException("Column length mismatch.  Column '" + node->name() +
                           "' has length " + std::to_string(node->type_length()) +
                           " not " + std::to_string(length));
  }
}

int64_t StreamWriter::SkipColumns(int num_columns_to_skip) {
  int num_columns_skipped = 0;

  for (; (num_columns_to_skip > num_columns_skipped) &&
         static_cast<std::size_t>(column_index_) < nodes_.size();
       ++num_columns_skipped) {
    const auto& node = nodes_[column_index_];

    if (node->is_required()) {
      throw ParquetException("Cannot skip column '" + node->name() +
                             "' as it is required.");
    }
    auto writer = row_group_writer_->column(column_index_++);

    WriteNullValue(writer);
  }
  return num_columns_skipped;
}

void StreamWriter::WriteNullValue(ColumnWriter* writer) {
  switch (writer->type()) {
    case Type::BOOLEAN:
      static_cast<BoolWriter*>(writer)->WriteBatch(kBatchSizeOne, &kDefLevelZero,
                                                   &kRepLevelZero, nullptr);
      break;
    case Type::INT32:
      static_cast<Int32Writer*>(writer)->WriteBatch(kBatchSizeOne, &kDefLevelZero,
                                                    &kRepLevelZero, nullptr);
      break;
    case Type::INT64:
      static_cast<Int64Writer*>(writer)->WriteBatch(kBatchSizeOne, &kDefLevelZero,
                                                    &kRepLevelZero, nullptr);
      break;
    case Type::BYTE_ARRAY:
      static_cast<ByteArrayWriter*>(writer)->WriteBatch(kBatchSizeOne, &kDefLevelZero,
                                                        &kRepLevelZero, nullptr);
      break;
    case Type::FIXED_LEN_BYTE_ARRAY:
      static_cast<FixedLenByteArrayWriter*>(writer)->WriteBatch(
          kBatchSizeOne, &kDefLevelZero, &kRepLevelZero, nullptr);
      break;
    case Type::FLOAT:
      static_cast<FloatWriter*>(writer)->WriteBatch(kBatchSizeOne, &kDefLevelZero,
                                                    &kRepLevelZero, nullptr);
      break;
    case Type::DOUBLE:
      static_cast<DoubleWriter*>(writer)->WriteBatch(kBatchSizeOne, &kDefLevelZero,
                                                     &kRepLevelZero, nullptr);
      break;
    case Type::INT96:
    case Type::UNDEFINED:
      throw ParquetException("Unexpected type: " + TypeToString(writer->type()));
      break;
  }
}

void StreamWriter::SkipOptionalColumn() {
  if (SkipColumns(1) != 1) {
    throw ParquetException("Failed to skip optional column at column index " +
                           std::to_string(column_index_));
  }
}

void StreamWriter::EndRow() {
  if (!file_writer_) {
    throw ParquetException("StreamWriter not initialized");
  }
  if (static_cast<std::size_t>(column_index_) < nodes_.size()) {
    throw ParquetException("Cannot end row with " + std::to_string(column_index_) +
                           " of " + std::to_string(nodes_.size()) + " columns written");
  }
  column_index_ = 0;
  ++current_row_;

  if (max_row_group_size_ > 0) {
    if (row_group_size_ > max_row_group_size_) {
      EndRowGroup();
    }
    // Initialize for each row with size already written
    // (compressed + uncompressed).
    //
    row_group_size_ = row_group_writer_->total_bytes_written() +
                      row_group_writer_->total_compressed_bytes();
  }
}

void StreamWriter::EndRowGroup() {
  if (!file_writer_) {
    throw ParquetException("StreamWriter not initialized");
  }
  // Avoid creating empty row groups.
  if (row_group_writer_->num_rows() > 0) {
    row_group_writer_->Close();
    row_group_writer_.reset(file_writer_->AppendBufferedRowGroup());
  }
}

StreamWriter& operator<<(StreamWriter& os, EndRowType) {
  os.EndRow();
  return os;
}

StreamWriter& operator<<(StreamWriter& os, EndRowGroupType) {
  os.EndRowGroup();
  return os;
}

}  // namespace parquet

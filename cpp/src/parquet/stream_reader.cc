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

#include <set>
#include <utility>

namespace parquet {

constexpr int64_t StreamReader::kBatchSizeOne;

// The converted type expected by the stream reader does not always
// exactly match with the schema in the Parquet file.  The following
// is a list of converted types which are allowed instead of the
// expected converted type.
// Each pair given is:
//   {<StreamReader expected type>, <Parquet file converted type>}
// So for example {ConvertedType::INT_32, ConvertedType::NONE} means
// that if the StreamReader was expecting the converted type INT_32,
// then it will allow the Parquet file to use the converted type
// NONE.
//
static const std::set<std::pair<ConvertedType::type, ConvertedType::type> >
    converted_type_exceptions = {{ConvertedType::INT_32, ConvertedType::NONE},
                                 {ConvertedType::INT_64, ConvertedType::NONE},
                                 {ConvertedType::INT_32, ConvertedType::DECIMAL},
                                 {ConvertedType::INT_64, ConvertedType::DECIMAL},
                                 {ConvertedType::UTF8, ConvertedType::NONE}};

StreamReader::StreamReader(std::unique_ptr<ParquetFileReader> reader)
    : file_reader_{std::move(reader)}, eof_{false} {
  file_metadata_ = file_reader_->metadata();

  auto schema = file_metadata_->schema();
  auto group_node = schema->group_node();

  nodes_.resize(schema->num_columns());

  for (auto i = 0; i < schema->num_columns(); ++i) {
    nodes_[i] = std::static_pointer_cast<schema::PrimitiveNode>(group_node->field(i));
  }
  NextRowGroup();
}

int StreamReader::num_columns() const {
  // Check for file metadata i.e. object is not default constructed.
  if (file_metadata_) {
    return file_metadata_->num_columns();
  }
  return 0;
}

int64_t StreamReader::num_rows() const {
  // Check for file metadata i.e. object is not default constructed.
  if (file_metadata_) {
    return file_metadata_->num_rows();
  }
  return 0;
}

StreamReader& StreamReader::operator>>(bool& v) {
  CheckColumn(Type::BOOLEAN, ConvertedType::NONE);
  Read<BoolReader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(int8_t& v) {
  CheckColumn(Type::INT32, ConvertedType::INT_8);
  Read<Int32Reader, int32_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(uint8_t& v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_8);
  Read<Int32Reader, int32_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(int16_t& v) {
  CheckColumn(Type::INT32, ConvertedType::INT_16);
  Read<Int32Reader, int32_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(uint16_t& v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_16);
  Read<Int32Reader, int32_t>(&v);
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

StreamReader& StreamReader::operator>>(optional<bool>& v) {
  CheckColumn(Type::BOOLEAN, ConvertedType::NONE);
  ReadOptional<BoolReader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<int8_t>& v) {
  CheckColumn(Type::INT32, ConvertedType::INT_8);
  ReadOptional<Int32Reader, int32_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<uint8_t>& v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_8);
  ReadOptional<Int32Reader, int32_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<int16_t>& v) {
  CheckColumn(Type::INT32, ConvertedType::INT_16);
  ReadOptional<Int32Reader, int32_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<uint16_t>& v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_16);
  ReadOptional<Int32Reader, int32_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<int32_t>& v) {
  CheckColumn(Type::INT32, ConvertedType::INT_32);
  ReadOptional<Int32Reader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<uint32_t>& v) {
  CheckColumn(Type::INT32, ConvertedType::UINT_32);
  ReadOptional<Int32Reader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<int64_t>& v) {
  CheckColumn(Type::INT64, ConvertedType::INT_64);
  ReadOptional<Int64Reader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<uint64_t>& v) {
  CheckColumn(Type::INT64, ConvertedType::UINT_64);
  ReadOptional<Int64Reader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<float>& v) {
  CheckColumn(Type::FLOAT, ConvertedType::NONE);
  ReadOptional<FloatReader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<double>& v) {
  CheckColumn(Type::DOUBLE, ConvertedType::NONE);
  ReadOptional<DoubleReader>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<std::chrono::milliseconds>& v) {
  CheckColumn(Type::INT64, ConvertedType::TIMESTAMP_MILLIS);
  ReadOptional<Int64Reader, int64_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<std::chrono::microseconds>& v) {
  CheckColumn(Type::INT64, ConvertedType::TIMESTAMP_MICROS);
  ReadOptional<Int64Reader, int64_t>(&v);
  return *this;
}

StreamReader& StreamReader::operator>>(optional<char>& v) {
  CheckColumn(Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE, 1);
  FixedLenByteArray flba;

  if (ReadOptional(&flba)) {
    v = static_cast<char>(flba.ptr[0]);
  } else {
    v.reset();
  }
  return *this;
}

StreamReader& StreamReader::operator>>(optional<std::string>& v) {
  CheckColumn(Type::BYTE_ARRAY, ConvertedType::UTF8);
  ByteArray ba;

  if (ReadOptional(&ba)) {
    v = std::string(reinterpret_cast<const char*>(ba.ptr), ba.len);
  } else {
    v.reset();
  }
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
  int16_t def_level;
  int16_t rep_level;
  int64_t values_read;

  reader->ReadBatch(kBatchSizeOne, &def_level, &rep_level, v, &values_read);

  if (values_read != 1) {
    ThrowReadFailedException(node);
  }
}

bool StreamReader::ReadOptional(ByteArray* v) {
  const auto& node = nodes_[column_index_];
  auto reader = static_cast<ByteArrayReader*>(column_readers_[column_index_++].get());
  int16_t def_level;
  int16_t rep_level;
  int64_t values_read;

  reader->ReadBatch(kBatchSizeOne, &def_level, &rep_level, v, &values_read);

  if (values_read == 1) {
    return true;
  } else if ((values_read == 0) && (def_level == 0)) {
    return false;
  }
  ThrowReadFailedException(node);
}

void StreamReader::Read(FixedLenByteArray* v) {
  const auto& node = nodes_[column_index_];
  auto reader =
      static_cast<FixedLenByteArrayReader*>(column_readers_[column_index_++].get());
  int16_t def_level;
  int16_t rep_level;
  int64_t values_read;

  reader->ReadBatch(kBatchSizeOne, &def_level, &rep_level, v, &values_read);

  if (values_read != 1) {
    ThrowReadFailedException(node);
  }
}

bool StreamReader::ReadOptional(FixedLenByteArray* v) {
  const auto& node = nodes_[column_index_];
  auto reader =
      static_cast<FixedLenByteArrayReader*>(column_readers_[column_index_++].get());
  int16_t def_level;
  int16_t rep_level;
  int64_t values_read;

  reader->ReadBatch(kBatchSizeOne, &def_level, &rep_level, v, &values_read);

  if (values_read == 1) {
    return true;
  } else if ((values_read == 0) && (def_level == 0)) {
    return false;
  }
  ThrowReadFailedException(node);
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
  ++current_row_;

  if (!column_readers_[0]->HasNext()) {
    NextRowGroup();
  }
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
      row_group_row_offset_ = current_row_;
      return;
    }
  }
  // No more row groups found.
  SetEof();
}

void StreamReader::SetEof() {
  // Do not reset file_metadata_ to ensure queries on the number of
  // rows/columns still function.
  eof_ = true;
  file_reader_.reset();
  row_group_reader_.reset();
  column_readers_.clear();
  nodes_.clear();
}

int64_t StreamReader::SkipRows(int64_t num_rows_to_skip) {
  if (0 != column_index_) {
    throw ParquetException("Must finish reading current row before skipping rows.");
  }
  int64_t num_rows_remaining_to_skip = num_rows_to_skip;

  while (!eof_ && (num_rows_remaining_to_skip > 0)) {
    int64_t num_rows_in_row_group = row_group_reader_->metadata()->num_rows();
    int64_t num_rows_remaining_in_row_group =
        num_rows_in_row_group - current_row_ - row_group_row_offset_;

    if (num_rows_remaining_in_row_group > num_rows_remaining_to_skip) {
      for (auto reader : column_readers_) {
        SkipRowsInColumn(reader.get(), num_rows_remaining_to_skip);
      }
      current_row_ += num_rows_remaining_to_skip;
      num_rows_remaining_to_skip = 0;
    } else {
      num_rows_remaining_to_skip -= num_rows_remaining_in_row_group;
      current_row_ += num_rows_remaining_in_row_group;
      NextRowGroup();
    }
  }
  return num_rows_to_skip - num_rows_remaining_to_skip;
}

int64_t StreamReader::SkipColumns(int64_t num_columns_to_skip) {
  int64_t num_columns_skipped = 0;

  if (!eof_) {
    for (; (num_columns_to_skip > num_columns_skipped) &&
           static_cast<std::size_t>(column_index_) < nodes_.size();
         ++column_index_) {
      SkipRowsInColumn(column_readers_[column_index_].get(), 1);
      ++num_columns_skipped;
    }
  }
  return num_columns_skipped;
}

void StreamReader::SkipRowsInColumn(ColumnReader* reader, int64_t num_rows_to_skip) {
  int64_t num_skipped = 0;

  switch (reader->type()) {
    case Type::BOOLEAN:
      num_skipped = static_cast<BoolReader*>(reader)->Skip(num_rows_to_skip);
      break;
    case Type::INT32:
      num_skipped = static_cast<Int32Reader*>(reader)->Skip(num_rows_to_skip);
      break;
    case Type::INT64:
      num_skipped = static_cast<Int64Reader*>(reader)->Skip(num_rows_to_skip);
      break;
    case Type::BYTE_ARRAY:
      num_skipped = static_cast<ByteArrayReader*>(reader)->Skip(num_rows_to_skip);
      break;
    case Type::FIXED_LEN_BYTE_ARRAY:
      num_skipped = static_cast<FixedLenByteArrayReader*>(reader)->Skip(num_rows_to_skip);
      break;
    case Type::FLOAT:
      num_skipped = static_cast<FloatReader*>(reader)->Skip(num_rows_to_skip);
      break;
    case Type::DOUBLE:
      num_skipped = static_cast<DoubleReader*>(reader)->Skip(num_rows_to_skip);
      break;
    case Type::INT96:
      num_skipped = static_cast<Int96Reader*>(reader)->Skip(num_rows_to_skip);
      break;
    case Type::UNDEFINED:
      throw ParquetException("Unexpected type: " + TypeToString(reader->type()));
      break;
  }
  if (num_rows_to_skip != num_skipped) {
    throw ParquetException("Skipped " + std::to_string(num_skipped) + "/" +
                           std::to_string(num_rows_to_skip) + " rows in column " +
                           reader->descr()->name());
  }
}

void StreamReader::CheckColumn(Type::type physical_type,
                               ConvertedType::type converted_type, int length) {
  if (static_cast<std::size_t>(column_index_) >= nodes_.size()) {
    if (eof_) {
      ParquetException::EofException();
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
    // The converted type does not always match with the value
    // provided so check the set of exceptions.
    if (converted_type_exceptions.find({converted_type, node->converted_type()}) ==
        converted_type_exceptions.end()) {
      throw ParquetException("Column converted type mismatch.  Column '" + node->name() +
                             "' has converted type '" +
                             ConvertedTypeToString(node->converted_type()) + "' not '" +
                             ConvertedTypeToString(converted_type) + "'");
    }
  }
  // Length must be exact.
  if (length != node->type_length()) {
    throw ParquetException("Column length mismatch.  Column '" + node->name() +
                           "' has length " + std::to_string(node->type_length()) +
                           "] not " + std::to_string(length));
  }
}  // namespace parquet

void StreamReader::ThrowReadFailedException(
    const std::shared_ptr<schema::PrimitiveNode>& node) {
  throw ParquetException("Failed to read value for column '" + node->name() +
                         "' on row " + std::to_string(current_row_));
}

StreamReader& operator>>(StreamReader& os, EndRowType) {
  os.EndRow();
  return os;
}

}  // namespace parquet

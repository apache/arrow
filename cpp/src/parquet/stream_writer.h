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

#ifndef PARQUET_STREAM_WRITER_H
#define PARQUET_STREAM_WRITER_H

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/string_view.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"

namespace parquet {

/// \brief A class for writing Parquet files using an output stream type API.
///
/// The values given must be of the correct type i.e. the type must
/// match the file schema exactly otherwise a ParquetException will be
/// thrown.
///
/// The user must explicitly indicate the end of the row using the
/// EndRow() function or EndRow output manipulator.
///
/// A maximum row group size can be configured, the default size is
/// 512MB.  Alternatively the row group size can be set to zero and the
/// user can create new row groups by calling the EndRowGroup()
/// function or using the EndRowGroup output manipulator.
///
/// Currently there is no support for optional or repeated fields.
///
class PARQUET_EXPORT StreamWriter {
 public:
  // N.B. Default constructed objects are not usable.  This
  //      constructor is provided so that the object may be move
  //      assigned afterwards.
  StreamWriter() = default;

  explicit StreamWriter(std::unique_ptr<ParquetFileWriter> writer);

  ~StreamWriter();

  static void SetDefaultMaxRowGroupSize(int64_t max_size);

  void SetMaxRowGroupSize(int64_t max_size);

  // Moving is possible.
  StreamWriter(StreamWriter&&) noexcept = default;
  StreamWriter& operator=(StreamWriter&&) noexcept = default;

  // Copying is not allowed.
  StreamWriter(const StreamWriter&) = delete;
  StreamWriter& operator=(const StreamWriter&) = delete;

  StreamWriter& operator<<(bool v);

  StreamWriter& operator<<(int8_t v);

  StreamWriter& operator<<(uint8_t v);

  StreamWriter& operator<<(int16_t v);

  StreamWriter& operator<<(uint16_t v);

  StreamWriter& operator<<(int32_t v);

  StreamWriter& operator<<(uint32_t v);

  StreamWriter& operator<<(int64_t v);

  StreamWriter& operator<<(uint64_t v);

  StreamWriter& operator<<(const std::chrono::milliseconds& v);

  StreamWriter& operator<<(const std::chrono::microseconds& v);

  StreamWriter& operator<<(float v);

  StreamWriter& operator<<(double v);

  StreamWriter& operator<<(char v);

  /// \brief Helper class to write fixed length strings.
  /// This is useful as the standard string view (such as
  /// arrow::util::string_view) is for variable length data.
  struct PARQUET_EXPORT FixedStringView {
    FixedStringView() = default;

    explicit FixedStringView(const char* data_ptr);

    FixedStringView(const char* data_ptr, std::size_t data_len);

    const char* data{NULLPTR};
    std::size_t size{0};
  };

  // Output operators for fixed length strings.
  template <int N>
  StreamWriter& operator<<(const char (&v)[N]) {
    return WriteFixedLength(v, N);
  }
  template <std::size_t N>
  StreamWriter& operator<<(const std::array<char, N>& v) {
    return WriteFixedLength(v.data(), N);
  }
  StreamWriter& operator<<(FixedStringView v);

  // Output operators for variable length strings.
  StreamWriter& operator<<(const char* v);
  StreamWriter& operator<<(arrow::util::string_view v);

  // Terminate the current row and advance to next one.
  void EndRow();

  // Terminate the current row group and create new one.
  void EndRowGroup();

 protected:
  template <typename WriterType, typename T>
  StreamWriter& Write(const T v) {
    auto writer = static_cast<WriterType*>(row_group_writer_->column(column_index_++));

    writer->WriteBatch(1, NULLPTR, NULLPTR, &v);

    if (max_row_group_size_ > 0) {
      row_group_size_ += writer->EstimatedBufferedValueBytes();
    }
    return *this;
  }

  StreamWriter& WriteVariableLength(const char* data_ptr, std::size_t data_len);

  StreamWriter& WriteFixedLength(const char* data_ptr, std::size_t data_len);

  void CheckColumn(Type::type physical_type, ConvertedType::type converted_type,
                   int length = -1);

 private:
  using node_ptr_type = std::shared_ptr<schema::PrimitiveNode>;

  struct null_deleter {
    void operator()(void*) {}
  };

  int64_t row_group_size_{0};
  int64_t max_row_group_size_{default_row_group_size_};
  int32_t column_index_{0};
  std::unique_ptr<ParquetFileWriter> file_writer_;
  std::unique_ptr<RowGroupWriter, null_deleter> row_group_writer_;
  std::vector<node_ptr_type> nodes_;

  static int64_t default_row_group_size_;
};

struct PARQUET_EXPORT EndRowType {};
constexpr EndRowType EndRow;

struct PARQUET_EXPORT EndRowGroupType {};
constexpr EndRowGroupType EndRowGroup;

PARQUET_EXPORT
StreamWriter& operator<<(StreamWriter&, EndRowType);

PARQUET_EXPORT
StreamWriter& operator<<(StreamWriter&, EndRowGroupType);

}  // namespace parquet

#endif  // PARQUET_STREAM_WRITER_H

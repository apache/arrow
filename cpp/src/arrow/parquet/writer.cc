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

#include "arrow/parquet/writer.h"

#include "arrow/array.h"
#include "arrow/types/primitive.h"
#include "arrow/parquet/utils.h"
#include "arrow/util/status.h"

namespace arrow {

namespace parquet {

class FileWriter::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<::parquet::ParquetFileWriter> writer);

  Status NewRowGroup(int64_t chunk_size);
  template <typename ParquetType, typename CType>
  Status TypedWriteBatch(::parquet::ColumnWriter* writer, const PrimitiveArray* data);
  Status WriteFlatColumnChunk(const PrimitiveArray* data);
  Status Close();

  virtual ~Impl() {}

 private:
  MemoryPool* pool_;
  PoolBuffer data_buffer_;
  PoolBuffer def_levels_buffer_;
  std::unique_ptr<::parquet::ParquetFileWriter> writer_;
  ::parquet::RowGroupWriter* row_group_writer_;
};

FileWriter::Impl::Impl(
    MemoryPool* pool, std::unique_ptr<::parquet::ParquetFileWriter> writer)
    : pool_(pool),
      data_buffer_(pool),
      writer_(std::move(writer)),
      row_group_writer_(nullptr) {}

Status FileWriter::Impl::NewRowGroup(int64_t chunk_size) {
  if (row_group_writer_ != nullptr) { PARQUET_CATCH_NOT_OK(row_group_writer_->Close()); }
  PARQUET_CATCH_NOT_OK(row_group_writer_ = writer_->AppendRowGroup(chunk_size));
  return Status::OK();
}

template <typename ParquetType, typename CType>
Status FileWriter::Impl::TypedWriteBatch(
    ::parquet::ColumnWriter* column_writer, const PrimitiveArray* data) {
  auto data_ptr = reinterpret_cast<const CType*>(data->data()->data());
  auto writer =
      reinterpret_cast<::parquet::TypedColumnWriter<ParquetType>*>(column_writer);
  if (writer->descr()->max_definition_level() == 0) {
    // no nulls, just dump the data
    writer->WriteBatch(data->length(), nullptr, nullptr, data_ptr);
  } else if (writer->descr()->max_definition_level() == 1) {
    def_levels_buffer_.Resize(data->length() * sizeof(int16_t));
    int16_t* def_levels_ptr =
        reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    std::fill(def_levels_ptr, def_levels_ptr + data->length(), 1);
    if (data->null_count() == 0) {
      PARQUET_CATCH_NOT_OK(
          writer->WriteBatch(data->length(), def_levels_ptr, nullptr, data_ptr));
    } else {
      data_buffer_.Resize((data->length() - data->null_count()) * sizeof(CType));
      auto buffer_ptr = reinterpret_cast<CType*>(data_buffer_.mutable_data());
      size_t buffer_idx = 0;
      for (size_t i = 0; i < data->length(); i++) {
        if (data->IsNull(i)) {
          def_levels_ptr[i] = 0;
        } else {
          buffer_ptr[buffer_idx++] = data_ptr[i];
        }
      }
      PARQUET_CATCH_NOT_OK(
          writer->WriteBatch(data->length(), def_levels_ptr, nullptr, buffer_ptr));
    }
  } else {
    return Status::NotImplemented("no support for max definition level > 1 yet");
  }
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

Status FileWriter::Impl::Close() {
  if (row_group_writer_ != nullptr) { PARQUET_CATCH_NOT_OK(row_group_writer_->Close()); }
  PARQUET_CATCH_NOT_OK(writer_->Close());
  return Status::OK();
}

#define TYPED_BATCH_CASE(ENUM, ArrowType, ParquetType, CType) \
  case Type::ENUM:                                            \
    return TypedWriteBatch<ParquetType, CType>(writer, data); \
    break;

Status FileWriter::Impl::WriteFlatColumnChunk(const PrimitiveArray* data) {
  ::parquet::ColumnWriter* writer = row_group_writer_->NextColumn();
  switch (data->type_enum()) {
    TYPED_BATCH_CASE(INT32, Int32Type, ::parquet::Int32Type, int32_t)
    TYPED_BATCH_CASE(INT64, Int64Type, ::parquet::Int64Type, int64_t)
    TYPED_BATCH_CASE(FLOAT, FloatType, ::parquet::FloatType, float)
    TYPED_BATCH_CASE(DOUBLE, DoubleType, ::parquet::DoubleType, double)
    default:
      return Status::NotImplemented(data->type()->ToString());
  }
}

FileWriter::FileWriter(
    MemoryPool* pool, std::unique_ptr<::parquet::ParquetFileWriter> writer)
    : impl_(new FileWriter::Impl(pool, std::move(writer))) {}

Status FileWriter::NewRowGroup(int64_t chunk_size) {
  return impl_->NewRowGroup(chunk_size);
}

Status FileWriter::WriteFlatColumnChunk(const PrimitiveArray* data) {
  return impl_->WriteFlatColumnChunk(data);
}

Status FileWriter::Close() {
  return impl_->Close();
}

FileWriter::~FileWriter() {}

}  // namespace parquet

}  // namespace arrow

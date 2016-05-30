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

#include <algorithm>
#include <vector>

#include "arrow/array.h"
#include "arrow/column.h"
#include "arrow/table.h"
#include "arrow/types/construct.h"
#include "arrow/types/primitive.h"
#include "arrow/parquet/schema.h"
#include "arrow/parquet/utils.h"
#include "arrow/util/status.h"

using parquet::ParquetFileWriter;
using parquet::schema::GroupNode;

namespace arrow {

namespace parquet {

class FileWriter::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<::parquet::ParquetFileWriter> writer);

  Status NewRowGroup(int64_t chunk_size);
  template <typename ParquetType>
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

template <typename ParquetType>
Status FileWriter::Impl::TypedWriteBatch(
    ::parquet::ColumnWriter* column_writer, const PrimitiveArray* data) {
  auto data_ptr =
      reinterpret_cast<const typename ParquetType::c_type*>(data->data()->data());
  auto writer =
      reinterpret_cast<::parquet::TypedColumnWriter<ParquetType>*>(column_writer);
  if (writer->descr()->max_definition_level() == 0) {
    // no nulls, just dump the data
    PARQUET_CATCH_NOT_OK(writer->WriteBatch(data->length(), nullptr, nullptr, data_ptr));
  } else if (writer->descr()->max_definition_level() == 1) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(data->length() * sizeof(int16_t)));
    int16_t* def_levels_ptr =
        reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    if (data->null_count() == 0) {
      std::fill(def_levels_ptr, def_levels_ptr + data->length(), 1);
      PARQUET_CATCH_NOT_OK(
          writer->WriteBatch(data->length(), def_levels_ptr, nullptr, data_ptr));
    } else {
      RETURN_NOT_OK(data_buffer_.Resize(
          (data->length() - data->null_count()) * sizeof(typename ParquetType::c_type)));
      auto buffer_ptr =
          reinterpret_cast<typename ParquetType::c_type*>(data_buffer_.mutable_data());
      int buffer_idx = 0;
      for (size_t i = 0; i < data->length(); i++) {
        if (data->IsNull(i)) {
          def_levels_ptr[i] = 0;
        } else {
          def_levels_ptr[i] = 1;
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

#define TYPED_BATCH_CASE(ENUM, ArrowType, ParquetType) \
  case Type::ENUM:                                     \
    return TypedWriteBatch<ParquetType>(writer, data); \
    break;

Status FileWriter::Impl::WriteFlatColumnChunk(const PrimitiveArray* data) {
  ::parquet::ColumnWriter* writer;
  PARQUET_CATCH_NOT_OK(writer = row_group_writer_->NextColumn());
  switch (data->type_enum()) {
    TYPED_BATCH_CASE(INT32, Int32Type, ::parquet::Int32Type)
    TYPED_BATCH_CASE(INT64, Int64Type, ::parquet::Int64Type)
    TYPED_BATCH_CASE(FLOAT, FloatType, ::parquet::FloatType)
    TYPED_BATCH_CASE(DOUBLE, DoubleType, ::parquet::DoubleType)
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

// Create a slice of a PrimitiveArray.
//
// This method is specially crafted for WriteFlatTable and assumes the following:
//  * chunk_size is a multiple of 512
Status TemporaryArraySlice(int64_t chunk, int64_t chunk_size, const PrimitiveArray* array,
    std::shared_ptr<PrimitiveArray>* out) {
  // The last chunk may be smaller than the chunk_size
  const int64_t size = std::min(chunk_size, array->length() - chunk * chunk_size);
  const int64_t buffer_offset = chunk * chunk_size * array->type()->value_size();
  const int64_t value_size = size * array->type()->value_size();
  auto chunk_buffer = std::make_shared<Buffer>(array->data(), buffer_offset, value_size);
  std::shared_ptr<Buffer> null_bitmap;
  int32_t null_count = 0;
  if (array->null_count() > 0) {
    int64_t null_offset = (chunk * chunk_size) / 8;
    int64_t null_size = util::ceil_byte(size) / 8;
    null_bitmap = std::make_shared<Buffer>(array->null_bitmap(), null_offset, null_size);
    for (int64_t k = 0; k < size; k++) {
      if (!util::get_bit(null_bitmap->data(), k)) { null_count++; }
    }
  }
  std::shared_ptr<Array> out_array;
  RETURN_NOT_OK(MakePrimitiveArray(
      array->type(), size, chunk_buffer, null_count, null_bitmap, &out_array));
  *out = std::static_pointer_cast<PrimitiveArray>(out_array);
  return Status::OK();
}

Status WriteFlatTable(const Table* table, MemoryPool* pool,
    std::shared_ptr<::parquet::OutputStream> sink, int64_t chunk_size) {
  // Ensure alignment of sliced PrimitiveArray, esp. the null bitmap
  // TODO: Support other chunksizes than multiples of 512
  if (((chunk_size & 511) != 0) && (chunk_size != table->num_rows())) {
    return Status::NotImplemented(
        "Only chunk sizes that are a multiple of 512 are supported");
  }

  std::shared_ptr<::parquet::SchemaDescriptor> parquet_schema;
  RETURN_NOT_OK(ToParquetSchema(table->schema().get(), &parquet_schema));
  auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema());
  std::unique_ptr<ParquetFileWriter> parquet_writer =
      ParquetFileWriter::Open(sink, schema_node);
  FileWriter writer(pool, std::move(parquet_writer));

  // TODO: Support writing chunked arrays.
  for (int i = 0; i < table->num_columns(); i++) {
    if (table->column(i)->data()->num_chunks() != 1) {
      return Status::NotImplemented("No support for writing chunked arrays yet.");
    }
  }

  // Cast to PrimitiveArray instances as we work with them.
  std::vector<std::shared_ptr<PrimitiveArray>> arrays(table->num_columns());
  for (int i = 0; i < table->num_columns(); i++) {
    // num_chunks == 1 as per above loop
    std::shared_ptr<Array> array = table->column(i)->data()->chunk(0);
    auto primitive_array = std::dynamic_pointer_cast<PrimitiveArray>(array);
    if (!primitive_array) {
      return Status::NotImplemented("Table must consist of PrimitiveArray instances");
    }
    arrays[i] = primitive_array;
  }

  for (int chunk = 0; chunk * chunk_size < table->num_rows(); chunk++) {
    int64_t size = std::min(chunk_size, table->num_rows() - chunk * chunk_size);
    RETURN_NOT_OK(writer.NewRowGroup(size));
    for (int i = 0; i < table->num_columns(); i++) {
      std::shared_ptr<PrimitiveArray> array;
      RETURN_NOT_OK(TemporaryArraySlice(chunk, chunk_size, arrays[i].get(), &array));
      RETURN_NOT_OK(writer.WriteFlatColumnChunk(array.get()));
    }
  }

  return writer.Close();
}

}  // namespace parquet

}  // namespace arrow

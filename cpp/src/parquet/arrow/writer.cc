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

#include "parquet/arrow/writer.h"

#include <algorithm>
#include <vector>

#include "parquet/util/logging.h"

#include "parquet/arrow/schema.h"

#include "arrow/api.h"

using arrow::BinaryArray;
using arrow::MemoryPool;
using arrow::PoolBuffer;
using arrow::PrimitiveArray;
using arrow::Status;
using arrow::Table;

using parquet::ParquetFileWriter;
using parquet::ParquetVersion;
using parquet::schema::GroupNode;

namespace parquet {
namespace arrow {

namespace BitUtil = ::arrow::BitUtil;

class FileWriter::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer);

  Status NewRowGroup(int64_t chunk_size);
  template <typename ParquetType, typename ArrowType>
  Status TypedWriteBatch(
      ColumnWriter* writer, const PrimitiveArray* data, int64_t offset, int64_t length);

  // TODO(uwe): Same code as in reader.cc the only difference is the name of the temporary
  // buffer
  template <typename InType, typename OutType>
  struct can_copy_ptr {
    static constexpr bool value =
        std::is_same<InType, OutType>::value ||
        (std::is_integral<InType>{} && std::is_integral<OutType>{} &&
            (sizeof(InType) == sizeof(OutType)));
  };

  template <typename InType, typename OutType,
      typename std::enable_if<can_copy_ptr<InType, OutType>::value>::type* = nullptr>
  Status ConvertPhysicalType(const InType* in_ptr, int64_t, const OutType** out_ptr) {
    *out_ptr = reinterpret_cast<const OutType*>(in_ptr);
    return Status::OK();
  }

  template <typename InType, typename OutType,
      typename std::enable_if<not can_copy_ptr<InType, OutType>::value>::type* = nullptr>
  Status ConvertPhysicalType(
      const InType* in_ptr, int64_t length, const OutType** out_ptr) {
    RETURN_NOT_OK(data_buffer_.Resize(length * sizeof(OutType)));
    OutType* mutable_out_ptr = reinterpret_cast<OutType*>(data_buffer_.mutable_data());
    std::copy(in_ptr, in_ptr + length, mutable_out_ptr);
    *out_ptr = mutable_out_ptr;
    return Status::OK();
  }

  Status WriteFlatColumnChunk(const PrimitiveArray* data, int64_t offset, int64_t length);
  Status WriteFlatColumnChunk(const BinaryArray* data, int64_t offset, int64_t length);
  Status Close();

  virtual ~Impl() {}

 private:
  friend class FileWriter;

  MemoryPool* pool_;
  // Buffer used for storing the data of an array converted to the physical type
  // as expected by parquet-cpp.
  PoolBuffer data_buffer_;
  PoolBuffer def_levels_buffer_;
  std::unique_ptr<ParquetFileWriter> writer_;
  RowGroupWriter* row_group_writer_;
};

FileWriter::Impl::Impl(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer)
    : pool_(pool),
      data_buffer_(pool),
      writer_(std::move(writer)),
      row_group_writer_(nullptr) {}

Status FileWriter::Impl::NewRowGroup(int64_t chunk_size) {
  if (row_group_writer_ != nullptr) { PARQUET_CATCH_NOT_OK(row_group_writer_->Close()); }
  PARQUET_CATCH_NOT_OK(row_group_writer_ = writer_->AppendRowGroup(chunk_size));
  return Status::OK();
}

template <typename ParquetType, typename ArrowType>
Status FileWriter::Impl::TypedWriteBatch(ColumnWriter* column_writer,
    const PrimitiveArray* data, int64_t offset, int64_t length) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;

  DCHECK((offset + length) <= data->length());
  auto data_ptr = reinterpret_cast<const ArrowCType*>(data->data()->data()) + offset;
  auto writer = reinterpret_cast<TypedColumnWriter<ParquetType>*>(column_writer);
  if (writer->descr()->max_definition_level() == 0) {
    // no nulls, just dump the data
    const ParquetCType* data_writer_ptr = nullptr;
    RETURN_NOT_OK((ConvertPhysicalType<ArrowCType, ParquetCType>(
        data_ptr, length, &data_writer_ptr)));
    PARQUET_CATCH_NOT_OK(writer->WriteBatch(length, nullptr, nullptr, data_writer_ptr));
  } else if (writer->descr()->max_definition_level() == 1) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(length * sizeof(int16_t)));
    int16_t* def_levels_ptr =
        reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    if (data->null_count() == 0) {
      std::fill(def_levels_ptr, def_levels_ptr + length, 1);
      const ParquetCType* data_writer_ptr = nullptr;
      RETURN_NOT_OK((ConvertPhysicalType<ArrowCType, ParquetCType>(
          data_ptr, length, &data_writer_ptr)));
      PARQUET_CATCH_NOT_OK(
          writer->WriteBatch(length, def_levels_ptr, nullptr, data_writer_ptr));
    } else {
      RETURN_NOT_OK(data_buffer_.Resize(length * sizeof(ParquetCType)));
      auto buffer_ptr = reinterpret_cast<ParquetCType*>(data_buffer_.mutable_data());
      int buffer_idx = 0;
      for (int i = 0; i < length; i++) {
        if (data->IsNull(offset + i)) {
          def_levels_ptr[i] = 0;
        } else {
          def_levels_ptr[i] = 1;
          buffer_ptr[buffer_idx++] = static_cast<ParquetCType>(data_ptr[i]);
        }
      }
      PARQUET_CATCH_NOT_OK(
          writer->WriteBatch(length, def_levels_ptr, nullptr, buffer_ptr));
    }
  } else {
    return Status::NotImplemented("no support for max definition level > 1 yet");
  }
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

// This specialization seems quite similar but it significantly differs in two points:
// * offset is added at the most latest time to the pointer as we have sub-byte access
// * Arrow data is stored bitwise thus we cannot use std::copy to transform from
//   ArrowType::c_type to ParquetType::c_type
template <>
Status FileWriter::Impl::TypedWriteBatch<BooleanType, ::arrow::BooleanType>(
    ColumnWriter* column_writer, const PrimitiveArray* data, int64_t offset,
    int64_t length) {
  DCHECK((offset + length) <= data->length());
  RETURN_NOT_OK(data_buffer_.Resize(length));
  auto data_ptr = reinterpret_cast<const uint8_t*>(data->data()->data());
  auto buffer_ptr = reinterpret_cast<bool*>(data_buffer_.mutable_data());
  auto writer = reinterpret_cast<TypedColumnWriter<BooleanType>*>(column_writer);
  if (writer->descr()->max_definition_level() == 0) {
    // no nulls, just dump the data
    for (int64_t i = 0; i < length; i++) {
      buffer_ptr[i] = BitUtil::GetBit(data_ptr, offset + i);
    }
    PARQUET_CATCH_NOT_OK(writer->WriteBatch(length, nullptr, nullptr, buffer_ptr));
  } else if (writer->descr()->max_definition_level() == 1) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(length * sizeof(int16_t)));
    int16_t* def_levels_ptr =
        reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    if (data->null_count() == 0) {
      std::fill(def_levels_ptr, def_levels_ptr + length, 1);
      for (int64_t i = 0; i < length; i++) {
        buffer_ptr[i] = BitUtil::GetBit(data_ptr, offset + i);
      }
      // TODO(PARQUET-644): write boolean values as a packed bitmap
      PARQUET_CATCH_NOT_OK(
          writer->WriteBatch(length, def_levels_ptr, nullptr, buffer_ptr));
    } else {
      int buffer_idx = 0;
      for (int i = 0; i < length; i++) {
        if (data->IsNull(offset + i)) {
          def_levels_ptr[i] = 0;
        } else {
          def_levels_ptr[i] = 1;
          buffer_ptr[buffer_idx++] = BitUtil::GetBit(data_ptr, offset + i);
        }
      }
      PARQUET_CATCH_NOT_OK(
          writer->WriteBatch(length, def_levels_ptr, nullptr, buffer_ptr));
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

#define TYPED_BATCH_CASE(ENUM, ArrowType, ParquetType)                            \
  case ::arrow::Type::ENUM:                                                       \
    return TypedWriteBatch<ParquetType, ArrowType>(writer, data, offset, length); \
    break;

Status FileWriter::Impl::WriteFlatColumnChunk(
    const PrimitiveArray* data, int64_t offset, int64_t length) {
  ColumnWriter* writer;
  PARQUET_CATCH_NOT_OK(writer = row_group_writer_->NextColumn());
  switch (data->type_enum()) {
    TYPED_BATCH_CASE(BOOL, ::arrow::BooleanType, BooleanType)
    TYPED_BATCH_CASE(UINT8, ::arrow::UInt8Type, Int32Type)
    TYPED_BATCH_CASE(INT8, ::arrow::Int8Type, Int32Type)
    TYPED_BATCH_CASE(UINT16, ::arrow::UInt16Type, Int32Type)
    TYPED_BATCH_CASE(INT16, ::arrow::Int16Type, Int32Type)
    case ::arrow::Type::UINT32:
      if (writer_->properties()->version() == ParquetVersion::PARQUET_1_0) {
        // Parquet 1.0 reader cannot read the UINT_32 logical type. Thus we need
        // to use the larger Int64Type to store them lossless.
        return TypedWriteBatch<Int64Type, ::arrow::UInt32Type>(
            writer, data, offset, length);
      } else {
        return TypedWriteBatch<Int32Type, ::arrow::UInt32Type>(
            writer, data, offset, length);
      }
      TYPED_BATCH_CASE(INT32, ::arrow::Int32Type, Int32Type)
      TYPED_BATCH_CASE(UINT64, ::arrow::UInt64Type, Int64Type)
      TYPED_BATCH_CASE(INT64, ::arrow::Int64Type, Int64Type)
      TYPED_BATCH_CASE(TIMESTAMP, ::arrow::TimestampType, Int64Type)
      TYPED_BATCH_CASE(FLOAT, ::arrow::FloatType, FloatType)
      TYPED_BATCH_CASE(DOUBLE, ::arrow::DoubleType, DoubleType)
    default:
      return Status::NotImplemented(data->type()->ToString());
  }
}

Status FileWriter::Impl::WriteFlatColumnChunk(
    const BinaryArray* data, int64_t offset, int64_t length) {
  ColumnWriter* column_writer;
  PARQUET_CATCH_NOT_OK(column_writer = row_group_writer_->NextColumn());
  DCHECK((offset + length) <= data->length());
  RETURN_NOT_OK(data_buffer_.Resize(length * sizeof(ByteArray)));
  auto buffer_ptr = reinterpret_cast<ByteArray*>(data_buffer_.mutable_data());
  // In the case of an array consisting of only empty strings or all null,
  // data->data() points already to a nullptr, thus data->data()->data() will
  // segfault.
  const uint8_t* data_ptr = nullptr;
  if (data->data()) {
    data_ptr = reinterpret_cast<const uint8_t*>(data->data()->data());
    DCHECK(data_ptr != nullptr);
  }
  auto writer = reinterpret_cast<TypedColumnWriter<ByteArrayType>*>(column_writer);
  if (writer->descr()->max_definition_level() > 0) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(length * sizeof(int16_t)));
  }
  int16_t* def_levels_ptr = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
  if (writer->descr()->max_definition_level() == 0 || data->null_count() == 0) {
    // no nulls, just dump the data
    for (int64_t i = 0; i < length; i++) {
      buffer_ptr[i] =
          ByteArray(data->value_length(i + offset), data_ptr + data->value_offset(i));
    }
    if (writer->descr()->max_definition_level() > 0) {
      std::fill(def_levels_ptr, def_levels_ptr + length, 1);
    }
    PARQUET_CATCH_NOT_OK(writer->WriteBatch(length, def_levels_ptr, nullptr, buffer_ptr));
  } else if (writer->descr()->max_definition_level() == 1) {
    int buffer_idx = 0;
    for (int64_t i = 0; i < length; i++) {
      if (data->IsNull(offset + i)) {
        def_levels_ptr[i] = 0;
      } else {
        def_levels_ptr[i] = 1;
        buffer_ptr[buffer_idx++] = ByteArray(
            data->value_length(i + offset), data_ptr + data->value_offset(i + offset));
      }
    }
    PARQUET_CATCH_NOT_OK(writer->WriteBatch(length, def_levels_ptr, nullptr, buffer_ptr));
  } else {
    return Status::NotImplemented("no support for max definition level > 1 yet");
  }
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

FileWriter::FileWriter(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer)
    : impl_(new FileWriter::Impl(pool, std::move(writer))) {}

Status FileWriter::NewRowGroup(int64_t chunk_size) {
  return impl_->NewRowGroup(chunk_size);
}

Status FileWriter::WriteFlatColumnChunk(
    const ::arrow::Array* array, int64_t offset, int64_t length) {
  int64_t real_length = length;
  if (length == -1) { real_length = array->length(); }
  if (array->type_enum() == ::arrow::Type::STRING ||
      array->type_enum() == ::arrow::Type::BINARY) {
    auto binary_array = static_cast<const ::arrow::BinaryArray*>(array);
    DCHECK(binary_array);
    return impl_->WriteFlatColumnChunk(binary_array, offset, real_length);
  } else {
    auto primitive_array = dynamic_cast<const PrimitiveArray*>(array);
    if (!primitive_array) {
      return Status::NotImplemented("Table must consist of PrimitiveArray instances");
    }
    return impl_->WriteFlatColumnChunk(primitive_array, offset, real_length);
  }
}

Status FileWriter::Close() {
  return impl_->Close();
}

MemoryPool* FileWriter::memory_pool() const {
  return impl_->pool_;
}

FileWriter::~FileWriter() {}

Status WriteFlatTable(const Table* table, MemoryPool* pool,
    const std::shared_ptr<OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties) {
  std::shared_ptr<SchemaDescriptor> parquet_schema;
  RETURN_NOT_OK(
      ToParquetSchema(table->schema().get(), *properties.get(), &parquet_schema));
  auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());
  std::unique_ptr<ParquetFileWriter> parquet_writer =
      ParquetFileWriter::Open(sink, schema_node, properties);
  FileWriter writer(pool, std::move(parquet_writer));

  // TODO(ARROW-232) Support writing chunked arrays.
  for (int i = 0; i < table->num_columns(); i++) {
    if (table->column(i)->data()->num_chunks() != 1) {
      return Status::NotImplemented("No support for writing chunked arrays yet.");
    }
  }

  for (int chunk = 0; chunk * chunk_size < table->num_rows(); chunk++) {
    int64_t offset = chunk * chunk_size;
    int64_t size = std::min(chunk_size, table->num_rows() - offset);
    RETURN_NOT_OK_ELSE(writer.NewRowGroup(size), PARQUET_IGNORE_NOT_OK(writer.Close()));
    for (int i = 0; i < table->num_columns(); i++) {
      std::shared_ptr<::arrow::Array> array = table->column(i)->data()->chunk(0);
      RETURN_NOT_OK_ELSE(writer.WriteFlatColumnChunk(array.get(), offset, size),
          PARQUET_IGNORE_NOT_OK(writer.Close()));
    }
  }

  return writer.Close();
}

Status WriteFlatTable(const Table* table, MemoryPool* pool,
    const std::shared_ptr<::arrow::io::OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties) {
  auto wrapper = std::make_shared<ArrowOutputStream>(sink);
  return WriteFlatTable(table, pool, wrapper, chunk_size, properties);
}

}  // namespace arrow

}  // namespace parquet

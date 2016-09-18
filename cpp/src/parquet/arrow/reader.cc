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

#include "parquet/arrow/reader.h"

#include <algorithm>
#include <queue>
#include <string>
#include <vector>

#include "parquet/arrow/io.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/utils.h"

#include "arrow/column.h"
#include "arrow/schema.h"
#include "arrow/table.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/util/status.h"

using arrow::Array;
using arrow::Column;
using arrow::Field;
using arrow::MemoryPool;
using arrow::PoolBuffer;
using arrow::Status;
using arrow::Table;

// Help reduce verbosity
using ParquetRAS = parquet::RandomAccessSource;
using ParquetReader = parquet::ParquetFileReader;

namespace parquet {
namespace arrow {

template <typename ArrowType>
struct ArrowTypeTraits {
  typedef ::arrow::NumericBuilder<ArrowType> builder_type;
};

template <>
struct ArrowTypeTraits<BooleanType> {
  typedef ::arrow::BooleanBuilder builder_type;
};

template <typename ArrowType>
using BuilderType = typename ArrowTypeTraits<ArrowType>::builder_type;

class FileReader::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader);
  virtual ~Impl() {}

  bool CheckForFlatColumn(const ColumnDescriptor* descr);
  Status GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out);
  Status ReadFlatColumn(int i, std::shared_ptr<Array>* out);
  Status ReadFlatTable(std::shared_ptr<Table>* out);

 private:
  MemoryPool* pool_;
  std::unique_ptr<ParquetFileReader> reader_;
};

class FlatColumnReader::Impl {
 public:
  Impl(MemoryPool* pool, const ColumnDescriptor* descr,
      ParquetFileReader* reader, int column_index);
  virtual ~Impl() {}

  Status NextBatch(int batch_size, std::shared_ptr<Array>* out);
  template <typename ArrowType, typename ParquetType>
  Status TypedReadBatch(int batch_size, std::shared_ptr<Array>* out);

  template <typename ArrowType, typename ParquetType>
  Status ReadNullableFlatBatch(const int16_t* def_levels,
      typename ParquetType::c_type* values, int64_t values_read, int64_t levels_read,
      BuilderType<ArrowType>* builder);
  template <typename ArrowType, typename ParquetType>
  Status ReadNonNullableBatch(typename ParquetType::c_type* values, int64_t values_read,
      BuilderType<ArrowType>* builder);

 private:
  void NextRowGroup();

  template <typename InType, typename OutType>
  struct can_copy_ptr {
    static constexpr bool value =
        std::is_same<InType, OutType>::value ||
        (std::is_integral<InType>{} && std::is_integral<OutType>{} &&
            (sizeof(InType) == sizeof(OutType)));
  };

  template <typename InType, typename OutType,
      typename std::enable_if<can_copy_ptr<InType, OutType>::value>::type* = nullptr>
  Status ConvertPhysicalType(
      const InType* in_ptr, int64_t length, const OutType** out_ptr) {
    *out_ptr = reinterpret_cast<const OutType*>(in_ptr);
    return Status::OK();
  }

  template <typename InType, typename OutType,
      typename std::enable_if<not can_copy_ptr<InType, OutType>::value>::type* = nullptr>
  Status ConvertPhysicalType(
      const InType* in_ptr, int64_t length, const OutType** out_ptr) {
    RETURN_NOT_OK(values_builder_buffer_.Resize(length * sizeof(OutType)));
    OutType* mutable_out_ptr =
        reinterpret_cast<OutType*>(values_builder_buffer_.mutable_data());
    std::copy(in_ptr, in_ptr + length, mutable_out_ptr);
    *out_ptr = mutable_out_ptr;
    return Status::OK();
  }

  MemoryPool* pool_;
  const ColumnDescriptor* descr_;
  ParquetFileReader* reader_;
  int column_index_;
  int next_row_group_;
  std::shared_ptr<ColumnReader> column_reader_;
  std::shared_ptr<Field> field_;

  PoolBuffer values_buffer_;
  PoolBuffer def_levels_buffer_;
  PoolBuffer values_builder_buffer_;
  PoolBuffer valid_bytes_buffer_;
};

FileReader::Impl::Impl(
    MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader)
    : pool_(pool), reader_(std::move(reader)) {}

bool FileReader::Impl::CheckForFlatColumn(const ColumnDescriptor* descr) {
  if ((descr->max_repetition_level() > 0) || (descr->max_definition_level() > 1)) {
    return false;
  } else if ((descr->max_definition_level() == 1) &&
             (descr->schema_node()->repetition() != Repetition::OPTIONAL)) {
    return false;
  }
  return true;
}

Status FileReader::Impl::GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out) {
  const SchemaDescriptor* schema = reader_->metadata()->schema();

  if (!CheckForFlatColumn(schema->Column(i))) {
    return Status::Invalid("The requested column is not flat");
  }
  std::unique_ptr<FlatColumnReader::Impl> impl(
      new FlatColumnReader::Impl(pool_, schema->Column(i), reader_.get(), i));
  *out = std::unique_ptr<FlatColumnReader>(new FlatColumnReader(std::move(impl)));
  return Status::OK();
}

Status FileReader::Impl::ReadFlatColumn(int i, std::shared_ptr<Array>* out) {
  std::unique_ptr<FlatColumnReader> flat_column_reader;
  RETURN_NOT_OK(GetFlatColumn(i, &flat_column_reader));
  return flat_column_reader->NextBatch(reader_->metadata()->num_rows(), out);
}

Status FileReader::Impl::ReadFlatTable(std::shared_ptr<Table>* table) {
  auto descr = reader_->metadata()->schema();

  const std::string& name = descr->name();
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(FromParquetSchema(descr, &schema));

  int num_columns = reader_->metadata()->num_columns();

  std::vector<std::shared_ptr<Column>> columns(num_columns);
  for (int i = 0; i < num_columns; i++) {
    std::shared_ptr<Array> array;
    RETURN_NOT_OK(ReadFlatColumn(i, &array));
    columns[i] = std::make_shared<Column>(schema->field(i), array);
  }

  *table = std::make_shared<Table>(name, schema, columns);
  return Status::OK();
}

FileReader::FileReader(
    MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader)
    : impl_(new FileReader::Impl(pool, std::move(reader))) {}

FileReader::~FileReader() {}

// Static ctor
Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
    ParquetAllocator* allocator, std::unique_ptr<FileReader>* reader) {
  std::unique_ptr<ParquetReadSource> source(new ParquetReadSource(allocator));
  RETURN_NOT_OK(source->Open(file));

  // TODO(wesm): reader properties
  std::unique_ptr<ParquetReader> pq_reader;
  PARQUET_CATCH_NOT_OK(pq_reader = ParquetReader::Open(std::move(source)));

  // Use the same memory pool as the ParquetAllocator
  reader->reset(new FileReader(allocator->pool(), std::move(pq_reader)));
  return Status::OK();
}

Status FileReader::GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out) {
  return impl_->GetFlatColumn(i, out);
}

Status FileReader::ReadFlatColumn(int i, std::shared_ptr<Array>* out) {
  return impl_->ReadFlatColumn(i, out);
}

Status FileReader::ReadFlatTable(std::shared_ptr<Table>* out) {
  return impl_->ReadFlatTable(out);
}

FlatColumnReader::Impl::Impl(MemoryPool* pool, const ColumnDescriptor* descr,
    ParquetFileReader* reader, int column_index)
    : pool_(pool),
      descr_(descr),
      reader_(reader),
      column_index_(column_index),
      next_row_group_(0),
      values_buffer_(pool),
      def_levels_buffer_(pool) {
  NodeToField(descr_->schema_node(), &field_);
  NextRowGroup();
}

template <typename ArrowType, typename ParquetType>
Status FlatColumnReader::Impl::ReadNonNullableBatch(typename ParquetType::c_type* values,
    int64_t values_read, BuilderType<ArrowType>* builder) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;

  DCHECK(builder);
  const ArrowCType* values_ptr = nullptr;
  RETURN_NOT_OK(
      (ConvertPhysicalType<ParquetCType, ArrowCType>(values, values_read, &values_ptr)));
  RETURN_NOT_OK(builder->Append(values_ptr, values_read));
  return Status::OK();
}

template <typename ArrowType, typename ParquetType>
Status FlatColumnReader::Impl::ReadNullableFlatBatch(const int16_t* def_levels,
    typename ParquetType::c_type* values, int64_t values_read, int64_t levels_read,
    BuilderType<ArrowType>* builder) {
  using ArrowCType = typename ArrowType::c_type;

  DCHECK(builder);
  RETURN_NOT_OK(values_builder_buffer_.Resize(levels_read * sizeof(ArrowCType)));
  RETURN_NOT_OK(valid_bytes_buffer_.Resize(levels_read * sizeof(uint8_t)));
  auto values_ptr = reinterpret_cast<ArrowCType*>(values_builder_buffer_.mutable_data());
  uint8_t* valid_bytes = valid_bytes_buffer_.mutable_data();
  int values_idx = 0;
  for (int64_t i = 0; i < levels_read; i++) {
    if (def_levels[i] < descr_->max_definition_level()) {
      valid_bytes[i] = 0;
    } else {
      valid_bytes[i] = 1;
      values_ptr[i] = values[values_idx++];
    }
  }
  RETURN_NOT_OK(builder->Append(values_ptr, levels_read, valid_bytes));
  return Status::OK();
}

template <typename ArrowType, typename ParquetType>
Status FlatColumnReader::Impl::TypedReadBatch(
    int batch_size, std::shared_ptr<Array>* out) {
  using ParquetCType = typename ParquetType::c_type;

  int values_to_read = batch_size;
  BuilderType<ArrowType> builder(pool_, field_->type);
  while ((values_to_read > 0) && column_reader_) {
    values_buffer_.Resize(values_to_read * sizeof(ParquetCType));
    if (descr_->max_definition_level() > 0) {
      def_levels_buffer_.Resize(values_to_read * sizeof(int16_t));
    }
    auto reader = dynamic_cast<TypedColumnReader<ParquetType>*>(column_reader_.get());
    int64_t values_read;
    int64_t levels_read;
    int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    auto values = reinterpret_cast<ParquetCType*>(values_buffer_.mutable_data());
    PARQUET_CATCH_NOT_OK(levels_read = reader->ReadBatch(
                             values_to_read, def_levels, nullptr, values, &values_read));
    values_to_read -= levels_read;
    if (descr_->max_definition_level() == 0) {
      RETURN_NOT_OK(
          (ReadNonNullableBatch<ArrowType, ParquetType>(values, values_read, &builder)));
    } else {
      // As per the defintion and checks for flat columns:
      // descr_->max_definition_level() == 1
      RETURN_NOT_OK((ReadNullableFlatBatch<ArrowType, ParquetType>(
          def_levels, values, values_read, levels_read, &builder)));
    }
    if (!column_reader_->HasNext()) { NextRowGroup(); }
  }
  *out = builder.Finish();
  return Status::OK();
}

template <>
Status FlatColumnReader::Impl::TypedReadBatch<::arrow::StringType, ByteArrayType>(
    int batch_size, std::shared_ptr<Array>* out) {
  int values_to_read = batch_size;
  ::arrow::StringBuilder builder(pool_, field_->type);
  while ((values_to_read > 0) && column_reader_) {
    values_buffer_.Resize(values_to_read * sizeof(ByteArray));
    if (descr_->max_definition_level() > 0) {
      def_levels_buffer_.Resize(values_to_read * sizeof(int16_t));
    }
    auto reader =
        dynamic_cast<TypedColumnReader<ByteArrayType>*>(column_reader_.get());
    int64_t values_read;
    int64_t levels_read;
    int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    auto values = reinterpret_cast<ByteArray*>(values_buffer_.mutable_data());
    PARQUET_CATCH_NOT_OK(levels_read = reader->ReadBatch(
                             values_to_read, def_levels, nullptr, values, &values_read));
    values_to_read -= levels_read;
    if (descr_->max_definition_level() == 0) {
      for (int64_t i = 0; i < levels_read; i++) {
        RETURN_NOT_OK(
            builder.Append(reinterpret_cast<const char*>(values[i].ptr), values[i].len));
      }
    } else {
      // descr_->max_definition_level() == 1
      int values_idx = 0;
      for (int64_t i = 0; i < levels_read; i++) {
        if (def_levels[i] < descr_->max_definition_level()) {
          RETURN_NOT_OK(builder.AppendNull());
        } else {
          RETURN_NOT_OK(
              builder.Append(reinterpret_cast<const char*>(values[values_idx].ptr),
                  values[values_idx].len));
          values_idx++;
        }
      }
    }
    if (!column_reader_->HasNext()) { NextRowGroup(); }
  }
  *out = builder.Finish();
  return Status::OK();
}

#define TYPED_BATCH_CASE(ENUM, ArrowType, ParquetType)              \
  case ::arrow::Type::ENUM:                                         \
    return TypedReadBatch<ArrowType, ParquetType>(batch_size, out); \
    break;

Status FlatColumnReader::Impl::NextBatch(int batch_size, std::shared_ptr<Array>* out) {
  if (!column_reader_) {
    // Exhausted all row groups.
    *out = nullptr;
    return Status::OK();
  }

  switch (field_->type->type) {
    TYPED_BATCH_CASE(BOOL, ::arrow::BooleanType, BooleanType)
    TYPED_BATCH_CASE(UINT8, ::arrow::UInt8Type, Int32Type)
    TYPED_BATCH_CASE(INT8, ::arrow::Int8Type, Int32Type)
    TYPED_BATCH_CASE(UINT16, ::arrow::UInt16Type, Int32Type)
    TYPED_BATCH_CASE(INT16, ::arrow::Int16Type, Int32Type)
    TYPED_BATCH_CASE(UINT32, ::arrow::UInt32Type, Int32Type)
    TYPED_BATCH_CASE(INT32, ::arrow::Int32Type, Int32Type)
    TYPED_BATCH_CASE(UINT64, ::arrow::UInt64Type, Int64Type)
    TYPED_BATCH_CASE(INT64, ::arrow::Int64Type, Int64Type)
    TYPED_BATCH_CASE(FLOAT, ::arrow::FloatType, FloatType)
    TYPED_BATCH_CASE(DOUBLE, ::arrow::DoubleType, DoubleType)
    TYPED_BATCH_CASE(STRING, ::arrow::StringType, ByteArrayType)
    TYPED_BATCH_CASE(TIMESTAMP, ::arrow::TimestampType, Int64Type)
    default:
      return Status::NotImplemented(field_->type->ToString());
  }
}

void FlatColumnReader::Impl::NextRowGroup() {
  if (next_row_group_ < reader_->metadata()->num_row_groups()) {
    column_reader_ = reader_->RowGroup(next_row_group_)->Column(column_index_);
    next_row_group_++;
  } else {
    column_reader_ = nullptr;
  }
}

FlatColumnReader::FlatColumnReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FlatColumnReader::~FlatColumnReader() {}

Status FlatColumnReader::NextBatch(int batch_size, std::shared_ptr<Array>* out) {
  return impl_->NextBatch(batch_size, out);
}

}  // namespace arrow
}  // namespace parquet

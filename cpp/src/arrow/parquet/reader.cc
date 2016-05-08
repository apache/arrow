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

#include "arrow/parquet/reader.h"

#include <queue>

#include "arrow/parquet/schema.h"
#include "arrow/schema.h"
#include "arrow/types/primitive.h"
#include "arrow/util/status.h"

using parquet::ColumnReader;
using parquet::TypedColumnReader;

namespace arrow {
namespace parquet {

class FileReader::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<::parquet::ParquetFileReader> reader);
  virtual ~Impl() {}

  Status GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out);

 private:
  MemoryPool* pool_;
  std::unique_ptr<::parquet::ParquetFileReader> reader_;
};

class FlatColumnReader::Impl {
 public:
  Impl(MemoryPool* pool, const ::parquet::ColumnDescriptor* descr,
      std::queue<std::shared_ptr<ColumnReader>>&& column_readers);
  virtual ~Impl() {}

  Status NextBatch(int batch_size, std::shared_ptr<Array>* out);
  template <typename ArrowType, typename ParquetType, typename CType>
  Status TypedReadBatch(int batch_size, std::shared_ptr<Array>* out);

 private:
  MemoryPool* pool_;
  const ::parquet::ColumnDescriptor* descr_;
  std::queue<std::shared_ptr<ColumnReader>> column_readers_;
  std::shared_ptr<Field> field_;
};

FileReader::Impl::Impl(
    MemoryPool* pool, std::unique_ptr<::parquet::ParquetFileReader> reader)
    : pool_(pool), reader_(std::move(reader)) {}

Status FileReader::Impl::GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out) {
  std::queue<std::shared_ptr<ColumnReader>> column_readers;
  for (int rg = 0; rg < reader_->num_row_groups(); rg++) {
    column_readers.push(reader_->RowGroup(rg)->Column(i));
  }
  std::unique_ptr<FlatColumnReader::Impl> impl(new FlatColumnReader::Impl(
      pool_, reader_->descr()->Column(i), std::move(column_readers)));
  *out = std::unique_ptr<FlatColumnReader>(new FlatColumnReader(std::move(impl)));
  return Status::OK();
}

FileReader::FileReader(
    MemoryPool* pool, std::unique_ptr<::parquet::ParquetFileReader> reader)
    : impl_(new FileReader::Impl(pool, std::move(reader))) {}

FileReader::~FileReader() {}

Status FileReader::GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out) {
  return impl_->GetFlatColumn(i, out);
}

Status FileReader::ReadFlatColumn(int i, std::shared_ptr<Array>* out) {
  return Status::OK();
}

FlatColumnReader::Impl::Impl(MemoryPool* pool, const ::parquet::ColumnDescriptor* descr,
    std::queue<std::shared_ptr<ColumnReader>>&& column_readers)
    : pool_(pool), descr_(descr), column_readers_(column_readers) {
  NodeToField(descr_->schema_node(), &field_);
}

template <typename ArrowType, typename ParquetType, typename CType>
Status FlatColumnReader::Impl::TypedReadBatch(
    int batch_size, std::shared_ptr<Array>* out) {
  int values_to_read = batch_size;
  NumericBuilder<ArrowType> builder(pool_, field_->type);
  while ((values_to_read > 0) && (column_readers_.size() > 0)) {
    // TODO: This is a lot malloc-thresing and not using the memory pool.
    std::vector<CType> values(values_to_read);
    std::vector<int16_t> def_levels(values_to_read);
    auto reader =
        dynamic_cast<TypedColumnReader<ParquetType>*>(column_readers_.front().get());
    int64_t values_read;
    values_to_read -= reader->ReadBatch(
        values_to_read, def_levels.data(), nullptr, values.data(), &values_read);
    if (descr_->max_definition_level() == 0) {
      builder.Append(values.data(), values_read);
    } else {
      return Status::NotImplemented("no support for definition levels yet");
    }
    if (!column_readers_.front()->HasNext()) { column_readers_.pop(); }
  }
  *out = builder.Finish();
  return Status::OK();
}

#define TYPED_BATCH_CASE(ENUM, ArrowType, ParquetType, CType)              \
  case Type::ENUM:                                                         \
    return TypedReadBatch<ArrowType, ParquetType, CType>(batch_size, out); \
    break;

Status FlatColumnReader::Impl::NextBatch(int batch_size, std::shared_ptr<Array>* out) {
  if (column_readers_.size() == 0) {
    // Exhausted all readers.
    *out = std::shared_ptr<Array>(nullptr);
  }

  if (descr_->max_repetition_level() > 0) {
    return Status::NotImplemented("no support for repetition yet");
  }

  *out = std::shared_ptr<Array>(nullptr);
  switch (field_->type->type) {
    TYPED_BATCH_CASE(INT32, Int32Type, ::parquet::Int32Type, int32_t)
    TYPED_BATCH_CASE(INT64, Int64Type, ::parquet::Int64Type, int64_t)
    TYPED_BATCH_CASE(FLOAT, FloatType, ::parquet::FloatType, float)
    TYPED_BATCH_CASE(DOUBLE, DoubleType, ::parquet::DoubleType, double)
    default:
      return Status::NotImplemented(field_->type->ToString());
  }
}

FlatColumnReader::FlatColumnReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FlatColumnReader::~FlatColumnReader() {}

Status FlatColumnReader::NextBatch(int batch_size, std::shared_ptr<Array>* out) {
  return impl_->NextBatch(batch_size, out);
}

}  // namespace parquet
}  // namespace arrow

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

#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "arrow/io/interfaces.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "jni/parquet/adapter.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/schema.h"

namespace jni {
namespace parquet {
namespace adapters {

using RecordBatchReader = arrow::RecordBatchReader;
using Table = arrow::Table;

class ParquetFileReader::Impl {
 public:
  Impl() = default;
  ~Impl() {}

  Status Open(std::shared_ptr<RandomAccessFile>& file, MemoryPool* pool,
              ::parquet::ArrowReaderProperties properties) {
    file_ = file;
    RETURN_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(file_), properties, &parquet_reader_));
    return Status::OK();
  }

  Status InitRecordBatchReader(const std::vector<int>& column_indices,
                               const std::vector<int>& row_group_indices) {
    RETURN_NOT_OK(
        GetRecordBatchReader(row_group_indices, column_indices, &record_batch_reader_));
    return Status::OK();
  }

  Status InitRecordBatchReader(const std::vector<int>& column_indices, int64_t start_pos,
                               int64_t end_pos) {
    std::vector<int> row_group_indices =
        GetRowGroupIndices(parquet_reader_->num_row_groups(), start_pos, end_pos);
    RETURN_NOT_OK(InitRecordBatchReader(column_indices, row_group_indices));
    return Status::OK();
  }

  Status ReadSchema(std::shared_ptr<Schema>* out) {
    RETURN_NOT_OK(parquet_reader_->GetSchema(out));
    return Status::OK();
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) {
    RETURN_NOT_OK(record_batch_reader_->ReadNext(out));
    return Status::OK();
  }

 private:
  std::shared_ptr<RandomAccessFile> file_;
  std::unique_ptr<::parquet::arrow::FileReader> parquet_reader_;
  std::shared_ptr<RecordBatchReader> record_batch_reader_;

  Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                              const std::vector<int>& column_indices,
                              std::shared_ptr<RecordBatchReader>* rb_reader) {
    if (column_indices.empty()) {
      return parquet_reader_->GetRecordBatchReader(row_group_indices, rb_reader);
    } else {
      return parquet_reader_->GetRecordBatchReader(row_group_indices, column_indices,
                                                   rb_reader);
    }
  }

  std::vector<int> GetRowGroupIndices(int num_row_groups, int64_t start_pos,
                                      int64_t end_pos) {
    std::unique_ptr<::parquet::ParquetFileReader> reader =
        ::parquet::ParquetFileReader::Open(file_);
    std::vector<int> row_group_indices;
    int64_t pos = 0;
    for (int i = 0; i < num_row_groups; i++) {
      if (pos >= start_pos && pos < end_pos) {
        row_group_indices.push_back(i);
        break;
      }
      pos += reader->RowGroup(i)->metadata()->total_byte_size();
    }
    if (row_group_indices.empty()) {
      row_group_indices.push_back(num_row_groups - 1);
    }
    return row_group_indices;
  }
};

ParquetFileReader::ParquetFileReader() : impl_(new Impl()) {}

ParquetFileReader::~ParquetFileReader() {}

Status ParquetFileReader::Open(std::shared_ptr<RandomAccessFile>& file, MemoryPool* pool,
                               ::parquet::ArrowReaderProperties properties,
                               std::unique_ptr<ParquetFileReader>* reader) {
  auto result = std::unique_ptr<ParquetFileReader>(new ParquetFileReader());
  RETURN_NOT_OK(result->impl_->Open(file, pool, properties));
  *reader = std::move(result);
  return Status::OK();
}

Status ParquetFileReader::Open(std::shared_ptr<RandomAccessFile>& file, MemoryPool* pool,
                               std::unique_ptr<ParquetFileReader>* reader) {
  ::parquet::ArrowReaderProperties properties(true);
  return Open(file, pool, properties, reader);
}

Status ParquetFileReader::InitRecordBatchReader(
    const std::vector<int>& column_indices, const std::vector<int>& row_group_indices) {
  return impl_->InitRecordBatchReader(column_indices, row_group_indices);
}

Status ParquetFileReader::InitRecordBatchReader(const std::vector<int>& column_indices,
                                                int64_t start_pos, int64_t end_pos) {
  return impl_->InitRecordBatchReader(column_indices, start_pos, end_pos);
}

Status ParquetFileReader::ReadSchema(std::shared_ptr<Schema>* out) {
  return impl_->ReadSchema(out);
}

Status ParquetFileReader::ReadNext(std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadNext(out);
}

class ParquetFileWriter::Impl {
 public:
  Impl() = default;
  ~Impl() {}

  Status Open(std::shared_ptr<OutputStream>& output_stream, MemoryPool* pool,
              std::shared_ptr<Schema> schema,
              std::shared_ptr<::parquet::ArrowWriterProperties> properties) {
    output_stream_ = output_stream;
    schema_ = schema;
    std::shared_ptr<::parquet::schema::GroupNode> parquet_schema;
    RETURN_NOT_OK(GetParquetSchema(schema, &parquet_schema));
    RETURN_NOT_OK(::parquet::arrow::FileWriter::Make(
        pool, ::parquet::ParquetFileWriter::Open(output_stream, parquet_schema), schema,
        properties, &parquet_writer_));
    return Status::OK();
  }

  Status WriteNext(std::shared_ptr<RecordBatch> in) {
    std::lock_guard<std::mutex> lck(thread_mtx_);
    record_batch_buffer_list_.push_back(in);
    return Status::OK();
  }

  Status Flush() {
    std::shared_ptr<Table> table;
    RETURN_NOT_OK(Table::FromRecordBatches(record_batch_buffer_list_, &table));
    RETURN_NOT_OK(parquet_writer_->WriteTable(*table.get(), table->num_rows()));
    RETURN_NOT_OK(output_stream_->Flush());
    return Status::OK();
  }

  Status GetSchema(std::shared_ptr<Schema>* out) {
    *out = schema_;
    return Status::OK();
  }

 private:
  MemoryPool* pool_;
  std::shared_ptr<Schema> schema_;
  std::mutex thread_mtx_;
  std::shared_ptr<OutputStream> output_stream_;
  std::unique_ptr<::parquet::arrow::FileWriter> parquet_writer_;
  std::vector<std::shared_ptr<::arrow::RecordBatch>> record_batch_buffer_list_;

  Status GetParquetSchema(std::shared_ptr<Schema> schema,
                          std::shared_ptr<::parquet::schema::GroupNode>* parquet_schema) {
    std::shared_ptr<::parquet::WriterProperties> properties =
        ::parquet::default_writer_properties();
    std::shared_ptr<::parquet::SchemaDescriptor> schema_description;
    RETURN_NOT_OK(::parquet::arrow::ToParquetSchema(schema.get(), *properties.get(),
                                                    &schema_description));

    ::parquet::schema::NodeVector group_node_fields;
    for (int i = 0; i < schema_description->group_node()->field_count(); i++) {
      group_node_fields.push_back(schema_description->group_node()->field(i));
    }
    *parquet_schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
        ::parquet::schema::GroupNode::Make(
            schema_description->schema_root()->name(),
            schema_description->schema_root()->repetition(), group_node_fields));

    return Status::OK();
  }
};

ParquetFileWriter::ParquetFileWriter() : impl_(new Impl()) {}
ParquetFileWriter::~ParquetFileWriter() {}

Status ParquetFileWriter::Open(
    std::shared_ptr<OutputStream>& output_stream, MemoryPool* pool,
    std::shared_ptr<Schema> schema,
    std::shared_ptr<::parquet::ArrowWriterProperties> properties,
    std::unique_ptr<ParquetFileWriter>* writer) {
  auto result = std::unique_ptr<ParquetFileWriter>(new ParquetFileWriter());
  RETURN_NOT_OK(result->impl_->Open(output_stream, pool, schema, properties));
  *writer = std::move(result);
  return Status::OK();
}

Status ParquetFileWriter::Open(std::shared_ptr<OutputStream>& output_stream,
                               MemoryPool* pool, std::shared_ptr<Schema> schema,
                               std::unique_ptr<ParquetFileWriter>* writer) {
  return Open(output_stream, pool, schema, ::parquet::default_arrow_writer_properties(),
              writer);
}

Status ParquetFileWriter::WriteNext(std::shared_ptr<RecordBatch> in) {
  return impl_->WriteNext(in);
}

Status ParquetFileWriter::Flush() { return impl_->Flush(); }

Status ParquetFileWriter::GetSchema(std::shared_ptr<Schema>* out) {
  return impl_->GetSchema(out);
}

}  // namespace adapters
}  // namespace parquet
}  // namespace jni

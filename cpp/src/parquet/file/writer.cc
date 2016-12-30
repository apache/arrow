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

#include "parquet/file/writer.h"

#include "parquet/file/writer-internal.h"
#include "parquet/util/memory.h"

using parquet::schema::GroupNode;

namespace parquet {

// ----------------------------------------------------------------------
// RowGroupWriter public API

RowGroupWriter::RowGroupWriter(std::unique_ptr<Contents> contents)
    : contents_(std::move(contents)) {}

void RowGroupWriter::Close() {
  if (contents_) {
    contents_->Close();
    contents_.reset();
  }
}

ColumnWriter* RowGroupWriter::NextColumn() {
  return contents_->NextColumn();
}

// ----------------------------------------------------------------------
// ParquetFileWriter public API

ParquetFileWriter::ParquetFileWriter() {}

ParquetFileWriter::~ParquetFileWriter() {
  Close();
}

std::unique_ptr<ParquetFileWriter> ParquetFileWriter::Open(
    const std::shared_ptr<::arrow::io::OutputStream>& sink,
    const std::shared_ptr<GroupNode>& schema,
    const std::shared_ptr<WriterProperties>& properties) {
  return Open(std::make_shared<ArrowOutputStream>(sink), schema, properties);
}

std::unique_ptr<ParquetFileWriter> ParquetFileWriter::Open(
    const std::shared_ptr<OutputStream>& sink,
    const std::shared_ptr<schema::GroupNode>& schema,
    const std::shared_ptr<WriterProperties>& properties) {
  auto contents = FileSerializer::Open(sink, schema, properties);
  std::unique_ptr<ParquetFileWriter> result(new ParquetFileWriter());
  result->Open(std::move(contents));
  return result;
}

const SchemaDescriptor* ParquetFileWriter::schema() const {
  return contents_->schema();
}

const ColumnDescriptor* ParquetFileWriter::descr(int i) const {
  return contents_->schema()->Column(i);
}

void ParquetFileWriter::Open(std::unique_ptr<ParquetFileWriter::Contents> contents) {
  contents_ = std::move(contents);
}

void ParquetFileWriter::Close() {
  if (contents_) {
    contents_->Close();
    contents_.reset();
  }
}

RowGroupWriter* ParquetFileWriter::AppendRowGroup(int64_t num_rows) {
  return contents_->AppendRowGroup(num_rows);
}

const std::shared_ptr<WriterProperties>& ParquetFileWriter::properties() const {
  return contents_->properties();
}

}  // namespace parquet

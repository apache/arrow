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

#include "parquet/file_writer.h"

#include <utility>
#include <vector>

#include "parquet/column_writer.h"
#include "parquet/deprecated_io.h"
#include "parquet/platform.h"
#include "parquet/schema.h"

using arrow::MemoryPool;

using parquet::schema::GroupNode;

namespace parquet {

// FIXME: copied from reader-internal.cc
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

// ----------------------------------------------------------------------
// RowGroupWriter public API

RowGroupWriter::RowGroupWriter(std::unique_ptr<Contents> contents)
    : contents_(std::move(contents)) {}

void RowGroupWriter::Close() {
  if (contents_) {
    contents_->Close();
  }
}

ColumnWriter* RowGroupWriter::NextColumn() { return contents_->NextColumn(); }

ColumnWriter* RowGroupWriter::column(int i) { return contents_->column(i); }

int64_t RowGroupWriter::total_compressed_bytes() const {
  return contents_->total_compressed_bytes();
}

int64_t RowGroupWriter::total_bytes_written() const {
  return contents_->total_bytes_written();
}

int RowGroupWriter::current_column() { return contents_->current_column(); }

int RowGroupWriter::num_columns() const { return contents_->num_columns(); }

int64_t RowGroupWriter::num_rows() const { return contents_->num_rows(); }

inline void ThrowRowsMisMatchError(int col, int64_t prev, int64_t curr) {
  std::stringstream ss;
  ss << "Column " << col << " had " << curr << " while previous column had " << prev;
  throw ParquetException(ss.str());
}

// ----------------------------------------------------------------------
// RowGroupSerializer

// RowGroupWriter::Contents implementation for the Parquet file specification
class RowGroupSerializer : public RowGroupWriter::Contents {
 public:
  RowGroupSerializer(const std::shared_ptr<ArrowOutputStream>& sink,
                     RowGroupMetaDataBuilder* metadata,
                     const WriterProperties* properties, bool buffered_row_group = false)
      : sink_(sink),
        metadata_(metadata),
        properties_(properties),
        total_bytes_written_(0),
        closed_(false),
        current_column_index_(0),
        num_rows_(0),
        buffered_row_group_(buffered_row_group) {
    if (buffered_row_group) {
      InitColumns();
    } else {
      column_writers_.push_back(nullptr);
    }
  }

  int num_columns() const override { return metadata_->num_columns(); }

  int64_t num_rows() const override {
    CheckRowsWritten();
    // CheckRowsWritten ensures num_rows_ is set correctly
    return num_rows_;
  }

  ColumnWriter* NextColumn() override {
    if (buffered_row_group_) {
      throw ParquetException(
          "NextColumn() is not supported when a RowGroup is written by size");
    }

    if (column_writers_[0]) {
      CheckRowsWritten();
    }

    // Throws an error if more columns are being written
    auto col_meta = metadata_->NextColumnChunk();

    if (column_writers_[0]) {
      total_bytes_written_ += column_writers_[0]->Close();
    }

    ++current_column_index_;

    const ColumnDescriptor* column_descr = col_meta->descr();
    std::unique_ptr<PageWriter> pager =
        PageWriter::Open(sink_, properties_->compression(column_descr->path()), col_meta,
                         properties_->memory_pool());
    column_writers_[0] = ColumnWriter::Make(col_meta, std::move(pager), properties_);
    return column_writers_[0].get();
  }

  ColumnWriter* column(int i) override {
    if (!buffered_row_group_) {
      throw ParquetException(
          "column() is only supported when a BufferedRowGroup is being written");
    }

    if (i >= 0 && i < static_cast<int>(column_writers_.size())) {
      return column_writers_[i].get();
    }
    return nullptr;
  }

  int current_column() const override { return metadata_->current_column(); }

  int64_t total_compressed_bytes() const override {
    int64_t total_compressed_bytes = 0;
    for (size_t i = 0; i < column_writers_.size(); i++) {
      if (column_writers_[i]) {
        total_compressed_bytes += column_writers_[i]->total_compressed_bytes();
      }
    }
    return total_compressed_bytes;
  }

  int64_t total_bytes_written() const override {
    int64_t total_bytes_written = 0;
    for (size_t i = 0; i < column_writers_.size(); i++) {
      if (column_writers_[i]) {
        total_bytes_written += column_writers_[i]->total_bytes_written();
      }
    }
    return total_bytes_written;
  }

  void Close() override {
    if (!closed_) {
      closed_ = true;
      CheckRowsWritten();

      for (size_t i = 0; i < column_writers_.size(); i++) {
        if (column_writers_[i]) {
          total_bytes_written_ += column_writers_[i]->Close();
          column_writers_[i].reset();
        }
      }

      column_writers_.clear();

      // Ensures all columns have been written
      metadata_->set_num_rows(num_rows_);
      metadata_->Finish(total_bytes_written_);
    }
  }

 private:
  std::shared_ptr<ArrowOutputStream> sink_;
  mutable RowGroupMetaDataBuilder* metadata_;
  const WriterProperties* properties_;
  int64_t total_bytes_written_;
  bool closed_;
  int current_column_index_;
  mutable int64_t num_rows_;
  bool buffered_row_group_;

  void CheckRowsWritten() const {
    // verify when only one column is written at a time
    if (!buffered_row_group_ && column_writers_.size() > 0 && column_writers_[0]) {
      int64_t current_col_rows = column_writers_[0]->rows_written();
      if (num_rows_ == 0) {
        num_rows_ = current_col_rows;
      } else if (num_rows_ != current_col_rows) {
        ThrowRowsMisMatchError(current_column_index_, current_col_rows, num_rows_);
      }
    } else if (buffered_row_group_ &&
               column_writers_.size() > 0) {  // when buffered_row_group = true
      int64_t current_col_rows = column_writers_[0]->rows_written();
      for (int i = 1; i < static_cast<int>(column_writers_.size()); i++) {
        int64_t current_col_rows_i = column_writers_[i]->rows_written();
        if (current_col_rows != current_col_rows_i) {
          ThrowRowsMisMatchError(i, current_col_rows_i, current_col_rows);
        }
      }
      num_rows_ = current_col_rows;
    }
  }

  void InitColumns() {
    for (int i = 0; i < num_columns(); i++) {
      auto col_meta = metadata_->NextColumnChunk();
      const ColumnDescriptor* column_descr = col_meta->descr();
      std::unique_ptr<PageWriter> pager =
          PageWriter::Open(sink_, properties_->compression(column_descr->path()),
                           col_meta, properties_->memory_pool(), buffered_row_group_);
      column_writers_.push_back(
          ColumnWriter::Make(col_meta, std::move(pager), properties_));
    }
  }

  std::vector<std::shared_ptr<ColumnWriter>> column_writers_;
};

// ----------------------------------------------------------------------
// FileSerializer

// An implementation of ParquetFileWriter::Contents that deals with the Parquet
// file structure, Thrift serialization, and other internal matters

class FileSerializer : public ParquetFileWriter::Contents {
 public:
  static std::unique_ptr<ParquetFileWriter::Contents> Open(
      const std::shared_ptr<ArrowOutputStream>& sink,
      const std::shared_ptr<GroupNode>& schema,
      const std::shared_ptr<WriterProperties>& properties,
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata) {
    std::unique_ptr<ParquetFileWriter::Contents> result(
        new FileSerializer(sink, schema, properties, key_value_metadata));

    return result;
  }

  void Close() override {
    if (is_open_) {
      // If any functions here raise an exception, we set is_open_ to be false
      // so that this does not get called again (possibly causing segfault)
      is_open_ = false;
      if (row_group_writer_) {
        num_rows_ += row_group_writer_->num_rows();
        row_group_writer_->Close();
      }
      row_group_writer_.reset();

      // Write magic bytes and metadata
      file_metadata_ = metadata_->Finish();
      WriteFileMetaData(*file_metadata_, sink_.get());
    }
  }

  int num_columns() const override { return schema_.num_columns(); }

  int num_row_groups() const override { return num_row_groups_; }

  int64_t num_rows() const override { return num_rows_; }

  const std::shared_ptr<WriterProperties>& properties() const override {
    return properties_;
  }

  RowGroupWriter* AppendRowGroup(bool buffered_row_group) {
    if (row_group_writer_) {
      row_group_writer_->Close();
    }
    num_row_groups_++;
    auto rg_metadata = metadata_->AppendRowGroup();
    std::unique_ptr<RowGroupWriter::Contents> contents(new RowGroupSerializer(
        sink_, rg_metadata, properties_.get(), buffered_row_group));
    row_group_writer_.reset(new RowGroupWriter(std::move(contents)));
    return row_group_writer_.get();
  }

  RowGroupWriter* AppendRowGroup() override { return AppendRowGroup(false); }

  RowGroupWriter* AppendBufferedRowGroup() override { return AppendRowGroup(true); }

  ~FileSerializer() override {
    try {
      Close();
    } catch (...) {
    }
  }

 private:
  FileSerializer(const std::shared_ptr<ArrowOutputStream>& sink,
                 const std::shared_ptr<GroupNode>& schema,
                 const std::shared_ptr<WriterProperties>& properties,
                 const std::shared_ptr<const KeyValueMetadata>& key_value_metadata)
      : ParquetFileWriter::Contents(schema, key_value_metadata),
        sink_(sink),
        is_open_(true),
        properties_(properties),
        num_row_groups_(0),
        num_rows_(0),
        metadata_(FileMetaDataBuilder::Make(&schema_, properties, key_value_metadata)) {
    StartFile();
  }

  std::shared_ptr<ArrowOutputStream> sink_;
  bool is_open_;
  const std::shared_ptr<WriterProperties> properties_;
  int num_row_groups_;
  int64_t num_rows_;
  std::unique_ptr<FileMetaDataBuilder> metadata_;
  // Only one of the row group writers is active at a time
  std::unique_ptr<RowGroupWriter> row_group_writer_;

  void StartFile() {
    // Parquet files always start with PAR1
    PARQUET_THROW_NOT_OK(sink_->Write(PARQUET_MAGIC, 4));
  }
};

// ----------------------------------------------------------------------
// ParquetFileWriter public API

ParquetFileWriter::ParquetFileWriter() {}

ParquetFileWriter::~ParquetFileWriter() {
  try {
    Close();
  } catch (...) {
  }
}

std::unique_ptr<ParquetFileWriter> ParquetFileWriter::Open(
    const std::shared_ptr<::arrow::io::OutputStream>& sink,
    const std::shared_ptr<GroupNode>& schema,
    const std::shared_ptr<WriterProperties>& properties,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata) {
  auto contents = FileSerializer::Open(sink, schema, properties, key_value_metadata);
  std::unique_ptr<ParquetFileWriter> result(new ParquetFileWriter());
  result->Open(std::move(contents));
  return result;
}

std::unique_ptr<ParquetFileWriter> ParquetFileWriter::Open(
    const std::shared_ptr<OutputStream>& sink,
    const std::shared_ptr<schema::GroupNode>& schema,
    const std::shared_ptr<WriterProperties>& properties,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata) {
  return Open(std::make_shared<ParquetOutputWrapper>(sink), schema, properties,
              key_value_metadata);
}

void WriteFileMetaData(const FileMetaData& file_metadata, ArrowOutputStream* sink) {
  int64_t position = -1;
  PARQUET_THROW_NOT_OK(sink->Tell(&position));

  // Write MetaData
  uint32_t metadata_len = static_cast<uint32_t>(position);

  file_metadata.WriteTo(sink);
  PARQUET_THROW_NOT_OK(sink->Tell(&position));
  metadata_len = static_cast<uint32_t>(position) - metadata_len;

  // Write Footer
  PARQUET_THROW_NOT_OK(sink->Write(reinterpret_cast<uint8_t*>(&metadata_len), 4));
  PARQUET_THROW_NOT_OK(sink->Write(PARQUET_MAGIC, 4));
}

void WriteFileMetaData(const FileMetaData& file_metadata, OutputStream* sink) {
  ParquetOutputWrapper wrapper(sink);
  return WriteFileMetaData(file_metadata, &wrapper);
}

void WriteMetaDataFile(const FileMetaData& file_metadata, ArrowOutputStream* sink) {
  PARQUET_THROW_NOT_OK(sink->Write(PARQUET_MAGIC, 4));
  return WriteFileMetaData(file_metadata, sink);
}

const SchemaDescriptor* ParquetFileWriter::schema() const { return contents_->schema(); }

const ColumnDescriptor* ParquetFileWriter::descr(int i) const {
  return contents_->schema()->Column(i);
}

int ParquetFileWriter::num_columns() const { return contents_->num_columns(); }

int64_t ParquetFileWriter::num_rows() const { return contents_->num_rows(); }

int ParquetFileWriter::num_row_groups() const { return contents_->num_row_groups(); }

const std::shared_ptr<const KeyValueMetadata>& ParquetFileWriter::key_value_metadata()
    const {
  return contents_->key_value_metadata();
}

const std::shared_ptr<FileMetaData> ParquetFileWriter::metadata() const {
  return file_metadata_;
}

void ParquetFileWriter::Open(std::unique_ptr<ParquetFileWriter::Contents> contents) {
  contents_ = std::move(contents);
}

void ParquetFileWriter::Close() {
  if (contents_) {
    contents_->Close();
    file_metadata_ = contents_->metadata();
    contents_.reset();
  }
}

RowGroupWriter* ParquetFileWriter::AppendRowGroup() {
  return contents_->AppendRowGroup();
}

RowGroupWriter* ParquetFileWriter::AppendBufferedRowGroup() {
  return contents_->AppendBufferedRowGroup();
}

RowGroupWriter* ParquetFileWriter::AppendRowGroup(int64_t num_rows) {
  return AppendRowGroup();
}

const std::shared_ptr<WriterProperties>& ParquetFileWriter::properties() const {
  return contents_->properties();
}

}  // namespace parquet

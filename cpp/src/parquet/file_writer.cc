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

#include <cstddef>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "parquet/column_writer.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/internal_file_encryptor.h"
#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using arrow::MemoryPool;

using parquet::schema::GroupNode;

namespace parquet {

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
  RowGroupSerializer(std::shared_ptr<ArrowOutputStream> sink,
                     RowGroupMetaDataBuilder* metadata, int16_t row_group_ordinal,
                     const WriterProperties* properties, bool buffered_row_group = false,
                     InternalFileEncryptor* file_encryptor = nullptr)
      : sink_(std::move(sink)),
        metadata_(metadata),
        properties_(properties),
        total_bytes_written_(0),
        closed_(false),
        row_group_ordinal_(row_group_ordinal),
        next_column_index_(0),
        num_rows_(0),
        buffered_row_group_(buffered_row_group),
        file_encryptor_(file_encryptor) {
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

    ++next_column_index_;

    const auto& path = col_meta->descr()->path();
    auto meta_encryptor =
        file_encryptor_ ? file_encryptor_->GetColumnMetaEncryptor(path->ToDotString())
                        : nullptr;
    auto data_encryptor =
        file_encryptor_ ? file_encryptor_->GetColumnDataEncryptor(path->ToDotString())
                        : nullptr;
    std::unique_ptr<PageWriter> pager = PageWriter::Open(
        sink_, properties_->compression(path), properties_->compression_level(path),
        col_meta, row_group_ordinal_, static_cast<int16_t>(next_column_index_ - 1),
        properties_->memory_pool(), false, meta_encryptor, data_encryptor);
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
      metadata_->Finish(total_bytes_written_, row_group_ordinal_);
    }
  }

 private:
  std::shared_ptr<ArrowOutputStream> sink_;
  mutable RowGroupMetaDataBuilder* metadata_;
  const WriterProperties* properties_;
  int64_t total_bytes_written_;
  bool closed_;
  int16_t row_group_ordinal_;
  int next_column_index_;
  mutable int64_t num_rows_;
  bool buffered_row_group_;
  InternalFileEncryptor* file_encryptor_;

  void CheckRowsWritten() const {
    // verify when only one column is written at a time
    if (!buffered_row_group_ && column_writers_.size() > 0 && column_writers_[0]) {
      int64_t current_col_rows = column_writers_[0]->rows_written();
      if (num_rows_ == 0) {
        num_rows_ = current_col_rows;
      } else if (num_rows_ != current_col_rows) {
        ThrowRowsMisMatchError(next_column_index_, current_col_rows, num_rows_);
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
      const auto& path = col_meta->descr()->path();
      auto meta_encryptor =
          file_encryptor_ ? file_encryptor_->GetColumnMetaEncryptor(path->ToDotString())
                          : nullptr;
      auto data_encryptor =
          file_encryptor_ ? file_encryptor_->GetColumnDataEncryptor(path->ToDotString())
                          : nullptr;
      std::unique_ptr<PageWriter> pager = PageWriter::Open(
          sink_, properties_->compression(path), properties_->compression_level(path),
          col_meta, static_cast<int16_t>(row_group_ordinal_),
          static_cast<int16_t>(next_column_index_++), properties_->memory_pool(),
          buffered_row_group_, meta_encryptor, data_encryptor);
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
      std::shared_ptr<ArrowOutputStream> sink, std::shared_ptr<GroupNode> schema,
      std::shared_ptr<WriterProperties> properties,
      std::shared_ptr<const KeyValueMetadata> key_value_metadata) {
    std::unique_ptr<ParquetFileWriter::Contents> result(
        new FileSerializer(std::move(sink), std::move(schema), std::move(properties),
                           std::move(key_value_metadata)));

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
      auto file_encryption_properties = properties_->file_encryption_properties();

      if (file_encryption_properties == nullptr) {  // Non encrypted file.
        file_metadata_ = metadata_->Finish();
        WriteFileMetaData(*file_metadata_, sink_.get());
      } else {  // Encrypted file
        CloseEncryptedFile(file_encryption_properties);
      }
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
        sink_, rg_metadata, static_cast<int16_t>(num_row_groups_ - 1), properties_.get(),
        buffered_row_group, file_encryptor_.get()));
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
  FileSerializer(std::shared_ptr<ArrowOutputStream> sink,
                 std::shared_ptr<GroupNode> schema,
                 std::shared_ptr<WriterProperties> properties,
                 std::shared_ptr<const KeyValueMetadata> key_value_metadata)
      : ParquetFileWriter::Contents(std::move(schema), std::move(key_value_metadata)),
        sink_(std::move(sink)),
        is_open_(true),
        properties_(std::move(properties)),
        num_row_groups_(0),
        num_rows_(0),
        metadata_(FileMetaDataBuilder::Make(&schema_, properties_, key_value_metadata_)) {
    PARQUET_ASSIGN_OR_THROW(int64_t position, sink_->Tell());
    if (position == 0) {
      StartFile();
    } else {
      throw ParquetException("Appending to file not implemented.");
    }
  }

  void CloseEncryptedFile(FileEncryptionProperties* file_encryption_properties) {
    // Encrypted file with encrypted footer
    if (file_encryption_properties->encrypted_footer()) {
      // encrypted footer
      file_metadata_ = metadata_->Finish();

      PARQUET_ASSIGN_OR_THROW(int64_t position, sink_->Tell());
      uint64_t metadata_start = static_cast<uint64_t>(position);
      auto crypto_metadata = metadata_->GetCryptoMetaData();
      WriteFileCryptoMetaData(*crypto_metadata, sink_.get());

      auto footer_encryptor = file_encryptor_->GetFooterEncryptor();
      WriteEncryptedFileMetadata(*file_metadata_, sink_.get(), footer_encryptor, true);
      PARQUET_ASSIGN_OR_THROW(position, sink_->Tell());
      uint32_t footer_and_crypto_len = static_cast<uint32_t>(position - metadata_start);
      PARQUET_THROW_NOT_OK(
          sink_->Write(reinterpret_cast<uint8_t*>(&footer_and_crypto_len), 4));
      PARQUET_THROW_NOT_OK(sink_->Write(kParquetEMagic, 4));
    } else {  // Encrypted file with plaintext footer
      file_metadata_ = metadata_->Finish();
      auto footer_signing_encryptor = file_encryptor_->GetFooterSigningEncryptor();
      WriteEncryptedFileMetadata(*file_metadata_, sink_.get(), footer_signing_encryptor,
                                 false);
    }
    if (file_encryptor_) {
      file_encryptor_->WipeOutEncryptionKeys();
    }
  }

  std::shared_ptr<ArrowOutputStream> sink_;
  bool is_open_;
  const std::shared_ptr<WriterProperties> properties_;
  int num_row_groups_;
  int64_t num_rows_;
  std::unique_ptr<FileMetaDataBuilder> metadata_;
  // Only one of the row group writers is active at a time
  std::unique_ptr<RowGroupWriter> row_group_writer_;

  std::unique_ptr<InternalFileEncryptor> file_encryptor_;

  void StartFile() {
    auto file_encryption_properties = properties_->file_encryption_properties();
    if (file_encryption_properties == nullptr) {
      // Unencrypted parquet files always start with PAR1
      PARQUET_THROW_NOT_OK(sink_->Write(kParquetMagic, 4));
    } else {
      // Check that all columns in columnEncryptionProperties exist in the schema.
      auto encrypted_columns = file_encryption_properties->encrypted_columns();
      // if columnEncryptionProperties is empty, every column in file schema will be
      // encrypted with footer key.
      if (encrypted_columns.size() != 0) {
        std::vector<std::string> column_path_vec;
        // First, save all column paths in schema.
        for (int i = 0; i < num_columns(); i++) {
          column_path_vec.push_back(schema_.Column(i)->path()->ToDotString());
        }
        // Check if column exists in schema.
        for (const auto& elem : encrypted_columns) {
          auto it = std::find(column_path_vec.begin(), column_path_vec.end(), elem.first);
          if (it == column_path_vec.end()) {
            std::stringstream ss;
            ss << "Encrypted column " + elem.first + " not in file schema";
            throw ParquetException(ss.str());
          }
        }
      }

      file_encryptor_.reset(new InternalFileEncryptor(file_encryption_properties,
                                                      properties_->memory_pool()));
      if (file_encryption_properties->encrypted_footer()) {
        PARQUET_THROW_NOT_OK(sink_->Write(kParquetEMagic, 4));
      } else {
        // Encrypted file with plaintext footer mode.
        PARQUET_THROW_NOT_OK(sink_->Write(kParquetMagic, 4));
      }
    }
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
    std::shared_ptr<::arrow::io::OutputStream> sink, std::shared_ptr<GroupNode> schema,
    std::shared_ptr<WriterProperties> properties,
    std::shared_ptr<const KeyValueMetadata> key_value_metadata) {
  auto contents =
      FileSerializer::Open(std::move(sink), std::move(schema), std::move(properties),
                           std::move(key_value_metadata));
  std::unique_ptr<ParquetFileWriter> result(new ParquetFileWriter());
  result->Open(std::move(contents));
  return result;
}

void WriteFileMetaData(const FileMetaData& file_metadata, ArrowOutputStream* sink) {
  // Write MetaData
  PARQUET_ASSIGN_OR_THROW(int64_t position, sink->Tell());
  uint32_t metadata_len = static_cast<uint32_t>(position);

  file_metadata.WriteTo(sink);
  PARQUET_ASSIGN_OR_THROW(position, sink->Tell());
  metadata_len = static_cast<uint32_t>(position) - metadata_len;

  // Write Footer
  PARQUET_THROW_NOT_OK(sink->Write(reinterpret_cast<uint8_t*>(&metadata_len), 4));
  PARQUET_THROW_NOT_OK(sink->Write(kParquetMagic, 4));
}

void WriteMetaDataFile(const FileMetaData& file_metadata, ArrowOutputStream* sink) {
  PARQUET_THROW_NOT_OK(sink->Write(kParquetMagic, 4));
  return WriteFileMetaData(file_metadata, sink);
}

void WriteEncryptedFileMetadata(const FileMetaData& file_metadata,
                                ArrowOutputStream* sink,
                                const std::shared_ptr<Encryptor>& encryptor,
                                bool encrypt_footer) {
  if (encrypt_footer) {  // Encrypted file with encrypted footer
    // encrypt and write to sink
    file_metadata.WriteTo(sink, encryptor);
  } else {  // Encrypted file with plaintext footer mode.
    PARQUET_ASSIGN_OR_THROW(int64_t position, sink->Tell());
    uint32_t metadata_len = static_cast<uint32_t>(position);
    file_metadata.WriteTo(sink, encryptor);
    PARQUET_ASSIGN_OR_THROW(position, sink->Tell());
    metadata_len = static_cast<uint32_t>(position) - metadata_len;

    PARQUET_THROW_NOT_OK(sink->Write(reinterpret_cast<uint8_t*>(&metadata_len), 4));
    PARQUET_THROW_NOT_OK(sink->Write(kParquetMagic, 4));
  }
}

void WriteFileCryptoMetaData(const FileCryptoMetaData& crypto_metadata,
                             ArrowOutputStream* sink) {
  crypto_metadata.WriteTo(sink);
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

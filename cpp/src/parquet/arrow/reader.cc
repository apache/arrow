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
#include <cstring>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"
#include "arrow/util/thread_pool.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/column_reader.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"

using arrow::Array;
using arrow::BooleanArray;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::Field;
using arrow::Future;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::RecordBatchReader;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::StructArray;
using arrow::Table;
using arrow::TimestampArray;
using arrow::internal::Iota;

using parquet::schema::GroupNode;
using parquet::schema::Node;
using parquet::schema::PrimitiveNode;

// Help reduce verbosity
using ParquetReader = parquet::ParquetFileReader;

using parquet::internal::RecordReader;

#define BEGIN_PARQUET_CATCH_EXCEPTIONS try {
#define END_PARQUET_CATCH_EXCEPTIONS             \
  }                                              \
  catch (const ::parquet::ParquetException& e) { \
    return ::arrow::Status::IOError(e.what());   \
  }

namespace parquet {
namespace arrow {

class ColumnReaderImpl : public ColumnReader {
 public:
  enum ReaderType { PRIMITIVE, LIST, STRUCT };

  virtual Status GetDefLevels(const int16_t** data, int64_t* length) = 0;
  virtual Status GetRepLevels(const int16_t** data, int64_t* length) = 0;
  virtual const std::shared_ptr<Field> field() = 0;

  virtual const ColumnDescriptor* descr() const = 0;

  virtual ReaderType type() const = 0;
};

std::shared_ptr<std::unordered_set<int>> VectorToSharedSet(
    const std::vector<int>& values) {
  std::shared_ptr<std::unordered_set<int>> result(new std::unordered_set<int>());
  result->insert(values.begin(), values.end());
  return result;
}

// ----------------------------------------------------------------------
// FileReaderImpl forward declaration

class FileReaderImpl : public FileReader {
 public:
  FileReaderImpl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader,
                 const ArrowReaderProperties& properties)
      : pool_(pool), reader_(std::move(reader)), reader_properties_(properties) {}

  Status Init() {
    return SchemaManifest::Make(reader_->metadata()->schema(),
                                reader_->metadata()->key_value_metadata(),
                                reader_properties_, &manifest_);
  }

  FileColumnIteratorFactory SomeRowGroupsFactory(std::vector<int> row_groups) {
    return [row_groups](int i, ParquetFileReader* reader) {
      return new FileColumnIterator(i, reader, row_groups);
    };
  }

  FileColumnIteratorFactory AllRowGroupsFactory() {
    return SomeRowGroupsFactory(Iota(reader_->metadata()->num_row_groups()));
  }

  Status BoundsCheckColumn(int column) {
    if (column < 0 || column >= this->num_columns()) {
      return Status::Invalid("Column index out of bounds (got ", column,
                             ", should be "
                             "between 0 and ",
                             this->num_columns() - 1, ")");
    }
    return Status::OK();
  }

  Status BoundsCheckRowGroup(int row_group) {
    // row group indices check
    if (row_group < 0 || row_group >= num_row_groups()) {
      return Status::Invalid("Some index in row_group_indices is ", row_group,
                             ", which is either < 0 or >= num_row_groups(",
                             num_row_groups(), ")");
    }
    return Status::OK();
  }

  int64_t GetTotalRecords(const std::vector<int>& row_groups, int column_chunk = 0) {
    // Can throw exception
    int64_t records = 0;
    for (auto row_group : row_groups) {
      records += reader_->metadata()
                     ->RowGroup(row_group)
                     ->ColumnChunk(column_chunk)
                     ->num_values();
    }
    return records;
  }

  std::shared_ptr<RowGroupReader> RowGroup(int row_group_index) override;

  Status ReadTable(const std::vector<int>& indices,
                   std::shared_ptr<Table>* out) override {
    return ReadRowGroups(Iota(reader_->metadata()->num_row_groups()), indices, out);
  }

  Status GetFieldReader(int i,
                        const std::shared_ptr<std::unordered_set<int>>& included_leaves,
                        const std::vector<int>& row_groups,
                        std::unique_ptr<ColumnReaderImpl>* out) {
    auto ctx = std::make_shared<ReaderContext>();
    ctx->reader = reader_.get();
    ctx->pool = pool_;
    ctx->iterator_factory = SomeRowGroupsFactory(row_groups);
    ctx->filter_leaves = true;
    ctx->included_leaves = included_leaves;
    return GetReader(manifest_.schema_fields[i], ctx, out);
  }

  Status GetColumn(int i, FileColumnIteratorFactory iterator_factory,
                   std::unique_ptr<ColumnReader>* out);

  Status ReadSchemaField(int i,
                         const std::shared_ptr<std::unordered_set<int>>& included_leaves,
                         const std::vector<int>& row_groups,
                         std::shared_ptr<Field>* out_field,
                         std::shared_ptr<ChunkedArray>* out) {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    std::unique_ptr<ColumnReaderImpl> reader;
    RETURN_NOT_OK(GetFieldReader(i, included_leaves, row_groups, &reader));

    *out_field = reader->field();

    // TODO(wesm): This calculation doesn't make much sense when we have repeated
    // schema nodes
    int64_t records_to_read = GetTotalRecords(row_groups, i);
    return reader->NextBatch(records_to_read, out);
    END_PARQUET_CATCH_EXCEPTIONS
  }

  Status GetColumn(int i, std::unique_ptr<ColumnReader>* out) override {
    return GetColumn(i, AllRowGroupsFactory(), out);
  }

  Status GetSchema(std::shared_ptr<::arrow::Schema>* out) override {
    return FromParquetSchema(reader_->metadata()->schema(), reader_properties_,
                             reader_->metadata()->key_value_metadata(), out);
  }

  Status ReadSchemaField(int i,
                         const std::shared_ptr<std::unordered_set<int>>& included_leaves,
                         const std::vector<int>& row_groups,
                         std::shared_ptr<ChunkedArray>* out) {
    std::shared_ptr<Field> unused;
    return ReadSchemaField(i, included_leaves, row_groups, &unused, out);
  }

  Status ReadSchemaField(int i,
                         const std::shared_ptr<std::unordered_set<int>>& included_leaves,
                         std::shared_ptr<ChunkedArray>* out) {
    return ReadSchemaField(i, included_leaves,
                           Iota(reader_->metadata()->num_row_groups()), out);
  }

  Status ReadSchemaField(int i, std::shared_ptr<ChunkedArray>* out) override {
    auto included_leaves = VectorToSharedSet(Iota(reader_->metadata()->num_columns()));
    return ReadSchemaField(i, included_leaves,
                           Iota(reader_->metadata()->num_row_groups()), out);
  }

  Status ReadColumn(int i, const std::vector<int>& row_groups,
                    std::shared_ptr<ChunkedArray>* out) {
    std::unique_ptr<ColumnReader> flat_column_reader;
    RETURN_NOT_OK(GetColumn(i, SomeRowGroupsFactory(row_groups), &flat_column_reader));
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    int64_t records_to_read = GetTotalRecords(row_groups, i);
    return flat_column_reader->NextBatch(records_to_read, out);
    END_PARQUET_CATCH_EXCEPTIONS
  }

  Status ReadColumn(int i, std::shared_ptr<ChunkedArray>* out) override {
    return ReadColumn(i, Iota(reader_->metadata()->num_row_groups()), out);
  }

  Status ReadTable(std::shared_ptr<Table>* table) override {
    return ReadTable(Iota(reader_->metadata()->num_columns()), table);
  }

  Status ReadRowGroups(const std::vector<int>& row_groups,
                       const std::vector<int>& indices,
                       std::shared_ptr<Table>* table) override;

  Status ReadRowGroups(const std::vector<int>& row_groups,
                       std::shared_ptr<Table>* table) override {
    return ReadRowGroups(row_groups, Iota(reader_->metadata()->num_columns()), table);
  }

  Status ReadRowGroup(int row_group_index, const std::vector<int>& column_indices,
                      std::shared_ptr<Table>* out) override {
    return ReadRowGroups({row_group_index}, column_indices, out);
  }

  Status ReadRowGroup(int i, std::shared_ptr<Table>* table) override {
    return ReadRowGroup(i, Iota(reader_->metadata()->num_columns()), table);
  }

  Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                              const std::vector<int>& column_indices,
                              std::unique_ptr<RecordBatchReader>* out) override;

  Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                              std::unique_ptr<RecordBatchReader>* out) override {
    return GetRecordBatchReader(row_group_indices,
                                Iota(reader_->metadata()->num_columns()), out);
  }

  int num_columns() const { return reader_->metadata()->num_columns(); }

  ParquetFileReader* parquet_reader() const override { return reader_.get(); }

  int num_row_groups() const override { return reader_->metadata()->num_row_groups(); }

  void set_use_threads(bool use_threads) override {
    reader_properties_.set_use_threads(use_threads);
  }

  Status ScanContents(std::vector<int> columns, const int32_t column_batch_size,
                      int64_t* num_rows) override {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    *num_rows = ScanFileContents(columns, column_batch_size, reader_.get());
    return Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
  }

  MemoryPool* pool_;
  std::unique_ptr<ParquetFileReader> reader_;
  ArrowReaderProperties reader_properties_;

  SchemaManifest manifest_;
};

class RowGroupRecordBatchReader : public ::arrow::RecordBatchReader {
 public:
  RowGroupRecordBatchReader(std::vector<std::unique_ptr<ColumnReaderImpl>> field_readers,
                            std::shared_ptr<::arrow::Schema> schema, int64_t batch_size)
      : field_readers_(std::move(field_readers)),
        schema_(std::move(schema)),
        batch_size_(batch_size) {}

  ~RowGroupRecordBatchReader() override {}

  std::shared_ptr<::arrow::Schema> schema() const override { return schema_; }

  static Status Make(const std::vector<int>& row_groups,
                     const std::vector<int>& column_indices, FileReaderImpl* reader,
                     int64_t batch_size,
                     std::unique_ptr<::arrow::RecordBatchReader>* out) {
    std::vector<int> field_indices;
    if (!reader->manifest_.GetFieldIndices(column_indices, &field_indices)) {
      return Status::Invalid("Invalid column index");
    }

    std::vector<std::unique_ptr<ColumnReaderImpl>> field_readers(field_indices.size());
    std::vector<std::shared_ptr<Field>> fields;

    auto included_leaves = VectorToSharedSet(column_indices);
    for (size_t i = 0; i < field_indices.size(); ++i) {
      RETURN_NOT_OK(reader->GetFieldReader(field_indices[i], included_leaves, row_groups,
                                           &field_readers[i]));
      fields.push_back(field_readers[i]->field());
    }
    out->reset(new RowGroupRecordBatchReader(std::move(field_readers),
                                             ::arrow::schema(fields), batch_size));
    return Status::OK();
  }

  Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* out) override {
    // TODO (hatemhelal): Consider refactoring this to share logic with ReadTable as this
    // does not currently honor the use_threads option.
    std::vector<std::shared_ptr<ChunkedArray>> columns(field_readers_.size());
    for (size_t i = 0; i < field_readers_.size(); ++i) {
      RETURN_NOT_OK(field_readers_[i]->NextBatch(batch_size_, &columns[i]));
      if (columns[i]->num_chunks() > 1) {
        return Status::NotImplemented("This class cannot yet iterate chunked arrays");
      }
    }

    // Create an intermediate table and use TableBatchReader as an adaptor to a
    // RecordBatch
    std::shared_ptr<Table> table = Table::Make(schema_, columns);
    RETURN_NOT_OK(table->Validate());
    ::arrow::TableBatchReader table_batch_reader(*table);
    return table_batch_reader.ReadNext(out);
  }

 private:
  std::vector<std::unique_ptr<ColumnReaderImpl>> field_readers_;
  std::shared_ptr<::arrow::Schema> schema_;
  int64_t batch_size_;
};

class ColumnChunkReaderImpl : public ColumnChunkReader {
 public:
  ColumnChunkReaderImpl(FileReaderImpl* impl, int row_group_index, int column_index)
      : impl_(impl), column_index_(column_index), row_group_index_(row_group_index) {}

  Status Read(std::shared_ptr<::arrow::ChunkedArray>* out) override {
    return impl_->ReadColumn(column_index_, {row_group_index_}, out);
  }

 private:
  FileReaderImpl* impl_;
  int column_index_;
  int row_group_index_;
};

class RowGroupReaderImpl : public RowGroupReader {
 public:
  RowGroupReaderImpl(FileReaderImpl* impl, int row_group_index)
      : impl_(impl), row_group_index_(row_group_index) {}

  std::shared_ptr<ColumnChunkReader> Column(int column_index) override {
    return std::shared_ptr<ColumnChunkReader>(
        new ColumnChunkReaderImpl(impl_, row_group_index_, column_index));
  }

  Status ReadTable(const std::vector<int>& column_indices,
                   std::shared_ptr<::arrow::Table>* out) override {
    return impl_->ReadRowGroup(row_group_index_, column_indices, out);
  }

  Status ReadTable(std::shared_ptr<::arrow::Table>* out) override {
    return impl_->ReadRowGroup(row_group_index_, out);
  }

 private:
  FileReaderImpl* impl_;
  int row_group_index_;
};

// Leaf reader is for primitive arrays and primitive children of nested arrays
class LeafReader : public ColumnReaderImpl {
 public:
  LeafReader(std::shared_ptr<ReaderContext> ctx, std::shared_ptr<Field> field,
             std::unique_ptr<FileColumnIterator> input)
      : ctx_(std::move(ctx)),
        field_(std::move(field)),
        input_(std::move(input)),
        descr_(input_->descr()) {
    record_reader_ = RecordReader::Make(
        descr_, ctx_->pool, field_->type()->id() == ::arrow::Type::DICTIONARY);
    NextRowGroup();
  }

  Status GetDefLevels(const int16_t** data, int64_t* length) override {
    *data = record_reader_->def_levels();
    *length = record_reader_->levels_position();
    return Status::OK();
  }

  Status GetRepLevels(const int16_t** data, int64_t* length) override {
    *data = record_reader_->rep_levels();
    *length = record_reader_->levels_position();
    return Status::OK();
  }

  Status NextBatch(int64_t records_to_read, std::shared_ptr<ChunkedArray>* out) override {
    BEGIN_PARQUET_CATCH_EXCEPTIONS

    // Pre-allocation gives much better performance for flat columns
    record_reader_->Reserve(records_to_read);

    record_reader_->Reset();
    while (records_to_read > 0) {
      if (!record_reader_->HasMoreData()) {
        break;
      }
      int64_t records_read = record_reader_->ReadRecords(records_to_read);
      records_to_read -= records_read;
      if (records_read == 0) {
        NextRowGroup();
      }
    }
    RETURN_NOT_OK(TransferColumnData(record_reader_.get(), field_->type(), descr_,
                                     ctx_->pool, out));
    return Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
  }

  const std::shared_ptr<Field> field() override { return field_; }
  const ColumnDescriptor* descr() const override { return descr_; }

  ReaderType type() const override { return PRIMITIVE; }

 private:
  void NextRowGroup() {
    std::unique_ptr<PageReader> page_reader = input_->NextChunk();
    record_reader_->SetPageReader(std::move(page_reader));
  }

  std::shared_ptr<ReaderContext> ctx_;
  std::shared_ptr<Field> field_;
  std::unique_ptr<FileColumnIterator> input_;
  const ColumnDescriptor* descr_;
  std::shared_ptr<RecordReader> record_reader_;
};

class NestedListReader : public ColumnReaderImpl {
 public:
  NestedListReader(std::shared_ptr<ReaderContext> ctx, std::shared_ptr<Field> field,
                   int16_t max_definition_level, int16_t max_repetition_level,
                   std::unique_ptr<ColumnReaderImpl> item_reader)
      : ctx_(std::move(ctx)),
        field_(std::move(field)),
        max_definition_level_(max_definition_level),
        max_repetition_level_(max_repetition_level),
        item_reader_(std::move(item_reader)) {}

  Status GetDefLevels(const int16_t** data, int64_t* length) override {
    return item_reader_->GetDefLevels(data, length);
  }

  Status GetRepLevels(const int16_t** data, int64_t* length) override {
    return item_reader_->GetRepLevels(data, length);
  }

  Status NextBatch(int64_t records_to_read, std::shared_ptr<ChunkedArray>* out) override {
    if (item_reader_->type() == ColumnReaderImpl::STRUCT) {
      return Status::Invalid("Mix of struct and list types not yet supported");
    }

    RETURN_NOT_OK(item_reader_->NextBatch(records_to_read, out));

    // ARROW-3762(wesm): If item reader yields a chunked array, we reject as
    // this is not yet implemented
    if ((*out)->num_chunks() > 1) {
      return Status::NotImplemented(
          "Nested data conversions not implemented for chunked array outputs");
    }

    const int16_t* def_levels;
    const int16_t* rep_levels;
    int64_t num_levels;
    RETURN_NOT_OK(item_reader_->GetDefLevels(&def_levels, &num_levels));
    RETURN_NOT_OK(item_reader_->GetRepLevels(&rep_levels, &num_levels));
    std::shared_ptr<Array> result;
    RETURN_NOT_OK(ReconstructNestedList((*out)->chunk(0), field_, max_definition_level_,
                                        max_repetition_level_, def_levels, rep_levels,
                                        num_levels, ctx_->pool, &result));
    *out = std::make_shared<ChunkedArray>(result);
    return Status::OK();
  }

  const std::shared_ptr<Field> field() override { return field_; }

  const ColumnDescriptor* descr() const override { return nullptr; }

  ReaderType type() const override { return LIST; }

 private:
  std::shared_ptr<ReaderContext> ctx_;
  std::shared_ptr<Field> field_;
  int16_t max_definition_level_;
  int16_t max_repetition_level_;
  std::unique_ptr<ColumnReaderImpl> item_reader_;
};

class PARQUET_NO_EXPORT StructReader : public ColumnReaderImpl {
 public:
  explicit StructReader(std::shared_ptr<ReaderContext> ctx,
                        const SchemaField& schema_field,
                        std::shared_ptr<Field> filtered_field,
                        std::vector<std::unique_ptr<ColumnReaderImpl>>&& children)
      : ctx_(std::move(ctx)),
        schema_field_(schema_field),
        filtered_field_(std::move(filtered_field)),
        struct_def_level_(schema_field.max_definition_level),
        children_(std::move(children)) {}

  Status NextBatch(int64_t records_to_read, std::shared_ptr<ChunkedArray>* out) override;
  Status GetDefLevels(const int16_t** data, int64_t* length) override;
  Status GetRepLevels(const int16_t** data, int64_t* length) override;
  const std::shared_ptr<Field> field() override { return filtered_field_; }
  const ColumnDescriptor* descr() const override { return nullptr; }
  ReaderType type() const override { return STRUCT; }

 private:
  std::shared_ptr<ReaderContext> ctx_;
  SchemaField schema_field_;
  std::shared_ptr<Field> filtered_field_;
  int16_t struct_def_level_;
  std::vector<std::unique_ptr<ColumnReaderImpl>> children_;
  std::shared_ptr<ResizableBuffer> def_levels_buffer_;
  Status DefLevelsToNullArray(std::shared_ptr<Buffer>* null_bitmap, int64_t* null_count);
};

Status StructReader::DefLevelsToNullArray(std::shared_ptr<Buffer>* null_bitmap_out,
                                          int64_t* null_count_out) {
  auto null_count = 0;
  const int16_t* def_levels_data;
  int64_t def_levels_length;
  RETURN_NOT_OK(GetDefLevels(&def_levels_data, &def_levels_length));
  ARROW_ASSIGN_OR_RAISE(auto null_bitmap,
                        AllocateEmptyBitmap(def_levels_length, ctx_->pool));
  uint8_t* null_bitmap_ptr = null_bitmap->mutable_data();
  for (int64_t i = 0; i < def_levels_length; i++) {
    if (def_levels_data[i] < struct_def_level_) {
      // Mark null
      null_count += 1;
    } else {
      DCHECK_EQ(def_levels_data[i], struct_def_level_);
      ::arrow::BitUtil::SetBit(null_bitmap_ptr, i);
    }
  }

  *null_count_out = null_count;
  *null_bitmap_out = (null_count == 0) ? nullptr : null_bitmap;
  return Status::OK();
}

// TODO(itaiin): Consider caching the results of this calculation -
//   note that this is only used once for each read for now
Status StructReader::GetDefLevels(const int16_t** data, int64_t* length) {
  *data = nullptr;
  if (children_.size() == 0) {
    // Empty struct
    *length = 0;
    return Status::OK();
  }

  // We have at least one child
  const int16_t* child_def_levels;
  int64_t child_length = 0;
  bool found_nullable_child = false;
  int16_t* result_levels = nullptr;

  int child_index = 0;
  while (child_index < static_cast<int>(children_.size())) {
    if (!children_[child_index]->field()->nullable()) {
      ++child_index;
      continue;
    }
    RETURN_NOT_OK(children_[child_index]->GetDefLevels(&child_def_levels, &child_length));
    auto size = child_length * sizeof(int16_t);
    ARROW_ASSIGN_OR_RAISE(def_levels_buffer_, AllocateResizableBuffer(size, ctx_->pool));
    // Initialize with the minimal def level
    std::memset(def_levels_buffer_->mutable_data(), -1, size);
    result_levels = reinterpret_cast<int16_t*>(def_levels_buffer_->mutable_data());
    found_nullable_child = true;
    break;
  }

  if (!found_nullable_child) {
    *data = nullptr;
    *length = 0;
    return Status::OK();
  }

  // Look at the rest of the children

  // When a struct is defined, all of its children def levels are at least at
  // nesting level, and def level equals nesting level.
  // When a struct is not defined, all of its children def levels are less than
  // the nesting level, and the def level equals max(children def levels)
  // All other possibilities are malformed definition data.
  for (; child_index < static_cast<int>(children_.size()); ++child_index) {
    // Child is non-nullable, and therefore has no definition levels
    if (!children_[child_index]->field()->nullable()) {
      continue;
    }

    auto& child = children_[child_index];
    int64_t current_child_length;
    RETURN_NOT_OK(child->GetDefLevels(&child_def_levels, &current_child_length));

    if (child_length != current_child_length) {
      std::stringstream ss;
      ss << "Parquet struct decoding error. Expected to decode " << child_length
         << " definition levels"
         << " from child field \"" << child->field()->ToString() << "\" in parent \""
         << this->field()->ToString() << "\" but was only able to decode "
         << current_child_length;
      return Status::IOError(ss.str());
    }

    DCHECK_EQ(child_length, current_child_length);
    for (int64_t i = 0; i < child_length; i++) {
      // Check that value is either uninitialized, or current
      // and previous children def levels agree on the struct level
      DCHECK((result_levels[i] == -1) || ((result_levels[i] >= struct_def_level_) ==
                                          (child_def_levels[i] >= struct_def_level_)));
      result_levels[i] =
          std::max(result_levels[i], std::min(child_def_levels[i], struct_def_level_));
    }
  }
  *data = reinterpret_cast<const int16_t*>(def_levels_buffer_->data());
  *length = static_cast<int64_t>(child_length);
  return Status::OK();
}

Status StructReader::GetRepLevels(const int16_t** data, int64_t* length) {
  return Status::NotImplemented("GetRepLevels is not implemented for struct");
}

Status StructReader::NextBatch(int64_t records_to_read,
                               std::shared_ptr<ChunkedArray>* out) {
  std::vector<std::shared_ptr<Array>> children_arrays;
  std::shared_ptr<Buffer> null_bitmap;
  int64_t null_count;

  // Gather children arrays and def levels
  for (auto& child : children_) {
    if (child->type() == ColumnReaderImpl::LIST) {
      return Status::Invalid("Mix of struct and list types not yet supported");
    }

    std::shared_ptr<ChunkedArray> field;
    RETURN_NOT_OK(child->NextBatch(records_to_read, &field));

    if (field->num_chunks() > 1) {
      return Status::Invalid("Chunked field reads not yet supported with StructArray");
    }
    children_arrays.push_back(field->chunk(0));
  }

  RETURN_NOT_OK(DefLevelsToNullArray(&null_bitmap, &null_count));

  int64_t struct_length = children_arrays[0]->length();
  for (size_t i = 1; i < children_arrays.size(); ++i) {
    if (children_arrays[i]->length() != struct_length) {
      // TODO(wesm): This should really only occur if the Parquet file is
      // malformed. Should this be a DCHECK?
      return Status::Invalid("Struct children had different lengths");
    }
  }

  auto result = std::make_shared<StructArray>(field()->type(), struct_length,
                                              children_arrays, null_bitmap, null_count);
  *out = std::make_shared<ChunkedArray>(result);
  return Status::OK();
}

// ----------------------------------------------------------------------
// File reader implementation

Status GetReader(const SchemaField& field, const std::shared_ptr<ReaderContext>& ctx,
                 std::unique_ptr<ColumnReaderImpl>* out) {
  BEGIN_PARQUET_CATCH_EXCEPTIONS

  auto type_id = field.field->type()->id();
  if (field.children.size() == 0) {
    if (!field.is_leaf()) {
      return Status::Invalid("Parquet non-leaf node has no children");
    }
    std::unique_ptr<FileColumnIterator> input(
        ctx->iterator_factory(field.column_index, ctx->reader));
    out->reset(new LeafReader(ctx, field.field, std::move(input)));
  } else if (type_id == ::arrow::Type::LIST) {
    // We can only read lists-of-lists or structs at the moment
    auto list_field = field.field;
    auto child = &field.children[0];
    while (child->field->type()->id() == ::arrow::Type::LIST) {
      child = &child->children[0];
    }
    if (child->field->type()->id() == ::arrow::Type::STRUCT) {
      return Status::NotImplemented(
          "Reading lists of structs from Parquet files "
          "not yet supported: ",
          field.field->ToString());
    }
    if (!ctx->IncludesLeaf(child->column_index)) {
      *out = nullptr;
      return Status::OK();
    }
    std::unique_ptr<ColumnReaderImpl> child_reader;
    RETURN_NOT_OK(GetReader(*child, ctx, &child_reader));
    // Use the max definition/repetition level of the leaf here
    out->reset(new NestedListReader(ctx, list_field, child->max_definition_level,
                                    child->max_repetition_level,
                                    std::move(child_reader)));
  } else if (type_id == ::arrow::Type::STRUCT) {
    std::vector<std::shared_ptr<Field>> child_fields;
    std::vector<std::unique_ptr<ColumnReaderImpl>> child_readers;
    for (const auto& child : field.children) {
      if (child.is_leaf() && !ctx->IncludesLeaf(child.column_index)) {
        // Excluded leaf
        continue;
      }
      std::unique_ptr<ColumnReaderImpl> child_reader;
      RETURN_NOT_OK(GetReader(child, ctx, &child_reader));
      if (!child_reader) {
        // If all children were pruned, then we do not try to read this field
        continue;
      }
      child_fields.push_back(child.field);
      child_readers.emplace_back(std::move(child_reader));
    }
    if (child_fields.size() == 0) {
      *out = nullptr;
      return Status::OK();
    }
    auto filtered_field =
        ::arrow::field(field.field->name(), ::arrow::struct_(child_fields),
                       field.field->nullable(), field.field->metadata());
    out->reset(new StructReader(ctx, field, filtered_field, std::move(child_readers)));
  } else {
    return Status::Invalid("Unsupported nested type: ", field.field->ToString());
  }
  return Status::OK();

  END_PARQUET_CATCH_EXCEPTIONS
}

Status FileReaderImpl::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                            const std::vector<int>& column_indices,
                                            std::unique_ptr<RecordBatchReader>* out) {
  // column indices check
  for (auto row_group_index : row_group_indices) {
    RETURN_NOT_OK(BoundsCheckRowGroup(row_group_index));
  }

  if (reader_properties_.pre_buffer()) {
    // PARQUET-1698/PARQUET-1820: pre-buffer row groups/column chunks if enabled
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    reader_->PreBuffer(row_group_indices, column_indices,
                       reader_properties_.cache_options());
    END_PARQUET_CATCH_EXCEPTIONS
  }

  return RowGroupRecordBatchReader::Make(row_group_indices, column_indices, this,
                                         reader_properties_.batch_size(), out);
}

Status FileReaderImpl::GetColumn(int i, FileColumnIteratorFactory iterator_factory,
                                 std::unique_ptr<ColumnReader>* out) {
  RETURN_NOT_OK(BoundsCheckColumn(i));
  auto ctx = std::make_shared<ReaderContext>();
  ctx->reader = reader_.get();
  ctx->pool = pool_;
  ctx->iterator_factory = iterator_factory;
  ctx->filter_leaves = false;
  std::unique_ptr<ColumnReaderImpl> result;
  RETURN_NOT_OK(GetReader(manifest_.schema_fields[i], ctx, &result));
  out->reset(result.release());
  return Status::OK();
}

Status FileReaderImpl::ReadRowGroups(const std::vector<int>& row_groups,
                                     const std::vector<int>& indices,
                                     std::shared_ptr<Table>* out) {
  BEGIN_PARQUET_CATCH_EXCEPTIONS

  // We only need to read schema fields which have columns indicated
  // in the indices vector
  std::vector<int> field_indices;
  if (!manifest_.GetFieldIndices(indices, &field_indices)) {
    return Status::Invalid("Invalid column index");
  }

  // PARQUET-1698/PARQUET-1820: pre-buffer row groups/column chunks if enabled
  if (reader_properties_.pre_buffer()) {
    parquet_reader()->PreBuffer(row_groups, indices, reader_properties_.cache_options());
  }

  int num_fields = static_cast<int>(field_indices.size());
  std::vector<std::shared_ptr<Field>> fields(num_fields);
  std::vector<std::shared_ptr<ChunkedArray>> columns(num_fields);

  auto included_leaves = VectorToSharedSet(indices);
  auto ReadColumnFunc = [&](int i) {
    return ReadSchemaField(field_indices[i], included_leaves, row_groups, &fields[i],
                           &columns[i]);
  };

  if (reader_properties_.use_threads()) {
    std::vector<Future<Status>> futures(num_fields);
    auto pool = ::arrow::internal::GetCpuThreadPool();
    for (int i = 0; i < num_fields; i++) {
      ARROW_ASSIGN_OR_RAISE(futures[i], pool->Submit(ReadColumnFunc, i));
    }
    Status final_status = Status::OK();
    for (auto& fut : futures) {
      Status st = fut.status();
      if (!st.ok()) {
        final_status = std::move(st);
      }
    }
    RETURN_NOT_OK(final_status);
  } else {
    for (int i = 0; i < num_fields; i++) {
      RETURN_NOT_OK(ReadColumnFunc(i));
    }
  }

  auto result_schema = ::arrow::schema(fields, manifest_.schema_metadata);
  *out = Table::Make(result_schema, columns);
  return (*out)->Validate();
  END_PARQUET_CATCH_EXCEPTIONS
}

std::shared_ptr<RowGroupReader> FileReaderImpl::RowGroup(int row_group_index) {
  return std::make_shared<RowGroupReaderImpl>(this, row_group_index);
}

// ----------------------------------------------------------------------
// Public factory functions

Status FileReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                        std::shared_ptr<RecordBatchReader>* out) {
  std::unique_ptr<RecordBatchReader> tmp;
  ARROW_RETURN_NOT_OK(GetRecordBatchReader(row_group_indices, &tmp));
  out->reset(tmp.release());
  return Status::OK();
}

Status FileReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                        const std::vector<int>& column_indices,
                                        std::shared_ptr<RecordBatchReader>* out) {
  std::unique_ptr<RecordBatchReader> tmp;
  ARROW_RETURN_NOT_OK(GetRecordBatchReader(row_group_indices, column_indices, &tmp));
  out->reset(tmp.release());
  return Status::OK();
}

Status FileReader::Make(::arrow::MemoryPool* pool,
                        std::unique_ptr<ParquetFileReader> reader,
                        const ArrowReaderProperties& properties,
                        std::unique_ptr<FileReader>* out) {
  out->reset(new FileReaderImpl(pool, std::move(reader), properties));
  return static_cast<FileReaderImpl*>(out->get())->Init();
}

Status FileReader::Make(::arrow::MemoryPool* pool,
                        std::unique_ptr<ParquetFileReader> reader,
                        std::unique_ptr<FileReader>* out) {
  return Make(pool, std::move(reader), default_arrow_reader_properties(), out);
}

FileReaderBuilder::FileReaderBuilder()
    : pool_(::arrow::default_memory_pool()),
      properties_(default_arrow_reader_properties()) {}

Status FileReaderBuilder::Open(std::shared_ptr<::arrow::io::RandomAccessFile> file,
                               const ReaderProperties& properties,
                               std::shared_ptr<FileMetaData> metadata) {
  PARQUET_CATCH_NOT_OK(raw_reader_ = ParquetReader::Open(std::move(file), properties,
                                                         std::move(metadata)));
  return Status::OK();
}

FileReaderBuilder* FileReaderBuilder::memory_pool(::arrow::MemoryPool* pool) {
  pool_ = pool;
  return this;
}

FileReaderBuilder* FileReaderBuilder::properties(
    const ArrowReaderProperties& arg_properties) {
  properties_ = arg_properties;
  return this;
}

Status FileReaderBuilder::Build(std::unique_ptr<FileReader>* out) {
  return FileReader::Make(pool_, std::move(raw_reader_), properties_, out);
}

Status OpenFile(std::shared_ptr<::arrow::io::RandomAccessFile> file, MemoryPool* pool,
                std::unique_ptr<FileReader>* reader) {
  FileReaderBuilder builder;
  RETURN_NOT_OK(builder.Open(std::move(file)));
  return builder.memory_pool(pool)->Build(reader);
}

namespace internal {

Status FuzzReader(std::unique_ptr<FileReader> reader) {
  auto st = Status::OK();
  for (int i = 0; i < reader->num_row_groups(); ++i) {
    std::shared_ptr<Table> table;
    auto row_group_status = reader->ReadRowGroup(i, &table);
    if (row_group_status.ok()) {
      row_group_status &= table->ValidateFull();
    }
    st &= row_group_status;
  }
  return st;
}

Status FuzzReader(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<::arrow::Buffer>(data, size);
  auto file = std::make_shared<::arrow::io::BufferReader>(buffer);
  FileReaderBuilder builder;
  RETURN_NOT_OK(builder.Open(std::move(file)));

  std::unique_ptr<FileReader> reader;
  RETURN_NOT_OK(builder.Build(&reader));
  return FuzzReader(std::move(reader));
}

}  // namespace internal

}  // namespace arrow
}  // namespace parquet

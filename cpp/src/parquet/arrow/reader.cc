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
#include <deque>
#include <functional>
#include <future>
#include <numeric>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread-pool.h"

#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/column_reader.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema-internal.h"
#include "parquet/schema.h"

using arrow::Array;
using arrow::BooleanArray;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::Field;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::StructArray;
using arrow::Table;
using arrow::TimestampArray;

using parquet::schema::Node;

// Help reduce verbosity
using ParquetReader = parquet::ParquetFileReader;
using arrow::RecordBatchReader;

using parquet::internal::RecordReader;

#define BEGIN_PARQUET_CATCH_EXCEPTIONS try {
#define END_PARQUET_CATCH_EXCEPTIONS             \
  }                                              \
  catch (const ::parquet::ParquetException& e) { \
    return ::arrow::Status::IOError(e.what());   \
  }

namespace parquet {
namespace arrow {

class ColumnChunkReaderImpl;
class ColumnReaderImpl;

ArrowReaderProperties default_arrow_reader_properties() {
  static ArrowReaderProperties default_reader_props;
  return default_reader_props;
}

// ----------------------------------------------------------------------
// Iteration utilities

// Abstraction to decouple row group iteration details from the ColumnReader,
// so we can read only a single row group if we want
class FileColumnIterator {
 public:
  explicit FileColumnIterator(int column_index, ParquetFileReader* reader,
                              std::vector<int> row_groups)
      : column_index_(column_index),
        reader_(reader),
        schema_(reader->metadata()->schema()),
        row_groups_(row_groups.begin(), row_groups.end()) {}

  virtual ~FileColumnIterator() {}

  std::unique_ptr<::parquet::PageReader> NextChunk() {
    if (row_groups_.empty()) {
      return nullptr;
    }

    auto row_group_reader = reader_->RowGroup(row_groups_.front());
    row_groups_.pop_front();
    return row_group_reader->GetColumnPageReader(column_index_);
  }

  const SchemaDescriptor* schema() const { return schema_; }

  const ColumnDescriptor* descr() const { return schema_->Column(column_index_); }

  std::shared_ptr<FileMetaData> metadata() const { return reader_->metadata(); }

  int column_index() const { return column_index_; }

 protected:
  int column_index_;
  ParquetFileReader* reader_;
  const SchemaDescriptor* schema_;
  std::deque<int> row_groups_;
};

using FileColumnIteratorFactory =
    std::function<FileColumnIterator*(int, ParquetFileReader*)>;

class RowGroupRecordBatchReader : public ::arrow::RecordBatchReader {
 public:
  explicit RowGroupRecordBatchReader(const std::vector<int>& row_group_indices,
                                     const std::vector<int>& column_indices,
                                     std::shared_ptr<::arrow::Schema> schema,
                                     FileReader* reader, int64_t batch_size)
      : column_readers_(),
        row_group_indices_(row_group_indices),
        column_indices_(column_indices),
        schema_(schema),
        file_reader_(reader),
        batch_size_(batch_size) {}

  ~RowGroupRecordBatchReader() override {}

  std::shared_ptr<::arrow::Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* out) override {
    if (column_readers_.empty()) {
      // Initialize the column readers
      column_readers_.reserve(column_indices_.size());

      for (size_t i = 0; i < column_indices_.size(); ++i) {
        ColumnReaderPtr tmp;
        RETURN_NOT_OK(file_reader_->GetColumn(column_indices_[i], &tmp));
        column_readers_.emplace_back(std::move(tmp));
      }
    }

    // TODO (hatemhelal): Consider refactoring this to share logic with ReadTable as this
    // does not currently honor the use_threads option.
    std::vector<std::shared_ptr<ChunkedArray>> columns(column_indices_.size());

    for (size_t i = 0; i < column_indices_.size(); ++i) {
      RETURN_NOT_OK(column_readers_[i]->NextBatch(batch_size_, &columns[i]));
    }

    // Create an intermediate table and use TableBatchReader as an adaptor to a
    // RecordBatch
    std::shared_ptr<Table> table = Table::Make(schema_, columns);
    RETURN_NOT_OK(table->Validate());
    ::arrow::TableBatchReader table_batch_reader(*table);
    return table_batch_reader.ReadNext(out);
  }

 private:
  using ColumnReaderPtr = std::unique_ptr<ColumnReader>;
  std::vector<ColumnReaderPtr> column_readers_;
  std::vector<int> row_group_indices_;
  std::vector<int> column_indices_;
  std::shared_ptr<::arrow::Schema> schema_;
  FileReader* file_reader_;
  int64_t batch_size_;
};

// ----------------------------------------------------------------------
// FileReaderImpl forward declaration

class FileReaderImpl : public FileReader {
 public:
  FileReaderImpl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader,
                 const ArrowReaderProperties& properties)
      : pool_(pool), reader_(std::move(reader)), reader_properties_(properties) {}

  Status Init() {
    // TODO(wesm): Smarter schema/column-reader initialization for nested data
    return Status::OK();
  }

  FileColumnIteratorFactory SomeRowGroupsFactory(std::vector<int> row_groups) {
    return [row_groups](int i, ParquetFileReader* reader) {
      return new FileColumnIterator(i, reader, row_groups);
    };
  }

  std::vector<int> AllRowGroups() {
    std::vector<int> row_groups(reader_->metadata()->num_row_groups());
    std::iota(row_groups.begin(), row_groups.end(), 0);
    return row_groups;
  }

  std::vector<int> AllColumnIndices() {
    std::vector<int> indices(reader_->metadata()->num_columns());
    std::iota(indices.begin(), indices.end(), 0);
    return indices;
  }

  FileColumnIteratorFactory AllRowGroupsFactory() {
    return SomeRowGroupsFactory(AllRowGroups());
  }

  int64_t GetTotalRecords(const std::vector<int>& row_groups, int column_chunk = 0) {
    // Can throw exception
    int64_t records = 0;
    for (int j = 0; j < static_cast<int>(row_groups.size()); j++) {
      records += reader_->metadata()
                     ->RowGroup(row_groups[j])
                     ->ColumnChunk(column_chunk)
                     ->num_values();
    }
    return records;
  }

  std::shared_ptr<RowGroupReader> RowGroup(int row_group_index) override;

  Status GetReaderForNode(int index, const Node* node, const std::vector<int>& indices,
                          int16_t def_level, FileColumnIteratorFactory iterator_factory,
                          std::unique_ptr<ColumnReaderImpl>* out);

  Status ReadTable(const std::vector<int>& indices,
                   std::shared_ptr<Table>* out) override {
    return ReadRowGroups(AllRowGroups(), indices, out);
  }

  Status GetColumn(int i, FileColumnIteratorFactory iterator_factory,
                   std::unique_ptr<ColumnReader>* out);

  Status GetColumn(int i, std::unique_ptr<ColumnReader>* out) override {
    return GetColumn(i, AllRowGroupsFactory(), out);
  }

  Status GetSchema(std::shared_ptr<::arrow::Schema>* out) override {
    return FromParquetSchema(reader_->metadata()->schema(), reader_properties_,
                             reader_->metadata()->key_value_metadata(), out);
  }

  Status GetSchema(const std::vector<int>& indices,
                   std::shared_ptr<::arrow::Schema>* out) override {
    return FromParquetSchema(reader_->metadata()->schema(), indices, reader_properties_,
                             reader_->metadata()->key_value_metadata(), out);
  }

  Status ReadSchemaField(int i, const std::vector<int>& indices,
                         const std::vector<int>& row_groups,
                         std::shared_ptr<ChunkedArray>* out);

  Status ReadSchemaField(int i, const std::vector<int>& indices,
                         std::shared_ptr<ChunkedArray>* out) {
    return ReadSchemaField(i, indices, AllRowGroups(), out);
  }

  Status ReadSchemaField(int i, std::shared_ptr<ChunkedArray>* out) override {
    return ReadSchemaField(i, AllColumnIndices(), AllRowGroups(), out);
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
    return ReadColumn(i, AllRowGroups(), out);
  }

  Status ReadTable(std::shared_ptr<Table>* table) override {
    std::vector<int> indices(reader_->metadata()->num_columns());
    for (size_t i = 0; i < indices.size(); ++i) {
      indices[i] = static_cast<int>(i);
    }
    return ReadTable(indices, table);
  }

  Status ReadRowGroups(const std::vector<int>& row_groups,
                       const std::vector<int>& indices,
                       std::shared_ptr<Table>* table) override;

  Status ReadRowGroups(const std::vector<int>& row_groups,
                       std::shared_ptr<Table>* table) override {
    return ReadRowGroups(row_groups, AllColumnIndices(), table);
  }

  Status ReadRowGroup(int row_group_index, const std::vector<int>& column_indices,
                      std::shared_ptr<Table>* out) override {
    return ReadRowGroups({row_group_index}, column_indices, out);
  }

  Status ReadRowGroup(int i, std::shared_ptr<Table>* table) override {
    return ReadRowGroup(i, AllColumnIndices(), table);
  }

  std::vector<int> GetDictionaryIndices(const std::vector<int>& indices) {
    // Select the column indices that were read as DictionaryArray
    std::vector<int> dict_indices(indices);
    auto remove_func = [this](int i) { return !reader_properties_.read_dictionary(i); };
    auto it = std::remove_if(dict_indices.begin(), dict_indices.end(), remove_func);
    dict_indices.erase(it, dict_indices.end());
    return dict_indices;
  }

  std::shared_ptr<::arrow::Schema> FixSchema(
      const ::arrow::Schema& old_schema, const std::vector<int>& dict_indices,
      const std::vector<std::shared_ptr<::arrow::ChunkedArray>>& columns) {
    // Fix the schema with the actual DictionaryType that was read
    auto fields = old_schema.fields();

    for (int idx : dict_indices) {
      fields[idx] = old_schema.field(idx)->WithType(columns[idx]->type());
    }
    return std::make_shared<::arrow::Schema>(fields, old_schema.metadata());
  }

  Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                              std::shared_ptr<RecordBatchReader>* out) override {
    return GetRecordBatchReader(row_group_indices, AllColumnIndices(), out);
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

  Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                              const std::vector<int>& column_indices,
                              std::shared_ptr<RecordBatchReader>* out) override {
    // column indices check
    std::shared_ptr<::arrow::Schema> schema;
    RETURN_NOT_OK(GetSchema(column_indices, &schema));
    for (auto row_group_index : row_group_indices) {
      RETURN_NOT_OK(BoundsCheckRowGroup(row_group_index));
    }
    *out = std::make_shared<RowGroupRecordBatchReader>(row_group_indices, column_indices,
                                                       schema, this, batch_size());
    return Status::OK();
  }

  int num_columns() const { return reader_->metadata()->num_columns(); }

  ParquetFileReader* parquet_reader() const override { return reader_.get(); }

  int num_row_groups() const override { return reader_->metadata()->num_row_groups(); }

  void set_use_threads(bool use_threads) override {
    reader_properties_.set_use_threads(use_threads);
  }

  int64_t batch_size() const { return reader_properties_.batch_size(); }

  Status ScanContents(std::vector<int> columns, const int32_t column_batch_size,
                      int64_t* num_rows) override {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    *num_rows = ScanFileContents(columns, column_batch_size, reader_.get());
    return Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
  }

 private:
  MemoryPool* pool_;
  std::unique_ptr<ParquetFileReader> reader_;
  ArrowReaderProperties reader_properties_;
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

class ColumnReaderImpl : public ColumnReader {
 public:
  virtual Status GetDefLevels(const int16_t** data, size_t* length) = 0;
  virtual Status GetRepLevels(const int16_t** data, size_t* length) = 0;
  virtual const std::shared_ptr<Field> field() = 0;
};

// Reader implementation for primitive arrays
class PARQUET_NO_EXPORT PrimitiveImpl : public ColumnReaderImpl {
 public:
  PrimitiveImpl(MemoryPool* pool, std::unique_ptr<FileColumnIterator> input,
                bool read_dictionary)
      : pool_(pool), input_(std::move(input)), descr_(input_->descr()) {
    record_reader_ = RecordReader::Make(descr_, pool_, read_dictionary);
    Status s = NodeToField(*input_->descr()->schema_node(), &field_);
    DCHECK_OK(s);
    NextRowGroup();
  }

  Status GetDefLevels(const int16_t** data, size_t* length) override {
    *data = record_reader_->def_levels();
    *length = record_reader_->levels_written();
    return Status::OK();
  }

  Status GetRepLevels(const int16_t** data, size_t* length) override {
    *data = record_reader_->rep_levels();
    *length = record_reader_->levels_written();
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
    RETURN_NOT_OK(
        TransferColumnData(record_reader_.get(), field_->type(), descr_, pool_, out));

    // Nest nested types, if not nested returns unmodifed
    RETURN_NOT_OK(WrapIntoListArray(out));
    return Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
  }

  Status WrapIntoListArray(std::shared_ptr<ChunkedArray>* inout_array);

  const std::shared_ptr<Field> field() override { return field_; }

 private:
  void NextRowGroup() {
    std::unique_ptr<PageReader> page_reader = input_->NextChunk();
    record_reader_->SetPageReader(std::move(page_reader));
  }

  MemoryPool* pool_;
  std::unique_ptr<FileColumnIterator> input_;
  const ColumnDescriptor* descr_;

  std::shared_ptr<RecordReader> record_reader_;
  std::shared_ptr<Field> field_;
};

Status PrimitiveImpl::WrapIntoListArray(std::shared_ptr<ChunkedArray>* inout_array) {
  if (descr_->max_repetition_level() == 0) {
    // Flat, no action
    return Status::OK();
  }

  std::shared_ptr<Array> flat_array;

  // ARROW-3762(wesm): If inout_array is a chunked array, we reject as this is
  // not yet implemented
  if ((*inout_array)->num_chunks() > 1) {
    return Status::NotImplemented(
        "Nested data conversions not implemented for "
        "chunked array outputs");
  }
  flat_array = (*inout_array)->chunk(0);

  const int16_t* def_levels = record_reader_->def_levels();
  const int16_t* rep_levels = record_reader_->rep_levels();
  const int64_t total_levels_read = record_reader_->levels_position();

  std::shared_ptr<::arrow::Schema> arrow_schema;
  RETURN_NOT_OK(FromParquetSchema(
      input_->schema(), {input_->column_index()}, default_arrow_reader_properties(),
      input_->metadata()->key_value_metadata(), &arrow_schema));
  std::shared_ptr<Field> current_field = arrow_schema->field(0);

  if (current_field->type()->num_children() > 0 &&
      flat_array->type_id() == ::arrow::Type::DICTIONARY) {
    // XXX(wesm): Handling of nested types and dictionary encoding needs to be
    // significantly refactored
    return Status::Invalid("Cannot have nested types containing dictionary arrays yet");
  }

  // Walk downwards to extract nullability
  std::vector<bool> nullable;
  std::vector<std::shared_ptr<::arrow::Int32Builder>> offset_builders;
  std::vector<std::shared_ptr<::arrow::BooleanBuilder>> valid_bits_builders;
  nullable.push_back(current_field->nullable());
  while (current_field->type()->num_children() > 0) {
    if (current_field->type()->num_children() > 1) {
      return Status::NotImplemented("Fields with more than one child are not supported.");
    } else {
      if (current_field->type()->id() != ::arrow::Type::LIST) {
        return Status::NotImplemented("Currently only nesting with Lists is supported.");
      }
      current_field = current_field->type()->child(0);
    }
    offset_builders.emplace_back(
        std::make_shared<::arrow::Int32Builder>(::arrow::int32(), pool_));
    valid_bits_builders.emplace_back(
        std::make_shared<::arrow::BooleanBuilder>(::arrow::boolean(), pool_));
    nullable.push_back(current_field->nullable());
  }

  int64_t list_depth = offset_builders.size();
  // This describes the minimal definition that describes a level that
  // reflects a value in the primitive values array.
  int16_t values_def_level = descr_->max_definition_level();
  if (nullable[nullable.size() - 1]) {
    values_def_level--;
  }

  // The definition levels that are needed so that a list is declared
  // as empty and not null.
  std::vector<int16_t> empty_def_level(list_depth);
  int def_level = 0;
  for (int i = 0; i < list_depth; i++) {
    if (nullable[i]) {
      def_level++;
    }
    empty_def_level[i] = static_cast<int16_t>(def_level);
    def_level++;
  }

  int32_t values_offset = 0;
  std::vector<int64_t> null_counts(list_depth, 0);
  for (int64_t i = 0; i < total_levels_read; i++) {
    int16_t rep_level = rep_levels[i];
    if (rep_level < descr_->max_repetition_level()) {
      for (int64_t j = rep_level; j < list_depth; j++) {
        if (j == (list_depth - 1)) {
          RETURN_NOT_OK(offset_builders[j]->Append(values_offset));
        } else {
          RETURN_NOT_OK(offset_builders[j]->Append(
              static_cast<int32_t>(offset_builders[j + 1]->length())));
        }

        if (((empty_def_level[j] - 1) == def_levels[i]) && (nullable[j])) {
          RETURN_NOT_OK(valid_bits_builders[j]->Append(false));
          null_counts[j]++;
          break;
        } else {
          RETURN_NOT_OK(valid_bits_builders[j]->Append(true));
          if (empty_def_level[j] == def_levels[i]) {
            break;
          }
        }
      }
    }
    if (def_levels[i] >= values_def_level) {
      values_offset++;
    }
  }
  // Add the final offset to all lists
  for (int64_t j = 0; j < list_depth; j++) {
    if (j == (list_depth - 1)) {
      RETURN_NOT_OK(offset_builders[j]->Append(values_offset));
    } else {
      RETURN_NOT_OK(offset_builders[j]->Append(
          static_cast<int32_t>(offset_builders[j + 1]->length())));
    }
  }

  std::vector<std::shared_ptr<Buffer>> offsets;
  std::vector<std::shared_ptr<Buffer>> valid_bits;
  std::vector<int64_t> list_lengths;
  for (int64_t j = 0; j < list_depth; j++) {
    list_lengths.push_back(offset_builders[j]->length() - 1);
    std::shared_ptr<Array> array;
    RETURN_NOT_OK(offset_builders[j]->Finish(&array));
    offsets.emplace_back(std::static_pointer_cast<Int32Array>(array)->values());
    RETURN_NOT_OK(valid_bits_builders[j]->Finish(&array));
    valid_bits.emplace_back(std::static_pointer_cast<BooleanArray>(array)->values());
  }

  std::shared_ptr<Array> output = flat_array;
  for (int64_t j = list_depth - 1; j >= 0; j--) {
    auto list_type =
        ::arrow::list(::arrow::field("item", output->type(), nullable[j + 1]));
    output = std::make_shared<::arrow::ListArray>(list_type, list_lengths[j], offsets[j],
                                                  output, valid_bits[j], null_counts[j]);
  }
  *inout_array = std::make_shared<ChunkedArray>(output);
  return Status::OK();
}

// Reader implementation for struct array

class PARQUET_NO_EXPORT StructImpl : public ColumnReaderImpl {
 public:
  explicit StructImpl(const std::vector<std::shared_ptr<ColumnReaderImpl>>& children,
                      int16_t struct_def_level, MemoryPool* pool, const Node* node)
      : children_(children), struct_def_level_(struct_def_level), pool_(pool) {
    InitField(node, children);
  }

  Status NextBatch(int64_t records_to_read, std::shared_ptr<ChunkedArray>* out) override;
  Status GetDefLevels(const int16_t** data, size_t* length) override;
  Status GetRepLevels(const int16_t** data, size_t* length) override;
  const std::shared_ptr<Field> field() override { return field_; }

 private:
  std::vector<std::shared_ptr<ColumnReaderImpl>> children_;
  int16_t struct_def_level_;
  MemoryPool* pool_;
  std::shared_ptr<Field> field_;
  std::shared_ptr<ResizableBuffer> def_levels_buffer_;

  Status DefLevelsToNullArray(std::shared_ptr<Buffer>* null_bitmap, int64_t* null_count);
  void InitField(const Node* node,
                 const std::vector<std::shared_ptr<ColumnReaderImpl>>& children);
};

Status StructImpl::DefLevelsToNullArray(std::shared_ptr<Buffer>* null_bitmap_out,
                                        int64_t* null_count_out) {
  std::shared_ptr<Buffer> null_bitmap;
  auto null_count = 0;
  const int16_t* def_levels_data;
  size_t def_levels_length;
  RETURN_NOT_OK(GetDefLevels(&def_levels_data, &def_levels_length));
  RETURN_NOT_OK(AllocateEmptyBitmap(pool_, def_levels_length, &null_bitmap));
  uint8_t* null_bitmap_ptr = null_bitmap->mutable_data();
  for (size_t i = 0; i < def_levels_length; i++) {
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
Status StructImpl::GetDefLevels(const int16_t** data, size_t* length) {
  *data = nullptr;
  if (children_.size() == 0) {
    // Empty struct
    *length = 0;
    return Status::OK();
  }

  // We have at least one child
  const int16_t* child_def_levels;
  size_t child_length;
  RETURN_NOT_OK(children_[0]->GetDefLevels(&child_def_levels, &child_length));
  auto size = child_length * sizeof(int16_t);
  RETURN_NOT_OK(AllocateResizableBuffer(pool_, size, &def_levels_buffer_));
  // Initialize with the minimal def level
  std::memset(def_levels_buffer_->mutable_data(), -1, size);
  auto result_levels = reinterpret_cast<int16_t*>(def_levels_buffer_->mutable_data());

  // When a struct is defined, all of its children def levels are at least at
  // nesting level, and def level equals nesting level.
  // When a struct is not defined, all of its children def levels are less than
  // the nesting level, and the def level equals max(children def levels)
  // All other possibilities are malformed definition data.
  for (auto& child : children_) {
    size_t current_child_length;
    RETURN_NOT_OK(child->GetDefLevels(&child_def_levels, &current_child_length));
    DCHECK_EQ(child_length, current_child_length);
    for (size_t i = 0; i < child_length; i++) {
      // Check that value is either uninitialized, or current
      // and previous children def levels agree on the struct level
      DCHECK((result_levels[i] == -1) || ((result_levels[i] >= struct_def_level_) ==
                                          (child_def_levels[i] >= struct_def_level_)));
      result_levels[i] =
          std::max(result_levels[i], std::min(child_def_levels[i], struct_def_level_));
    }
  }
  *data = reinterpret_cast<const int16_t*>(def_levels_buffer_->data());
  *length = child_length;
  return Status::OK();
}

void StructImpl::InitField(
    const Node* node, const std::vector<std::shared_ptr<ColumnReaderImpl>>& children) {
  // Make a shallow node to field conversion from the children fields
  std::vector<std::shared_ptr<::arrow::Field>> fields(children.size());
  for (size_t i = 0; i < children.size(); i++) {
    fields[i] = children[i]->field();
  }

  auto type = ::arrow::struct_(fields);
  field_ = ::arrow::field(node->name(), type, node->is_optional());
}

Status StructImpl::GetRepLevels(const int16_t** data, size_t* length) {
  return Status::NotImplemented("GetRepLevels is not implemented for struct");
}

Status StructImpl::NextBatch(int64_t records_to_read,
                             std::shared_ptr<ChunkedArray>* out) {
  std::vector<std::shared_ptr<Array>> children_arrays;
  std::shared_ptr<Buffer> null_bitmap;
  int64_t null_count;

  // Gather children arrays and def levels
  for (auto& child : children_) {
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

Status FileReaderImpl::GetReaderForNode(int index, const Node* node,
                                        const std::vector<int>& indices,
                                        int16_t def_level,
                                        FileColumnIteratorFactory iterator_factory,
                                        std::unique_ptr<ColumnReaderImpl>* out) {
  *out = nullptr;

  if (schema::IsSimpleStruct(node)) {
    const schema::GroupNode* group = static_cast<const schema::GroupNode*>(node);
    std::vector<std::shared_ptr<ColumnReaderImpl>> children;
    for (int i = 0; i < group->field_count(); i++) {
      std::unique_ptr<ColumnReaderImpl> child_reader;
      // TODO(itaiin): Remove the -1 index hack when all types of nested reads
      // are supported. This currently just signals the lower level reader resolution
      // to abort
      RETURN_NOT_OK(GetReaderForNode(index, group->field(i).get(), indices,
                                     static_cast<int16_t>(def_level + 1),
                                     iterator_factory, &child_reader));
      if (child_reader != nullptr) {
        children.push_back(std::move(child_reader));
      }
    }

    if (children.size() > 0) {
      *out = std::unique_ptr<ColumnReaderImpl>(
          new StructImpl(children, def_level, pool_, node));
    }
  } else {
    // This should be a flat field case - translate the field index to
    // the correct column index by walking down to the leaf node
    const Node* walker = node;
    while (!walker->is_primitive()) {
      DCHECK(walker->is_group());
      auto group = static_cast<const schema::GroupNode*>(walker);
      if (group->field_count() != 1) {
        return Status::NotImplemented("lists with structs are not supported.");
      }
      walker = group->field(0).get();
    }
    auto column_index = reader_->metadata()->schema()->ColumnIndex(*walker);

    // If the index of the column is found then a reader for the column is needed.
    // Otherwise *out keeps the nullptr value.
    if (std::find(indices.begin(), indices.end(), column_index) != indices.end()) {
      std::unique_ptr<ColumnReader> reader;
      RETURN_NOT_OK(GetColumn(column_index, iterator_factory, &reader));
      *out = std::unique_ptr<ColumnReaderImpl>(
          static_cast<ColumnReaderImpl*>(reader.release()));
    }
  }

  return Status::OK();
}

Status FileReaderImpl::ReadRowGroups(const std::vector<int>& row_groups,
                                     const std::vector<int>& indices,
                                     std::shared_ptr<Table>* out) {
  BEGIN_PARQUET_CATCH_EXCEPTIONS

  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(GetSchema(indices, &schema));

  // We only need to read schema fields which have columns indicated
  // in the indices vector
  std::vector<int> field_indices;
  if (!schema::ColumnIndicesToFieldIndices(*reader_->metadata()->schema(), indices,
                                           &field_indices)) {
    return Status::Invalid("Invalid column index");
  }
  int num_fields = static_cast<int>(field_indices.size());
  std::vector<std::shared_ptr<ChunkedArray>> columns(num_fields);

  auto ReadColumnFunc = [&](int i) {
    return ReadSchemaField(field_indices[i], indices, row_groups, &columns[i]);
  };

  if (reader_properties_.use_threads()) {
    std::vector<std::future<Status>> futures;
    auto pool = ::arrow::internal::GetCpuThreadPool();
    for (int i = 0; i < num_fields; i++) {
      futures.push_back(pool->Submit(ReadColumnFunc, i));
    }
    Status final_status = Status::OK();
    for (auto& fut : futures) {
      Status st = fut.get();
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

  auto dict_indices = GetDictionaryIndices(indices);
  if (!dict_indices.empty()) {
    schema = FixSchema(*schema, dict_indices, columns);
  }
  std::shared_ptr<Table> table = Table::Make(schema, columns);
  RETURN_NOT_OK(table->Validate());
  *out = table;
  return Status::OK();
  END_PARQUET_CATCH_EXCEPTIONS
}

Status FileReaderImpl::GetColumn(int i, FileColumnIteratorFactory iterator_factory,
                                 std::unique_ptr<ColumnReader>* out) {
  if (i < 0 || i >= this->num_columns()) {
    return Status::Invalid("Column index out of bounds (got ", i,
                           ", should be "
                           "between 0 and ",
                           this->num_columns() - 1, ")");
  }

  std::unique_ptr<FileColumnIterator> input(iterator_factory(i, reader_.get()));
  *out = std::unique_ptr<ColumnReader>(
      new PrimitiveImpl(pool_, std::move(input), reader_properties_.read_dictionary(i)));
  return Status::OK();
}

Status FileReaderImpl::ReadSchemaField(int i, const std::vector<int>& indices,
                                       const std::vector<int>& row_groups,
                                       std::shared_ptr<ChunkedArray>* out) {
  BEGIN_PARQUET_CATCH_EXCEPTIONS
  auto parquet_schema = reader_->metadata()->schema();
  auto node = parquet_schema->group_node()->field(i).get();
  std::unique_ptr<ColumnReaderImpl> reader_impl;
  RETURN_NOT_OK(GetReaderForNode(i, node, indices, 1, SomeRowGroupsFactory(row_groups),
                                 &reader_impl));
  if (reader_impl == nullptr) {
    *out = nullptr;
    return Status::OK();
  }
  // TODO(wesm): This calculation doesn't make much sense when we have repeated
  // schema nodes
  int64_t records_to_read = GetTotalRecords(row_groups, i);
  return reader_impl->NextBatch(records_to_read, out);
  END_PARQUET_CATCH_EXCEPTIONS
}

std::shared_ptr<RowGroupReader> FileReaderImpl::RowGroup(int row_group_index) {
  return std::make_shared<RowGroupReaderImpl>(this, row_group_index);
}

// ----------------------------------------------------------------------
// Public factory functions

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

Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                MemoryPool* pool, const ReaderProperties& props,
                const std::shared_ptr<FileMetaData>& metadata,
                std::unique_ptr<FileReader>* reader) {
  std::unique_ptr<ParquetReader> pq_reader;
  PARQUET_CATCH_NOT_OK(pq_reader = ParquetReader::Open(file, props, metadata));
  return FileReader::Make(pool, std::move(pq_reader), default_arrow_reader_properties(),
                          reader);
}

Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                MemoryPool* pool, std::unique_ptr<FileReader>* reader) {
  return OpenFile(file, pool, ::parquet::default_reader_properties(), nullptr, reader);
}

Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                ::arrow::MemoryPool* pool, const ArrowReaderProperties& properties,
                std::unique_ptr<FileReader>* reader) {
  std::unique_ptr<ParquetReader> pq_reader;
  PARQUET_CATCH_NOT_OK(pq_reader = ParquetReader::Open(
                           file, ::parquet::default_reader_properties(), nullptr));
  return FileReader::Make(pool, std::move(pq_reader), properties, reader);
}

}  // namespace arrow
}  // namespace parquet

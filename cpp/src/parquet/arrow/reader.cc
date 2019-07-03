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
#include <climits>
#include <cstring>
#include <deque>
#include <future>
#include <numeric>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread-pool.h"

// For arrow::compute::Datum. This should perhaps be promoted. See ARROW-4022
#include "arrow/compute/kernel.h"

#include "parquet/arrow/record_reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/column_reader.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema-internal.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using arrow::Array;
using arrow::BooleanArray;
using arrow::ChunkedArray;
using arrow::Column;
using arrow::Field;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::StructArray;
using arrow::Table;
using arrow::TimestampArray;

// For Array/ChunkedArray variant
using arrow::compute::Datum;

using parquet::schema::Node;

// Help reduce verbosity
using ParquetReader = parquet::ParquetFileReader;
using arrow::RecordBatchReader;

using parquet::internal::RecordReader;

namespace parquet {
namespace arrow {

using ::arrow::BitUtil::FromBigEndian;
using ::arrow::internal::SafeLeftShift;
using ::arrow::util::SafeLoadAs;

template <typename ArrowType>
using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

namespace {

Status GetSingleChunk(const ChunkedArray& chunked, std::shared_ptr<Array>* out) {
  DCHECK_GT(chunked.num_chunks(), 0);
  if (chunked.num_chunks() > 1) {
    return Status::Invalid("Function call returned a chunked array");
  }
  *out = chunked.chunk(0);
  return Status::OK();
}

}  // namespace

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

  static FileColumnIterator* MakeAllRowGroupsIterator(int column_index,
                                                      ParquetFileReader* reader) {
    std::vector<int> row_groups(reader->metadata()->num_row_groups());
    std::iota(row_groups.begin(), row_groups.end(), 0);
    return new FileColumnIterator(column_index, reader, row_groups);
  }

  static FileColumnIterator* MakeSingleRowGroupIterator(int column_index,
                                                        ParquetFileReader* reader,
                                                        int row_group) {
    return new FileColumnIterator(column_index, reader, {row_group});
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
    std::vector<std::shared_ptr<Column>> columns(column_indices_.size());

    for (size_t i = 0; i < column_indices_.size(); ++i) {
      std::shared_ptr<ChunkedArray> array;
      RETURN_NOT_OK(column_readers_[i]->NextBatch(batch_size_, &array));
      columns[i] = std::make_shared<Column>(schema_->field(static_cast<int>(i)), array);
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
// File reader implementation

using FileColumnIteratorFactory =
    std::function<FileColumnIterator*(int, ParquetFileReader*)>;

class FileReader::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader,
       const ArrowReaderProperties& properties)
      : pool_(pool), reader_(std::move(reader)), reader_properties_(properties) {}

  virtual ~Impl() {}

  Status GetColumn(int i, FileColumnIteratorFactory iterator_factory,
                   std::unique_ptr<ColumnReader>* out);

  Status ReadSchemaField(int i, std::shared_ptr<ChunkedArray>* out);
  Status ReadSchemaField(int i, const std::vector<int>& indices,
                         std::shared_ptr<ChunkedArray>* out);
  Status ReadColumn(int i, std::shared_ptr<ChunkedArray>* out);
  Status ReadColumnChunk(int column_index, int row_group_index,
                         std::shared_ptr<ChunkedArray>* out);
  Status ReadColumnChunk(int column_index, const std::vector<int>& indices,
                         int row_group_index, std::shared_ptr<ChunkedArray>* out);

  Status GetReaderForNode(int index, const Node* node, const std::vector<int>& indices,
                          int16_t def_level, FileColumnIteratorFactory iterator_factory,
                          std::unique_ptr<ColumnReader::ColumnReaderImpl>* out);

  Status GetSchema(std::shared_ptr<::arrow::Schema>* out);
  Status GetSchema(const std::vector<int>& indices,
                   std::shared_ptr<::arrow::Schema>* out);
  Status ReadRowGroup(int row_group_index, std::shared_ptr<Table>* table);
  Status ReadRowGroup(int row_group_index, const std::vector<int>& indices,
                      std::shared_ptr<::arrow::Table>* out);
  Status ReadTable(const std::vector<int>& indices, std::shared_ptr<Table>* table);
  Status ReadTable(std::shared_ptr<Table>* table);
  Status ReadRowGroups(const std::vector<int>& row_groups, std::shared_ptr<Table>* table);
  Status ReadRowGroups(const std::vector<int>& row_groups,
                       const std::vector<int>& indices,
                       std::shared_ptr<::arrow::Table>* out);

  bool CheckForFlatColumn(const ColumnDescriptor* descr);
  bool CheckForFlatListColumn(const ColumnDescriptor* descr);

  const ParquetFileReader* parquet_reader() const { return reader_.get(); }

  int num_row_groups() const { return reader_->metadata()->num_row_groups(); }

  int num_columns() const { return reader_->metadata()->num_columns(); }

  void set_use_threads(bool use_threads) {
    reader_properties_.set_use_threads(use_threads);
  }

  ParquetFileReader* reader() { return reader_.get(); }

  std::vector<int> GetDictionaryIndices(const std::vector<int>& indices);
  std::shared_ptr<::arrow::Schema> FixSchema(
      const ::arrow::Schema& old_schema, const std::vector<int>& dict_indices,
      std::vector<std::shared_ptr<::arrow::Column>>& columns);

  int64_t batch_size() const { return reader_properties_.batch_size(); }

 private:
  MemoryPool* pool_;
  std::unique_ptr<ParquetFileReader> reader_;
  ArrowReaderProperties reader_properties_;
};

class ColumnReader::ColumnReaderImpl {
 public:
  virtual ~ColumnReaderImpl() {}
  virtual Status NextBatch(int64_t records_to_read,
                           std::shared_ptr<ChunkedArray>* out) = 0;
  virtual Status GetDefLevels(const int16_t** data, size_t* length) = 0;
  virtual Status GetRepLevels(const int16_t** data, size_t* length) = 0;
  virtual const std::shared_ptr<Field> field() = 0;
};

// Reader implementation for primitive arrays
class PARQUET_NO_EXPORT PrimitiveImpl : public ColumnReader::ColumnReaderImpl {
 public:
  PrimitiveImpl(MemoryPool* pool, std::unique_ptr<FileColumnIterator> input,
                const bool read_dictionary)
      : pool_(pool), input_(std::move(input)), descr_(input_->descr()) {
    record_reader_ = RecordReader::Make(descr_, pool_, read_dictionary);
    Status s = NodeToField(*input_->descr()->schema_node(), &field_);
    DCHECK_OK(s);
    NextRowGroup();
  }

  Status NextBatch(int64_t records_to_read, std::shared_ptr<ChunkedArray>* out) override;

  template <typename ParquetType>
  Status WrapIntoListArray(Datum* inout_array);

  Status GetDefLevels(const int16_t** data, size_t* length) override;
  Status GetRepLevels(const int16_t** data, size_t* length) override;

  const std::shared_ptr<Field> field() override { return field_; }

 private:
  void NextRowGroup();

  MemoryPool* pool_;
  std::unique_ptr<FileColumnIterator> input_;
  const ColumnDescriptor* descr_;

  std::shared_ptr<RecordReader> record_reader_;

  std::shared_ptr<Field> field_;
};

// Reader implementation for struct array
class PARQUET_NO_EXPORT StructImpl : public ColumnReader::ColumnReaderImpl {
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

FileReader::FileReader(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader,
                       const ArrowReaderProperties& properties)
    : impl_(new FileReader::Impl(pool, std::move(reader), properties)) {}

FileReader::~FileReader() {}

Status FileReader::Impl::GetColumn(int i, FileColumnIteratorFactory iterator_factory,
                                   std::unique_ptr<ColumnReader>* out) {
  if (i < 0 || i >= this->num_columns()) {
    return Status::Invalid("Column index out of bounds (got ", i,
                           ", should be "
                           "between 0 and ",
                           this->num_columns() - 1, ")");
  }

  std::unique_ptr<FileColumnIterator> input(iterator_factory(i, reader_.get()));
  bool read_dict = reader_properties_.read_dictionary(i);

  std::unique_ptr<ColumnReader::ColumnReaderImpl> impl(
      new PrimitiveImpl(pool_, std::move(input), read_dict));
  *out = std::unique_ptr<ColumnReader>(new ColumnReader(std::move(impl)));
  return Status::OK();
}

Status FileReader::Impl::GetReaderForNode(
    int index, const Node* node, const std::vector<int>& indices, int16_t def_level,
    FileColumnIteratorFactory iterator_factory,
    std::unique_ptr<ColumnReader::ColumnReaderImpl>* out) {
  *out = nullptr;

  if (schema::IsSimpleStruct(node)) {
    const schema::GroupNode* group = static_cast<const schema::GroupNode*>(node);
    std::vector<std::shared_ptr<ColumnReader::ColumnReaderImpl>> children;
    for (int i = 0; i < group->field_count(); i++) {
      std::unique_ptr<ColumnReader::ColumnReaderImpl> child_reader;
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
      *out = std::unique_ptr<ColumnReader::ColumnReaderImpl>(
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
      *out = std::move(reader->impl_);
    }
  }

  return Status::OK();
}

Status FileReader::Impl::ReadSchemaField(int i, std::shared_ptr<ChunkedArray>* out) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t j = 0; j < indices.size(); ++j) {
    indices[j] = static_cast<int>(j);
  }

  return ReadSchemaField(i, indices, out);
}

Status FileReader::Impl::ReadSchemaField(int i, const std::vector<int>& indices,
                                         std::shared_ptr<ChunkedArray>* out) {
  auto iterator_factory = FileColumnIterator::MakeAllRowGroupsIterator;
  auto parquet_schema = reader_->metadata()->schema();
  auto node = parquet_schema->group_node()->field(i).get();
  std::unique_ptr<ColumnReader::ColumnReaderImpl> reader_impl;

  RETURN_NOT_OK(GetReaderForNode(i, node, indices, 1, iterator_factory, &reader_impl));
  if (reader_impl == nullptr) {
    *out = nullptr;
    return Status::OK();
  }

  std::unique_ptr<ColumnReader> reader(new ColumnReader(std::move(reader_impl)));

  // TODO(wesm): This calculation doesn't make much sense when we have repeated
  // schema nodes
  int64_t records_to_read = 0;

  const FileMetaData& metadata = *reader_->metadata();
  for (int j = 0; j < metadata.num_row_groups(); j++) {
    records_to_read += metadata.RowGroup(j)->ColumnChunk(i)->num_values();
  }

  return reader->NextBatch(records_to_read, out);
}

Status FileReader::Impl::ReadColumn(int i, std::shared_ptr<ChunkedArray>* out) {
  auto iterator_factory = FileColumnIterator::MakeAllRowGroupsIterator;
  std::unique_ptr<ColumnReader> flat_column_reader;
  RETURN_NOT_OK(GetColumn(i, iterator_factory, &flat_column_reader));

  int64_t records_to_read = 0;
  for (int j = 0; j < reader_->metadata()->num_row_groups(); j++) {
    records_to_read += reader_->metadata()->RowGroup(j)->ColumnChunk(i)->num_values();
  }

  return flat_column_reader->NextBatch(records_to_read, out);
}

Status FileReader::Impl::GetSchema(const std::vector<int>& indices,
                                   std::shared_ptr<::arrow::Schema>* out) {
  return FromParquetSchema(reader_->metadata()->schema(), indices,
                           reader_->metadata()->key_value_metadata(), out);
}

Status FileReader::Impl::GetSchema(std::shared_ptr<::arrow::Schema>* out) {
  return FromParquetSchema(reader_->metadata()->schema(),
                           reader_->metadata()->key_value_metadata(), out);
}

Status FileReader::Impl::ReadColumnChunk(int column_index, int row_group_index,
                                         std::shared_ptr<ChunkedArray>* out) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = static_cast<int>(i);
  }

  return ReadColumnChunk(column_index, indices, row_group_index, out);
}

Status FileReader::Impl::ReadColumnChunk(int column_index,
                                         const std::vector<int>& indices,
                                         int row_group_index,
                                         std::shared_ptr<ChunkedArray>* out) {
  auto rg_metadata = reader_->metadata()->RowGroup(row_group_index);
  int64_t records_to_read = rg_metadata->ColumnChunk(column_index)->num_values();

  auto parquet_schema = reader_->metadata()->schema();
  auto node = parquet_schema->group_node()->field(column_index).get();
  std::unique_ptr<ColumnReader::ColumnReaderImpl> reader_impl;

  FileColumnIteratorFactory iterator_factory = [row_group_index](
                                                   int i, ParquetFileReader* reader) {
    return FileColumnIterator::MakeSingleRowGroupIterator(i, reader, row_group_index);
  };
  RETURN_NOT_OK(
      GetReaderForNode(column_index, node, indices, 1, iterator_factory, &reader_impl));
  if (reader_impl == nullptr) {
    *out = nullptr;
    return Status::OK();
  }
  ColumnReader reader(std::move(reader_impl));

  return reader.NextBatch(records_to_read, out);
}

Status FileReader::Impl::ReadRowGroup(int row_group_index,
                                      const std::vector<int>& indices,
                                      std::shared_ptr<Table>* out) {
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(GetSchema(indices, &schema));

  auto rg_metadata = reader_->metadata()->RowGroup(row_group_index);

  // We only need to read schema fields which have columns indicated
  // in the indices vector
  std::vector<int> field_indices;
  if (!schema::ColumnIndicesToFieldIndices(*reader_->metadata()->schema(), indices,
                                           &field_indices)) {
    return Status::Invalid("Invalid column index");
  }
  int num_fields = static_cast<int>(field_indices.size());
  std::vector<std::shared_ptr<Column>> columns(num_fields);

  // TODO(wesm): Refactor to share more code with ReadTable

  auto ReadColumnFunc = [&indices, &field_indices, &row_group_index, &schema, &columns,
                         this](int i) {
    std::shared_ptr<ChunkedArray> array;
    RETURN_NOT_OK(ReadColumnChunk(field_indices[i], indices, row_group_index, &array));
    columns[i] = std::make_shared<Column>(schema->field(i), array);
    return Status::OK();
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
}

Status FileReader::Impl::ReadTable(const std::vector<int>& indices,
                                   std::shared_ptr<Table>* out) {
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
  std::vector<std::shared_ptr<Column>> columns(num_fields);

  auto ReadColumnFunc = [&indices, &field_indices, &schema, &columns, this](int i) {
    std::shared_ptr<ChunkedArray> array;
    RETURN_NOT_OK(ReadSchemaField(field_indices[i], indices, &array));
    columns[i] = std::make_shared<Column>(schema->field(i), array);
    return Status::OK();
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
}

Status FileReader::Impl::ReadTable(std::shared_ptr<Table>* table) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = static_cast<int>(i);
  }
  return ReadTable(indices, table);
}

Status FileReader::Impl::ReadRowGroups(const std::vector<int>& row_groups,
                                       const std::vector<int>& indices,
                                       std::shared_ptr<Table>* table) {
  std::vector<std::shared_ptr<Table>> tables(row_groups.size(), nullptr);

  for (size_t i = 0; i < row_groups.size(); ++i) {
    RETURN_NOT_OK(ReadRowGroup(row_groups[i], indices, &tables[i]));
  }
  return ConcatenateTables(tables, table);
}

Status FileReader::Impl::ReadRowGroups(const std::vector<int>& row_groups,
                                       std::shared_ptr<Table>* table) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = static_cast<int>(i);
  }
  return ReadRowGroups(row_groups, indices, table);
}

Status FileReader::Impl::ReadRowGroup(int i, std::shared_ptr<Table>* table) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = static_cast<int>(i);
  }
  return ReadRowGroup(i, indices, table);
}

std::vector<int> FileReader::Impl::GetDictionaryIndices(const std::vector<int>& indices) {
  // Select the column indices that were read as DictionaryArray
  std::vector<int> dict_indices(indices);
  auto remove_func = [this](int i) { return !reader_properties_.read_dictionary(i); };
  auto it = std::remove_if(dict_indices.begin(), dict_indices.end(), remove_func);
  dict_indices.erase(it, dict_indices.end());
  return dict_indices;
}

std::shared_ptr<::arrow::Schema> FileReader::Impl::FixSchema(
    const ::arrow::Schema& old_schema, const std::vector<int>& dict_indices,
    std::vector<std::shared_ptr<::arrow::Column>>& columns) {
  // Fix the schema with the actual DictionaryType that was read
  auto fields = old_schema.fields();

  for (int idx : dict_indices) {
    auto name = columns[idx]->name();
    auto dict_array = columns[idx]->data();
    auto dict_field = std::make_shared<::arrow::Field>(name, dict_array->type());
    fields[idx] = dict_field;
    columns[idx] = std::make_shared<Column>(dict_field, dict_array);
  }

  return std::make_shared<::arrow::Schema>(fields, old_schema.metadata());
}

// Static ctor
Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                MemoryPool* allocator, const ReaderProperties& props,
                const std::shared_ptr<FileMetaData>& metadata,
                std::unique_ptr<FileReader>* reader) {
  std::unique_ptr<ParquetReader> pq_reader;
  PARQUET_CATCH_NOT_OK(pq_reader = ParquetReader::Open(file, props, metadata));
  reader->reset(new FileReader(allocator, std::move(pq_reader)));
  return Status::OK();
}

Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                MemoryPool* allocator, std::unique_ptr<FileReader>* reader) {
  return OpenFile(file, allocator, ::parquet::default_reader_properties(), nullptr,
                  reader);
}

Status OpenFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file,
                ::arrow::MemoryPool* allocator, const ArrowReaderProperties& properties,
                std::unique_ptr<FileReader>* reader) {
  std::unique_ptr<ParquetReader> pq_reader;
  PARQUET_CATCH_NOT_OK(pq_reader = ParquetReader::Open(
                           file, ::parquet::default_reader_properties(), nullptr));
  reader->reset(new FileReader(allocator, std::move(pq_reader), properties));
  return Status::OK();
}

Status FileReader::GetColumn(int i, std::unique_ptr<ColumnReader>* out) {
  auto iterator_factory = FileColumnIterator::MakeAllRowGroupsIterator;
  return impl_->GetColumn(i, iterator_factory, out);
}

Status FileReader::GetSchema(std::shared_ptr<::arrow::Schema>* out) {
  return impl_->GetSchema(out);
}

Status FileReader::GetSchema(const std::vector<int>& indices,
                             std::shared_ptr<::arrow::Schema>* out) {
  return impl_->GetSchema(indices, out);
}

Status FileReader::ReadColumn(int i, std::shared_ptr<ChunkedArray>* out) {
  try {
    return impl_->ReadColumn(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadSchemaField(int i, std::shared_ptr<ChunkedArray>* out) {
  try {
    return impl_->ReadSchemaField(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadColumn(int i, std::shared_ptr<Array>* out) {
  std::shared_ptr<ChunkedArray> chunked_out;
  RETURN_NOT_OK(ReadColumn(i, &chunked_out));
  return GetSingleChunk(*chunked_out, out);
}

Status FileReader::ReadSchemaField(int i, std::shared_ptr<Array>* out) {
  std::shared_ptr<ChunkedArray> chunked_out;
  RETURN_NOT_OK(ReadSchemaField(i, &chunked_out));
  return GetSingleChunk(*chunked_out, out);
}

Status FileReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                        std::shared_ptr<RecordBatchReader>* out) {
  std::vector<int> indices(impl_->num_columns());

  for (size_t j = 0; j < indices.size(); ++j) {
    indices[j] = static_cast<int>(j);
  }

  return GetRecordBatchReader(row_group_indices, indices, out);
}

Status FileReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                        const std::vector<int>& column_indices,
                                        std::shared_ptr<RecordBatchReader>* out) {
  // column indices check
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(GetSchema(column_indices, &schema));

  // row group indices check
  int max_num = num_row_groups();
  for (auto row_group_index : row_group_indices) {
    if (row_group_index < 0 || row_group_index >= max_num) {
      return Status::Invalid("Some index in row_group_indices is ", row_group_index,
                             ", which is either < 0 or >= num_row_groups(", max_num, ")");
    }
  }

  *out = std::make_shared<RowGroupRecordBatchReader>(row_group_indices, column_indices,
                                                     schema, this, impl_->batch_size());
  return Status::OK();
}

Status FileReader::ReadTable(std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadTable(out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadTable(const std::vector<int>& indices,
                             std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadTable(indices, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadRowGroup(int i, std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadRowGroup(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadRowGroup(int i, const std::vector<int>& indices,
                                std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadRowGroup(i, indices, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadRowGroups(const std::vector<int>& row_groups,
                                 std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadRowGroups(row_groups, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadRowGroups(const std::vector<int>& row_groups,
                                 const std::vector<int>& indices,
                                 std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadRowGroups(row_groups, indices, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

std::shared_ptr<RowGroupReader> FileReader::RowGroup(int row_group_index) {
  return std::shared_ptr<RowGroupReader>(
      new RowGroupReader(impl_.get(), row_group_index));
}

int FileReader::num_row_groups() const { return impl_->num_row_groups(); }

void FileReader::set_num_threads(int num_threads) {}

void FileReader::set_use_threads(bool use_threads) {
  impl_->set_use_threads(use_threads);
}

Status FileReader::ScanContents(std::vector<int> columns, const int32_t column_batch_size,
                                int64_t* num_rows) {
  try {
    *num_rows = ScanFileContents(columns, column_batch_size, impl_->reader());
    return Status::OK();
  } catch (const ::parquet::ParquetException& e) {
    return Status::IOError(e.what());
  }
}

const ParquetFileReader* FileReader::parquet_reader() const {
  return impl_->parquet_reader();
}

template <typename ParquetType>
Status PrimitiveImpl::WrapIntoListArray(Datum* inout_array) {
  if (descr_->max_repetition_level() == 0) {
    // Flat, no action
    return Status::OK();
  }

  std::shared_ptr<Array> flat_array;

  // ARROW-3762(wesm): If inout_array is a chunked array, we reject as this is
  // not yet implemented
  if (inout_array->kind() == Datum::CHUNKED_ARRAY) {
    if (inout_array->chunked_array()->num_chunks() > 1) {
      return Status::NotImplemented(
          "Nested data conversions not implemented for "
          "chunked array outputs");
    }
    flat_array = inout_array->chunked_array()->chunk(0);
  } else {
    DCHECK_EQ(Datum::ARRAY, inout_array->kind());
    flat_array = inout_array->make_array();
  }

  const int16_t* def_levels = record_reader_->def_levels();
  const int16_t* rep_levels = record_reader_->rep_levels();
  const int64_t total_levels_read = record_reader_->levels_position();

  std::shared_ptr<::arrow::Schema> arrow_schema;
  RETURN_NOT_OK(FromParquetSchema(input_->schema(), {input_->column_index()},
                                  input_->metadata()->key_value_metadata(),
                                  &arrow_schema));
  std::shared_ptr<Field> current_field = arrow_schema->field(0);

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
  *inout_array = output;
  return Status::OK();
}

template <typename ArrowType, typename ParquetType>
struct supports_fast_path_impl {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;
  static constexpr bool value = std::is_same<ArrowCType, ParquetCType>::value;
};

template <typename ArrowType>
struct supports_fast_path_impl<ArrowType, ByteArrayType> {
  static constexpr bool value = false;
};

template <typename ArrowType>
struct supports_fast_path_impl<ArrowType, FLBAType> {
  static constexpr bool value = false;
};

template <typename ArrowType, typename ParquetType>
using supports_fast_path =
    typename std::enable_if<supports_fast_path_impl<ArrowType, ParquetType>::value>::type;

template <typename ArrowType, typename ParquetType, typename Enable = void>
struct TransferFunctor {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;

  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    static_assert(!std::is_same<ArrowType, ::arrow::Int32Type>::value,
                  "The fast path transfer functor should be used "
                  "for primitive values");

    int64_t length = reader->values_written();
    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(ArrowCType), &data));

    auto values = reinterpret_cast<const ParquetCType*>(reader->values());
    auto out_ptr = reinterpret_cast<ArrowCType*>(data->mutable_data());
    std::copy(values, values + length, out_ptr);

    if (reader->nullable_values()) {
      std::shared_ptr<ResizableBuffer> is_valid = reader->ReleaseIsValid();
      *out = std::make_shared<ArrayType<ArrowType>>(type, length, data, is_valid,
                                                    reader->null_count());
    } else {
      *out = std::make_shared<ArrayType<ArrowType>>(type, length, data);
    }
    return Status::OK();
  }
};

template <typename ArrowType, typename ParquetType>
struct TransferFunctor<ArrowType, ParquetType,
                       supports_fast_path<ArrowType, ParquetType>> {
  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    int64_t length = reader->values_written();
    std::shared_ptr<ResizableBuffer> values = reader->ReleaseValues();

    if (reader->nullable_values()) {
      std::shared_ptr<ResizableBuffer> is_valid = reader->ReleaseIsValid();
      *out = std::make_shared<ArrayType<ArrowType>>(type, length, values, is_valid,
                                                    reader->null_count());
    } else {
      *out = std::make_shared<ArrayType<ArrowType>>(type, length, values);
    }
    return Status::OK();
  }
};

template <>
struct TransferFunctor<::arrow::BooleanType, BooleanType> {
  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    int64_t length = reader->values_written();
    std::shared_ptr<Buffer> data;

    const int64_t buffer_size = BitUtil::BytesForBits(length);
    RETURN_NOT_OK(::arrow::AllocateBuffer(pool, buffer_size, &data));

    // Transfer boolean values to packed bitmap
    auto values = reinterpret_cast<const bool*>(reader->values());
    uint8_t* data_ptr = data->mutable_data();
    memset(data_ptr, 0, buffer_size);

    for (int64_t i = 0; i < length; i++) {
      if (values[i]) {
        ::arrow::BitUtil::SetBit(data_ptr, i);
      }
    }

    if (reader->nullable_values()) {
      std::shared_ptr<ResizableBuffer> is_valid = reader->ReleaseIsValid();
      RETURN_NOT_OK(is_valid->Resize(BitUtil::BytesForBits(length), false));
      *out = std::make_shared<BooleanArray>(type, length, data, is_valid,
                                            reader->null_count());
    } else {
      *out = std::make_shared<BooleanArray>(type, length, data);
    }
    return Status::OK();
  }
};

template <>
struct TransferFunctor<::arrow::TimestampType, Int96Type> {
  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    int64_t length = reader->values_written();
    auto values = reinterpret_cast<const Int96*>(reader->values());

    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(int64_t), &data));

    auto data_ptr = reinterpret_cast<int64_t*>(data->mutable_data());
    for (int64_t i = 0; i < length; i++) {
      *data_ptr++ = Int96GetNanoSeconds(values[i]);
    }

    if (reader->nullable_values()) {
      std::shared_ptr<ResizableBuffer> is_valid = reader->ReleaseIsValid();
      *out = std::make_shared<TimestampArray>(type, length, data, is_valid,
                                              reader->null_count());
    } else {
      *out = std::make_shared<TimestampArray>(type, length, data);
    }

    return Status::OK();
  }
};

template <>
struct TransferFunctor<::arrow::Date64Type, Int32Type> {
  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    int64_t length = reader->values_written();
    auto values = reinterpret_cast<const int32_t*>(reader->values());

    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(int64_t), &data));
    auto out_ptr = reinterpret_cast<int64_t*>(data->mutable_data());

    for (int64_t i = 0; i < length; i++) {
      *out_ptr++ = static_cast<int64_t>(values[i]) * kMillisecondsPerDay;
    }

    if (reader->nullable_values()) {
      std::shared_ptr<ResizableBuffer> is_valid = reader->ReleaseIsValid();
      *out = std::make_shared<::arrow::Date64Array>(type, length, data, is_valid,
                                                    reader->null_count());
    } else {
      *out = std::make_shared<::arrow::Date64Array>(type, length, data);
    }
    return Status::OK();
  }
};

template <typename ArrowType, typename ParquetType>
struct TransferFunctor<
    ArrowType, ParquetType,
    typename std::enable_if<
        (std::is_base_of<::arrow::BinaryType, ArrowType>::value ||
         std::is_same<::arrow::FixedSizeBinaryType, ArrowType>::value) &&
        (std::is_same<ParquetType, ByteArrayType>::value ||
         std::is_same<ParquetType, FLBAType>::value)>::type> {
  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    std::vector<std::shared_ptr<Array>> chunks = reader->GetBuilderChunks();
    *out = std::make_shared<ChunkedArray>(chunks);
    return Status::OK();
  }
};

static uint64_t BytesToInteger(const uint8_t* bytes, int32_t start, int32_t stop) {
  const int32_t length = stop - start;

  DCHECK_GE(length, 0);
  DCHECK_LE(length, 8);

  switch (length) {
    case 0:
      return 0;
    case 1:
      return bytes[start];
    case 2:
      return FromBigEndian(SafeLoadAs<uint16_t>(bytes + start));
    case 3: {
      const uint64_t first_two_bytes = FromBigEndian(SafeLoadAs<uint16_t>(bytes + start));
      const uint64_t last_byte = bytes[stop - 1];
      return first_two_bytes << 8 | last_byte;
    }
    case 4:
      return FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
    case 5: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t last_byte = bytes[stop - 1];
      return first_four_bytes << 8 | last_byte;
    }
    case 6: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t last_two_bytes =
          FromBigEndian(SafeLoadAs<uint16_t>(bytes + start + 4));
      return first_four_bytes << 16 | last_two_bytes;
    }
    case 7: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t second_two_bytes =
          FromBigEndian(SafeLoadAs<uint16_t>(bytes + start + 4));
      const uint64_t last_byte = bytes[stop - 1];
      return first_four_bytes << 24 | second_two_bytes << 8 | last_byte;
    }
    case 8:
      return FromBigEndian(SafeLoadAs<uint64_t>(bytes + start));
    default: {
      DCHECK(false);
      return UINT64_MAX;
    }
  }
}

static constexpr int32_t kMinDecimalBytes = 1;
static constexpr int32_t kMaxDecimalBytes = 16;

/// \brief Convert a sequence of big-endian bytes to one int64_t (high bits) and one
/// uint64_t (low bits).
static void BytesToIntegerPair(const uint8_t* bytes, const int32_t length,
                               int64_t* out_high, uint64_t* out_low) {
  DCHECK_GE(length, kMinDecimalBytes);
  DCHECK_LE(length, kMaxDecimalBytes);

  // XXX This code is copied from Decimal::FromBigEndian

  int64_t high, low;

  // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  // sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  // 1. Extract the high bytes
  // Stop byte of the high bytes
  const int32_t high_bits_offset = std::max(0, length - 8);
  const auto high_bits = BytesToInteger(bytes, 0, high_bits_offset);

  if (high_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    high = high_bits;
  } else {
    high = -1 * (is_negative && length < kMaxDecimalBytes);
    // Shift left enough bits to make room for the incoming int64_t
    high = SafeLeftShift(high, high_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    high |= high_bits;
  }

  // 2. Extract the low bytes
  // Stop byte of the low bytes
  const int32_t low_bits_offset = std::min(length, 8);
  const auto low_bits = BytesToInteger(bytes, high_bits_offset, length);

  if (low_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    low = low_bits;
  } else {
    // Sign extend the low bits if necessary
    low = -1 * (is_negative && length < 8);
    // Shift left enough bits to make room for the incoming int64_t
    low = SafeLeftShift(low, low_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    low |= low_bits;
  }

  *out_high = high;
  *out_low = static_cast<uint64_t>(low);
}

static inline void RawBytesToDecimalBytes(const uint8_t* value, int32_t byte_width,
                                          uint8_t* out_buf) {
  // view the first 8 bytes as an unsigned 64-bit integer
  auto low = reinterpret_cast<uint64_t*>(out_buf);

  // view the second 8 bytes as a signed 64-bit integer
  auto high = reinterpret_cast<int64_t*>(out_buf + sizeof(uint64_t));

  // Convert the fixed size binary array bytes into a Decimal128 compatible layout
  BytesToIntegerPair(value, byte_width, high, low);
}

// ----------------------------------------------------------------------
// BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY -> Decimal128

template <typename T>
Status ConvertToDecimal128(const Array& array, const std::shared_ptr<::arrow::DataType>&,
                           MemoryPool* pool, std::shared_ptr<Array>*) {
  return Status::NotImplemented("not implemented");
}

template <>
Status ConvertToDecimal128<FLBAType>(const Array& array,
                                     const std::shared_ptr<::arrow::DataType>& type,
                                     MemoryPool* pool, std::shared_ptr<Array>* out) {
  const auto& fixed_size_binary_array =
      static_cast<const ::arrow::FixedSizeBinaryArray&>(array);

  // The byte width of each decimal value
  const int32_t type_length =
      static_cast<const ::arrow::Decimal128Type&>(*type).byte_width();

  // number of elements in the entire array
  const int64_t length = fixed_size_binary_array.length();

  // Get the byte width of the values in the FixedSizeBinaryArray. Most of the time
  // this will be different from the decimal array width because we write the minimum
  // number of bytes necessary to represent a given precision
  const int32_t byte_width =
      static_cast<const ::arrow::FixedSizeBinaryType&>(*fixed_size_binary_array.type())
          .byte_width();

  // allocate memory for the decimal array
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));

  // raw bytes that we can write to
  uint8_t* out_ptr = data->mutable_data();

  // convert each FixedSizeBinary value to valid decimal bytes
  const int64_t null_count = fixed_size_binary_array.null_count();
  if (null_count > 0) {
    for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
      if (!fixed_size_binary_array.IsNull(i)) {
        RawBytesToDecimalBytes(fixed_size_binary_array.GetValue(i), byte_width, out_ptr);
      }
    }
  } else {
    for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
      RawBytesToDecimalBytes(fixed_size_binary_array.GetValue(i), byte_width, out_ptr);
    }
  }

  *out = std::make_shared<::arrow::Decimal128Array>(
      type, length, data, fixed_size_binary_array.null_bitmap(), null_count);

  return Status::OK();
}

template <>
Status ConvertToDecimal128<ByteArrayType>(const Array& array,
                                          const std::shared_ptr<::arrow::DataType>& type,
                                          MemoryPool* pool, std::shared_ptr<Array>* out) {
  const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(array);
  const int64_t length = binary_array.length();

  const auto& decimal_type = static_cast<const ::arrow::Decimal128Type&>(*type);
  const int64_t type_length = decimal_type.byte_width();

  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));

  // raw bytes that we can write to
  uint8_t* out_ptr = data->mutable_data();

  const int64_t null_count = binary_array.null_count();

  // convert each BinaryArray value to valid decimal bytes
  for (int64_t i = 0; i < length; i++, out_ptr += type_length) {
    int32_t record_len = 0;
    const uint8_t* record_loc = binary_array.GetValue(i, &record_len);

    if ((record_len < 0) || (record_len > type_length)) {
      return Status::Invalid("Invalid BYTE_ARRAY size");
    }

    auto out_ptr_view = reinterpret_cast<uint64_t*>(out_ptr);
    out_ptr_view[0] = 0;
    out_ptr_view[1] = 0;

    // only convert rows that are not null if there are nulls, or
    // all rows, if there are not
    if (((null_count > 0) && !binary_array.IsNull(i)) || (null_count <= 0)) {
      RawBytesToDecimalBytes(record_loc, record_len, out_ptr);
    }
  }

  *out = std::make_shared<::arrow::Decimal128Array>(
      type, length, data, binary_array.null_bitmap(), null_count);
  return Status::OK();
}

/// \brief Convert an arrow::BinaryArray to an arrow::Decimal128Array
/// We do this by:
/// 1. Creating an arrow::BinaryArray from the RecordReader's builder
/// 2. Allocating a buffer for the arrow::Decimal128Array
/// 3. Converting the big-endian bytes in each BinaryArray entry to two integers
///    representing the high and low bits of each decimal value.
template <typename ArrowType, typename ParquetType>
struct TransferFunctor<
    ArrowType, ParquetType,
    typename std::enable_if<std::is_same<ArrowType, ::arrow::Decimal128Type>::value &&
                            (std::is_same<ParquetType, ByteArrayType>::value ||
                             std::is_same<ParquetType, FLBAType>::value)>::type> {
  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    DCHECK_EQ(type->id(), ::arrow::Type::DECIMAL);

    ::arrow::ArrayVector chunks = reader->GetBuilderChunks();

    for (size_t i = 0; i < chunks.size(); ++i) {
      std::shared_ptr<Array> chunk_as_decimal;
      RETURN_NOT_OK(
          ConvertToDecimal128<ParquetType>(*chunks[i], type, pool, &chunk_as_decimal));

      // Replace the chunk, which will hopefully also free memory as we go
      chunks[i] = chunk_as_decimal;
    }
    *out = std::make_shared<ChunkedArray>(chunks);
    return Status::OK();
  }
};

/// \brief Convert an Int32 or Int64 array into a Decimal128Array
/// The parquet spec allows systems to write decimals in int32, int64 if the values are
/// small enough to fit in less 4 bytes or less than 8 bytes, respectively.
/// This function implements the conversion from int32 and int64 arrays to decimal arrays.
template <typename ParquetIntegerType,
          typename = typename std::enable_if<
              std::is_same<ParquetIntegerType, Int32Type>::value ||
              std::is_same<ParquetIntegerType, Int64Type>::value>::type>
static Status DecimalIntegerTransfer(RecordReader* reader, MemoryPool* pool,
                                     const std::shared_ptr<::arrow::DataType>& type,
                                     Datum* out) {
  DCHECK_EQ(type->id(), ::arrow::Type::DECIMAL);

  const int64_t length = reader->values_written();

  using ElementType = typename ParquetIntegerType::c_type;
  static_assert(std::is_same<ElementType, int32_t>::value ||
                    std::is_same<ElementType, int64_t>::value,
                "ElementType must be int32_t or int64_t");

  const auto values = reinterpret_cast<const ElementType*>(reader->values());

  const auto& decimal_type = static_cast<const ::arrow::Decimal128Type&>(*type);
  const int64_t type_length = decimal_type.byte_width();

  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));
  uint8_t* out_ptr = data->mutable_data();

  using ::arrow::BitUtil::FromLittleEndian;

  for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
    // sign/zero extend int32_t values, otherwise a no-op
    const auto value = static_cast<int64_t>(values[i]);

    auto out_ptr_view = reinterpret_cast<uint64_t*>(out_ptr);

    // No-op on little endian machines, byteswap on big endian
    out_ptr_view[0] = FromLittleEndian(static_cast<uint64_t>(value));

    // no need to byteswap here because we're sign/zero extending exactly 8 bytes
    out_ptr_view[1] = static_cast<uint64_t>(value < 0 ? -1 : 0);
  }

  if (reader->nullable_values()) {
    std::shared_ptr<ResizableBuffer> is_valid = reader->ReleaseIsValid();
    *out = std::make_shared<::arrow::Decimal128Array>(type, length, data, is_valid,
                                                      reader->null_count());
  } else {
    *out = std::make_shared<::arrow::Decimal128Array>(type, length, data);
  }
  return Status::OK();
}

template <>
struct TransferFunctor<::arrow::Decimal128Type, Int32Type> {
  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    return DecimalIntegerTransfer<Int32Type>(reader, pool, type, out);
  }
};

template <>
struct TransferFunctor<::arrow::Decimal128Type, Int64Type> {
  Status operator()(RecordReader* reader, MemoryPool* pool,
                    const std::shared_ptr<::arrow::DataType>& type, Datum* out) {
    return DecimalIntegerTransfer<Int64Type>(reader, pool, type, out);
  }
};

#define TRANSFER_DATA(ArrowType, ParquetType)                                \
  TransferFunctor<ArrowType, ParquetType> func;                              \
  RETURN_NOT_OK(func(record_reader_.get(), pool_, field_->type(), &result)); \
  RETURN_NOT_OK(WrapIntoListArray<ParquetType>(&result))

#define TRANSFER_CASE(ENUM, ArrowType, ParquetType) \
  case ::arrow::Type::ENUM: {                       \
    TRANSFER_DATA(ArrowType, ParquetType);          \
  } break;

Status PrimitiveImpl::NextBatch(int64_t records_to_read,
                                std::shared_ptr<ChunkedArray>* out) {
  try {
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
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }

  Datum result;
  switch (field_->type()->id()) {
    TRANSFER_CASE(BOOL, ::arrow::BooleanType, BooleanType)
    TRANSFER_CASE(UINT8, ::arrow::UInt8Type, Int32Type)
    TRANSFER_CASE(INT8, ::arrow::Int8Type, Int32Type)
    TRANSFER_CASE(UINT16, ::arrow::UInt16Type, Int32Type)
    TRANSFER_CASE(INT16, ::arrow::Int16Type, Int32Type)
    TRANSFER_CASE(UINT32, ::arrow::UInt32Type, Int32Type)
    TRANSFER_CASE(INT32, ::arrow::Int32Type, Int32Type)
    TRANSFER_CASE(UINT64, ::arrow::UInt64Type, Int64Type)
    TRANSFER_CASE(INT64, ::arrow::Int64Type, Int64Type)
    TRANSFER_CASE(FLOAT, ::arrow::FloatType, FloatType)
    TRANSFER_CASE(DOUBLE, ::arrow::DoubleType, DoubleType)
    TRANSFER_CASE(STRING, ::arrow::StringType, ByteArrayType)
    TRANSFER_CASE(BINARY, ::arrow::BinaryType, ByteArrayType)
    TRANSFER_CASE(DATE32, ::arrow::Date32Type, Int32Type)
    TRANSFER_CASE(DATE64, ::arrow::Date64Type, Int32Type)
    TRANSFER_CASE(FIXED_SIZE_BINARY, ::arrow::FixedSizeBinaryType, FLBAType)
    case ::arrow::Type::NA: {
      result = std::make_shared<::arrow::NullArray>(record_reader_->values_written());
      RETURN_NOT_OK(WrapIntoListArray<Int32Type>(&result));
      break;
    }
    case ::arrow::Type::DECIMAL: {
      switch (descr_->physical_type()) {
        case ::parquet::Type::INT32: {
          TRANSFER_DATA(::arrow::Decimal128Type, Int32Type);
        } break;
        case ::parquet::Type::INT64: {
          TRANSFER_DATA(::arrow::Decimal128Type, Int64Type);
        } break;
        case ::parquet::Type::BYTE_ARRAY: {
          TRANSFER_DATA(::arrow::Decimal128Type, ByteArrayType);
        } break;
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
          TRANSFER_DATA(::arrow::Decimal128Type, FLBAType);
        } break;
        default:
          return Status::Invalid(
              "Physical type for decimal must be int32, int64, byte array, or fixed "
              "length binary");
      }
    } break;
    case ::arrow::Type::TIMESTAMP: {
      ::arrow::TimestampType* timestamp_type =
          static_cast<::arrow::TimestampType*>(field_->type().get());
      switch (timestamp_type->unit()) {
        case ::arrow::TimeUnit::MILLI:
        case ::arrow::TimeUnit::MICRO: {
          TRANSFER_DATA(::arrow::TimestampType, Int64Type);
        } break;
        case ::arrow::TimeUnit::NANO: {
          if (descr_->physical_type() == ::parquet::Type::INT96) {
            TRANSFER_DATA(::arrow::TimestampType, Int96Type);
          } else {
            TRANSFER_DATA(::arrow::TimestampType, Int64Type);
          }
        } break;
        default:
          return Status::NotImplemented("TimeUnit not supported");
      }
    } break;
      TRANSFER_CASE(TIME32, ::arrow::Time32Type, Int32Type)
      TRANSFER_CASE(TIME64, ::arrow::Time64Type, Int64Type)
    default:
      return Status::NotImplemented("No support for reading columns of type ",
                                    field_->type()->ToString());
  }

  DCHECK_NE(result.kind(), Datum::NONE);

  if (result.kind() == Datum::ARRAY) {
    *out = std::make_shared<ChunkedArray>(result.make_array());
  } else if (result.kind() == Datum::CHUNKED_ARRAY) {
    *out = result.chunked_array();
  } else {
    DCHECK(false) << "Should be impossible";
  }
  return Status::OK();
}

void PrimitiveImpl::NextRowGroup() {
  std::unique_ptr<PageReader> page_reader = input_->NextChunk();
  record_reader_->SetPageReader(std::move(page_reader));
}

Status PrimitiveImpl::GetDefLevels(const int16_t** data, size_t* length) {
  *data = record_reader_->def_levels();
  *length = record_reader_->levels_written();
  return Status::OK();
}

Status PrimitiveImpl::GetRepLevels(const int16_t** data, size_t* length) {
  *data = record_reader_->rep_levels();
  *length = record_reader_->levels_written();
  return Status::OK();
}

ColumnReader::ColumnReader(std::unique_ptr<ColumnReaderImpl> impl)
    : impl_(std::move(impl)) {}

ColumnReader::~ColumnReader() {}

Status ColumnReader::NextBatch(int64_t records_to_read,
                               std::shared_ptr<ChunkedArray>* out) {
  return impl_->NextBatch(records_to_read, out);
}

Status ColumnReader::NextBatch(int64_t records_to_read, std::shared_ptr<Array>* out) {
  std::shared_ptr<ChunkedArray> chunked_out;
  RETURN_NOT_OK(impl_->NextBatch(records_to_read, &chunked_out));
  return GetSingleChunk(*chunked_out, out);
}

// StructImpl methods

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
  field_ = ::arrow::field(node->name(), type);
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

std::shared_ptr<ColumnChunkReader> RowGroupReader::Column(int column_index) {
  return std::shared_ptr<ColumnChunkReader>(
      new ColumnChunkReader(impl_, row_group_index_, column_index));
}

Status RowGroupReader::ReadTable(const std::vector<int>& column_indices,
                                 std::shared_ptr<::arrow::Table>* out) {
  return impl_->ReadRowGroup(row_group_index_, column_indices, out);
}

Status RowGroupReader::ReadTable(std::shared_ptr<::arrow::Table>* out) {
  return impl_->ReadRowGroup(row_group_index_, out);
}

RowGroupReader::~RowGroupReader() {}

RowGroupReader::RowGroupReader(FileReader::Impl* impl, int row_group_index)
    : impl_(impl), row_group_index_(row_group_index) {}

Status ColumnChunkReader::Read(std::shared_ptr<::arrow::ChunkedArray>* out) {
  return impl_->ReadColumnChunk(column_index_, row_group_index_, out);
}

Status ColumnChunkReader::Read(std::shared_ptr<::arrow::Array>* out) {
  std::shared_ptr<ChunkedArray> chunked_out;
  RETURN_NOT_OK(Read(&chunked_out));
  return GetSingleChunk(*chunked_out, out);
}

ColumnChunkReader::~ColumnChunkReader() {}

ColumnChunkReader::ColumnChunkReader(FileReader::Impl* impl, int row_group_index,
                                     int column_index)
    : impl_(impl), column_index_(column_index), row_group_index_(row_group_index) {}

}  // namespace arrow
}  // namespace parquet

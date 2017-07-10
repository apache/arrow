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
#include <atomic>
#include <chrono>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/bit-util.h"

#include "parquet/arrow/schema.h"
#include "parquet/util/schema-util.h"

using arrow::Array;
using arrow::BooleanArray;
using arrow::Column;
using arrow::Field;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::StructArray;
using arrow::MemoryPool;
using arrow::PoolBuffer;
using arrow::Status;
using arrow::Table;

using parquet::schema::NodePtr;

// Help reduce verbosity
using ParquetReader = parquet::ParquetFileReader;

namespace parquet {
namespace arrow {

constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kNanosecondsInADay = 86400LL * 1000LL * 1000LL * 1000LL;

static inline int64_t impala_timestamp_to_nanoseconds(const Int96& impala_timestamp) {
  int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;
  int64_t nanoseconds = *(reinterpret_cast<const int64_t*>(&(impala_timestamp.value)));
  return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

template <typename ArrowType>
using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

// ----------------------------------------------------------------------
// Helper for parallel for-loop

template <class FUNCTION>
Status ParallelFor(int nthreads, int num_tasks, FUNCTION&& func) {
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(nthreads);
  std::atomic<int> task_counter(0);

  std::mutex error_mtx;
  bool error_occurred = false;
  Status error;

  for (int thread_id = 0; thread_id < nthreads; ++thread_id) {
    thread_pool.emplace_back(
        [&num_tasks, &task_counter, &error, &error_occurred, &error_mtx, &func]() {
          int task_id;
          while (!error_occurred) {
            task_id = task_counter.fetch_add(1);
            if (task_id >= num_tasks) { break; }
            Status s = func(task_id);
            if (!s.ok()) {
              std::lock_guard<std::mutex> lock(error_mtx);
              error_occurred = true;
              error = s;
              break;
            }
          }
        });
  }
  for (auto&& thread : thread_pool) {
    thread.join();
  }
  if (error_occurred) { return error; }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Iteration utilities

// Abstraction to decouple row group iteration details from the ColumnReader,
// so we can read only a single row group if we want
class FileColumnIterator {
 public:
  explicit FileColumnIterator(int column_index, ParquetFileReader* reader)
      : column_index_(column_index),
        reader_(reader),
        schema_(reader->metadata()->schema()) {}

  virtual ~FileColumnIterator() {}

  virtual std::shared_ptr<::parquet::ColumnReader> Next() = 0;

  const SchemaDescriptor* schema() const { return schema_; }

  const ColumnDescriptor* descr() const { return schema_->Column(column_index_); }

  std::shared_ptr<FileMetaData> metadata() const { return reader_->metadata(); }

  int column_index() const { return column_index_; }

 protected:
  int column_index_;
  ParquetFileReader* reader_;
  const SchemaDescriptor* schema_;
};

class AllRowGroupsIterator : public FileColumnIterator {
 public:
  explicit AllRowGroupsIterator(int column_index, ParquetFileReader* reader)
      : FileColumnIterator(column_index, reader), next_row_group_(0) {}

  std::shared_ptr<::parquet::ColumnReader> Next() override {
    std::shared_ptr<::parquet::ColumnReader> result;
    if (next_row_group_ < reader_->metadata()->num_row_groups()) {
      result = reader_->RowGroup(next_row_group_)->Column(column_index_);
      next_row_group_++;
    } else {
      result = nullptr;
    }
    return result;
  };

 private:
  int next_row_group_;
};

class SingleRowGroupIterator : public FileColumnIterator {
 public:
  explicit SingleRowGroupIterator(
      int column_index, int row_group_number, ParquetFileReader* reader)
      : FileColumnIterator(column_index, reader),
        row_group_number_(row_group_number),
        done_(false) {}

  std::shared_ptr<::parquet::ColumnReader> Next() override {
    if (done_) { return nullptr; }

    auto result = reader_->RowGroup(row_group_number_)->Column(column_index_);
    done_ = true;
    return result;
  };

 private:
  int row_group_number_;
  bool done_;
};

// ----------------------------------------------------------------------
// File reader implementation

class FileReader::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader)
      : pool_(pool), reader_(std::move(reader)), num_threads_(1) {}

  virtual ~Impl() {}

  Status GetColumn(int i, std::unique_ptr<ColumnReader>* out);
  Status ReadSchemaField(int i, std::shared_ptr<Array>* out);
  Status ReadSchemaField(
      int i, const std::vector<int>& indices, std::shared_ptr<Array>* out);
  Status GetReaderForNode(int index, const NodePtr& node, const std::vector<int>& indices,
      int16_t def_level, std::unique_ptr<ColumnReader::Impl>* out);
  Status ReadColumn(int i, std::shared_ptr<Array>* out);
  Status GetSchema(std::shared_ptr<::arrow::Schema>* out);
  Status GetSchema(
      const std::vector<int>& indices, std::shared_ptr<::arrow::Schema>* out);
  Status ReadRowGroup(int row_group_index, const std::vector<int>& indices,
      std::shared_ptr<::arrow::Table>* out);
  Status ReadTable(const std::vector<int>& indices, std::shared_ptr<Table>* table);
  Status ReadTable(std::shared_ptr<Table>* table);
  Status ReadRowGroup(int i, std::shared_ptr<Table>* table);

  bool CheckForFlatColumn(const ColumnDescriptor* descr);
  bool CheckForFlatListColumn(const ColumnDescriptor* descr);

  const ParquetFileReader* parquet_reader() const { return reader_.get(); }

  int num_row_groups() const { return reader_->metadata()->num_row_groups(); }

  void set_num_threads(int num_threads) { num_threads_ = num_threads; }

 private:
  MemoryPool* pool_;
  std::unique_ptr<ParquetFileReader> reader_;

  int num_threads_;
};

typedef const int16_t* ValueLevelsPtr;

class ColumnReader::Impl {
 public:
  virtual ~Impl() {}
  virtual Status NextBatch(int batch_size, std::shared_ptr<Array>* out) = 0;
  virtual Status GetDefLevels(ValueLevelsPtr* data, size_t* length) = 0;
  virtual Status GetRepLevels(ValueLevelsPtr* data, size_t* length) = 0;
  virtual const std::shared_ptr<Field> field() = 0;
};

// Reader implementation for primitive arrays
class PARQUET_NO_EXPORT PrimitiveImpl : public ColumnReader::Impl {
 public:
  PrimitiveImpl(MemoryPool* pool, std::unique_ptr<FileColumnIterator> input)
      : pool_(pool),
        input_(std::move(input)),
        descr_(input_->descr()),
        values_buffer_(pool),
        def_levels_buffer_(pool),
        rep_levels_buffer_(pool) {
    NodeToField(input_->descr()->schema_node(), &field_);
    NextRowGroup();
  }

  virtual ~PrimitiveImpl() {}

  Status NextBatch(int batch_size, std::shared_ptr<Array>* out) override;

  template <typename ArrowType, typename ParquetType>
  Status TypedReadBatch(int batch_size, std::shared_ptr<Array>* out);

  template <typename ArrowType>
  Status ReadByteArrayBatch(int batch_size, std::shared_ptr<Array>* out);

  template <typename ArrowType>
  Status ReadFLBABatch(int batch_size, int byte_width, std::shared_ptr<Array>* out);

  template <typename ArrowType>
  Status InitDataBuffer(int batch_size);
  Status InitValidBits(int batch_size);
  template <typename ArrowType, typename ParquetType>
  Status ReadNullableBatch(TypedColumnReader<ParquetType>* reader, int16_t* def_levels,
      int16_t* rep_levels, int64_t values_to_read, int64_t* levels_read,
      int64_t* values_read);
  template <typename ArrowType, typename ParquetType>
  Status ReadNonNullableBatch(TypedColumnReader<ParquetType>* reader,
      int64_t values_to_read, int64_t* levels_read);
  Status WrapIntoListArray(const int16_t* def_levels, const int16_t* rep_levels,
      int64_t total_values_read, std::shared_ptr<Array>* array);

  Status GetDefLevels(ValueLevelsPtr* data, size_t* length) override;
  Status GetRepLevels(ValueLevelsPtr* data, size_t* length) override;

  const std::shared_ptr<Field> field() override { return field_; }

 private:
  void NextRowGroup();

  template <typename InType, typename OutType>
  struct can_copy_ptr {
    static constexpr bool value =
        std::is_same<InType, OutType>::value ||
        (std::is_integral<InType>{} && std::is_integral<OutType>{} &&
            (sizeof(InType) == sizeof(OutType)));
  };

  MemoryPool* pool_;
  std::unique_ptr<FileColumnIterator> input_;
  const ColumnDescriptor* descr_;

  std::shared_ptr<::parquet::ColumnReader> column_reader_;
  std::shared_ptr<Field> field_;

  PoolBuffer values_buffer_;
  PoolBuffer def_levels_buffer_;
  PoolBuffer rep_levels_buffer_;
  std::shared_ptr<PoolBuffer> data_buffer_;
  uint8_t* data_buffer_ptr_;
  std::shared_ptr<PoolBuffer> valid_bits_buffer_;
  uint8_t* valid_bits_ptr_;
  int64_t valid_bits_idx_;
  int64_t null_count_;
};

// Reader implementation for struct array
class PARQUET_NO_EXPORT StructImpl : public ColumnReader::Impl {
 public:
  explicit StructImpl(const std::vector<std::shared_ptr<Impl>>& children,
      int16_t struct_def_level, MemoryPool* pool, const NodePtr& node)
      : children_(children),
        struct_def_level_(struct_def_level),
        pool_(pool),
        def_levels_buffer_(pool) {
    InitField(node, children);
  }

  virtual ~StructImpl() {}

  Status NextBatch(int batch_size, std::shared_ptr<Array>* out) override;
  Status GetDefLevels(ValueLevelsPtr* data, size_t* length) override;
  Status GetRepLevels(ValueLevelsPtr* data, size_t* length) override;
  const std::shared_ptr<Field> field() override { return field_; }

 private:
  std::vector<std::shared_ptr<Impl>> children_;
  int16_t struct_def_level_;
  MemoryPool* pool_;
  std::shared_ptr<Field> field_;
  PoolBuffer def_levels_buffer_;

  Status DefLevelsToNullArray(
      std::shared_ptr<MutableBuffer>* null_bitmap, int64_t* null_count);
  void InitField(const NodePtr& node, const std::vector<std::shared_ptr<Impl>>& children);
};

FileReader::FileReader(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader)
    : impl_(new FileReader::Impl(pool, std::move(reader))) {}

FileReader::~FileReader() {}

Status FileReader::Impl::GetColumn(int i, std::unique_ptr<ColumnReader>* out) {
  std::unique_ptr<FileColumnIterator> input(new AllRowGroupsIterator(i, reader_.get()));

  std::unique_ptr<ColumnReader::Impl> impl(new PrimitiveImpl(pool_, std::move(input)));
  *out = std::unique_ptr<ColumnReader>(new ColumnReader(std::move(impl)));
  return Status::OK();
}

Status FileReader::Impl::GetReaderForNode(int index, const NodePtr& node,
    const std::vector<int>& indices, int16_t def_level,
    std::unique_ptr<ColumnReader::Impl>* out) {
  *out = nullptr;

  if (IsSimpleStruct(node)) {
    const schema::GroupNode* group = static_cast<const schema::GroupNode*>(node.get());
    std::vector<std::shared_ptr<ColumnReader::Impl>> children;
    for (int i = 0; i < group->field_count(); i++) {
      std::unique_ptr<ColumnReader::Impl> child_reader;
      // TODO(itaiin): Remove the -1 index hack when all types of nested reads
      // are supported. This currently just signals the lower level reader resolution
      // to abort
      RETURN_NOT_OK(GetReaderForNode(
          index, group->field(i), indices, def_level + 1, &child_reader));
      if (child_reader != nullptr) { children.push_back(std::move(child_reader)); }
    }

    if (children.size() > 0) {
      *out = std::unique_ptr<ColumnReader::Impl>(
          new StructImpl(children, def_level, pool_, node));
    }
  } else {
    // This should be a flat field case - translate the field index to
    // the correct column index by walking down to the leaf node
    NodePtr walker = node;
    while (!walker->is_primitive()) {
      DCHECK(walker->is_group());
      auto group = static_cast<GroupNode*>(walker.get());
      if (group->field_count() != 1) {
        return Status::NotImplemented("lists with structs are not supported.");
      }
      walker = group->field(0);
    }
    auto column_index = reader_->metadata()->schema()->ColumnIndex(*walker.get());

    // If the index of the column is found then a reader for the coliumn is needed.
    // Otherwise *out keeps the nullptr value.
    if (std::find(indices.begin(), indices.end(), column_index) != indices.end()) {
      std::unique_ptr<ColumnReader> reader;
      RETURN_NOT_OK(GetColumn(column_index, &reader));
      *out = std::move(reader->impl_);
    }
  }

  return Status::OK();
}

Status FileReader::Impl::ReadSchemaField(int i, std::shared_ptr<Array>* out) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t j = 0; j < indices.size(); ++j) {
    indices[j] = static_cast<int>(j);
  }

  return ReadSchemaField(i, indices, out);
}

Status FileReader::Impl::ReadSchemaField(
    int i, const std::vector<int>& indices, std::shared_ptr<Array>* out) {
  auto parquet_schema = reader_->metadata()->schema();

  auto node = parquet_schema->group_node()->field(i);
  std::unique_ptr<ColumnReader::Impl> reader_impl;

  RETURN_NOT_OK(GetReaderForNode(i, node, indices, 1, &reader_impl));
  if (reader_impl == nullptr) {
    *out = nullptr;
    return Status::OK();
  }

  std::unique_ptr<ColumnReader> reader(new ColumnReader(std::move(reader_impl)));

  int64_t batch_size = 0;
  for (int j = 0; j < reader_->metadata()->num_row_groups(); j++) {
    batch_size += reader_->metadata()->RowGroup(j)->ColumnChunk(i)->num_values();
  }

  return reader->NextBatch(static_cast<int>(batch_size), out);
}

Status FileReader::Impl::ReadColumn(int i, std::shared_ptr<Array>* out) {
  std::unique_ptr<ColumnReader> flat_column_reader;
  RETURN_NOT_OK(GetColumn(i, &flat_column_reader));

  int64_t batch_size = 0;
  for (int j = 0; j < reader_->metadata()->num_row_groups(); j++) {
    batch_size += reader_->metadata()->RowGroup(j)->ColumnChunk(i)->num_values();
  }

  return flat_column_reader->NextBatch(static_cast<int>(batch_size), out);
}

Status FileReader::Impl::GetSchema(
    const std::vector<int>& indices, std::shared_ptr<::arrow::Schema>* out) {
  auto descr = reader_->metadata()->schema();
  auto parquet_key_value_metadata = reader_->metadata()->key_value_metadata();
  return FromParquetSchema(descr, indices, parquet_key_value_metadata, out);
}

Status FileReader::Impl::ReadRowGroup(int row_group_index,
    const std::vector<int>& indices, std::shared_ptr<::arrow::Table>* out) {
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(GetSchema(indices, &schema));

  auto rg_metadata = reader_->metadata()->RowGroup(row_group_index);

  int num_columns = static_cast<int>(indices.size());
  int nthreads = std::min<int>(num_threads_, num_columns);
  std::vector<std::shared_ptr<Column>> columns(num_columns);

  // TODO(wesm): Refactor to share more code with ReadTable

  auto ReadColumnFunc = [&indices, &row_group_index, &schema, &columns, &rg_metadata,
      this](int i) {
    int column_index = indices[i];
    int64_t batch_size = rg_metadata->ColumnChunk(column_index)->num_values();

    std::unique_ptr<FileColumnIterator> input(
        new SingleRowGroupIterator(column_index, row_group_index, reader_.get()));

    std::unique_ptr<ColumnReader::Impl> impl(new PrimitiveImpl(pool_, std::move(input)));
    ColumnReader flat_column_reader(std::move(impl));

    std::shared_ptr<Array> array;
    RETURN_NOT_OK(flat_column_reader.NextBatch(static_cast<int>(batch_size), &array));
    columns[i] = std::make_shared<Column>(schema->field(i), array);
    return Status::OK();
  };

  if (nthreads == 1) {
    for (int i = 0; i < num_columns; i++) {
      RETURN_NOT_OK(ReadColumnFunc(i));
    }
  } else {
    RETURN_NOT_OK(ParallelFor(nthreads, num_columns, ReadColumnFunc));
  }

  *out = std::make_shared<Table>(schema, columns);
  return Status::OK();
}

Status FileReader::Impl::ReadTable(
    const std::vector<int>& indices, std::shared_ptr<Table>* table) {
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(GetSchema(indices, &schema));

  // We only need to read schema fields which have columns indicated
  // in the indices vector
  std::vector<int> field_indices;
  if (!ColumnIndicesToFieldIndices(
          *reader_->metadata()->schema(), indices, &field_indices)) {
    return Status::Invalid("Invalid column index");
  }

  std::vector<std::shared_ptr<Column>> columns(field_indices.size());
  auto ReadColumnFunc = [&indices, &field_indices, &schema, &columns, this](int i) {
    std::shared_ptr<Array> array;
    RETURN_NOT_OK(ReadSchemaField(field_indices[i], indices, &array));
    columns[i] = std::make_shared<Column>(schema->field(i), array);
    return Status::OK();
  };

  int num_fields = static_cast<int>(field_indices.size());
  int nthreads = std::min<int>(num_threads_, num_fields);
  if (nthreads == 1) {
    for (int i = 0; i < num_fields; i++) {
      RETURN_NOT_OK(ReadColumnFunc(i));
    }
  } else {
    RETURN_NOT_OK(ParallelFor(nthreads, num_fields, ReadColumnFunc));
  }

  *table = std::make_shared<Table>(schema, columns);
  return Status::OK();
}

Status FileReader::Impl::ReadTable(std::shared_ptr<Table>* table) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = static_cast<int>(i);
  }
  return ReadTable(indices, table);
}

Status FileReader::Impl::ReadRowGroup(int i, std::shared_ptr<Table>* table) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = static_cast<int>(i);
  }
  return ReadRowGroup(i, indices, table);
}

// Static ctor
Status OpenFile(const std::shared_ptr<::arrow::io::ReadableFileInterface>& file,
    MemoryPool* allocator, const ReaderProperties& props,
    const std::shared_ptr<FileMetaData>& metadata, std::unique_ptr<FileReader>* reader) {
  std::unique_ptr<RandomAccessSource> io_wrapper(new ArrowInputFile(file));
  std::unique_ptr<ParquetReader> pq_reader;
  PARQUET_CATCH_NOT_OK(
      pq_reader = ParquetReader::Open(std::move(io_wrapper), props, metadata));
  reader->reset(new FileReader(allocator, std::move(pq_reader)));
  return Status::OK();
}

Status OpenFile(const std::shared_ptr<::arrow::io::ReadableFileInterface>& file,
    MemoryPool* allocator, std::unique_ptr<FileReader>* reader) {
  return OpenFile(
      file, allocator, ::parquet::default_reader_properties(), nullptr, reader);
}

Status FileReader::GetColumn(int i, std::unique_ptr<ColumnReader>* out) {
  return impl_->GetColumn(i, out);
}

Status FileReader::ReadColumn(int i, std::shared_ptr<Array>* out) {
  try {
    return impl_->ReadColumn(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadSchemaField(int i, std::shared_ptr<Array>* out) {
  try {
    return impl_->ReadSchemaField(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadTable(std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadTable(out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadTable(
    const std::vector<int>& indices, std::shared_ptr<Table>* out) {
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

Status FileReader::ReadRowGroup(
    int i, const std::vector<int>& indices, std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadRowGroup(i, indices, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

int FileReader::num_row_groups() const {
  return impl_->num_row_groups();
}

void FileReader::set_num_threads(int num_threads) {
  impl_->set_num_threads(num_threads);
}

const ParquetFileReader* FileReader::parquet_reader() const {
  return impl_->parquet_reader();
}

template <typename ArrowType, typename ParquetType>
Status PrimitiveImpl::ReadNonNullableBatch(TypedColumnReader<ParquetType>* reader,
    int64_t values_to_read, int64_t* levels_read) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;

  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(ParquetCType), false));
  auto values = reinterpret_cast<ParquetCType*>(values_buffer_.mutable_data());
  int64_t values_read;
  PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(static_cast<int>(values_to_read),
                           nullptr, nullptr, values, &values_read));

  ArrowCType* out_ptr = reinterpret_cast<ArrowCType*>(data_buffer_ptr_);
  std::copy(values, values + values_read, out_ptr + valid_bits_idx_);
  valid_bits_idx_ += values_read;

  return Status::OK();
}

#define NONNULLABLE_BATCH_FAST_PATH(ArrowType, ParquetType, CType)               \
  template <>                                                                    \
  Status PrimitiveImpl::ReadNonNullableBatch<ArrowType, ParquetType>(            \
      TypedColumnReader<ParquetType> * reader, int64_t values_to_read,           \
      int64_t * levels_read) {                                                   \
    int64_t values_read;                                                         \
    CType* out_ptr = reinterpret_cast<CType*>(data_buffer_ptr_);                 \
    PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(                       \
                             static_cast<int>(values_to_read), nullptr, nullptr, \
                             out_ptr + valid_bits_idx_, &values_read));          \
                                                                                 \
    valid_bits_idx_ += values_read;                                              \
                                                                                 \
    return Status::OK();                                                         \
  }

NONNULLABLE_BATCH_FAST_PATH(::arrow::Int32Type, Int32Type, int32_t)
NONNULLABLE_BATCH_FAST_PATH(::arrow::Int64Type, Int64Type, int64_t)
NONNULLABLE_BATCH_FAST_PATH(::arrow::FloatType, FloatType, float)
NONNULLABLE_BATCH_FAST_PATH(::arrow::DoubleType, DoubleType, double)
NONNULLABLE_BATCH_FAST_PATH(::arrow::Date32Type, Int32Type, int32_t)
NONNULLABLE_BATCH_FAST_PATH(::arrow::TimestampType, Int64Type, int64_t)
NONNULLABLE_BATCH_FAST_PATH(::arrow::Time32Type, Int32Type, int32_t)
NONNULLABLE_BATCH_FAST_PATH(::arrow::Time64Type, Int64Type, int64_t)

template <>
Status PrimitiveImpl::ReadNonNullableBatch<::arrow::TimestampType, Int96Type>(
    TypedColumnReader<Int96Type>* reader, int64_t values_to_read, int64_t* levels_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(Int96), false));
  auto values = reinterpret_cast<Int96*>(values_buffer_.mutable_data());
  int64_t values_read;
  PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(static_cast<int>(values_to_read),
                           nullptr, nullptr, values, &values_read));

  int64_t* out_ptr = reinterpret_cast<int64_t*>(data_buffer_ptr_) + valid_bits_idx_;
  for (int64_t i = 0; i < values_read; i++) {
    *out_ptr++ = impala_timestamp_to_nanoseconds(values[i]);
  }
  valid_bits_idx_ += values_read;

  return Status::OK();
}

template <>
Status PrimitiveImpl::ReadNonNullableBatch<::arrow::Date64Type, Int32Type>(
    TypedColumnReader<Int32Type>* reader, int64_t values_to_read, int64_t* levels_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(int32_t), false));
  auto values = reinterpret_cast<int32_t*>(values_buffer_.mutable_data());
  int64_t values_read;
  PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(static_cast<int>(values_to_read),
                           nullptr, nullptr, values, &values_read));

  int64_t* out_ptr = reinterpret_cast<int64_t*>(data_buffer_ptr_) + valid_bits_idx_;
  for (int64_t i = 0; i < values_read; i++) {
    *out_ptr++ = static_cast<int64_t>(values[i]) * 86400000;
  }
  valid_bits_idx_ += values_read;

  return Status::OK();
}

template <>
Status PrimitiveImpl::ReadNonNullableBatch<::arrow::BooleanType, BooleanType>(
    TypedColumnReader<BooleanType>* reader, int64_t values_to_read,
    int64_t* levels_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(bool), false));
  auto values = reinterpret_cast<bool*>(values_buffer_.mutable_data());
  int64_t values_read;
  PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(static_cast<int>(values_to_read),
                           nullptr, nullptr, values, &values_read));

  for (int64_t i = 0; i < values_read; i++) {
    if (values[i]) { ::arrow::BitUtil::SetBit(data_buffer_ptr_, valid_bits_idx_); }
    valid_bits_idx_++;
  }

  return Status::OK();
}

template <typename ArrowType, typename ParquetType>
Status PrimitiveImpl::ReadNullableBatch(TypedColumnReader<ParquetType>* reader,
    int16_t* def_levels, int16_t* rep_levels, int64_t values_to_read,
    int64_t* levels_read, int64_t* values_read) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;

  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(ParquetCType), false));
  auto values = reinterpret_cast<ParquetCType*>(values_buffer_.mutable_data());
  int64_t null_count;
  PARQUET_CATCH_NOT_OK(reader->ReadBatchSpaced(static_cast<int>(values_to_read),
      def_levels, rep_levels, values, valid_bits_ptr_, valid_bits_idx_, levels_read,
      values_read, &null_count));

  auto data_ptr = reinterpret_cast<ArrowCType*>(data_buffer_ptr_);
  INIT_BITSET(valid_bits_ptr_, static_cast<int>(valid_bits_idx_));

  for (int64_t i = 0; i < *values_read; i++) {
    if (bitset_valid_bits_ptr_ & (1 << bit_offset_valid_bits_ptr_)) {
      data_ptr[valid_bits_idx_ + i] = values[i];
    }
    READ_NEXT_BITSET(valid_bits_ptr_);
  }
  null_count_ += null_count;
  valid_bits_idx_ += *values_read;

  return Status::OK();
}

#define NULLABLE_BATCH_FAST_PATH(ArrowType, ParquetType, CType)                    \
  template <>                                                                      \
  Status PrimitiveImpl::ReadNullableBatch<ArrowType, ParquetType>(                 \
      TypedColumnReader<ParquetType> * reader, int16_t * def_levels,               \
      int16_t * rep_levels, int64_t values_to_read, int64_t * levels_read,         \
      int64_t * values_read) {                                                     \
    auto data_ptr = reinterpret_cast<CType*>(data_buffer_ptr_);                    \
    int64_t null_count;                                                            \
    PARQUET_CATCH_NOT_OK(reader->ReadBatchSpaced(static_cast<int>(values_to_read), \
        def_levels, rep_levels, data_ptr + valid_bits_idx_, valid_bits_ptr_,       \
        valid_bits_idx_, levels_read, values_read, &null_count));                  \
                                                                                   \
    valid_bits_idx_ += *values_read;                                               \
    null_count_ += null_count;                                                     \
                                                                                   \
    return Status::OK();                                                           \
  }

NULLABLE_BATCH_FAST_PATH(::arrow::Int32Type, Int32Type, int32_t)
NULLABLE_BATCH_FAST_PATH(::arrow::Int64Type, Int64Type, int64_t)
NULLABLE_BATCH_FAST_PATH(::arrow::FloatType, FloatType, float)
NULLABLE_BATCH_FAST_PATH(::arrow::DoubleType, DoubleType, double)
NULLABLE_BATCH_FAST_PATH(::arrow::Date32Type, Int32Type, int32_t)
NULLABLE_BATCH_FAST_PATH(::arrow::TimestampType, Int64Type, int64_t)
NULLABLE_BATCH_FAST_PATH(::arrow::Time32Type, Int32Type, int32_t)
NULLABLE_BATCH_FAST_PATH(::arrow::Time64Type, Int64Type, int64_t)

template <>
Status PrimitiveImpl::ReadNullableBatch<::arrow::TimestampType, Int96Type>(
    TypedColumnReader<Int96Type>* reader, int16_t* def_levels, int16_t* rep_levels,
    int64_t values_to_read, int64_t* levels_read, int64_t* values_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(Int96), false));
  auto values = reinterpret_cast<Int96*>(values_buffer_.mutable_data());
  int64_t null_count;
  PARQUET_CATCH_NOT_OK(reader->ReadBatchSpaced(static_cast<int>(values_to_read),
      def_levels, rep_levels, values, valid_bits_ptr_, valid_bits_idx_, levels_read,
      values_read, &null_count));

  auto data_ptr = reinterpret_cast<int64_t*>(data_buffer_ptr_);
  INIT_BITSET(valid_bits_ptr_, static_cast<int>(valid_bits_idx_));
  for (int64_t i = 0; i < *values_read; i++) {
    if (bitset_valid_bits_ptr_ & (1 << bit_offset_valid_bits_ptr_)) {
      data_ptr[valid_bits_idx_ + i] = impala_timestamp_to_nanoseconds(values[i]);
    }
    READ_NEXT_BITSET(valid_bits_ptr_);
  }
  null_count_ += null_count;
  valid_bits_idx_ += *values_read;

  return Status::OK();
}

template <>
Status PrimitiveImpl::ReadNullableBatch<::arrow::Date64Type, Int32Type>(
    TypedColumnReader<Int32Type>* reader, int16_t* def_levels, int16_t* rep_levels,
    int64_t values_to_read, int64_t* levels_read, int64_t* values_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(int32_t), false));
  auto values = reinterpret_cast<int32_t*>(values_buffer_.mutable_data());
  int64_t null_count;
  PARQUET_CATCH_NOT_OK(reader->ReadBatchSpaced(static_cast<int>(values_to_read),
      def_levels, rep_levels, values, valid_bits_ptr_, valid_bits_idx_, levels_read,
      values_read, &null_count));

  auto data_ptr = reinterpret_cast<int64_t*>(data_buffer_ptr_);
  INIT_BITSET(valid_bits_ptr_, static_cast<int>(valid_bits_idx_));
  for (int64_t i = 0; i < *values_read; i++) {
    if (bitset_valid_bits_ptr_ & (1 << bit_offset_valid_bits_ptr_)) {
      data_ptr[valid_bits_idx_ + i] = static_cast<int64_t>(values[i]) * 86400000;
    }
    READ_NEXT_BITSET(valid_bits_ptr_);
  }
  null_count_ += null_count;
  valid_bits_idx_ += *values_read;

  return Status::OK();
}

template <>
Status PrimitiveImpl::ReadNullableBatch<::arrow::BooleanType, BooleanType>(
    TypedColumnReader<BooleanType>* reader, int16_t* def_levels, int16_t* rep_levels,
    int64_t values_to_read, int64_t* levels_read, int64_t* values_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(bool), false));
  auto values = reinterpret_cast<bool*>(values_buffer_.mutable_data());
  int64_t null_count;
  PARQUET_CATCH_NOT_OK(reader->ReadBatchSpaced(static_cast<int>(values_to_read),
      def_levels, rep_levels, values, valid_bits_ptr_, valid_bits_idx_, levels_read,
      values_read, &null_count));

  INIT_BITSET(valid_bits_ptr_, static_cast<int>(valid_bits_idx_));
  for (int64_t i = 0; i < *values_read; i++) {
    if (bitset_valid_bits_ptr_ & (1 << bit_offset_valid_bits_ptr_)) {
      if (values[i]) { ::arrow::BitUtil::SetBit(data_buffer_ptr_, valid_bits_idx_ + i); }
    }
    READ_NEXT_BITSET(valid_bits_ptr_);
  }
  valid_bits_idx_ += *values_read;
  null_count_ += null_count;

  return Status::OK();
}

template <typename ArrowType>
Status PrimitiveImpl::InitDataBuffer(int batch_size) {
  using ArrowCType = typename ArrowType::c_type;
  data_buffer_ = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(data_buffer_->Resize(batch_size * sizeof(ArrowCType), false));
  data_buffer_ptr_ = data_buffer_->mutable_data();

  return Status::OK();
}

template <>
Status PrimitiveImpl::InitDataBuffer<::arrow::BooleanType>(int batch_size) {
  data_buffer_ = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(data_buffer_->Resize(::arrow::BitUtil::CeilByte(batch_size) / 8, false));
  data_buffer_ptr_ = data_buffer_->mutable_data();
  memset(data_buffer_ptr_, 0, data_buffer_->size());

  return Status::OK();
}

Status PrimitiveImpl::InitValidBits(int batch_size) {
  valid_bits_idx_ = 0;
  if (descr_->max_definition_level() > 0) {
    int valid_bits_size =
        static_cast<int>(::arrow::BitUtil::CeilByte(batch_size + 1)) / 8;
    valid_bits_buffer_ = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(valid_bits_buffer_->Resize(valid_bits_size, false));
    valid_bits_ptr_ = valid_bits_buffer_->mutable_data();
    memset(valid_bits_ptr_, 0, valid_bits_size);
    null_count_ = 0;
  }
  return Status::OK();
}

Status PrimitiveImpl::WrapIntoListArray(const int16_t* def_levels,
    const int16_t* rep_levels, int64_t total_levels_read, std::shared_ptr<Array>* array) {
  std::shared_ptr<::arrow::Schema> arrow_schema;
  RETURN_NOT_OK(FromParquetSchema(input_->schema(), {input_->column_index()},
      input_->metadata()->key_value_metadata(), &arrow_schema));
  std::shared_ptr<Field> current_field = arrow_schema->field(0);

  if (descr_->max_repetition_level() > 0) {
    // Walk downwards to extract nullability
    std::vector<bool> nullable;
    std::vector<std::shared_ptr<::arrow::Int32Builder>> offset_builders;
    std::vector<std::shared_ptr<::arrow::BooleanBuilder>> valid_bits_builders;
    nullable.push_back(current_field->nullable());
    while (current_field->type()->num_children() > 0) {
      if (current_field->type()->num_children() > 1) {
        return Status::NotImplemented(
            "Fields with more than one child are not supported.");
      } else {
        if (current_field->type()->id() != ::arrow::Type::LIST) {
          return Status::NotImplemented(
              "Currently only nesting with Lists is supported.");
        }
        current_field = current_field->type()->child(0);
      }
      offset_builders.emplace_back(
          std::make_shared<::arrow::Int32Builder>(pool_, ::arrow::int32()));
      valid_bits_builders.emplace_back(
          std::make_shared<::arrow::BooleanBuilder>(pool_, ::arrow::boolean()));
      nullable.push_back(current_field->nullable());
    }

    int64_t list_depth = offset_builders.size();
    // This describes the minimal definition that describes a level that
    // reflects a value in the primitive values array.
    int16_t values_def_level = descr_->max_definition_level();
    if (nullable[nullable.size() - 1]) { values_def_level--; }

    // The definition levels that are needed so that a list is declared
    // as empty and not null.
    std::vector<int16_t> empty_def_level(list_depth);
    int def_level = 0;
    for (int i = 0; i < list_depth; i++) {
      if (nullable[i]) { def_level++; }
      empty_def_level[i] = def_level;
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
            if (empty_def_level[j] == def_levels[i]) { break; }
          }
        }
      }
      if (def_levels[i] >= values_def_level) { values_offset++; }
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
      offsets.emplace_back(std::static_pointer_cast<Int32Array>(array)->data());
      RETURN_NOT_OK(valid_bits_builders[j]->Finish(&array));
      valid_bits.emplace_back(std::static_pointer_cast<BooleanArray>(array)->data());
    }

    std::shared_ptr<Array> output(*array);
    for (int64_t j = list_depth - 1; j >= 0; j--) {
      auto list_type = std::make_shared<::arrow::ListType>(
          std::make_shared<Field>("item", output->type(), nullable[j + 1]));
      output = std::make_shared<::arrow::ListArray>(
          list_type, list_lengths[j], offsets[j], output, valid_bits[j], null_counts[j]);
    }
    *array = output;
  }
  return Status::OK();
}

template <typename ArrowType, typename ParquetType>
Status PrimitiveImpl::TypedReadBatch(int batch_size, std::shared_ptr<Array>* out) {
  using ArrowCType = typename ArrowType::c_type;

  int values_to_read = batch_size;
  int total_levels_read = 0;
  RETURN_NOT_OK(InitDataBuffer<ArrowType>(batch_size));
  RETURN_NOT_OK(InitValidBits(batch_size));
  if (descr_->max_definition_level() > 0) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(batch_size * sizeof(int16_t), false));
  }
  if (descr_->max_repetition_level() > 0) {
    RETURN_NOT_OK(rep_levels_buffer_.Resize(batch_size * sizeof(int16_t), false));
  }
  int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
  int16_t* rep_levels = reinterpret_cast<int16_t*>(rep_levels_buffer_.mutable_data());

  while ((values_to_read > 0) && column_reader_) {
    auto reader = dynamic_cast<TypedColumnReader<ParquetType>*>(column_reader_.get());
    int64_t values_read;
    int64_t levels_read;
    if (descr_->max_definition_level() == 0) {
      RETURN_NOT_OK((ReadNonNullableBatch<ArrowType, ParquetType>(
          reader, values_to_read, &values_read)));
    } else {
      // As per the defintion and checks for flat (list) columns:
      // descr_->max_definition_level() > 0, <= 3
      RETURN_NOT_OK((ReadNullableBatch<ArrowType, ParquetType>(reader,
          def_levels + total_levels_read, rep_levels + total_levels_read, values_to_read,
          &levels_read, &values_read)));
      total_levels_read += static_cast<int>(levels_read);
    }
    values_to_read -= static_cast<int>(values_read);
    if (!column_reader_->HasNext()) { NextRowGroup(); }
  }

  // Shrink arrays as they may be larger than the output.
  RETURN_NOT_OK(data_buffer_->Resize(valid_bits_idx_ * sizeof(ArrowCType)));
  if (descr_->max_definition_level() > 0) {
    if (valid_bits_idx_ < batch_size * 0.8) {
      RETURN_NOT_OK(valid_bits_buffer_->Resize(
          ::arrow::BitUtil::CeilByte(valid_bits_idx_) / 8, false));
    }
    *out = std::make_shared<ArrayType<ArrowType>>(
        field_->type(), valid_bits_idx_, data_buffer_, valid_bits_buffer_, null_count_);
    // Relase the ownership as the Buffer is now part of a new Array
    valid_bits_buffer_.reset();
  } else {
    *out = std::make_shared<ArrayType<ArrowType>>(
        field_->type(), valid_bits_idx_, data_buffer_);
  }
  // Relase the ownership as the Buffer is now part of a new Array
  data_buffer_.reset();

  // Check if we should transform this array into an list array.
  return WrapIntoListArray(def_levels, rep_levels, total_levels_read, out);
}

template <>
Status PrimitiveImpl::TypedReadBatch<::arrow::BooleanType, BooleanType>(
    int batch_size, std::shared_ptr<Array>* out) {
  int values_to_read = batch_size;
  int total_levels_read = 0;
  RETURN_NOT_OK(InitDataBuffer<::arrow::BooleanType>(batch_size));
  RETURN_NOT_OK(InitValidBits(batch_size));
  if (descr_->max_definition_level() > 0) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(batch_size * sizeof(int16_t), false));
  }
  if (descr_->max_repetition_level() > 0) {
    RETURN_NOT_OK(rep_levels_buffer_.Resize(batch_size * sizeof(int16_t), false));
  }
  int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
  int16_t* rep_levels = reinterpret_cast<int16_t*>(rep_levels_buffer_.mutable_data());

  while ((values_to_read > 0) && column_reader_) {
    auto reader = dynamic_cast<TypedColumnReader<BooleanType>*>(column_reader_.get());
    int64_t values_read;
    int64_t levels_read;
    int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    if (descr_->max_definition_level() == 0) {
      RETURN_NOT_OK((ReadNonNullableBatch<::arrow::BooleanType, BooleanType>(
          reader, values_to_read, &values_read)));
    } else {
      // As per the defintion and checks for flat columns:
      // descr_->max_definition_level() == 1
      RETURN_NOT_OK((ReadNullableBatch<::arrow::BooleanType, BooleanType>(reader,
          def_levels + total_levels_read, rep_levels + total_levels_read, values_to_read,
          &levels_read, &values_read)));
      total_levels_read += static_cast<int>(levels_read);
    }
    values_to_read -= static_cast<int>(values_read);
    if (!column_reader_->HasNext()) { NextRowGroup(); }
  }

  if (descr_->max_definition_level() > 0) {
    // TODO: Shrink arrays in the case they are too large
    if (valid_bits_idx_ < batch_size * 0.8) {
      // Shrink arrays as they are larger than the output.
      // TODO(PARQUET-761/ARROW-360): Use realloc internally to shrink the arrays
      //    without the need for a copy. Given a decent underlying allocator this
      //    should still free some underlying pages to the OS.

      auto data_buffer = std::make_shared<PoolBuffer>(pool_);
      RETURN_NOT_OK(data_buffer->Resize(valid_bits_idx_ * sizeof(bool)));
      memcpy(data_buffer->mutable_data(), data_buffer_->data(), data_buffer->size());
      data_buffer_ = data_buffer;

      auto valid_bits_buffer = std::make_shared<PoolBuffer>(pool_);
      RETURN_NOT_OK(
          valid_bits_buffer->Resize(::arrow::BitUtil::CeilByte(valid_bits_idx_) / 8));
      memcpy(valid_bits_buffer->mutable_data(), valid_bits_buffer_->data(),
          valid_bits_buffer->size());
      valid_bits_buffer_ = valid_bits_buffer;
    }
    *out = std::make_shared<BooleanArray>(
        field_->type(), valid_bits_idx_, data_buffer_, valid_bits_buffer_, null_count_);
    // Relase the ownership
    data_buffer_.reset();
    valid_bits_buffer_.reset();
  } else {
    *out = std::make_shared<BooleanArray>(field_->type(), valid_bits_idx_, data_buffer_);
    data_buffer_.reset();
  }

  // Check if we should transform this array into an list array.
  return WrapIntoListArray(def_levels, rep_levels, total_levels_read, out);
}

template <typename ArrowType>
Status PrimitiveImpl::ReadByteArrayBatch(int batch_size, std::shared_ptr<Array>* out) {
  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;

  int total_levels_read = 0;
  if (descr_->max_definition_level() > 0) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(batch_size * sizeof(int16_t), false));
  }
  if (descr_->max_repetition_level() > 0) {
    RETURN_NOT_OK(rep_levels_buffer_.Resize(batch_size * sizeof(int16_t), false));
  }
  int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
  int16_t* rep_levels = reinterpret_cast<int16_t*>(rep_levels_buffer_.mutable_data());

  int values_to_read = batch_size;
  BuilderType builder(pool_);
  while ((values_to_read > 0) && column_reader_) {
    RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(ByteArray), false));
    auto reader = dynamic_cast<TypedColumnReader<ByteArrayType>*>(column_reader_.get());
    int64_t values_read;
    int64_t levels_read;
    auto values = reinterpret_cast<ByteArray*>(values_buffer_.mutable_data());
    PARQUET_CATCH_NOT_OK(
        levels_read = reader->ReadBatch(values_to_read, def_levels + total_levels_read,
            rep_levels + total_levels_read, values, &values_read));
    values_to_read -= static_cast<int>(levels_read);
    if (descr_->max_definition_level() == 0) {
      for (int64_t i = 0; i < levels_read; i++) {
        RETURN_NOT_OK(
            builder.Append(reinterpret_cast<const char*>(values[i].ptr), values[i].len));
      }
    } else {
      // descr_->max_definition_level() > 0
      int values_idx = 0;
      int nullable_elements = descr_->schema_node()->is_optional();
      for (int64_t i = 0; i < levels_read; i++) {
        if (nullable_elements &&
            (def_levels[i + total_levels_read] == (descr_->max_definition_level() - 1))) {
          RETURN_NOT_OK(builder.AppendNull());
        } else if (def_levels[i + total_levels_read] == descr_->max_definition_level()) {
          RETURN_NOT_OK(
              builder.Append(reinterpret_cast<const char*>(values[values_idx].ptr),
                  values[values_idx].len));
          values_idx++;
        }
      }
      total_levels_read += static_cast<int>(levels_read);
    }
    if (!column_reader_->HasNext()) { NextRowGroup(); }
  }

  RETURN_NOT_OK(builder.Finish(out));
  // Check if we should transform this array into an list array.
  return WrapIntoListArray(def_levels, rep_levels, total_levels_read, out);
}

template <typename ArrowType>
Status PrimitiveImpl::ReadFLBABatch(
    int batch_size, int byte_width, std::shared_ptr<Array>* out) {
  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  int total_levels_read = 0;
  if (descr_->max_definition_level() > 0) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(batch_size * sizeof(int16_t), false));
  }
  if (descr_->max_repetition_level() > 0) {
    RETURN_NOT_OK(rep_levels_buffer_.Resize(batch_size * sizeof(int16_t), false));
  }
  int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
  int16_t* rep_levels = reinterpret_cast<int16_t*>(rep_levels_buffer_.mutable_data());

  int values_to_read = batch_size;
  BuilderType builder(pool_, ::arrow::fixed_size_binary(byte_width));
  while ((values_to_read > 0) && column_reader_) {
    RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(FLBA), false));
    auto reader = dynamic_cast<TypedColumnReader<FLBAType>*>(column_reader_.get());
    int64_t values_read;
    int64_t levels_read;
    auto values = reinterpret_cast<FLBA*>(values_buffer_.mutable_data());
    PARQUET_CATCH_NOT_OK(
        levels_read = reader->ReadBatch(values_to_read, def_levels + total_levels_read,
            rep_levels + total_levels_read, values, &values_read));
    values_to_read -= static_cast<int>(levels_read);
    if (descr_->max_definition_level() == 0) {
      for (int64_t i = 0; i < levels_read; i++) {
        RETURN_NOT_OK(builder.Append(values[i].ptr));
      }
    } else {
      int values_idx = 0;
      int nullable_elements = descr_->schema_node()->is_optional();
      for (int64_t i = 0; i < levels_read; i++) {
        if (nullable_elements &&
            (def_levels[i + total_levels_read] == (descr_->max_definition_level() - 1))) {
          RETURN_NOT_OK(builder.AppendNull());
        } else if (def_levels[i + total_levels_read] == descr_->max_definition_level()) {
          RETURN_NOT_OK(builder.Append(values[values_idx].ptr));
          values_idx++;
        }
      }
      total_levels_read += static_cast<int>(levels_read);
    }
    if (!column_reader_->HasNext()) { NextRowGroup(); }
  }

  RETURN_NOT_OK(builder.Finish(out));
  // Check if we should transform this array into an list array.
  return WrapIntoListArray(def_levels, rep_levels, total_levels_read, out);
}

template <>
Status PrimitiveImpl::TypedReadBatch<::arrow::BinaryType, ByteArrayType>(
    int batch_size, std::shared_ptr<Array>* out) {
  return ReadByteArrayBatch<::arrow::BinaryType>(batch_size, out);
}

template <>
Status PrimitiveImpl::TypedReadBatch<::arrow::StringType, ByteArrayType>(
    int batch_size, std::shared_ptr<Array>* out) {
  return ReadByteArrayBatch<::arrow::StringType>(batch_size, out);
}

#define TYPED_BATCH_CASE(ENUM, ArrowType, ParquetType)              \
  case ::arrow::Type::ENUM:                                         \
    return TypedReadBatch<ArrowType, ParquetType>(batch_size, out); \
    break;

Status PrimitiveImpl::NextBatch(int batch_size, std::shared_ptr<Array>* out) {
  if (!column_reader_) {
    // Exhausted all row groups.
    *out = nullptr;
    return Status::OK();
  }

  switch (field_->type()->id()) {
    case ::arrow::Type::NA:
      *out = std::make_shared<::arrow::NullArray>(batch_size);
      return Status::OK();
      break;
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
      TYPED_BATCH_CASE(BINARY, ::arrow::BinaryType, ByteArrayType)
      TYPED_BATCH_CASE(DATE32, ::arrow::Date32Type, Int32Type)
      TYPED_BATCH_CASE(DATE64, ::arrow::Date64Type, Int32Type)
    case ::arrow::Type::FIXED_SIZE_BINARY: {
      int32_t byte_width =
          static_cast<::arrow::FixedSizeBinaryType*>(field_->type().get())->byte_width();
      return ReadFLBABatch<::arrow::FixedSizeBinaryType>(batch_size, byte_width, out);
      break;
    }
    case ::arrow::Type::TIMESTAMP: {
      ::arrow::TimestampType* timestamp_type =
          static_cast<::arrow::TimestampType*>(field_->type().get());
      switch (timestamp_type->unit()) {
        case ::arrow::TimeUnit::MILLI:
          return TypedReadBatch<::arrow::TimestampType, Int64Type>(batch_size, out);
          break;
        case ::arrow::TimeUnit::MICRO:
          return TypedReadBatch<::arrow::TimestampType, Int64Type>(batch_size, out);
          break;
        case ::arrow::TimeUnit::NANO:
          return TypedReadBatch<::arrow::TimestampType, Int96Type>(batch_size, out);
          break;
        default:
          return Status::NotImplemented("TimeUnit not supported");
      }
      break;
    }
      TYPED_BATCH_CASE(TIME32, ::arrow::Time32Type, Int32Type)
      TYPED_BATCH_CASE(TIME64, ::arrow::Time64Type, Int64Type)
    default:
      std::stringstream ss;
      ss << "No support for reading columns of type " << field_->type()->ToString();
      return Status::NotImplemented(ss.str());
  }
}

void PrimitiveImpl::NextRowGroup() {
  column_reader_ = input_->Next();
}

Status PrimitiveImpl::GetDefLevels(ValueLevelsPtr* data, size_t* length) {
  *data = reinterpret_cast<ValueLevelsPtr>(def_levels_buffer_.data());
  *length = def_levels_buffer_.size() / sizeof(int16_t);
  return Status::OK();
}

Status PrimitiveImpl::GetRepLevels(ValueLevelsPtr* data, size_t* length) {
  *data = reinterpret_cast<ValueLevelsPtr>(rep_levels_buffer_.data());
  *length = rep_levels_buffer_.size() / sizeof(int16_t);
  return Status::OK();
}

ColumnReader::ColumnReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

ColumnReader::~ColumnReader() {}

Status ColumnReader::NextBatch(int batch_size, std::shared_ptr<Array>* out) {
  return impl_->NextBatch(batch_size, out);
}

// StructImpl methods

Status StructImpl::DefLevelsToNullArray(
    std::shared_ptr<MutableBuffer>* null_bitmap_out, int64_t* null_count_out) {
  std::shared_ptr<MutableBuffer> null_bitmap;
  auto null_count = 0;
  ValueLevelsPtr def_levels_data;
  size_t def_levels_length;
  RETURN_NOT_OK(GetDefLevels(&def_levels_data, &def_levels_length));
  RETURN_NOT_OK(GetEmptyBitmap(pool_, def_levels_length, &null_bitmap));
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
Status StructImpl::GetDefLevels(ValueLevelsPtr* data, size_t* length) {
  *data = nullptr;
  if (children_.size() == 0) {
    // Empty struct
    *length = 0;
    return Status::OK();
  }

  // We have at least one child
  ValueLevelsPtr child_def_levels;
  size_t child_length;
  RETURN_NOT_OK(children_[0]->GetDefLevels(&child_def_levels, &child_length));
  auto size = child_length * sizeof(int16_t);
  def_levels_buffer_.Resize(size);
  // Initialize with the minimal def level
  std::memset(def_levels_buffer_.mutable_data(), -1, size);
  auto result_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());

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
  *data = reinterpret_cast<ValueLevelsPtr>(def_levels_buffer_.data());
  *length = child_length;
  return Status::OK();
}

void StructImpl::InitField(
    const NodePtr& node, const std::vector<std::shared_ptr<Impl>>& children) {
  // Make a shallow node to field conversion from the children fields
  std::vector<std::shared_ptr<::arrow::Field>> fields(children.size());
  for (size_t i = 0; i < children.size(); i++) {
    fields[i] = children[i]->field();
  }
  auto type = std::make_shared<::arrow::StructType>(fields);
  field_ = std::make_shared<Field>(node->name(), type);
}

Status StructImpl::GetRepLevels(ValueLevelsPtr* data, size_t* length) {
  return Status::NotImplemented("GetRepLevels is not implemented for struct");
}

Status StructImpl::NextBatch(int batch_size, std::shared_ptr<Array>* out) {
  std::vector<std::shared_ptr<Array>> children_arrays;
  std::shared_ptr<MutableBuffer> null_bitmap;
  int64_t null_count;

  // Gather children arrays and def levels
  for (auto& child : children_) {
    std::shared_ptr<Array> child_array;

    RETURN_NOT_OK(child->NextBatch(batch_size, &child_array));

    children_arrays.push_back(child_array);
  }

  RETURN_NOT_OK(DefLevelsToNullArray(&null_bitmap, &null_count));

  *out = std::make_shared<StructArray>(
      field()->type(), batch_size, children_arrays, null_bitmap, null_count);

  return Status::OK();
}

}  // namespace arrow
}  // namespace parquet

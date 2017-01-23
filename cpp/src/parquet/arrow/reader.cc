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

#include "parquet/arrow/schema.h"
#include "parquet/util/bit-util.h"

#include "arrow/api.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"

using arrow::Array;
using arrow::Column;
using arrow::Field;
using arrow::MemoryPool;
using arrow::PoolBuffer;
using arrow::Status;
using arrow::Table;

// Help reduce verbosity
using ParquetReader = parquet::ParquetFileReader;

namespace parquet {
namespace arrow {

constexpr int64_t kJulianToUnixEpochDays = 2440588L;
constexpr int64_t kNanosecondsInADay = 86400L * 1000L * 1000L * 1000L;

static inline int64_t impala_timestamp_to_nanoseconds(const Int96& impala_timestamp) {
  int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;
  int64_t nanoseconds = *(reinterpret_cast<const int64_t*>(&(impala_timestamp.value)));
  return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

template <typename ArrowType>
using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

class FileReader::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader);
  virtual ~Impl() {}

  bool CheckForFlatColumn(const ColumnDescriptor* descr);
  Status GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out);
  Status ReadFlatColumn(int i, std::shared_ptr<Array>* out);
  Status ReadFlatTable(std::shared_ptr<Table>* out);
  Status ReadFlatTable(
      const std::vector<int>& column_indices, std::shared_ptr<Table>* out);
  const ParquetFileReader* parquet_reader() const { return reader_.get(); }

  void set_num_threads(int num_threads) { num_threads_ = num_threads; }

 private:
  MemoryPool* pool_;
  std::unique_ptr<ParquetFileReader> reader_;

  int num_threads_;
};

class FlatColumnReader::Impl {
 public:
  Impl(MemoryPool* pool, const ColumnDescriptor* descr, ParquetFileReader* reader,
      int column_index);
  virtual ~Impl() {}

  Status NextBatch(int batch_size, std::shared_ptr<Array>* out);

  template <typename ArrowType, typename ParquetType>
  Status TypedReadBatch(int batch_size, std::shared_ptr<Array>* out);

  template <typename ArrowType>
  Status ReadByteArrayBatch(int batch_size, std::shared_ptr<Array>* out);

  template <typename ArrowType>
  Status InitDataBuffer(int batch_size);
  template <typename ArrowType, typename ParquetType>
  Status ReadNullableFlatBatch(TypedColumnReader<ParquetType>* reader,
      int16_t* def_levels, int64_t values_to_read, int64_t* levels_read);
  template <typename ArrowType, typename ParquetType>
  Status ReadNonNullableBatch(TypedColumnReader<ParquetType>* reader,
      int64_t values_to_read, int64_t* levels_read);

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
  const ColumnDescriptor* descr_;
  ParquetFileReader* reader_;
  int column_index_;
  int next_row_group_;
  std::shared_ptr<ColumnReader> column_reader_;
  std::shared_ptr<Field> field_;

  PoolBuffer values_buffer_;
  PoolBuffer def_levels_buffer_;
  std::shared_ptr<PoolBuffer> data_buffer_;
  uint8_t* data_buffer_ptr_;
  std::shared_ptr<PoolBuffer> valid_bits_buffer_;
  uint8_t* valid_bits_ptr_;
  int64_t valid_bits_idx_;
  int64_t null_count_;
};

FileReader::Impl::Impl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader)
    : pool_(pool), reader_(std::move(reader)), num_threads_(1) {}

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
  std::vector<int> column_indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < column_indices.size(); ++i) {
    column_indices[i] = i;
  }
  return ReadFlatTable(column_indices, table);
}

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

Status FileReader::Impl::ReadFlatTable(
    const std::vector<int>& indices, std::shared_ptr<Table>* table) {
  auto descr = reader_->metadata()->schema();

  const std::string& name = descr->name();
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(FromParquetSchema(descr, indices, &schema));

  int num_columns = static_cast<int>(indices.size());
  int nthreads = std::min<int>(num_threads_, num_columns);
  std::vector<std::shared_ptr<Column>> columns(num_columns);

  auto ReadColumn = [&indices, &schema, &columns, this](int i) {
    std::shared_ptr<Array> array;
    RETURN_NOT_OK(ReadFlatColumn(indices[i], &array));
    columns[i] = std::make_shared<Column>(schema->field(indices[i]), array);
    return Status::OK();
  };

  if (nthreads == 1) {
    for (int i = 0; i < num_columns; i++) {
      RETURN_NOT_OK(ReadColumn(i));
    }
  } else {
    RETURN_NOT_OK(ParallelFor(nthreads, num_columns, ReadColumn));
  }

  *table = std::make_shared<Table>(name, schema, columns);
  return Status::OK();
}

FileReader::FileReader(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader)
    : impl_(new FileReader::Impl(pool, std::move(reader))) {}

FileReader::~FileReader() {}

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

Status FileReader::GetFlatColumn(int i, std::unique_ptr<FlatColumnReader>* out) {
  return impl_->GetFlatColumn(i, out);
}

Status FileReader::ReadFlatColumn(int i, std::shared_ptr<Array>* out) {
  try {
    return impl_->ReadFlatColumn(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadFlatTable(std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadFlatTable(out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadFlatTable(
    const std::vector<int>& column_indices, std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadFlatTable(column_indices, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

void FileReader::set_num_threads(int num_threads) {
  impl_->set_num_threads(num_threads);
}

const ParquetFileReader* FileReader::parquet_reader() const {
  return impl_->parquet_reader();
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
Status FlatColumnReader::Impl::ReadNonNullableBatch(
    TypedColumnReader<ParquetType>* reader, int64_t values_to_read,
    int64_t* levels_read) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;

  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(ParquetCType), false));
  auto values = reinterpret_cast<ParquetCType*>(values_buffer_.mutable_data());
  int64_t values_read;
  PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(
                           values_to_read, nullptr, nullptr, values, &values_read));

  ArrowCType* out_ptr = reinterpret_cast<ArrowCType*>(data_buffer_ptr_);
  std::copy(values, values + values_read, out_ptr + valid_bits_idx_);
  valid_bits_idx_ += values_read;

  return Status::OK();
}

#define NONNULLABLE_BATCH_FAST_PATH(ArrowType, ParquetType, CType)                 \
  template <>                                                                      \
  Status FlatColumnReader::Impl::ReadNonNullableBatch<ArrowType, ParquetType>(     \
      TypedColumnReader<ParquetType> * reader, int64_t values_to_read,             \
      int64_t * levels_read) {                                                     \
    int64_t values_read;                                                           \
    CType* out_ptr = reinterpret_cast<CType*>(data_buffer_ptr_);                   \
    PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(values_to_read, nullptr, \
                             nullptr, out_ptr + valid_bits_idx_, &values_read));   \
                                                                                   \
    valid_bits_idx_ += values_read;                                                \
                                                                                   \
    return Status::OK();                                                           \
  }

NONNULLABLE_BATCH_FAST_PATH(::arrow::Int32Type, Int32Type, int32_t)
NONNULLABLE_BATCH_FAST_PATH(::arrow::Int64Type, Int64Type, int64_t)
NONNULLABLE_BATCH_FAST_PATH(::arrow::FloatType, FloatType, float)
NONNULLABLE_BATCH_FAST_PATH(::arrow::DoubleType, DoubleType, double)

template <>
Status FlatColumnReader::Impl::ReadNonNullableBatch<::arrow::TimestampType, Int96Type>(
    TypedColumnReader<Int96Type>* reader, int64_t values_to_read, int64_t* levels_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(Int96Type), false));
  auto values = reinterpret_cast<Int96*>(values_buffer_.mutable_data());
  int64_t values_read;
  PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(
                           values_to_read, nullptr, nullptr, values, &values_read));

  int64_t* out_ptr = reinterpret_cast<int64_t*>(data_buffer_ptr_);
  for (int64_t i = 0; i < values_read; i++) {
    *out_ptr++ = impala_timestamp_to_nanoseconds(values[i]);
  }
  valid_bits_idx_ += values_read;

  return Status::OK();
}

template <>
Status FlatColumnReader::Impl::ReadNonNullableBatch<::arrow::BooleanType, BooleanType>(
    TypedColumnReader<BooleanType>* reader, int64_t values_to_read,
    int64_t* levels_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(bool), false));
  auto values = reinterpret_cast<bool*>(values_buffer_.mutable_data());
  int64_t values_read;
  PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatch(
                           values_to_read, nullptr, nullptr, values, &values_read));

  for (int64_t i = 0; i < values_read; i++) {
    if (values[i]) { ::arrow::BitUtil::SetBit(data_buffer_ptr_, valid_bits_idx_); }
    valid_bits_idx_++;
  }

  return Status::OK();
}

template <typename ArrowType, typename ParquetType>
Status FlatColumnReader::Impl::ReadNullableFlatBatch(
    TypedColumnReader<ParquetType>* reader, int16_t* def_levels, int64_t values_to_read,
    int64_t* levels_read) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;

  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(ParquetCType), false));
  auto values = reinterpret_cast<ParquetCType*>(values_buffer_.mutable_data());
  int null_count;
  PARQUET_CATCH_NOT_OK(*levels_read =
                           reader->ReadBatchSpaced(values_to_read, def_levels, nullptr,
                               values, &null_count, valid_bits_ptr_, valid_bits_idx_));

  auto data_ptr = reinterpret_cast<ArrowCType*>(data_buffer_ptr_);
  INIT_BITSET(valid_bits_ptr_, valid_bits_idx_);

  for (int64_t i = 0; i < *levels_read; i++) {
    if (bitset & (1 << bit_offset)) { data_ptr[valid_bits_idx_ + i] = values[i]; }
    READ_NEXT_BITSET(valid_bits_ptr_);
  }
  null_count_ += null_count;
  valid_bits_idx_ += *levels_read;

  return Status::OK();
}

#define NULLABLE_BATCH_FAST_PATH(ArrowType, ParquetType, CType)                 \
  template <>                                                                   \
  Status FlatColumnReader::Impl::ReadNullableFlatBatch<ArrowType, ParquetType>( \
      TypedColumnReader<ParquetType> * reader, int16_t * def_levels,            \
      int64_t values_to_read, int64_t * levels_read) {                          \
    auto data_ptr = reinterpret_cast<CType*>(data_buffer_ptr_);                 \
    int null_count;                                                             \
    PARQUET_CATCH_NOT_OK(*levels_read = reader->ReadBatchSpaced(values_to_read, \
                             def_levels, nullptr, data_ptr + valid_bits_idx_,   \
                             &null_count, valid_bits_ptr_, valid_bits_idx_));   \
                                                                                \
    valid_bits_idx_ += *levels_read;                                            \
    null_count_ += null_count;                                                  \
                                                                                \
    return Status::OK();                                                        \
  }

NULLABLE_BATCH_FAST_PATH(::arrow::Int32Type, Int32Type, int32_t)
NULLABLE_BATCH_FAST_PATH(::arrow::Int64Type, Int64Type, int64_t)
NULLABLE_BATCH_FAST_PATH(::arrow::FloatType, FloatType, float)
NULLABLE_BATCH_FAST_PATH(::arrow::DoubleType, DoubleType, double)

template <>
Status FlatColumnReader::Impl::ReadNullableFlatBatch<::arrow::TimestampType, Int96Type>(
    TypedColumnReader<Int96Type>* reader, int16_t* def_levels, int64_t values_to_read,
    int64_t* levels_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(Int96Type), false));
  auto values = reinterpret_cast<Int96*>(values_buffer_.mutable_data());
  int null_count;
  PARQUET_CATCH_NOT_OK(*levels_read =
                           reader->ReadBatchSpaced(values_to_read, def_levels, nullptr,
                               values, &null_count, valid_bits_ptr_, valid_bits_idx_));

  auto data_ptr = reinterpret_cast<int64_t*>(data_buffer_ptr_);
  INIT_BITSET(valid_bits_ptr_, valid_bits_idx_);
  for (int64_t i = 0; i < *levels_read; i++) {
    if (bitset & (1 << bit_offset)) {
      data_ptr[valid_bits_idx_ + i] = impala_timestamp_to_nanoseconds(values[i]);
    }
    READ_NEXT_BITSET(valid_bits_ptr_);
  }
  null_count_ += null_count;
  valid_bits_idx_ += *levels_read;

  return Status::OK();
}

template <>
Status FlatColumnReader::Impl::ReadNullableFlatBatch<::arrow::BooleanType, BooleanType>(
    TypedColumnReader<BooleanType>* reader, int16_t* def_levels, int64_t values_to_read,
    int64_t* levels_read) {
  RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(bool), false));
  auto values = reinterpret_cast<bool*>(values_buffer_.mutable_data());
  int null_count;
  PARQUET_CATCH_NOT_OK(*levels_read =
                           reader->ReadBatchSpaced(values_to_read, def_levels, nullptr,
                               values, &null_count, valid_bits_ptr_, valid_bits_idx_));

  INIT_BITSET(valid_bits_ptr_, valid_bits_idx_);
  for (int64_t i = 0; i < *levels_read; i++) {
    if (bitset & (1 << bit_offset)) {
      if (values[i]) { ::arrow::BitUtil::SetBit(data_buffer_ptr_, valid_bits_idx_ + i); }
    }
    READ_NEXT_BITSET(valid_bits_ptr_);
  }
  valid_bits_idx_ += *levels_read;
  null_count_ += null_count;

  return Status::OK();
}

template <typename ArrowType>
Status FlatColumnReader::Impl::InitDataBuffer(int batch_size) {
  using ArrowCType = typename ArrowType::c_type;
  data_buffer_ = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(data_buffer_->Resize(batch_size * sizeof(ArrowCType), false));
  data_buffer_ptr_ = data_buffer_->mutable_data();

  return Status::OK();
}

template <>
Status FlatColumnReader::Impl::InitDataBuffer<::arrow::BooleanType>(int batch_size) {
  data_buffer_ = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(data_buffer_->Resize(::arrow::BitUtil::CeilByte(batch_size) / 8, false));
  data_buffer_ptr_ = data_buffer_->mutable_data();
  memset(data_buffer_ptr_, 0, data_buffer_->size());

  return Status::OK();
}

template <typename ArrowType, typename ParquetType>
Status FlatColumnReader::Impl::TypedReadBatch(
    int batch_size, std::shared_ptr<Array>* out) {
  using ArrowCType = typename ArrowType::c_type;

  int values_to_read = batch_size;
  RETURN_NOT_OK(InitDataBuffer<ArrowType>(batch_size));
  valid_bits_idx_ = 0;
  if (descr_->max_definition_level() > 0) {
    int valid_bits_size = ::arrow::BitUtil::CeilByte(batch_size + 1) / 8;
    valid_bits_buffer_ = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(valid_bits_buffer_->Resize(valid_bits_size, false));
    valid_bits_ptr_ = valid_bits_buffer_->mutable_data();
    memset(valid_bits_ptr_, 0, valid_bits_size);
    null_count_ = 0;
  }

  while ((values_to_read > 0) && column_reader_) {
    if (descr_->max_definition_level() > 0) {
      RETURN_NOT_OK(def_levels_buffer_.Resize(values_to_read * sizeof(int16_t), false));
    }
    auto reader = dynamic_cast<TypedColumnReader<ParquetType>*>(column_reader_.get());
    int64_t levels_read;
    int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    if (descr_->max_definition_level() == 0) {
      RETURN_NOT_OK((ReadNonNullableBatch<ArrowType, ParquetType>(
          reader, values_to_read, &levels_read)));
    } else {
      // As per the defintion and checks for flat columns:
      // descr_->max_definition_level() == 1
      RETURN_NOT_OK((ReadNullableFlatBatch<ArrowType, ParquetType>(
          reader, def_levels, values_to_read, &levels_read)));
    }
    values_to_read -= levels_read;
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
      RETURN_NOT_OK(data_buffer->Resize(valid_bits_idx_ * sizeof(ArrowCType), false));
      memcpy(data_buffer->mutable_data(), data_buffer_->data(), data_buffer->size());
      data_buffer_ = data_buffer;

      auto valid_bits_buffer = std::make_shared<PoolBuffer>(pool_);
      RETURN_NOT_OK(valid_bits_buffer->Resize(
          ::arrow::BitUtil::CeilByte(valid_bits_idx_) / 8, false));
      memcpy(valid_bits_buffer->mutable_data(), valid_bits_buffer_->data(),
          valid_bits_buffer->size());
      valid_bits_buffer_ = valid_bits_buffer;
    }
    *out = std::make_shared<ArrayType<ArrowType>>(
        field_->type, valid_bits_idx_, data_buffer_, null_count_, valid_bits_buffer_);
    // Relase the ownership
    data_buffer_.reset();
    valid_bits_buffer_.reset();
    return Status::OK();
  } else {
    *out = std::make_shared<ArrayType<ArrowType>>(
        field_->type, valid_bits_idx_, data_buffer_);
    data_buffer_.reset();
    return Status::OK();
  }
}

template <>
Status FlatColumnReader::Impl::TypedReadBatch<::arrow::BooleanType, BooleanType>(
    int batch_size, std::shared_ptr<Array>* out) {
  int values_to_read = batch_size;
  RETURN_NOT_OK(InitDataBuffer<::arrow::BooleanType>(batch_size));
  valid_bits_idx_ = 0;
  if (descr_->max_definition_level() > 0) {
    valid_bits_buffer_ = std::make_shared<PoolBuffer>(pool_);
    int valid_bits_size = ::arrow::BitUtil::CeilByte(batch_size + 1) / 8;
    RETURN_NOT_OK(valid_bits_buffer_->Resize(valid_bits_size, false));
    valid_bits_ptr_ = valid_bits_buffer_->mutable_data();
    memset(valid_bits_ptr_, 0, valid_bits_size);
    null_count_ = 0;
  }

  while ((values_to_read > 0) && column_reader_) {
    if (descr_->max_definition_level() > 0) {
      RETURN_NOT_OK(def_levels_buffer_.Resize(values_to_read * sizeof(int16_t), false));
    }
    auto reader = dynamic_cast<TypedColumnReader<BooleanType>*>(column_reader_.get());
    int64_t levels_read;
    int16_t* def_levels = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    if (descr_->max_definition_level() == 0) {
      RETURN_NOT_OK((ReadNonNullableBatch<::arrow::BooleanType, BooleanType>(
          reader, values_to_read, &levels_read)));
    } else {
      // As per the defintion and checks for flat columns:
      // descr_->max_definition_level() == 1
      RETURN_NOT_OK((ReadNullableFlatBatch<::arrow::BooleanType, BooleanType>(
          reader, def_levels, values_to_read, &levels_read)));
    }
    values_to_read -= levels_read;
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
    *out = std::make_shared<::arrow::BooleanArray>(
        field_->type, valid_bits_idx_, data_buffer_, null_count_, valid_bits_buffer_);
    // Relase the ownership
    data_buffer_.reset();
    valid_bits_buffer_.reset();
    return Status::OK();
  } else {
    *out = std::make_shared<::arrow::BooleanArray>(
        field_->type, valid_bits_idx_, data_buffer_);
    data_buffer_.reset();
    return Status::OK();
  }
}

template <typename ArrowType>
Status FlatColumnReader::Impl::ReadByteArrayBatch(
    int batch_size, std::shared_ptr<Array>* out) {
  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;

  int values_to_read = batch_size;
  BuilderType builder(pool_, field_->type);
  while ((values_to_read > 0) && column_reader_) {
    RETURN_NOT_OK(values_buffer_.Resize(values_to_read * sizeof(ByteArray), false));
    if (descr_->max_definition_level() > 0) {
      RETURN_NOT_OK(def_levels_buffer_.Resize(values_to_read * sizeof(int16_t), false));
    }
    auto reader = dynamic_cast<TypedColumnReader<ByteArrayType>*>(column_reader_.get());
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
  return builder.Finish(out);
}

template <>
Status FlatColumnReader::Impl::TypedReadBatch<::arrow::BinaryType, ByteArrayType>(
    int batch_size, std::shared_ptr<Array>* out) {
  return ReadByteArrayBatch<::arrow::BinaryType>(batch_size, out);
}

template <>
Status FlatColumnReader::Impl::TypedReadBatch<::arrow::StringType, ByteArrayType>(
    int batch_size, std::shared_ptr<Array>* out) {
  return ReadByteArrayBatch<::arrow::StringType>(batch_size, out);
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
    TYPED_BATCH_CASE(BINARY, ::arrow::BinaryType, ByteArrayType)
    case ::arrow::Type::TIMESTAMP: {
      ::arrow::TimestampType* timestamp_type =
          static_cast<::arrow::TimestampType*>(field_->type.get());
      switch (timestamp_type->unit) {
        case ::arrow::TimeUnit::MILLI:
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

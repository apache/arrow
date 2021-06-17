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

#include "arrow/adapters/orc/adapter.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/adapters/orc/adapter_util.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/table_builder.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/macros.h"
#include "arrow/util/range.h"
#include "arrow/util/visibility.h"
#include "orc/Exceptions.hh"

// alias to not interfere with nested orc namespace
namespace liborc = orc;

#define ORC_THROW_NOT_OK(s)                   \
  do {                                        \
    Status _s = (s);                          \
    if (!_s.ok()) {                           \
      std::stringstream ss;                   \
      ss << "Arrow error: " << _s.ToString(); \
      throw liborc::ParseError(ss.str());     \
    }                                         \
  } while (0)

#define ORC_ASSIGN_OR_THROW_IMPL(status_name, lhs, rexpr) \
  auto status_name = (rexpr);                             \
  ORC_THROW_NOT_OK(status_name.status());                 \
  lhs = std::move(status_name).ValueOrDie();

#define ORC_ASSIGN_OR_THROW(lhs, rexpr)                                              \
  ORC_ASSIGN_OR_THROW_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                           lhs, rexpr);

#define ORC_BEGIN_CATCH_NOT_OK try {
#define ORC_END_CATCH_NOT_OK                   \
  }                                            \
  catch (const liborc::ParseError& e) {        \
    return Status::IOError(e.what());          \
  }                                            \
  catch (const liborc::InvalidArgument& e) {   \
    return Status::Invalid(e.what());          \
  }                                            \
  catch (const liborc::NotImplementedYet& e) { \
    return Status::NotImplemented(e.what());   \
  }

#define ORC_CATCH_NOT_OK(_s)  \
  ORC_BEGIN_CATCH_NOT_OK(_s); \
  ORC_END_CATCH_NOT_OK

namespace arrow {
namespace adapters {
namespace orc {

namespace {

// The following are required by ORC to be uint64_t
constexpr uint64_t kOrcWriterBatchSize = 128 * 1024;
constexpr uint64_t kOrcNaturalWriteSize = 128 * 1024;

using internal::checked_cast;

class ArrowInputFile : public liborc::InputStream {
 public:
  explicit ArrowInputFile(const std::shared_ptr<io::RandomAccessFile>& file)
      : file_(file) {}

  uint64_t getLength() const override {
    ORC_ASSIGN_OR_THROW(int64_t size, file_->GetSize());
    return static_cast<uint64_t>(size);
  }

  uint64_t getNaturalReadSize() const override { return 128 * 1024; }

  void read(void* buf, uint64_t length, uint64_t offset) override {
    ORC_ASSIGN_OR_THROW(int64_t bytes_read, file_->ReadAt(offset, length, buf));

    if (static_cast<uint64_t>(bytes_read) != length) {
      throw liborc::ParseError("Short read from arrow input file");
    }
  }

  const std::string& getName() const override {
    static const std::string filename("ArrowInputFile");
    return filename;
  }

 private:
  std::shared_ptr<io::RandomAccessFile> file_;
};

struct StripeInformation {
  uint64_t offset;
  uint64_t length;
  uint64_t num_rows;
  uint64_t first_row_of_stripe;
};

// The number of rows to read in a ColumnVectorBatch
constexpr int64_t kReadRowsBatch = 1000;

class OrcStripeReader : public RecordBatchReader {
 public:
  OrcStripeReader(std::unique_ptr<liborc::RowReader> row_reader,
                  std::shared_ptr<Schema> schema, int64_t batch_size, MemoryPool* pool)
      : row_reader_(std::move(row_reader)),
        schema_(schema),
        pool_(pool),
        batch_size_{batch_size} {}

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    std::unique_ptr<liborc::ColumnVectorBatch> batch;
    ORC_CATCH_NOT_OK(batch = row_reader_->createRowBatch(batch_size_));

    const liborc::Type& type = row_reader_->getSelectedType();
    if (!row_reader_->next(*batch)) {
      out->reset();
      return Status::OK();
    }

    std::unique_ptr<RecordBatchBuilder> builder;
    RETURN_NOT_OK(RecordBatchBuilder::Make(schema_, pool_, batch->numElements, &builder));

    // The top-level type must be a struct to read into an arrow table
    const auto& struct_batch = checked_cast<liborc::StructVectorBatch&>(*batch);

    for (int i = 0; i < builder->num_fields(); i++) {
      RETURN_NOT_OK(AppendBatch(type.getSubtype(i), struct_batch.fields[i], 0,
                                batch->numElements, builder->GetField(i)));
    }

    RETURN_NOT_OK(builder->Flush(out));
    return Status::OK();
  }

 private:
  std::unique_ptr<liborc::RowReader> row_reader_;
  std::shared_ptr<Schema> schema_;
  MemoryPool* pool_;
  int64_t batch_size_;
};

}  // namespace

class ORCFileReader::Impl {
 public:
  Impl() {}
  ~Impl() {}

  Status Open(const std::shared_ptr<io::RandomAccessFile>& file, MemoryPool* pool) {
    std::unique_ptr<ArrowInputFile> io_wrapper(new ArrowInputFile(file));
    liborc::ReaderOptions options;
    std::unique_ptr<liborc::Reader> liborc_reader;
    ORC_CATCH_NOT_OK(liborc_reader = createReader(std::move(io_wrapper), options));
    pool_ = pool;
    reader_ = std::move(liborc_reader);
    current_row_ = 0;

    return Init();
  }

  Status Init() {
    int64_t nstripes = reader_->getNumberOfStripes();
    stripes_.resize(nstripes);
    std::unique_ptr<liborc::StripeInformation> stripe;
    uint64_t first_row_of_stripe = 0;
    for (int i = 0; i < nstripes; ++i) {
      stripe = reader_->getStripe(i);
      stripes_[i] = StripeInformation({stripe->getOffset(), stripe->getLength(),
                                       stripe->getNumberOfRows(), first_row_of_stripe});
      first_row_of_stripe += stripe->getNumberOfRows();
    }
    return Status::OK();
  }

  int64_t NumberOfStripes() { return stripes_.size(); }

  int64_t NumberOfRows() { return reader_->getNumberOfRows(); }

  Status ReadSchema(std::shared_ptr<Schema>* out) {
    const liborc::Type& type = reader_->getType();
    return GetArrowSchema(type, out);
  }

  Status ReadSchema(const liborc::RowReaderOptions& opts, std::shared_ptr<Schema>* out) {
    std::unique_ptr<liborc::RowReader> row_reader;
    ORC_CATCH_NOT_OK(row_reader = reader_->createRowReader(opts));
    const liborc::Type& type = row_reader->getSelectedType();
    return GetArrowSchema(type, out);
  }

  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() {
    const std::list<std::string> keys = reader_->getMetadataKeys();
    auto metadata = std::make_shared<KeyValueMetadata>();
    for (const auto& key : keys) {
      metadata->Append(key, reader_->getMetadataValue(key));
    }
    return std::const_pointer_cast<const KeyValueMetadata>(metadata);
  }

  Status GetArrowSchema(const liborc::Type& type, std::shared_ptr<Schema>* out) {
    if (type.getKind() != liborc::STRUCT) {
      return Status::NotImplemented(
          "Only ORC files with a top-level struct "
          "can be handled");
    }
    int size = static_cast<int>(type.getSubtypeCount());
    std::vector<std::shared_ptr<Field>> fields;
    for (int child = 0; child < size; ++child) {
      std::shared_ptr<DataType> elemtype;
      RETURN_NOT_OK(GetArrowType(type.getSubtype(child), &elemtype));
      std::string name = type.getFieldName(child);
      fields.push_back(field(name, elemtype));
    }
    ARROW_ASSIGN_OR_RAISE(auto metadata, ReadMetadata());
    *out = std::make_shared<Schema>(std::move(fields), std::move(metadata));
    return Status::OK();
  }

  Status Read(std::shared_ptr<Table>* out) {
    liborc::RowReaderOptions opts;
    std::shared_ptr<Schema> schema;
    RETURN_NOT_OK(ReadSchema(opts, &schema));
    return ReadTable(opts, schema, out);
  }

  Status Read(const std::shared_ptr<Schema>& schema, std::shared_ptr<Table>* out) {
    liborc::RowReaderOptions opts;
    return ReadTable(opts, schema, out);
  }

  Status Read(const std::vector<int>& include_indices, std::shared_ptr<Table>* out) {
    liborc::RowReaderOptions opts;
    RETURN_NOT_OK(SelectIndices(&opts, include_indices));
    std::shared_ptr<Schema> schema;
    RETURN_NOT_OK(ReadSchema(opts, &schema));
    return ReadTable(opts, schema, out);
  }

  Status Read(const std::shared_ptr<Schema>& schema,
              const std::vector<int>& include_indices, std::shared_ptr<Table>* out) {
    liborc::RowReaderOptions opts;
    RETURN_NOT_OK(SelectIndices(&opts, include_indices));
    return ReadTable(opts, schema, out);
  }

  Status ReadStripe(int64_t stripe, std::shared_ptr<RecordBatch>* out) {
    liborc::RowReaderOptions opts;
    RETURN_NOT_OK(SelectStripe(&opts, stripe));
    std::shared_ptr<Schema> schema;
    RETURN_NOT_OK(ReadSchema(opts, &schema));
    return ReadBatch(opts, schema, stripes_[stripe].num_rows, out);
  }

  Status ReadStripe(int64_t stripe, const std::vector<int>& include_indices,
                    std::shared_ptr<RecordBatch>* out) {
    liborc::RowReaderOptions opts;
    RETURN_NOT_OK(SelectIndices(&opts, include_indices));
    RETURN_NOT_OK(SelectStripe(&opts, stripe));
    std::shared_ptr<Schema> schema;
    RETURN_NOT_OK(ReadSchema(opts, &schema));
    return ReadBatch(opts, schema, stripes_[stripe].num_rows, out);
  }

  Status SelectStripe(liborc::RowReaderOptions* opts, int64_t stripe) {
    ARROW_RETURN_IF(stripe < 0 || stripe >= NumberOfStripes(),
                    Status::Invalid("Out of bounds stripe: ", stripe));

    opts->range(stripes_[stripe].offset, stripes_[stripe].length);
    return Status::OK();
  }

  Status SelectStripeWithRowNumber(liborc::RowReaderOptions* opts, int64_t row_number,
                                   StripeInformation* out) {
    ARROW_RETURN_IF(row_number >= NumberOfRows(),
                    Status::Invalid("Out of bounds row number: ", row_number));

    for (auto it = stripes_.begin(); it != stripes_.end(); it++) {
      if (static_cast<uint64_t>(row_number) >= it->first_row_of_stripe &&
          static_cast<uint64_t>(row_number) < it->first_row_of_stripe + it->num_rows) {
        opts->range(it->offset, it->length);
        *out = *it;
        return Status::OK();
      }
    }

    return Status::Invalid("Invalid row number", row_number);
  }

  Status SelectIndices(liborc::RowReaderOptions* opts,
                       const std::vector<int>& include_indices) {
    std::list<uint64_t> include_indices_list;
    for (auto it = include_indices.begin(); it != include_indices.end(); ++it) {
      ARROW_RETURN_IF(*it < 0, Status::Invalid("Negative field index"));
      include_indices_list.push_back(*it);
    }
    opts->includeTypes(include_indices_list);
    return Status::OK();
  }

  Status ReadTable(const liborc::RowReaderOptions& row_opts,
                   const std::shared_ptr<Schema>& schema, std::shared_ptr<Table>* out) {
    liborc::RowReaderOptions opts(row_opts);
    std::vector<std::shared_ptr<RecordBatch>> batches(stripes_.size());
    for (size_t stripe = 0; stripe < stripes_.size(); stripe++) {
      opts.range(stripes_[stripe].offset, stripes_[stripe].length);
      RETURN_NOT_OK(ReadBatch(opts, schema, stripes_[stripe].num_rows, &batches[stripe]));
    }
    return Table::FromRecordBatches(schema, std::move(batches)).Value(out);
  }

  Status ReadBatch(const liborc::RowReaderOptions& opts,
                   const std::shared_ptr<Schema>& schema, int64_t nrows,
                   std::shared_ptr<RecordBatch>* out) {
    std::unique_ptr<liborc::RowReader> row_reader;
    std::unique_ptr<liborc::ColumnVectorBatch> batch;

    ORC_BEGIN_CATCH_NOT_OK
    row_reader = reader_->createRowReader(opts);
    batch = row_reader->createRowBatch(std::min(nrows, kReadRowsBatch));
    ORC_END_CATCH_NOT_OK

    std::unique_ptr<RecordBatchBuilder> builder;
    RETURN_NOT_OK(RecordBatchBuilder::Make(schema, pool_, nrows, &builder));

    // The top-level type must be a struct to read into an arrow table
    const auto& struct_batch = checked_cast<liborc::StructVectorBatch&>(*batch);

    const liborc::Type& type = row_reader->getSelectedType();
    while (row_reader->next(*batch)) {
      for (int i = 0; i < builder->num_fields(); i++) {
        RETURN_NOT_OK(AppendBatch(type.getSubtype(i), struct_batch.fields[i], 0,
                                  batch->numElements, builder->GetField(i)));
      }
    }
    RETURN_NOT_OK(builder->Flush(out));
    return Status::OK();
  }

  Status Seek(int64_t row_number) {
    ARROW_RETURN_IF(row_number >= NumberOfRows(),
                    Status::Invalid("Out of bounds row number: ", row_number));

    current_row_ = row_number;
    return Status::OK();
  }

  Status NextStripeReader(int64_t batch_size, const std::vector<int>& include_indices,
                          std::shared_ptr<RecordBatchReader>* out) {
    if (current_row_ >= NumberOfRows()) {
      out->reset();
      return Status::OK();
    }

    liborc::RowReaderOptions opts;
    if (!include_indices.empty()) {
      RETURN_NOT_OK(SelectIndices(&opts, include_indices));
    }
    StripeInformation stripe_info({0, 0, 0, 0});
    RETURN_NOT_OK(SelectStripeWithRowNumber(&opts, current_row_, &stripe_info));
    std::shared_ptr<Schema> schema;
    RETURN_NOT_OK(ReadSchema(opts, &schema));
    std::unique_ptr<liborc::RowReader> row_reader;

    ORC_BEGIN_CATCH_NOT_OK
    row_reader = reader_->createRowReader(opts);
    row_reader->seekToRow(current_row_);
    current_row_ = stripe_info.first_row_of_stripe + stripe_info.num_rows;
    ORC_END_CATCH_NOT_OK

    *out = std::shared_ptr<RecordBatchReader>(
        new OrcStripeReader(std::move(row_reader), schema, batch_size, pool_));
    return Status::OK();
  }

  Status NextStripeReader(int64_t batch_size, std::shared_ptr<RecordBatchReader>* out) {
    return NextStripeReader(batch_size, {}, out);
  }

 private:
  MemoryPool* pool_;
  std::unique_ptr<liborc::Reader> reader_;
  std::vector<StripeInformation> stripes_;
  int64_t current_row_;
};

ORCFileReader::ORCFileReader() { impl_.reset(new ORCFileReader::Impl()); }

ORCFileReader::~ORCFileReader() {}

Status ORCFileReader::Open(const std::shared_ptr<io::RandomAccessFile>& file,
                           MemoryPool* pool, std::unique_ptr<ORCFileReader>* reader) {
  auto result = std::unique_ptr<ORCFileReader>(new ORCFileReader());
  RETURN_NOT_OK(result->impl_->Open(file, pool));
  *reader = std::move(result);
  return Status::OK();
}

Result<std::shared_ptr<const KeyValueMetadata>> ORCFileReader::ReadMetadata() {
  return impl_->ReadMetadata();
}

Status ORCFileReader::ReadSchema(std::shared_ptr<Schema>* out) {
  return impl_->ReadSchema(out);
}

Status ORCFileReader::Read(std::shared_ptr<Table>* out) { return impl_->Read(out); }

Status ORCFileReader::Read(const std::shared_ptr<Schema>& schema,
                           std::shared_ptr<Table>* out) {
  return impl_->Read(schema, out);
}

Status ORCFileReader::Read(const std::vector<int>& include_indices,
                           std::shared_ptr<Table>* out) {
  return impl_->Read(include_indices, out);
}

Status ORCFileReader::Read(const std::shared_ptr<Schema>& schema,
                           const std::vector<int>& include_indices,
                           std::shared_ptr<Table>* out) {
  return impl_->Read(schema, include_indices, out);
}

Status ORCFileReader::ReadStripe(int64_t stripe, std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadStripe(stripe, out);
}

Status ORCFileReader::ReadStripe(int64_t stripe, const std::vector<int>& include_indices,
                                 std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadStripe(stripe, include_indices, out);
}

Status ORCFileReader::Seek(int64_t row_number) { return impl_->Seek(row_number); }

Status ORCFileReader::NextStripeReader(int64_t batch_sizes,
                                       std::shared_ptr<RecordBatchReader>* out) {
  return impl_->NextStripeReader(batch_sizes, out);
}

Status ORCFileReader::NextStripeReader(int64_t batch_size,
                                       const std::vector<int>& include_indices,
                                       std::shared_ptr<RecordBatchReader>* out) {
  return impl_->NextStripeReader(batch_size, include_indices, out);
}

int64_t ORCFileReader::NumberOfStripes() { return impl_->NumberOfStripes(); }

int64_t ORCFileReader::NumberOfRows() { return impl_->NumberOfRows(); }

namespace {

class ArrowOutputStream : public liborc::OutputStream {
 public:
  explicit ArrowOutputStream(arrow::io::OutputStream& output_stream)
      : output_stream_(output_stream), length_(0) {}

  uint64_t getLength() const override { return length_; }

  uint64_t getNaturalWriteSize() const override { return kOrcNaturalWriteSize; }

  void write(const void* buf, size_t length) override {
    ORC_THROW_NOT_OK(output_stream_.Write(buf, static_cast<int64_t>(length)));
    length_ += static_cast<int64_t>(length);
  }

  // Mandatory due to us implementing an ORC virtual class.
  // Used by ORC for error messages, not used by Arrow
  const std::string& getName() const override {
    static const std::string filename("ArrowOutputFile");
    return filename;
  }

  void close() override {
    if (!output_stream_.closed()) {
      ORC_THROW_NOT_OK(output_stream_.Close());
    }
  }

  void set_length(int64_t length) { length_ = length; }

 private:
  arrow::io::OutputStream& output_stream_;
  int64_t length_;
};

}  // namespace

class ORCFileWriter::Impl {
 public:
  Status Open(arrow::io::OutputStream* output_stream) {
    out_stream_ = std::unique_ptr<liborc::OutputStream>(
        checked_cast<liborc::OutputStream*>(new ArrowOutputStream(*output_stream)));
    return Status::OK();
  }

  Status Write(const Table& table) {
    std::unique_ptr<liborc::WriterOptions> orc_options =
        std::unique_ptr<liborc::WriterOptions>(new liborc::WriterOptions());
    ARROW_ASSIGN_OR_RAISE(auto orc_schema, GetOrcType(*(table.schema())));
    ORC_CATCH_NOT_OK(
        writer_ = liborc::createWriter(*orc_schema, out_stream_.get(), *orc_options))

    int64_t num_rows = table.num_rows();
    const int num_cols_ = table.num_columns();
    std::vector<int64_t> arrow_index_offset(num_cols_, 0);
    std::vector<int> arrow_chunk_offset(num_cols_, 0);
    std::unique_ptr<liborc::ColumnVectorBatch> batch =
        writer_->createRowBatch(kOrcWriterBatchSize);
    liborc::StructVectorBatch* root =
        internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
    while (num_rows > 0) {
      for (int i = 0; i < num_cols_; i++) {
        RETURN_NOT_OK(adapters::orc::WriteBatch(
            *(table.column(i)), kOrcWriterBatchSize, &(arrow_chunk_offset[i]),
            &(arrow_index_offset[i]), (root->fields)[i]));
      }
      root->numElements = (root->fields)[0]->numElements;
      writer_->add(*batch);
      batch->clear();
      num_rows -= kOrcWriterBatchSize;
    }
    return Status::OK();
  }

  Status Close() {
    writer_->close();
    return Status::OK();
  }

 private:
  std::unique_ptr<liborc::Writer> writer_;
  std::unique_ptr<liborc::OutputStream> out_stream_;
};

ORCFileWriter::~ORCFileWriter() {}

ORCFileWriter::ORCFileWriter() { impl_.reset(new ORCFileWriter::Impl()); }

Result<std::unique_ptr<ORCFileWriter>> ORCFileWriter::Open(
    io::OutputStream* output_stream) {
  std::unique_ptr<ORCFileWriter> result =
      std::unique_ptr<ORCFileWriter>(new ORCFileWriter());
  Status status = result->impl_->Open(output_stream);
  RETURN_NOT_OK(status);
  return std::move(result);
}

Status ORCFileWriter::Write(const Table& table) { return impl_->Write(table); }

Status ORCFileWriter::Close() { return impl_->Close(); }

}  // namespace orc
}  // namespace adapters
}  // namespace arrow

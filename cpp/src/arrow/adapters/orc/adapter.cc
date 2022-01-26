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

#include "arrow/adapters/orc/util.h"
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

// The following is required by ORC to be uint64_t
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

  int64_t NumberOfRows() { return static_cast<int64_t>(reader_->getNumberOfRows()); }

  FileVersion GetFileVersion() {
    liborc::FileVersion orc_file_version = reader_->getFormatVersion();
    return FileVersion(orc_file_version.getMajor(), orc_file_version.getMinor());
  }

  Result<Compression::type> GetCompression() {
    liborc::CompressionKind orc_compression = reader_->getCompression();
    switch (orc_compression) {
      case liborc::CompressionKind::CompressionKind_NONE:
        return Compression::UNCOMPRESSED;
      case liborc::CompressionKind::CompressionKind_ZLIB:
        return Compression::GZIP;
      case liborc::CompressionKind::CompressionKind_SNAPPY:
        return Compression::SNAPPY;
      case liborc::CompressionKind::CompressionKind_LZ4:
        return Compression::LZ4;
      case liborc::CompressionKind::CompressionKind_ZSTD:
        return Compression::ZSTD;
      default:
        // liborc::CompressionKind::CompressionKind_MAX isn't really a compression type
        return Status::Invalid("Compression type not supported by Arrow");
    }
  }

  std::string GetSoftwareVersion() { return reader_->getSoftwareVersion(); }

  int64_t GetCompressionSize() {
    return static_cast<int64_t>(reader_->getCompressionSize());
  }

  int64_t GetRowIndexStride() {
    return static_cast<int64_t>(reader_->getRowIndexStride());
  }

  WriterId GetWriterId() {
    return static_cast<WriterId>(static_cast<int8_t>(reader_->getWriterId()));
  }

  int32_t GetWriterIdValue() { return static_cast<int32_t>(reader_->getWriterIdValue()); }

  WriterVersion GetWriterVersion() {
    return static_cast<WriterVersion>(static_cast<int8_t>(reader_->getWriterVersion()));
  }

  int64_t GetNumberOfStripeStatistics() {
    return static_cast<int64_t>(reader_->getNumberOfStripeStatistics());
  }

  int64_t GetContentLength() { return static_cast<int64_t>(reader_->getContentLength()); }

  int64_t GetStripeStatisticsLength() {
    return static_cast<int64_t>(reader_->getStripeStatisticsLength());
  }

  int64_t GetFileFooterLength() {
    return static_cast<int64_t>(reader_->getFileFooterLength());
  }

  int64_t GetFilePostscriptLength() {
    return static_cast<int64_t>(reader_->getFilePostscriptLength());
  }

  int64_t GetFileLength() { return static_cast<int64_t>(reader_->getFileLength()); }

  std::string GetSerializedFileTail() { return reader_->getSerializedFileTail(); }

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

  Status Read(const std::vector<std::string>& include_names,
              std::shared_ptr<Table>* out) {
    liborc::RowReaderOptions opts;
    RETURN_NOT_OK(SelectNames(&opts, include_names));
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

  Status ReadStripe(int64_t stripe, const std::vector<std::string>& include_names,
                    std::shared_ptr<RecordBatch>* out) {
    liborc::RowReaderOptions opts;
    RETURN_NOT_OK(SelectNames(&opts, include_names));
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

  Status SelectNames(liborc::RowReaderOptions* opts,
                     const std::vector<std::string>& include_names) {
    std::list<std::string> include_names_list(include_names.begin(), include_names.end());
    opts->include(include_names_list);
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
  return Open(file, pool).Value(reader);
}

Result<std::unique_ptr<ORCFileReader>> ORCFileReader::Open(
    const std::shared_ptr<io::RandomAccessFile>& file, MemoryPool* pool) {
  auto result = std::unique_ptr<ORCFileReader>(new ORCFileReader());
  RETURN_NOT_OK(result->impl_->Open(file, pool));
  return std::move(result);
}

Result<std::shared_ptr<const KeyValueMetadata>> ORCFileReader::ReadMetadata() {
  return impl_->ReadMetadata();
}

Status ORCFileReader::ReadSchema(std::shared_ptr<Schema>* out) {
  return impl_->ReadSchema(out);
}

Result<std::shared_ptr<Schema>> ORCFileReader::ReadSchema() {
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(impl_->ReadSchema(&schema));
  return schema;
}

Status ORCFileReader::Read(std::shared_ptr<Table>* out) { return impl_->Read(out); }

Result<std::shared_ptr<Table>> ORCFileReader::Read() {
  std::shared_ptr<Table> table;
  RETURN_NOT_OK(impl_->Read(&table));
  return table;
}

Status ORCFileReader::Read(const std::shared_ptr<Schema>& schema,
                           std::shared_ptr<Table>* out) {
  return impl_->Read(schema, out);
}

Result<std::shared_ptr<Table>> ORCFileReader::Read(
    const std::shared_ptr<Schema>& schema) {
  std::shared_ptr<Table> table;
  RETURN_NOT_OK(impl_->Read(schema, &table));
  return table;
}

Status ORCFileReader::Read(const std::vector<int>& include_indices,
                           std::shared_ptr<Table>* out) {
  return impl_->Read(include_indices, out);
}

Result<std::shared_ptr<Table>> ORCFileReader::Read(
    const std::vector<int>& include_indices) {
  std::shared_ptr<Table> table;
  RETURN_NOT_OK(impl_->Read(include_indices, &table));
  return table;
}

Result<std::shared_ptr<Table>> ORCFileReader::Read(
    const std::vector<std::string>& include_names) {
  std::shared_ptr<Table> table;
  RETURN_NOT_OK(impl_->Read(include_names, &table));
  return table;
}

Status ORCFileReader::Read(const std::shared_ptr<Schema>& schema,
                           const std::vector<int>& include_indices,
                           std::shared_ptr<Table>* out) {
  return impl_->Read(schema, include_indices, out);
}

Result<std::shared_ptr<Table>> ORCFileReader::Read(
    const std::shared_ptr<Schema>& schema, const std::vector<int>& include_indices) {
  std::shared_ptr<Table> table;
  RETURN_NOT_OK(impl_->Read(schema, include_indices, &table));
  return table;
}

Status ORCFileReader::ReadStripe(int64_t stripe, std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadStripe(stripe, out);
}

Result<std::shared_ptr<RecordBatch>> ORCFileReader::ReadStripe(int64_t stripe) {
  std::shared_ptr<RecordBatch> recordBatch;
  RETURN_NOT_OK(impl_->ReadStripe(stripe, &recordBatch));
  return recordBatch;
}

Status ORCFileReader::ReadStripe(int64_t stripe, const std::vector<int>& include_indices,
                                 std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadStripe(stripe, include_indices, out);
}

Result<std::shared_ptr<RecordBatch>> ORCFileReader::ReadStripe(
    int64_t stripe, const std::vector<int>& include_indices) {
  std::shared_ptr<RecordBatch> recordBatch;
  RETURN_NOT_OK(impl_->ReadStripe(stripe, include_indices, &recordBatch));
  return recordBatch;
}

Result<std::shared_ptr<RecordBatch>> ORCFileReader::ReadStripe(
    int64_t stripe, const std::vector<std::string>& include_names) {
  std::shared_ptr<RecordBatch> recordBatch;
  RETURN_NOT_OK(impl_->ReadStripe(stripe, include_names, &recordBatch));
  return recordBatch;
}

Status ORCFileReader::Seek(int64_t row_number) { return impl_->Seek(row_number); }

Status ORCFileReader::NextStripeReader(int64_t batch_sizes,
                                       std::shared_ptr<RecordBatchReader>* out) {
  return impl_->NextStripeReader(batch_sizes, out);
}

Result<std::shared_ptr<RecordBatchReader>> ORCFileReader::NextStripeReader(
    int64_t batch_size) {
  std::shared_ptr<RecordBatchReader> reader;
  RETURN_NOT_OK(impl_->NextStripeReader(batch_size, &reader));
  return reader;
}

Status ORCFileReader::NextStripeReader(int64_t batch_size,
                                       const std::vector<int>& include_indices,
                                       std::shared_ptr<RecordBatchReader>* out) {
  return impl_->NextStripeReader(batch_size, include_indices, out);
}

Result<std::shared_ptr<RecordBatchReader>> ORCFileReader::NextStripeReader(
    int64_t batch_size, const std::vector<int>& include_indices) {
  std::shared_ptr<RecordBatchReader> reader;
  RETURN_NOT_OK(impl_->NextStripeReader(batch_size, include_indices, &reader));
  return reader;
}

int64_t ORCFileReader::NumberOfStripes() { return impl_->NumberOfStripes(); }

int64_t ORCFileReader::NumberOfRows() { return impl_->NumberOfRows(); }

FileVersion ORCFileReader::GetFileVersion() { return impl_->GetFileVersion(); }

std::string ORCFileReader::GetSoftwareVersion() { return impl_->GetSoftwareVersion(); }

Result<Compression::type> ORCFileReader::GetCompression() {
  return impl_->GetCompression();
}

int64_t ORCFileReader::GetCompressionSize() { return impl_->GetCompressionSize(); }

int64_t ORCFileReader::GetRowIndexStride() { return impl_->GetRowIndexStride(); }

WriterId ORCFileReader::GetWriterId() { return impl_->GetWriterId(); }

int32_t ORCFileReader::GetWriterIdValue() { return impl_->GetWriterIdValue(); }

WriterVersion ORCFileReader::GetWriterVersion() { return impl_->GetWriterVersion(); }

int64_t ORCFileReader::GetNumberOfStripeStatistics() {
  return impl_->GetNumberOfStripeStatistics();
}

int64_t ORCFileReader::GetContentLength() { return impl_->GetContentLength(); }

int64_t ORCFileReader::GetStripeStatisticsLength() {
  return impl_->GetStripeStatisticsLength();
}

int64_t ORCFileReader::GetFileFooterLength() { return impl_->GetFileFooterLength(); }

int64_t ORCFileReader::GetFilePostscriptLength() {
  return impl_->GetFilePostscriptLength();
}

int64_t ORCFileReader::GetFileLength() { return impl_->GetFileLength(); }

std::string ORCFileReader::GetSerializedFileTail() {
  return impl_->GetSerializedFileTail();
}

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

Result<liborc::WriterOptions> MakeOrcWriterOptions(
    arrow::adapters::orc::WriteOptions options) {
  liborc::WriterOptions orc_options;
  orc_options.setFileVersion(
      liborc::FileVersion(static_cast<uint32_t>(options.file_version.major_version()),
                          static_cast<uint32_t>(options.file_version.minor_version())));
  orc_options.setStripeSize(static_cast<uint64_t>(options.stripe_size));
  orc_options.setCompressionBlockSize(
      static_cast<uint64_t>(options.compression_block_size));
  orc_options.setCompressionStrategy(static_cast<liborc::CompressionStrategy>(
      static_cast<int8_t>(options.compression_strategy)));
  orc_options.setRowIndexStride(static_cast<uint64_t>(options.row_index_stride));
  orc_options.setPaddingTolerance(options.padding_tolerance);
  orc_options.setDictionaryKeySizeThreshold(options.dictionary_key_size_threshold);
  std::set<uint64_t> orc_bloom_filter_columns;
  std::for_each(options.bloom_filter_columns.begin(), options.bloom_filter_columns.end(),
                [&orc_bloom_filter_columns](const int64_t col) {
                  orc_bloom_filter_columns.insert(static_cast<uint64_t>(col));
                });
  orc_options.setColumnsUseBloomFilter(std::move(orc_bloom_filter_columns));
  orc_options.setBloomFilterFPP(options.bloom_filter_fpp);
  switch (options.compression) {
    case Compression::UNCOMPRESSED:
      orc_options.setCompression(liborc::CompressionKind::CompressionKind_NONE);
      break;
    case Compression::GZIP:
      orc_options.setCompression(liborc::CompressionKind::CompressionKind_ZLIB);
      break;
    case Compression::SNAPPY:
      orc_options.setCompression(liborc::CompressionKind::CompressionKind_SNAPPY);
      break;
    case Compression::LZ4:
      orc_options.setCompression(liborc::CompressionKind::CompressionKind_LZ4);
      break;
    case Compression::ZSTD:
      orc_options.setCompression(liborc::CompressionKind::CompressionKind_ZSTD);
      break;
    default:
      return Status::Invalid("Compression type not supported by ORC");
  }
  return orc_options;
}

}  // namespace

class ORCFileWriter::Impl {
 public:
  Status Open(arrow::io::OutputStream* output_stream, const WriteOptions& write_options) {
    out_stream_ = std::unique_ptr<liborc::OutputStream>(
        checked_cast<liborc::OutputStream*>(new ArrowOutputStream(*output_stream)));
    write_options_ = write_options;
    return Status::OK();
  }

  Status Write(const Table& table) {
    ARROW_ASSIGN_OR_RAISE(auto orc_schema, GetOrcType(*(table.schema())));
    ARROW_ASSIGN_OR_RAISE(auto orc_options, MakeOrcWriterOptions(write_options_));
    auto batch_size = static_cast<uint64_t>(write_options_.batch_size);
    ORC_CATCH_NOT_OK(
        writer_ = liborc::createWriter(*orc_schema, out_stream_.get(), orc_options))

    int64_t num_rows = table.num_rows();
    const int num_cols = table.num_columns();
    std::vector<int64_t> arrow_index_offset(num_cols, 0);
    std::vector<int> arrow_chunk_offset(num_cols, 0);
    std::unique_ptr<liborc::ColumnVectorBatch> batch =
        writer_->createRowBatch(batch_size);
    liborc::StructVectorBatch* root =
        internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
    while (num_rows > 0) {
      for (int i = 0; i < num_cols; i++) {
        RETURN_NOT_OK(adapters::orc::WriteBatch(
            *(table.column(i)), batch_size, &(arrow_chunk_offset[i]),
            &(arrow_index_offset[i]), (root->fields)[i]));
      }
      root->numElements = (root->fields)[0]->numElements;
      writer_->add(*batch);
      batch->clear();
      num_rows -= batch_size;
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
  WriteOptions write_options_;
};

ORCFileWriter::~ORCFileWriter() {}

ORCFileWriter::ORCFileWriter() { impl_.reset(new ORCFileWriter::Impl()); }

Result<std::unique_ptr<ORCFileWriter>> ORCFileWriter::Open(
    io::OutputStream* output_stream, const WriteOptions& writer_options) {
  std::unique_ptr<ORCFileWriter> result =
      std::unique_ptr<ORCFileWriter>(new ORCFileWriter());
  Status status = result->impl_->Open(output_stream, writer_options);
  RETURN_NOT_OK(status);
  return std::move(result);
}

Status ORCFileWriter::Write(const Table& table) { return impl_->Write(table); }

Status ORCFileWriter::Close() { return impl_->Close(); }

}  // namespace orc
}  // namespace adapters
}  // namespace arrow

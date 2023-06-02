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
    ARROW_ASSIGN_OR_RAISE(builder,
                          RecordBatchBuilder::Make(schema_, pool_, batch->numElements));

    // The top-level type must be a struct to read into an arrow table
    const auto& struct_batch = checked_cast<liborc::StructVectorBatch&>(*batch);

    for (int i = 0; i < builder->num_fields(); i++) {
      RETURN_NOT_OK(AppendBatch(type.getSubtype(i), struct_batch.fields[i], 0,
                                batch->numElements, builder->GetField(i)));
    }

    ARROW_ASSIGN_OR_RAISE(*out, builder->Flush());
    return Status::OK();
  }

 private:
  std::unique_ptr<liborc::RowReader> row_reader_;
  std::shared_ptr<Schema> schema_;
  MemoryPool* pool_;
  int64_t batch_size_;
};

liborc::RowReaderOptions default_row_reader_options() {
  liborc::RowReaderOptions options;
  // Orc timestamp type is error-prone since it serializes values in the writer timezone
  // and reads them back in the reader timezone. To avoid this, both the Apache Orc C++
  // writer and reader set the timezone to GMT by default to avoid any conversion.
  // We follow the same practice here explicitly to make sure readers are aware of this.
  options.setTimezoneName("GMT");
  return options;
}

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
      stripes_[i] = StripeInformation({static_cast<int64_t>(stripe->getOffset()),
                                       static_cast<int64_t>(stripe->getLength()),
                                       static_cast<int64_t>(stripe->getNumberOfRows()),
                                       static_cast<int64_t>(first_row_of_stripe)});
      first_row_of_stripe += stripe->getNumberOfRows();
    }
    return Status::OK();
  }

  int64_t NumberOfStripes() { return stripes_.size(); }

  int64_t NumberOfRows() { return static_cast<int64_t>(reader_->getNumberOfRows()); }

  StripeInformation GetStripeInformation(int64_t stripe) { return stripes_[stripe]; }

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

  Result<std::shared_ptr<Schema>> ReadSchema() {
    const liborc::Type& type = reader_->getType();
    return GetArrowSchema(type);
  }

  Result<std::shared_ptr<Schema>> ReadSchema(const liborc::RowReaderOptions& opts) {
    std::unique_ptr<liborc::RowReader> row_reader;
    ORC_CATCH_NOT_OK(row_reader = reader_->createRowReader(opts));
    const liborc::Type& type = row_reader->getSelectedType();
    return GetArrowSchema(type);
  }

  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() {
    const std::list<std::string> keys = reader_->getMetadataKeys();
    auto metadata = std::make_shared<KeyValueMetadata>();
    for (const auto& key : keys) {
      metadata->Append(key, reader_->getMetadataValue(key));
    }
    return std::const_pointer_cast<const KeyValueMetadata>(metadata);
  }

  Result<std::shared_ptr<Schema>> GetArrowSchema(const liborc::Type& type) {
    if (type.getKind() != liborc::STRUCT) {
      return Status::NotImplemented(
          "Only ORC files with a top-level struct "
          "can be handled");
    }
    int size = static_cast<int>(type.getSubtypeCount());
    std::vector<std::shared_ptr<Field>> fields;
    fields.reserve(size);
    for (int child = 0; child < size; ++child) {
      const std::string& name = type.getFieldName(child);
      ARROW_ASSIGN_OR_RAISE(auto elem_field, GetArrowField(name, type.getSubtype(child)));
      fields.push_back(std::move(elem_field));
    }
    ARROW_ASSIGN_OR_RAISE(auto metadata, ReadMetadata());
    return std::make_shared<Schema>(std::move(fields), std::move(metadata));
  }

  Result<std::shared_ptr<Table>> Read() {
    liborc::RowReaderOptions opts = default_row_reader_options();
    ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchema());
    return ReadTable(opts, schema);
  }

  Result<std::shared_ptr<Table>> Read(const std::shared_ptr<Schema>& schema) {
    liborc::RowReaderOptions opts = default_row_reader_options();
    return ReadTable(opts, schema);
  }

  Result<std::shared_ptr<Table>> Read(const std::vector<int>& include_indices) {
    liborc::RowReaderOptions opts = default_row_reader_options();
    RETURN_NOT_OK(SelectIndices(&opts, include_indices));
    ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchema(opts));
    return ReadTable(opts, schema);
  }

  Result<std::shared_ptr<Table>> Read(const std::vector<std::string>& include_names) {
    liborc::RowReaderOptions opts = default_row_reader_options();
    RETURN_NOT_OK(SelectNames(&opts, include_names));
    ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchema(opts));
    return ReadTable(opts, schema);
  }

  Result<std::shared_ptr<Table>> Read(const std::shared_ptr<Schema>& schema,
                                      const std::vector<int>& include_indices) {
    liborc::RowReaderOptions opts = default_row_reader_options();
    RETURN_NOT_OK(SelectIndices(&opts, include_indices));
    return ReadTable(opts, schema);
  }

  Result<std::shared_ptr<RecordBatch>> ReadStripe(int64_t stripe) {
    liborc::RowReaderOptions opts = default_row_reader_options();
    RETURN_NOT_OK(SelectStripe(&opts, stripe));
    ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchema(opts));
    return ReadBatch(opts, schema, stripes_[stripe].num_rows);
  }

  Result<std::shared_ptr<RecordBatch>> ReadStripe(
      int64_t stripe, const std::vector<int>& include_indices) {
    liborc::RowReaderOptions opts = default_row_reader_options();
    RETURN_NOT_OK(SelectIndices(&opts, include_indices));
    RETURN_NOT_OK(SelectStripe(&opts, stripe));
    ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchema(opts));
    return ReadBatch(opts, schema, stripes_[stripe].num_rows);
  }

  Result<std::shared_ptr<RecordBatch>> ReadStripe(
      int64_t stripe, const std::vector<std::string>& include_names) {
    liborc::RowReaderOptions opts = default_row_reader_options();
    RETURN_NOT_OK(SelectNames(&opts, include_names));
    RETURN_NOT_OK(SelectStripe(&opts, stripe));
    ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchema(opts));
    return ReadBatch(opts, schema, stripes_[stripe].num_rows);
  }

  Status SelectStripe(liborc::RowReaderOptions* opts, int64_t stripe) {
    ARROW_RETURN_IF(stripe < 0 || stripe >= NumberOfStripes(),
                    Status::Invalid("Out of bounds stripe: ", stripe));

    opts->range(static_cast<uint64_t>(stripes_[stripe].offset),
                static_cast<uint64_t>(stripes_[stripe].length));
    return Status::OK();
  }

  Status SelectStripeWithRowNumber(liborc::RowReaderOptions* opts, int64_t row_number,
                                   StripeInformation* out) {
    ARROW_RETURN_IF(row_number >= NumberOfRows(),
                    Status::Invalid("Out of bounds row number: ", row_number));

    for (auto it = stripes_.begin(); it != stripes_.end(); it++) {
      if (row_number >= it->first_row_id &&
          row_number < it->first_row_id + it->num_rows) {
        opts->range(static_cast<uint64_t>(it->offset), static_cast<uint64_t>(it->length));
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
    opts->include(include_indices_list);
    return Status::OK();
  }

  Status SelectNames(liborc::RowReaderOptions* opts,
                     const std::vector<std::string>& include_names) {
    std::list<std::string> include_names_list(include_names.begin(), include_names.end());
    opts->include(include_names_list);
    return Status::OK();
  }

  Result<std::shared_ptr<Table>> ReadTable(const liborc::RowReaderOptions& row_opts,
                                           const std::shared_ptr<Schema>& schema) {
    liborc::RowReaderOptions opts(row_opts);
    std::vector<std::shared_ptr<RecordBatch>> batches(stripes_.size());
    for (size_t stripe = 0; stripe < stripes_.size(); stripe++) {
      opts.range(static_cast<uint64_t>(stripes_[stripe].offset),
                 static_cast<uint64_t>(stripes_[stripe].length));
      ARROW_ASSIGN_OR_RAISE(batches[stripe],
                            ReadBatch(opts, schema, stripes_[stripe].num_rows));
    }
    return Table::FromRecordBatches(schema, std::move(batches));
  }

  Result<std::shared_ptr<RecordBatch>> ReadBatch(const liborc::RowReaderOptions& opts,
                                                 const std::shared_ptr<Schema>& schema,
                                                 int64_t nrows) {
    std::unique_ptr<liborc::RowReader> row_reader;
    std::unique_ptr<liborc::ColumnVectorBatch> batch;

    ORC_BEGIN_CATCH_NOT_OK
    row_reader = reader_->createRowReader(opts);
    batch = row_reader->createRowBatch(std::min(nrows, kReadRowsBatch));
    ORC_END_CATCH_NOT_OK

    std::unique_ptr<RecordBatchBuilder> builder;
    ARROW_ASSIGN_OR_RAISE(builder, RecordBatchBuilder::Make(schema, pool_, nrows));

    // The top-level type must be a struct to read into an arrow table
    const auto& struct_batch = checked_cast<liborc::StructVectorBatch&>(*batch);

    const liborc::Type& type = row_reader->getSelectedType();
    while (row_reader->next(*batch)) {
      for (int i = 0; i < builder->num_fields(); i++) {
        RETURN_NOT_OK(AppendBatch(type.getSubtype(i), struct_batch.fields[i], 0,
                                  batch->numElements, builder->GetField(i)));
      }
    }

    return builder->Flush();
  }

  Status Seek(int64_t row_number) {
    ARROW_RETURN_IF(row_number >= NumberOfRows(),
                    Status::Invalid("Out of bounds row number: ", row_number));

    current_row_ = row_number;
    return Status::OK();
  }

  Result<std::shared_ptr<RecordBatchReader>> NextStripeReader(
      int64_t batch_size, const std::vector<int>& include_indices) {
    if (current_row_ >= NumberOfRows()) {
      return nullptr;
    }

    liborc::RowReaderOptions opts = default_row_reader_options();
    if (!include_indices.empty()) {
      RETURN_NOT_OK(SelectIndices(&opts, include_indices));
    }
    StripeInformation stripe_info({0, 0, 0, 0});
    RETURN_NOT_OK(SelectStripeWithRowNumber(&opts, current_row_, &stripe_info));
    ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchema(opts));
    std::unique_ptr<liborc::RowReader> row_reader;

    ORC_BEGIN_CATCH_NOT_OK
    row_reader = reader_->createRowReader(opts);
    row_reader->seekToRow(current_row_);
    current_row_ = stripe_info.first_row_id + stripe_info.num_rows;
    ORC_END_CATCH_NOT_OK

    return std::make_shared<OrcStripeReader>(std::move(row_reader), schema, batch_size,
                                             pool_);
  }

  Result<std::shared_ptr<RecordBatchReader>> GetRecordBatchReader(
      int64_t batch_size, const std::vector<std::string>& include_names) {
    liborc::RowReaderOptions opts = default_row_reader_options();
    if (!include_names.empty()) {
      RETURN_NOT_OK(SelectNames(&opts, include_names));
    }
    ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchema(opts));
    std::unique_ptr<liborc::RowReader> row_reader;

    ORC_BEGIN_CATCH_NOT_OK
    row_reader = reader_->createRowReader(opts);
    ORC_END_CATCH_NOT_OK

    return std::make_shared<OrcStripeReader>(std::move(row_reader), schema, batch_size,
                                             pool_);
  }

  Result<std::shared_ptr<RecordBatchReader>> NextStripeReader(int64_t batch_size) {
    std::vector<int> empty_vec;
    return NextStripeReader(batch_size, empty_vec);
  }

 private:
  MemoryPool* pool_;
  std::unique_ptr<liborc::Reader> reader_;
  std::vector<StripeInformation> stripes_;
  int64_t current_row_;
};

ORCFileReader::ORCFileReader() { impl_.reset(new ORCFileReader::Impl()); }

ORCFileReader::~ORCFileReader() {}

Result<std::unique_ptr<ORCFileReader>> ORCFileReader::Open(
    const std::shared_ptr<io::RandomAccessFile>& file, MemoryPool* pool) {
  auto result = std::unique_ptr<ORCFileReader>(new ORCFileReader());
  RETURN_NOT_OK(result->impl_->Open(file, pool));
  return std::move(result);
}

Result<std::shared_ptr<const KeyValueMetadata>> ORCFileReader::ReadMetadata() {
  return impl_->ReadMetadata();
}

Result<std::shared_ptr<Schema>> ORCFileReader::ReadSchema() {
  return impl_->ReadSchema();
}

Result<std::shared_ptr<Table>> ORCFileReader::Read() { return impl_->Read(); }

Result<std::shared_ptr<Table>> ORCFileReader::Read(
    const std::shared_ptr<Schema>& schema) {
  return impl_->Read(schema);
}

Result<std::shared_ptr<Table>> ORCFileReader::Read(
    const std::vector<int>& include_indices) {
  return impl_->Read(include_indices);
}

Result<std::shared_ptr<Table>> ORCFileReader::Read(
    const std::vector<std::string>& include_names) {
  return impl_->Read(include_names);
}

Result<std::shared_ptr<Table>> ORCFileReader::Read(
    const std::shared_ptr<Schema>& schema, const std::vector<int>& include_indices) {
  return impl_->Read(schema, include_indices);
}

Result<std::shared_ptr<RecordBatch>> ORCFileReader::ReadStripe(int64_t stripe) {
  return impl_->ReadStripe(stripe);
}

Result<std::shared_ptr<RecordBatch>> ORCFileReader::ReadStripe(
    int64_t stripe, const std::vector<int>& include_indices) {
  return impl_->ReadStripe(stripe, include_indices);
}

Result<std::shared_ptr<RecordBatch>> ORCFileReader::ReadStripe(
    int64_t stripe, const std::vector<std::string>& include_names) {
  return impl_->ReadStripe(stripe, include_names);
}

Status ORCFileReader::Seek(int64_t row_number) { return impl_->Seek(row_number); }

Result<std::shared_ptr<RecordBatchReader>> ORCFileReader::NextStripeReader(
    int64_t batch_size) {
  return impl_->NextStripeReader(batch_size);
}

Result<std::shared_ptr<RecordBatchReader>> ORCFileReader::GetRecordBatchReader(
    int64_t batch_size, const std::vector<std::string>& include_names) {
  return impl_->GetRecordBatchReader(batch_size, include_names);
}

Result<std::shared_ptr<RecordBatchReader>> ORCFileReader::NextStripeReader(
    int64_t batch_size, const std::vector<int>& include_indices) {
  return impl_->NextStripeReader(batch_size, include_indices);
}

int64_t ORCFileReader::NumberOfStripes() { return impl_->NumberOfStripes(); }

int64_t ORCFileReader::NumberOfRows() { return impl_->NumberOfRows(); }

StripeInformation ORCFileReader::GetStripeInformation(int64_t stripe) {
  return impl_->GetStripeInformation(stripe);
}

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

  void close() override {}

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
  // Orc timestamp type is error-prone since it serializes values in the writer timezone
  // and reads them back in the reader timezone. To avoid this, both the Apache Orc C++
  // writer and reader set the timezone to GMT by default to avoid any conversion.
  // We follow the same practice here explicitly to make sure readers are aware of this.
  orc_options.setTimezoneName("GMT");
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
    if (!writer_.get()) {
      ARROW_ASSIGN_OR_RAISE(orc_schema_, GetOrcType(*(table.schema())));
      ARROW_ASSIGN_OR_RAISE(auto orc_options, MakeOrcWriterOptions(write_options_));
      arrow_schema_ = table.schema();
      ORC_CATCH_NOT_OK(
          writer_ = liborc::createWriter(*orc_schema_, out_stream_.get(), orc_options))
    } else {
      bool schemas_matching = table.schema()->Equals(arrow_schema_, false);
      if (!schemas_matching) {
        return Status::TypeError(
            "The schema of the RecordBatch does not match"
            " the initial schema. All exported RecordBatches/Tables"
            " must have the same schema.\nInitial:\n",
            *arrow_schema_, "\nCurrent:\n", *table.schema());
      }
    }
    auto batch_size = static_cast<uint64_t>(write_options_.batch_size);
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
            *table.column(i), batch_size, &(arrow_chunk_offset[i]),
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
    if (writer_) {
      writer_->close();
    }
    return Status::OK();
  }

 private:
  std::unique_ptr<liborc::Writer> writer_;
  std::unique_ptr<liborc::OutputStream> out_stream_;
  std::shared_ptr<Schema> arrow_schema_;
  WriteOptions write_options_;
  std::unique_ptr<liborc::Type> orc_schema_;
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

Status ORCFileWriter::Write(const RecordBatch& record_batch) {
  auto table = Table::Make(record_batch.schema(), record_batch.columns());
  return impl_->Write(*table);
}

Status ORCFileWriter::Close() { return impl_->Close(); }

}  // namespace orc
}  // namespace adapters
}  // namespace arrow

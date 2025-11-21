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

#include "arrow/dataset/file_parquet.h"

#include <functional>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/compute/api_scalar.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/parquet_encryption_config.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/test_common.h"
#include "arrow/io/util_internal.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/range.h"

#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace dataset {

using parquet::ArrowWriterProperties;
using parquet::default_arrow_writer_properties;

using parquet::default_writer_properties;
using parquet::WriterProperties;

using parquet::CreateOutputStream;
using parquet::arrow::WriteTable;

using testing::Pointee;

class ParquetFormatHelper {
 public:
  using FormatType = ParquetFileFormat;

  static Result<std::shared_ptr<Buffer>> Write(
      RecordBatchReader* reader,
      const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
          default_arrow_writer_properties()) {
    auto pool = ::arrow::default_memory_pool();
    std::shared_ptr<Buffer> out;
    auto sink = CreateOutputStream(pool);
    RETURN_NOT_OK(WriteRecordBatchReader(reader, pool, sink, default_writer_properties(),
                                         arrow_properties));
    return sink->Finish();
  }
  static std::shared_ptr<ParquetFileFormat> MakeFormat() {
    return std::make_shared<ParquetFileFormat>();
  }

 private:
  static Status WriteRecordBatch(const RecordBatch& batch,
                                 parquet::arrow::FileWriter* writer) {
    auto schema = batch.schema();

    if (!schema->Equals(*writer->schema(), false)) {
      return Status::Invalid("RecordBatch schema does not match this writer's. batch:'",
                             schema->ToString(), "' this:'", writer->schema()->ToString(),
                             "'");
    }

    RETURN_NOT_OK(writer->NewRowGroup());
    for (int i = 0; i < batch.num_columns(); i++) {
      RETURN_NOT_OK(writer->WriteColumnChunk(*batch.column(i)));
    }

    return Status::OK();
  }

  static Status WriteRecordBatchReader(RecordBatchReader* reader,
                                       parquet::arrow::FileWriter* writer) {
    auto schema = reader->schema();

    if (!schema->Equals(*writer->schema(), false)) {
      return Status::Invalid("RecordBatch schema does not match this writer's. batch:'",
                             schema->ToString(), "' this:'", writer->schema()->ToString(),
                             "'");
    }

    return MakeFunctionIterator([reader] { return reader->Next(); })
        .Visit([&](std::shared_ptr<RecordBatch> batch) {
          return WriteRecordBatch(*batch, writer);
        });
  }

  static Status WriteRecordBatchReader(
      RecordBatchReader* reader, MemoryPool* pool,
      const std::shared_ptr<io::OutputStream>& sink,
      const std::shared_ptr<WriterProperties>& properties = default_writer_properties(),
      const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
          default_arrow_writer_properties()) {
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    ARROW_ASSIGN_OR_RAISE(writer,
                          parquet::arrow::FileWriter::Open(*reader->schema(), pool, sink,
                                                           properties, arrow_properties));
    RETURN_NOT_OK(WriteRecordBatchReader(reader, writer.get()));
    return writer->Close();
  }
};

class DelayedBufferReader : public ::arrow::io::BufferReader {
 public:
  explicit DelayedBufferReader(const std::shared_ptr<::arrow::Buffer>& buffer)
      : ::arrow::io::BufferReader(buffer) {}

  ::arrow::Future<std::shared_ptr<Buffer>> ReadAsync(
      const ::arrow::io::IOContext& io_context, int64_t position,
      int64_t nbytes) override {
    read_async_count.fetch_add(1);
    auto self = std::dynamic_pointer_cast<DelayedBufferReader>(shared_from_this());
    return DeferNotOk(::arrow::io::internal::SubmitIO(
        io_context, [self, position, nbytes]() -> Result<std::shared_ptr<Buffer>> {
          std::this_thread::sleep_for(std::chrono::seconds(1));
          return self->DoReadAt(position, nbytes);
        }));
  }

  std::atomic<int> read_async_count{0};
};

using CustomizeScanOptionsWithThreadPool =
    std::function<void(ScanOptions&, arrow::internal::ThreadPool*)>;

class TestParquetFileFormat : public FileFormatFixtureMixin<ParquetFormatHelper> {
 public:
  RecordBatchIterator Batches(Fragment* fragment) {
    EXPECT_OK_AND_ASSIGN(auto batch_gen, fragment->ScanBatchesAsync(opts_));
    return MakeGeneratorIterator(batch_gen);
  }

  std::shared_ptr<RecordBatch> SingleBatch(Fragment* fragment) {
    auto batches = IteratorToVector(Batches(fragment));
    EXPECT_EQ(batches.size(), 1);
    return batches.front();
  }

  void CountRowsAndBatchesInScan(Fragment* fragment, int64_t expected_rows,
                                 int64_t expected_batches) {
    int64_t actual_rows = 0;
    int64_t actual_batches = 0;

    for (auto maybe_batch : Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      actual_rows += batch->num_rows();
      ++actual_batches;
    }

    EXPECT_EQ(actual_rows, expected_rows);
    EXPECT_EQ(actual_batches, expected_batches);
  }

  void CountRowsAndBatchesInScan(const std::shared_ptr<Fragment>& fragment,
                                 int64_t expected_rows, int64_t expected_batches) {
    return CountRowsAndBatchesInScan(fragment.get(), expected_rows, expected_batches);
  }

  void CountRowGroupsInFragment(const std::shared_ptr<Fragment>& fragment,
                                std::vector<int> expected_row_groups,
                                compute::Expression filter) {
    SetFilter(filter);

    auto parquet_fragment = checked_pointer_cast<ParquetFileFragment>(fragment);
    ASSERT_OK_AND_ASSIGN(auto fragments, parquet_fragment->SplitByRowGroup(opts_->filter))

    EXPECT_EQ(fragments.size(), expected_row_groups.size());
    for (size_t i = 0; i < fragments.size(); i++) {
      auto expected = expected_row_groups[i];
      auto parquet_fragment = checked_pointer_cast<ParquetFileFragment>(fragments[i]);

      EXPECT_EQ(parquet_fragment->row_groups(), std::vector<int>{expected});
      EXPECT_EQ(SingleBatch(parquet_fragment.get())->num_rows(), expected + 1);
    }
  }

  void TestMultithreadedRegression(CustomizeScanOptionsWithThreadPool customizer) {
    auto reader = MakeGeneratedRecordBatch(schema({field("utf8", utf8())}), 10000, 100);
    ASSERT_OK_AND_ASSIGN(auto buffer, ParquetFormatHelper::Write(reader.get()));

    std::vector<Future<>> completes;
    std::vector<std::shared_ptr<arrow::internal::ThreadPool>> pools;

    for (int idx = 0; idx < 2; ++idx) {
      auto buffer_reader = std::make_shared<DelayedBufferReader>(buffer);
      auto source = std::make_shared<FileSource>(buffer_reader, buffer->size());
      auto fragment = MakeFragment(*source);
      std::shared_ptr<Scanner> scanner;

      {
        auto options = std::make_shared<ScanOptions>();
        ASSERT_OK_AND_ASSIGN(auto thread_pool, arrow::internal::ThreadPool::Make(1));
        pools.emplace_back(thread_pool);
        customizer(*options, pools.back().get());
        auto fragment_scan_options = std::make_shared<ParquetFragmentScanOptions>();
        fragment_scan_options->arrow_reader_properties->set_pre_buffer(true);

        options->fragment_scan_options = fragment_scan_options;
        ScannerBuilder builder(ArithmeticDatasetFixture::schema(), fragment, options);

        ASSERT_OK(builder.UseThreads(true));
        ASSERT_OK(builder.BatchSize(10000));
        ASSERT_OK_AND_ASSIGN(scanner, builder.Finish());
      }

      ASSERT_OK_AND_ASSIGN(auto batch, scanner->Head(10000));
      [[maybe_unused]] auto fut = scanner->ScanBatchesUnorderedAsync();
      // Random ReadAsync calls, generate some futures to make the state machine
      // more complex.
      for (int yy = 0; yy < 16; yy++) {
        completes.emplace_back(
            buffer_reader->ReadAsync(::arrow::io::IOContext(), 0, 1001));
      }
      scanner = nullptr;
    }

    for (auto& f : completes) {
      f.Wait();
    }
  }
};

TEST_F(TestParquetFileFormat, InspectFailureWithRelevantError) {
  TestInspectFailureWithRelevantError(StatusCode::Invalid, "Parquet");
}
TEST_F(TestParquetFileFormat, Inspect) { TestInspect(); }

TEST_F(TestParquetFileFormat, InspectDictEncoded) {
  auto reader = GetRecordBatchReader(schema({field("utf8", utf8())}));
  auto source = GetFileSource(reader.get());

  format_->reader_options.dict_columns = {"utf8"};
  ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));

  Schema expected_schema({field("utf8", dictionary(int32(), utf8()))});
  AssertSchemaEqual(*actual, expected_schema, /* check_metadata = */ false);
}

TEST_F(TestParquetFileFormat, IsSupported) { TestIsSupported(); }

TEST_F(TestParquetFileFormat, WriteRecordBatchReader) { TestWrite(); }

TEST_F(TestParquetFileFormat, WriteRecordBatchReaderCustomOptions) {
  TimeUnit::type coerce_timestamps_to = TimeUnit::MICRO,
                 coerce_timestamps_from = TimeUnit::NANO;

  auto reader =
      GetRecordBatchReader(schema({field("ts", timestamp(coerce_timestamps_from))}));
  auto options =
      checked_pointer_cast<ParquetFileWriteOptions>(format_->DefaultWriteOptions());
  options->writer_properties = parquet::WriterProperties::Builder()
                                   .created_by("TestParquetFileFormat")
                                   ->disable_statistics()
                                   ->build();
  options->arrow_writer_properties = parquet::ArrowWriterProperties::Builder()
                                         .coerce_timestamps(coerce_timestamps_to)
                                         ->allow_truncated_timestamps()
                                         ->build();

  auto written = WriteToBuffer(reader->schema(), options);

  EXPECT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(FileSource{written}));
  EXPECT_OK_AND_ASSIGN(auto actual_schema, fragment->ReadPhysicalSchema());
  AssertSchemaEqual(Schema({field("ts", timestamp(coerce_timestamps_to))}),
                    *actual_schema);
}

TEST_F(TestParquetFileFormat, CountRows) { TestCountRows(); }

TEST_F(TestParquetFileFormat, FragmentEquals) { TestFragmentEquals(); }

TEST_F(TestParquetFileFormat, CountRowsPredicatePushdown) {
  constexpr int64_t kNumRowGroups = 16;
  constexpr int64_t kTotalNumRows = kNumRowGroups * (kNumRowGroups + 1) / 2;

  // See PredicatePushdown test below for a description of the generated data
  auto reader = ArithmeticDatasetFixture::GetRecordBatchReader(kNumRowGroups);
  auto source = GetFileSource(reader.get());
  auto options = std::make_shared<ScanOptions>();

  auto fragment = MakeFragment(*source);

  ASSERT_FINISHES_OK_AND_EQ(std::make_optional<int64_t>(kTotalNumRows),
                            fragment->CountRows(literal(true), options));

  for (int i = 1; i <= kNumRowGroups; i++) {
    SCOPED_TRACE(i);
    // The row group for which all values in column i64 == i has i rows
    auto predicate = less_equal(field_ref("i64"), literal(i));
    ASSERT_OK_AND_ASSIGN(predicate, predicate.Bind(*reader->schema()));
    auto expected = i * (i + 1) / 2;
    ASSERT_FINISHES_OK_AND_EQ(std::make_optional<int64_t>(expected),
                              fragment->CountRows(predicate, options));

    predicate = and_(less_equal(field_ref("i64"), literal(i)),
                     greater_equal(field_ref("i64"), literal(i)));
    ASSERT_OK_AND_ASSIGN(predicate, predicate.Bind(*reader->schema()));
    ASSERT_FINISHES_OK_AND_EQ(std::make_optional<int64_t>(i),
                              fragment->CountRows(predicate, options));

    predicate = equal(field_ref("i64"), literal(i));
    ASSERT_OK_AND_ASSIGN(predicate, predicate.Bind(*reader->schema()));
    ASSERT_FINISHES_OK_AND_EQ(std::make_optional<int64_t>(i),
                              fragment->CountRows(predicate, options));
  }

  // Ensure nulls are properly handled
  {
    auto dataset_schema = schema({field("i64", int64())});
    auto null_batch = RecordBatchFromJSON(dataset_schema, R"([
[null],
[null],
[null]
])");
    auto batch = RecordBatchFromJSON(dataset_schema, R"([
[1],
[2]
])");
    auto batch2 = RecordBatchFromJSON(dataset_schema, R"([
[4],
[4]
])");
    ASSERT_OK_AND_ASSIGN(auto reader, RecordBatchReader::Make({null_batch, batch, batch2},
                                                              dataset_schema));
    auto source = GetFileSource(reader.get());
    auto fragment = MakeFragment(*source);
    ASSERT_OK_AND_ASSIGN(
        auto predicate,
        greater_equal(field_ref("i64"), literal(1)).Bind(*dataset_schema));
    ASSERT_FINISHES_OK_AND_EQ(std::make_optional<int64_t>(4),
                              fragment->CountRows(predicate, options));

    ASSERT_OK_AND_ASSIGN(predicate, is_null(field_ref("i64")).Bind(*dataset_schema));
    ASSERT_FINISHES_OK_AND_EQ(std::make_optional<int64_t>(3),
                              fragment->CountRows(predicate, options));

    ASSERT_OK_AND_ASSIGN(predicate, is_valid(field_ref("i64")).Bind(*dataset_schema));
    ASSERT_FINISHES_OK_AND_EQ(std::make_optional<int64_t>(4),
                              fragment->CountRows(predicate, options));
  }
}

TEST_F(TestParquetFileFormat, CachedMetadata) {
  // Create a test file
  auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
  std::shared_ptr<Schema> test_schema = schema({field("x", int32())});
  std::shared_ptr<RecordBatch> batch = RecordBatchFromJSON(test_schema, "[[0]]");
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<io::OutputStream> out_stream,
                       mock_fs->OpenOutputStream("/foo.parquet"));
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<FileWriter> writer,
      format_->MakeWriter(out_stream, test_schema, format_->DefaultWriteOptions(),
                          {mock_fs, "/foo.parquet"}));
  ASSERT_OK(writer->Write(batch));
  ASSERT_FINISHES_OK(writer->Finish());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<io::RandomAccessFile> test_file,
                       mock_fs->OpenInputFile("/foo.parquet"));
  std::shared_ptr<io::TrackedRandomAccessFile> tracked_input =
      io::TrackedRandomAccessFile::Make(test_file.get());

  FileSource source(tracked_input);
  ASSERT_OK_AND_ASSIGN(auto fragment,
                       format_->MakeFragment(std::move(source), literal(true)));
  auto pq_fragment = checked_cast<ParquetFileFragment*>(fragment.get());

  // Read the file the first time, will read metadata
  auto options = std::make_shared<ScanOptions>();
  options->filter = literal(true);
  ASSERT_OK_AND_ASSIGN(
      auto projection_descr,
      ProjectionDescr::FromNames({"x"}, *test_schema, options->add_augmented_fields));
  options->projected_schema = projection_descr.schema;
  options->projection = projection_descr.expression;
  ASSERT_OK_AND_ASSIGN(auto generator, fragment->ScanBatchesAsync(options));
  ASSERT_FINISHES_OK(CollectAsyncGenerator(std::move(generator)));
  ASSERT_NE(nullptr, pq_fragment->metadata());

  ASSERT_GT(tracked_input->bytes_read(), 0);
  int64_t bytes_read_first_time = tracked_input->bytes_read();
  ASSERT_OK(tracked_input->Seek(0));

  // Read the file the second time, should not read metadata
  tracked_input->ResetStats();
  ASSERT_OK_AND_ASSIGN(generator, fragment->ScanBatchesAsync(options));
  ASSERT_FINISHES_OK(CollectAsyncGenerator(std::move(generator)));
  ASSERT_LT(tracked_input->bytes_read(), bytes_read_first_time);

  // Clear cached metadata
  ASSERT_OK(fragment->ClearCachedMetadata());
  ASSERT_EQ(nullptr, pq_fragment->metadata());

  // Read the file a third time, should read metadata
  tracked_input->ResetStats();
  ASSERT_OK_AND_ASSIGN(generator, fragment->ScanBatchesAsync(options));
  ASSERT_FINISHES_OK(CollectAsyncGenerator(std::move(generator)));
  ASSERT_EQ(tracked_input->bytes_read(), bytes_read_first_time);
  ASSERT_NE(nullptr, pq_fragment->metadata());
}

TEST_F(TestParquetFileFormat, MultithreadedScan) {
  constexpr int64_t kNumRowGroups = 16;

  // See PredicatePushdown test below for a description of the generated data
  auto reader = ArithmeticDatasetFixture::GetRecordBatchReader(kNumRowGroups);
  auto source = GetFileSource(reader.get());
  auto options = std::make_shared<ScanOptions>();

  auto fragment = MakeFragment(*source);

  FragmentDataset dataset(ArithmeticDatasetFixture::schema(), {fragment});
  ScannerBuilder builder({&dataset, [](...) {}});

  ASSERT_OK(builder.UseThreads(true));
  ASSERT_OK(builder.Project({call("add", {field_ref("i64"), literal(3)})}, {""}));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());

  ASSERT_OK_AND_ASSIGN(auto gen, scanner->ScanBatchesUnorderedAsync());

  auto collect_fut = CollectAsyncGenerator(gen);
  ASSERT_OK_AND_ASSIGN(auto batches, collect_fut.result());

  ASSERT_EQ(batches.size(), kNumRowGroups);
}

TEST_F(TestParquetFileFormat, SingleThreadExecutor) {
  // Reset capacity for io executor
  struct PoolResetGuard {
    int original_capacity = io::GetIOThreadPoolCapacity();
    ~PoolResetGuard() { DCHECK_OK(io::SetIOThreadPoolCapacity(original_capacity)); }
  } guard;
  ASSERT_OK(io::SetIOThreadPoolCapacity(1));

  auto reader = GetRecordBatchReader(schema({field("utf8", utf8())}));

  ASSERT_OK_AND_ASSIGN(auto buffer, ParquetFormatHelper::Write(reader.get()));
  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto source = std::make_shared<FileSource>(std::move(buffer_reader), buffer->size());
  auto options = std::make_shared<ScanOptions>();

  {
    auto fragment = MakeFragment(*source);
    auto count_rows = fragment->CountRows(literal(true), options);
    ASSERT_OK_AND_ASSIGN(auto result, count_rows.MoveResult());
    ASSERT_EQ(expected_rows(), result);
  }
}

class TestParquetFileSystemDataset : public WriteFileSystemDatasetMixin,
                                     public testing::Test {
 public:
  void SetUp() override {
    MakeSourceDataset();
    check_metadata_ = false;
    auto parquet_format = std::make_shared<ParquetFileFormat>();
    format_ = parquet_format;
    SetWriteOptions(parquet_format->DefaultWriteOptions());
  }
};

TEST_F(TestParquetFileSystemDataset, WriteWithIdenticalPartitioningSchema) {
  TestWriteWithIdenticalPartitioningSchema();
}

TEST_F(TestParquetFileSystemDataset, WriteWithUnrelatedPartitioningSchema) {
  TestWriteWithUnrelatedPartitioningSchema();
}

TEST_F(TestParquetFileSystemDataset, WriteWithSupersetPartitioningSchema) {
  TestWriteWithSupersetPartitioningSchema();
}

TEST_F(TestParquetFileSystemDataset, WriteWithEmptyPartitioningSchema) {
  TestWriteWithEmptyPartitioningSchema();
}

TEST_F(TestParquetFileSystemDataset, WriteWithEncryptionConfigNotSupported) {
#ifndef PARQUET_REQUIRE_ENCRYPTION
  // Create a dummy ParquetEncryptionConfig
  std::shared_ptr<ParquetEncryptionConfig> encryption_config =
      std::make_shared<ParquetEncryptionConfig>();

  auto options =
      checked_pointer_cast<ParquetFileWriteOptions>(format_->DefaultWriteOptions());

  // Set the encryption config in the options
  options->parquet_encryption_config = encryption_config;

  // Setup mock filesystem and test data
  auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
  std::shared_ptr<Schema> test_schema = schema({field("x", int32())});
  std::shared_ptr<RecordBatch> batch = RecordBatchFromJSON(test_schema, "[[0]]");
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<io::OutputStream> out_stream,
                       mock_fs->OpenOutputStream("/foo.parquet"));
  // Try to create a writer with the encryption config
  auto result =
      format_->MakeWriter(out_stream, test_schema, options, {mock_fs, "/foo.parquet"});
  // Expect an error if encryption is not supported in the build
  EXPECT_TRUE(result.status().IsNotImplemented());
#endif
}

class TestParquetFileFormatScan : public FileFormatScanMixin<ParquetFormatHelper> {
 public:
  std::shared_ptr<RecordBatch> SingleBatch(std::shared_ptr<Fragment> fragment) {
    auto batches = IteratorToVector(PhysicalBatches(fragment));
    EXPECT_EQ(batches.size(), 1);
    return batches.front();
  }

  void CountRowsAndBatchesInScan(std::shared_ptr<Fragment> fragment,
                                 int64_t expected_rows, int64_t expected_batches) {
    int64_t actual_rows = 0;
    int64_t actual_batches = 0;

    for (auto maybe_batch : PhysicalBatches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      actual_rows += batch->num_rows();
      ++actual_batches;
    }

    EXPECT_EQ(actual_rows, expected_rows);
    EXPECT_EQ(actual_batches, expected_batches);
  }

  void CountRowGroupsInFragment(const std::shared_ptr<Fragment>& fragment,
                                std::vector<int> expected_row_groups,
                                compute::Expression filter) {
    SetFilter(filter);

    auto parquet_fragment = checked_pointer_cast<ParquetFileFragment>(fragment);
    ASSERT_OK_AND_ASSIGN(auto fragments, parquet_fragment->SplitByRowGroup(opts_->filter))

    EXPECT_EQ(fragments.size(), expected_row_groups.size());
    for (size_t i = 0; i < fragments.size(); i++) {
      auto expected = expected_row_groups[i];
      auto parquet_fragment = checked_pointer_cast<ParquetFileFragment>(fragments[i]);

      EXPECT_EQ(parquet_fragment->row_groups(), std::vector<int>{expected});
      EXPECT_EQ(SingleBatch(parquet_fragment)->num_rows(), expected + 1);
    }
  }
};

TEST_P(TestParquetFileFormatScan, ScanRecordBatchReader) { TestScan(); }
TEST_P(TestParquetFileFormatScan, ScanBatchSize) { TestScanBatchSize(); }
TEST_P(TestParquetFileFormatScan, ScanNoReadahead) { TestScanNoReadahead(); }
TEST_P(TestParquetFileFormatScan, ScanRecordBatchReaderProjected) { TestScanProjected(); }
TEST_P(TestParquetFileFormatScan, ScanRecordBatchReaderProjectedNested) {
  // TODO(ARROW-1888): enable fine-grained column projection.
  TestScanProjectedNested(/*fine_grained_selection=*/false);
}
TEST_P(TestParquetFileFormatScan, ScanRecordBatchReaderProjectedMissingCols) {
  TestScanProjectedMissingCols();
}
TEST_P(TestParquetFileFormatScan, ScanRecordBatchReaderWithVirtualColumn) {
  TestScanWithVirtualColumn();
}
TEST_P(TestParquetFileFormatScan, ScanRecordBatchReaderWithDuplicateColumn) {
  TestScanWithDuplicateColumn();
}
TEST_P(TestParquetFileFormatScan, ScanRecordBatchReaderWithDuplicateColumnError) {
  TestScanWithDuplicateColumnError();
}
TEST_P(TestParquetFileFormatScan, ScanWithPushdownNulls) { TestScanWithPushdownNulls(); }
TEST_P(TestParquetFileFormatScan, ScanRecordBatchReaderDictEncoded) {
  auto reader = GetRecordBatchReader(schema({field("utf8", utf8())}));
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  SetFilter(literal(true));
  format_->reader_options.dict_columns = {"utf8"};
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  int64_t row_count = 0;
  Schema expected_schema({field("utf8", dictionary(int32(), utf8()))});

  for (auto maybe_batch : PhysicalBatches(fragment)) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    row_count += batch->num_rows();
    AssertSchemaEqual(*batch->schema(), expected_schema, /* check_metadata = */ false);
  }
  ASSERT_EQ(row_count, expected_rows());
}
TEST_P(TestParquetFileFormatScan, ScanRecordBatchReaderPreBuffer) {
  auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  SetFilter(literal(true));

  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));
  auto fragment_scan_options = std::make_shared<ParquetFragmentScanOptions>();
  fragment_scan_options->arrow_reader_properties->set_pre_buffer(true);
  opts_->fragment_scan_options = fragment_scan_options;

  int64_t row_count = 0;
  for (auto maybe_batch : PhysicalBatches(fragment)) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    row_count += batch->num_rows();
  }
  ASSERT_EQ(row_count, expected_rows());
}
TEST_P(TestParquetFileFormatScan, PredicatePushdown) {
  // Given a number `n`, the arithmetic dataset creates n RecordBatches where
  // each RecordBatch is keyed by a unique integer in [1, n]. Let `rb_i` denote
  // the record batch keyed by `i`. `rb_i` is composed of `i` rows where all
  // values are a variant of `i`, e.g. {"i64": i, "u8": i, ... }.
  //
  // Thus the ArithmeticDataset(n) has n RecordBatches and the total number of
  // rows is n(n+1)/2.
  //
  // This test uses the Fragment directly, and so no post-filtering is
  // applied via ScanOptions' evaluator. Thus, counting the number of returned
  // rows and returned row groups is a good enough proxy to check if pushdown
  // predicate is working.

  constexpr int64_t kNumRowGroups = 16;
  constexpr int64_t kTotalNumRows = kNumRowGroups * (kNumRowGroups + 1) / 2;

  auto reader = ArithmeticDatasetFixture::GetRecordBatchReader(kNumRowGroups);
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  SetFilter(literal(true));
  CountRowsAndBatchesInScan(fragment, kTotalNumRows, kNumRowGroups);

  for (int64_t i = 1; i <= kNumRowGroups; i++) {
    SetFilter(equal(field_ref("i64"), literal(i)));
    CountRowsAndBatchesInScan(fragment, i, 1);
  }

  // Out of bound filters should skip all RowGroups.
  SetFilter(literal(false));
  CountRowsAndBatchesInScan(fragment, 0, 0);
  SetFilter(equal(field_ref("i64"), literal<int64_t>(kNumRowGroups + 1)));
  CountRowsAndBatchesInScan(fragment, 0, 0);
  SetFilter(equal(field_ref("i64"), literal<int64_t>(-1)));
  CountRowsAndBatchesInScan(fragment, 0, 0);
  // No rows match 1 and 2.
  SetFilter(and_(equal(field_ref("i64"), literal<int64_t>(1)),
                 equal(field_ref("u8"), literal<uint8_t>(2))));
  CountRowsAndBatchesInScan(fragment, 0, 0);

  SetFilter(or_(equal(field_ref("i64"), literal<int64_t>(2)),
                equal(field_ref("i64"), literal<int64_t>(4))));
  CountRowsAndBatchesInScan(fragment, 2 + 4, 2);

  SetFilter(less(field_ref("i64"), literal<int64_t>(6)));
  CountRowsAndBatchesInScan(fragment, 5 * (5 + 1) / 2, 5);

  SetFilter(greater_equal(field_ref("i64"), literal<int64_t>(6)));
  CountRowsAndBatchesInScan(fragment, kTotalNumRows - (5 * (5 + 1) / 2),
                            kNumRowGroups - 5);
}

TEST_P(TestParquetFileFormatScan, PredicatePushdownRowGroupFragments) {
  constexpr int64_t kNumRowGroups = 16;

  auto reader = ArithmeticDatasetFixture::GetRecordBatchReader(kNumRowGroups);
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  auto all_row_groups = ::arrow::internal::Iota(static_cast<int>(kNumRowGroups));
  CountRowGroupsInFragment(fragment, all_row_groups, literal(true));

  for (int i = 0; i < kNumRowGroups; ++i) {
    CountRowGroupsInFragment(fragment, {i}, equal(field_ref("i64"), literal(i + 1)));
  }

  // Out of bound filters should skip all RowGroups.
  CountRowGroupsInFragment(fragment, {}, literal(false));
  CountRowGroupsInFragment(fragment, {},
                           equal(field_ref("i64"), literal(kNumRowGroups + 1)));
  CountRowGroupsInFragment(fragment, {}, equal(field_ref("i64"), literal(-1)));

  // No rows match 1 and 2.
  CountRowGroupsInFragment(
      fragment, {},
      and_(equal(field_ref("i64"), literal(1)), equal(field_ref("u8"), literal(2))));
  CountRowGroupsInFragment(
      fragment, {},
      and_(equal(field_ref("i64"), literal(2)), equal(field_ref("i64"), literal(4))));

  CountRowGroupsInFragment(
      fragment, {1, 3},
      or_(equal(field_ref("i64"), literal(2)), equal(field_ref("i64"), literal(4))));

  auto set = ArrayFromJSON(int64(), "[2, 4]");
  CountRowGroupsInFragment(
      fragment, {1, 3},
      call("is_in", {field_ref("i64")}, compute::SetLookupOptions{set}));

  CountRowGroupsInFragment(fragment, {0, 1, 2, 3, 4}, less(field_ref("i64"), literal(6)));

  CountRowGroupsInFragment(fragment,
                           ::arrow::internal::Iota(5, static_cast<int>(kNumRowGroups)),
                           greater_equal(field_ref("i64"), literal(6)));

  CountRowGroupsInFragment(fragment, {5, 6},
                           and_(greater_equal(field_ref("i64"), literal(6)),
                                less(field_ref("i64"), literal(8))));

  // nested field reference
  CountRowGroupsInFragment(fragment, {0, 1, 2, 3, 4},
                           less(field_ref(FieldRef("struct", "i32")), literal(6)));
  CountRowGroupsInFragment(fragment, {1},
                           equal(field_ref(FieldRef("struct", "str")), literal("2")));
}

TEST_P(TestParquetFileFormatScan, ExplicitRowGroupSelection) {
  constexpr int64_t kNumRowGroups = 16;
  constexpr int64_t kTotalNumRows = kNumRowGroups * (kNumRowGroups + 1) / 2;

  auto reader = ArithmeticDatasetFixture::GetRecordBatchReader(kNumRowGroups);
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  SetFilter(literal(true));

  auto row_groups_fragment = [&](std::vector<int> row_groups) {
    EXPECT_OK_AND_ASSIGN(auto fragment,
                         format_->MakeFragment(*source, literal(true),
                                               /*physical_schema=*/nullptr, row_groups));
    return fragment;
  };

  // select all row groups
  EXPECT_OK_AND_ASSIGN(auto all_row_groups_fragment,
                       format_->MakeFragment(*source, literal(true))
                           .Map([](std::shared_ptr<FileFragment> f) {
                             return checked_pointer_cast<ParquetFileFragment>(f);
                           }));

  EXPECT_EQ(all_row_groups_fragment->row_groups(), std::vector<int>{});

  ARROW_EXPECT_OK(all_row_groups_fragment->EnsureCompleteMetadata());
  CountRowsAndBatchesInScan(all_row_groups_fragment, kTotalNumRows, kNumRowGroups);

  // individual selection selects a single row group
  for (int i = 0; i < kNumRowGroups; ++i) {
    CountRowsAndBatchesInScan(row_groups_fragment({i}), i + 1, 1);
    EXPECT_EQ(row_groups_fragment({i})->row_groups(), std::vector<int>{i});
  }

  for (int i = 0; i < kNumRowGroups; ++i) {
    // conflicting selection/filter
    SetFilter(equal(field_ref("i64"), literal(i)));
    CountRowsAndBatchesInScan(row_groups_fragment({i}), 0, 0);
  }

  for (int i = 0; i < kNumRowGroups; ++i) {
    // identical selection/filter
    SetFilter(equal(field_ref("i64"), literal(i + 1)));
    CountRowsAndBatchesInScan(row_groups_fragment({i}), i + 1, 1);
  }

  SetFilter(greater(field_ref("i64"), literal(3)));
  CountRowsAndBatchesInScan(row_groups_fragment({2, 3, 4, 5}), 4 + 5 + 6, 3);

  ASSERT_OK_AND_ASSIGN(auto batch_gen,
                       row_groups_fragment({kNumRowGroups + 1})->ScanBatchesAsync(opts_));
  Status scan_status = CollectAsyncGenerator(batch_gen).status();

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      IndexError,
      testing::HasSubstr("only has " + std::to_string(kNumRowGroups) + " row groups"),
      scan_status);
}

TEST_P(TestParquetFileFormatScan, PredicatePushdownRowGroupFragmentsUsingStringColumn) {
  auto table = TableFromJSON(schema({field("x", utf8())}),
                             {
                                 R"([{"x": "a"}])",
                                 R"([{"x": "b"}, {"x": "b"}])",
                                 R"([{"x": "c"}, {"x": "c"}, {"x": "c"}])",
                                 R"([{"x": "a"}, {"x": "b"}, {"x": "c"}, {"x": "d"}])",
                             });
  TableBatchReader reader(*table);
  auto source = GetFileSource(&reader);

  SetSchema(reader.schema()->fields());
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  CountRowGroupsInFragment(fragment, {0, 3}, equal(field_ref("x"), literal("a")));
}

TEST_P(TestParquetFileFormatScan, PredicatePushdownRowGroupFragmentsUsingDurationColumn) {
  // GH-37111: Parquet arrow stores writer schema and possible field_id in
  // key_value_metadata when store_schema enabled. When storing `arrow::duration`, it will
  // be stored as int64. This test ensures that dataset can parse the writer schema
  // correctly.
  auto table = TableFromJSON(schema({field("t", duration(TimeUnit::NANO))}),
                             {
                                 R"([{"t": 1}])",
                                 R"([{"t": 2}, {"t": 3}])",
                             });
  TableBatchReader table_reader(*table);
  ASSERT_OK_AND_ASSIGN(
      auto buffer,
      ParquetFormatHelper::Write(
          &table_reader, ArrowWriterProperties::Builder().store_schema()->build()));
  auto source = std::make_shared<FileSource>(buffer);
  SetSchema({field("t", duration(TimeUnit::NANO))});
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  auto expr = equal(field_ref("t"), literal(::arrow::DurationScalar(1, TimeUnit::NANO)));
  CountRowGroupsInFragment(fragment, {0}, expr);
}

TEST_P(TestParquetFileFormatScan,
       PredicatePushdownRowGroupFragmentsUsingTimestampColumn) {
  // GH-37799: Parquet arrow will change TimeUnit::SECOND to TimeUnit::MILLI
  // because parquet LogicalType doesn't support SECOND.
  for (auto time_unit : {TimeUnit::MILLI, TimeUnit::SECOND}) {
    auto table = TableFromJSON(schema({field("t", time32(time_unit))}),
                               {
                                   R"([{"t": 1}])",
                                   R"([{"t": 2}, {"t": 3}])",
                               });
    TableBatchReader table_reader(*table);
    ARROW_SCOPED_TRACE("time_unit=", time_unit);
    ASSERT_OK_AND_ASSIGN(
        auto source,
        ParquetFormatHelper::Write(
            &table_reader, ArrowWriterProperties::Builder().store_schema()->build())
            .As<FileSource>());
    SetSchema({field("t", time32(time_unit))});
    ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(source));

    auto expr = equal(field_ref("t"), literal(::arrow::Time32Scalar(1, time_unit)));
    CountRowGroupsInFragment(fragment, {0}, expr);
  }
}

// Tests projection with nested/indexed FieldRefs.
// https://github.com/apache/arrow/issues/35579
TEST_P(TestParquetFileFormatScan, ProjectWithNonNamedFieldRefs) {
  auto table_schema = schema(
      {field("info", struct_({field("name", utf8()),
                              field("data", struct_({field("amount", float64()),
                                                     field("percent", float32())}))}))});
  auto table = TableFromJSON(table_schema, {R"([
    {"info": {"name": "a", "data": {"amount": 10.3, "percent": 0.1}}},
    {"info": {"name": "b", "data": {"amount": 11.6, "percent": 0.2}}},
    {"info": {"name": "c", "data": {"amount": 12.9, "percent": 0.3}}},
    {"info": {"name": "d", "data": {"amount": 14.2, "percent": 0.4}}},
    {"info": {"name": "e", "data": {"amount": 15.5, "percent": 0.5}}},
    {"info": {"name": "f", "data": {"amount": 16.8, "percent": 0.6}}}])"});
  ASSERT_OK_AND_ASSIGN(auto expected_batch, table->CombineChunksToBatch());

  TableBatchReader reader(*table);
  SetSchema(reader.schema()->fields());

  auto source = GetFileSource(&reader);
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  std::vector<FieldRef> equivalent_refs = {
      FieldRef("info", "data", "percent"), FieldRef("info", 1, 1),
      FieldRef(0, 1, "percent"),           FieldRef(0, 1, 1),
      FieldRef(0, FieldRef("data", 1)),    FieldRef(FieldRef(0), FieldRef(1, 1)),
  };
  for (const auto& ref : equivalent_refs) {
    ARROW_SCOPED_TRACE("ref = ", ref.ToString());

    Project({field_ref(ref)}, {"value"});
    auto batch = SingleBatch(fragment);
    AssertBatchesEqual(*expected_batch, *batch);
  }
}

INSTANTIATE_TEST_SUITE_P(TestScan, TestParquetFileFormatScan,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

TEST(TestParquetStatistics, NullMax) {
  auto field = ::arrow::field("x", float32());
  ASSERT_OK_AND_ASSIGN(std::string dir_string,
                       arrow::internal::GetEnvVar("PARQUET_TEST_DATA"));
  auto reader =
      parquet::ParquetFileReader::OpenFile(dir_string + "/nan_in_stats.parquet");
  auto statistics = reader->RowGroup(0)->metadata()->ColumnChunk(0)->statistics();
  auto stat_expression =
      ParquetFileFragment::EvaluateStatisticsAsExpression(*field, *statistics);
  EXPECT_EQ(stat_expression->ToString(), "(x >= 1)");
}

TEST(TestParquetStatistics, NoNullCount) {
  auto field = ::arrow::field("x", int32());
  auto parquet_node_ptr = ::parquet::schema::Int32("x", ::parquet::Repetition::REQUIRED);
  ::parquet::ColumnDescriptor descr(parquet_node_ptr, /*max_definition_level=*/1,
                                    /*max_repetition_level=*/0);

  auto int32_to_parquet_stats = [](int32_t v) {
    std::string value;
    auto le = ::arrow::bit_util::ToLittleEndian(v);
    value.resize(sizeof(le));
    memcpy(value.data(), &le, sizeof(le));
    return value;
  };
  {
    // Base case: when null_count is not set, the expression might contain null
    ::parquet::EncodedStatistics encoded_stats;
    encoded_stats.set_min(int32_to_parquet_stats(1));
    encoded_stats.set_max(int32_to_parquet_stats(100));
    encoded_stats.has_null_count = false;
    encoded_stats.all_null_value = false;
    encoded_stats.null_count = 0;
    auto stats = ::parquet::Statistics::Make(&descr, &encoded_stats, /*num_values=*/10);

    auto stat_expression =
        ParquetFileFragment::EvaluateStatisticsAsExpression(*field, *stats);
    ASSERT_TRUE(stat_expression.has_value());
    EXPECT_EQ(stat_expression->ToString(),
              "(((x >= 1) and (x <= 100)) or is_null(x, {nan_is_null=false}))");
  }
  {
    // Special case: when num_value is 0, it would return
    // "is_null".
    ::parquet::EncodedStatistics encoded_stats;
    encoded_stats.has_null_count = true;
    encoded_stats.null_count = 1;
    encoded_stats.all_null_value = true;
    auto stats = ::parquet::Statistics::Make(&descr, &encoded_stats, /*num_values=*/0);
    auto stat_expression =
        ParquetFileFragment::EvaluateStatisticsAsExpression(*field, *stats);
    ASSERT_TRUE(stat_expression.has_value());
    EXPECT_EQ(stat_expression->ToString(), "is_null(x, {nan_is_null=false})");

    encoded_stats.has_null_count = false;
    encoded_stats.all_null_value = false;
    stats = ::parquet::Statistics::Make(&descr, &encoded_stats, /*num_values=*/0);
    stat_expression = ParquetFileFragment::EvaluateStatisticsAsExpression(*field, *stats);
    ASSERT_TRUE(stat_expression.has_value());
    EXPECT_EQ(stat_expression->ToString(), "is_null(x, {nan_is_null=false})");
  }
}

TEST_F(TestParquetFileFormat, MultithreadedScanRegression) {
  // GH-38438: This test is similar to MultithreadedScan, but it try to use self
  // designed Executor and DelayedBufferReader to mock async execution to make
  // the state machine more complex.
  CustomizeScanOptionsWithThreadPool customize_io_context =
      [](ScanOptions& options, arrow::internal::ThreadPool* pool) {
        options.io_context = ::arrow::io::IOContext(::arrow::default_memory_pool(), pool);
      };
  TestMultithreadedRegression(customize_io_context);
}

TEST_F(TestParquetFileFormat, MultithreadedComputeRegression) {
  // GH-43694: Test similar situation as MultithreadedScanRegression but with
  // the customized CPU executor instead
  CustomizeScanOptionsWithThreadPool customize_cpu_executor =
      [](ScanOptions& options, arrow::internal::ThreadPool* pool) {
        options.cpu_executor = pool;
      };
  TestMultithreadedRegression(customize_cpu_executor);
}

}  // namespace dataset
}  // namespace arrow

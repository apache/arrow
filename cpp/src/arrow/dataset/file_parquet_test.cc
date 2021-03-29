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

#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/dataset/test_util.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/range.h"

#include "parquet/arrow/writer.h"
#include "parquet/metadata.h"

namespace arrow {
namespace dataset {

constexpr int64_t kBatchSize = 1UL << 12;
constexpr int64_t kBatchRepetitions = 1 << 5;
constexpr int64_t kNumRows = kBatchSize * kBatchRepetitions;

using parquet::ArrowWriterProperties;
using parquet::default_arrow_writer_properties;

using parquet::default_writer_properties;
using parquet::WriterProperties;

using parquet::CreateOutputStream;
using parquet::arrow::WriteTable;

using testing::Pointee;

using internal::checked_pointer_cast;

class ArrowParquetWriterMixin : public ::testing::Test {
 public:
  Status WriteRecordBatch(const RecordBatch& batch, parquet::arrow::FileWriter* writer) {
    auto schema = batch.schema();
    auto size = batch.num_rows();

    if (!schema->Equals(*writer->schema(), false)) {
      return Status::Invalid("RecordBatch schema does not match this writer's. batch:'",
                             schema->ToString(), "' this:'", writer->schema()->ToString(),
                             "'");
    }

    RETURN_NOT_OK(writer->NewRowGroup(size));
    for (int i = 0; i < batch.num_columns(); i++) {
      RETURN_NOT_OK(writer->WriteColumnChunk(*batch.column(i)));
    }

    return Status::OK();
  }

  Status WriteRecordBatchReader(RecordBatchReader* reader,
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

  Status WriteRecordBatchReader(
      RecordBatchReader* reader, MemoryPool* pool,
      const std::shared_ptr<io::OutputStream>& sink,
      const std::shared_ptr<WriterProperties>& properties = default_writer_properties(),
      const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
          default_arrow_writer_properties()) {
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    RETURN_NOT_OK(parquet::arrow::FileWriter::Open(
        *reader->schema(), pool, sink, properties, arrow_properties, &writer));
    RETURN_NOT_OK(WriteRecordBatchReader(reader, writer.get()));
    return writer->Close();
  }

  std::shared_ptr<Buffer> Write(RecordBatchReader* reader) {
    auto pool = ::arrow::default_memory_pool();

    std::shared_ptr<Buffer> out;

    auto sink = CreateOutputStream(pool);

    ARROW_EXPECT_OK(WriteRecordBatchReader(reader, pool, sink));
    // XXX the rest of the test may crash if this fails, since out will be nullptr
    EXPECT_OK_AND_ASSIGN(out, sink->Finish());

    return out;
  }

  std::shared_ptr<Buffer> Write(const Table& table) {
    auto pool = ::arrow::default_memory_pool();

    std::shared_ptr<Buffer> out;
    auto sink = CreateOutputStream(pool);

    ARROW_EXPECT_OK(WriteTable(table, pool, sink, 1U << 16));
    // XXX the rest of the test may crash if this fails, since out will be nullptr
    EXPECT_OK_AND_ASSIGN(out, sink->Finish());
    return out;
  }
};

class TestParquetFileFormat : public ArrowParquetWriterMixin {
 public:
  std::unique_ptr<FileSource> GetFileSource(RecordBatchReader* reader) {
    auto buffer = Write(reader);
    return internal::make_unique<FileSource>(std::move(buffer));
  }

  std::unique_ptr<RecordBatchReader> GetRecordBatchReader(
      std::shared_ptr<Schema> schema) {
    return MakeGeneratedRecordBatch(schema, kBatchSize, kBatchRepetitions);
  }

  Result<std::shared_ptr<io::BufferOutputStream>> GetFileSink() {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ResizableBuffer> buffer,
                          AllocateResizableBuffer(0));
    return std::make_shared<io::BufferOutputStream>(buffer);
  }

  RecordBatchIterator Batches(ScanTaskIterator scan_task_it) {
    return MakeFlattenIterator(MakeMaybeMapIterator(
        [](std::shared_ptr<ScanTask> scan_task) { return scan_task->Execute(); },
        std::move(scan_task_it)));
  }

  RecordBatchIterator Batches(Fragment* fragment) {
    EXPECT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(opts_));
    return Batches(std::move(scan_task_it));
  }

  void SetFilter(Expression filter) {
    ASSERT_OK_AND_ASSIGN(opts_->filter, filter.Bind(*opts_->dataset_schema));
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
                                std::vector<int> expected_row_groups, Expression filter) {
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

  void SetSchema(std::vector<std::shared_ptr<Field>> fields) {
    opts_ = std::make_shared<ScanOptions>();
    opts_->dataset_schema = schema(std::move(fields));
    ASSERT_OK(SetProjection(opts_.get(), opts_->dataset_schema->field_names()));
  }

 protected:
  std::shared_ptr<ParquetFileFormat> format_ = std::make_shared<ParquetFileFormat>();
  std::shared_ptr<ScanOptions> opts_;
};

TEST_F(TestParquetFileFormat, ScanRecordBatchReader) {
  auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  SetFilter(literal(true));
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    row_count += batch->num_rows();
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestParquetFileFormat, ScanRecordBatchReaderDictEncoded) {
  auto reader = GetRecordBatchReader(schema({field("utf8", utf8())}));
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  SetFilter(literal(true));

  format_->reader_options.dict_columns = {"utf8"};
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  ASSERT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(opts_));
  int64_t row_count = 0;

  Schema expected_schema({field("utf8", dictionary(int32(), utf8()))});

  for (auto maybe_task : scan_task_it) {
    ASSERT_OK_AND_ASSIGN(auto task, maybe_task);
    ASSERT_OK_AND_ASSIGN(auto rb_it, task->Execute());
    for (auto maybe_batch : rb_it) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      row_count += batch->num_rows();
      AssertSchemaEqual(*batch->schema(), expected_schema, /* check_metadata = */ false);
    }
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestParquetFileFormat, ScanRecordBatchReaderPreBuffer) {
  auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  SetFilter(literal(true));

  format_->reader_options.pre_buffer = true;
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));
  ASSERT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(opts_));

  int64_t task_count = 0;
  int64_t row_count = 0;

  for (auto maybe_task : scan_task_it) {
    ASSERT_OK_AND_ASSIGN(auto task, maybe_task);
    task_count += 1;
    ASSERT_OK_AND_ASSIGN(auto rb_it, task->Execute());
    for (auto maybe_batch : rb_it) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      row_count += batch->num_rows();
    }
  }

  ASSERT_EQ(task_count, kBatchRepetitions);
  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestParquetFileFormat, OpenFailureWithRelevantError) {
  std::shared_ptr<Buffer> buf = std::make_shared<Buffer>(util::string_view(""));
  auto result = format_->Inspect(FileSource(buf));
  EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, testing::HasSubstr("<Buffer>"),
                                  result.status());

  constexpr auto file_name = "herp/derp";
  ASSERT_OK_AND_ASSIGN(
      auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {fs::File(file_name)}));
  result = format_->Inspect({file_name, fs});
  EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, testing::HasSubstr(file_name),
                                  result.status());
}

static auto f32 = field("f32", float32());
static auto f64 = field("f64", float64());
static auto i32 = field("i32", int32());
static auto i64 = field("i64", int64());

TEST_F(TestParquetFileFormat, ScanRecordBatchReaderProjected) {
  SetSchema({f64, i64, f32, i32});
  ASSERT_OK(SetProjection(opts_.get(), {"f64"}));
  SetFilter(equal(field_ref("i32"), literal(0)));

  // NB: projection is applied by the scanner; FileFragment does not evaluate it so
  // we will not drop "i32" even though it is not projected since we need it for
  // filtering
  auto expected_schema = schema({f64, i32});

  auto reader = GetRecordBatchReader(opts_->dataset_schema);
  auto source = GetFileSource(reader.get());
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    row_count += batch->num_rows();
    AssertSchemaEqual(*batch->schema(), *expected_schema,
                      /*check_metadata=*/false);
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestParquetFileFormat, ScanRecordBatchReaderProjectedMissingCols) {
  SetSchema({f64, i64, f32, i32});
  ASSERT_OK(SetProjection(opts_.get(), {"f64"}));
  SetFilter(equal(field_ref("i32"), literal(0)));

  auto reader_without_i32 = GetRecordBatchReader(schema({f64, i64, f32}));
  auto reader_without_f64 = GetRecordBatchReader(schema({i64, f32, i32}));
  auto reader = GetRecordBatchReader(schema({f64, i64, f32, i32}));

  auto readers = {reader.get(), reader_without_i32.get(), reader_without_f64.get()};
  for (auto reader : readers) {
    auto source = GetFileSource(reader);
    ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

    // NB: projection is applied by the scanner; FileFragment does not evaluate it so
    // we will not drop "i32" even though it is not projected since we need it for
    // filtering
    //
    // in the case where a file doesn't contain a referenced field, we won't
    // materialize it as nulls later
    std::shared_ptr<Schema> expected_schema;
    if (reader == reader_without_i32.get()) {
      expected_schema = schema({f64});
    } else if (reader == reader_without_f64.get()) {
      expected_schema = schema({i32});
    } else {
      expected_schema = schema({f64, i32});
    }

    int64_t row_count = 0;

    for (auto maybe_batch : Batches(fragment.get())) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      row_count += batch->num_rows();
      AssertSchemaEqual(*batch->schema(), *expected_schema,
                        /*check_metadata=*/false);
    }

    ASSERT_EQ(row_count, kNumRows);
  }
}

TEST_F(TestParquetFileFormat, Inspect) {
  auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
  auto source = GetFileSource(reader.get());

  ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));
  AssertSchemaEqual(*actual, *reader->schema(), /*check_metadata=*/false);
}

TEST_F(TestParquetFileFormat, InspectDictEncoded) {
  auto reader = GetRecordBatchReader(schema({field("utf8", utf8())}));
  auto source = GetFileSource(reader.get());

  format_->reader_options.dict_columns = {"utf8"};
  ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));

  Schema expected_schema({field("utf8", dictionary(int32(), utf8()))});
  AssertSchemaEqual(*actual, expected_schema, /* check_metadata = */ false);
}

TEST_F(TestParquetFileFormat, IsSupported) {
  auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
  auto source = GetFileSource(reader.get());

  bool supported = false;

  std::shared_ptr<Buffer> buf = std::make_shared<Buffer>(util::string_view(""));
  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(FileSource(buf)));
  ASSERT_EQ(supported, false);

  buf = std::make_shared<Buffer>(util::string_view("corrupted"));
  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(FileSource(buf)));
  ASSERT_EQ(supported, false);

  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(*source));
  EXPECT_EQ(supported, true);
}

TEST_F(TestParquetFileFormat, PredicatePushdown) {
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

TEST_F(TestParquetFileFormat, PredicatePushdownRowGroupFragments) {
  constexpr int64_t kNumRowGroups = 16;

  auto reader = ArithmeticDatasetFixture::GetRecordBatchReader(kNumRowGroups);
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  auto all_row_groups = internal::Iota(static_cast<int>(kNumRowGroups));
  CountRowGroupsInFragment(fragment, all_row_groups, literal(true));

  // FIXME this is only meaningful if "not here" is a virtual column
  // CountRowGroupsInFragment(fragment, all_row_groups, "not here"_ == 0);

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

  // TODO(bkietz): better Assume support for InExpression
  // auto set = ArrayFromJSON(int64(), "[2, 4]");
  // CountRowGroupsInFragment(fragment, {1, 3}, field_ref("i64").In(set));

  CountRowGroupsInFragment(fragment, {0, 1, 2, 3, 4}, less(field_ref("i64"), literal(6)));

  CountRowGroupsInFragment(fragment, internal::Iota(5, static_cast<int>(kNumRowGroups)),
                           greater_equal(field_ref("i64"), literal(6)));

  CountRowGroupsInFragment(fragment, {5, 6},
                           and_(greater_equal(field_ref("i64"), literal(6)),
                                less(field_ref("i64"), literal(8))));
}

TEST_F(TestParquetFileFormat, PredicatePushdownRowGroupFragmentsUsingStringColumn) {
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

TEST_F(TestParquetFileFormat, ExplicitRowGroupSelection) {
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
  EXPECT_OK_AND_ASSIGN(
      auto all_row_groups_fragment,
      format_->MakeFragment(*source, literal(true))
          .Map([](std::shared_ptr<FileFragment> f) {
            return internal::checked_pointer_cast<ParquetFileFragment>(f);
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

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      IndexError,
      testing::HasSubstr("only has " + std::to_string(kNumRowGroups) + " row groups"),
      row_groups_fragment({kNumRowGroups + 1})->Scan(opts_));
}

TEST_F(TestParquetFileFormat, WriteRecordBatchReader) {
  std::shared_ptr<RecordBatchReader> reader =
      GetRecordBatchReader(schema({field("f64", float64())}));
  auto source = GetFileSource(reader.get());
  reader = GetRecordBatchReader(schema({field("f64", float64())}));

  SetSchema(reader->schema()->fields());

  EXPECT_OK_AND_ASSIGN(auto sink, GetFileSink());

  auto options = format_->DefaultWriteOptions();
  EXPECT_OK_AND_ASSIGN(auto writer, format_->MakeWriter(sink, reader->schema(), options));
  ASSERT_OK(writer->Write(reader.get()));
  ASSERT_OK(writer->Finish());

  EXPECT_OK_AND_ASSIGN(auto written, sink->Finish());

  AssertBufferEqual(*written, *source->buffer());
}

TEST_F(TestParquetFileFormat, WriteRecordBatchReaderCustomOptions) {
  TimeUnit::type coerce_timestamps_to = TimeUnit::MICRO,
                 coerce_timestamps_from = TimeUnit::NANO;

  std::shared_ptr<RecordBatchReader> reader =
      GetRecordBatchReader(schema({field("ts", timestamp(coerce_timestamps_from))}));

  SetSchema(reader->schema()->fields());

  EXPECT_OK_AND_ASSIGN(auto sink, GetFileSink());

  auto options =
      checked_pointer_cast<ParquetFileWriteOptions>(format_->DefaultWriteOptions());
  options->writer_properties = parquet::WriterProperties::Builder()
                                   .created_by("TestParquetFileFormat")
                                   ->disable_statistics()
                                   ->build();

  options->arrow_writer_properties = parquet::ArrowWriterProperties::Builder()
                                         .coerce_timestamps(coerce_timestamps_to)
                                         ->build();

  EXPECT_OK_AND_ASSIGN(auto writer, format_->MakeWriter(sink, reader->schema(), options));
  ASSERT_OK(writer->Write(reader.get()));
  ASSERT_OK(writer->Finish());

  EXPECT_OK_AND_ASSIGN(auto written, sink->Finish());
  EXPECT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(FileSource{written}));

  EXPECT_OK_AND_ASSIGN(auto actual_schema, fragment->ReadPhysicalSchema());
  AssertSchemaEqual(Schema({field("ts", timestamp(coerce_timestamps_to))}),
                    *actual_schema);
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

}  // namespace dataset
}  // namespace arrow

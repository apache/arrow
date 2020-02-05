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
#include "arrow/dataset/filter.h"
#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "parquet/arrow/writer.h"

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
using parquet::arrow::FileWriter;
using parquet::arrow::WriteTable;

using testing::Pointee;

Status WriteRecordBatch(const RecordBatch& batch, FileWriter* writer) {
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

Status WriteRecordBatchReader(RecordBatchReader* reader, FileWriter* writer) {
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
  std::unique_ptr<FileWriter> writer;
  RETURN_NOT_OK(FileWriter::Open(*reader->schema(), pool, sink, properties,
                                 arrow_properties, &writer));
  RETURN_NOT_OK(WriteRecordBatchReader(reader, writer.get()));
  return writer->Close();
}

class ArrowParquetWriterMixin : public ::testing::Test {
 public:
  std::shared_ptr<Buffer> Write(std::vector<RecordBatchReader*> readers) {
    auto pool = ::arrow::default_memory_pool();

    std::shared_ptr<Buffer> out;

    for (auto reader : readers) {
      auto sink = CreateOutputStream(pool);

      ARROW_EXPECT_OK(WriteRecordBatchReader(reader, pool, sink));
      // XXX the rest of the test may crash if this fails, since out will be nullptr
      EXPECT_OK_AND_ASSIGN(out, sink->Finish());
    }
    return out;
  }

  std::shared_ptr<Buffer> Write(RecordBatchReader* reader) {
    return Write(std::vector<RecordBatchReader*>{reader});
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

class ParquetBufferFixtureMixin : public ArrowParquetWriterMixin {
 public:
  std::unique_ptr<FileSource> GetFileSource(RecordBatchReader* reader) {
    auto buffer = Write(reader);
    return internal::make_unique<FileSource>(std::move(buffer));
  }

  std::unique_ptr<FileSource> GetFileSource(std::vector<RecordBatchReader*> readers) {
    auto buffer = Write(std::move(readers));
    return internal::make_unique<FileSource>(std::move(buffer));
  }

  std::unique_ptr<RecordBatchReader> GetRecordBatchReader() {
    auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
    int64_t i = 0;
    return MakeGeneratedRecordBatch(
        batch->schema(), [batch, i](std::shared_ptr<RecordBatch>* out) mutable {
          *out = i++ < kBatchRepetitions ? batch : nullptr;
          return Status::OK();
        });
  }

 protected:
  std::shared_ptr<Schema> schema_ = schema({field("f64", float64())});
};

class TestParquetFileFormat : public ParquetBufferFixtureMixin {
 protected:
  std::shared_ptr<ScanOptions> opts_;
  std::shared_ptr<ScanContext> ctx_ = std::make_shared<ScanContext>();
};

TEST_F(TestParquetFileFormat, ScanRecordBatchReader) {
  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());

  opts_ = ScanOptions::Make(reader->schema());
  auto fragment = std::make_shared<ParquetFragment>(*source, opts_);

  ASSERT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(ctx_));
  int64_t row_count = 0;

  for (auto maybe_task : scan_task_it) {
    ASSERT_OK_AND_ASSIGN(auto task, std::move(maybe_task));
    ASSERT_OK_AND_ASSIGN(auto rb_it, task->Execute());
    for (auto maybe_batch : rb_it) {
      ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
      row_count += batch->num_rows();
    }
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestParquetFileFormat, OpenFailureWithRelevantError) {
  auto format = ParquetFileFormat();

  std::shared_ptr<Buffer> buf = std::make_shared<Buffer>(util::string_view(""));
  auto result = format.Inspect(FileSource(buf));
  EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, testing::HasSubstr("<Buffer>"),
                                  result.status());

  constexpr auto file_name = "herp/derp";
  ASSERT_OK_AND_ASSIGN(
      auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {fs::File(file_name)}));
  result = format.Inspect({file_name, fs.get()});
  EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, testing::HasSubstr(file_name),
                                  result.status());
}

TEST_F(TestParquetFileFormat, ScanRecordBatchReaderProjected) {
  schema_ = schema({field("f64", float64()), field("i64", int64()),
                    field("f32", float32()), field("i32", int32())});

  opts_ = ScanOptions::Make(schema_);
  opts_->projector = RecordBatchProjector(SchemaFromColumnNames(schema_, {"f64"}));
  opts_->filter = equal(field_ref("i32"), scalar(0));

  // NB: projector is applied by the scanner; ParquetFragment does not evaluate it so
  // we will not drop "i32" even though it is not in the projector's schema
  auto expected_schema = schema({field("f64", float64()), field("i32", int32())});

  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());
  auto fragment = std::make_shared<ParquetFragment>(*source, opts_);

  ASSERT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(ctx_));
  int64_t row_count = 0;

  for (auto maybe_task : scan_task_it) {
    ASSERT_OK_AND_ASSIGN(auto task, std::move(maybe_task));
    ASSERT_OK_AND_ASSIGN(auto rb_it, task->Execute());
    for (auto maybe_batch : rb_it) {
      ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
      row_count += batch->num_rows();
      ASSERT_EQ(*batch->schema(), *expected_schema);
    }
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestParquetFileFormat, ScanRecordBatchReaderProjectedMissingCols) {
  schema_ =
      schema({field("f64", float64()), field("i64", int64()), field("f32", float32())});
  auto reader_without_i32 = GetRecordBatchReader();

  schema_ =
      schema({field("i64", int64()), field("f32", float32()), field("i32", int32())});
  auto reader_without_f64 = GetRecordBatchReader();

  schema_ = schema({field("f64", float64()), field("i64", int64()),
                    field("f32", float32()), field("i32", int32())});
  auto reader = GetRecordBatchReader();

  opts_ = ScanOptions::Make(schema_);
  opts_->projector = RecordBatchProjector(SchemaFromColumnNames(schema_, {"f64"}));
  opts_->filter = equal(field_ref("i32"), scalar(0));

  // NB: projector is applied by the scanner; ParquetFragment does not evaluate it so
  // we will not drop "i32" even though it is not in the projector's schema
  auto expected_schema = schema({field("f64", float64()), field("i32", int32())});

  auto source =
      GetFileSource({reader.get(), reader_without_i32.get(), reader_without_f64.get()});
  auto fragment = std::make_shared<ParquetFragment>(*source, opts_);

  ASSERT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(ctx_));
  int64_t row_count = 0;

  for (auto maybe_task : scan_task_it) {
    ASSERT_OK_AND_ASSIGN(auto task, std::move(maybe_task));
    ASSERT_OK_AND_ASSIGN(auto rb_it, task->Execute());
    for (auto maybe_batch : rb_it) {
      ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
      row_count += batch->num_rows();
    }
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestParquetFileFormat, Inspect) {
  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());
  auto format = ParquetFileFormat();

  ASSERT_OK_AND_ASSIGN(auto actual, format.Inspect(*source.get()));
  EXPECT_EQ(*actual, *schema_);
}

TEST_F(TestParquetFileFormat, IsSupported) {
  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());
  auto format = ParquetFileFormat();

  bool supported = false;

  std::shared_ptr<Buffer> buf = std::make_shared<Buffer>(util::string_view(""));
  ASSERT_OK_AND_ASSIGN(supported, format.IsSupported(FileSource(buf)));
  ASSERT_EQ(supported, false);

  buf = std::make_shared<Buffer>(util::string_view("corrupted"));
  ASSERT_OK_AND_ASSIGN(supported, format.IsSupported(FileSource(buf)));
  ASSERT_EQ(supported, false);

  ASSERT_OK_AND_ASSIGN(supported, format.IsSupported(*source));
  EXPECT_EQ(supported, true);
}

void CountRowsInScan(ScanTaskIterator& it, int64_t expected_rows,
                     int64_t expected_batches) {
  int64_t actual_rows = 0;
  int64_t actual_batches = 0;

  for (auto maybe_scan_task : it) {
    ASSERT_OK_AND_ASSIGN(auto scan_task, std::move(maybe_scan_task));
    ASSERT_OK_AND_ASSIGN(auto rb_it, scan_task->Execute());
    for (auto maybe_record_batch : rb_it) {
      ASSERT_OK_AND_ASSIGN(auto record_batch, std::move(maybe_record_batch));
      actual_rows += record_batch->num_rows();
      actual_batches++;
    }
  }

  EXPECT_EQ(actual_rows, expected_rows);
  EXPECT_EQ(actual_batches, expected_batches);
}

class TestParquetFileFormatPushDown : public TestParquetFileFormat {
 public:
  void CountRowsAndBatchesInScan(Fragment& fragment, int64_t expected_rows,
                                 int64_t expected_batches) {
    int64_t actual_rows = 0;
    int64_t actual_batches = 0;

    ASSERT_OK_AND_ASSIGN(auto it, fragment.Scan(ctx_));
    for (auto maybe_scan_task : it) {
      ASSERT_OK_AND_ASSIGN(auto scan_task, std::move(maybe_scan_task));
      ASSERT_OK_AND_ASSIGN(auto rb_it, scan_task->Execute());
      for (auto maybe_record_batch : rb_it) {
        ASSERT_OK_AND_ASSIGN(auto record_batch, std::move(maybe_record_batch));
        actual_rows += record_batch->num_rows();
        actual_batches++;
      }
    }

    EXPECT_EQ(actual_rows, expected_rows);
    EXPECT_EQ(actual_batches, expected_batches);
  }
};

TEST_F(TestParquetFileFormatPushDown, Basic) {
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

  opts_ = ScanOptions::Make(reader->schema());
  auto fragment = std::make_shared<ParquetFragment>(*source, opts_);

  opts_->filter = scalar(true);
  CountRowsAndBatchesInScan(*fragment, kTotalNumRows, kNumRowGroups);

  for (int64_t i = 1; i <= kNumRowGroups; i++) {
    opts_->filter = ("i64"_ == int64_t(i)).Copy();
    CountRowsAndBatchesInScan(*fragment, i, 1);
  }

  /* Out of bound filters should skip all RowGroups. */
  opts_->filter = scalar(false);
  CountRowsAndBatchesInScan(*fragment, 0, 0);
  opts_->filter = ("i64"_ == int64_t(kNumRowGroups + 1)).Copy();
  CountRowsAndBatchesInScan(*fragment, 0, 0);
  opts_->filter = ("i64"_ == int64_t(-1)).Copy();
  CountRowsAndBatchesInScan(*fragment, 0, 0);
  // No rows match 1 and 2.
  opts_->filter = ("i64"_ == int64_t(1) and "u8"_ == uint8_t(2)).Copy();
  CountRowsAndBatchesInScan(*fragment, 0, 0);

  opts_->filter = ("i64"_ == int64_t(2) or "i64"_ == int64_t(4)).Copy();
  CountRowsAndBatchesInScan(*fragment, 2 + 4, 2);

  opts_->filter = ("i64"_ < int64_t(6)).Copy();
  CountRowsAndBatchesInScan(*fragment, 5 * (5 + 1) / 2, 5);

  opts_->filter = ("i64"_ >= int64_t(6)).Copy();
  CountRowsAndBatchesInScan(*fragment, kTotalNumRows - (5 * (5 + 1) / 2),
                            kNumRowGroups - 5);
}

}  // namespace dataset
}  // namespace arrow

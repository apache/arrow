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

#include "arrow/dataset/file_ipc.h"

#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace dataset {

constexpr int64_t kBatchSize = 1UL << 12;
constexpr int64_t kBatchRepetitions = 1 << 5;
constexpr int64_t kNumRows = kBatchSize * kBatchRepetitions;

using internal::checked_pointer_cast;

class ArrowIpcWriterMixin : public ::testing::Test {
 public:
  std::shared_ptr<Buffer> Write(RecordBatchReader* reader) {
    EXPECT_OK_AND_ASSIGN(auto sink, io::BufferOutputStream::Create());

    EXPECT_OK_AND_ASSIGN(auto writer, ipc::NewFileWriter(sink.get(), reader->schema()));

    std::vector<std::shared_ptr<RecordBatch>> batches;
    ARROW_EXPECT_OK(reader->ReadAll(&batches));
    for (auto batch : batches) {
      ARROW_EXPECT_OK(writer->WriteRecordBatch(*batch));
    }

    ARROW_EXPECT_OK(writer->Close());

    EXPECT_OK_AND_ASSIGN(auto out, sink->Finish());
    return out;
  }

  std::shared_ptr<Buffer> Write(const Table& table) {
    EXPECT_OK_AND_ASSIGN(auto sink, io::BufferOutputStream::Create());
    EXPECT_OK_AND_ASSIGN(auto writer, ipc::NewFileWriter(sink.get(), table.schema()));

    ARROW_EXPECT_OK(writer->WriteTable(table));

    ARROW_EXPECT_OK(writer->Close());

    EXPECT_OK_AND_ASSIGN(auto out, sink->Finish());
    return out;
  }
};

class TestIpcFileFormat : public ArrowIpcWriterMixin {
 public:
  std::unique_ptr<FileSource> GetFileSource(RecordBatchReader* reader) {
    auto buffer = Write(reader);
    return internal::make_unique<FileSource>(std::move(buffer));
  }

  std::unique_ptr<RecordBatchReader> GetRecordBatchReader(
      std::shared_ptr<Schema> schema = nullptr) {
    return MakeGeneratedRecordBatch(schema ? schema : schema_, kBatchSize,
                                    kBatchRepetitions);
  }

  Result<WritableFileSource> GetFileSink() {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ResizableBuffer> buffer,
                          AllocateResizableBuffer(0));
    return WritableFileSource(std::move(buffer));
  }

  RecordBatchIterator Batches(ScanTaskIterator scan_task_it) {
    return MakeFlattenIterator(MakeMaybeMapIterator(
        [](std::shared_ptr<ScanTask> scan_task) { return scan_task->Execute(); },
        std::move(scan_task_it)));
  }

  RecordBatchIterator Batches(Fragment* fragment) {
    EXPECT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(opts_, ctx_));
    return Batches(std::move(scan_task_it));
  }

 protected:
  std::shared_ptr<IpcFileFormat> format_ = std::make_shared<IpcFileFormat>();
  std::shared_ptr<ScanOptions> opts_;
  std::shared_ptr<ScanContext> ctx_ = std::make_shared<ScanContext>();
  std::shared_ptr<Schema> schema_ = schema({field("f64", float64())});
};

TEST_F(TestIpcFileFormat, ScanRecordBatchReader) {
  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());

  opts_ = ScanOptions::Make(reader->schema());
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
    row_count += batch->num_rows();
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestIpcFileFormat, ScanRecordBatchReaderWithVirtualColumn) {
  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());

  opts_ = ScanOptions::Make(schema({schema_->field(0), field("virtual", int32())}));
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
    AssertSchemaEqual(*batch->schema(), *schema_);
    row_count += batch->num_rows();
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestIpcFileFormat, WriteRecordBatchReader) {
  std::shared_ptr<RecordBatchReader> reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());
  reader = GetRecordBatchReader();

  opts_ = ScanOptions::Make(reader->schema());

  EXPECT_OK_AND_ASSIGN(auto sink, GetFileSink());

  EXPECT_OK_AND_ASSIGN(auto fragment, format_->WriteFragment(sink, scalar(true), reader));

  AssertBufferEqual(*sink.buffer(), *source->buffer());
}

class TestIpcFileSystemDataset : public TestIpcFileFormat,
                                 public MakeFileSystemDatasetMixin {
 public:
  TestIpcFileSystemDataset() {
    using PathAndContent = std::vector<std::pair<std::string, std::string>>;
    auto files = PathAndContent{
        {"/dataset/year=2018/month=01/dat0.json", R"([
        {"region": "NY", "model": "3", "sales": 742.0, "country": "US"},
        {"region": "NY", "model": "S", "sales": 304.125, "country": "US"},
        {"region": "NY", "model": "Y", "sales": 27.5, "country": "US"}
      ])"},
        {"/dataset/year=2018/month=01/dat1.json", R"([
        {"region": "CA", "model": "3", "sales": 512, "country": "CA"},
        {"region": "CA", "model": "S", "sales": 978, "country": "CA"},
        {"region": "NY", "model": "X", "sales": 136.25, "country": "US"},
        {"region": "CA", "model": "X", "sales": 1.0, "country": "CA"},
        {"region": "CA", "model": "Y", "sales": 69, "country": "CA"}
      ])"},
        {"/dataset/year=2019/month=01/dat0.json", R"([
        {"region": "QC", "model": "3", "sales": 273.5, "country": "US"},
        {"region": "QC", "model": "S", "sales": 13, "country": "US"},
        {"region": "QC", "model": "X", "sales": 54, "country": "US"},
        {"region": "QC", "model": "S", "sales": 10, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 21, "country": "US"}
      ])"},
        {"/dataset/year=2019/month=01/dat1.json", R"([
        {"region": "QC", "model": "3", "sales": 152.25, "country": "CA"},
        {"region": "QC", "model": "X", "sales": 42, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 37, "country": "CA"}
      ])"},
        {"/dataset/.pesky", "garbage content"},
    };

    auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
    for (const auto& f : files) {
      ARROW_EXPECT_OK(mock_fs->CreateFile(f.first, f.second, /* recursive */ true));
    }

    fs_ = mock_fs;
  }
};

TEST_F(TestIpcFileSystemDataset, Write) {
  /// schema for the whole dataset (both source and destination)
  schema_ = schema({
      field("region", utf8()),
      field("model", utf8()),
      field("sales", float64()),
      field("year", int32()),
      field("month", int32()),
      field("country", utf8()),
  });

  /// Dummy file format for source dataset. Note that it isn't partitioned on country
  auto source_format = std::make_shared<JSONRecordBatchFileFormat>(
      SchemaFromColumnNames(schema_, {"region", "model", "sales", "country"}));

  fs::FileSelector s;
  s.base_dir = "/dataset";
  s.recursive = true;

  FileSystemFactoryOptions options;
  options.selector_ignore_prefixes = {"."};
  options.partitioning = HivePartitioning::MakeFactory();
  ASSERT_OK_AND_ASSIGN(auto factory,
                       FileSystemDatasetFactory::Make(fs_, s, source_format, options));
  ASSERT_OK_AND_ASSIGN(dataset_, factory->Finish());

  /// now copy the source dataset from json format to ipc
  std::vector<std::string> desired_partition_fields = {"country", "year", "month"};

  ASSERT_OK_AND_ASSIGN(
      WritePlan plan,
      DirectoryPartitioning::MakeFactory(desired_partition_fields)
          ->MakeWritePlan(schema_, dataset_->GetFragments(),
                          SchemaFromColumnNames(schema_, desired_partition_fields)));

  plan.SetFormat(format_);
  plan.SetBaseDir(fs_, "new_root/");
  ASSERT_OK_AND_ASSIGN(auto written, plan.Execute());

  // XXX first thing a user will be annoyed by: we don't support left padding the month
  // field with 0.
  EXPECT_THAT(
      written->files(),
      testing::ElementsAre("new_root/US/2018/1/dat.ipc", "new_root/CA/2018/1/dat.ipc",
                           "new_root/US/2019/1/dat.ipc", "new_root/CA/2019/1/dat.ipc"));
}

TEST_F(TestIpcFileFormat, OpenFailureWithRelevantError) {
  std::shared_ptr<Buffer> buf = std::make_shared<Buffer>(util::string_view(""));
  auto result = format_->Inspect(FileSource(buf));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("<Buffer>"),
                                  result.status());

  constexpr auto file_name = "herp/derp";
  ASSERT_OK_AND_ASSIGN(
      auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {fs::File(file_name)}));
  result = format_->Inspect({file_name, fs});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr(file_name),
                                  result.status());
}

TEST_F(TestIpcFileFormat, ScanRecordBatchReaderProjected) {
  schema_ = schema({field("f64", float64()), field("i64", int64()),
                    field("f32", float32()), field("i32", int32())});

  opts_ = ScanOptions::Make(schema_);
  opts_->projector = RecordBatchProjector(SchemaFromColumnNames(schema_, {"f64"}));
  opts_->filter = equal(field_ref("i32"), scalar(0));

  // NB: projector is applied by the scanner; FileFragment does not evaluate it so
  // we will not drop "i32" even though it is not in the projector's schema
  auto expected_schema = schema({field("f64", float64()), field("i32", int32())});

  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
    row_count += batch->num_rows();
    AssertSchemaEqual(*batch->schema(), *expected_schema,
                      /*check_metadata=*/false);
  }

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestIpcFileFormat, ScanRecordBatchReaderProjectedMissingCols) {
  auto reader_without_i32 = GetRecordBatchReader(
      schema({field("f64", float64()), field("i64", int64()), field("f32", float32())}));

  auto reader_without_f64 = GetRecordBatchReader(
      schema({field("i64", int64()), field("f32", float32()), field("i32", int32())}));

  auto reader =
      GetRecordBatchReader(schema({field("f64", float64()), field("i64", int64()),
                                   field("f32", float32()), field("i32", int32())}));

  schema_ = reader->schema();
  opts_ = ScanOptions::Make(schema_);
  opts_->projector = RecordBatchProjector(SchemaFromColumnNames(schema_, {"f64"}));
  opts_->filter = equal(field_ref("i32"), scalar(0));

  auto readers = {reader.get(), reader_without_i32.get(), reader_without_f64.get()};
  for (auto reader : readers) {
    auto source = GetFileSource(reader);
    ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

    // NB: projector is applied by the scanner; Fragment does not evaluate it.
    // We will not drop "i32" even though it is not in the projector's schema.
    //
    // in the case where a file doesn't contain a referenced field, we won't
    // materialize it (the filter/projector will populate it with nulls later)
    std::shared_ptr<Schema> expected_schema;
    if (reader == reader_without_i32.get()) {
      expected_schema = schema({field("f64", float64())});
    } else if (reader == reader_without_f64.get()) {
      expected_schema = schema({field("i32", int32())});
    } else {
      expected_schema = schema({field("f64", float64()), field("i32", int32())});
    }

    int64_t row_count = 0;

    for (auto maybe_batch : Batches(fragment.get())) {
      ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
      row_count += batch->num_rows();
      AssertSchemaEqual(*batch->schema(), *expected_schema,
                        /*check_metadata=*/false);
    }

    ASSERT_EQ(row_count, kNumRows);
  }
}

TEST_F(TestIpcFileFormat, Inspect) {
  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());

  ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));
  EXPECT_EQ(*actual, *schema_);
}

TEST_F(TestIpcFileFormat, IsSupported) {
  auto reader = GetRecordBatchReader();
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

}  // namespace dataset
}  // namespace arrow

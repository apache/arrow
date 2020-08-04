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

  ASSERT_OK(format_->WriteFragment(reader.get(), sink.get()));

  EXPECT_OK_AND_ASSIGN(auto written, sink->Finish());

  AssertBufferEqual(*written, *source->buffer());
}

class TestIpcFileSystemDataset : public TestIpcFileFormat,
                                 public MakeFileSystemDatasetMixin {
 public:
  using PathAndContent = std::unordered_map<std::string, std::string>;

  void SetUp() override {
    PathAndContent source_files;

    source_files["/dataset/year=2018/month=01/dat0.json"] = R"([
        {"region": "NY", "model": "3", "sales": 742.0, "country": "US"},
        {"region": "NY", "model": "S", "sales": 304.125, "country": "US"},
        {"region": "NY", "model": "Y", "sales": 27.5, "country": "US"}
      ])";
    source_files["/dataset/year=2018/month=01/dat1.json"] = R"([
        {"region": "QC", "model": "3", "sales": 512, "country": "CA"},
        {"region": "QC", "model": "S", "sales": 978, "country": "CA"},
        {"region": "NY", "model": "X", "sales": 136.25, "country": "US"},
        {"region": "QC", "model": "X", "sales": 1.0, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 69, "country": "CA"}
      ])";
    source_files["/dataset/year=2019/month=01/dat0.json"] = R"([
        {"region": "CA", "model": "3", "sales": 273.5, "country": "US"},
        {"region": "CA", "model": "S", "sales": 13, "country": "US"},
        {"region": "CA", "model": "X", "sales": 54, "country": "US"},
        {"region": "QC", "model": "S", "sales": 10, "country": "CA"},
        {"region": "CA", "model": "Y", "sales": 21, "country": "US"}
      ])";
    source_files["/dataset/year=2019/month=01/dat1.json"] = R"([
        {"region": "QC", "model": "3", "sales": 152.25, "country": "CA"},
        {"region": "QC", "model": "X", "sales": 42, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 37, "country": "CA"}
      ])";
    source_files["/dataset/.pesky"] = "garbage content";

    auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
    for (const auto& f : source_files) {
      ARROW_EXPECT_OK(mock_fs->CreateFile(f.first, f.second, /* recursive */ true));
    }
    fs_ = mock_fs;

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
  }

  void AssertWrittenAsExpected() {
    std::vector<std::string> files;
    for (const auto& file_contents : expected_files_) {
      files.push_back(file_contents.first);
    }
    EXPECT_THAT(checked_pointer_cast<FileSystemDataset>(written_)->files(),
                testing::UnorderedElementsAreArray(files));

    for (auto maybe_fragment : written_->GetFragments()) {
      ASSERT_OK_AND_ASSIGN(auto fragment, std::move(maybe_fragment));

      ASSERT_OK_AND_ASSIGN(auto actual_physical_schema, fragment->ReadPhysicalSchema());
      AssertSchemaEqual(*expected_physical_schema_, *actual_physical_schema,
                        /*verbose=*/true);

      const auto& path = checked_pointer_cast<FileFragment>(fragment)->source().path();

      auto expected_struct = ArrayFromJSON(struct_(expected_physical_schema_->fields()),
                                           {expected_files_[path]});

      ASSERT_OK_AND_ASSIGN(auto scanner, ScannerBuilder(actual_physical_schema, fragment,
                                                        std::make_shared<ScanContext>())
                                             .Finish());
      ASSERT_OK_AND_ASSIGN(auto actual_table, scanner->ToTable());
      ASSERT_OK_AND_ASSIGN(actual_table, actual_table->CombineChunks());
      std::shared_ptr<Array> actual_struct;

      for (auto maybe_batch :
           IteratorFromReader(std::make_shared<TableBatchReader>(*actual_table))) {
        ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
        ASSERT_OK_AND_ASSIGN(actual_struct, batch->ToStructArray());
      }

      AssertArraysEqual(*expected_struct, *actual_struct, /*verbose=*/true);
    }
  }

  PathAndContent expected_files_;
  std::shared_ptr<Schema> expected_physical_schema_;
  std::shared_ptr<Dataset> written_;
};

TEST_F(TestIpcFileSystemDataset, WriteWithIdenticalPartitioningSchema) {
  auto desired_partitioning = std::make_shared<DirectoryPartitioning>(
      SchemaFromColumnNames(schema_, {"year", "month"}));

  ASSERT_OK(FileSystemDataset::Write(
      schema_, format_, fs_, "new_root/", desired_partitioning,
      std::make_shared<ScanContext>(), dataset_->GetFragments()));

  fs::FileSelector s;
  s.recursive = true;
  s.base_dir = "/new_root";

  FileSystemFactoryOptions options;
  options.partitioning = desired_partitioning;
  ASSERT_OK_AND_ASSIGN(auto factory,
                       FileSystemDatasetFactory::Make(fs_, s, format_, options));
  ASSERT_OK_AND_ASSIGN(written_, factory->Finish());

  expected_files_["/new_root/2018/1/dat_0.ipc"] = R"([
        {"region": "NY", "model": "3", "sales": 742.0, "country": "US"},
        {"region": "NY", "model": "S", "sales": 304.125, "country": "US"},
        {"region": "NY", "model": "Y", "sales": 27.5, "country": "US"}
      ])";
  expected_files_["/new_root/2018/1/dat_1.ipc"] = R"([
        {"region": "QC", "model": "3", "sales": 512, "country": "CA"},
        {"region": "QC", "model": "S", "sales": 978, "country": "CA"},
        {"region": "NY", "model": "X", "sales": 136.25, "country": "US"},
        {"region": "QC", "model": "X", "sales": 1.0, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 69, "country": "CA"}
      ])";
  expected_files_["/new_root/2019/1/dat_2.ipc"] = R"([
        {"region": "CA", "model": "3", "sales": 273.5, "country": "US"},
        {"region": "CA", "model": "S", "sales": 13, "country": "US"},
        {"region": "CA", "model": "X", "sales": 54, "country": "US"},
        {"region": "QC", "model": "S", "sales": 10, "country": "CA"},
        {"region": "CA", "model": "Y", "sales": 21, "country": "US"}
      ])";
  expected_files_["/new_root/2019/1/dat_3.ipc"] = R"([
        {"region": "QC", "model": "3", "sales": 152.25, "country": "CA"},
        {"region": "QC", "model": "X", "sales": 42, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 37, "country": "CA"}
      ])";
  expected_physical_schema_ =
      SchemaFromColumnNames(schema_, {"region", "model", "sales", "country"});

  AssertWrittenAsExpected();
}

TEST_F(TestIpcFileSystemDataset, WriteWithUnrelatedPartitioningSchema) {
  auto desired_partitioning = std::make_shared<DirectoryPartitioning>(
      SchemaFromColumnNames(schema_, {"country", "region"}));

  ASSERT_OK(FileSystemDataset::Write(
      schema_, format_, fs_, "new_root/", desired_partitioning,
      std::make_shared<ScanContext>(), dataset_->GetFragments()));

  fs::FileSelector s;
  s.recursive = true;
  s.base_dir = "/new_root";

  FileSystemFactoryOptions options;
  options.partitioning = desired_partitioning;
  ASSERT_OK_AND_ASSIGN(auto factory,
                       FileSystemDatasetFactory::Make(fs_, s, format_, options));
  ASSERT_OK_AND_ASSIGN(written_, factory->Finish());

  // XXX first thing a user will be annoyed by: we don't support left
  // padding the month field with 0.
  expected_files_["/new_root/US/NY/dat_0.ipc"] = R"([
        {"year": 2018, "month": 1, "model": "3", "sales": 742.0},
        {"year": 2018, "month": 1, "model": "S", "sales": 304.125},
        {"year": 2018, "month": 1, "model": "Y", "sales": 27.5}
  ])";
  expected_files_["/new_root/US/NY/dat_1.ipc"] = R"([
        {"year": 2018, "month": 1, "model": "X", "sales": 136.25}
  ])";
  expected_files_["/new_root/CA/QC/dat_1.ipc"] = R"([
        {"year": 2018, "month": 1, "model": "3", "sales": 512},
        {"year": 2018, "month": 1, "model": "S", "sales": 978},
        {"year": 2018, "month": 1, "model": "X", "sales": 1.0},
        {"year": 2018, "month": 1, "model": "Y", "sales": 69}
  ])";
  expected_files_["/new_root/US/CA/dat_2.ipc"] = R"([
        {"year": 2019, "month": 1, "model": "3", "sales": 273.5},
        {"year": 2019, "month": 1, "model": "S", "sales": 13},
        {"year": 2019, "month": 1, "model": "X", "sales": 54},
        {"year": 2019, "month": 1, "model": "Y", "sales": 21}
  ])";
  expected_files_["/new_root/CA/QC/dat_2.ipc"] = R"([
        {"year": 2019, "month": 1, "model": "S", "sales": 10}
  ])";
  expected_files_["/new_root/CA/QC/dat_3.ipc"] = R"([
        {"year": 2019, "month": 1, "model": "3", "sales": 152.25},
        {"year": 2019, "month": 1, "model": "X", "sales": 42},
        {"year": 2019, "month": 1, "model": "Y", "sales": 37}
  ])";
  expected_physical_schema_ =
      SchemaFromColumnNames(schema_, {"model", "sales", "year", "month"});

  AssertWrittenAsExpected();
}

TEST_F(TestIpcFileSystemDataset, WriteWithSupersetPartitioningSchema) {
  auto desired_partitioning = std::make_shared<DirectoryPartitioning>(
      SchemaFromColumnNames(schema_, {"year", "month", "country", "region"}));

  ASSERT_OK(FileSystemDataset::Write(
      schema_, format_, fs_, "new_root/", desired_partitioning,
      std::make_shared<ScanContext>(), dataset_->GetFragments()));

  fs::FileSelector s;
  s.recursive = true;
  s.base_dir = "/new_root";

  FileSystemFactoryOptions options;
  options.partitioning = desired_partitioning;
  ASSERT_OK_AND_ASSIGN(auto factory,
                       FileSystemDatasetFactory::Make(fs_, s, format_, options));
  ASSERT_OK_AND_ASSIGN(written_, factory->Finish());

  // XXX first thing a user will be annoyed by: we don't support left
  // padding the month field with 0.
  expected_files_["/new_root/2018/1/US/NY/dat_0.ipc"] = R"([
        {"model": "3", "sales": 742.0},
        {"model": "S", "sales": 304.125},
        {"model": "Y", "sales": 27.5}
  ])";
  expected_files_["/new_root/2018/1/US/NY/dat_1.ipc"] = R"([
        {"model": "X", "sales": 136.25}
  ])";
  expected_files_["/new_root/2018/1/CA/QC/dat_1.ipc"] = R"([
        {"model": "3", "sales": 512},
        {"model": "S", "sales": 978},
        {"model": "X", "sales": 1.0},
        {"model": "Y", "sales": 69}
  ])";
  expected_files_["/new_root/2019/1/US/CA/dat_2.ipc"] = R"([
        {"model": "3", "sales": 273.5},
        {"model": "S", "sales": 13},
        {"model": "X", "sales": 54},
        {"model": "Y", "sales": 21}
  ])";
  expected_files_["/new_root/2019/1/CA/QC/dat_2.ipc"] = R"([
        {"model": "S", "sales": 10}
  ])";
  expected_files_["/new_root/2019/1/CA/QC/dat_3.ipc"] = R"([
        {"model": "3", "sales": 152.25},
        {"model": "X", "sales": 42},
        {"model": "Y", "sales": 37}
  ])";
  expected_physical_schema_ = SchemaFromColumnNames(schema_, {"model", "sales"});

  AssertWrittenAsExpected();
}

TEST_F(TestIpcFileSystemDataset, WriteWithEmptyPartitioningSchema) {
  auto desired_partitioning =
      std::make_shared<DirectoryPartitioning>(SchemaFromColumnNames(schema_, {}));

  ASSERT_OK(FileSystemDataset::Write(
      schema_, format_, fs_, "new_root/", desired_partitioning,
      std::make_shared<ScanContext>(), dataset_->GetFragments()));

  fs::FileSelector s;
  s.recursive = true;
  s.base_dir = "/new_root";

  FileSystemFactoryOptions options;
  options.partitioning = desired_partitioning;
  ASSERT_OK_AND_ASSIGN(auto factory,
                       FileSystemDatasetFactory::Make(fs_, s, format_, options));
  ASSERT_OK_AND_ASSIGN(written_, factory->Finish());

  expected_files_["/new_root/dat_0.ipc"] = R"([
        {"country": "US", "region": "NY", "year": 2018, "month": 1, "model": "3", "sales": 742.0},
        {"country": "US", "region": "NY", "year": 2018, "month": 1, "model": "S", "sales": 304.125},
        {"country": "US", "region": "NY", "year": 2018, "month": 1, "model": "Y", "sales": 27.5}
  ])";
  expected_files_["/new_root/dat_1.ipc"] = R"([
        {"country": "CA", "region": "QC", "year": 2018, "month": 1, "model": "3", "sales": 512},
        {"country": "CA", "region": "QC", "year": 2018, "month": 1, "model": "S", "sales": 978},
        {"country": "US", "region": "NY", "year": 2018, "month": 1, "model": "X", "sales": 136.25},
        {"country": "CA", "region": "QC", "year": 2018, "month": 1, "model": "X", "sales": 1.0},
        {"country": "CA", "region": "QC", "year": 2018, "month": 1, "model": "Y", "sales": 69}
  ])";
  expected_files_["/new_root/dat_2.ipc"] = R"([
        {"country": "US", "region": "CA", "year": 2019, "month": 1, "model": "3", "sales": 273.5},
        {"country": "US", "region": "CA", "year": 2019, "month": 1, "model": "S", "sales": 13},
        {"country": "US", "region": "CA", "year": 2019, "month": 1, "model": "X", "sales": 54},
        {"country": "CA", "region": "QC", "year": 2019, "month": 1, "model": "S", "sales": 10},
        {"country": "US", "region": "CA", "year": 2019, "month": 1, "model": "Y", "sales": 21}
  ])";
  expected_files_["/new_root/dat_3.ipc"] = R"([
        {"country": "CA", "region": "QC", "year": 2019, "month": 1, "model": "3", "sales": 152.25},
        {"country": "CA", "region": "QC", "year": 2019, "month": 1, "model": "X", "sales": 42},
        {"country": "CA", "region": "QC", "year": 2019, "month": 1, "model": "Y", "sales": 37}
  ])";
  expected_physical_schema_ = schema_;

  AssertWrittenAsExpected();
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

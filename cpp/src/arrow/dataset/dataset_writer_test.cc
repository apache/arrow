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

#include "arrow/dataset/dataset_writer.h"

#include <chrono>
#include <mutex>
#include <optional>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "gtest/gtest.h"

using namespace std::string_view_literals;  // NOLINT

namespace arrow {
namespace dataset {
namespace internal {

using arrow::fs::internal::MockFileInfo;
using arrow::fs::internal::MockFileSystem;

class DatasetWriterTestFixture : public testing::Test {
 protected:
  struct ExpectedFile {
    std::string filename;
    uint64_t start;
    uint64_t num_rows;
    int num_record_batches;

    ExpectedFile(std::string filename, uint64_t start, uint64_t num_rows)
        : filename(std::move(filename)),
          start(start),
          num_rows(num_rows),
          num_record_batches(1) {}

    ExpectedFile(std::string filename, uint64_t start, uint64_t num_rows,
                 int num_record_batches)
        : filename(std::move(filename)),
          start(start),
          num_rows(num_rows),
          num_record_batches(num_record_batches) {}
  };

  void SetUp() override {
    fs::TimePoint mock_now = std::chrono::system_clock::now();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<fs::FileSystem> fs,
                         MockFileSystem::Make(mock_now, {::arrow::fs::Dir("testdir")}));
    filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
    schema_ = schema({field("int64", int64())});
    write_options_.filesystem = filesystem_;
    write_options_.basename_template = "chunk-{i}.arrow";
    write_options_.base_dir = "testdir";
    write_options_.writer_pre_finish = [this](FileWriter* writer) {
      pre_finish_visited_.push_back(writer->destination().path);
      return Status::OK();
    };
    write_options_.writer_post_finish = [this](FileWriter* writer) {
      post_finish_visited_.push_back(writer->destination().path);
      return Status::OK();
    };
    std::shared_ptr<FileFormat> format = std::make_shared<IpcFileFormat>();
    write_options_.file_write_options = format->DefaultWriteOptions();
    scheduler_finished_ =
        util::AsyncTaskScheduler::Make([&](util::AsyncTaskScheduler* scheduler) {
          scheduler_ = scheduler;
          scheduler->AddSimpleTask(
              [&] { return test_done_with_tasks_; },
              "DatasetWriterTestFixture::WaitForTestMethodToFinish"sv);
          return Status::OK();
        });
  }

  void TearDown() override {
    if (!test_done_with_tasks_.is_finished()) {
      test_done_with_tasks_.MarkFinished();
      ASSERT_FINISHES_OK(scheduler_finished_);
    }
  }

  std::unique_ptr<DatasetWriter> MakeDatasetWriter(
      uint64_t max_rows = kDefaultDatasetWriterMaxRowsQueued) {
    EXPECT_OK_AND_ASSIGN(auto dataset_writer,
                         DatasetWriter::Make(
                             write_options_, scheduler_, [] {}, [] {}, [] {}, max_rows));
    return dataset_writer;
  }

  void EndWriterChecked(DatasetWriter* writer) {
    writer->Finish();
    test_done_with_tasks_.MarkFinished();
    ASSERT_FINISHES_OK(scheduler_finished_);
  }

  std::shared_ptr<fs::GatedMockFilesystem> UseGatedFs() {
    fs::TimePoint mock_now = std::chrono::system_clock::now();
    auto fs = std::make_shared<fs::GatedMockFilesystem>(mock_now);
    ARROW_EXPECT_OK(fs->CreateDir("testdir"));
    write_options_.filesystem = fs;
    filesystem_ = fs;
    return fs;
  }

  std::shared_ptr<RecordBatch> MakeBatch(uint64_t start, uint64_t num_rows) {
    Int64Builder builder;
    for (uint64_t i = 0; i < num_rows; i++) {
      ARROW_EXPECT_OK(builder.Append(i + start));
    }
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<Array> arr, builder.Finish());
    return RecordBatch::Make(schema_, static_cast<int64_t>(num_rows), {std::move(arr)});
  }

  std::shared_ptr<RecordBatch> MakeBatch(uint64_t num_rows) {
    std::shared_ptr<RecordBatch> batch = MakeBatch(counter_, num_rows);
    counter_ += num_rows;
    return batch;
  }

  std::optional<MockFileInfo> FindFile(const std::string& filename) {
    for (const auto& mock_file : filesystem_->AllFiles()) {
      if (mock_file.full_path == filename) {
        return mock_file;
      }
    }
    return std::nullopt;
  }

  void AssertVisited(const std::vector<std::string>& actual_paths,
                     const std::string& expected_path) {
    const auto found = std::find(actual_paths.begin(), actual_paths.end(), expected_path);
    ASSERT_NE(found, actual_paths.end())
        << "The file " << expected_path << " was not in the list of files visited";
  }

  std::shared_ptr<RecordBatch> ReadAsBatch(std::string_view data, int* num_batches) {
    std::shared_ptr<io::RandomAccessFile> in_stream =
        std::make_shared<io::BufferReader>(data);
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<ipc::RecordBatchFileReader> reader,
                         ipc::RecordBatchFileReader::Open(in_stream));
    RecordBatchVector batches;
    *num_batches = reader->num_record_batches();
    EXPECT_GT(*num_batches, 0);
    for (int i = 0; i < reader->num_record_batches(); i++) {
      EXPECT_OK_AND_ASSIGN(std::shared_ptr<RecordBatch> next_batch,
                           reader->ReadRecordBatch(i));
      batches.push_back(next_batch);
    }
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<Table> table, Table::FromRecordBatches(batches));
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<Table> combined_table, table->CombineChunks());
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<RecordBatch> batch,
                         TableBatchReader(*combined_table).Next());
    return batch;
  }

  void AssertFileCreated(const std::optional<MockFileInfo>& maybe_file,
                         const std::string& expected_filename) {
    ASSERT_TRUE(maybe_file.has_value())
        << "The file " << expected_filename << " was not created";
    {
      SCOPED_TRACE("pre_finish");
      AssertVisited(pre_finish_visited_, expected_filename);
    }
    {
      SCOPED_TRACE("post_finish");
      AssertVisited(post_finish_visited_, expected_filename);
    }
  }

  void AssertCreatedData(const std::vector<ExpectedFile>& expected_files) {
    counter_ = 0;
    for (const auto& expected_file : expected_files) {
      std::optional<MockFileInfo> written_file = FindFile(expected_file.filename);
      AssertFileCreated(written_file, expected_file.filename);
      int num_batches = 0;
      AssertBatchesEqual(*MakeBatch(expected_file.start, expected_file.num_rows),
                         *ReadAsBatch(written_file->data, &num_batches));
      ASSERT_EQ(expected_file.num_record_batches, num_batches);
    }
  }

  void AssertFilesCreated(const std::vector<std::string>& expected_files) {
    for (const std::string& expected_file : expected_files) {
      std::optional<MockFileInfo> written_file = FindFile(expected_file);
      AssertFileCreated(written_file, expected_file);
    }
  }

  void AssertNotFiles(const std::vector<std::string>& expected_non_files) {
    for (const auto& expected_non_file : expected_non_files) {
      std::optional<MockFileInfo> file = FindFile(expected_non_file);
      ASSERT_FALSE(file.has_value());
    }
  }

  void AssertEmptyFiles(const std::vector<std::string>& expected_empty_files) {
    for (const auto& expected_empty_file : expected_empty_files) {
      std::optional<MockFileInfo> file = FindFile(expected_empty_file);
      ASSERT_TRUE(file.has_value());
      ASSERT_EQ("", file->data);
    }
  }

  std::shared_ptr<MockFileSystem> filesystem_;
  std::shared_ptr<Schema> schema_;
  std::vector<std::string> pre_finish_visited_;
  std::vector<std::string> post_finish_visited_;
  Future<> test_done_with_tasks_ = Future<>::Make();
  util::AsyncTaskScheduler* scheduler_;
  Future<> scheduler_finished_;
  FileSystemDatasetWriteOptions write_options_;
  uint64_t counter_ = 0;
};

TEST_F(DatasetWriterTestFixture, Basic) {
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(100), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0.arrow", 0, 100}});
}

TEST_F(DatasetWriterTestFixture, BasicFilePrefix) {
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(100), "", "1_");
  EndWriterChecked(dataset_writer.get());
  AssertFilesCreated({"testdir/1_chunk-0.arrow"});
}

TEST_F(DatasetWriterTestFixture, BasicFileDirectoryPrefix) {
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(100), "a", "1_");
  EndWriterChecked(dataset_writer.get());
  AssertFilesCreated({"testdir/a/1_chunk-0.arrow"});
}

TEST_F(DatasetWriterTestFixture, DirectoryCreateFails) {
  // This should fail to be created
  write_options_.base_dir = "///doesnotexist";
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(100), "a", "1_");
  dataset_writer->Finish();
  test_done_with_tasks_.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, scheduler_finished_);
}

TEST_F(DatasetWriterTestFixture, MaxRowsOneWrite) {
  write_options_.max_rows_per_file = 10;
  write_options_.max_rows_per_group = 10;
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(35), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0.arrow", 0, 10},
                     {"testdir/chunk-1.arrow", 10, 10},
                     {"testdir/chunk-2.arrow", 20, 10},
                     {"testdir/chunk-3.arrow", 30, 5}});
}

TEST_F(DatasetWriterTestFixture, MaxRowsOneWriteWithFunctor) {
  // Left padding with up to four zeros
  write_options_.max_rows_per_group = 10;
  write_options_.max_rows_per_file = 10;
  write_options_.basename_template_functor = [](int v) {
    size_t n_zero = 4;
    return std::string(n_zero - std::min(n_zero, std::to_string(v).length()), '0') +
           std::to_string(v);
  };
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(25), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0000.arrow", 0, 10},
                     {"testdir/chunk-0001.arrow", 10, 10},
                     {"testdir/chunk-0002.arrow", 20, 5}});
}

TEST_F(DatasetWriterTestFixture, MaxRowsOneWriteWithBrokenFunctor) {
  // Rewriting an exiting file will error out
  write_options_.max_rows_per_group = 10;
  write_options_.max_rows_per_file = 10;
  write_options_.basename_template_functor = [](int v) { return "SAME"; };
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(25), "");
  dataset_writer->Finish();
  test_done_with_tasks_.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, scheduler_finished_);
}

TEST_F(DatasetWriterTestFixture, MaxRowsManyWrites) {
  write_options_.max_rows_per_file = 10;
  write_options_.max_rows_per_group = 10;
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(3), "");
  dataset_writer->WriteRecordBatch(MakeBatch(3), "");
  dataset_writer->WriteRecordBatch(MakeBatch(3), "");
  dataset_writer->WriteRecordBatch(MakeBatch(3), "");
  dataset_writer->WriteRecordBatch(MakeBatch(3), "");
  dataset_writer->WriteRecordBatch(MakeBatch(3), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData(
      {{"testdir/chunk-0.arrow", 0, 10, 4}, {"testdir/chunk-1.arrow", 10, 8, 3}});
}

TEST_F(DatasetWriterTestFixture, MinRowGroup) {
  write_options_.min_rows_per_group = 20;
  auto dataset_writer = MakeDatasetWriter();
  // Test hitting the limit exactly and inexactly
  dataset_writer->WriteRecordBatch(MakeBatch(5), "");
  dataset_writer->WriteRecordBatch(MakeBatch(5), "");
  dataset_writer->WriteRecordBatch(MakeBatch(5), "");
  dataset_writer->WriteRecordBatch(MakeBatch(5), "");
  dataset_writer->WriteRecordBatch(MakeBatch(5), "");
  dataset_writer->WriteRecordBatch(MakeBatch(5), "");
  dataset_writer->WriteRecordBatch(MakeBatch(4), "");
  dataset_writer->WriteRecordBatch(MakeBatch(4), "");
  dataset_writer->WriteRecordBatch(MakeBatch(4), "");
  dataset_writer->WriteRecordBatch(MakeBatch(4), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0.arrow", 0, 46, 3}});
}

TEST_F(DatasetWriterTestFixture, MaxRowGroup) {
  write_options_.max_rows_per_group = 10;
  auto dataset_writer = MakeDatasetWriter();
  // Test hitting the limit exactly and inexactly
  dataset_writer->WriteRecordBatch(MakeBatch(10), "");
  dataset_writer->WriteRecordBatch(MakeBatch(15), "");
  dataset_writer->WriteRecordBatch(MakeBatch(15), "");
  dataset_writer->WriteRecordBatch(MakeBatch(20), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0.arrow", 0, 60, 7}});
}

TEST_F(DatasetWriterTestFixture, MinAndMaxRowGroup) {
  write_options_.max_rows_per_group = 10;
  write_options_.min_rows_per_group = 10;
  auto dataset_writer = MakeDatasetWriter();
  // Test hitting the limit exactly and inexactly
  dataset_writer->WriteRecordBatch(MakeBatch(10), "");
  dataset_writer->WriteRecordBatch(MakeBatch(15), "");
  dataset_writer->WriteRecordBatch(MakeBatch(15), "");
  dataset_writer->WriteRecordBatch(MakeBatch(20), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0.arrow", 0, 60, 6}});
}

TEST_F(DatasetWriterTestFixture, MinRowGroupBackpressure) {
  // This tests the case where we end up queuing too much data because we're waiting for
  // enough data to form a min row group and we fill up the dataset writer (it should
  // auto-evict)
  write_options_.min_rows_per_group = 10;
  auto dataset_writer = MakeDatasetWriter(100);
  std::vector<ExpectedFile> expected_files;
  for (int i = 0; i < 12; i++) {
    expected_files.push_back({"testdir/" + std::to_string(i) + "/chunk-0.arrow",
                              static_cast<uint64_t>(i * 9), 9, 1});
    dataset_writer->WriteRecordBatch(MakeBatch(9), std::to_string(i));
  }
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData(expected_files);
}

TEST_F(DatasetWriterTestFixture, ConcurrentWritesSameFile) {
  // Use a gated filesystem to queue up many writes behind a file open to make sure the
  // file isn't opened multiple times.
  auto gated_fs = UseGatedFs();
  auto dataset_writer = MakeDatasetWriter();
  for (int i = 0; i < 10; i++) {
    dataset_writer->WriteRecordBatch(MakeBatch(10), "");
  }
  ASSERT_OK(gated_fs->WaitForOpenOutputStream(1));
  ASSERT_OK(gated_fs->UnlockOpenOutputStream(1));
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0.arrow", 0, 100, 10}});
}

TEST_F(DatasetWriterTestFixture, ConcurrentWritesDifferentFiles) {
  // NBATCHES must be less than I/O executor concurrency to avoid deadlock / test failure
  constexpr int NBATCHES = 6;
  auto gated_fs = UseGatedFs();
  std::vector<ExpectedFile> expected_files;
  auto dataset_writer = MakeDatasetWriter();
  for (int i = 0; i < NBATCHES; i++) {
    std::string i_str = std::to_string(i);
    expected_files.push_back(ExpectedFile{"testdir/part" + i_str + "/chunk-0.arrow",
                                          static_cast<uint64_t>(i) * 10, 10});
    dataset_writer->WriteRecordBatch(MakeBatch(10), "part" + i_str);
  }
  ASSERT_OK(gated_fs->WaitForOpenOutputStream(NBATCHES));
  ASSERT_OK(gated_fs->UnlockOpenOutputStream(NBATCHES));
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData(expected_files);
}

TEST_F(DatasetWriterTestFixture, MaxOpenFiles) {
  auto gated_fs = UseGatedFs();
  std::atomic<bool> paused = false;
  write_options_.max_open_files = 2;
  EXPECT_OK_AND_ASSIGN(auto dataset_writer,
                       DatasetWriter::Make(
                           write_options_, scheduler_, [&] { paused = true; },
                           [&] { paused = false; }, [] {}));

  dataset_writer->WriteRecordBatch(MakeBatch(10), "part0");
  dataset_writer->WriteRecordBatch(MakeBatch(10), "part1");
  dataset_writer->WriteRecordBatch(MakeBatch(10), "part0");
  dataset_writer->WriteRecordBatch(MakeBatch(10), "part2");
  // Backpressure will be applied until an existing file can be evicted
  ASSERT_TRUE(paused);

  // Ungate the writes to relieve the pressure, testdir/part0 should be closed
  ASSERT_OK(gated_fs->WaitForOpenOutputStream(2));
  ASSERT_OK(gated_fs->UnlockOpenOutputStream(5));
  // This should free up things and allow us to continue
  BusyWait(10, [&] { return !paused; });

  dataset_writer->WriteRecordBatch(MakeBatch(10), "part0");
  // Following call should resume existing write but, on slow test systems, the old
  // write may have already been finished
  dataset_writer->WriteRecordBatch(MakeBatch(10), "part1");
  EndWriterChecked(dataset_writer.get());
  AssertFilesCreated({"testdir/part0/chunk-0.arrow", "testdir/part0/chunk-1.arrow",
                      "testdir/part1/chunk-0.arrow", "testdir/part2/chunk-0.arrow"});
}

TEST_F(DatasetWriterTestFixture, NoExistingDirectory) {
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<fs::FileSystem> fs,
                       MockFileSystem::Make(mock_now, {::arrow::fs::Dir("testdir")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
  write_options_.filesystem = filesystem_;
  write_options_.existing_data_behavior = ExistingDataBehavior::kDeleteMatchingPartitions;
  write_options_.base_dir = "testdir/subdir";
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(100), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/subdir/chunk-0.arrow", 0, 100}});
}

TEST_F(DatasetWriterTestFixture, DeleteExistingData) {
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<fs::FileSystem> fs,
      MockFileSystem::Make(
          mock_now, {::arrow::fs::Dir("testdir"), fs::File("testdir/subdir/foo.txt"),
                     fs::File("testdir/chunk-5.arrow"), fs::File("testdir/blah.txt")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
  write_options_.filesystem = filesystem_;
  write_options_.existing_data_behavior = ExistingDataBehavior::kDeleteMatchingPartitions;
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(100), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0.arrow", 0, 100}});
  AssertNotFiles({"testdir/chunk-5.arrow", "testdir/blah.txt", "testdir/subdir/foo.txt"});
}

TEST_F(DatasetWriterTestFixture, PartitionedDeleteExistingData) {
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<fs::FileSystem> fs,
      MockFileSystem::Make(
          mock_now, {::arrow::fs::Dir("testdir"), fs::File("testdir/part0/foo.arrow"),
                     fs::File("testdir/part1/bar.arrow")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
  write_options_.filesystem = filesystem_;
  write_options_.existing_data_behavior = ExistingDataBehavior::kDeleteMatchingPartitions;
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(100), "part0");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/part0/chunk-0.arrow", 0, 100}});
  AssertNotFiles({"testdir/part0/foo.arrow"});
  AssertEmptyFiles({"testdir/part1/bar.arrow"});
}

TEST_F(DatasetWriterTestFixture, LeaveExistingData) {
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<fs::FileSystem> fs,
      MockFileSystem::Make(
          mock_now, {::arrow::fs::Dir("testdir"), fs::File("testdir/chunk-0.arrow"),
                     fs::File("testdir/chunk-5.arrow"), fs::File("testdir/blah.txt")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
  write_options_.filesystem = filesystem_;
  write_options_.existing_data_behavior = ExistingDataBehavior::kOverwriteOrIgnore;
  auto dataset_writer = MakeDatasetWriter();
  dataset_writer->WriteRecordBatch(MakeBatch(100), "");
  EndWriterChecked(dataset_writer.get());
  AssertCreatedData({{"testdir/chunk-0.arrow", 0, 100}});
  AssertEmptyFiles({"testdir/chunk-5.arrow", "testdir/blah.txt"});
}

TEST_F(DatasetWriterTestFixture, ErrOnExistingData) {
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<fs::FileSystem> fs,
      MockFileSystem::Make(
          mock_now, {::arrow::fs::Dir("testdir"), fs::File("testdir/chunk-0.arrow"),
                     fs::File("testdir/chunk-5.arrow"), fs::File("testdir/blah.txt")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
  write_options_.filesystem = filesystem_;
  ASSERT_RAISES(Invalid, DatasetWriter::Make(
                             write_options_, scheduler_, [] {}, [] {}, [] {}));
  AssertEmptyFiles(
      {"testdir/chunk-0.arrow", "testdir/chunk-5.arrow", "testdir/blah.txt"});

  // only a single file in the target directory
  fs::TimePoint mock_now2 = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<fs::FileSystem> fs2,
      MockFileSystem::Make(
          mock_now2, {::arrow::fs::Dir("testdir"), fs::File("testdir/part-0.arrow")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs2);
  write_options_.filesystem = filesystem_;
  write_options_.base_dir = "testdir";
  ASSERT_RAISES(Invalid, DatasetWriter::Make(
                             write_options_, scheduler_, [] {}, [] {}, [] {}));
  AssertEmptyFiles({"testdir/part-0.arrow"});
}

}  // namespace internal
}  // namespace dataset
}  // namespace arrow

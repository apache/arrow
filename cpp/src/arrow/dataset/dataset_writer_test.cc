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
#include <vector>

#include "arrow/dataset/file_ipc.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/optional.h"
#include "gtest/gtest.h"

namespace arrow {
namespace dataset {

using arrow::fs::internal::MockFileInfo;
using arrow::fs::internal::MockFileSystem;

struct ExpectedFile {
  std::string filename;
  uint64_t start;
  uint64_t num_rows;
};

class DatasetWriterTestFixture : public testing::Test {
 protected:
  void SetUp() override {
    fs::TimePoint mock_now = std::chrono::system_clock::now();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<fs::FileSystem> fs,
                         MockFileSystem::Make(mock_now, {::arrow::fs::Dir("testdir")}));
    filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
    schema_ = schema({field("int64", int64())});
    write_options_.filesystem = filesystem_;
    write_options_.basename_template = "part-{i}.arrow";
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
  }

  std::shared_ptr<fs::GatedMockFilesystem> UseGatedFs() {
    fs::TimePoint mock_now = std::chrono::system_clock::now();
    auto fs = std::make_shared<fs::GatedMockFilesystem>(mock_now);
    ARROW_EXPECT_OK(fs->CreateDir("testdir"));
    write_options_.filesystem = fs;
    filesystem_ = fs;
    return fs;
  }

  std::shared_ptr<RecordBatch> MakeBatch(uint64_t num_rows) {
    Int64Builder builder;
    for (uint64_t i = counter_; i < counter_ + num_rows; i++) {
      ARROW_EXPECT_OK(builder.Append(i));
    }
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<Array> arr, builder.Finish());
    counter_ += num_rows;
    return RecordBatch::Make(schema_, static_cast<int64_t>(num_rows), {std::move(arr)});
  }

  util::optional<MockFileInfo> FindFile(const std::string& filename) {
    for (const auto& mock_file : filesystem_->AllFiles()) {
      if (mock_file.full_path == filename) {
        return mock_file;
      }
    }
    return util::nullopt;
  }

  void AssertVisited(const std::vector<std::string>& actual_paths,
                     const std::string& expected_path) {
    std::vector<std::string>::const_iterator found =
        std::find(actual_paths.begin(), actual_paths.end(), expected_path);
    ASSERT_NE(found, actual_paths.end())
        << "The file " << expected_path << " was not in the list of files visited";
  }

  void AssertFiles(const std::vector<ExpectedFile>& expected_files) {
    for (const auto& expected_file : expected_files) {
      util::optional<MockFileInfo> written_file = FindFile(expected_file.filename);
      ASSERT_TRUE(written_file.has_value())
          << "The file " << expected_file.filename << " was not created";
      {
        SCOPED_TRACE("pre_finish");
        AssertVisited(pre_finish_visited_, expected_file.filename);
      }
      {
        SCOPED_TRACE("post_finish");
        AssertVisited(post_finish_visited_, expected_file.filename);
      }
      // FIXME Check contents
    }
  }

  void AssertNotFiles(const std::vector<std::string>& expected_non_files) {
    for (const auto& expected_non_file : expected_non_files) {
      util::optional<MockFileInfo> file = FindFile(expected_non_file);
      ASSERT_FALSE(file.has_value());
    }
  }

  void AssertEmptyFiles(const std::vector<std::string>& expected_empty_files) {
    for (const auto& expected_empty_file : expected_empty_files) {
      util::optional<MockFileInfo> file = FindFile(expected_empty_file);
      ASSERT_TRUE(file.has_value());
      ASSERT_EQ("", file->data);
    }
  }

  std::shared_ptr<MockFileSystem> filesystem_;
  std::shared_ptr<Schema> schema_;
  std::vector<std::string> pre_finish_visited_;
  std::vector<std::string> post_finish_visited_;
  FileSystemDatasetWriteOptions write_options_;
  uint64_t counter_ = 0;
};

TEST_F(DatasetWriterTestFixture, Basic) {
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));
  Future<> queue_fut = dataset_writer->WriteRecordBatch(MakeBatch(100), "");
  AssertFinished(queue_fut);
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles({{"testdir/part-0.arrow", 0, 100}});
}

TEST_F(DatasetWriterTestFixture, MaxRowsOneWrite) {
  write_options_.max_rows_per_file = 10;
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));
  Future<> queue_fut = dataset_writer->WriteRecordBatch(MakeBatch(35), "");
  AssertFinished(queue_fut);
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles({{"testdir/part-0.arrow", 0, 10},
               {"testdir/part-1.arrow", 10, 20},
               {"testdir/part-2.arrow", 20, 30},
               {"testdir/part-3.arrow", 30, 35}});
}

TEST_F(DatasetWriterTestFixture, MaxRowsManyWrites) {
  write_options_.max_rows_per_file = 10;
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(3), ""));
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(3), ""));
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(3), ""));
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(3), ""));
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(3), ""));
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(3), ""));
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles({{"testdir/part-0.arrow", 0, 10}, {"testdir/part-1.arrow", 10, 8}});
}

TEST_F(DatasetWriterTestFixture, ConcurrentWritesSameFile) {
  auto gated_fs = UseGatedFs();
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));
  for (int i = 0; i < 10; i++) {
    Future<> queue_fut = dataset_writer->WriteRecordBatch(MakeBatch(10), "");
    AssertFinished(queue_fut);
    ASSERT_FINISHES_OK(queue_fut);
  }
  ASSERT_OK(gated_fs->WaitForOpenOutputStream(1));
  ASSERT_OK(gated_fs->UnlockOpenOutputStream(1));
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles({{"testdir/part-0.arrow", 0, 100}});
}

TEST_F(DatasetWriterTestFixture, ConcurrentWritesDifferentFiles) {
  // NBATCHES must be less than I/O executor concurrency to avoid deadlock / test failure
  constexpr int NBATCHES = 6;
  auto gated_fs = UseGatedFs();
  std::vector<ExpectedFile> expected_files;
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));
  for (int i = 0; i < NBATCHES; i++) {
    std::string i_str = std::to_string(i);
    expected_files.push_back(ExpectedFile{"testdir/part" + i_str + "/part-0.arrow",
                                          static_cast<uint64_t>(i) * 10,
                                          (static_cast<uint64_t>(i + 1) * 10)});
    Future<> queue_fut = dataset_writer->WriteRecordBatch(MakeBatch(10), "part" + i_str);
    AssertFinished(queue_fut);
    ASSERT_FINISHES_OK(queue_fut);
  }
  ASSERT_OK(gated_fs->WaitForOpenOutputStream(NBATCHES));
  ASSERT_OK(gated_fs->UnlockOpenOutputStream(NBATCHES));
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles(expected_files);
}

TEST_F(DatasetWriterTestFixture, MaxOpenFiles) {
  auto gated_fs = UseGatedFs();
  write_options_.max_open_files = 2;
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));

  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(10), "part0"));
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(10), "part1"));
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(10), "part0"));
  Future<> fut = dataset_writer->WriteRecordBatch(MakeBatch(10), "part2");
  // Backpressure will be applied until an existing file can be evicted
  AssertNotFinished(fut);

  // Ungate the writes to relieve the pressure, testdir/part0 should be closed
  ASSERT_OK(gated_fs->WaitForOpenOutputStream(2));
  ASSERT_OK(gated_fs->UnlockOpenOutputStream(5));
  ASSERT_FINISHES_OK(fut);

  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(10), "part0"));
  // Following call should resume existing write but, on slow test systems, the old
  // write may have already been finished
  ASSERT_FINISHES_OK(dataset_writer->WriteRecordBatch(MakeBatch(10), "part1"));
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles({{"testdir/part0/part-0.arrow", 0, 10},
               {"testdir/part0/part-0.arrow", 20, 10},
               {"testdir/part0/part-1.arrow", 40, 10},
               {"testdir/part1/part-0.arrow", 10, 10},
               {"testdir/part1/part-0.arrow", 50, 10},
               {"testdir/part2/part-0.arrow", 30, 10}});
}

TEST_F(DatasetWriterTestFixture, DeleteExistingData) {
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<fs::FileSystem> fs,
                       MockFileSystem::Make(mock_now, {::arrow::fs::Dir("testdir"),
                                                       fs::File("testdir/part-5.arrow"),
                                                       fs::File("testdir/blah.txt")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
  write_options_.filesystem = filesystem_;
  write_options_.existing_data_behavior = kDeleteMatchingPartitions;
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));
  Future<> queue_fut = dataset_writer->WriteRecordBatch(MakeBatch(100), "");
  AssertFinished(queue_fut);
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles({{"testdir/part-0.arrow", 0, 100}});
  AssertNotFiles({"testdir/part-5.arrow", "testdir/blah.txt"});
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
  write_options_.existing_data_behavior = kDeleteMatchingPartitions;
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));
  Future<> queue_fut = dataset_writer->WriteRecordBatch(MakeBatch(100), "part0");
  AssertFinished(queue_fut);
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles({{"testdir/part0/part-0.arrow", 0, 100}});
  AssertNotFiles({"testdir/part0/foo.arrow"});
  AssertEmptyFiles({"testdir/part1/bar.arrow"});
}

TEST_F(DatasetWriterTestFixture, LeaveExistingData) {
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<fs::FileSystem> fs,
      MockFileSystem::Make(
          mock_now, {::arrow::fs::Dir("testdir"), fs::File("testdir/part-0.arrow"),
                     fs::File("testdir/part-5.arrow"), fs::File("testdir/blah.txt")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
  write_options_.filesystem = filesystem_;
  write_options_.existing_data_behavior = kOverwriteOrIgnore;
  EXPECT_OK_AND_ASSIGN(auto dataset_writer, DatasetWriter::Make(write_options_));
  Future<> queue_fut = dataset_writer->WriteRecordBatch(MakeBatch(100), "");
  AssertFinished(queue_fut);
  ASSERT_FINISHES_OK(dataset_writer->Finish());
  AssertFiles({{"testdir/part-0.arrow", 0, 100}});
  AssertEmptyFiles({"testdir/part-5.arrow", "testdir/blah.txt"});
}

TEST_F(DatasetWriterTestFixture, ErrOnExistingData) {
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<fs::FileSystem> fs,
      MockFileSystem::Make(
          mock_now, {::arrow::fs::Dir("testdir"), fs::File("testdir/part-0.arrow"),
                     fs::File("testdir/part-5.arrow"), fs::File("testdir/blah.txt")}));
  filesystem_ = std::dynamic_pointer_cast<MockFileSystem>(fs);
  write_options_.filesystem = filesystem_;
  ASSERT_RAISES(Invalid, DatasetWriter::Make(write_options_));
  AssertEmptyFiles({"testdir/part-0.arrow", "testdir/part-5.arrow", "testdir/blah.txt"});
}

}  // namespace dataset
}  // namespace arrow

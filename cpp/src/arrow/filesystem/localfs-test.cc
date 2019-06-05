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

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/test-util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io-util.h"

namespace arrow {
namespace fs {
namespace internal {

using ::arrow::internal::TemporaryDir;

TimePoint CurrentTimePoint() {
  auto now = std::chrono::system_clock::now();
  return TimePoint(
      std::chrono::duration_cast<TimePoint::duration>(now.time_since_epoch()));
}

////////////////////////////////////////////////////////////////////////////
// Generic LocalFileSystem tests

class TestLocalFSGeneric : public ::testing::Test, public GenericFileSystemTest {
 public:
  void SetUp() override {
    ASSERT_OK(TemporaryDir::Make("test-localfs-", &temp_dir_));
    local_fs_ = std::make_shared<LocalFileSystem>();
    fs_ = std::make_shared<SubTreeFileSystem>(temp_dir_->path().ToString(), local_fs_);
  }

 protected:
  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  std::unique_ptr<TemporaryDir> temp_dir_;
  std::shared_ptr<LocalFileSystem> local_fs_;
  std::shared_ptr<FileSystem> fs_;
};

GENERIC_FS_TEST_FUNCTIONS(TestLocalFSGeneric);

////////////////////////////////////////////////////////////////////////////
// Concrete LocalFileSystem tests

class TestLocalFS : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK(TemporaryDir::Make("test-localfs-", &temp_dir_));
    local_fs_ = std::make_shared<LocalFileSystem>();
    fs_ = std::make_shared<SubTreeFileSystem>(temp_dir_->path().ToString(), local_fs_);
  }

 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::shared_ptr<LocalFileSystem> local_fs_;
  std::shared_ptr<FileSystem> fs_;
};

TEST_F(TestLocalFS, DirectoryMTime) {
  TimePoint t1 = CurrentTimePoint();
  ASSERT_OK(fs_->CreateDir("AB/CD/EF"));
  TimePoint t2 = CurrentTimePoint();

  std::vector<FileStats> stats;
  ASSERT_OK(fs_->GetTargetStats({"AB", "AB/CD/EF"}, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB", FileType::Directory);
  AssertFileStats(stats[1], "AB/CD/EF", FileType::Directory);

  // NOTE: creating AB/CD updates AB's modification time, but creating
  // AB/CD/EF doesn't.  So AB/CD/EF's modification time should always be
  // the same as or after AB's modification time.
  AssertDurationBetween(stats[1].mtime() - stats[0].mtime(), 0, kTimeSlack);
  // Depending on filesystem time granularity, the recorded time could be
  // before the system time when doing the modification.
  AssertDurationBetween(stats[0].mtime() - t1, -kTimeSlack, kTimeSlack);
  AssertDurationBetween(t2 - stats[1].mtime(), -kTimeSlack, kTimeSlack);
}

TEST_F(TestLocalFS, FileMTime) {
  TimePoint t1 = CurrentTimePoint();
  ASSERT_OK(fs_->CreateDir("AB/CD"));
  CreateFile(fs_.get(), "AB/CD/ab", "data");
  TimePoint t2 = CurrentTimePoint();

  std::vector<FileStats> stats;
  ASSERT_OK(fs_->GetTargetStats({"AB", "AB/CD/ab"}, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB", FileType::Directory);
  AssertFileStats(stats[1], "AB/CD/ab", FileType::File, 4);

  AssertDurationBetween(stats[1].mtime() - stats[0].mtime(), 0, kTimeSlack);
  AssertDurationBetween(stats[0].mtime() - t1, -kTimeSlack, kTimeSlack);
  AssertDurationBetween(t2 - stats[1].mtime(), -kTimeSlack, kTimeSlack);
}

// TODO check UNC paths on Windows, e.g. "\\server\share\path\file" (networked)
// or "\\?\C:\path\file" (extended-length paths)

}  // namespace internal
}  // namespace fs
}  // namespace arrow

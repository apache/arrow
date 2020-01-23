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

#include <cerrno>
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace fs {
namespace internal {

using ::arrow::internal::PlatformFilename;
using ::arrow::internal::TemporaryDir;

class LocalFSTestMixin : public ::testing::Test {
 public:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("test-localfs-"));
  }

 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;
};

struct CommonPathFormatter {
  std::string operator()(std::string fn) { return fn; }
  bool supports_uri() { return true; }
};

#ifdef _WIN32
struct ExtendedLengthPathFormatter {
  std::string operator()(std::string fn) { return "//?/" + fn; }
  // The path prefix conflicts with URI syntax
  bool supports_uri() { return false; }
};

using PathFormatters = ::testing::Types<CommonPathFormatter, ExtendedLengthPathFormatter>;
#else
using PathFormatters = ::testing::Types<CommonPathFormatter>;
#endif

////////////////////////////////////////////////////////////////////////////
// Generic LocalFileSystem tests

template <typename PathFormatter>
class TestLocalFSGeneric : public LocalFSTestMixin, public GenericFileSystemTest {
 public:
  void SetUp() override {
    LocalFSTestMixin::SetUp();
    local_fs_ = std::make_shared<LocalFileSystem>(options());
    auto path = PathFormatter()(temp_dir_->path().ToString());
    fs_ = std::make_shared<SubTreeFileSystem>(path, local_fs_);
  }

 protected:
  virtual LocalFileSystemOptions options() { return LocalFileSystemOptions::Defaults(); }

  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  std::shared_ptr<LocalFileSystem> local_fs_;
  std::shared_ptr<FileSystem> fs_;
};

TYPED_TEST_CASE(TestLocalFSGeneric, PathFormatters);

GENERIC_FS_TYPED_TEST_FUNCTIONS(TestLocalFSGeneric);

class TestLocalFSGenericMMap : public TestLocalFSGeneric<CommonPathFormatter> {
 protected:
  LocalFileSystemOptions options() override {
    auto options = LocalFileSystemOptions::Defaults();
    options.use_mmap = true;
    return options;
  }
};

GENERIC_FS_TEST_FUNCTIONS(TestLocalFSGenericMMap);

////////////////////////////////////////////////////////////////////////////
// Concrete LocalFileSystem tests

template <typename PathFormatter>
class TestLocalFS : public LocalFSTestMixin {
 public:
  void SetUp() {
    LocalFSTestMixin::SetUp();
    path_formatter_ = PathFormatter();
    local_fs_ = std::make_shared<LocalFileSystem>();
    local_path_ = EnsureTrailingSlash(path_formatter_(temp_dir_->path().ToString()));
    fs_ = std::make_shared<SubTreeFileSystem>(local_path_, local_fs_);
  }

  void TestFileSystemFromUri(const std::string& uri) {
    if (!path_formatter_.supports_uri()) {
      return;  // skip
    }
    std::string path;
    ASSERT_OK_AND_ASSIGN(fs_, FileSystemFromUri(uri, &path));
    ASSERT_EQ(path, local_path_);

    // Test that the right location on disk is accessed
    CreateFile(fs_.get(), local_path_ + "abc", "some data");
    CheckConcreteFile(this->temp_dir_->path().ToString() + "abc", 9);
  }

  void TestLocalUri(const std::string& uri, const std::string& expected_path) {
    // Like TestFileSystemFromUri, but with an arbitrary non-existing path
    if (!path_formatter_.supports_uri()) {
      return;  // skip
    }
    std::string path;
    ASSERT_OK_AND_ASSIGN(fs_, FileSystemFromUri(uri, &path));
    ASSERT_EQ(fs_->type_name(), "local");
    ASSERT_EQ(path, expected_path);
  }

  void TestInvalidUri(const std::string& uri) {
    if (!path_formatter_.supports_uri()) {
      return;  // skip
    }
    ASSERT_RAISES(Invalid, FileSystemFromUri(uri));
  }

  void CheckConcreteFile(const std::string& path, int64_t expected_size) {
    ASSERT_OK_AND_ASSIGN(auto fn, PlatformFilename::FromString(path));
    ASSERT_OK_AND_ASSIGN(int fd, ::arrow::internal::FileOpenReadable(fn));
    auto result = ::arrow::internal::FileGetSize(fd);
    ASSERT_OK(::arrow::internal::FileClose(fd));
    ASSERT_OK_AND_ASSIGN(int64_t size, result);
    ASSERT_EQ(size, expected_size);
  }

 protected:
  PathFormatter path_formatter_;
  std::shared_ptr<LocalFileSystem> local_fs_;
  std::shared_ptr<FileSystem> fs_;
  std::string local_path_;
};

TYPED_TEST_CASE(TestLocalFS, PathFormatters);

TYPED_TEST(TestLocalFS, CorrectPathExists) {
  // Test that the right location on disk is accessed
  std::shared_ptr<io::OutputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, this->fs_->OpenOutputStream("abc"));
  std::string data = "some data";
  auto data_size = static_cast<int64_t>(data.size());
  ASSERT_OK(stream->Write(data.data(), data_size));
  ASSERT_OK(stream->Close());

  // Now check the file's existence directly, bypassing the FileSystem abstraction
  this->CheckConcreteFile(this->temp_dir_->path().ToString() + "abc", data_size);
}

TYPED_TEST(TestLocalFS, FileSystemFromUriFile) {
  this->TestFileSystemFromUri("file:" + this->local_path_);
}

TYPED_TEST(TestLocalFS, FileSystemFromUriNoScheme) {
  this->TestFileSystemFromUri(this->local_path_);
}

TYPED_TEST(TestLocalFS, FileSystemFromUriFileBackslashes) {
#ifdef _WIN32
  this->TestFileSystemFromUri(ToBackslashes("file:" + this->local_path_));

  // Variations
  this->TestLocalUri("file:" + this->path_formatter_("C:foo\\bar"), "C:foo/bar");
  this->TestLocalUri("file:" + this->path_formatter_("C:\\foo\\bar"), "C:/foo/bar");
  this->TestLocalUri("file:" + this->path_formatter_("C:bar\\"), "C:bar/");
#else
  this->TestInvalidUri(ToBackslashes("file:" + this->local_path_));
#endif
}

TYPED_TEST(TestLocalFS, FileSystemFromUriNoSchemeBackslashes) {
#ifdef _WIN32
  this->TestFileSystemFromUri(ToBackslashes(this->local_path_));

  // Variations
  this->TestLocalUri(this->path_formatter_("C:foo\\bar"), "C:foo/bar");
  this->TestLocalUri(this->path_formatter_("C:\\foo\\bar"), "C:/foo/bar");
  this->TestLocalUri(this->path_formatter_("C:bar\\"), "C:bar/");
#else
  this->TestInvalidUri(ToBackslashes(this->local_path_));
#endif
}

TYPED_TEST(TestLocalFS, DirectoryMTime) {
  TimePoint t1 = CurrentTimePoint();
  ASSERT_OK(this->fs_->CreateDir("AB/CD/EF"));
  TimePoint t2 = CurrentTimePoint();

  std::vector<FileStats> stats;
  ASSERT_OK_AND_ASSIGN(stats, this->fs_->GetTargetStats({"AB", "AB/CD/EF", "xxx"}));
  ASSERT_EQ(stats.size(), 3);
  AssertFileStats(stats[0], "AB", FileType::Directory);
  AssertFileStats(stats[1], "AB/CD/EF", FileType::Directory);
  AssertFileStats(stats[2], "xxx", FileType::NonExistent);

  // NOTE: creating AB/CD updates AB's modification time, but creating
  // AB/CD/EF doesn't.  So AB/CD/EF's modification time should always be
  // the same as or after AB's modification time.
  AssertDurationBetween(stats[1].mtime() - stats[0].mtime(), 0, kTimeSlack);
  // Depending on filesystem time granularity, the recorded time could be
  // before the system time when doing the modification.
  AssertDurationBetween(stats[0].mtime() - t1, -kTimeSlack, kTimeSlack);
  AssertDurationBetween(t2 - stats[1].mtime(), -kTimeSlack, kTimeSlack);
}

TYPED_TEST(TestLocalFS, FileMTime) {
  TimePoint t1 = CurrentTimePoint();
  ASSERT_OK(this->fs_->CreateDir("AB/CD"));
  CreateFile(this->fs_.get(), "AB/CD/ab", "data");
  TimePoint t2 = CurrentTimePoint();

  std::vector<FileStats> stats;
  ASSERT_OK_AND_ASSIGN(stats, this->fs_->GetTargetStats({"AB", "AB/CD/ab", "xxx"}));
  ASSERT_EQ(stats.size(), 3);
  AssertFileStats(stats[0], "AB", FileType::Directory);
  AssertFileStats(stats[1], "AB/CD/ab", FileType::File, 4);
  AssertFileStats(stats[2], "xxx", FileType::NonExistent);

  AssertDurationBetween(stats[1].mtime() - stats[0].mtime(), 0, kTimeSlack);
  AssertDurationBetween(stats[0].mtime() - t1, -kTimeSlack, kTimeSlack);
  AssertDurationBetween(t2 - stats[1].mtime(), -kTimeSlack, kTimeSlack);
}

// TODO Should we test backslash paths on Windows?
// SubTreeFileSystem isn't compatible with them.

}  // namespace internal
}  // namespace fs
}  // namespace arrow

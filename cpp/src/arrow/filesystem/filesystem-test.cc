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

#include <memory>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/io/interfaces.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace fs {
namespace internal {

TEST(FileStats, BaseName) {
  auto st = FileStats();
  ASSERT_EQ(st.base_name(), "");
  st.set_path("foo");
  ASSERT_EQ(st.base_name(), "foo");
  st.set_path("foo/bar/baz.qux");
  ASSERT_EQ(st.base_name(), "baz.qux");
}

////////////////////////////////////////////////////////////////////////////
// MockFileSystem tests

class TestMockFS : public ::testing::Test {
 public:
  void SetUp() {
    time_ = TimePoint(TimePoint::duration(42));
    fs_ = std::make_shared<MockFileSystem>(time_);
  }

  Status WriteString(io::OutputStream* stream, const std::string& s) {
    return stream->Write(s.data(), static_cast<int64_t>(s.length()));
  }

  void CheckDirs(const std::vector<DirInfo>& expected) {
    ASSERT_EQ(fs_->AllDirs(), expected);
  }

  void CheckDirPaths(const std::vector<std::string>& expected) {
    std::vector<DirInfo> infos;
    infos.reserve(expected.size());
    for (const auto& s : expected) {
      infos.push_back({s, time_});
    }
    ASSERT_EQ(fs_->AllDirs(), infos);
  }

  void CheckFiles(const std::vector<FileInfo>& expected) {
    ASSERT_EQ(fs_->AllFiles(), expected);
  }

  void CreateFile(const std::string& path, const std::string& data) {
    std::shared_ptr<io::OutputStream> stream;
    ASSERT_OK(fs_->OpenAppendStream(path, &stream));
    ASSERT_OK(WriteString(stream.get(), data));
    ASSERT_OK(stream->Close());
  }

 protected:
  TimePoint time_;
  std::shared_ptr<MockFileSystem> fs_;
};

void AssertFileStats(const FileStats& st, const std::string& path, FileType type) {
  ASSERT_EQ(st.path(), path);
  ASSERT_EQ(st.type(), type);
}

void AssertFileStats(const FileStats& st, const std::string& path, FileType type,
                     TimePoint mtime) {
  AssertFileStats(st, path, type);
  ASSERT_EQ(st.mtime(), mtime);
}

void AssertFileStats(const FileStats& st, const std::string& path, FileType type,
                     TimePoint mtime, int64_t size) {
  AssertFileStats(st, path, type, mtime);
  ASSERT_EQ(st.size(), size);
}

TEST_F(TestMockFS, Empty) {
  CheckDirs({});
  CheckFiles({});
}

TEST_F(TestMockFS, CreateDir) {
  ASSERT_OK(fs_->CreateDir("AB"));
  ASSERT_OK(fs_->CreateDir("AB/CD/EF"));  // Recursive
  // Non-recursive, parent doesn't exist
  ASSERT_RAISES(IOError, fs_->CreateDir("AB/GH/IJ", false /* recursive */));
  ASSERT_OK(fs_->CreateDir("AB/GH", false /* recursive */));
  ASSERT_OK(fs_->CreateDir("AB/GH/IJ", false /* recursive */));
  // Idempotency
  ASSERT_OK(fs_->CreateDir("AB/GH/IJ", false /* recursive */));
  ASSERT_OK(fs_->CreateDir("XY"));
  CheckDirs({{"AB", time_},
             {"AB/CD", time_},
             {"AB/CD/EF", time_},
             {"AB/GH", time_},
             {"AB/GH/IJ", time_},
             {"XY", time_}});
  CheckFiles({});

  // Cannot create a directory as child of a file
  CreateFile("AB/cd", "");
  auto st = fs_->CreateDir("AB/cd/EF/GH", true /* recursive */);
  std::string expected_msg =
      "Cannot create directory 'AB/cd/EF/GH': ancestor 'AB/cd' is a regular file";
  ASSERT_RAISES(IOError, st);
  ASSERT_EQ(st.message(), expected_msg);
  st = fs_->CreateDir("AB/cd/EF", false /* recursive */);
  expected_msg = "Cannot create directory 'AB/cd/EF': ancestor 'AB/cd' is a regular file";
  ASSERT_RAISES(IOError, st);
  ASSERT_EQ(st.message(), expected_msg);
}

TEST_F(TestMockFS, DeleteDir) {
  ASSERT_OK(fs_->CreateDir("AB/CD/EF"));
  ASSERT_OK(fs_->CreateDir("AB/GH/IJ"));
  ASSERT_OK(fs_->DeleteDir("AB/CD"));
  ASSERT_OK(fs_->DeleteDir("AB/GH/IJ"));
  CheckDirs({{"AB", time_}, {"AB/GH", time_}});
  CheckFiles({});
  ASSERT_RAISES(IOError, fs_->DeleteDir("AB/CD"));
  ASSERT_OK(fs_->DeleteDir("AB"));
  CheckDirs({});
  CheckFiles({});
}

TEST_F(TestMockFS, DeleteFile) {
  ASSERT_OK(fs_->CreateDir("AB"));
  CreateFile("AB/cd", "data");
  CheckDirs({{"AB", time_}});
  CheckFiles({{"AB/cd", time_, "data"}});

  ASSERT_OK(fs_->DeleteFile("AB/cd"));
  CheckDirs({{"AB", time_}});
  CheckFiles({});

  CreateFile("ab", "data");
  CheckDirs({{"AB", time_}});
  CheckFiles({{"ab", time_, "data"}});

  ASSERT_OK(fs_->DeleteFile("ab"));
  CheckDirs({{"AB", time_}});
  CheckFiles({});

  // File doesn't exist
  ASSERT_RAISES(IOError, fs_->DeleteFile("ab"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("AB/cd"));

  // Not a file
  ASSERT_RAISES(IOError, fs_->DeleteFile("AB"));
  CheckDirs({{"AB", time_}});
  CheckFiles({});
}

TEST_F(TestMockFS, GetTargetStatsSingle) {
  ASSERT_OK(fs_->CreateDir("AB/CD"));
  CreateFile("AB/CD/ef", "some data");

  FileStats st;
  ASSERT_OK(fs_->GetTargetStats("AB", &st));
  AssertFileStats(st, "AB", FileType::Directory, time_);
  ASSERT_EQ(st.base_name(), "AB");
  ASSERT_OK(fs_->GetTargetStats("AB/CD", &st));
  AssertFileStats(st, "AB/CD", FileType::Directory, time_);
  ASSERT_EQ(st.base_name(), "CD");
  ASSERT_OK(fs_->GetTargetStats("AB/CD/ef", &st));
  AssertFileStats(st, "AB/CD/ef", FileType::File, time_, 9);
  ASSERT_EQ(st.base_name(), "ef");
  ASSERT_OK(fs_->GetTargetStats("zz", &st));
  AssertFileStats(st, "zz", FileType::NonExistent);
  ASSERT_EQ(st.base_name(), "zz");

  // Invalid path
  ASSERT_RAISES(Invalid, fs_->GetTargetStats("//foo//bar//baz//", &st));
}

TEST_F(TestMockFS, GetTargetStatsVector) {
  ASSERT_OK(fs_->CreateDir("AB/CD"));
  CreateFile("AB/CD/ef", "some data");

  std::vector<FileStats> stats;
  ASSERT_OK(
      fs_->GetTargetStats({"AB", "AB/CD", "AB/zz", "zz", "XX/zz", "AB/CD/ef"}, &stats));
  ASSERT_EQ(stats.size(), 6);
  AssertFileStats(stats[0], "AB", FileType::Directory, time_);
  AssertFileStats(stats[1], "AB/CD", FileType::Directory, time_);
  AssertFileStats(stats[2], "AB/zz", FileType::NonExistent);
  AssertFileStats(stats[3], "zz", FileType::NonExistent);
  AssertFileStats(stats[4], "XX/zz", FileType::NonExistent);
  AssertFileStats(stats[5], "AB/CD/ef", FileType::File, time_, 9);

  // Invalid path
  ASSERT_RAISES(Invalid,
                fs_->GetTargetStats({"AB", "AB/CD", "//foo//bar//baz//"}, &stats));
}

TEST_F(TestMockFS, GetTargetStatsSelector) {
  // Non-recursive tests for GetTargetStats(Selector, ...).
  ASSERT_OK(fs_->CreateDir("AB/CD"));
  CreateFile("ab", "data");
  CreateFile("AB/cd", "some data");
  CreateFile("AB/CD/ef", "some other data");
  CreateFile("AB/CD/gh", "yet other data");

  Selector s;
  s.base_dir = "";
  std::vector<FileStats> stats;
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB", FileType::Directory, time_);
  AssertFileStats(stats[1], "ab", FileType::File, time_, 4);

  s.base_dir = "AB";
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB/CD", FileType::Directory, time_);
  AssertFileStats(stats[1], "AB/cd", FileType::File, time_, 9);

  s.base_dir = "AB/CD";
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB/CD/ef", FileType::File, time_, 15);
  AssertFileStats(stats[1], "AB/CD/gh", FileType::File, time_, 14);

  // Doesn't exist
  s.base_dir = "XX";
  ASSERT_RAISES(IOError, fs_->GetTargetStats(s, &stats));
  s.allow_non_existent = true;
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 0);
  s.allow_non_existent = false;

  // Not a dir
  s.base_dir = "ab";
  ASSERT_RAISES(IOError, fs_->GetTargetStats(s, &stats));
}

TEST_F(TestMockFS, GetTargetStatsSelectorRecursive) {
  // Recursive tests for GetTargetStats(Selector, ...).
  ASSERT_OK(fs_->CreateDir("AB/CD"));
  CreateFile("ab", "data");
  CreateFile("AB/cd", "some data");
  CreateFile("AB/CD/ef", "some other data");
  CreateFile("AB/CD/gh", "yet other data");

  Selector s;
  s.base_dir = "";
  s.recursive = true;
  std::vector<FileStats> stats;
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 6);
  AssertFileStats(stats[0], "AB", FileType::Directory, time_);
  AssertFileStats(stats[1], "AB/CD", FileType::Directory, time_);
  AssertFileStats(stats[2], "AB/CD/ef", FileType::File, time_, 15);
  AssertFileStats(stats[3], "AB/CD/gh", FileType::File, time_, 14);
  AssertFileStats(stats[4], "AB/cd", FileType::File, time_, 9);
  AssertFileStats(stats[5], "ab", FileType::File, time_, 4);

  s.base_dir = "AB";
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 4);
  AssertFileStats(stats[0], "AB/CD", FileType::Directory, time_);
  AssertFileStats(stats[1], "AB/CD/ef", FileType::File, time_, 15);
  AssertFileStats(stats[2], "AB/CD/gh", FileType::File, time_, 14);
  AssertFileStats(stats[3], "AB/cd", FileType::File, time_, 9);

  // Doesn't exist
  s.base_dir = "XX";
  ASSERT_RAISES(IOError, fs_->GetTargetStats(s, &stats));
  s.allow_non_existent = true;
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 0);
  s.allow_non_existent = false;

  // Not a dir
  s.base_dir = "ab";
  ASSERT_RAISES(IOError, fs_->GetTargetStats(s, &stats));
}

TEST_F(TestMockFS, OpenOutputStream) {
  std::shared_ptr<io::OutputStream> stream;
  int64_t position = -1;

  ASSERT_OK(fs_->OpenOutputStream("ab", &stream));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 0);
  ASSERT_FALSE(stream->closed());
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(stream->closed());
  CheckDirs({});
  CheckFiles({{"ab", time_, ""}});

  // Parent does not exist
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("ab/cd", &stream));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("ef/gh", &stream));
  CheckDirs({});
  CheckFiles({{"ab", time_, ""}});

  ASSERT_OK(fs_->CreateDir("CD"));
  ASSERT_OK(fs_->OpenOutputStream("CD/ef", &stream));
  ASSERT_OK(WriteString(stream.get(), "some "));
  ASSERT_OK(WriteString(stream.get(), "data"));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 9);
  ASSERT_OK(stream->Close());
  CheckDirs({{"CD", time_}});
  CheckFiles({{"CD/ef", time_, "some data"}, {"ab", time_, ""}});

  // Overwrite
  ASSERT_OK(fs_->OpenOutputStream("CD/ef", &stream));
  ASSERT_OK(WriteString(stream.get(), "overwritten"));
  ASSERT_OK(stream->Close());
  CheckDirs({{"CD", time_}});
  CheckFiles({{"CD/ef", time_, "overwritten"}, {"ab", time_, ""}});

  // Cannot turn dir into file
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("CD", &stream));
  CheckDirs({{"CD", time_}});
}

TEST_F(TestMockFS, OpenAppendStream) {
  std::shared_ptr<io::OutputStream> stream;
  int64_t position = -1;

  ASSERT_OK(fs_->OpenAppendStream("ab", &stream));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 0);
  ASSERT_OK(WriteString(stream.get(), "some "));
  ASSERT_OK(WriteString(stream.get(), "data"));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 9);
  ASSERT_OK(stream->Close());
  CheckDirs({});
  CheckFiles({{"ab", time_, "some data"}});

  ASSERT_OK(fs_->OpenAppendStream("ab", &stream));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 9);
  ASSERT_OK(WriteString(stream.get(), " appended"));
  ASSERT_OK(stream->Close());
  CheckDirs({});
  CheckFiles({{"ab", time_, "some data appended"}});
}

TEST_F(TestMockFS, OpenInputStream) {
  ASSERT_OK(fs_->CreateDir("AB"));
  CreateFile("AB/ab", "some data");

  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(fs_->OpenInputStream("AB/ab", &stream));
  ASSERT_OK(stream->Read(4, &buffer));
  AssertBufferEqual(*buffer, "some");
  ASSERT_OK(stream->Read(6, &buffer));
  AssertBufferEqual(*buffer, " data");
  ASSERT_OK(stream->Read(1, &buffer));
  AssertBufferEqual(*buffer, "");
  ASSERT_OK(stream->Close());
  ASSERT_RAISES(Invalid, stream->Read(1, &buffer));  // Stream is closed

  // File does not exist
  ASSERT_RAISES(IOError, fs_->OpenInputStream("AB/cd", &stream));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("cd", &stream));
}

TEST_F(TestMockFS, OpenInputFile) {
  ASSERT_OK(fs_->CreateDir("AB"));
  CreateFile("AB/ab", "some other data");

  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buffer;
  int64_t size = -1;
  ASSERT_OK(fs_->OpenInputFile("AB/ab", &file));
  ASSERT_OK(file->ReadAt(5, 6, &buffer));
  AssertBufferEqual(*buffer, "other ");
  ASSERT_OK(file->GetSize(&size));
  ASSERT_EQ(size, 15);
  ASSERT_OK(file->Close());
  ASSERT_RAISES(Invalid, file->ReadAt(1, 1, &buffer));  // Stream is closed

  // File does not exist
  ASSERT_RAISES(IOError, fs_->OpenInputFile("AB/cd", &file));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("cd", &file));
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow

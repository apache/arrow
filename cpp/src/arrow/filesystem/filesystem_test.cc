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
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace fs {
namespace internal {

void AssertPartsEqual(const std::vector<std::string>& parts,
                      const std::vector<std::string>& expected) {
  ASSERT_EQ(parts, expected);
}

void AssertPairEqual(const std::pair<std::string, std::string>& pair,
                     const std::pair<std::string, std::string>& expected) {
  ASSERT_EQ(pair, expected);
}

TEST(FileStats, BaseName) {
  auto st = FileStats();
  ASSERT_EQ(st.base_name(), "");
  st.set_path("foo");
  ASSERT_EQ(st.base_name(), "foo");
  st.set_path("foo/bar/baz.qux");
  ASSERT_EQ(st.base_name(), "baz.qux");
}

TEST(PathUtil, SplitAbstractPath) {
  std::vector<std::string> parts;

  parts = SplitAbstractPath("");
  AssertPartsEqual(parts, {});
  parts = SplitAbstractPath("abc");
  AssertPartsEqual(parts, {"abc"});
  parts = SplitAbstractPath("abc/def.ghi");
  AssertPartsEqual(parts, {"abc", "def.ghi"});
  parts = SplitAbstractPath("abc/def/ghi");
  AssertPartsEqual(parts, {"abc", "def", "ghi"});
  parts = SplitAbstractPath("abc\\def\\ghi");
  AssertPartsEqual(parts, {"abc\\def\\ghi"});

  // Trailing slash
  parts = SplitAbstractPath("abc/");
  AssertPartsEqual(parts, {"abc"});
  parts = SplitAbstractPath("abc/def.ghi/");
  AssertPartsEqual(parts, {"abc", "def.ghi"});
  parts = SplitAbstractPath("abc/def.ghi\\");
  AssertPartsEqual(parts, {"abc", "def.ghi\\"});

  // Leading slash
  parts = SplitAbstractPath("/");
  AssertPartsEqual(parts, {});
  parts = SplitAbstractPath("/abc");
  AssertPartsEqual(parts, {"abc"});
  parts = SplitAbstractPath("/abc/def.ghi");
  AssertPartsEqual(parts, {"abc", "def.ghi"});
  parts = SplitAbstractPath("/abc/def.ghi/");
  AssertPartsEqual(parts, {"abc", "def.ghi"});
}

TEST(PathUtil, GetAbstractPathParent) {
  std::pair<std::string, std::string> pair;

  pair = GetAbstractPathParent("");
  AssertPairEqual(pair, {"", ""});
  pair = GetAbstractPathParent("abc");
  AssertPairEqual(pair, {"", "abc"});
  pair = GetAbstractPathParent("abc/def/ghi");
  AssertPairEqual(pair, {"abc/def", "ghi"});
  pair = GetAbstractPathParent("abc/def\\ghi");
  AssertPairEqual(pair, {"abc", "def\\ghi"});
}

TEST(PathUtil, ValidateAbstractPathParts) {
  ASSERT_OK(ValidateAbstractPathParts({}));
  ASSERT_OK(ValidateAbstractPathParts({"abc"}));
  ASSERT_OK(ValidateAbstractPathParts({"abc", "def"}));
  ASSERT_OK(ValidateAbstractPathParts({"abc", "def.ghi"}));
  ASSERT_OK(ValidateAbstractPathParts({"abc", "def\\ghi"}));

  // Empty path component
  ASSERT_RAISES(Invalid, ValidateAbstractPathParts({""}));
  ASSERT_RAISES(Invalid, ValidateAbstractPathParts({"abc", "", "def"}));

  // Separator in component
  ASSERT_RAISES(Invalid, ValidateAbstractPathParts({"/"}));
  ASSERT_RAISES(Invalid, ValidateAbstractPathParts({"abc/def"}));
}

TEST(PathUtil, ConcatAbstractPath) {
  ASSERT_EQ("abc", ConcatAbstractPath("", "abc"));
  ASSERT_EQ("abc/def", ConcatAbstractPath("abc", "def"));
  ASSERT_EQ("abc/def/ghi", ConcatAbstractPath("abc/def", "ghi"));

  ASSERT_EQ("abc/def", ConcatAbstractPath("abc/", "def"));
  ASSERT_EQ("abc/def/ghi", ConcatAbstractPath("abc/def/", "ghi"));

  ASSERT_EQ("/abc", ConcatAbstractPath("/", "abc"));
  ASSERT_EQ("/abc/def", ConcatAbstractPath("/abc", "def"));
  ASSERT_EQ("/abc/def", ConcatAbstractPath("/abc/", "def"));
}

TEST(PathUtil, JoinAbstractPath) {
  std::vector<std::string> parts = {"abc", "def", "ghi", "jkl"};

  ASSERT_EQ("abc/def/ghi/jkl", JoinAbstractPath(parts.begin(), parts.end()));
  ASSERT_EQ("def/ghi", JoinAbstractPath(parts.begin() + 1, parts.begin() + 3));
  ASSERT_EQ("", JoinAbstractPath(parts.begin(), parts.begin()));
}

TEST(PathUtil, EnsureTrailingSlash) {
  ASSERT_EQ("", EnsureTrailingSlash(""));
  ASSERT_EQ("/", EnsureTrailingSlash("/"));
  ASSERT_EQ("abc/", EnsureTrailingSlash("abc"));
  ASSERT_EQ("abc/", EnsureTrailingSlash("abc/"));
  ASSERT_EQ("/abc/", EnsureTrailingSlash("/abc"));
  ASSERT_EQ("/abc/", EnsureTrailingSlash("/abc/"));
}

////////////////////////////////////////////////////////////////////////////
// Generic MockFileSystem tests

class TestMockFSGeneric : public ::testing::Test, public GenericFileSystemTest {
 public:
  void SetUp() override {
    time_ = TimePoint(TimePoint::duration(42));
    fs_ = std::make_shared<MockFileSystem>(time_);
  }

 protected:
  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  TimePoint time_;
  std::shared_ptr<MockFileSystem> fs_;
};

GENERIC_FS_TEST_FUNCTIONS(TestMockFSGeneric);

////////////////////////////////////////////////////////////////////////////
// Concrete MockFileSystem tests

class TestMockFS : public ::testing::Test {
 public:
  void SetUp() override {
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
    ::arrow::fs::CreateFile(fs_.get(), path, data);
  }

 protected:
  TimePoint time_;
  std::shared_ptr<MockFileSystem> fs_;
};

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
}

TEST_F(TestMockFS, GetTargetStatsSingle) {
  ASSERT_OK(fs_->CreateDir("AB/CD"));
  CreateFile("AB/CD/ef", "some data");

  FileStats st;
  ASSERT_OK(fs_->GetTargetStats("AB", &st));
  AssertFileStats(st, "AB", FileType::Directory, time_);
  ASSERT_EQ(st.base_name(), "AB");
  ASSERT_OK(fs_->GetTargetStats("AB/CD/ef", &st));
  AssertFileStats(st, "AB/CD/ef", FileType::File, time_, 9);
  ASSERT_EQ(st.base_name(), "ef");

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
  ASSERT_OK(fs_->CreateDir("AB/CD"));
  CreateFile("ab", "data");

  Selector s;
  s.base_dir = "";
  std::vector<FileStats> stats;
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB", FileType::Directory, time_);
  AssertFileStats(stats[1], "ab", FileType::File, time_, 4);

  s.recursive = true;
  ASSERT_OK(fs_->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 3);
  AssertFileStats(stats[0], "AB", FileType::Directory, time_);
  AssertFileStats(stats[1], "AB/CD", FileType::Directory, time_);
  AssertFileStats(stats[2], "ab", FileType::File, time_, 4);

  // Invalid path
  s.base_dir = "//foo//bar//baz//";
  ASSERT_RAISES(Invalid, fs_->GetTargetStats(s, &stats));
}

TEST_F(TestMockFS, OpenOutputStream) {
  std::shared_ptr<io::OutputStream> stream;

  ASSERT_OK(fs_->OpenOutputStream("ab", &stream));
  ASSERT_OK(stream->Close());
  CheckDirs({});
  CheckFiles({{"ab", time_, ""}});
}

TEST_F(TestMockFS, OpenAppendStream) {
  std::shared_ptr<io::OutputStream> stream;

  ASSERT_OK(fs_->OpenAppendStream("ab", &stream));
  ASSERT_OK(WriteString(stream.get(), "some "));
  ASSERT_OK(stream->Close());

  ASSERT_OK(fs_->OpenAppendStream("ab", &stream));
  ASSERT_OK(WriteString(stream.get(), "data"));
  ASSERT_OK(stream->Close());
  CheckDirs({});
  CheckFiles({{"ab", time_, "some data"}});
}

////////////////////////////////////////////////////////////////////////////
// Concrete SubTreeFileSystem tests

class TestSubTreeFileSystem : public TestMockFS {
 public:
  void SetUp() override {
    TestMockFS::SetUp();
    ASSERT_OK(fs_->CreateDir("sub/tree"));
    subfs_ = std::make_shared<SubTreeFileSystem>("sub/tree", fs_);
  }

  void CreateFile(const std::string& path, const std::string& data) {
    ::arrow::fs::CreateFile(subfs_.get(), path, data);
  }

 protected:
  std::shared_ptr<SubTreeFileSystem> subfs_;
};

TEST_F(TestSubTreeFileSystem, CreateDir) {
  ASSERT_OK(subfs_->CreateDir("AB"));
  ASSERT_OK(subfs_->CreateDir("AB/CD/EF"));  // Recursive
  // Non-recursive, parent doesn't exist
  ASSERT_RAISES(IOError, subfs_->CreateDir("AB/GH/IJ", false /* recursive */));
  ASSERT_OK(subfs_->CreateDir("AB/GH", false /* recursive */));
  ASSERT_OK(subfs_->CreateDir("AB/GH/IJ", false /* recursive */));
  // Can't create root dir
  ASSERT_RAISES(IOError, subfs_->CreateDir(""));
  CheckDirs({{"sub", time_},
             {"sub/tree", time_},
             {"sub/tree/AB", time_},
             {"sub/tree/AB/CD", time_},
             {"sub/tree/AB/CD/EF", time_},
             {"sub/tree/AB/GH", time_},
             {"sub/tree/AB/GH/IJ", time_}});
  CheckFiles({});
}

TEST_F(TestSubTreeFileSystem, DeleteDir) {
  ASSERT_OK(subfs_->CreateDir("AB/CD/EF"));
  ASSERT_OK(subfs_->CreateDir("AB/GH/IJ"));
  ASSERT_OK(subfs_->DeleteDir("AB/CD"));
  ASSERT_OK(subfs_->DeleteDir("AB/GH/IJ"));
  CheckDirs({{"sub", time_},
             {"sub/tree", time_},
             {"sub/tree/AB", time_},
             {"sub/tree/AB/GH", time_}});
  CheckFiles({});
  ASSERT_RAISES(IOError, subfs_->DeleteDir("AB/CD"));
  ASSERT_OK(subfs_->DeleteDir("AB"));
  CheckDirs({{"sub", time_}, {"sub/tree", time_}});
  CheckFiles({});

  // Can't delete root dir
  ASSERT_RAISES(IOError, subfs_->DeleteDir(""));
  CheckDirs({{"sub", time_}, {"sub/tree", time_}});
  CheckFiles({});
}

TEST_F(TestSubTreeFileSystem, DeleteFile) {
  ASSERT_OK(subfs_->CreateDir("AB"));

  CreateFile("ab", "");
  CheckFiles({{"sub/tree/ab", time_, ""}});
  ASSERT_OK(subfs_->DeleteFile("ab"));
  CheckFiles({});

  CreateFile("AB/cd", "");
  CheckFiles({{"sub/tree/AB/cd", time_, ""}});
  ASSERT_OK(subfs_->DeleteFile("AB/cd"));
  CheckFiles({});

  ASSERT_RAISES(IOError, subfs_->DeleteFile("non-existent"));
  ASSERT_RAISES(IOError, subfs_->DeleteFile(""));
}

TEST_F(TestSubTreeFileSystem, MoveFile) {
  CreateFile("ab", "");
  CheckFiles({{"sub/tree/ab", time_, ""}});
  ASSERT_OK(subfs_->Move("ab", "cd"));
  CheckFiles({{"sub/tree/cd", time_, ""}});

  ASSERT_OK(subfs_->CreateDir("AB"));
  ASSERT_OK(subfs_->Move("cd", "AB/ef"));
  CheckFiles({{"sub/tree/AB/ef", time_, ""}});

  ASSERT_RAISES(IOError, subfs_->Move("AB/ef", ""));
  ASSERT_RAISES(IOError, subfs_->Move("", "xxx"));
  CheckFiles({{"sub/tree/AB/ef", time_, ""}});
  CheckDirs({{"sub", time_}, {"sub/tree", time_}, {"sub/tree/AB", time_}});
}

TEST_F(TestSubTreeFileSystem, MoveDir) {
  ASSERT_OK(subfs_->CreateDir("AB/CD/EF"));
  ASSERT_OK(subfs_->Move("AB/CD", "GH"));
  CheckDirs({{"sub", time_},
             {"sub/tree", time_},
             {"sub/tree/AB", time_},
             {"sub/tree/GH", time_},
             {"sub/tree/GH/EF", time_}});

  ASSERT_RAISES(IOError, subfs_->Move("AB", ""));
}

TEST_F(TestSubTreeFileSystem, CopyFile) {
  CreateFile("ab", "data");
  CheckFiles({{"sub/tree/ab", time_, "data"}});
  ASSERT_OK(subfs_->CopyFile("ab", "cd"));
  CheckFiles({{"sub/tree/ab", time_, "data"}, {"sub/tree/cd", time_, "data"}});

  ASSERT_OK(subfs_->CreateDir("AB"));
  ASSERT_OK(subfs_->CopyFile("cd", "AB/ef"));
  CheckFiles({{"sub/tree/AB/ef", time_, "data"},
              {"sub/tree/ab", time_, "data"},
              {"sub/tree/cd", time_, "data"}});

  ASSERT_RAISES(IOError, subfs_->CopyFile("ab", ""));
  ASSERT_RAISES(IOError, subfs_->CopyFile("", "xxx"));
  CheckFiles({{"sub/tree/AB/ef", time_, "data"},
              {"sub/tree/ab", time_, "data"},
              {"sub/tree/cd", time_, "data"}});
}

TEST_F(TestSubTreeFileSystem, OpenInputStream) {
  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buffer;
  CreateFile("ab", "data");

  ASSERT_OK(subfs_->OpenInputStream("ab", &stream));
  ASSERT_OK(stream->Read(4, &buffer));
  AssertBufferEqual(*buffer, "data");
  ASSERT_OK(stream->Close());

  ASSERT_RAISES(IOError, subfs_->OpenInputStream("non-existent", &stream));
  ASSERT_RAISES(IOError, subfs_->OpenInputStream("", &stream));
}

TEST_F(TestSubTreeFileSystem, OpenInputFile) {
  std::shared_ptr<io::RandomAccessFile> stream;
  std::shared_ptr<Buffer> buffer;
  CreateFile("ab", "some data");

  ASSERT_OK(subfs_->OpenInputFile("ab", &stream));
  ASSERT_OK(stream->ReadAt(5, 4, &buffer));
  AssertBufferEqual(*buffer, "data");
  ASSERT_OK(stream->Close());

  ASSERT_RAISES(IOError, subfs_->OpenInputFile("non-existent", &stream));
  ASSERT_RAISES(IOError, subfs_->OpenInputFile("", &stream));
}

TEST_F(TestSubTreeFileSystem, OpenOutputStream) {
  std::shared_ptr<io::OutputStream> stream;

  ASSERT_OK(subfs_->OpenOutputStream("ab", &stream));
  ASSERT_OK(stream->Write("data"));
  ASSERT_OK(stream->Close());
  CheckFiles({{"sub/tree/ab", time_, "data"}});

  ASSERT_OK(subfs_->CreateDir("AB"));
  ASSERT_OK(subfs_->OpenOutputStream("AB/cd", &stream));
  ASSERT_OK(stream->Write("other"));
  ASSERT_OK(stream->Close());
  CheckFiles({{"sub/tree/AB/cd", time_, "other"}, {"sub/tree/ab", time_, "data"}});

  ASSERT_RAISES(IOError, subfs_->OpenOutputStream("non-existent/xxx", &stream));
  ASSERT_RAISES(IOError, subfs_->OpenOutputStream("AB", &stream));
  ASSERT_RAISES(IOError, subfs_->OpenOutputStream("", &stream));
  CheckFiles({{"sub/tree/AB/cd", time_, "other"}, {"sub/tree/ab", time_, "data"}});
}

TEST_F(TestSubTreeFileSystem, OpenAppendStream) {
  std::shared_ptr<io::OutputStream> stream;

  ASSERT_OK(subfs_->OpenAppendStream("ab", &stream));
  ASSERT_OK(stream->Write("some"));
  ASSERT_OK(stream->Close());
  CheckFiles({{"sub/tree/ab", time_, "some"}});

  ASSERT_OK(subfs_->OpenAppendStream("ab", &stream));
  ASSERT_OK(stream->Write(" data"));
  ASSERT_OK(stream->Close());
  CheckFiles({{"sub/tree/ab", time_, "some data"}});
}

TEST_F(TestSubTreeFileSystem, GetTargetStatsSingle) {
  FileStats st;
  ASSERT_OK(subfs_->CreateDir("AB/CD"));

  ASSERT_OK(subfs_->GetTargetStats("AB", &st));
  AssertFileStats(st, "AB", FileType::Directory, time_);
  ASSERT_OK(subfs_->GetTargetStats("AB/CD", &st));
  AssertFileStats(st, "AB/CD", FileType::Directory, time_);

  CreateFile("ab", "data");
  ASSERT_OK(subfs_->GetTargetStats("ab", &st));
  AssertFileStats(st, "ab", FileType::File, time_, 4);

  ASSERT_OK(subfs_->GetTargetStats("non-existent", &st));
  AssertFileStats(st, "non-existent", FileType::NonExistent);
}

TEST_F(TestSubTreeFileSystem, GetTargetStatsVector) {
  std::vector<FileStats> stats;

  ASSERT_OK(subfs_->CreateDir("AB/CD"));
  CreateFile("ab", "data");
  CreateFile("AB/cd", "other data");

  ASSERT_OK(subfs_->GetTargetStats({"ab", "AB", "AB/cd", "non-existent"}, &stats));
  ASSERT_EQ(stats.size(), 4);
  AssertFileStats(stats[0], "ab", FileType::File, time_, 4);
  AssertFileStats(stats[1], "AB", FileType::Directory, time_);
  AssertFileStats(stats[2], "AB/cd", FileType::File, time_, 10);
  AssertFileStats(stats[3], "non-existent", FileType::NonExistent);
}

TEST_F(TestSubTreeFileSystem, GetTargetStatsSelector) {
  std::vector<FileStats> stats;
  Selector selector;

  ASSERT_OK(subfs_->CreateDir("AB/CD"));
  CreateFile("ab", "data");
  CreateFile("AB/cd", "data2");
  CreateFile("AB/CD/ef", "data34");

  selector.base_dir = "AB";
  selector.recursive = false;
  ASSERT_OK(subfs_->GetTargetStats(selector, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB/CD", FileType::Directory, time_);
  AssertFileStats(stats[1], "AB/cd", FileType::File, time_, 5);

  selector.recursive = true;
  ASSERT_OK(subfs_->GetTargetStats(selector, &stats));
  ASSERT_EQ(stats.size(), 3);
  AssertFileStats(stats[0], "AB/CD", FileType::Directory, time_);
  AssertFileStats(stats[1], "AB/CD/ef", FileType::File, time_, 6);
  AssertFileStats(stats[2], "AB/cd", FileType::File, time_, 5);

  selector.base_dir = "";
  selector.recursive = false;
  ASSERT_OK(subfs_->GetTargetStats(selector, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB", FileType::Directory, time_);
  AssertFileStats(stats[1], "ab", FileType::File, time_, 4);

  selector.recursive = true;
  ASSERT_OK(subfs_->GetTargetStats(selector, &stats));
  ASSERT_EQ(stats.size(), 5);
  AssertFileStats(stats[0], "AB", FileType::Directory, time_);
  AssertFileStats(stats[1], "AB/CD", FileType::Directory, time_);
  AssertFileStats(stats[2], "AB/CD/ef", FileType::File, time_, 6);
  AssertFileStats(stats[3], "AB/cd", FileType::File, time_, 5);
  AssertFileStats(stats[4], "ab", FileType::File, time_, 4);

  selector.base_dir = "non-existent";
  ASSERT_RAISES(IOError, subfs_->GetTargetStats(selector, &stats));
  selector.allow_non_existent = true;
  ASSERT_OK(subfs_->GetTargetStats(selector, &stats));
  ASSERT_EQ(stats.size(), 0);
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow

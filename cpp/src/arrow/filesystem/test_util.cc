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

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/filesystem/test_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace fs {

namespace {

std::vector<FileStats> GetAllWithType(FileSystem* fs, FileType type) {
  Selector selector;
  selector.base_dir = "";
  selector.recursive = true;
  std::vector<FileStats> stats;
  ABORT_NOT_OK(fs->GetTargetStats(selector, &stats));
  std::vector<FileStats> result;
  for (const auto& st : stats) {
    if (st.type() == type) {
      result.push_back(st);
    }
  }
  return result;
}

std::vector<FileStats> GetAllDirs(FileSystem* fs) {
  return GetAllWithType(fs, FileType::Directory);
}

std::vector<FileStats> GetAllFiles(FileSystem* fs) {
  return GetAllWithType(fs, FileType::File);
}

void AssertPaths(const std::vector<FileStats>& stats,
                 const std::vector<std::string>& expected_paths) {
  auto sorted_stats = stats;
  SortStats(&sorted_stats);
  std::vector<std::string> paths(sorted_stats.size());
  std::transform(sorted_stats.begin(), sorted_stats.end(), paths.begin(),
                 [&](const FileStats& st) { return st.path(); });

  ASSERT_EQ(paths, expected_paths);
}

void AssertAllDirs(FileSystem* fs, const std::vector<std::string>& expected_paths) {
  AssertPaths(GetAllDirs(fs), expected_paths);
}

void AssertAllFiles(FileSystem* fs, const std::vector<std::string>& expected_paths) {
  AssertPaths(GetAllFiles(fs), expected_paths);
}

Status WriteString(io::OutputStream* stream, const std::string& s) {
  return stream->Write(s.data(), static_cast<int64_t>(s.length()));
}

void AssertFileContents(FileSystem* fs, const std::string& path,
                        const std::string& expected_data) {
  FileStats st;
  ASSERT_OK(fs->GetTargetStats(path, &st));
  ASSERT_EQ(st.type(), FileType::File) << "For path '" << path << "'";
  ASSERT_EQ(st.size(), static_cast<int64_t>(expected_data.length()))
      << "For path '" << path << "'";

  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buffer, leftover;
  ASSERT_OK(fs->OpenInputStream(path, &stream));
  ASSERT_OK(stream->Read(st.size(), &buffer));
  AssertBufferEqual(*buffer, expected_data);
  // No data left in stream
  ASSERT_OK(stream->Read(1, &leftover));
  ASSERT_EQ(leftover->size(), 0);

  ASSERT_OK(stream->Close());
}

void ValidateTimePoint(TimePoint tp) { ASSERT_GE(tp.time_since_epoch().count(), 0); }

};  // namespace

void CreateFile(FileSystem* fs, const std::string& path, const std::string& data) {
  std::shared_ptr<io::OutputStream> stream;
  ASSERT_OK(fs->OpenOutputStream(path, &stream));
  ASSERT_OK(WriteString(stream.get(), data));
  ASSERT_OK(stream->Close());
}

void SortStats(std::vector<FileStats>* stats) {
  std::sort(stats->begin(), stats->end(),
            [](const FileStats& left, const FileStats& right) -> bool {
              return left.path() < right.path();
            });
}

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

void AssertFileStats(const FileStats& st, const std::string& path, FileType type,
                     int64_t size) {
  AssertFileStats(st, path, type);
  ASSERT_EQ(st.size(), size);
}

template <typename... Args>
void AssertFileStats(FileSystem* fs, const std::string& path, Args&&... args) {
  FileStats st;
  ASSERT_OK(fs->GetTargetStats(path, &st));
  AssertFileStats(st, path, std::forward<Args>(args)...);
}

////////////////////////////////////////////////////////////////////////////
// GenericFileSystemTest implementation

// XXX is there a way we can test mtimes reliably and precisely?

GenericFileSystemTest::~GenericFileSystemTest() {}

void GenericFileSystemTest::TestEmpty(FileSystem* fs) {
  auto dirs = GetAllDirs(fs);
  ASSERT_EQ(dirs.size(), 0);
  auto files = GetAllFiles(fs);
  ASSERT_EQ(files.size(), 0);
}

void GenericFileSystemTest::TestCreateDir(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB"));
  ASSERT_OK(fs->CreateDir("AB/CD/EF"));  // Recursive
  // Non-recursive, parent doesn't exist
  ASSERT_RAISES(IOError, fs->CreateDir("AB/GH/IJ", false /* recursive */));
  ASSERT_OK(fs->CreateDir("AB/GH", false /* recursive */));
  ASSERT_OK(fs->CreateDir("AB/GH/IJ", false /* recursive */));
  // Idempotency
  ASSERT_OK(fs->CreateDir("AB/GH/IJ", false /* recursive */));
  ASSERT_OK(fs->CreateDir("XY"));

  AssertAllDirs(fs, {"AB", "AB/CD", "AB/CD/EF", "AB/GH", "AB/GH/IJ", "XY"});
  AssertAllFiles(fs, {});

  // Cannot create a directory as child of a file
  CreateFile(fs, "AB/def", "");
  ASSERT_RAISES(IOError, fs->CreateDir("AB/def/EF/GH", true /* recursive */));
  ASSERT_RAISES(IOError, fs->CreateDir("AB/def/EF", false /* recursive */));

  AssertAllDirs(fs, {"AB", "AB/CD", "AB/CD/EF", "AB/GH", "AB/GH/IJ", "XY"});
  AssertAllFiles(fs, {"AB/def"});
}

void GenericFileSystemTest::TestDeleteDir(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB/CD/EF"));
  ASSERT_OK(fs->CreateDir("AB/GH/IJ"));
  CreateFile(fs, "AB/abc", "");
  CreateFile(fs, "AB/CD/def", "");
  CreateFile(fs, "AB/CD/EF/ghi", "");
  ASSERT_OK(fs->DeleteDir("AB/CD"));
  ASSERT_OK(fs->DeleteDir("AB/GH/IJ"));

  AssertAllDirs(fs, {"AB", "AB/GH"});
  AssertAllFiles(fs, {"AB/abc"});

  // File doesn't exist
  ASSERT_RAISES(IOError, fs->DeleteDir("AB/GH/IJ"));
  ASSERT_RAISES(IOError, fs->DeleteDir(""));

  AssertAllDirs(fs, {"AB", "AB/GH"});

  // Not a directory
  CreateFile(fs, "AB/def", "");
  ASSERT_RAISES(IOError, fs->DeleteDir("AB/def"));

  AssertAllDirs(fs, {"AB", "AB/GH"});
  AssertAllFiles(fs, {"AB/abc", "AB/def"});
}

void GenericFileSystemTest::TestDeleteFile(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB"));
  CreateFile(fs, "AB/def", "");
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {"AB/def"});

  ASSERT_OK(fs->DeleteFile("AB/def"));
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {});

  CreateFile(fs, "abc", "data");
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {"abc"});

  ASSERT_OK(fs->DeleteFile("abc"));
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {});

  // File doesn't exist
  ASSERT_RAISES(IOError, fs->DeleteFile("abc"));
  ASSERT_RAISES(IOError, fs->DeleteFile("AB/def"));

  // Not a file
  ASSERT_RAISES(IOError, fs->DeleteFile("AB"));
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {});
}

void GenericFileSystemTest::TestDeleteFiles(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB"));
  CreateFile(fs, "abc", "");
  CreateFile(fs, "AB/def", "123");
  CreateFile(fs, "AB/ghi", "456");
  CreateFile(fs, "AB/jkl", "789");
  CreateFile(fs, "AB/mno", "789");
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {"AB/def", "AB/ghi", "AB/jkl", "AB/mno", "abc"});

  // All successful
  ASSERT_OK(fs->DeleteFiles({"abc", "AB/def"}));
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {"AB/ghi", "AB/jkl", "AB/mno"});

  // One error: file doesn't exist
  ASSERT_RAISES(IOError, fs->DeleteFiles({"xx", "AB/jkl"}));
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {"AB/ghi", "AB/mno"});

  // One error: not a file
  ASSERT_RAISES(IOError, fs->DeleteFiles({"AB", "AB/mno"}));
  AssertAllDirs(fs, {"AB"});
  AssertAllFiles(fs, {"AB/ghi"});
}

void GenericFileSystemTest::TestMoveFile(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB/CD"));
  ASSERT_OK(fs->CreateDir("EF"));
  CreateFile(fs, "abc", "data");
  std::vector<std::string> all_dirs{"AB", "AB/CD", "EF"};
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"abc"});

  // Move inside root dir
  FileStats st;
  ASSERT_OK(fs->Move("abc", "def"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"def"});
  AssertFileStats(fs, "def", FileType::File, 4);
  AssertFileContents(fs, "def", "data");

  // Move out of root dir
  ASSERT_OK(fs->Move("def", "AB/CD/ghi"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"AB/CD/ghi"});
  AssertFileStats(fs, "AB/CD/ghi", FileType::File, 4);
  AssertFileContents(fs, "AB/CD/ghi", "data");

  ASSERT_OK(fs->Move("AB/CD/ghi", "EF/jkl"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"EF/jkl"});
  AssertFileStats(fs, "EF/jkl", FileType::File, 4);
  AssertFileContents(fs, "EF/jkl", "data");

  // Move back into root dir
  ASSERT_OK(fs->Move("EF/jkl", "mno"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"mno"});
  AssertFileStats(fs, "mno", FileType::File, 4);
  AssertFileContents(fs, "mno", "data");

  // Destination is a file => clobber
  CreateFile(fs, "AB/pqr", "other data");
  AssertAllFiles(fs, {"AB/pqr", "mno"});
  ASSERT_OK(fs->Move("mno", "AB/pqr"));
  AssertAllFiles(fs, {"AB/pqr"});
  AssertFileStats(fs, "AB/pqr", FileType::File, 4);
  AssertFileContents(fs, "AB/pqr", "data");

  // Identical source and destination: allowed to succeed or raise IOError,
  // but should not lose data.
  auto err = fs->Move("AB/pqr", "AB/pqr");
  if (!err.ok()) {
    ASSERT_RAISES(IOError, err);
  }
  AssertAllFiles(fs, {"AB/pqr"});
  AssertFileStats(fs, "AB/pqr", FileType::File, 4);
  AssertFileContents(fs, "AB/pqr", "data");

  // Source doesn't exist
  ASSERT_RAISES(IOError, fs->Move("abc", "def"));
  // Parent destination doesn't exist
  ASSERT_RAISES(IOError, fs->Move("AB/pqr", "XX/mno"));
  // Parent destination is not a directory
  CreateFile(fs, "xxx", "");
  ASSERT_RAISES(IOError, fs->Move("AB/pqr", "xxx/mno"));
  // Destination is a directory
  ASSERT_RAISES(IOError, fs->Move("AB/pqr", "EF"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"AB/pqr", "xxx"});
}

void GenericFileSystemTest::TestMoveDir(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB/CD"));
  ASSERT_OK(fs->CreateDir("EF"));
  CreateFile(fs, "AB/abc", "abc data");
  CreateFile(fs, "AB/CD/def", "def data");
  CreateFile(fs, "EF/ghi", "ghi data");
  AssertAllDirs(fs, {"AB", "AB/CD", "EF"});
  AssertAllFiles(fs, {"AB/CD/def", "AB/abc", "EF/ghi"});

  // Move inside root dir
  ASSERT_OK(fs->Move("AB", "GH"));
  AssertAllDirs(fs, {"EF", "GH", "GH/CD"});
  AssertAllFiles(fs, {"EF/ghi", "GH/CD/def", "GH/abc"});

  // Move out of root dir
  ASSERT_OK(fs->Move("GH", "EF/IJ"));
  AssertAllDirs(fs, {"EF", "EF/IJ", "EF/IJ/CD"});
  AssertAllFiles(fs, {"EF/IJ/CD/def", "EF/IJ/abc", "EF/ghi"});

  // Move back into root dir
  ASSERT_OK(fs->Move("EF/IJ", "KL"));
  AssertAllDirs(fs, {"EF", "KL", "KL/CD"});
  AssertAllFiles(fs, {"EF/ghi", "KL/CD/def", "KL/abc"});

  // Overwrite file with directory => untested (implementation-dependent)

  // Identical source and destination: allowed to succeed or raise IOError,
  // but should not lose data.
  Status st = fs->Move("KL", "KL");
  if (!st.ok()) {
    ASSERT_RAISES(IOError, st);
  }
  AssertAllDirs(fs, {"EF", "KL", "KL/CD"});
  AssertAllFiles(fs, {"EF/ghi", "KL/CD/def", "KL/abc"});

  // Destination is a non-empty directory
  ASSERT_RAISES(IOError, fs->Move("KL", "EF"));
  AssertAllDirs(fs, {"EF", "KL", "KL/CD"});
  AssertAllFiles(fs, {"EF/ghi", "KL/CD/def", "KL/abc"});

  // Cannot move directory inside itself
  ASSERT_RAISES(IOError, fs->Move("KL", "KL/ZZ"));

  // (other errors tested in TestMoveFile)

  // Contents didn't change
  AssertAllDirs(fs, {"EF", "KL", "KL/CD"});
  AssertFileContents(fs, "KL/abc", "abc data");
  AssertFileContents(fs, "KL/CD/def", "def data");
}

void GenericFileSystemTest::TestCopyFile(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB/CD"));
  ASSERT_OK(fs->CreateDir("EF"));
  CreateFile(fs, "AB/abc", "data");
  std::vector<std::string> all_dirs{"AB", "AB/CD", "EF"};

  // Copy into root dir
  ASSERT_OK(fs->CopyFile("AB/abc", "def"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"AB/abc", "def"});

  // Copy out of root dir
  ASSERT_OK(fs->CopyFile("def", "EF/ghi"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"AB/abc", "EF/ghi", "def"});

  // Overwrite contents for one file => other data shouldn't change
  CreateFile(fs, "def", "other data");
  AssertFileContents(fs, "AB/abc", "data");
  AssertFileContents(fs, "def", "other data");
  AssertFileContents(fs, "EF/ghi", "data");

  // Destination is a file => clobber
  ASSERT_OK(fs->CopyFile("def", "AB/abc"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"AB/abc", "EF/ghi", "def"});
  AssertFileContents(fs, "AB/abc", "other data");
  AssertFileContents(fs, "def", "other data");
  AssertFileContents(fs, "EF/ghi", "data");

  // Identical source and destination: allowed to succeed or raise IOError,
  // but should not lose data.
  Status st = fs->CopyFile("def", "def");
  if (!st.ok()) {
    ASSERT_RAISES(IOError, st);
  }
  AssertAllFiles(fs, {"AB/abc", "EF/ghi", "def"});
  AssertFileContents(fs, "def", "other data");

  // Destination is a non-empty directory
  ASSERT_RAISES(IOError, fs->CopyFile("def", "AB"));
  // Source doesn't exist
  ASSERT_RAISES(IOError, fs->CopyFile("abc", "xxx"));
  // Parent destination doesn't exist
  ASSERT_RAISES(IOError, fs->Move("AB/abc", "XX/mno"));
  // Parent destination is not a directory
  ASSERT_RAISES(IOError, fs->Move("AB/abc", "def/mno"));
  AssertAllDirs(fs, all_dirs);
  AssertAllFiles(fs, {"AB/abc", "EF/ghi", "def"});
}

void GenericFileSystemTest::TestGetTargetStatsSingle(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB/CD/EF"));
  CreateFile(fs, "AB/CD/ghi", "some data");

  FileStats st;
  TimePoint first_time;
  ASSERT_OK(fs->GetTargetStats("AB", &st));
  AssertFileStats(st, "AB", FileType::Directory);
  ASSERT_EQ(st.base_name(), "AB");
  ASSERT_EQ(st.size(), kNoSize);
  first_time = st.mtime();
  ValidateTimePoint(first_time);

  ASSERT_OK(fs->GetTargetStats("AB/CD/EF", &st));
  AssertFileStats(st, "AB/CD/EF", FileType::Directory);
  ASSERT_EQ(st.base_name(), "EF");
  ASSERT_EQ(st.size(), kNoSize);
  // AB/CD's creation can impact AB's modification time, however, AB/CD/EF's
  // creation doesn't, so AB/CD/EF's mtime should be after AB's.
  AssertDurationBetween(st.mtime() - first_time, 0.0, kTimeSlack);

  ASSERT_OK(fs->GetTargetStats("AB/CD/ghi", &st));
  AssertFileStats(st, "AB/CD/ghi", FileType::File, 9);
  ASSERT_EQ(st.base_name(), "ghi");
  AssertDurationBetween(st.mtime() - first_time, 0.0, kTimeSlack);

  ASSERT_OK(fs->GetTargetStats("zz", &st));
  AssertFileStats(st, "zz", FileType::NonExistent);
  ASSERT_EQ(st.base_name(), "zz");
  ASSERT_EQ(st.size(), kNoSize);
  ASSERT_EQ(st.mtime(), kNoTime);
}

void GenericFileSystemTest::TestGetTargetStatsVector(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB/CD"));
  CreateFile(fs, "AB/CD/ghi", "some data");

  std::vector<FileStats> stats;
  TimePoint first_time;
  ASSERT_OK(
      fs->GetTargetStats({"AB", "AB/CD", "AB/zz", "zz", "XX/zz", "AB/CD/ghi"}, &stats));
  ASSERT_EQ(stats.size(), 6);
  AssertFileStats(stats[0], "AB", FileType::Directory);
  first_time = stats[0].mtime();
  ValidateTimePoint(first_time);
  AssertFileStats(stats[1], "AB/CD", FileType::Directory);
  AssertFileStats(stats[2], "AB/zz", FileType::NonExistent);
  AssertFileStats(stats[3], "zz", FileType::NonExistent);
  AssertFileStats(stats[4], "XX/zz", FileType::NonExistent);
  ASSERT_EQ(stats[4].size(), kNoSize);
  ASSERT_EQ(stats[4].mtime(), kNoTime);
  AssertFileStats(stats[5], "AB/CD/ghi", FileType::File, 9);
  AssertDurationBetween(stats[5].mtime() - first_time, 0.0, kTimeSlack);

  // Check the mtime is the same from one call to the other
  FileStats st;
  ASSERT_OK(fs->GetTargetStats("AB", &st));
  AssertFileStats(st, "AB", FileType::Directory);
  ASSERT_EQ(st.mtime(), first_time);
}

void GenericFileSystemTest::TestGetTargetStatsSelector(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB/CD"));
  CreateFile(fs, "abc", "data");
  CreateFile(fs, "AB/def", "some data");
  CreateFile(fs, "AB/CD/ghi", "some other data");
  CreateFile(fs, "AB/CD/jkl", "yet other data");

  TimePoint first_time;
  Selector s;
  s.base_dir = "";
  std::vector<FileStats> stats;
  ASSERT_OK(fs->GetTargetStats(s, &stats));
  // Need to sort results to make testing deterministic
  SortStats(&stats);
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB", FileType::Directory);
  first_time = stats[0].mtime();
  ValidateTimePoint(first_time);
  AssertFileStats(stats[1], "abc", FileType::File, 4);

  s.base_dir = "AB";
  ASSERT_OK(fs->GetTargetStats(s, &stats));
  SortStats(&stats);
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB/CD", FileType::Directory);
  AssertFileStats(stats[1], "AB/def", FileType::File, 9);

  s.base_dir = "AB/CD";
  ASSERT_OK(fs->GetTargetStats(s, &stats));
  SortStats(&stats);
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "AB/CD/ghi", FileType::File, 15);
  AssertFileStats(stats[1], "AB/CD/jkl", FileType::File, 14);
  AssertDurationBetween(stats[1].mtime() - first_time, 0.0, kTimeSlack);

  // Recursive
  s.base_dir = "AB";
  s.recursive = true;
  ASSERT_OK(fs->GetTargetStats(s, &stats));
  SortStats(&stats);
  ASSERT_EQ(stats.size(), 4);
  AssertFileStats(stats[0], "AB/CD", FileType::Directory);
  AssertFileStats(stats[1], "AB/CD/ghi", FileType::File, 15);
  AssertFileStats(stats[2], "AB/CD/jkl", FileType::File, 14);
  AssertFileStats(stats[3], "AB/def", FileType::File, 9);

  // Check the mtime is the same from one call to the other
  FileStats st;
  ASSERT_OK(fs->GetTargetStats("AB", &st));
  AssertFileStats(st, "AB", FileType::Directory);
  ASSERT_EQ(st.mtime(), first_time);

  // Doesn't exist
  s.base_dir = "XX";
  ASSERT_RAISES(IOError, fs->GetTargetStats(s, &stats));
  s.allow_non_existent = true;
  ASSERT_OK(fs->GetTargetStats(s, &stats));
  ASSERT_EQ(stats.size(), 0);
  s.allow_non_existent = false;

  // Not a dir
  s.base_dir = "abc";
  ASSERT_RAISES(IOError, fs->GetTargetStats(s, &stats));
}

void GenericFileSystemTest::TestOpenOutputStream(FileSystem* fs) {
  std::shared_ptr<io::OutputStream> stream;
  int64_t position = -1;

  ASSERT_OK(fs->OpenOutputStream("abc", &stream));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 0);
  ASSERT_FALSE(stream->closed());
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(stream->closed());
  AssertAllDirs(fs, {});
  AssertAllFiles(fs, {"abc"});
  AssertFileContents(fs, "abc", "");

  // Parent does not exist
  ASSERT_RAISES(IOError, fs->OpenOutputStream("abc/def", &stream));
  ASSERT_RAISES(IOError, fs->OpenOutputStream("ghi/jkl", &stream));
  AssertAllDirs(fs, {});
  AssertAllFiles(fs, {"abc"});

  // Several writes
  ASSERT_OK(fs->CreateDir("CD"));
  ASSERT_OK(fs->OpenOutputStream("CD/ghi", &stream));
  ASSERT_OK(WriteString(stream.get(), "some "));
  ASSERT_OK(WriteString(stream.get(), "data"));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 9);
  ASSERT_OK(stream->Close());
  AssertAllDirs(fs, {"CD"});
  AssertAllFiles(fs, {"CD/ghi", "abc"});
  AssertFileContents(fs, "CD/ghi", "some data");

  // Overwrite
  ASSERT_OK(fs->OpenOutputStream("CD/ghi", &stream));
  ASSERT_OK(WriteString(stream.get(), "overwritten"));
  ASSERT_OK(stream->Close());
  AssertAllDirs(fs, {"CD"});
  AssertAllFiles(fs, {"CD/ghi", "abc"});
  AssertFileContents(fs, "CD/ghi", "overwritten");

  ASSERT_RAISES(Invalid, WriteString(stream.get(), "x"));  // Stream is closed

  // Cannot turn dir into file
  ASSERT_RAISES(IOError, fs->OpenOutputStream("CD", &stream));
  AssertAllDirs(fs, {"CD"});
}

void GenericFileSystemTest::TestOpenAppendStream(FileSystem* fs) {
  std::shared_ptr<io::OutputStream> stream;
  int64_t position = -1;

  ASSERT_OK(fs->OpenAppendStream("abc", &stream));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 0);
  ASSERT_OK(WriteString(stream.get(), "some "));
  ASSERT_OK(WriteString(stream.get(), "data"));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 9);
  ASSERT_OK(stream->Close());
  AssertAllDirs(fs, {});
  AssertAllFiles(fs, {"abc"});
  AssertFileContents(fs, "abc", "some data");

  ASSERT_OK(fs->OpenAppendStream("abc", &stream));
  ASSERT_OK(stream->Tell(&position));
  ASSERT_EQ(position, 9);
  ASSERT_OK(WriteString(stream.get(), " appended"));
  ASSERT_OK(stream->Close());
  AssertAllDirs(fs, {});
  AssertAllFiles(fs, {"abc"});
  AssertFileContents(fs, "abc", "some data appended");

  ASSERT_RAISES(Invalid, WriteString(stream.get(), "x"));  // Stream is closed
}

void GenericFileSystemTest::TestOpenInputStream(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB"));
  CreateFile(fs, "AB/abc", "some data");

  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(fs->OpenInputStream("AB/abc", &stream));
  ASSERT_OK(stream->Read(4, &buffer));
  AssertBufferEqual(*buffer, "some");
  ASSERT_OK(stream->Read(6, &buffer));
  AssertBufferEqual(*buffer, " data");
  ASSERT_OK(stream->Read(1, &buffer));
  AssertBufferEqual(*buffer, "");
  ASSERT_OK(stream->Close());
  ASSERT_RAISES(Invalid, stream->Read(1, &buffer));  // Stream is closed

  // File does not exist
  ASSERT_RAISES(IOError, fs->OpenInputStream("AB/def", &stream));
  ASSERT_RAISES(IOError, fs->OpenInputStream("def", &stream));

  // Cannot open directory
  ASSERT_RAISES(IOError, fs->OpenInputStream("AB", &stream));
}

void GenericFileSystemTest::TestOpenInputFile(FileSystem* fs) {
  ASSERT_OK(fs->CreateDir("AB"));
  CreateFile(fs, "AB/abc", "some other data");

  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buffer;
  int64_t size = -1;
  ASSERT_OK(fs->OpenInputFile("AB/abc", &file));
  ASSERT_OK(file->ReadAt(5, 6, &buffer));
  AssertBufferEqual(*buffer, "other ");
  ASSERT_OK(file->GetSize(&size));
  ASSERT_EQ(size, 15);
  ASSERT_OK(file->Close());
  ASSERT_RAISES(Invalid, file->ReadAt(1, 1, &buffer));  // Stream is closed

  // File does not exist
  ASSERT_RAISES(IOError, fs->OpenInputFile("AB/def", &file));
  ASSERT_RAISES(IOError, fs->OpenInputFile("def", &file));

  // Cannot open directory
  ASSERT_RAISES(IOError, fs->OpenInputFile("AB", &file));
}

#define GENERIC_FS_TEST_DEFINE(FUNC_NAME) \
  void GenericFileSystemTest::FUNC_NAME() { FUNC_NAME(GetEmptyFileSystem().get()); }

GENERIC_FS_TEST_DEFINE(TestEmpty)
GENERIC_FS_TEST_DEFINE(TestCreateDir)
GENERIC_FS_TEST_DEFINE(TestDeleteDir)
GENERIC_FS_TEST_DEFINE(TestDeleteFile)
GENERIC_FS_TEST_DEFINE(TestDeleteFiles)
GENERIC_FS_TEST_DEFINE(TestMoveFile)
GENERIC_FS_TEST_DEFINE(TestMoveDir)
GENERIC_FS_TEST_DEFINE(TestCopyFile)
GENERIC_FS_TEST_DEFINE(TestGetTargetStatsSingle)
GENERIC_FS_TEST_DEFINE(TestGetTargetStatsVector)
GENERIC_FS_TEST_DEFINE(TestGetTargetStatsSelector)
GENERIC_FS_TEST_DEFINE(TestOpenOutputStream)
GENERIC_FS_TEST_DEFINE(TestOpenAppendStream)
GENERIC_FS_TEST_DEFINE(TestOpenInputStream)
GENERIC_FS_TEST_DEFINE(TestOpenInputFile)

}  // namespace fs
}  // namespace arrow

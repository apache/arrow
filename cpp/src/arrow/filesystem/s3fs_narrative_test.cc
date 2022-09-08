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

// "Narrative" test for S3.  This must be run manually against a S3 endpoint.
// The test bucket must exist and be empty (you can use --clear to delete its
// contents).

#include <iostream>
#include <sstream>
#include <string>

#include <gflags/gflags.h>

#include "arrow/filesystem/s3fs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"

DEFINE_bool(clear, false, "delete all bucket contents");
DEFINE_bool(test, false, "run narrative test against bucket");

DEFINE_bool(verbose, false, "be more verbose (includes AWS warnings)");
DEFINE_bool(debug, false, "be very verbose (includes AWS debug logs)");

DEFINE_string(access_key, "", "S3 access key");
DEFINE_string(secret_key, "", "S3 secret key");

DEFINE_string(bucket, "", "bucket name");
DEFINE_string(region, "", "AWS region");
DEFINE_string(endpoint, "", "Endpoint override (e.g. '127.0.0.1:9000')");
DEFINE_string(scheme, "https", "Connection scheme");

namespace arrow {
namespace fs {

#define ASSERT_RAISES_PRINT(context_msg, error_type, expr) \
  do {                                                     \
    auto _status_or_result = (expr);                       \
    ASSERT_RAISES(error_type, _status_or_result);          \
    PrintError(context_msg, _status_or_result);            \
  } while (0)

std::shared_ptr<FileSystem> MakeFileSystem() {
  std::shared_ptr<S3FileSystem> s3fs;
  S3Options options;
  if (!FLAGS_access_key.empty()) {
    options = S3Options::FromAccessKey(FLAGS_access_key, FLAGS_secret_key);
  } else {
    options = S3Options::Defaults();
  }
  options.endpoint_override = FLAGS_endpoint;
  options.scheme = FLAGS_scheme;
  options.region = FLAGS_region;
  s3fs = S3FileSystem::Make(options).ValueOrDie();
  return std::make_shared<SubTreeFileSystem>(FLAGS_bucket, s3fs);
}

void PrintError(const std::string& context_msg, const Status& st) {
  if (FLAGS_verbose) {
    std::cout << "-- Error printout (" << context_msg << ") --\n"
              << st.ToString() << std::endl;
  }
}

template <typename T>
void PrintError(const std::string& context_msg, const Result<T>& result) {
  PrintError(context_msg, result.status());
}

void CheckDirectory(FileSystem* fs, const std::string& path) {
  ASSERT_OK_AND_ASSIGN(auto info, fs->GetFileInfo(path));
  AssertFileInfo(info, path, FileType::Directory);
}

void ClearBucket(int argc, char** argv) {
  auto fs = MakeFileSystem();

  ASSERT_OK(fs->DeleteRootDirContents());
}

void TestBucket(int argc, char** argv) {
  auto fs = MakeFileSystem();
  std::vector<FileInfo> infos;
  FileSelector select;
  std::shared_ptr<io::InputStream> is;
  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buf;
  Status status;

  // Check bucket exists and is empty
  select.base_dir = "";
  select.allow_not_found = false;
  select.recursive = false;
  ASSERT_OK_AND_ASSIGN(infos, fs->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0) << "Bucket should be empty, perhaps use --clear?";

  // Create directory structure
  ASSERT_OK(fs->CreateDir("EmptyDir", /*recursive=*/false));
  ASSERT_OK(fs->CreateDir("Dir1", /*recursive=*/false));
  ASSERT_OK(fs->CreateDir("Dir1/Subdir", /*recursive=*/false));
  ASSERT_RAISES_PRINT("CreateDir in nonexistent parent", IOError,
                      fs->CreateDir("Dir2/Subdir", /*recursive=*/false));
  ASSERT_OK(fs->CreateDir("Dir2/Subdir", /*recursive=*/true));
  ASSERT_OK(fs->CreateDir("Nested/1/2/3/4", /*recursive=*/true));
  CreateFile(fs.get(), "File1", "first data");
  CreateFile(fs.get(), "Dir1/File2", "second data");
  CreateFile(fs.get(), "Dir2/Subdir/File3", "third data");
  CreateFile(fs.get(), "Nested/1/2/3/4/File4", "fourth data");

  // GetFileInfo(Selector)
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 5);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "Dir1", FileType::Directory);
  AssertFileInfo(infos[1], "Dir2", FileType::Directory);
  AssertFileInfo(infos[2], "EmptyDir", FileType::Directory);
  AssertFileInfo(infos[3], "File1", FileType::File, 10);
  AssertFileInfo(infos[4], "Nested", FileType::Directory);

  select.base_dir = "zzzz";
  ASSERT_RAISES_PRINT("GetFileInfo(Selector) with nonexisting base_dir", IOError,
                      fs->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  select.base_dir = "Dir1";
  select.allow_not_found = false;
  ASSERT_OK_AND_ASSIGN(infos, fs->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "Dir1/File2", FileType::File, 11);
  AssertFileInfo(infos[1], "Dir1/Subdir", FileType::Directory);

  select.base_dir = "Dir2";
  select.recursive = true;
  ASSERT_OK_AND_ASSIGN(infos, fs->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "Dir2/Subdir", FileType::Directory);
  AssertFileInfo(infos[1], "Dir2/Subdir/File3", FileType::File, 10);

  // GetFileInfo(single entry)
  CheckDirectory(fs.get(), "EmptyDir");
  CheckDirectory(fs.get(), "Dir1");
  CheckDirectory(fs.get(), "Nested");
  CheckDirectory(fs.get(), "Nested/1");
  CheckDirectory(fs.get(), "Nested/1/2");
  CheckDirectory(fs.get(), "Nested/1/2/3");
  CheckDirectory(fs.get(), "Nested/1/2/3/4");
  AssertFileInfo(fs.get(), "Nest", FileType::NotFound);

  // Read a file
  ASSERT_RAISES_PRINT("OpenInputStream with nonexistent file", IOError,
                      fs->OpenInputStream("zzz"));
  ASSERT_OK_AND_ASSIGN(is, fs->OpenInputStream("File1"));
  ASSERT_OK_AND_ASSIGN(buf, is->Read(5));
  AssertBufferEqual(*buf, "first");
  ASSERT_OK_AND_ASSIGN(buf, is->Read(10));
  AssertBufferEqual(*buf, " data");
  ASSERT_OK_AND_ASSIGN(buf, is->Read(10));
  AssertBufferEqual(*buf, "");
  ASSERT_OK(is->Close());

  ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile("Dir1/File2"));
  ASSERT_OK_AND_EQ(0, file->Tell());
  ASSERT_OK(file->Seek(7));
  ASSERT_OK_AND_EQ(7, file->Tell());
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "da");
  ASSERT_OK_AND_EQ(9, file->Tell());
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(2, 4));
  AssertBufferEqual(*buf, "cond");
  ASSERT_OK(file->Close());

  // Copy a file
  ASSERT_OK(fs->CopyFile("File1", "Dir2/File4"));
  AssertFileInfo(fs.get(), "File1", FileType::File, 10);
  AssertFileInfo(fs.get(), "Dir2/File4", FileType::File, 10);
  AssertFileContents(fs.get(), "Dir2/File4", "first data");

  // Copy a file over itself
  ASSERT_OK(fs->CopyFile("File1", "File1"));
  AssertFileInfo(fs.get(), "File1", FileType::File, 10);
  AssertFileContents(fs.get(), "File1", "first data");

  // Move a file
  ASSERT_OK(fs->Move("Dir2/File4", "File5"));
  AssertFileInfo(fs.get(), "Dir2/File4", FileType::NotFound);
  AssertFileInfo(fs.get(), "File5", FileType::File, 10);
  AssertFileContents(fs.get(), "File5", "first data");

  // Move a file over itself
  ASSERT_OK(fs->Move("File5", "File5"));
  AssertFileInfo(fs.get(), "File5", FileType::File, 10);
  AssertFileContents(fs.get(), "File5", "first data");
}

void TestMain(int argc, char** argv) {
  S3GlobalOptions options;
  options.log_level = FLAGS_debug
                          ? S3LogLevel::Debug
                          : (FLAGS_verbose ? S3LogLevel::Warn : S3LogLevel::Fatal);
  ASSERT_OK(InitializeS3(options));

  if (FLAGS_region.empty()) {
    ASSERT_OK_AND_ASSIGN(FLAGS_region, ResolveS3BucketRegion(FLAGS_bucket));
  }

  if (FLAGS_clear) {
    ClearBucket(argc, argv);
  } else if (FLAGS_test) {
    TestBucket(argc, argv);
  }

  ASSERT_OK(FinalizeS3());
}

}  // namespace fs
}  // namespace arrow

int main(int argc, char** argv) {
  std::stringstream ss;
  ss << "Narrative test for S3.  Needs an initialized empty bucket.\n";
  ss << "Usage: " << argv[0];
  gflags::SetUsageMessage(ss.str());
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_clear + FLAGS_test != 1) {
    ARROW_LOG(ERROR) << "Need exactly one of --test and --clear";
    return 2;
  }
  if (FLAGS_bucket.empty()) {
    ARROW_LOG(ERROR) << "--bucket is mandatory";
    return 2;
  }

  arrow::fs::TestMain(argc, argv);
  if (::testing::Test::HasFatalFailure() || ::testing::Test::HasNonfatalFailure()) {
    return 1;
  } else {
    std::cout << "Ok" << std::endl;
    return 0;
  }
}

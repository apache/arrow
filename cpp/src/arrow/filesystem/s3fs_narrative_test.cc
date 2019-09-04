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
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"

DEFINE_bool(clear, false, "delete all bucket contents");
DEFINE_bool(test, false, "run narrative test against bucket");

DEFINE_string(access_key, "", "S3 access key");
DEFINE_string(secret_key, "", "S3 secret key");

DEFINE_string(bucket, "", "bucket name");
DEFINE_string(region, arrow::fs::kS3DefaultRegion, "AWS region");
DEFINE_string(endpoint, "", "Endpoint override (e.g. '127.0.0.1:9000')");
DEFINE_string(scheme, "https", "Connection scheme");

namespace arrow {
namespace fs {

std::shared_ptr<FileSystem> MakeFileSystem() {
  std::shared_ptr<S3FileSystem> s3fs;
  S3Options options;
  options.access_key = FLAGS_access_key;
  options.secret_key = FLAGS_secret_key;
  options.endpoint_override = FLAGS_endpoint;
  options.scheme = FLAGS_scheme;
  options.region = FLAGS_region;
  ABORT_NOT_OK(S3FileSystem::Make(options, &s3fs));
  return std::make_shared<SubTreeFileSystem>(FLAGS_bucket, s3fs);
}

void ClearBucket(int argc, char** argv) {
  auto fs = MakeFileSystem();

  ASSERT_OK(fs->DeleteDirContents(""));
}

void TestBucket(int argc, char** argv) {
  auto fs = MakeFileSystem();
  FileStats st;
  std::vector<FileStats> stats;
  Selector select;
  std::shared_ptr<io::InputStream> is;
  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buf;
  int64_t pos;

  // Check bucket exists and is empty
  select.base_dir = "";
  select.allow_non_existent = false;
  select.recursive = false;
  ASSERT_OK(fs->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 0) << "Bucket should be empty, perhaps use --clear?";

  // Create directory structure
  ASSERT_OK(fs->CreateDir("EmptyDir", /*recursive=*/false));
  ASSERT_OK(fs->CreateDir("Dir1", /*recursive=*/false));
  ASSERT_OK(fs->CreateDir("Dir1/Subdir", /*recursive=*/false));
  ASSERT_RAISES(IOError, fs->CreateDir("Dir2/Subdir", /*recursive=*/false));
  ASSERT_OK(fs->CreateDir("Dir2/Subdir", /*recursive=*/true));
  CreateFile(fs.get(), "File1", "first data");
  CreateFile(fs.get(), "Dir1/File2", "second data");
  CreateFile(fs.get(), "Dir2/Subdir/File3", "third data");

  // GetTargetStats(Selector)
  select.base_dir = "";
  ASSERT_OK(fs->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 4);
  SortStats(&stats);
  AssertFileStats(stats[0], "Dir1", FileType::Directory);
  AssertFileStats(stats[1], "Dir2", FileType::Directory);
  AssertFileStats(stats[2], "EmptyDir", FileType::Directory);
  AssertFileStats(stats[3], "File1", FileType::File, 10);

  select.base_dir = "zzzz";
  ASSERT_RAISES(IOError, fs->GetTargetStats(select, &stats));
  select.allow_non_existent = true;
  ASSERT_OK(fs->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 0);

  select.base_dir = "Dir1";
  select.allow_non_existent = false;
  ASSERT_OK(fs->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "Dir1/File2", FileType::File, 11);
  AssertFileStats(stats[1], "Dir1/Subdir", FileType::Directory);

  select.base_dir = "Dir2";
  select.recursive = true;
  ASSERT_OK(fs->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "Dir2/Subdir", FileType::Directory);
  AssertFileStats(stats[1], "Dir2/Subdir/File3", FileType::File, 10);

  // Read a file
  ASSERT_RAISES(IOError, fs->OpenInputStream("zzz", &is));
  ASSERT_OK(fs->OpenInputStream("File1", &is));
  ASSERT_OK(is->Read(5, &buf));
  AssertBufferEqual(*buf, "first");
  ASSERT_OK(is->Read(10, &buf));
  AssertBufferEqual(*buf, " data");
  ASSERT_OK(is->Read(10, &buf));
  AssertBufferEqual(*buf, "");
  ASSERT_OK(is->Close());

  ASSERT_OK(fs->OpenInputFile("Dir1/File2", &file));
  ASSERT_OK(file->Tell(&pos));
  ASSERT_EQ(pos, 0);
  ASSERT_OK(file->Seek(7));
  ASSERT_OK(file->Tell(&pos));
  ASSERT_EQ(pos, 7);
  ASSERT_OK(file->Read(2, &buf));
  AssertBufferEqual(*buf, "da");
  ASSERT_OK(file->Tell(&pos));
  ASSERT_EQ(pos, 9);
  ASSERT_OK(file->ReadAt(2, 4, &buf));
  AssertBufferEqual(*buf, "cond");
  ASSERT_OK(file->Close());

  // Copy a file
  ASSERT_OK(fs->CopyFile("File1", "Dir2/File4"));
  AssertFileStats(fs.get(), "File1", FileType::File, 10);
  AssertFileStats(fs.get(), "Dir2/File4", FileType::File, 10);
  AssertFileContents(fs.get(), "Dir2/File4", "first data");

  // Copy a file over itself
  ASSERT_OK(fs->CopyFile("File1", "File1"));
  AssertFileStats(fs.get(), "File1", FileType::File, 10);
  AssertFileContents(fs.get(), "File1", "first data");

  // Move a file
  ASSERT_OK(fs->Move("Dir2/File4", "File5"));
  AssertFileStats(fs.get(), "Dir2/File4", FileType::NonExistent);
  AssertFileStats(fs.get(), "File5", FileType::File, 10);
  AssertFileContents(fs.get(), "File5", "first data");

  // Move a file over itself
  ASSERT_OK(fs->Move("File5", "File5"));
  AssertFileStats(fs.get(), "File5", FileType::File, 10);
  AssertFileContents(fs.get(), "File5", "first data");
}

void TestMain(int argc, char** argv) {
  S3GlobalOptions options;
  options.log_level = S3LogLevel::Fatal;
  ASSERT_OK(InitializeS3(options));

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

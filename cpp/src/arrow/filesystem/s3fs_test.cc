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

#include <exception>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <boost/process.hpp>

#include <gtest/gtest.h>

#ifdef _WIN32
// Undefine preprocessor macros that interfere with AWS function / method names
#ifdef GetMessage
#undef GetMessage
#endif
#ifdef GetObject
#undef GetObject
#endif
#endif

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3_internal.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace fs {

using ::arrow::internal::PlatformFilename;
using ::arrow::internal::TemporaryDir;

using ::arrow::fs::internal::ConnectRetryStrategy;
using ::arrow::fs::internal::ErrorToStatus;
using ::arrow::fs::internal::OutcomeToStatus;
using ::arrow::fs::internal::ToAwsString;

namespace bp = boost::process;

// NOTE: Connecting in Python:
// >>> fs = s3fs.S3FileSystem(key='minio', secret='miniopass',
// client_kwargs=dict(endpoint_url='http://127.0.0.1:9000'))
// >>> fs.ls('')
// ['bucket']
// or:
// >>> from fs_s3fs import S3FS
// >>> fs = S3FS('bucket', endpoint_url='http://127.0.0.1:9000',
// aws_access_key_id='minio', aws_secret_access_key='miniopass')

#define ARROW_AWS_ASSIGN_OR_FAIL_IMPL(outcome_name, lhs, rexpr) \
  auto outcome_name = (rexpr);                                  \
  if (!outcome_name.IsSuccess()) {                              \
    FAIL() << "'" ARROW_STRINGIFY(rexpr) "' failed with "       \
           << outcome_name.GetError().GetMessage();             \
  }                                                             \
  lhs = std::move(outcome_name).GetResultWithOwnership();

#define ARROW_AWS_ASSIGN_OR_FAIL_NAME(x, y) ARROW_CONCAT(x, y)

#define ARROW_AWS_ASSIGN_OR_FAIL(lhs, rexpr) \
  ARROW_AWS_ASSIGN_OR_FAIL_IMPL(             \
      ARROW_AWS_ASSIGN_OR_FAIL_NAME(_aws_error_or_value, __COUNTER__), lhs, rexpr);

// TODO: allocate an ephemeral port
static const char* kMinioExecutableName = "minio";
static const char* kMinioAccessKey = "minio";
static const char* kMinioSecretKey = "miniopass";

static std::string GenerateConnectString() {
  std::stringstream ss;
  ss << "127.0.0.1:" << GetListenPort();
  return ss.str();
}

// A minio test server, managed as a child process

class MinioTestServer {
 public:
  Status Start();

  Status Stop();

  std::string connect_string() const { return connect_string_; }

  std::string access_key() const { return kMinioAccessKey; }

  std::string secret_key() const { return kMinioSecretKey; }

 private:
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string connect_string_;
  std::shared_ptr<::boost::process::child> server_process_;
};

Status MinioTestServer::Start() {
  RETURN_NOT_OK(TemporaryDir::Make("s3fs-test-", &temp_dir_));

  // Get a copy of the current environment.
  // (NOTE: using "auto" would return a native_environment that mutates
  //  the current environment)
  bp::environment env = boost::this_process::environment();
  env["MINIO_ACCESS_KEY"] = kMinioAccessKey;
  env["MINIO_SECRET_KEY"] = kMinioSecretKey;

  connect_string_ = GenerateConnectString();

  auto exe_path = bp::search_path(kMinioExecutableName);
  if (exe_path.empty()) {
    return Status::IOError("Failed to find minio executable ('", kMinioExecutableName,
                           "') in PATH");
  }

  try {
    // NOTE: --quiet makes startup faster by suppressing remote version check
    server_process_ = std::make_shared<bp::child>(
        env, exe_path, "server", "--quiet", "--compat", "--address", connect_string_,
        temp_dir_->path().ToString());
  } catch (const std::exception& e) {
    return Status::IOError("Failed to launch Minio server: ", e.what());
  }
  return Status::OK();
}

Status MinioTestServer::Stop() {
  if (server_process_ && server_process_->valid()) {
    // Brutal shutdown
    server_process_->terminate();
    server_process_->wait();
  }
  return Status::OK();
}

// A global test "environment", to ensure that the S3 API is initialized before
// running unit tests.

class S3Environment : public ::testing::Environment {
 public:
  virtual ~S3Environment() {}

  void SetUp() override {
    // Change this to increase logging during tests
    S3GlobalOptions options;
    options.log_level = S3LogLevel::Fatal;
    ASSERT_OK(InitializeS3(options));
  }

  void TearDown() override { ASSERT_OK(FinalizeS3()); }

 protected:
  Aws::SDKOptions options_;
};

::testing::Environment* s3_env = ::testing::AddGlobalTestEnvironment(new S3Environment);

class S3TestMixin : public ::testing::Test {
 public:
  void SetUp() override {
    ASSERT_OK(minio_.Start());

    client_config_.endpointOverride = ToAwsString(minio_.connect_string());
    client_config_.scheme = Aws::Http::Scheme::HTTP;
    client_config_.retryStrategy = std::make_shared<ConnectRetryStrategy>();
    credentials_ = {ToAwsString(minio_.access_key()), ToAwsString(minio_.secret_key())};
    bool use_virtual_addressing = false;
    client_.reset(
        new Aws::S3::S3Client(credentials_, client_config_,
                              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                              use_virtual_addressing));
  }

  void TearDown() override { ASSERT_OK(minio_.Stop()); }

 protected:
  MinioTestServer minio_;
  Aws::Client::ClientConfiguration client_config_;
  Aws::Auth::AWSCredentials credentials_;
  std::unique_ptr<Aws::S3::S3Client> client_;
};

void AssertGetObject(Aws::S3::Model::GetObjectResult& result,
                     const std::string& expected) {
  auto length = static_cast<int64_t>(expected.length());
  ASSERT_EQ(result.GetContentLength(), length);
  auto& stream = result.GetBody();
  std::string actual;
  actual.resize(length + 1);
  stream.read(&actual[0], length + 1);
  ASSERT_EQ(stream.gcount(), length);  // EOF was reached before length + 1
  actual.resize(length);
  ASSERT_EQ(actual, expected);
}

void AssertObjectContents(Aws::S3::S3Client* client, const std::string& bucket,
                          const std::string& key, const std::string& expected) {
  Aws::S3::Model::GetObjectRequest req;
  req.SetBucket(ToAwsString(bucket));
  req.SetKey(ToAwsString(key));
  ARROW_AWS_ASSIGN_OR_FAIL(auto result, client->GetObject(req));
  AssertGetObject(result, expected);
}

////////////////////////////////////////////////////////////////////////////
// Basic test for the Minio test server.

class TestMinioServer : public S3TestMixin {
 public:
  void SetUp() override { S3TestMixin::SetUp(); }

 protected:
};

TEST_F(TestMinioServer, Connect) {
  // Just a dummy connection test.  Check that we can list buckets,
  // and that there are none (the server is launched in an empty temp dir).
  ARROW_AWS_ASSIGN_OR_FAIL(auto bucket_list, client_->ListBuckets());
  ASSERT_EQ(bucket_list.GetBuckets().size(), 0);
}

////////////////////////////////////////////////////////////////////////////
// Concrete S3 tests

class TestS3FS : public S3TestMixin {
 public:
  void SetUp() override {
    S3TestMixin::SetUp();
    options_.access_key = minio_.access_key();
    options_.secret_key = minio_.secret_key();
    options_.scheme = "http";
    options_.endpoint_override = minio_.connect_string();
    ASSERT_OK(S3FileSystem::Make(options_, &fs_));

    // Set up test bucket
    {
      Aws::S3::Model::CreateBucketRequest req;
      req.SetBucket(ToAwsString("bucket"));
      ASSERT_OK(OutcomeToStatus(client_->CreateBucket(req)));
      req.SetBucket(ToAwsString("empty-bucket"));
      ASSERT_OK(OutcomeToStatus(client_->CreateBucket(req)));
    }
    {
      Aws::S3::Model::PutObjectRequest req;
      req.SetBucket(ToAwsString("bucket"));
      req.SetKey(ToAwsString("emptydir/"));
      ASSERT_OK(OutcomeToStatus(client_->PutObject(req)));
      // NOTE: no need to create intermediate "directories" somedir/ and
      // somedir/subdir/
      req.SetKey(ToAwsString("somedir/subdir/subfile"));
      req.SetBody(std::make_shared<std::stringstream>("sub data"));
      ASSERT_OK(OutcomeToStatus(client_->PutObject(req)));
      req.SetKey(ToAwsString("somefile"));
      req.SetBody(std::make_shared<std::stringstream>("some data"));
      ASSERT_OK(OutcomeToStatus(client_->PutObject(req)));
    }
  }

 protected:
  S3Options options_;
  std::shared_ptr<S3FileSystem> fs_;
};

TEST_F(TestS3FS, GetTargetStatsRoot) {
  FileStats st;
  AssertFileStats(fs_.get(), "", FileType::Directory);
}

TEST_F(TestS3FS, GetTargetStatsBucket) {
  FileStats st;
  AssertFileStats(fs_.get(), "bucket", FileType::Directory);
  AssertFileStats(fs_.get(), "empty-bucket", FileType::Directory);
  AssertFileStats(fs_.get(), "non-existent-bucket", FileType::NonExistent);
}

TEST_F(TestS3FS, GetTargetStatsObject) {
  FileStats st;

  // "Directories"
  AssertFileStats(fs_.get(), "bucket/emptydir", FileType::Directory, kNoSize);
  AssertFileStats(fs_.get(), "bucket/somedir", FileType::Directory, kNoSize);
  AssertFileStats(fs_.get(), "bucket/somedir/subdir", FileType::Directory, kNoSize);

  // "Files"
  AssertFileStats(fs_.get(), "bucket/somefile", FileType::File, 9);
  AssertFileStats(fs_.get(), "bucket/somedir/subdir/subfile", FileType::File, 8);

  // Non-existent
  AssertFileStats(fs_.get(), "bucket/emptyd", FileType::NonExistent);
  AssertFileStats(fs_.get(), "bucket/somed", FileType::NonExistent);
  AssertFileStats(fs_.get(), "non-existent-bucket/somed", FileType::NonExistent);
}

TEST_F(TestS3FS, GetTargetStatsSelector) {
  Selector select;
  std::vector<FileStats> stats;

  // Root dir
  select.base_dir = "";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 2);
  SortStats(&stats);
  AssertFileStats(stats[0], "bucket", FileType::Directory);
  AssertFileStats(stats[1], "empty-bucket", FileType::Directory);

  // Empty bucket
  select.base_dir = "empty-bucket";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 0);
  // Non-existent bucket
  select.base_dir = "non-existent-bucket";
  ASSERT_RAISES(IOError, fs_->GetTargetStats(select, &stats));
  select.allow_non_existent = true;
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 0);
  select.allow_non_existent = false;
  // Non-empty bucket
  select.base_dir = "bucket";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  SortStats(&stats);
  ASSERT_EQ(stats.size(), 3);
  AssertFileStats(stats[0], "bucket/emptydir", FileType::Directory);
  AssertFileStats(stats[1], "bucket/somedir", FileType::Directory);
  AssertFileStats(stats[2], "bucket/somefile", FileType::File, 9);

  // Empty "directory"
  select.base_dir = "bucket/emptydir";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 0);
  // Non-empty "directories"
  select.base_dir = "bucket/somedir";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 1);
  AssertFileStats(stats[0], "bucket/somedir/subdir", FileType::Directory);
  select.base_dir = "bucket/somedir/subdir";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 1);
  AssertFileStats(stats[0], "bucket/somedir/subdir/subfile", FileType::File, 8);
  // Non-existent
  select.base_dir = "bucket/non-existent";
  ASSERT_RAISES(IOError, fs_->GetTargetStats(select, &stats));
  select.allow_non_existent = true;
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 0);
  select.allow_non_existent = false;
}

TEST_F(TestS3FS, GetTargetStatsSelectorRecursive) {
  Selector select;
  std::vector<FileStats> stats;
  select.recursive = true;

  // Root dir
  select.base_dir = "";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 7);
  SortStats(&stats);
  AssertFileStats(stats[0], "bucket", FileType::Directory);
  AssertFileStats(stats[1], "bucket/emptydir", FileType::Directory);
  AssertFileStats(stats[2], "bucket/somedir", FileType::Directory);
  AssertFileStats(stats[3], "bucket/somedir/subdir", FileType::Directory);
  AssertFileStats(stats[4], "bucket/somedir/subdir/subfile", FileType::File, 8);
  AssertFileStats(stats[5], "bucket/somefile", FileType::File, 9);
  AssertFileStats(stats[6], "empty-bucket", FileType::Directory);

  // Empty bucket
  select.base_dir = "empty-bucket";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 0);

  // Non-empty bucket
  select.base_dir = "bucket";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  SortStats(&stats);
  ASSERT_EQ(stats.size(), 5);
  AssertFileStats(stats[0], "bucket/emptydir", FileType::Directory);
  AssertFileStats(stats[1], "bucket/somedir", FileType::Directory);
  AssertFileStats(stats[2], "bucket/somedir/subdir", FileType::Directory);
  AssertFileStats(stats[3], "bucket/somedir/subdir/subfile", FileType::File, 8);
  AssertFileStats(stats[4], "bucket/somefile", FileType::File, 9);

  // Empty "directory"
  select.base_dir = "bucket/emptydir";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 0);

  // Non-empty "directories"
  select.base_dir = "bucket/somedir";
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  SortStats(&stats);
  ASSERT_EQ(stats.size(), 2);
  AssertFileStats(stats[0], "bucket/somedir/subdir", FileType::Directory);
  AssertFileStats(stats[1], "bucket/somedir/subdir/subfile", FileType::File, 8);
}

TEST_F(TestS3FS, CreateDir) {
  FileStats st;

  // Existing bucket
  ASSERT_OK(fs_->CreateDir("bucket"));
  AssertFileStats(fs_.get(), "bucket", FileType::Directory);

  // New bucket
  AssertFileStats(fs_.get(), "new-bucket", FileType::NonExistent);
  ASSERT_OK(fs_->CreateDir("new-bucket"));
  AssertFileStats(fs_.get(), "new-bucket", FileType::Directory);

  // Existing "directory"
  AssertFileStats(fs_.get(), "bucket/somedir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("bucket/somedir"));
  AssertFileStats(fs_.get(), "bucket/somedir", FileType::Directory);

  AssertFileStats(fs_.get(), "bucket/emptydir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("bucket/emptydir"));
  AssertFileStats(fs_.get(), "bucket/emptydir", FileType::Directory);

  // New "directory"
  AssertFileStats(fs_.get(), "bucket/newdir", FileType::NonExistent);
  ASSERT_OK(fs_->CreateDir("bucket/newdir"));
  AssertFileStats(fs_.get(), "bucket/newdir", FileType::Directory);

  // New "directory", recursive
  ASSERT_OK(fs_->CreateDir("bucket/newdir/newsub/newsubsub", /*recursive=*/true));
  AssertFileStats(fs_.get(), "bucket/newdir/newsub", FileType::Directory);
  AssertFileStats(fs_.get(), "bucket/newdir/newsub/newsubsub", FileType::Directory);

  // Existing "file", should fail
  ASSERT_RAISES(IOError, fs_->CreateDir("bucket/somefile"));
}

TEST_F(TestS3FS, DeleteFile) {
  FileStats st;

  // Bucket
  ASSERT_RAISES(IOError, fs_->DeleteFile("bucket"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("empty-bucket"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("non-existent-bucket"));

  // "File"
  ASSERT_OK(fs_->DeleteFile("bucket/somefile"));
  AssertFileStats(fs_.get(), "bucket/somefile", FileType::NonExistent);
  ASSERT_RAISES(IOError, fs_->DeleteFile("bucket/somefile"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("bucket/non-existent"));

  // "Directory"
  ASSERT_RAISES(IOError, fs_->DeleteFile("bucket/somedir"));
  AssertFileStats(fs_.get(), "bucket/somedir", FileType::Directory);
}

TEST_F(TestS3FS, DeleteDir) {
  Selector select;
  select.base_dir = "bucket";
  std::vector<FileStats> stats;
  FileStats st;

  // Empty "directory"
  ASSERT_OK(fs_->DeleteDir("bucket/emptydir"));
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 2);
  SortStats(&stats);
  AssertFileStats(stats[0], "bucket/somedir", FileType::Directory);
  AssertFileStats(stats[1], "bucket/somefile", FileType::File);

  // Non-empty "directory"
  ASSERT_OK(fs_->DeleteDir("bucket/somedir"));
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 1);
  AssertFileStats(stats[0], "bucket/somefile", FileType::File);

  // Leaving parent "directory" empty
  ASSERT_OK(fs_->CreateDir("bucket/newdir/newsub/newsubsub"));
  ASSERT_OK(fs_->DeleteDir("bucket/newdir/newsub"));
  ASSERT_OK(fs_->GetTargetStats(select, &stats));
  ASSERT_EQ(stats.size(), 2);
  SortStats(&stats);
  AssertFileStats(stats[0], "bucket/newdir", FileType::Directory);  // still exists
  AssertFileStats(stats[1], "bucket/somefile", FileType::File);

  // Bucket
  ASSERT_OK(fs_->DeleteDir("bucket"));
  ASSERT_OK(fs_->GetTargetStats("bucket", &st));
  AssertFileStats(fs_.get(), "bucket", FileType::NonExistent);
}

TEST_F(TestS3FS, CopyFile) {
  FileStats st;

  // "File"
  ASSERT_OK(fs_->CopyFile("bucket/somefile", "bucket/newfile"));
  AssertFileStats(fs_.get(), "bucket/newfile", FileType::File, 9);
  AssertObjectContents(client_.get(), "bucket", "newfile", "some data");
  AssertFileStats(fs_.get(), "bucket/somefile", FileType::File, 9);  // still exists
  // Overwrite
  ASSERT_OK(fs_->CopyFile("bucket/somedir/subdir/subfile", "bucket/newfile"));
  AssertFileStats(fs_.get(), "bucket/newfile", FileType::File, 8);
  AssertObjectContents(client_.get(), "bucket", "newfile", "sub data");

  // Non-existent
  ASSERT_RAISES(IOError, fs_->CopyFile("bucket/non-existent", "bucket/newfile2"));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("non-existent-bucket/somefile", "bucket/newfile2"));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("bucket/somefile", "non-existent-bucket/newfile2"));
  AssertFileStats(fs_.get(), "bucket/newfile2", FileType::NonExistent);
}

TEST_F(TestS3FS, Move) {
  FileStats st;

  // "File"
  ASSERT_OK(fs_->Move("bucket/somefile", "bucket/newfile"));
  AssertFileStats(fs_.get(), "bucket/newfile", FileType::File, 9);
  AssertObjectContents(client_.get(), "bucket", "newfile", "some data");
  // Source was deleted
  AssertFileStats(fs_.get(), "bucket/somefile", FileType::NonExistent);

  // Overwrite
  ASSERT_OK(fs_->Move("bucket/somedir/subdir/subfile", "bucket/newfile"));
  AssertFileStats(fs_.get(), "bucket/newfile", FileType::File, 8);
  AssertObjectContents(client_.get(), "bucket", "newfile", "sub data");
  // Source was deleted
  AssertFileStats(fs_.get(), "bucket/somedir/subdir/subfile", FileType::NonExistent);

  // Non-existent
  ASSERT_RAISES(IOError, fs_->Move("bucket/non-existent", "bucket/newfile2"));
  ASSERT_RAISES(IOError, fs_->Move("non-existent-bucket/somefile", "bucket/newfile2"));
  ASSERT_RAISES(IOError, fs_->Move("bucket/somefile", "non-existent-bucket/newfile2"));
  AssertFileStats(fs_.get(), "bucket/newfile2", FileType::NonExistent);
}

TEST_F(TestS3FS, OpenInputStream) {
  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buf;

  // Non-existent
  ASSERT_RAISES(IOError, fs_->OpenInputStream("non-existent-bucket/somefile", &stream));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/zzzt", &stream));

  // "Files"
  ASSERT_OK(fs_->OpenInputStream("bucket/somefile", &stream));
  ASSERT_OK(stream->Read(2, &buf));
  AssertBufferEqual(*buf, "so");
  ASSERT_OK(stream->Read(5, &buf));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK(stream->Read(5, &buf));
  AssertBufferEqual(*buf, "ta");
  ASSERT_OK(stream->Read(5, &buf));
  AssertBufferEqual(*buf, "");

  ASSERT_OK(fs_->OpenInputStream("bucket/somedir/subdir/subfile", &stream));
  ASSERT_OK(stream->Read(100, &buf));
  AssertBufferEqual(*buf, "sub data");
  ASSERT_OK(stream->Read(100, &buf));
  AssertBufferEqual(*buf, "");

  // "Directories"
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/emptydir", &stream));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/somedir", &stream));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket", &stream));
}

TEST_F(TestS3FS, OpenInputFile) {
  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buf;
  int64_t nbytes = -1, pos = -1;

  // Non-existent
  ASSERT_RAISES(IOError, fs_->OpenInputFile("non-existent-bucket/somefile", &file));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("bucket/zzzt", &file));

  // "Files"
  ASSERT_OK(fs_->OpenInputFile("bucket/somefile", &file));
  ASSERT_OK(file->GetSize(&nbytes));
  ASSERT_EQ(nbytes, 9);
  ASSERT_OK(file->Read(4, &buf));
  AssertBufferEqual(*buf, "some");
  ASSERT_OK(file->GetSize(&nbytes));
  ASSERT_EQ(nbytes, 9);
  ASSERT_OK(file->Tell(&pos));
  ASSERT_EQ(pos, 4);

  ASSERT_OK(file->ReadAt(2, 5, &buf));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK(file->Tell(&pos));
  ASSERT_EQ(pos, 4);
  ASSERT_OK(file->ReadAt(5, 20, &buf));
  AssertBufferEqual(*buf, "data");
  ASSERT_OK(file->ReadAt(9, 20, &buf));
  AssertBufferEqual(*buf, "");
  // Reading past end of file
  ASSERT_RAISES(IOError, file->ReadAt(10, 20, &buf));

  ASSERT_OK(file->Seek(5));
  ASSERT_OK(file->Read(2, &buf));
  AssertBufferEqual(*buf, "da");
  ASSERT_OK(file->Seek(9));
  ASSERT_OK(file->Read(2, &buf));
  AssertBufferEqual(*buf, "");
  // Seeking past end of file
  ASSERT_RAISES(IOError, file->Seek(10));
}

TEST_F(TestS3FS, OpenOutputStream) {
  std::shared_ptr<io::OutputStream> stream;

  // Non-existent
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("non-existent-bucket/somefile", &stream));

  // Create new empty file
  ASSERT_OK(fs_->OpenOutputStream("bucket/newfile1", &stream));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "bucket", "newfile1", "");

  // Create new file with 1 small write
  ASSERT_OK(fs_->OpenOutputStream("bucket/newfile2", &stream));
  ASSERT_OK(stream->Write("some data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "bucket", "newfile2", "some data");

  // Create new file with 3 small writes
  ASSERT_OK(fs_->OpenOutputStream("bucket/newfile3", &stream));
  ASSERT_OK(stream->Write("some "));
  ASSERT_OK(stream->Write(""));
  ASSERT_OK(stream->Write("new data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "bucket", "newfile3", "some new data");

  // Create new file with some large writes
  std::string s1, s2, s3, s4, s5;
  s1 = random_string(6000000, /*seed =*/42);  // More than the 5 MB minimum part upload
  s2 = "xxx";
  s3 = random_string(6000000, 43);
  s4 = "zzz";
  s5 = random_string(600000, 44);
  ASSERT_OK(fs_->OpenOutputStream("bucket/newfile4", &stream));
  ASSERT_OK(stream->Write(s1));
  ASSERT_OK(stream->Write(s2));
  ASSERT_OK(stream->Write(s3));
  ASSERT_OK(stream->Write(s4));
  ASSERT_OK(stream->Write(s5));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "bucket", "newfile4", s1 + s2 + s3 + s4 + s5);

  // Overwrite
  ASSERT_OK(fs_->OpenOutputStream("bucket/newfile1", &stream));
  ASSERT_OK(stream->Write("overwritten data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "bucket", "newfile1", "overwritten data");

  // Overwrite and make empty
  ASSERT_OK(fs_->OpenOutputStream("bucket/newfile1", &stream));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "bucket", "newfile1", "");
}

TEST_F(TestS3FS, OpenOutputStreamAbort) {
  std::shared_ptr<io::OutputStream> stream;
  ASSERT_OK(fs_->OpenOutputStream("bucket/somefile", &stream));
  ASSERT_OK(stream->Write("new data"));
  // Destructor implicitly aborts stream and the underlying multipart upload.
  stream.reset();
  AssertObjectContents(client_.get(), "bucket", "somefile", "some data");
}

////////////////////////////////////////////////////////////////////////////
// Generic S3 tests

class TestS3FSGeneric : public S3TestMixin, public GenericFileSystemTest {
 public:
  void SetUp() override {
    S3TestMixin::SetUp();
    // Set up test bucket
    {
      Aws::S3::Model::CreateBucketRequest req;
      req.SetBucket(ToAwsString("s3fs-test-bucket"));
      ASSERT_OK(OutcomeToStatus(client_->CreateBucket(req)));
    }

    options_.access_key = minio_.access_key();
    options_.secret_key = minio_.secret_key();
    options_.scheme = "http";
    options_.endpoint_override = minio_.connect_string();
    ASSERT_OK(S3FileSystem::Make(options_, &s3fs_));
    fs_ = std::make_shared<SubTreeFileSystem>("s3fs-test-bucket", s3fs_);
  }

 protected:
  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  bool have_implicit_directories() const override { return true; }
  bool allow_write_file_over_dir() const override { return true; }
  bool allow_move_dir() const override { return false; }
  bool allow_append_to_file() const override { return false; }
  bool have_directory_mtimes() const override { return false; }

  S3Options options_;
  std::shared_ptr<S3FileSystem> s3fs_;
  std::shared_ptr<FileSystem> fs_;
};

GENERIC_FS_TEST_FUNCTIONS(TestS3FSGeneric);

}  // namespace fs
}  // namespace arrow

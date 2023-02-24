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
#include <exception>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
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
#include <aws/core/Version.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/sts/STSClient.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3_internal.h"
#include "arrow/filesystem/s3_test_util.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace fs {

using ::arrow::internal::checked_pointer_cast;
using ::arrow::internal::PlatformFilename;
using ::arrow::internal::UriEscape;

using ::arrow::fs::internal::ConnectRetryStrategy;
using ::arrow::fs::internal::ErrorToStatus;
using ::arrow::fs::internal::OutcomeToStatus;
using ::arrow::fs::internal::ToAwsString;

// Use "short" retry parameters to make tests faster
static constexpr int32_t kRetryInterval = 50;      /* milliseconds */
static constexpr int32_t kMaxRetryDuration = 6000; /* milliseconds */

::testing::Environment* s3_env = ::testing::AddGlobalTestEnvironment(new S3Environment);

::testing::Environment* minio_env =
    ::testing::AddGlobalTestEnvironment(new MinioTestEnvironment);

MinioTestEnvironment* GetMinioEnv() {
  return ::arrow::internal::checked_cast<MinioTestEnvironment*>(minio_env);
}

class ShortRetryStrategy : public S3RetryStrategy {
 public:
  bool ShouldRetry(const AWSErrorDetail& error, int64_t attempted_retries) override {
    if (error.message.find(kFileExistsMessage) != error.message.npos) {
      // Minio returns "file exists" errors (when calling mkdir) as internal errors,
      // which would trigger spurious retries.
      return false;
    }
    return IsRetryable(error) && (attempted_retries * kRetryInterval < kMaxRetryDuration);
  }

  int64_t CalculateDelayBeforeNextRetry(const AWSErrorDetail& error,
                                        int64_t attempted_retries) override {
    return kRetryInterval;
  }

  bool IsRetryable(const AWSErrorDetail& error) const {
    return error.should_retry || error.exception_name == "XMinioServerNotInitialized";
  }

#ifdef _WIN32
  static constexpr const char* kFileExistsMessage = "file already exists";
#else
  static constexpr const char* kFileExistsMessage = "file exists";
#endif
};

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

class AwsTestMixin : public ::testing::Test {
 public:
  // We set this environment variable to speed up tests by ensuring
  // DefaultAWSCredentialsProviderChain does not query (inaccessible)
  // EC2 metadata endpoint
  AwsTestMixin() : ec2_metadata_disabled_guard_("AWS_EC2_METADATA_DISABLED", "true") {}

  void SetUp() override {
#ifdef AWS_CPP_SDK_S3_NOT_SHARED
    auto aws_log_level = Aws::Utils::Logging::LogLevel::Fatal;
    aws_options_.loggingOptions.logLevel = aws_log_level;
    aws_options_.loggingOptions.logger_create_fn = [&aws_log_level] {
      return std::make_shared<Aws::Utils::Logging::ConsoleLogSystem>(aws_log_level);
    };
    Aws::InitAPI(aws_options_);
#endif
  }

  void TearDown() override {
#ifdef AWS_CPP_SDK_S3_NOT_SHARED
    Aws::ShutdownAPI(aws_options_);
#endif
  }

 private:
  EnvVarGuard ec2_metadata_disabled_guard_;
#ifdef AWS_CPP_SDK_S3_NOT_SHARED
  Aws::SDKOptions aws_options_;
#endif
};

class S3TestMixin : public AwsTestMixin {
 public:
  void SetUp() override {
    AwsTestMixin::SetUp();

    // Starting the server may fail, for example if the generated port number
    // was "stolen" by another process. Run a dummy S3 operation to make sure it
    // is running, otherwise retry a number of times.
    Status connect_status;
    int retries = kNumServerRetries;
    do {
      InitServerAndClient();
      connect_status = OutcomeToStatus("ListBuckets", client_->ListBuckets());
    } while (!connect_status.ok() && --retries > 0);
    ASSERT_OK(connect_status);
  }

  void TearDown() override {
    client_.reset();  // Aws::S3::S3Client destruction relies on AWS SDK, so it must be
                      // reset before Aws::ShutdownAPI
    AwsTestMixin::TearDown();
  }

 protected:
  void InitServerAndClient() {
    ASSERT_OK_AND_ASSIGN(minio_, GetMinioEnv()->GetOneServer());
    client_config_.reset(new Aws::Client::ClientConfiguration());
    client_config_->endpointOverride = ToAwsString(minio_->connect_string());
    client_config_->scheme = Aws::Http::Scheme::HTTP;
    client_config_->retryStrategy =
        std::make_shared<ConnectRetryStrategy>(kRetryInterval, kMaxRetryDuration);
    credentials_ = {ToAwsString(minio_->access_key()), ToAwsString(minio_->secret_key())};
    bool use_virtual_addressing = false;
    client_.reset(
        new Aws::S3::S3Client(credentials_, *client_config_,
                              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                              use_virtual_addressing));
  }

  // How many times to try launching a server in a row before decreeing failure
  static constexpr int kNumServerRetries = 3;

  std::shared_ptr<MinioTestServer> minio_;
  std::unique_ptr<Aws::Client::ClientConfiguration> client_config_;
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
  ASSERT_EQ(actual.size(), expected.size());
  ASSERT_TRUE(actual == expected);  // Avoid ASSERT_EQ on large data
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
// S3Options tests

class S3OptionsTest : public AwsTestMixin {};

TEST_F(S3OptionsTest, FromUri) {
  std::string path;
  S3Options options;

  ASSERT_OK_AND_ASSIGN(options, S3Options::FromUri("s3://", &path));
  ASSERT_EQ(options.region, "");
  ASSERT_EQ(options.scheme, "https");
  ASSERT_EQ(options.endpoint_override, "");
  ASSERT_EQ(path, "");

  ASSERT_OK_AND_ASSIGN(options, S3Options::FromUri("s3:", &path));
  ASSERT_EQ(path, "");

  ASSERT_OK_AND_ASSIGN(options, S3Options::FromUri("s3://access:secret@mybucket", &path));
  ASSERT_EQ(path, "mybucket");
  const auto creds = options.credentials_provider->GetAWSCredentials();
  ASSERT_EQ(creds.GetAWSAccessKeyId(), "access");
  ASSERT_EQ(creds.GetAWSSecretKey(), "secret");

  ASSERT_OK_AND_ASSIGN(options, S3Options::FromUri("s3://mybucket/", &path));
  ASSERT_NE(options.region, "");  // Some region was chosen
  ASSERT_EQ(options.scheme, "https");
  ASSERT_EQ(options.endpoint_override, "");
  ASSERT_EQ(path, "mybucket");

  ASSERT_OK_AND_ASSIGN(options, S3Options::FromUri("s3://mybucket/foo/bar/", &path));
  ASSERT_NE(options.region, "");
  ASSERT_EQ(options.scheme, "https");
  ASSERT_EQ(options.endpoint_override, "");
  ASSERT_EQ(path, "mybucket/foo/bar");

  // Region resolution with a well-known bucket
  ASSERT_OK_AND_ASSIGN(
      options, S3Options::FromUri("s3://aws-earth-mo-atmospheric-ukv-prd/", &path));
  ASSERT_EQ(options.region, "eu-west-2");

  // Explicit region override
  ASSERT_OK_AND_ASSIGN(
      options,
      S3Options::FromUri(
          "s3://mybucket/foo/bar/?region=utopia&endpoint_override=localhost&scheme=http",
          &path));
  ASSERT_EQ(options.region, "utopia");
  ASSERT_EQ(options.scheme, "http");
  ASSERT_EQ(options.endpoint_override, "localhost");
  ASSERT_EQ(path, "mybucket/foo/bar");

  // Missing bucket name
  ASSERT_RAISES(Invalid, S3Options::FromUri("s3:///foo/bar/", &path));

  // Invalid option
  ASSERT_RAISES(Invalid, S3Options::FromUri("s3://mybucket/?xxx=zzz", &path));
}

TEST_F(S3OptionsTest, FromAccessKey) {
  S3Options options;

  // session token is optional and should default to empty string
  options = S3Options::FromAccessKey("access", "secret");
  ASSERT_EQ(options.GetAccessKey(), "access");
  ASSERT_EQ(options.GetSecretKey(), "secret");
  ASSERT_EQ(options.GetSessionToken(), "");

  options = S3Options::FromAccessKey("access", "secret", "token");
  ASSERT_EQ(options.GetAccessKey(), "access");
  ASSERT_EQ(options.GetSecretKey(), "secret");
  ASSERT_EQ(options.GetSessionToken(), "token");
}

TEST_F(S3OptionsTest, FromAssumeRole) {
  S3Options options;

  // arn should be only required argument
  options = S3Options::FromAssumeRole("my_role_arn");
  options = S3Options::FromAssumeRole("my_role_arn", "session");
  options = S3Options::FromAssumeRole("my_role_arn", "session", "id");
  options = S3Options::FromAssumeRole("my_role_arn", "session", "id", 42);

  // test w/ custom STSClient (will not use DefaultAWSCredentialsProviderChain)
  Aws::Auth::AWSCredentials test_creds = Aws::Auth::AWSCredentials("access", "secret");
  std::shared_ptr<Aws::STS::STSClient> sts_client =
      std::make_shared<Aws::STS::STSClient>(Aws::STS::STSClient(test_creds));
  options = S3Options::FromAssumeRole("my_role_arn", "session", "id", 42, sts_client);
}

////////////////////////////////////////////////////////////////////////////
// Region resolution test

class S3RegionResolutionTest : public AwsTestMixin {};

TEST_F(S3RegionResolutionTest, PublicBucket) {
  ASSERT_OK_AND_EQ("us-east-2", ResolveS3BucketRegion("voltrondata-labs-datasets"));

  // Taken from a registry of open S3-hosted datasets
  // at https://github.com/awslabs/open-data-registry
  ASSERT_OK_AND_EQ("eu-west-2",
                   ResolveS3BucketRegion("aws-earth-mo-atmospheric-ukv-prd"));
  // Same again, cached
  ASSERT_OK_AND_EQ("eu-west-2",
                   ResolveS3BucketRegion("aws-earth-mo-atmospheric-ukv-prd"));
}

TEST_F(S3RegionResolutionTest, RestrictedBucket) {
  ASSERT_OK_AND_EQ("us-west-2", ResolveS3BucketRegion("ursa-labs-r-test"));
  // Same again, cached
  ASSERT_OK_AND_EQ("us-west-2", ResolveS3BucketRegion("ursa-labs-r-test"));
}

TEST_F(S3RegionResolutionTest, NonExistentBucket) {
  auto maybe_region = ResolveS3BucketRegion("ursa-labs-non-existent-bucket");
  ASSERT_RAISES(IOError, maybe_region);
  ASSERT_THAT(maybe_region.status().message(),
              ::testing::HasSubstr("Bucket 'ursa-labs-non-existent-bucket' not found"));
}

TEST_F(S3RegionResolutionTest, InvalidBucketName) {
  ASSERT_RAISES(Invalid, ResolveS3BucketRegion("s3:bucket"));
  ASSERT_RAISES(Invalid, ResolveS3BucketRegion("foo/bar"));
}

////////////////////////////////////////////////////////////////////////////
// S3FileSystem region test

class S3FileSystemRegionTest : public AwsTestMixin {};

TEST_F(S3FileSystemRegionTest, Default) {
  ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFromUri("s3://"));
  auto s3fs = checked_pointer_cast<S3FileSystem>(fs);
  ASSERT_EQ(s3fs->region(), "us-east-1");
}

// Skipped on Windows, as the AWS SDK ignores runtime environment changes:
// https://github.com/aws/aws-sdk-cpp/issues/1476

#ifndef _WIN32
TEST_F(S3FileSystemRegionTest, EnvironmentVariable) {
  // Region override with environment variable (AWS SDK >= 1.8)
  EnvVarGuard region_guard("AWS_DEFAULT_REGION", "eu-north-1");

  ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFromUri("s3://"));
  auto s3fs = checked_pointer_cast<S3FileSystem>(fs);

  if (Aws::Version::GetVersionMajor() > 1 || Aws::Version::GetVersionMinor() >= 8) {
    ASSERT_EQ(s3fs->region(), "eu-north-1");
  } else {
    ASSERT_EQ(s3fs->region(), "us-east-1");
  }
}
#endif

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
    // Most tests will create buckets
    options_.allow_bucket_creation = true;
    options_.allow_bucket_deletion = true;
    MakeFileSystem();
    // Set up test bucket
    {
      Aws::S3::Model::CreateBucketRequest req;
      req.SetBucket(ToAwsString("bucket"));
      ASSERT_OK(OutcomeToStatus("CreateBucket", client_->CreateBucket(req)));
      req.SetBucket(ToAwsString("empty-bucket"));
      ASSERT_OK(OutcomeToStatus("CreateBucket", client_->CreateBucket(req)));
    }
    {
      Aws::S3::Model::PutObjectRequest req;
      req.SetBucket(ToAwsString("bucket"));
      req.SetKey(ToAwsString("emptydir/"));
      req.SetBody(std::make_shared<std::stringstream>(""));
      ASSERT_OK(OutcomeToStatus("PutObject", client_->PutObject(req)));
      // NOTE: no need to create intermediate "directories" somedir/ and
      // somedir/subdir/
      req.SetKey(ToAwsString("somedir/subdir/subfile"));
      req.SetBody(std::make_shared<std::stringstream>("sub data"));
      ASSERT_OK(OutcomeToStatus("PutObject", client_->PutObject(req)));
      req.SetKey(ToAwsString("somefile"));
      req.SetBody(std::make_shared<std::stringstream>("some data"));
      req.SetContentType("x-arrow/test");
      ASSERT_OK(OutcomeToStatus("PutObject", client_->PutObject(req)));
      req.SetKey(ToAwsString("otherdir/1/2/3/otherfile"));
      req.SetBody(std::make_shared<std::stringstream>("other data"));
      ASSERT_OK(OutcomeToStatus("PutObject", client_->PutObject(req)));
    }
  }

  void MakeFileSystem() {
    options_.ConfigureAccessKey(minio_->access_key(), minio_->secret_key());
    options_.scheme = "http";
    options_.endpoint_override = minio_->connect_string();
    if (!options_.retry_strategy) {
      options_.retry_strategy = std::make_shared<ShortRetryStrategy>();
    }
    ASSERT_OK_AND_ASSIGN(fs_, S3FileSystem::Make(options_));
  }

  template <typename Matcher>
  void AssertMetadataRoundtrip(const std::string& path,
                               const std::shared_ptr<const KeyValueMetadata>& metadata,
                               Matcher&& matcher) {
    ASSERT_OK_AND_ASSIGN(auto output, fs_->OpenOutputStream(path, metadata));
    ASSERT_OK(output->Close());
    ASSERT_OK_AND_ASSIGN(auto input, fs_->OpenInputStream(path));
    ASSERT_OK_AND_ASSIGN(auto got_metadata, input->ReadMetadata());
    ASSERT_NE(got_metadata, nullptr);
    ASSERT_THAT(got_metadata->sorted_pairs(), matcher);
  }

  void AssertInfoAllBucketsRecursive(const std::vector<FileInfo>& infos) {
    AssertFileInfo(infos[0], "bucket", FileType::Directory);
    AssertFileInfo(infos[1], "bucket/emptydir", FileType::Directory);
    AssertFileInfo(infos[2], "bucket/otherdir", FileType::Directory);
    AssertFileInfo(infos[3], "bucket/otherdir/1", FileType::Directory);
    AssertFileInfo(infos[4], "bucket/otherdir/1/2", FileType::Directory);
    AssertFileInfo(infos[5], "bucket/otherdir/1/2/3", FileType::Directory);
    AssertFileInfo(infos[6], "bucket/otherdir/1/2/3/otherfile", FileType::File, 10);
    AssertFileInfo(infos[7], "bucket/somedir", FileType::Directory);
    AssertFileInfo(infos[8], "bucket/somedir/subdir", FileType::Directory);
    AssertFileInfo(infos[9], "bucket/somedir/subdir/subfile", FileType::File, 8);
    AssertFileInfo(infos[10], "bucket/somefile", FileType::File, 9);
    AssertFileInfo(infos[11], "empty-bucket", FileType::Directory);
  }

  void TestOpenOutputStream() {
    std::shared_ptr<io::OutputStream> stream;

    // Nonexistent
    ASSERT_RAISES(IOError, fs_->OpenOutputStream("nonexistent-bucket/somefile"));

    // URI
    ASSERT_RAISES(Invalid, fs_->OpenOutputStream("s3:bucket/newfile1"));

    // Create new empty file
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile1"));
    ASSERT_OK(stream->Close());
    AssertObjectContents(client_.get(), "bucket", "newfile1", "");

    // Create new file with 1 small write
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile2"));
    ASSERT_OK(stream->Write("some data"));
    ASSERT_OK(stream->Close());
    AssertObjectContents(client_.get(), "bucket", "newfile2", "some data");

    // Create new file with 3 small writes
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile3"));
    ASSERT_OK(stream->Write("some "));
    ASSERT_OK(stream->Write(""));
    ASSERT_OK(stream->Write("new data"));
    ASSERT_OK(stream->Close());
    AssertObjectContents(client_.get(), "bucket", "newfile3", "some new data");

    // Create new file with some large writes
    std::string s1, s2, s3, s4, s5, expected;
    s1 = random_string(6000000, /*seed =*/42);  // More than the 5 MB minimum part upload
    s2 = "xxx";
    s3 = random_string(6000000, 43);
    s4 = "zzz";
    s5 = random_string(600000, 44);
    expected = s1 + s2 + s3 + s4 + s5;
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile4"));
    for (auto input : {s1, s2, s3, s4, s5}) {
      ASSERT_OK(stream->Write(input));
      // Clobber source contents.  This shouldn't reflect in the data written.
      input.front() = 'x';
      input.back() = 'x';
    }
    ASSERT_OK(stream->Close());
    AssertObjectContents(client_.get(), "bucket", "newfile4", expected);

    // Overwrite
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile1"));
    ASSERT_OK(stream->Write("overwritten data"));
    ASSERT_OK(stream->Close());
    AssertObjectContents(client_.get(), "bucket", "newfile1", "overwritten data");

    // Overwrite and make empty
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile1"));
    ASSERT_OK(stream->Close());
    AssertObjectContents(client_.get(), "bucket", "newfile1", "");

    // Open file and then lose filesystem reference
    ASSERT_EQ(fs_.use_count(), 1);  // needed for test to work
    std::weak_ptr<S3FileSystem> weak_fs(fs_);
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile99"));
    fs_.reset();
    ASSERT_OK(stream->Write("some other data"));
    ASSERT_OK(stream->Close());
    ASSERT_TRUE(weak_fs.expired());
    AssertObjectContents(client_.get(), "bucket", "newfile99", "some other data");
  }

  void TestOpenOutputStreamAbort() {
    std::shared_ptr<io::OutputStream> stream;
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/somefile"));
    ASSERT_OK(stream->Write("new data"));
    // Abort() cancels the multipart upload.
    ASSERT_OK(stream->Abort());
    ASSERT_EQ(stream->closed(), true);
    AssertObjectContents(client_.get(), "bucket", "somefile", "some data");
  }

  void TestOpenOutputStreamDestructor() {
    std::shared_ptr<io::OutputStream> stream;
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/somefile"));
    ASSERT_OK(stream->Write("new data"));
    // Destructor implicitly closes stream and completes the multipart upload.
    stream.reset();
    AssertObjectContents(client_.get(), "bucket", "somefile", "new data");
  }

 protected:
  S3Options options_;
  std::shared_ptr<S3FileSystem> fs_;
};

TEST_F(TestS3FS, GetFileInfoRoot) { AssertFileInfo(fs_.get(), "", FileType::Directory); }

TEST_F(TestS3FS, GetFileInfoBucket) {
  AssertFileInfo(fs_.get(), "bucket", FileType::Directory);
  AssertFileInfo(fs_.get(), "empty-bucket", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-bucket", FileType::NotFound);
  // Trailing slashes
  AssertFileInfo(fs_.get(), "bucket/", FileType::Directory);
  AssertFileInfo(fs_.get(), "empty-bucket/", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-bucket/", FileType::NotFound);

  // URIs
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("s3:bucket"));
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("s3:empty-bucket"));
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("s3:nonexistent-bucket"));
}

TEST_F(TestS3FS, GetFileInfoObject) {
  // "Directories"
  AssertFileInfo(fs_.get(), "bucket/emptydir", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/otherdir", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/otherdir/1", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/otherdir/1/2", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/otherdir/1/2/3", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/somedir", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/somedir/subdir", FileType::Directory, kNoSize);

  // "Files"
  AssertFileInfo(fs_.get(), "bucket/otherdir/1/2/3/otherfile", FileType::File, 10);
  AssertFileInfo(fs_.get(), "bucket/somefile", FileType::File, 9);
  AssertFileInfo(fs_.get(), "bucket/somedir/subdir/subfile", FileType::File, 8);

  // Nonexistent
  AssertFileInfo(fs_.get(), "bucket/emptyd", FileType::NotFound);
  AssertFileInfo(fs_.get(), "bucket/somed", FileType::NotFound);
  AssertFileInfo(fs_.get(), "non-existent-bucket/somed", FileType::NotFound);

  // Trailing slashes
  AssertFileInfo(fs_.get(), "bucket/emptydir/", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/somefile/", FileType::File, 9);
  AssertFileInfo(fs_.get(), "bucket/emptyd/", FileType::NotFound);
  AssertFileInfo(fs_.get(), "non-existent-bucket/somed/", FileType::NotFound);

  // URIs
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("s3:bucket/emptydir"));
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("s3:bucket/otherdir"));
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("s3:bucket/somefile"));
}

TEST_F(TestS3FS, GetFileInfoSelector) {
  FileSelector select;
  std::vector<FileInfo> infos;

  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "bucket", FileType::Directory);
  AssertFileInfo(infos[1], "empty-bucket", FileType::Directory);

  // Empty bucket
  select.base_dir = "empty-bucket";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  // Nonexistent bucket
  select.base_dir = "nonexistent-bucket";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;
  // Non-empty bucket
  select.base_dir = "bucket";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 4);
  AssertFileInfo(infos[0], "bucket/emptydir", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/otherdir", FileType::Directory);
  AssertFileInfo(infos[2], "bucket/somedir", FileType::Directory);
  AssertFileInfo(infos[3], "bucket/somefile", FileType::File, 9);

  // Empty "directory"
  select.base_dir = "bucket/emptydir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  // Non-empty "directories"
  select.base_dir = "bucket/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "bucket/somedir/subdir", FileType::Directory);
  select.base_dir = "bucket/somedir/subdir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "bucket/somedir/subdir/subfile", FileType::File, 8);
  // Nonexistent
  select.base_dir = "bucket/nonexistent";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;

  // Trailing slashes
  select.base_dir = "empty-bucket/";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.base_dir = "nonexistent-bucket/";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.base_dir = "bucket/";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 4);

  // URIs
  select.base_dir = "s3:bucket";
  ASSERT_RAISES(Invalid, fs_->GetFileInfo(select));
  select.base_dir = "s3:bucket/somedir";
  ASSERT_RAISES(Invalid, fs_->GetFileInfo(select));
}

TEST_F(TestS3FS, GetFileInfoSelectorRecursive) {
  FileSelector select;
  std::vector<FileInfo> infos;
  select.recursive = true;

  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 12);
  SortInfos(&infos);
  AssertInfoAllBucketsRecursive(infos);

  // Empty bucket
  select.base_dir = "empty-bucket";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // Non-empty bucket
  select.base_dir = "bucket";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 10);
  AssertFileInfo(infos[0], "bucket/emptydir", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/otherdir", FileType::Directory);
  AssertFileInfo(infos[2], "bucket/otherdir/1", FileType::Directory);
  AssertFileInfo(infos[3], "bucket/otherdir/1/2", FileType::Directory);
  AssertFileInfo(infos[4], "bucket/otherdir/1/2/3", FileType::Directory);
  AssertFileInfo(infos[5], "bucket/otherdir/1/2/3/otherfile", FileType::File, 10);
  AssertFileInfo(infos[6], "bucket/somedir", FileType::Directory);
  AssertFileInfo(infos[7], "bucket/somedir/subdir", FileType::Directory);
  AssertFileInfo(infos[8], "bucket/somedir/subdir/subfile", FileType::File, 8);
  AssertFileInfo(infos[9], "bucket/somefile", FileType::File, 9);

  // Empty "directory"
  select.base_dir = "bucket/emptydir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // Non-empty "directories"
  select.base_dir = "bucket/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 2);
  AssertFileInfo(infos[0], "bucket/somedir/subdir", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/somedir/subdir/subfile", FileType::File, 8);

  select.base_dir = "bucket/otherdir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 4);
  AssertFileInfo(infos[0], "bucket/otherdir/1", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/otherdir/1/2", FileType::Directory);
  AssertFileInfo(infos[2], "bucket/otherdir/1/2/3", FileType::Directory);
  AssertFileInfo(infos[3], "bucket/otherdir/1/2/3/otherfile", FileType::File, 10);
}

TEST_F(TestS3FS, GetFileInfoGenerator) {
  FileSelector select;
  FileInfoVector infos;

  // Root dir
  select.base_dir = "";
  CollectFileInfoGenerator(fs_->GetFileInfoGenerator(select), &infos);
  ASSERT_EQ(infos.size(), 2);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "bucket", FileType::Directory);
  AssertFileInfo(infos[1], "empty-bucket", FileType::Directory);

  // Root dir, recursive
  select.recursive = true;
  CollectFileInfoGenerator(fs_->GetFileInfoGenerator(select), &infos);
  ASSERT_EQ(infos.size(), 12);
  SortInfos(&infos);
  AssertInfoAllBucketsRecursive(infos);

  // Non-root dir case is tested by generic tests
}

TEST_F(TestS3FS, CreateDir) {
  FileInfo st;

  // Existing bucket
  ASSERT_OK(fs_->CreateDir("bucket"));
  AssertFileInfo(fs_.get(), "bucket", FileType::Directory);

  // New bucket
  AssertFileInfo(fs_.get(), "new-bucket", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("new-bucket"));
  AssertFileInfo(fs_.get(), "new-bucket", FileType::Directory);

  // Existing "directory"
  AssertFileInfo(fs_.get(), "bucket/somedir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("bucket/somedir"));
  AssertFileInfo(fs_.get(), "bucket/somedir", FileType::Directory);

  AssertFileInfo(fs_.get(), "bucket/emptydir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("bucket/emptydir"));
  AssertFileInfo(fs_.get(), "bucket/emptydir", FileType::Directory);

  // New "directory"
  AssertFileInfo(fs_.get(), "bucket/newdir", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("bucket/newdir"));
  AssertFileInfo(fs_.get(), "bucket/newdir", FileType::Directory);

  // New "directory", recursive
  ASSERT_OK(fs_->CreateDir("bucket/newdir/newsub/newsubsub", /*recursive=*/true));
  AssertFileInfo(fs_.get(), "bucket/newdir/newsub", FileType::Directory);
  AssertFileInfo(fs_.get(), "bucket/newdir/newsub/newsubsub", FileType::Directory);

  // Existing "file", should fail
  ASSERT_RAISES(IOError, fs_->CreateDir("bucket/somefile"));

  // URI
  ASSERT_RAISES(Invalid, fs_->CreateDir("s3:bucket/newdir2"));
}

TEST_F(TestS3FS, DeleteFile) {
  // Bucket
  ASSERT_RAISES(IOError, fs_->DeleteFile("bucket"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("empty-bucket"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("nonexistent-bucket"));

  // "File"
  ASSERT_OK(fs_->DeleteFile("bucket/somefile"));
  AssertFileInfo(fs_.get(), "bucket/somefile", FileType::NotFound);
  ASSERT_RAISES(IOError, fs_->DeleteFile("bucket/somefile"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("bucket/nonexistent"));

  // "Directory"
  ASSERT_RAISES(IOError, fs_->DeleteFile("bucket/somedir"));
  AssertFileInfo(fs_.get(), "bucket/somedir", FileType::Directory);

  // URI
  ASSERT_RAISES(Invalid, fs_->DeleteFile("s3:bucket/somefile"));
}

TEST_F(TestS3FS, DeleteDir) {
  FileSelector select;
  select.base_dir = "bucket";
  std::vector<FileInfo> infos;

  // Empty "directory"
  ASSERT_OK(fs_->DeleteDir("bucket/emptydir"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 3);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "bucket/otherdir", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/somedir", FileType::Directory);
  AssertFileInfo(infos[2], "bucket/somefile", FileType::File);

  // Non-empty "directory"
  ASSERT_OK(fs_->DeleteDir("bucket/somedir"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  AssertFileInfo(infos[0], "bucket/otherdir", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/somefile", FileType::File);

  // Leaving parent "directory" empty
  ASSERT_OK(fs_->CreateDir("bucket/newdir/newsub/newsubsub"));
  ASSERT_OK(fs_->DeleteDir("bucket/newdir/newsub"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 3);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "bucket/newdir", FileType::Directory);  // still exists
  AssertFileInfo(infos[1], "bucket/otherdir", FileType::Directory);
  AssertFileInfo(infos[2], "bucket/somefile", FileType::File);

  // Bucket
  ASSERT_OK(fs_->DeleteDir("bucket"));
  AssertFileInfo(fs_.get(), "bucket", FileType::NotFound);

  // URI
  ASSERT_RAISES(Invalid, fs_->DeleteDir("s3:empty-bucket"));
}

TEST_F(TestS3FS, DeleteDirContents) {
  FileSelector select;
  select.base_dir = "bucket";
  std::vector<FileInfo> infos;

  ASSERT_OK(fs_->DeleteDirContents("bucket/doesnotexist", /*missing_dir_ok=*/true));
  ASSERT_OK(fs_->DeleteDirContents("bucket/emptydir"));
  ASSERT_OK(fs_->DeleteDirContents("bucket/somedir"));
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("bucket/somefile"));
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("bucket/doesnotexist"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 4);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "bucket/emptydir", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/otherdir", FileType::Directory);
  AssertFileInfo(infos[2], "bucket/somedir", FileType::Directory);
  AssertFileInfo(infos[3], "bucket/somefile", FileType::File);
}

TEST_F(TestS3FS, DeleteDirContentsAsync) {
  FileSelector select;
  select.base_dir = "bucket";
  std::vector<FileInfo> infos;

  ASSERT_FINISHES_OK(fs_->DeleteDirContentsAsync("bucket/emptydir"));
  ASSERT_FINISHES_OK(fs_->DeleteDirContentsAsync("bucket/somedir"));
  ASSERT_FINISHES_AND_RAISES(IOError, fs_->DeleteDirContentsAsync("bucket/somefile"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 4);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "bucket/emptydir", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/otherdir", FileType::Directory);
  AssertFileInfo(infos[2], "bucket/somedir", FileType::Directory);
  AssertFileInfo(infos[3], "bucket/somefile", FileType::File);
}

TEST_F(TestS3FS, CopyFile) {
  // "File"
  ASSERT_OK(fs_->CopyFile("bucket/somefile", "bucket/newfile"));
  AssertFileInfo(fs_.get(), "bucket/newfile", FileType::File, 9);
  AssertObjectContents(client_.get(), "bucket", "newfile", "some data");
  AssertFileInfo(fs_.get(), "bucket/somefile", FileType::File, 9);  // still exists
  // Overwrite
  ASSERT_OK(fs_->CopyFile("bucket/somedir/subdir/subfile", "bucket/newfile"));
  AssertFileInfo(fs_.get(), "bucket/newfile", FileType::File, 8);
  AssertObjectContents(client_.get(), "bucket", "newfile", "sub data");
  // ARROW-13048: URL-encoded paths
  ASSERT_OK(fs_->CopyFile("bucket/somefile", "bucket/a=2/newfile"));
  ASSERT_OK(fs_->CopyFile("bucket/a=2/newfile", "bucket/a=3/newfile"));
  // Nonexistent
  ASSERT_RAISES(IOError, fs_->CopyFile("bucket/nonexistent", "bucket/newfile2"));
  ASSERT_RAISES(IOError, fs_->CopyFile("nonexistent-bucket/somefile", "bucket/newfile2"));
  ASSERT_RAISES(IOError, fs_->CopyFile("bucket/somefile", "nonexistent-bucket/newfile2"));
  AssertFileInfo(fs_.get(), "bucket/newfile2", FileType::NotFound);
}

TEST_F(TestS3FS, Move) {
  // "File"
  ASSERT_OK(fs_->Move("bucket/somefile", "bucket/newfile"));
  AssertFileInfo(fs_.get(), "bucket/newfile", FileType::File, 9);
  AssertObjectContents(client_.get(), "bucket", "newfile", "some data");
  // Source was deleted
  AssertFileInfo(fs_.get(), "bucket/somefile", FileType::NotFound);

  // Overwrite
  ASSERT_OK(fs_->Move("bucket/somedir/subdir/subfile", "bucket/newfile"));
  AssertFileInfo(fs_.get(), "bucket/newfile", FileType::File, 8);
  AssertObjectContents(client_.get(), "bucket", "newfile", "sub data");
  // Source was deleted
  AssertFileInfo(fs_.get(), "bucket/somedir/subdir/subfile", FileType::NotFound);

  // ARROW-13048: URL-encoded paths
  ASSERT_OK(fs_->Move("bucket/newfile", "bucket/a=2/newfile"));
  ASSERT_OK(fs_->Move("bucket/a=2/newfile", "bucket/a=3/newfile"));

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->Move("bucket/non-existent", "bucket/newfile2"));
  ASSERT_RAISES(IOError, fs_->Move("nonexistent-bucket/somefile", "bucket/newfile2"));
  ASSERT_RAISES(IOError, fs_->Move("bucket/somefile", "nonexistent-bucket/newfile2"));
  AssertFileInfo(fs_.get(), "bucket/newfile2", FileType::NotFound);
}

TEST_F(TestS3FS, OpenInputStream) {
  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buf;

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->OpenInputStream("nonexistent-bucket/somefile"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/zzzt"));

  // URI
  ASSERT_RAISES(Invalid, fs_->OpenInputStream("s3:bucket/somefile"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("bucket/somefile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(2));
  AssertBufferEqual(*buf, "so");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "ta");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "");

  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("bucket/somedir/subdir/subfile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "sub data");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "");
  ASSERT_OK(stream->Close());

  // "Directories"
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/emptydir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket"));

  // Open file and then lose filesystem reference
  ASSERT_EQ(fs_.use_count(), 1);  // needed for test to work
  std::weak_ptr<S3FileSystem> weak_fs(fs_);
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("bucket/somefile"));
  fs_.reset();
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(10));
  AssertBufferEqual(*buf, "some data");
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(weak_fs.expired());
}

TEST_F(TestS3FS, OpenInputStreamMetadata) {
  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<const KeyValueMetadata> metadata;

  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("bucket/somefile"));
  ASSERT_FINISHES_OK_AND_ASSIGN(metadata, stream->ReadMetadataAsync());

  std::vector<std::pair<std::string, std::string>> expected_kv{
      {"Content-Length", "9"}, {"Content-Type", "x-arrow/test"}};
  ASSERT_NE(metadata, nullptr);
  ASSERT_THAT(metadata->sorted_pairs(), testing::IsSupersetOf(expected_kv));
}

TEST_F(TestS3FS, OpenInputFile) {
  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buf;

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->OpenInputFile("nonexistent-bucket/somefile"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("bucket/zzzt"));

  // URI
  ASSERT_RAISES(Invalid, fs_->OpenInputStream("s3:bucket/somefile"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(file, fs_->OpenInputFile("bucket/somefile"));
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_ASSIGN(buf, file->Read(4));
  AssertBufferEqual(*buf, "some");
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_EQ(4, file->Tell());

  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(2, 5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_EQ(4, file->Tell());
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(5, 20));
  AssertBufferEqual(*buf, "data");
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(9, 20));
  AssertBufferEqual(*buf, "");

  char result[10];
  ASSERT_OK_AND_EQ(5, file->ReadAt(2, 5, &result));
  ASSERT_OK_AND_EQ(4, file->ReadAt(5, 20, &result));
  ASSERT_OK_AND_EQ(0, file->ReadAt(9, 0, &result));

  // Reading past end of file
  ASSERT_RAISES(IOError, file->ReadAt(10, 20));

  ASSERT_OK(file->Seek(5));
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "da");
  ASSERT_OK(file->Seek(9));
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "");
  // Seeking past end of file
  ASSERT_RAISES(IOError, file->Seek(10));
}

TEST_F(TestS3FS, OpenOutputStreamBackgroundWrites) { TestOpenOutputStream(); }

TEST_F(TestS3FS, OpenOutputStreamSyncWrites) {
  options_.background_writes = false;
  MakeFileSystem();
  TestOpenOutputStream();
}

TEST_F(TestS3FS, OpenOutputStreamAbortBackgroundWrites) { TestOpenOutputStreamAbort(); }

TEST_F(TestS3FS, OpenOutputStreamAbortSyncWrites) {
  options_.background_writes = false;
  MakeFileSystem();
  TestOpenOutputStreamAbort();
}

TEST_F(TestS3FS, OpenOutputStreamDestructorBackgroundWrites) {
  TestOpenOutputStreamDestructor();
}

TEST_F(TestS3FS, OpenOutputStreamDestructorSyncWrite) {
  options_.background_writes = false;
  MakeFileSystem();
  TestOpenOutputStreamDestructor();
}

TEST_F(TestS3FS, OpenOutputStreamMetadata) {
  std::shared_ptr<io::OutputStream> stream;

  // Create new file with no explicit or default metadata
  // The Content-Type will still be set
  auto empty_metadata = KeyValueMetadata::Make({}, {});
  auto implicit_metadata =
      KeyValueMetadata::Make({"Content-Type"}, {"application/octet-stream"});
  AssertMetadataRoundtrip("bucket/mdfile0", empty_metadata,
                          testing::IsSupersetOf(implicit_metadata->sorted_pairs()));

  // Create new file with explicit metadata
  auto metadata = KeyValueMetadata::Make({"Content-Type", "Expires"},
                                         {"x-arrow/test6", "2016-02-05T20:08:35Z"});
  AssertMetadataRoundtrip("bucket/mdfile1", metadata,
                          testing::IsSupersetOf(metadata->sorted_pairs()));

  // Create new file with valid canned ACL
  // XXX: no easy way of testing the ACL actually gets set
  metadata = KeyValueMetadata::Make({"ACL"}, {"authenticated-read"});
  AssertMetadataRoundtrip("bucket/mdfile2", metadata, testing::_);

  // Create new file with default metadata
  auto default_metadata = KeyValueMetadata::Make({"Content-Type", "Content-Language"},
                                                 {"image/png", "fr_FR"});
  options_.default_metadata = default_metadata;
  MakeFileSystem();
  // (null, then empty metadata argument)
  AssertMetadataRoundtrip("bucket/mdfile3", nullptr,
                          testing::IsSupersetOf(default_metadata->sorted_pairs()));
  AssertMetadataRoundtrip("bucket/mdfile4", KeyValueMetadata::Make({}, {}),
                          testing::IsSupersetOf(default_metadata->sorted_pairs()));

  // Create new file with explicit metadata replacing default metadata
  metadata = KeyValueMetadata::Make({"Content-Type"}, {"x-arrow/test6"});
  AssertMetadataRoundtrip("bucket/mdfile5", metadata,
                          testing::IsSupersetOf(metadata->sorted_pairs()));
}

TEST_F(TestS3FS, FileSystemFromUri) {
  std::stringstream ss;
  ss << "s3://" << minio_->access_key() << ":" << minio_->secret_key()
     << "@bucket/somedir/subdir/subfile"
     << "?scheme=http&endpoint_override=" << UriEscape(minio_->connect_string());

  std::string path;
  ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFromUri(ss.str(), &path));
  ASSERT_EQ(path, "bucket/somedir/subdir/subfile");

  // Check the filesystem has the right connection parameters
  AssertFileInfo(fs.get(), path, FileType::File, 8);
}

TEST_F(TestS3FS, NoCreateDeleteBucket) {
  // Create a bucket to try deleting
  ASSERT_OK(fs_->CreateDir("test-no-delete"));

  options_.allow_bucket_creation = false;
  options_.allow_bucket_deletion = false;
  MakeFileSystem();

  auto maybe_create_dir = fs_->CreateDir("test-no-create");
  ASSERT_RAISES(IOError, maybe_create_dir);
  ASSERT_THAT(maybe_create_dir.message(),
              ::testing::HasSubstr("Bucket 'test-no-create' not found"));

  auto maybe_delete_dir = fs_->DeleteDir("test-no-delete");
  ASSERT_RAISES(IOError, maybe_delete_dir);
  ASSERT_THAT(maybe_delete_dir.message(),
              ::testing::HasSubstr("Would delete bucket 'test-no-delete'"));
}

// Simple retry strategy that records errors encountered and its emitted retry delays
class TestRetryStrategy : public S3RetryStrategy {
 public:
  bool ShouldRetry(const S3RetryStrategy::AWSErrorDetail& error,
                   int64_t attempted_retries) final {
    errors_encountered_.emplace_back(error);
    constexpr int64_t MAX_RETRIES = 2;
    return attempted_retries < MAX_RETRIES;
  }

  int64_t CalculateDelayBeforeNextRetry(const S3RetryStrategy::AWSErrorDetail& error,
                                        int64_t attempted_retries) final {
    int64_t delay = attempted_retries;
    retry_delays_.emplace_back(delay);
    return delay;
  }

  std::vector<S3RetryStrategy::AWSErrorDetail> GetErrorsEncountered() {
    return errors_encountered_;
  }
  std::vector<int64_t> GetRetryDelays() { return retry_delays_; }

 private:
  std::vector<S3RetryStrategy::AWSErrorDetail> errors_encountered_;
  std::vector<int64_t> retry_delays_;
};

TEST_F(TestS3FS, CustomRetryStrategy) {
  auto retry_strategy = std::make_shared<TestRetryStrategy>();
  options_.retry_strategy = retry_strategy;
  MakeFileSystem();
  // Attempt to open file that doesn't exist. Should hit TestRetryStrategy::ShouldRetry()
  // 3 times before bubbling back up here.
  ASSERT_RAISES(IOError, fs_->OpenInputStream("nonexistent-bucket/somefile"));
  ASSERT_EQ(retry_strategy->GetErrorsEncountered().size(), 3);
  for (const auto& error : retry_strategy->GetErrorsEncountered()) {
    ASSERT_EQ(static_cast<Aws::Client::CoreErrors>(error.error_type),
              Aws::Client::CoreErrors::RESOURCE_NOT_FOUND);
    ASSERT_EQ(error.message, "No response body.");
    ASSERT_EQ(error.exception_name, "");
    ASSERT_EQ(error.should_retry, false);
  }
  std::vector<int64_t> expected_retry_delays = {0, 1, 2};
  ASSERT_EQ(retry_strategy->GetRetryDelays(), expected_retry_delays);
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
      ASSERT_OK(OutcomeToStatus("CreateBucket", client_->CreateBucket(req)));
    }

    options_.ConfigureAccessKey(minio_->access_key(), minio_->secret_key());
    options_.scheme = "http";
    options_.endpoint_override = minio_->connect_string();
    options_.retry_strategy = std::make_shared<ShortRetryStrategy>();
    ASSERT_OK_AND_ASSIGN(s3fs_, S3FileSystem::Make(options_));
    fs_ = std::make_shared<SubTreeFileSystem>("s3fs-test-bucket", s3fs_);
  }

 protected:
  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  bool have_implicit_directories() const override { return true; }
  bool allow_write_file_over_dir() const override { return true; }
  bool allow_move_dir() const override { return false; }
  bool allow_append_to_file() const override { return false; }
  bool have_directory_mtimes() const override { return false; }
  bool have_flaky_directory_tree_deletion() const override {
#ifdef _WIN32
    // Recent Minio versions on Windows may not register deletion of all
    // directories in a tree when doing a bulk delete.
    return true;
#else
    return false;
#endif
  }
  bool have_file_metadata() const override { return true; }

  S3Options options_;
  std::shared_ptr<S3FileSystem> s3fs_;
  std::shared_ptr<FileSystem> fs_;
};

GENERIC_FS_TEST_FUNCTIONS(TestS3FSGeneric);

}  // namespace fs
}  // namespace arrow

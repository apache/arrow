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
#  ifdef GetMessage
#    undef GetMessage
#  endif
#  ifdef GetObject
#    undef GetObject
#  endif
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
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
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
#include "arrow/testing/matchers.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/range.h"
#include "arrow/util/string.h"

// TLS tests require the ability to set a custom CA file when initiating S3 client
// connections, which the AWS SDK currently only supports on Linux.
#if defined(__linux__)
#  define ENABLE_TLS_TESTS
#endif  // Linux

namespace arrow {
namespace fs {

using ::arrow::internal::checked_pointer_cast;
using ::arrow::internal::PlatformFilename;
using ::arrow::internal::ToChars;
using ::arrow::internal::Zip;
using ::arrow::util::UriEscape;

using ::arrow::fs::internal::CalculateSSECustomerKeyMD5;
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

::testing::Environment* minio_env_https =
    ::testing::AddGlobalTestEnvironment(new MinioTestEnvironment(/*enable_tls=*/true));

MinioTestEnvironment* GetMinioEnv(bool enable_tls) {
  if (enable_tls) {
    return ::arrow::internal::checked_cast<MinioTestEnvironment*>(minio_env_https);
  } else {
    return ::arrow::internal::checked_cast<MinioTestEnvironment*>(minio_env);
  }
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
  void SetUp() override {
#ifdef AWS_CPP_SDK_S3_PRIVATE_STATIC
    auto aws_log_level = Aws::Utils::Logging::LogLevel::Fatal;
    aws_options_.loggingOptions.logLevel = aws_log_level;
    aws_options_.loggingOptions.logger_create_fn = [&aws_log_level] {
      return std::make_shared<Aws::Utils::Logging::ConsoleLogSystem>(aws_log_level);
    };
    Aws::InitAPI(aws_options_);
#endif
  }

  void TearDown() override {
#ifdef AWS_CPP_SDK_S3_PRIVATE_STATIC
    Aws::ShutdownAPI(aws_options_);
#endif
  }

 private:
#ifdef AWS_CPP_SDK_S3_PRIVATE_STATIC
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
      ASSERT_OK(InitServerAndClient());
      connect_status = OutcomeToStatus("ListBuckets", client_->ListBuckets());
    } while (!connect_status.ok() && --retries > 0);
    ASSERT_OK(connect_status);
  }

  void TearDown() override {
    // Aws::S3::S3Client destruction relies on AWS SDK, so it must be
    // reset before Aws::ShutdownAPI
    client_.reset();
    client_config_.reset();

    AwsTestMixin::TearDown();
  }

 protected:
  Status InitServerAndClient() {
    ARROW_ASSIGN_OR_RAISE(minio_, GetMinioEnv(enable_tls_)->GetOneServer());
    client_config_.reset(new Aws::Client::ClientConfiguration());
    client_config_->endpointOverride = ToAwsString(minio_->connect_string());
    if (minio_->scheme() == "https") {
      client_config_->scheme = Aws::Http::Scheme::HTTPS;
      client_config_->caFile = ToAwsString(minio_->ca_file_path());
    } else {
      client_config_->scheme = Aws::Http::Scheme::HTTP;
    }
    client_config_->retryStrategy =
        std::make_shared<ConnectRetryStrategy>(kRetryInterval, kMaxRetryDuration);
    credentials_ = {ToAwsString(minio_->access_key()), ToAwsString(minio_->secret_key())};
    bool use_virtual_addressing = false;
    client_.reset(
        new Aws::S3::S3Client(credentials_, *client_config_,
                              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                              use_virtual_addressing));
    return Status::OK();
  }

  // How many times to try launching a server in a row before decreeing failure
  static constexpr int kNumServerRetries = 3;

  std::shared_ptr<MinioTestServer> minio_;
  std::unique_ptr<Aws::Client::ClientConfiguration> client_config_;
  Aws::Auth::AWSCredentials credentials_;
  std::unique_ptr<Aws::S3::S3Client> client_;
  // Use plain HTTP by default, as this allows us to listen on different loopback
  // addresses and thus minimize the risk of address reuse (HTTPS requires the
  // hostname to match the certificate's subject name, constraining us to a
  // single loopback address).
  bool enable_tls_ = false;
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
// Misc tests

class InternalsTest : public AwsTestMixin {};

TEST_F(InternalsTest, CalculateSSECustomerKeyMD5) {
  ASSERT_RAISES(Invalid, CalculateSSECustomerKeyMD5(""));  // invalid length
  ASSERT_RAISES(Invalid,
                CalculateSSECustomerKeyMD5(
                    "1234567890123456789012345678901234567890"));  // invalid length
  // valid case, with some non-ASCII character and a null byte in the sse_customer_key
  char sse_customer_key[32] = {};
  sse_customer_key[0] = '\x40';   // '@' character
  sse_customer_key[1] = '\0';     // null byte
  sse_customer_key[2] = '\xFF';   // non-ASCII
  sse_customer_key[31] = '\xFA';  // non-ASCII
  std::string sse_customer_key_string(sse_customer_key, sizeof(sse_customer_key));
  ASSERT_OK_AND_ASSIGN(auto md5, CalculateSSECustomerKeyMD5(sse_customer_key_string))
  ASSERT_EQ(md5, "97FTa6lj0hE7lshKdBy61g==");  // valid case
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
  ASSERT_EQ(options.smart_defaults, "standard");
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

  ASSERT_OK_AND_ASSIGN(
      options, S3Options::FromUri(
                   "s3://?allow_bucket_creation=true&smart_defaults=legacy", &path));
  ASSERT_TRUE(options.allow_bucket_creation);
  ASSERT_EQ(options.smart_defaults, "legacy");

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
  ASSERT_EQ(options.tls_verify_certificates, true);

  // Explicit tls related configuration
  ASSERT_OK_AND_ASSIGN(
      options,
      S3Options::FromUri("s3://mybucket/foo/bar/?tls_ca_dir_path=/test&tls_ca_file_path=/"
                         "test/test.pem&tls_verify_certificates=false",
                         &path));
  ASSERT_EQ(options.tls_ca_dir_path, "/test");
  ASSERT_EQ(options.tls_ca_file_path, "/test/test.pem");
  ASSERT_EQ(options.tls_verify_certificates, false);

  // Missing bucket name
  ASSERT_RAISES(Invalid, S3Options::FromUri("s3:///foo/bar/", &path));

  // Invalid option
  ASSERT_RAISES(Invalid, S3Options::FromUri("s3://mybucket/?xxx=zzz", &path));

  // Endpoint from environment variable
  {
    EnvVarGuard endpoint_guard("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:9000");
    ASSERT_OK_AND_ASSIGN(options, S3Options::FromUri("s3://mybucket/", &path));
    ASSERT_EQ(options.endpoint_override, "http://127.0.0.1:9000");
  }
  {
    EnvVarGuard endpoint_guard("AWS_ENDPOINT_URL", "http://127.0.0.1:9000");
    ASSERT_OK_AND_ASSIGN(options, S3Options::FromUri("s3://mybucket/", &path));
    ASSERT_EQ(options.endpoint_override, "http://127.0.0.1:9000");
  }
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
  auto maybe_region = ResolveS3BucketRegion("ursa-labs-nonexistent-bucket");
  ASSERT_RAISES(IOError, maybe_region);
  ASSERT_THAT(maybe_region.status().message(),
              ::testing::HasSubstr("Bucket 'ursa-labs-nonexistent-bucket' not found"));
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
    if (enable_tls_) {
      options_.tls_ca_file_path = minio_->ca_file_path();
    }
    MakeFileSystem();
    // Set up test bucket
    {
      Aws::S3::Model::CreateBucketRequest req;
      req.SetBucket(ToAwsString("bucket"));
      ASSERT_OK(OutcomeToStatus("CreateBucket", client_->CreateBucket(req)));
      req.SetBucket(ToAwsString("empty-bucket"));
      ASSERT_OK(OutcomeToStatus("CreateBucket", client_->CreateBucket(req)));
    }

    ASSERT_OK(PopulateTestBucket());
  }

  void TearDown() override {
    // Aws::S3::S3Client destruction relies on AWS SDK, so it must be
    // reset before Aws::ShutdownAPI
    fs_.reset();
    S3TestMixin::TearDown();
  }

  Status PopulateTestBucket() {
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(ToAwsString("bucket"));
    req.SetKey(ToAwsString("emptydir/"));
    RETURN_NOT_OK(OutcomeToStatus("PutObject", client_->PutObject(req)));
    // NOTE: no need to create intermediate "directories" somedir/ and
    // somedir/subdir/
    req.SetKey(ToAwsString("somedir/subdir/subfile"));
    req.SetBody(std::make_shared<std::stringstream>("sub data"));
    RETURN_NOT_OK(OutcomeToStatus("PutObject", client_->PutObject(req)));
    req.SetKey(ToAwsString("somefile"));
    req.SetBody(std::make_shared<std::stringstream>("some data"));
    req.SetContentType("x-arrow/test");
    RETURN_NOT_OK(OutcomeToStatus("PutObject", client_->PutObject(req)));
    req.SetKey(ToAwsString("otherdir/1/2/3/otherfile"));
    req.SetBody(std::make_shared<std::stringstream>("other data"));
    RETURN_NOT_OK(OutcomeToStatus("PutObject", client_->PutObject(req)));

    return Status::OK();
  }

  Status RestoreTestBucket() {
    // First empty the test bucket, and then re-upload initial test files.

    Aws::S3::Model::Delete delete_object;
    {
      // Mostly taken from
      // https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/cpp/example_code/s3/list_objects.cpp
      Aws::S3::Model::ListObjectsV2Request req;
      req.SetBucket(Aws::String{"bucket"});

      Aws::String continuation_token;
      do {
        if (!continuation_token.empty()) {
          req.SetContinuationToken(continuation_token);
        }

        auto outcome = client_->ListObjectsV2(req);

        if (!outcome.IsSuccess()) {
          return OutcomeToStatus("ListObjectsV2", outcome);
        } else {
          Aws::Vector<Aws::S3::Model::Object> objects = outcome.GetResult().GetContents();
          for (const auto& object : objects) {
            delete_object.AddObjects(
                Aws::S3::Model::ObjectIdentifier().WithKey(object.GetKey()));
          }

          continuation_token = outcome.GetResult().GetNextContinuationToken();
        }
      } while (!continuation_token.empty());
    }

    {
      Aws::S3::Model::DeleteObjectsRequest req;

      req.SetDelete(std::move(delete_object));
      req.SetBucket(Aws::String{"bucket"});

      RETURN_NOT_OK(OutcomeToStatus("DeleteObjects", client_->DeleteObjects(req)));
    }

    return PopulateTestBucket();
  }

  Result<std::shared_ptr<S3FileSystem>> MakeNewFileSystem(
      io::IOContext io_context = io::default_io_context()) {
    options_.ConfigureAccessKey(minio_->access_key(), minio_->secret_key());
    options_.scheme = minio_->scheme();
    options_.endpoint_override = minio_->connect_string();
    if (!options_.retry_strategy) {
      options_.retry_strategy = std::make_shared<ShortRetryStrategy>();
    }
    return S3FileSystem::Make(options_, io_context);
  }

  void MakeFileSystem() { ASSERT_OK_AND_ASSIGN(fs_, MakeNewFileSystem()); }

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

  void TestOpenOutputStream(bool allow_delayed_open) {
    std::shared_ptr<io::OutputStream> stream;

    if (allow_delayed_open) {
      ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("nonexistent-bucket/somefile"));
      ASSERT_RAISES(IOError, stream->Close());
    } else {
      // Nonexistent
      ASSERT_RAISES(IOError, fs_->OpenOutputStream("nonexistent-bucket/somefile"));
    }

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

  void TestOpenOutputStreamCloseAsyncDestructor() {
    std::shared_ptr<io::OutputStream> stream;
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/somefile"));
    ASSERT_OK(stream->Write("new data"));
    // Destructor implicitly closes stream and completes the multipart upload.
    // GH-37670: Testing it doesn't matter whether flush is triggered asynchronously
    // after CloseAsync or synchronously after stream.reset() since we're just
    // checking that `closeAsyncFut` keeps the stream alive until completion
    // rather than segfaulting on a dangling stream
    auto close_fut = stream->CloseAsync();
    stream.reset();
    ASSERT_OK(close_fut.MoveResult());
    AssertObjectContents(client_.get(), "bucket", "somefile", "new data");
  }

  void TestOpenOutputStreamCloseAsyncFutureDeadlock() {
    // This is inspired by GH-41862, though it fails to reproduce the actual
    // issue reported there (actual preconditions might be required).
    // Here we lose our reference to an output stream from its CloseAsync callback.
    // This should not deadlock.
    std::shared_ptr<io::OutputStream> stream;
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/somefile"));
    ASSERT_OK(stream->Write("new data"));
    auto close_fut = stream->CloseAsync();
    close_fut.AddCallback([stream = std::move(stream)](Status st) mutable {
      // Trigger stream destruction from callback
      stream.reset();
    });
    ASSERT_OK(close_fut.MoveResult());
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
  AssertFileInfo(fs_.get(), "nonexistent-bucket/somed", FileType::NotFound);

  // Trailing slashes
  AssertFileInfo(fs_.get(), "bucket/emptydir/", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/somefile/", FileType::File, 9);
  AssertFileInfo(fs_.get(), "bucket/emptyd/", FileType::NotFound);
  AssertFileInfo(fs_.get(), "nonexistent-bucket/somed/", FileType::NotFound);

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

TEST_F(TestS3FS, GetFileInfoGeneratorStress) {
  // This test is slow because it needs to create a bunch of seed files.  However, it is
  // the only test that stresses listing and deleting when there are more than 1000
  // files and paging is required.
  constexpr int32_t kNumDirs = 4;
  constexpr int32_t kNumFilesPerDir = 512;
  FileInfoVector expected_infos;

  ASSERT_OK(fs_->CreateDir("stress"));
  for (int32_t i = 0; i < kNumDirs; i++) {
    const std::string dir_path = "stress/" + ToChars(i);
    ASSERT_OK(fs_->CreateDir(dir_path));
    expected_infos.emplace_back(dir_path, FileType::Directory);

    std::vector<Future<>> tasks;
    for (int32_t j = 0; j < kNumFilesPerDir; j++) {
      // Create the files in parallel in hopes of speeding up this process as much as
      // possible
      const std::string file_name = ToChars(j);
      const std::string file_path = dir_path + "/" + file_name;
      expected_infos.emplace_back(file_path, FileType::File);
      ASSERT_OK_AND_ASSIGN(Future<> task,
                           ::arrow::internal::GetCpuThreadPool()->Submit(
                               [fs = fs_, file_name, file_path]() -> Status {
                                 ARROW_ASSIGN_OR_RAISE(
                                     std::shared_ptr<io::OutputStream> out_str,
                                     fs->OpenOutputStream(file_path));
                                 ARROW_RETURN_NOT_OK(out_str->Write(file_name));
                                 return out_str->Close();
                               }));
      tasks.push_back(std::move(task));
    }
    ASSERT_FINISHES_OK(AllFinished(tasks));
  }
  SortInfos(&expected_infos);

  FileSelector select;
  FileInfoVector infos;
  select.base_dir = "stress";
  select.recursive = true;

  // 32 is pretty fast, listing is much faster than the create step above
  constexpr int32_t kNumTasks = 32;
  for (int i = 0; i < kNumTasks; i++) {
    CollectFileInfoGenerator(fs_->GetFileInfoGenerator(select), &infos);
    SortInfos(&infos);
    // One info for each directory and one info for each file
    ASSERT_EQ(infos.size(), expected_infos.size());
    for (const auto&& [info, expected] : Zip(infos, expected_infos)) {
      AssertFileInfo(info, expected.path(), expected.type());
    }
  }

  ASSERT_OK(fs_->DeleteDirContents("stress"));

  CollectFileInfoGenerator(fs_->GetFileInfoGenerator(select), &infos);
  ASSERT_EQ(infos.size(), 0);
}

TEST_F(TestS3FS, GetFileInfoGeneratorCancelled) {
  FileSelector select;
  FileInfoVector infos;
  select.base_dir = "bucket";
  select.recursive = true;

  StopSource stop_source;
  io::IOContext cancellable_context(stop_source.token());
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<S3FileSystem> cancellable_fs,
                       MakeNewFileSystem(cancellable_context));
  stop_source.RequestStop();
  FileInfoGenerator generator = cancellable_fs->GetFileInfoGenerator(select);
  auto file_infos = CollectAsyncGenerator(std::move(generator));
  ASSERT_FINISHES_AND_RAISES(Cancelled, file_infos);
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
  ASSERT_OK(fs_->CreateDir("bucket/newdir", /*recursive=*/false));
  AssertFileInfo(fs_.get(), "bucket/newdir", FileType::Directory);

  // Non-recursive, but parent does not exist
  ASSERT_RAISES(IOError,
                fs_->CreateDir("bucket/newdir/newsub/newsubsub", /*recursive=*/false));

  // New "directory", recursive
  ASSERT_OK(fs_->CreateDir("bucket/newdir/newsub/newsubsub", /*recursive=*/true));
  AssertFileInfo(fs_.get(), "bucket/newdir/newsub", FileType::Directory);
  AssertFileInfo(fs_.get(), "bucket/newdir/newsub/newsubsub", FileType::Directory);

  // Existing "file", should fail
  ASSERT_RAISES(IOError, fs_->CreateDir("bucket/somefile", /*recursive=*/false));
  ASSERT_RAISES(IOError, fs_->CreateDir("bucket/somefile", /*recursive=*/true));

  // URI
  ASSERT_RAISES(Invalid, fs_->CreateDir("s3:bucket/newdir2"));

  // Extraneous slashes
  ASSERT_RAISES(Invalid, fs_->CreateDir("bucket//somedir"));
  ASSERT_RAISES(Invalid, fs_->CreateDir("bucket/somedir//newdir"));

  // check existing before creation
  options_.check_directory_existence_before_creation = true;
  MakeFileSystem();

  // Existing "file", should fail again
  ASSERT_RAISES(IOError, fs_->CreateDir("bucket/somefile", /*recursive=*/false));
  ASSERT_RAISES(IOError, fs_->CreateDir("bucket/somefile", /*recursive=*/true));

  // New "directory" again
  AssertFileInfo(fs_.get(), "bucket/checknewdir", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("bucket/checknewdir"));
  AssertFileInfo(fs_.get(), "bucket/checknewdir", FileType::Directory);

  ASSERT_RAISES(IOError, fs_->CreateDir("bucket/checknewdir/newsub/newsubsub/newsubsub/",
                                        /*recursive=*/false));

  // New "directory" again, recursive
  ASSERT_OK(fs_->CreateDir("bucket/checknewdir/newsub/newsubsub", /*recursive=*/true));
  AssertFileInfo(fs_.get(), "bucket/checknewdir/newsub", FileType::Directory);
  AssertFileInfo(fs_.get(), "bucket/checknewdir/newsub/newsubsub", FileType::Directory);
  AssertFileInfo(fs_.get(), "bucket/checknewdir/newsub/newsubsub/newsubsub",
                 FileType::NotFound);

  // Try creation with the same name
  ASSERT_OK(fs_->CreateDir("bucket/checknewdir/newsub/newsubsub/newsubsub/",
                           /*recursive=*/true));
  AssertFileInfo(fs_.get(), "bucket/checknewdir/newsub", FileType::Directory);
  AssertFileInfo(fs_.get(), "bucket/checknewdir/newsub/newsubsub", FileType::Directory);
  AssertFileInfo(fs_.get(), "bucket/checknewdir/newsub/newsubsub/newsubsub",
                 FileType::Directory);
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

  // Extraneous slashes
  ASSERT_RAISES(Invalid, fs_->DeleteDir("bucket//newdir"));
  ASSERT_RAISES(Invalid, fs_->DeleteDir("bucket/newdir//newsub"));
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
  ASSERT_RAISES(IOError, fs_->Move("bucket/nonexistent", "bucket/newfile2"));
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

// Minio only allows Server Side Encryption on HTTPS client connections.
#ifdef ENABLE_TLS_TESTS
class TestS3FSHTTPS : public TestS3FS {
 public:
  void SetUp() override {
    enable_tls_ = true;
    TestS3FS::SetUp();
  }
};

TEST_F(TestS3FSHTTPS, SSECustomerKeyMatch) {
  // normal write/read with correct SSE-C key
  std::shared_ptr<io::OutputStream> stream;
  options_.sse_customer_key = "12345678123456781234567812345678";
  for (const auto& allow_delayed_open : {false, true}) {
    ARROW_SCOPED_TRACE("allow_delayed_open = ", allow_delayed_open);
    options_.allow_delayed_open = allow_delayed_open;
    MakeFileSystem();
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile_with_sse_c"));
    ASSERT_OK(stream->Write("some"));
    ASSERT_OK(stream->Close());
    ASSERT_OK_AND_ASSIGN(auto file, fs_->OpenInputFile("bucket/newfile_with_sse_c"));
    ASSERT_OK_AND_ASSIGN(auto buf, file->Read(5));
    AssertBufferEqual(*buf, "some");
    ASSERT_OK(RestoreTestBucket());
  }
}

TEST_F(TestS3FSHTTPS, SSECustomerKeyMismatch) {
  std::shared_ptr<io::OutputStream> stream;
  for (const auto& allow_delayed_open : {false, true}) {
    ARROW_SCOPED_TRACE("allow_delayed_open = ", allow_delayed_open);
    options_.allow_delayed_open = allow_delayed_open;
    options_.sse_customer_key = "12345678123456781234567812345678";
    MakeFileSystem();
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile_with_sse_c"));
    ASSERT_OK(stream->Write("some"));
    ASSERT_OK(stream->Close());
    options_.sse_customer_key = "87654321876543218765432187654321";
    MakeFileSystem();
    ASSERT_RAISES(IOError, fs_->OpenInputFile("bucket/newfile_with_sse_c"));
    ASSERT_OK(RestoreTestBucket());
  }
}

TEST_F(TestS3FSHTTPS, SSECustomerKeyMissing) {
  std::shared_ptr<io::OutputStream> stream;
  for (const auto& allow_delayed_open : {false, true}) {
    ARROW_SCOPED_TRACE("allow_delayed_open = ", allow_delayed_open);
    options_.allow_delayed_open = allow_delayed_open;
    options_.sse_customer_key = "12345678123456781234567812345678";
    MakeFileSystem();
    ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("bucket/newfile_with_sse_c"));
    ASSERT_OK(stream->Write("some"));
    ASSERT_OK(stream->Close());

    options_.sse_customer_key = {};
    MakeFileSystem();
    ASSERT_RAISES(IOError, fs_->OpenInputFile("bucket/newfile_with_sse_c"));
    ASSERT_OK(RestoreTestBucket());
  }
}

TEST_F(TestS3FSHTTPS, SSECustomerKeyCopyFile) {
  ASSERT_OK_AND_ASSIGN(auto stream, fs_->OpenOutputStream("bucket/newfile_with_sse_c"));
  ASSERT_OK(stream->Write("some"));
  ASSERT_OK(stream->Close());
  ASSERT_OK(fs_->CopyFile("bucket/newfile_with_sse_c", "bucket/copied_with_sse_c"));

  ASSERT_OK_AND_ASSIGN(auto file, fs_->OpenInputFile("bucket/copied_with_sse_c"));
  ASSERT_OK_AND_ASSIGN(auto buf, file->Read(5));
  AssertBufferEqual(*buf, "some");
  ASSERT_OK(RestoreTestBucket());
}
#endif  // ENABLE_TLS_TESTS

struct S3OptionsTestParameters {
  bool background_writes{false};
  bool allow_delayed_open{false};

  void ApplyToS3Options(S3Options* options) const {
    options->background_writes = background_writes;
    options->allow_delayed_open = allow_delayed_open;
  }

  static std::vector<S3OptionsTestParameters> GetCartesianProduct() {
    return {
        S3OptionsTestParameters{true, false},
        S3OptionsTestParameters{false, false},
        S3OptionsTestParameters{true, true},
        S3OptionsTestParameters{false, true},
    };
  }

  std::string ToString() const {
    return std::string("background_writes = ") + (background_writes ? "true" : "false") +
           ", allow_delayed_open = " + (allow_delayed_open ? "true" : "false");
  }
};

TEST_F(TestS3FS, OpenOutputStream) {
  for (const auto& combination : S3OptionsTestParameters::GetCartesianProduct()) {
    ARROW_SCOPED_TRACE(combination.ToString());

    combination.ApplyToS3Options(&options_);
    MakeFileSystem();
    TestOpenOutputStream(combination.allow_delayed_open);
    ASSERT_OK(RestoreTestBucket());
  }
}

TEST_F(TestS3FS, OpenOutputStreamAbort) {
  for (const auto& combination : S3OptionsTestParameters::GetCartesianProduct()) {
    ARROW_SCOPED_TRACE(combination.ToString());

    combination.ApplyToS3Options(&options_);
    MakeFileSystem();
    TestOpenOutputStreamAbort();
    ASSERT_OK(RestoreTestBucket());
  }
}

TEST_F(TestS3FS, OpenOutputStreamDestructor) {
  for (const auto& combination : S3OptionsTestParameters::GetCartesianProduct()) {
    ARROW_SCOPED_TRACE(combination.ToString());

    combination.ApplyToS3Options(&options_);
    MakeFileSystem();
    TestOpenOutputStreamDestructor();
    ASSERT_OK(RestoreTestBucket());
  }
}

TEST_F(TestS3FS, OpenOutputStreamAsync) {
  for (const auto& combination : S3OptionsTestParameters::GetCartesianProduct()) {
    ARROW_SCOPED_TRACE(combination.ToString());

    combination.ApplyToS3Options(&options_);
    MakeFileSystem();
    TestOpenOutputStreamCloseAsyncDestructor();
  }
}

TEST_F(TestS3FS, OpenOutputStreamCloseAsyncFutureDeadlockBackgroundWrites) {
  TestOpenOutputStreamCloseAsyncFutureDeadlock();
  ASSERT_OK(RestoreTestBucket());
}

TEST_F(TestS3FS, OpenOutputStreamCloseAsyncFutureDeadlockSyncWrite) {
  options_.background_writes = false;
  MakeFileSystem();
  TestOpenOutputStreamCloseAsyncFutureDeadlock();
  ASSERT_OK(RestoreTestBucket());
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
     << "?scheme=" << minio_->scheme()
     << "&endpoint_override=" << UriEscape(minio_->connect_string());

  std::string path;
  ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFromUri(ss.str(), &path));
  ASSERT_EQ(path, "bucket/somedir/subdir/subfile");

  ASSERT_OK_AND_ASSIGN(path, fs->PathFromUri(ss.str()));
  ASSERT_EQ(path, "bucket/somedir/subdir/subfile");

  // Incorrect scheme
  ASSERT_THAT(fs->PathFromUri("file:///@bucket/somedir/subdir/subfile"),
              Raises(StatusCode::Invalid,
                     testing::HasSubstr("expected a URI with one of the schemes (s3)")));

  // Not a URI
  ASSERT_THAT(fs->PathFromUri("/@bucket/somedir/subdir/subfile"),
              Raises(StatusCode::Invalid, testing::HasSubstr("Expected a URI")));

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
  // Attempt to open file that doesn't exist. Should hit
  // TestRetryStrategy::ShouldRetry() 3 times before bubbling back up here.
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
    options_.scheme = minio_->scheme();
    options_.endpoint_override = minio_->connect_string();
    options_.retry_strategy = std::make_shared<ShortRetryStrategy>();
    ASSERT_OK_AND_ASSIGN(s3fs_, S3FileSystem::Make(options_));
    fs_ = std::make_shared<SubTreeFileSystem>("s3fs-test-bucket", s3fs_);
  }

  void TearDown() override {
    // Aws::S3::S3Client destruction relies on AWS SDK, so it must be
    // reset before Aws::ShutdownAPI
    s3fs_.reset();
    fs_.reset();
    S3TestMixin::TearDown();
  }

 protected:
  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  bool have_implicit_directories() const override { return true; }
  bool allow_write_file_over_dir() const override { return true; }
  bool allow_write_implicit_dir_over_file() const override {
    // Recent Minio versions allow this
    return true;
  }
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

////////////////////////////////////////////////////////////////////////////
// S3GlobalOptions::Defaults tests

TEST(S3GlobalOptions, DefaultsLogLevel) {
  // Verify we get the default value of Fatal
  ASSERT_EQ(S3LogLevel::Fatal, arrow::fs::S3GlobalOptions::Defaults().log_level);

  // Verify we get the value specified by env var and not the default
  {
    EnvVarGuard log_level_guard("ARROW_S3_LOG_LEVEL", "ERROR");
    ASSERT_EQ(S3LogLevel::Error, arrow::fs::S3GlobalOptions::Defaults().log_level);
  }

  // Verify we trim and case-insensitively compare the environment variable's value
  {
    EnvVarGuard log_level_guard("ARROW_S3_LOG_LEVEL", " eRrOr ");
    ASSERT_EQ(S3LogLevel::Error, arrow::fs::S3GlobalOptions::Defaults().log_level);
  }

  // Verify we get the default value of Fatal if our env var is invalid
  {
    EnvVarGuard log_level_guard("ARROW_S3_LOG_LEVEL", "invalid");
    ASSERT_EQ(S3LogLevel::Fatal, arrow::fs::S3GlobalOptions::Defaults().log_level);
  }
}

}  // namespace fs
}  // namespace arrow

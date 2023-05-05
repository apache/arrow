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

#include <algorithm>  // Missing include in boost/process

#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
// This boost/asio/io_context.hpp include is needless for no MinGW
// build.
//
// This is for including boost/asio/detail/socket_types.hpp before any
// "#include <windows.h>". boost/asio/detail/socket_types.hpp doesn't
// work if windows.h is already included. boost/process.h ->
// boost/process/args.hpp -> boost/process/detail/basic_cmd.hpp
// includes windows.h. boost/process/args.hpp is included before
// boost/process/async.h that includes
// boost/asio/detail/socket_types.hpp implicitly is included.
#include <boost/asio/io_context.hpp>
// We need BOOST_USE_WINDOWS_H definition with MinGW when we use
// boost/process.hpp. See BOOST_USE_WINDOWS_H=1 in
// cpp/cmake_modules/ThirdpartyToolchain.cmake for details.
#include <boost/process.hpp>
#include <boost/thread.hpp>

#include "arrow/filesystem/gcsfs.h"

#include <absl/time/time.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <google/cloud/credentials.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/options.h>
#include <gtest/gtest.h>

#include <array>
#include <random>
#include <string>
#include <thread>

#include "arrow/filesystem/gcsfs_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/util.h"
#include "arrow/util/future.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace fs {

namespace {

namespace bp = boost::process;
namespace gc = google::cloud;
namespace gcs = google::cloud::storage;

using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Not;
using ::testing::NotNull;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using ::testing::UnorderedElementsAreArray;

auto const* kLoremIpsum = R"""(
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
culpa qui officia deserunt mollit anim id est laborum.
)""";

class GcsTestbench : public ::testing::Environment {
 public:
  GcsTestbench() {
    port_ = std::to_string(GetListenPort());
    std::vector<std::string> names{"python3", "python"};
    // If the build script or application developer provides a value in the PYTHON
    // environment variable, then just use that.
    if (const auto* env = std::getenv("PYTHON")) {
      names = {env};
    }
    auto error = std::string(
        "Could not start GCS emulator."
        " Used the following list of python interpreter names:");
    for (const auto& interpreter : names) {
      auto exe_path = bp::search_path(interpreter);
      error += " " + interpreter;
      if (exe_path.empty()) {
        error += " (exe not found)";
        continue;
      }

      bp::ipstream output;
      server_process_ = bp::child(exe_path, "-m", "testbench", "--port", port_, group_,
                                  bp::std_err > output);
      // Wait for message: "* Restarting with"
      auto testbench_is_running = [&output, this](bp::child& process) {
        std::string line;
        std::chrono::time_point<std::chrono::steady_clock> end =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (server_process_.valid() && server_process_.running() &&
               std::chrono::steady_clock::now() < end) {
          if (output.peek() && std::getline(output, line)) {
            std::cerr << line << std::endl;
            if (line.find("* Restarting with") != std::string::npos) return true;
          } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
          }
        }
        return false;
      };

      if (testbench_is_running(server_process_)) break;
      error += " (failed to start)";
      server_process_.terminate();
      server_process_.wait();
    }
    if (server_process_.valid() && server_process_.valid()) return;
    error_ = std::move(error);
  }

  bool running() { return server_process_.running(); }

  ~GcsTestbench() override {
    // Brutal shutdown, kill the full process group because the GCS testbench may launch
    // additional children.
    group_.terminate();
    if (server_process_.valid()) {
      server_process_.wait();
    }
  }

  const std::string& port() const { return port_; }
  const std::string& error() const { return error_; }

 private:
  std::string port_;
  bp::child server_process_;
  bp::group group_;
  std::string error_;
};

GcsTestbench* Testbench() {
  static auto* const environment = [] { return new GcsTestbench; }();
  return environment;
}

auto* testbench_env = ::testing::AddGlobalTestEnvironment(Testbench());

class GcsIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_THAT(Testbench(), NotNull());
    ASSERT_EQ(Testbench()->error(), "");
    ASSERT_TRUE(Testbench()->running());

    // Initialize a PRNG with a small amount of entropy.
    generator_ = std::mt19937_64(std::random_device()());
    bucket_name_ = RandomChars(32);

    // Create a bucket and a small file in the testbench. This makes it easier to
    // bootstrap GcsFileSystem and its tests.
    auto client = gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>("http://127.0.0.1:" + Testbench()->port())
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials())
            .set<gcs::TransferStallTimeoutOption>(std::chrono::seconds(5))
            .set<gcs::RetryPolicyOption>(
                gcs::LimitedTimeRetryPolicy(std::chrono::seconds(45)).clone()));

    google::cloud::StatusOr<gcs::BucketMetadata> bucket = client.CreateBucketForProject(
        PreexistingBucketName(), "ignored-by-testbench", gcs::BucketMetadata{});
    ASSERT_TRUE(bucket.ok()) << "Failed to create bucket <" << PreexistingBucketName()
                             << ">, status=" << bucket.status();

    google::cloud::StatusOr<gcs::ObjectMetadata> object = client.InsertObject(
        PreexistingBucketName(), PreexistingObjectName(), kLoremIpsum);
    ASSERT_TRUE(object.ok()) << "Failed to create object <" << PreexistingObjectName()
                             << ">, status=" << object.status();
  }

  std::string PreexistingBucketName() const { return bucket_name_; }

  std::string PreexistingBucketPath() const { return PreexistingBucketName() + '/'; }

  static std::string PreexistingObjectName() { return "test-object-name"; }

  std::string PreexistingObjectPath() const {
    return PreexistingBucketPath() + PreexistingObjectName();
  }

  std::string NotFoundObjectPath() { return PreexistingBucketPath() + "not-found"; }

  GcsOptions TestGcsOptions() {
    auto options = GcsOptions::Anonymous();
    options.endpoint_override = "127.0.0.1:" + Testbench()->port();
    options.retry_limit_seconds = 60;
    return options;
  }

  gcs::Client GcsClient() {
    return gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>("http://127.0.0.1:" + Testbench()->port())
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials()));
  }

  std::string RandomLine(int lineno, std::size_t width) {
    auto line = std::to_string(lineno) + ":    ";
    line += RandomChars(width - line.size() - 1);
    line += '\n';
    return line;
  }

  std::size_t RandomIndex(std::size_t end) {
    return std::uniform_int_distribution<std::size_t>(0, end - 1)(generator_);
  }

  std::string RandomBucketName() { return RandomChars(32); }

  std::string RandomFolderName() { return RandomChars(32); }

  struct Hierarchy {
    std::string base_dir;
    std::vector<FileInfo> contents;
  };

  Result<Hierarchy> CreateHierarchy(std::shared_ptr<arrow::fs::FileSystem> fs) {
    const char* const kTestFolders[] = {
        "b",
        "b/0",
        "b/0/0",
        "b/1",
        "b/2",
        // Create some additional folders that should not appear in any listing of b/
        "aa",
        "ba",
        "c",
    };
    constexpr auto kFilesPerFolder = 2;
    auto base_dir = internal::ConcatAbstractPath(PreexistingBucketPath(), "b");
    auto result = Hierarchy{base_dir, {}};
    for (auto const* f : kTestFolders) {
      const auto folder = internal::ConcatAbstractPath(PreexistingBucketPath(), f);
      RETURN_NOT_OK(fs->CreateDir(folder, true));
      result.contents.push_back(arrow::fs::Dir(folder));
      for (int i = 0; i != kFilesPerFolder; ++i) {
        const auto filename =
            internal::ConcatAbstractPath(folder, "test-file-" + std::to_string(i));
        CreateFile(fs.get(), filename, filename);
        ARROW_ASSIGN_OR_RAISE(auto file_info, fs->GetFileInfo(filename));
        result.contents.push_back(std::move(file_info));
      }
    }
    return result;
  }

  // Directories must appear without a trailing slash in the results.
  std::vector<arrow::fs::FileInfo> static CleanupDirectoryNames(
      std::vector<arrow::fs::FileInfo> expected) {
    std::transform(expected.begin(), expected.end(), expected.begin(),
                   [](FileInfo const& info) {
                     if (!info.IsDirectory()) return info;
                     return Dir(std::string(internal::RemoveTrailingSlash(info.path())));
                   });
    return expected;
  }

 private:
  std::string RandomChars(std::size_t count) {
    auto const fillers = std::string("abcdefghijlkmnopqrstuvwxyz0123456789");
    std::uniform_int_distribution<std::size_t> d(0, fillers.size() - 1);
    std::string s;
    std::generate_n(std::back_inserter(s), count, [&] { return fillers[d(generator_)]; });
    return s;
  }

  std::mt19937_64 generator_;
  std::string bucket_name_;
};

class TestGCSFSGeneric : public GcsIntegrationTest, public GenericFileSystemTest {
 public:
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(GcsIntegrationTest::SetUp());
    auto bucket_name = RandomBucketName();
    gcs_fs_ = GcsFileSystem::Make(TestGcsOptions());
    ASSERT_OK(gcs_fs_->CreateDir(bucket_name, true));
    fs_ = std::make_shared<SubTreeFileSystem>(bucket_name, gcs_fs_);
  }

 protected:
  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  bool have_implicit_directories() const override { return true; }
  bool allow_write_file_over_dir() const override { return true; }
  bool allow_read_dir_as_file() const override { return true; }
  bool allow_move_dir() const override { return false; }
  bool allow_append_to_file() const override { return false; }
  bool have_directory_mtimes() const override { return false; }
  bool have_flaky_directory_tree_deletion() const override { return false; }
  bool have_file_metadata() const override { return true; }

  std::shared_ptr<GcsFileSystem> gcs_fs_;
  std::shared_ptr<FileSystem> fs_;
};

GENERIC_FS_TEST_FUNCTIONS(TestGCSFSGeneric);

TEST(GcsFileSystem, OptionsCompare) {
  auto a = GcsOptions::Anonymous();
  auto b = a;
  b.endpoint_override = "localhost:1234";
  EXPECT_TRUE(a.Equals(a));
  EXPECT_TRUE(b.Equals(b));
  auto c = b;
  c.scheme = "https";
  EXPECT_FALSE(b.Equals(c));
}

TEST(GcsFileSystem, OptionsAnonymous) {
  GcsOptions a = GcsOptions::Anonymous();
  EXPECT_THAT(a.credentials.holder(), NotNull());
  EXPECT_TRUE(a.credentials.anonymous());
  EXPECT_EQ(a.scheme, "http");
}

TEST(GcsFileSystem, OptionsFromUri) {
  std::string path;

  ASSERT_OK_AND_ASSIGN(GcsOptions options, GcsOptions::FromUri("gs://", &path));
  EXPECT_EQ(options.default_bucket_location, "");
  EXPECT_EQ(options.scheme, "https");
  EXPECT_EQ(options.endpoint_override, "");
  EXPECT_EQ(path, "");

  ASSERT_OK_AND_ASSIGN(options, GcsOptions::FromUri("gs:", &path));
  EXPECT_EQ(path, "");

  // Username/password is unsupported
  ASSERT_RAISES(Invalid, GcsOptions::FromUri("gs://access:secret@mybucket", &path));

  ASSERT_OK_AND_ASSIGN(options, GcsOptions::FromUri("gs://mybucket/", &path));
  EXPECT_EQ(options.default_bucket_location, "");
  EXPECT_EQ(options.scheme, "https");
  EXPECT_EQ(options.endpoint_override, "");
  EXPECT_EQ(path, "mybucket");

  ASSERT_OK_AND_ASSIGN(options, GcsOptions::FromUri("gs://mybucket/foo/bar/", &path));
  EXPECT_EQ(options.default_bucket_location, "");
  EXPECT_EQ(options.scheme, "https");
  EXPECT_EQ(options.endpoint_override, "");
  EXPECT_EQ(path, "mybucket/foo/bar");

  // Explicit default_bucket_location override
  ASSERT_OK_AND_ASSIGN(
      options,
      GcsOptions::FromUri("gs://mybucket/foo/bar/"
                          "?endpoint_override=localhost&scheme=http&location=us-west2"
                          "&retry_limit_seconds=40.5",
                          &path));
  EXPECT_EQ(options.default_bucket_location, "us-west2");
  EXPECT_EQ(options.scheme, "http");
  EXPECT_EQ(options.endpoint_override, "localhost");
  EXPECT_EQ(path, "mybucket/foo/bar");
  ASSERT_TRUE(options.retry_limit_seconds.has_value());
  EXPECT_EQ(*options.retry_limit_seconds, 40.5);

  // Missing bucket name
  ASSERT_RAISES(Invalid, GcsOptions::FromUri("gs:///foo/bar/", &path));

  // Invalid option
  ASSERT_RAISES(Invalid, GcsOptions::FromUri("gs://mybucket/?xxx=zzz", &path));

  // Invalid retry limit
  ASSERT_RAISES(Invalid,
                GcsOptions::FromUri("gs://foo/bar/?retry_limit_seconds=0", &path));
  ASSERT_RAISES(Invalid,
                GcsOptions::FromUri("gs://foo/bar/?retry_limit_seconds=-1", &path));
}

TEST(GcsFileSystem, OptionsAccessToken) {
  TimePoint expiration = std::chrono::system_clock::now() + std::chrono::minutes(5);
  auto a = GcsOptions::FromAccessToken(/*access_token=*/"accesst", expiration);
  EXPECT_THAT(a.credentials.holder(), NotNull());
  EXPECT_THAT(a.credentials.access_token(), Eq("accesst"));
  EXPECT_THAT(a.credentials.expiration(), Eq(expiration));
  EXPECT_EQ(a.scheme, "https");
}

TEST(GcsFileSystem, OptionsImpersonateServiceAccount) {
  TimePoint expiration = std::chrono::system_clock::now() + std::chrono::minutes(5);
  auto base = GcsOptions::FromAccessToken(/*access_token=*/"at", expiration);
  std::string account = "invalid-sa-test-only@my-project.iam.gserviceaccount.com";
  auto a = GcsOptions::FromImpersonatedServiceAccount(base.credentials, account);
  EXPECT_THAT(a.credentials.holder(), NotNull());
  EXPECT_THAT(a.credentials.access_token(), Eq("at"));
  EXPECT_THAT(a.credentials.expiration(), Eq(expiration));
  EXPECT_THAT(a.credentials.target_service_account(), Eq(account));

  EXPECT_EQ(a.scheme, "https");
}

TEST(GcsFileSystem, OptionsServiceAccountCredentials) {
  // While this service account key has the correct format, it cannot be used for
  // authentication because the key has been deactivated on the server-side, *and* the
  // account(s) involved are deleted *and* they are not the accounts or projects do not
  // match its contents.
  constexpr char kJsonKeyfileContents[] = R"""({
      "type": "service_account",
      "project_id": "foo-project",
      "private_key_id": "a1a111aa1111a11a11a11aa111a111a1a1111111",
      "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCltiF2oP3KJJ+S\ntTc1McylY+TuAi3AdohX7mmqIjd8a3eBYDHs7FlnUrFC4CRijCr0rUqYfg2pmk4a\n6TaKbQRAhWDJ7XD931g7EBvCtd8+JQBNWVKnP9ByJUaO0hWVniM50KTsWtyX3up/\nfS0W2R8Cyx4yvasE8QHH8gnNGtr94iiORDC7De2BwHi/iU8FxMVJAIyDLNfyk0hN\neheYKfIDBgJV2v6VaCOGWaZyEuD0FJ6wFeLybFBwibrLIBE5Y/StCrZoVZ5LocFP\nT4o8kT7bU6yonudSCyNMedYmqHj/iF8B2UN1WrYx8zvoDqZk0nxIglmEYKn/6U7U\ngyETGcW9AgMBAAECggEAC231vmkpwA7JG9UYbviVmSW79UecsLzsOAZnbtbn1VLT\nPg7sup7tprD/LXHoyIxK7S/jqINvPU65iuUhgCg3Rhz8+UiBhd0pCH/arlIdiPuD\n2xHpX8RIxAq6pGCsoPJ0kwkHSw8UTnxPV8ZCPSRyHV71oQHQgSl/WjNhRi6PQroB\nSqc/pS1m09cTwyKQIopBBVayRzmI2BtBxyhQp9I8t5b7PYkEZDQlbdq0j5Xipoov\n9EW0+Zvkh1FGNig8IJ9Wp+SZi3rd7KLpkyKPY7BK/g0nXBkDxn019cET0SdJOHQG\nDiHiv4yTRsDCHZhtEbAMKZEpku4WxtQ+JjR31l8ueQKBgQDkO2oC8gi6vQDcx/CX\nZ23x2ZUyar6i0BQ8eJFAEN+IiUapEeCVazuxJSt4RjYfwSa/p117jdZGEWD0GxMC\n+iAXlc5LlrrWs4MWUc0AHTgXna28/vii3ltcsI0AjWMqaybhBTTNbMFa2/fV2OX2\nUimuFyBWbzVc3Zb9KAG4Y7OmJQKBgQC5324IjXPq5oH8UWZTdJPuO2cgRsvKmR/r\n9zl4loRjkS7FiOMfzAgUiXfH9XCnvwXMqJpuMw2PEUjUT+OyWjJONEK4qGFJkbN5\n3ykc7p5V7iPPc7Zxj4mFvJ1xjkcj+i5LY8Me+gL5mGIrJ2j8hbuv7f+PWIauyjnp\nNx/0GVFRuQKBgGNT4D1L7LSokPmFIpYh811wHliE0Fa3TDdNGZnSPhaD9/aYyy78\nLkxYKuT7WY7UVvLN+gdNoVV5NsLGDa4cAV+CWPfYr5PFKGXMT/Wewcy1WOmJ5des\nAgMC6zq0TdYmMBN6WpKUpEnQtbmh3eMnuvADLJWxbH3wCkg+4xDGg2bpAoGAYRNk\nMGtQQzqoYNNSkfus1xuHPMA8508Z8O9pwKU795R3zQs1NAInpjI1sOVrNPD7Ymwc\nW7mmNzZbxycCUL/yzg1VW4P1a6sBBYGbw1SMtWxun4ZbnuvMc2CTCh+43/1l+FHe\nMmt46kq/2rH2jwx5feTbOE6P6PINVNRJh/9BDWECgYEAsCWcH9D3cI/QDeLG1ao7\nrE2NcknP8N783edM07Z/zxWsIsXhBPY3gjHVz2LDl+QHgPWhGML62M0ja/6SsJW3\nYvLLIc82V7eqcVJTZtaFkuht68qu/Jn1ezbzJMJ4YXDYo1+KFi+2CAGR06QILb+I\nlUtj+/nH3HDQjM4ltYfTPUg=\n-----END PRIVATE KEY-----\n",
      "client_email": "foo-email@foo-project.iam.gserviceaccount.com",
      "client_id": "100000000000000000001",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/foo-email%40foo-project.iam.gserviceaccount.com"
  })""";

  auto a = GcsOptions::FromServiceAccountCredentials(kJsonKeyfileContents);
  EXPECT_THAT(a.credentials.holder(), NotNull());
  EXPECT_THAT(a.credentials.json_credentials(), kJsonKeyfileContents);
  EXPECT_EQ(a.scheme, "https");
}

TEST(GcsFileSystem, ToArrowStatusOK) {
  Status actual = internal::ToArrowStatus(google::cloud::Status());
  EXPECT_TRUE(actual.ok());
}

TEST(GcsFileSystem, ToArrowStatus) {
  struct {
    google::cloud::StatusCode input;
    arrow::StatusCode expected;
  } cases[] = {
      {google::cloud::StatusCode::kCancelled, StatusCode::Cancelled},
      {google::cloud::StatusCode::kUnknown, StatusCode::UnknownError},
      {google::cloud::StatusCode::kInvalidArgument, StatusCode::Invalid},
      {google::cloud::StatusCode::kDeadlineExceeded, StatusCode::IOError},
      {google::cloud::StatusCode::kNotFound, StatusCode::IOError},
      {google::cloud::StatusCode::kAlreadyExists, StatusCode::AlreadyExists},
      {google::cloud::StatusCode::kPermissionDenied, StatusCode::IOError},
      {google::cloud::StatusCode::kUnauthenticated, StatusCode::IOError},
      {google::cloud::StatusCode::kResourceExhausted, StatusCode::CapacityError},
      {google::cloud::StatusCode::kFailedPrecondition, StatusCode::IOError},
      {google::cloud::StatusCode::kAborted, StatusCode::IOError},
      {google::cloud::StatusCode::kOutOfRange, StatusCode::Invalid},
      {google::cloud::StatusCode::kUnimplemented, StatusCode::NotImplemented},
      {google::cloud::StatusCode::kInternal, StatusCode::IOError},
      {google::cloud::StatusCode::kUnavailable, StatusCode::IOError},
      {google::cloud::StatusCode::kDataLoss, StatusCode::IOError},
  };

  for (const auto& test : cases) {
    auto status = google::cloud::Status(test.input, "test-message");
    auto message = [&] {
      std::ostringstream os;
      os << status;
      return os.str();
    }();
    SCOPED_TRACE("Testing with status=" + message);
    const auto actual = arrow::fs::internal::ToArrowStatus(status);
    EXPECT_EQ(actual.code(), test.expected);
    EXPECT_THAT(actual.message(), HasSubstr(message));
  }
}

TEST(GcsFileSystem, FileSystemCompare) {
  GcsOptions a_options;
  a_options.scheme = "http";
  auto a = GcsFileSystem::Make(a_options);
  EXPECT_THAT(a, NotNull());
  EXPECT_TRUE(a->Equals(*a));

  GcsOptions b_options;
  b_options.scheme = "http";
  b_options.endpoint_override = "localhost:1234";
  auto b = GcsFileSystem::Make(b_options);
  EXPECT_THAT(b, NotNull());
  EXPECT_TRUE(b->Equals(*b));

  EXPECT_FALSE(a->Equals(*b));
}

std::shared_ptr<const KeyValueMetadata> KeyValueMetadataForTest() {
  return arrow::key_value_metadata({
      {"Cache-Control", "test-only-cache-control"},
      {"Content-Disposition", "test-only-content-disposition"},
      {"Content-Encoding", "test-only-content-encoding"},
      {"Content-Language", "test-only-content-language"},
      {"Content-Type", "test-only-content-type"},
      {"customTime", "2021-10-26T01:02:03.456Z"},
      {"storageClass", "test-only-storage-class"},
      {"key", "test-only-value"},
      {"kmsKeyName", "test-only-kms-key-name"},
      {"predefinedAcl", "test-only-predefined-acl"},
      // computed with: /bin/echo -n "01234567" | openssl base64
      {"encryptionKeyBase64", "MDEyMzQ1Njc="},
  });
}

TEST(GcsFileSystem, ToEncryptionKey) {
  gcs::EncryptionKey key;
  ASSERT_OK_AND_ASSIGN(key,
                       arrow::fs::internal::ToEncryptionKey(KeyValueMetadataForTest()));
  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key.value().algorithm, "AES256");
  EXPECT_EQ(key.value().key, "MDEyMzQ1Njc=");
  // /bin/echo -n "01234567" | sha256sum | awk '{printf("%s", $1);}' |
  //       xxd -r -p | openssl base64
  // to get the SHA256 value of the key.
  EXPECT_EQ(key.value().sha256, "kkWSubED8U+DP6r7Z/SAaR8BmIqkV8AGF2n1jNRzEbw=");
}

TEST(GcsFileSystem, ToEncryptionKeyEmpty) {
  gcs::EncryptionKey key;
  ASSERT_OK_AND_ASSIGN(key, arrow::fs::internal::ToEncryptionKey({}));
  ASSERT_FALSE(key.has_value());
}

TEST(GcsFileSystem, ToKmsKeyName) {
  gcs::KmsKeyName key;
  ASSERT_OK_AND_ASSIGN(key, arrow::fs::internal::ToKmsKeyName(KeyValueMetadataForTest()));
  EXPECT_EQ(key.value_or(""), "test-only-kms-key-name");
}

TEST(GcsFileSystem, ToKmsKeyNameEmpty) {
  gcs::KmsKeyName key;
  ASSERT_OK_AND_ASSIGN(key, arrow::fs::internal::ToKmsKeyName({}));
  ASSERT_FALSE(key.has_value());
}

TEST(GcsFileSystem, ToPredefinedAcl) {
  gcs::PredefinedAcl predefined;
  ASSERT_OK_AND_ASSIGN(predefined,
                       arrow::fs::internal::ToPredefinedAcl(KeyValueMetadataForTest()));
  EXPECT_EQ(predefined.value_or(""), "test-only-predefined-acl");
}

TEST(GcsFileSystem, ToPredefinedAclEmpty) {
  gcs::PredefinedAcl predefined;
  ASSERT_OK_AND_ASSIGN(predefined, arrow::fs::internal::ToPredefinedAcl({}));
  ASSERT_FALSE(predefined.has_value());
}

TEST(GcsFileSystem, ToObjectMetadata) {
  gcs::WithObjectMetadata metadata;
  ASSERT_OK_AND_ASSIGN(metadata,
                       arrow::fs::internal::ToObjectMetadata(KeyValueMetadataForTest()));
  ASSERT_TRUE(metadata.has_value());
  EXPECT_EQ(metadata.value().cache_control(), "test-only-cache-control");
  EXPECT_EQ(metadata.value().content_disposition(), "test-only-content-disposition");
  EXPECT_EQ(metadata.value().content_encoding(), "test-only-content-encoding");
  EXPECT_EQ(metadata.value().content_language(), "test-only-content-language");
  EXPECT_EQ(metadata.value().content_type(), "test-only-content-type");
  ASSERT_TRUE(metadata.value().has_custom_time());
  EXPECT_THAT(metadata.value().metadata(),
              UnorderedElementsAre(Pair("key", "test-only-value")));
}

TEST(GcsFileSystem, ToObjectMetadataEmpty) {
  gcs::WithObjectMetadata metadata;
  ASSERT_OK_AND_ASSIGN(metadata, arrow::fs::internal::ToObjectMetadata({}));
  ASSERT_FALSE(metadata.has_value());
}

TEST(GcsFileSystem, ToObjectMetadataInvalidCustomTime) {
  auto metadata = arrow::fs::internal::ToObjectMetadata(arrow::key_value_metadata({
      {"customTime", "invalid"},
  }));
  EXPECT_EQ(metadata.status().code(), StatusCode::Invalid);
  EXPECT_THAT(metadata.status().message(), HasSubstr("Error parsing RFC-3339"));
}

TEST(GcsFileSystem, ObjectMetadataRoundtrip) {
  const auto source = KeyValueMetadataForTest();
  gcs::WithObjectMetadata tmp;
  ASSERT_OK_AND_ASSIGN(tmp, arrow::fs::internal::ToObjectMetadata(source));
  std::shared_ptr<const KeyValueMetadata> destination;
  ASSERT_OK_AND_ASSIGN(destination, arrow::fs::internal::FromObjectMetadata(tmp.value()));
  // Only a subset of the keys are configurable in gcs::ObjectMetadata, for example, the
  // size and CRC32C values of an object are only settable when the metadata is received
  // from the services. For those keys that should be preserved, verify they are preserved
  // in the round trip.
  for (auto key : {"Cache-Control", "Content-Disposition", "Content-Encoding",
                   "Content-Language", "Content-Type", "storageClass"}) {
    SCOPED_TRACE("Testing key " + std::string(key));
    ASSERT_OK_AND_ASSIGN(auto s, source->Get(key));
    ASSERT_OK_AND_ASSIGN(auto d, destination->Get(key));
    EXPECT_EQ(s, d);
  }
  // RFC-3339 formatted timestamps may differ in trivial things, e.g., the UTC timezone
  // can be represented as 'Z' or '+00:00'.
  ASSERT_OK_AND_ASSIGN(auto s, source->Get("customTime"));
  ASSERT_OK_AND_ASSIGN(auto d, destination->Get("customTime"));
  std::string err;
  absl::Time ts;
  absl::Time td;
  ASSERT_TRUE(absl::ParseTime(absl::RFC3339_full, s, &ts, &err)) << "error=" << err;
  ASSERT_TRUE(absl::ParseTime(absl::RFC3339_full, d, &td, &err)) << "error=" << err;
  EXPECT_NEAR(absl::ToDoubleSeconds(ts - td), 0, 0.5);
}

TEST_F(GcsIntegrationTest, GetFileInfoBucket) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  arrow::fs::AssertFileInfo(fs.get(), PreexistingBucketName(), FileType::Directory);

  // URI
  ASSERT_RAISES(Invalid, fs->GetFileInfo("gs://" + PreexistingBucketName()));
}

TEST_F(GcsIntegrationTest, GetFileInfoObjectWithNestedStructure) {
  // Adds detailed tests to handle cases of different edge cases
  // with directory naming conventions (e.g. with and without slashes).
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  constexpr auto kObjectName = "test-object-dir/some_other_dir/another_dir/foo";
  ASSERT_OK_AND_ASSIGN(
      auto output,
      fs->OpenOutputStream(PreexistingBucketPath() + kObjectName, /*metadata=*/{}));
  const auto data = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(data.data(), data.size()));
  ASSERT_OK(output->Close());

  // 0 is immediately after "/" lexicographically, ensure that this doesn't
  // cause unexpected issues.
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(PreexistingBucketPath() +
                                                        "test-object-dir/some_other_dir0",
                                                    /*metadata=*/{}));
  ASSERT_OK(output->Write(data.data(), data.size()));
  ASSERT_OK(output->Close());
  ASSERT_OK_AND_ASSIGN(
      output,
      fs->OpenOutputStream(PreexistingBucketPath() + kObjectName + "0", /*metadata=*/{}));
  ASSERT_OK(output->Write(data.data(), data.size()));
  ASSERT_OK(output->Close());

  AssertFileInfo(fs.get(), PreexistingBucketPath() + kObjectName, FileType::File);
  AssertFileInfo(fs.get(), PreexistingBucketPath() + kObjectName + "/",
                 FileType::NotFound);
  AssertFileInfo(fs.get(), PreexistingBucketPath() + "test-object-dir",
                 FileType::Directory);
  AssertFileInfo(fs.get(), PreexistingBucketPath() + "test-object-dir/",
                 FileType::Directory);
  AssertFileInfo(fs.get(), PreexistingBucketPath() + "test-object-dir/some_other_dir",
                 FileType::Directory);
  AssertFileInfo(fs.get(), PreexistingBucketPath() + "test-object-dir/some_other_dir/",
                 FileType::Directory);

  AssertFileInfo(fs.get(), PreexistingBucketPath() + "test-object-di",
                 FileType::NotFound);
  AssertFileInfo(fs.get(), PreexistingBucketPath() + "test-object-dir/some_other_di",
                 FileType::NotFound);
}

TEST_F(GcsIntegrationTest, GetFileInfoObjectNoExplicitObject) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  auto object =
      GcsClient().GetObjectMetadata(PreexistingBucketName(), PreexistingObjectName());
  ASSERT_TRUE(object.ok()) << "status=" << object.status();
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::File,
                            object->time_created(), static_cast<int64_t>(object->size()));

  // URI
  ASSERT_RAISES(Invalid, fs->GetFileInfo("gs://" + PreexistingObjectName()));
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorRecursive) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));
  std::vector<arrow::fs::FileInfo> expected;
  std::copy_if(hierarchy.contents.begin(), hierarchy.contents.end(),
               std::back_inserter(expected), [&](const arrow::fs::FileInfo& info) {
                 if (!fs::internal::IsAncestorOf(hierarchy.base_dir, info.path())) {
                   return false;
                 }
                 return hierarchy.base_dir != info.path();
               });
  expected = CleanupDirectoryNames(std::move(expected));

  auto selector = FileSelector();
  selector.base_dir = hierarchy.base_dir;
  selector.allow_not_found = false;
  selector.recursive = true;
  selector.max_recursion = 16;
  ASSERT_OK_AND_ASSIGN(auto results, fs->GetFileInfo(selector));
  EXPECT_THAT(results, UnorderedElementsAreArray(expected.begin(), expected.end()));

  // URI
  selector.base_dir = "gs://" + selector.base_dir;
  ASSERT_RAISES(Invalid, fs->GetFileInfo(selector));
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorNonRecursive) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));
  std::vector<arrow::fs::FileInfo> expected;
  std::copy_if(hierarchy.contents.begin(), hierarchy.contents.end(),
               std::back_inserter(expected), [&](const arrow::fs::FileInfo& info) {
                 if (info.path() == hierarchy.base_dir) return false;
                 return internal::GetAbstractPathParent(info.path()).first ==
                        hierarchy.base_dir;
               });

  auto selector = FileSelector();
  selector.base_dir = hierarchy.base_dir;
  selector.allow_not_found = false;
  selector.recursive = false;
  ASSERT_OK_AND_ASSIGN(auto results, fs->GetFileInfo(selector));
  EXPECT_THAT(results, UnorderedElementsAreArray(expected.begin(), expected.end()));
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorLimitedRecursion) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));

  for (const auto max_recursion : {0, 1, 2, 3}) {
    SCOPED_TRACE("Testing with max_recursion=" + std::to_string(max_recursion));
    const auto max_depth =
        internal::Depth(internal::EnsureTrailingSlash(hierarchy.base_dir)) +
        max_recursion + 1;  // Add 1 because files in a directory should be included
    std::vector<arrow::fs::FileInfo> expected;
    std::copy_if(hierarchy.contents.begin(), hierarchy.contents.end(),
                 std::back_inserter(expected), [&](const arrow::fs::FileInfo& info) {
                   if (info.path() == hierarchy.base_dir) return false;
                   if (!fs::internal::IsAncestorOf(hierarchy.base_dir, info.path())) {
                     return false;
                   }
                   return internal::Depth(info.path()) <= max_depth;
                 });
    expected = CleanupDirectoryNames(std::move(expected));
    auto selector = FileSelector();
    selector.base_dir = hierarchy.base_dir;
    selector.allow_not_found = true;
    selector.recursive = true;
    selector.max_recursion = max_recursion;
    ASSERT_OK_AND_ASSIGN(auto results, fs->GetFileInfo(selector));
    EXPECT_THAT(results, UnorderedElementsAreArray(expected.begin(), expected.end()));
  }
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorNotFoundTrue) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  auto selector = FileSelector();
  selector.base_dir = NotFoundObjectPath() + "/";
  selector.allow_not_found = true;
  selector.recursive = true;
  ASSERT_OK_AND_ASSIGN(auto results, fs->GetFileInfo(selector));
  EXPECT_EQ(results.size(), 0);
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorNotFoundFalse) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  auto selector = FileSelector();
  selector.base_dir = NotFoundObjectPath() + "/";
  selector.allow_not_found = false;
  selector.recursive = false;
  ASSERT_RAISES(IOError, fs->GetFileInfo(selector));
}

TEST_F(GcsIntegrationTest, CreateDirSuccessBucketOnly) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  auto bucket_name = RandomBucketName();
  ASSERT_OK(fs->CreateDir(bucket_name, false));
  arrow::fs::AssertFileInfo(fs.get(), bucket_name, FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirSuccessBucketAndFolder) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto path = PreexistingBucketPath() + RandomFolderName();
  ASSERT_OK(fs->CreateDir(path, false));
  arrow::fs::AssertFileInfo(fs.get(), path, FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirFailureFolderWithMissingBucket) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto path = std::string("not-a-bucket/new-folder");
  ASSERT_RAISES(IOError, fs->CreateDir(path, false));
}

TEST_F(GcsIntegrationTest, CreateDirRecursiveBucketOnly) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  auto bucket_name = RandomBucketName();
  ASSERT_OK(fs->CreateDir(bucket_name, true));
  arrow::fs::AssertFileInfo(fs.get(), bucket_name, FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirRecursiveFolderOnly) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto parent = PreexistingBucketPath() + RandomFolderName();
  const auto path = internal::ConcatAbstractPath(parent, "new-sub");
  ASSERT_OK(fs->CreateDir(path, true));
  arrow::fs::AssertFileInfo(fs.get(), path, FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), parent, FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirRecursiveBucketAndFolder) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  auto bucket_name = RandomBucketName();
  const auto parent = internal::ConcatAbstractPath(bucket_name, RandomFolderName());
  const auto path = internal::ConcatAbstractPath(parent, "new-sub");
  ASSERT_OK(fs->CreateDir(path, true));
  arrow::fs::AssertFileInfo(fs.get(), path, FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), parent, FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), bucket_name, FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirUri) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_RAISES(Invalid, fs->CreateDir("gs://" + RandomBucketName(), true));
}

TEST_F(GcsIntegrationTest, DeleteBucketDirSuccess) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_OK(fs->CreateDir("pyarrow-filesystem/", /*recursive=*/true));
  ASSERT_RAISES(Invalid, fs->CreateDir("/", false));
  ASSERT_OK(fs->DeleteDir("pyarrow-filesystem/"));
}

TEST_F(GcsIntegrationTest, DeleteDirSuccess) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));

  ASSERT_OK(fs->DeleteDir(hierarchy.base_dir));
  arrow::fs::AssertFileInfo(fs.get(), PreexistingBucketName(), FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::File);
  for (auto const& info : hierarchy.contents) {
    const auto expected_type = fs::internal::IsAncestorOf(hierarchy.base_dir, info.path())
                                   ? FileType::NotFound
                                   : info.type();
    arrow::fs::AssertFileInfo(fs.get(), info.path(), expected_type);
  }
}

TEST_F(GcsIntegrationTest, DeleteDirUri) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_RAISES(Invalid, fs->DeleteDir("gs://" + PreexistingBucketPath()));
}

TEST_F(GcsIntegrationTest, DeleteDirContentsSuccess) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));

  ASSERT_OK(fs->DeleteDirContents(hierarchy.base_dir));
  arrow::fs::AssertFileInfo(fs.get(), hierarchy.base_dir, FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), PreexistingBucketName(), FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::File);
  for (auto const& info : hierarchy.contents) {
    auto expected_type = FileType::NotFound;
    if (info.path() == hierarchy.base_dir ||
        !fs::internal::IsAncestorOf(hierarchy.base_dir, info.path())) {
      expected_type = info.type();
    }
    arrow::fs::AssertFileInfo(fs.get(), info.path(), expected_type);
  }
}

TEST_F(GcsIntegrationTest, DeleteRootDirContents) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, HasSubstr("too dangerous"),
                                  fs->DeleteRootDirContents());
}

TEST_F(GcsIntegrationTest, DeleteFileSuccess) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_OK(fs->DeleteFile(PreexistingObjectPath()));
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::NotFound);
}

TEST_F(GcsIntegrationTest, DeleteFileFailure) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_RAISES(IOError, fs->DeleteFile(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, DeleteFileDirectoryFails) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto path = PreexistingBucketPath() + "DeleteFileDirectoryFails/";
  ASSERT_RAISES(IOError, fs->DeleteFile(path));
}

TEST_F(GcsIntegrationTest, DeleteFileUri) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_RAISES(Invalid, fs->DeleteFile("gs://" + PreexistingObjectPath()));
}

TEST_F(GcsIntegrationTest, MoveFileSuccess) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto destination_path = PreexistingBucketPath() + "move-destination";
  ASSERT_OK(fs->Move(PreexistingObjectPath(), destination_path));
  arrow::fs::AssertFileInfo(fs.get(), destination_path, FileType::File);
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::NotFound);
}

TEST_F(GcsIntegrationTest, MoveFileCannotRenameBuckets) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_RAISES(IOError, fs->Move(PreexistingBucketPath(), "another-bucket/"));
}

TEST_F(GcsIntegrationTest, MoveFileCannotRenameDirectories) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_RAISES(IOError, fs->Move(PreexistingBucketPath() + "folder/",
                                  PreexistingBucketPath() + "new-name"));
}

TEST_F(GcsIntegrationTest, MoveFileCannotRenameToDirectory) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_OK(fs->CreateDir(PreexistingBucketPath() + "destination", false));
  ASSERT_RAISES(IOError, fs->Move(PreexistingObjectPath(),
                                  PreexistingBucketPath() + "destination"));
}

TEST_F(GcsIntegrationTest, MoveFileUri) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto destination_path = PreexistingBucketPath() + "move-destination";
  ASSERT_RAISES(Invalid, fs->Move("gs://" + PreexistingObjectPath(), destination_path));
  ASSERT_RAISES(Invalid, fs->Move(PreexistingObjectPath(), "gs://" + destination_path));
}

TEST_F(GcsIntegrationTest, CopyFileSuccess) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto destination_path = PreexistingBucketPath() + "copy-destination";
  ASSERT_OK(fs->CopyFile(PreexistingObjectPath(), destination_path));
  arrow::fs::AssertFileInfo(fs.get(), destination_path, FileType::File);
}

TEST_F(GcsIntegrationTest, CopyFileNotFound) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto destination_path = PreexistingBucketPath() + "copy-destination";
  ASSERT_RAISES(IOError, fs->CopyFile(NotFoundObjectPath(), destination_path));
}

TEST_F(GcsIntegrationTest, CopyFileUri) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  const auto destination_path = PreexistingBucketPath() + "copy-destination";
  ASSERT_RAISES(Invalid,
                fs->CopyFile("gs://" + PreexistingObjectPath(), destination_path));
  ASSERT_RAISES(Invalid,
                fs->CopyFile(PreexistingObjectPath(), "gs://" + destination_path));
}

TEST_F(GcsIntegrationTest, OpenInputStreamString) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs->OpenInputStream(PreexistingObjectPath()));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));

  EXPECT_EQ(std::string(buffer.data(), size), kLoremIpsum);
}

TEST_F(GcsIntegrationTest, OpenInputStreamStringBuffers) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs->OpenInputStream(PreexistingObjectPath()));

  std::string contents;
  std::shared_ptr<Buffer> buffer;
  do {
    ASSERT_OK_AND_ASSIGN(buffer, stream->Read(16));
    contents.append(buffer->ToString());
  } while (buffer && buffer->size() != 0);

  EXPECT_EQ(contents, kLoremIpsum);
}

TEST_F(GcsIntegrationTest, OpenInputStreamInfo) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingObjectPath()));

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs->OpenInputStream(info));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));

  EXPECT_EQ(std::string(buffer.data(), size), kLoremIpsum);
}

TEST_F(GcsIntegrationTest, OpenInputStreamEmpty) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  const auto object_path =
      internal::ConcatAbstractPath(PreexistingBucketName(), "empty-object.txt");
  CreateFile(fs.get(), object_path, std::string());

  ASSERT_OK_AND_ASSIGN(auto stream, fs->OpenInputStream(object_path));
  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));
  EXPECT_EQ(size, 0);
}

TEST_F(GcsIntegrationTest, OpenInputStreamNotFound) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  ASSERT_RAISES(IOError, fs->OpenInputStream(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, OpenInputStreamInfoInvalid) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingBucketPath()));
  ASSERT_RAISES(IOError, fs->OpenInputStream(info));

  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs->OpenInputStream(info));
}

TEST_F(GcsIntegrationTest, OpenInputStreamUri) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());
  ASSERT_RAISES(Invalid, fs->OpenInputStream("gs://" + PreexistingObjectPath()));
}

TEST_F(GcsIntegrationTest, OpenInputStreamReadMetadata) {
  auto client = GcsClient();
  const auto custom_time = std::chrono::system_clock::now() + std::chrono::hours(1);
  const std::string object_name = "OpenInputStreamMetadataTest/simple.txt";
  const gcs::ObjectMetadata expected =
      client
          .InsertObject(PreexistingBucketName(), object_name,
                        "The quick brown fox jumps over the lazy dog",
                        gcs::WithObjectMetadata(gcs::ObjectMetadata()
                                                    .set_content_type("text/plain")
                                                    .set_custom_time(custom_time)
                                                    .set_cache_control("no-cache")
                                                    .upsert_metadata("key0", "value0")))
          .value();

  auto fs = GcsFileSystem::Make(TestGcsOptions());
  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream,
                       fs->OpenInputStream(PreexistingBucketPath() + object_name));

  auto format_time = [](std::chrono::system_clock::time_point tp) {
    return absl::FormatTime(absl::RFC3339_full, absl::FromChrono(tp),
                            absl::UTCTimeZone());
  };

  std::shared_ptr<const KeyValueMetadata> actual;
  ASSERT_OK_AND_ASSIGN(actual, stream->ReadMetadata());
  ASSERT_OK_AND_EQ(expected.self_link(), actual->Get("selfLink"));
  ASSERT_OK_AND_EQ(expected.name(), actual->Get("name"));
  ASSERT_OK_AND_EQ(expected.bucket(), actual->Get("bucket"));
  ASSERT_OK_AND_EQ(std::to_string(expected.generation()), actual->Get("generation"));
  ASSERT_OK_AND_EQ(expected.content_type(), actual->Get("Content-Type"));
  ASSERT_OK_AND_EQ(format_time(expected.time_created()), actual->Get("timeCreated"));
  ASSERT_OK_AND_EQ(format_time(expected.updated()), actual->Get("updated"));
  ASSERT_FALSE(actual->Contains("timeDeleted"));
  ASSERT_OK_AND_EQ(format_time(custom_time), actual->Get("customTime"));
  ASSERT_OK_AND_EQ("false", actual->Get("temporaryHold"));
  ASSERT_OK_AND_EQ("false", actual->Get("eventBasedHold"));
  ASSERT_FALSE(actual->Contains("retentionExpirationTime"));
  ASSERT_OK_AND_EQ(expected.storage_class(), actual->Get("storageClass"));
  ASSERT_FALSE(actual->Contains("storageClassUpdated"));
  ASSERT_OK_AND_EQ(std::to_string(expected.size()), actual->Get("size"));
  ASSERT_OK_AND_EQ(expected.md5_hash(), actual->Get("md5Hash"));
  ASSERT_OK_AND_EQ(expected.media_link(), actual->Get("mediaLink"));
  ASSERT_OK_AND_EQ(expected.content_encoding(), actual->Get("Content-Encoding"));
  ASSERT_OK_AND_EQ(expected.content_disposition(), actual->Get("Content-Disposition"));
  ASSERT_OK_AND_EQ(expected.content_language(), actual->Get("Content-Language"));
  ASSERT_OK_AND_EQ(expected.cache_control(), actual->Get("Cache-Control"));
  auto p = expected.metadata().find("key0");
  ASSERT_TRUE(p != expected.metadata().end());
  ASSERT_OK_AND_EQ(p->second, actual->Get("metadata.key0"));
  ASSERT_EQ(expected.has_owner(), actual->Contains("owner.entity"));
  ASSERT_EQ(expected.has_owner(), actual->Contains("owner.entityId"));
  ASSERT_OK_AND_EQ(expected.crc32c(), actual->Get("crc32c"));
  ASSERT_OK_AND_EQ(std::to_string(expected.component_count()),
                   actual->Get("componentCount"));
  ASSERT_OK_AND_EQ(expected.etag(), actual->Get("etag"));
  ASSERT_FALSE(actual->Contains("customerEncryption.encryptionAlgorithm"));
  ASSERT_FALSE(actual->Contains("customerEncryption.keySha256"));
  ASSERT_FALSE(actual->Contains("kmsKeyName"));
}

TEST_F(GcsIntegrationTest, OpenInputStreamClosed) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  ASSERT_OK_AND_ASSIGN(auto stream, fs->OpenInputStream(PreexistingObjectPath()));
  ASSERT_OK(stream->Close());
  std::array<char, 16> buffer{};
  ASSERT_RAISES(Invalid, stream->Read(buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->Read(buffer.size()));
  ASSERT_RAISES(Invalid, stream->Tell());
}

TEST_F(GcsIntegrationTest, TestWriteWithDefaults) {
  auto options = TestGcsOptions();
  options.default_bucket_location = "utopia";
  options.default_metadata = arrow::key_value_metadata({{"foo", "bar"}});
  auto fs = GcsFileSystem::Make(options);
  std::string bucket = "new_bucket_with_default_location";
  auto file_name = "object_with_defaults";
  ASSERT_OK(fs->CreateDir(bucket, /*recursive=*/false));
  const auto path = bucket + "/" + file_name;
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, /*metadata=*/{}));
  const auto expected = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(expected.data(), expected.size()));
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));

  EXPECT_EQ(std::string(inbuf.data(), size), expected);
  auto object = GcsClient().GetObjectMetadata(bucket, file_name);
  ASSERT_TRUE(object.ok()) << "status=" << object.status();
  EXPECT_EQ(object->mutable_metadata()["foo"], "bar");
  auto bucket_info = GcsClient().GetBucketMetadata(bucket);
  ASSERT_TRUE(bucket_info.ok()) << "status=" << object.status();
  EXPECT_EQ(bucket_info->location(), "utopia");

  // Check that explicit metadata overrides the defaults.
  ASSERT_OK_AND_ASSIGN(
      output, fs->OpenOutputStream(
                  path, /*metadata=*/arrow::key_value_metadata({{"bar", "foo"}})));
  ASSERT_OK(output->Write(expected.data(), expected.size()));
  ASSERT_OK(output->Close());
  object = GcsClient().GetObjectMetadata(bucket, file_name);
  ASSERT_TRUE(object.ok()) << "status=" << object.status();
  EXPECT_EQ(object->mutable_metadata()["bar"], "foo");
  // Defaults are overwritten and not merged.
  EXPECT_FALSE(object->has_metadata("foo"));
}

TEST_F(GcsIntegrationTest, OpenOutputStreamSmall) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  const auto path = PreexistingBucketPath() + "test-write-object";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  const auto expected = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(expected.data(), expected.size()));
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));

  EXPECT_EQ(std::string(inbuf.data(), size), expected);
}

TEST_F(GcsIntegrationTest, OpenOutputStreamLarge) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  const auto path = PreexistingBucketPath() + "test-write-object";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  // These buffer sizes are intentionally not multiples of the upload quantum (256 KiB).
  std::array<std::int64_t, 3> sizes{257 * 1024, 258 * 1024, 259 * 1024};
  std::array<std::string, 3> buffers{
      std::string(sizes[0], 'A'),
      std::string(sizes[1], 'B'),
      std::string(sizes[2], 'C'),
  };
  auto expected = std::int64_t{0};
  for (auto i = 0; i != 3; ++i) {
    ASSERT_OK(output->Write(buffers[i].data(), buffers[i].size()));
    expected += sizes[i];
    ASSERT_EQ(output->Tell(), expected);
  }
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs->OpenInputStream(path));

  std::string contents;
  std::shared_ptr<Buffer> buffer;
  do {
    ASSERT_OK_AND_ASSIGN(buffer, input->Read(128 * 1024));
    ASSERT_TRUE(buffer);
    contents.append(buffer->ToString());
  } while (buffer->size() != 0);

  EXPECT_EQ(contents, buffers[0] + buffers[1] + buffers[2]);
}

TEST_F(GcsIntegrationTest, OpenOutputStreamClosed) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  const auto path = internal::ConcatAbstractPath(PreexistingBucketName(),
                                                 "open-output-stream-closed.txt");
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  ASSERT_OK(output->Close());
  ASSERT_RAISES(Invalid, output->Write(kLoremIpsum, std::strlen(kLoremIpsum)));
  ASSERT_RAISES(Invalid, output->Flush());
  ASSERT_RAISES(Invalid, output->Tell());
}

TEST_F(GcsIntegrationTest, OpenOutputStreamUri) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  const auto path =
      internal::ConcatAbstractPath(PreexistingBucketName(), "open-output-stream-uri.txt");
  ASSERT_RAISES(Invalid, fs->OpenInputStream("gs://" + path));
}

TEST_F(GcsIntegrationTest, OpenInputFileMixedReadVsReadAt) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  // Create a file large enough to make the random access tests non-trivial.
  auto constexpr kLineWidth = 100;
  auto constexpr kLineCount = 4096;
  std::vector<std::string> lines(kLineCount);
  int lineno = 0;
  std::generate_n(lines.begin(), lines.size(),
                  [&] { return RandomLine(++lineno, kLineWidth); });

  const auto path =
      PreexistingBucketPath() + "OpenInputFileMixedReadVsReadAt/object-name";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  for (auto const& line : lines) {
    ASSERT_OK(output->Write(line.data(), line.size()));
  }
  ASSERT_OK(output->Close());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
  for (int i = 0; i != 32; ++i) {
    SCOPED_TRACE("Iteration " + std::to_string(i));
    // Verify sequential reads work as expected.
    std::array<char, kLineWidth> buffer{};
    std::int64_t size;
    {
      ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
      EXPECT_EQ(lines[2 * i], actual->ToString());
    }
    {
      ASSERT_OK_AND_ASSIGN(size, file->Read(buffer.size(), buffer.data()));
      EXPECT_EQ(size, kLineWidth);
      auto actual = std::string{buffer.begin(), buffer.end()};
      EXPECT_EQ(lines[2 * i + 1], actual);
    }

    // Verify random reads interleave too.
    auto const index = RandomIndex(kLineCount);
    auto const position = index * kLineWidth;
    ASSERT_OK_AND_ASSIGN(size, file->ReadAt(position, buffer.size(), buffer.data()));
    EXPECT_EQ(size, kLineWidth);
    auto actual = std::string{buffer.begin(), buffer.end()};
    EXPECT_EQ(lines[index], actual);

    // Verify random reads using buffers work.
    ASSERT_OK_AND_ASSIGN(auto b, file->ReadAt(position, kLineWidth));
    EXPECT_EQ(lines[index], b->ToString());
  }
}

TEST_F(GcsIntegrationTest, OpenInputFileRandomSeek) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  // Create a file large enough to make the random access tests non-trivial.
  auto constexpr kLineWidth = 100;
  auto constexpr kLineCount = 4096;
  std::vector<std::string> lines(kLineCount);
  int lineno = 0;
  std::generate_n(lines.begin(), lines.size(),
                  [&] { return RandomLine(++lineno, kLineWidth); });

  const auto path = PreexistingBucketPath() + "OpenInputFileRandomSeek/object-name";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  for (auto const& line : lines) {
    ASSERT_OK(output->Write(line.data(), line.size()));
  }
  ASSERT_OK(output->Close());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
  for (int i = 0; i != 32; ++i) {
    SCOPED_TRACE("Iteration " + std::to_string(i));
    // Verify sequential reads work as expected.
    auto const index = RandomIndex(kLineCount);
    auto const position = index * kLineWidth;
    ASSERT_OK(file->Seek(position));
    ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
    EXPECT_EQ(lines[index], actual->ToString());
  }
}

TEST_F(GcsIntegrationTest, OpenInputFileIoContext) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  // Create a test file.
  const auto path = PreexistingBucketPath() + "OpenInputFileIoContext/object-name";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  const std::string contents = "The quick brown fox jumps over the lazy dog";
  ASSERT_OK(output->Write(contents.data(), contents.size()));
  ASSERT_OK(output->Close());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
  EXPECT_EQ(fs->io_context().external_id(), file->io_context().external_id());
}

TEST_F(GcsIntegrationTest, OpenInputFileInfo) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingObjectPath()));

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(info));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  auto constexpr kStart = 16;
  ASSERT_OK_AND_ASSIGN(size, file->ReadAt(kStart, buffer.size(), buffer.data()));

  auto const expected = std::string(kLoremIpsum).substr(kStart);
  EXPECT_EQ(std::string(buffer.data(), size), expected);
}

TEST_F(GcsIntegrationTest, OpenInputFileNotFound) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  ASSERT_RAISES(IOError, fs->OpenInputFile(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, OpenInputFileInfoInvalid) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingBucketPath()));
  ASSERT_RAISES(IOError, fs->OpenInputFile(info));

  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs->OpenInputFile(info));
}

TEST_F(GcsIntegrationTest, OpenInputFileClosed) {
  auto fs = GcsFileSystem::Make(TestGcsOptions());

  ASSERT_OK_AND_ASSIGN(auto stream, fs->OpenInputFile(PreexistingObjectPath()));
  ASSERT_OK(stream->Close());
  std::array<char, 16> buffer{};
  ASSERT_RAISES(Invalid, stream->Tell());
  ASSERT_RAISES(Invalid, stream->Read(buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->Read(buffer.size()));
  ASSERT_RAISES(Invalid, stream->ReadAt(1, buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->ReadAt(1, 1));
  ASSERT_RAISES(Invalid, stream->Seek(2));
}

TEST_F(GcsIntegrationTest, TestFileSystemFromUri) {
  // Smoke test for FileSystemFromUri
  std::string path;
  ASSERT_OK_AND_ASSIGN(
      auto fs,
      FileSystemFromUri(std::string("gs://anonymous@") + PreexistingBucketPath(), &path));
  EXPECT_EQ(fs->type_name(), "gcs");
  EXPECT_EQ(path, PreexistingBucketName());
  ASSERT_OK_AND_ASSIGN(
      path, fs->PathFromUri(std::string("gs://anonymous@") + PreexistingBucketPath()));
  EXPECT_EQ(path, PreexistingBucketName());
  ASSERT_OK_AND_ASSIGN(auto fs2, FileSystemFromUri(std::string("gcs://anonymous@") +
                                                   PreexistingBucketPath()));
  EXPECT_EQ(fs2->type_name(), "gcs");
  ASSERT_THAT(fs->PathFromUri("/foo/bar"),
              Raises(StatusCode::Invalid, testing::HasSubstr("Expected a URI")));
  ASSERT_THAT(
      fs->PathFromUri("s3:///foo/bar"),
      Raises(StatusCode::Invalid,
             testing::HasSubstr("expected a URI with one of the schemes (gs, gcs)")));
}

}  // namespace
}  // namespace fs
}  // namespace arrow

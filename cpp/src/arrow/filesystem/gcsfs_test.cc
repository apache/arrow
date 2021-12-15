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

#include "arrow/filesystem/gcsfs.h"

#include <absl/time/time.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <google/cloud/credentials.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/options.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/process.hpp>
#include <random>
#include <string>

#include "arrow/filesystem/gcsfs_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace fs {
/// Custom comparison for FileInfo, we need this to use complex googletest matchers.
inline bool operator==(const FileInfo& a, const FileInfo& b) {
  return a.path() == b.path() && a.type() == b.type();
}

namespace {

namespace bp = boost::process;
namespace gc = google::cloud;
namespace gcs = google::cloud::storage;

using ::testing::HasSubstr;
using ::testing::IsEmpty;
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
        "Cloud not start GCS emulator."
        " Used the following list of python interpreter names:");
    for (const auto& interpreter : names) {
      auto exe_path = bp::search_path(interpreter);
      error += " " + interpreter;
      if (exe_path.empty()) {
        error += " (exe not found)";
        continue;
      }

      server_process_ = bp::child(boost::this_process::environment(), exe_path, "-m",
                                  "testbench", "--port", port_, group_);
      if (server_process_.valid() && server_process_.running()) break;
      error += " (failed to start)";
      server_process_.terminate();
      server_process_.wait();
    }
    if (server_process_.valid() && server_process_.valid()) return;
    error_ = std::move(error);
  }

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
    ASSERT_THAT(Testbench()->error(), IsEmpty());

    // Initialize a PRNG with a small amount of entropy.
    generator_ = std::mt19937_64(std::random_device()());
    bucket_name_ = RandomChars(32);

    // Create a bucket and a small file in the testbench. This makes it easier to
    // bootstrap GcsFileSystem and its tests.
    auto client = gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>("http://127.0.0.1:" + Testbench()->port())
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials()));
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

  std::string RandomFolderName() { return RandomChars(32) + "/"; }

  struct Hierarchy {
    std::string base_dir;
    std::vector<FileInfo> contents;
  };

  Result<Hierarchy> CreateHierarchy(std::shared_ptr<arrow::fs::FileSystem> fs) {
    const char* const kTestFolders[] = {
        "a/", "a/0/", "a/0/0/", "a/1/", "a/2/",
    };
    constexpr auto kFilesPerFolder = 4;
    auto result = Hierarchy{PreexistingBucketPath() + "a/", {}};
    for (auto const* f : kTestFolders) {
      const auto folder = PreexistingBucketPath() + f;
      RETURN_NOT_OK(fs->CreateDir(folder, true));
      result.contents.push_back(arrow::fs::Dir(folder));
      for (int i = 0; i != kFilesPerFolder; ++i) {
        const auto filename = folder + "test-file-" + std::to_string(i);
        CreateFile(fs.get(), filename, filename);
        result.contents.push_back(arrow::fs::File(filename));
      }
    }
    return result;
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
  EXPECT_THAT(a.credentials, NotNull());
  EXPECT_EQ(a.scheme, "http");
}

TEST(GcsFileSystem, OptionsAccessToken) {
  auto a = GcsOptions::FromAccessToken(
      "invalid-access-token-test-only",
      std::chrono::system_clock::now() + std::chrono::minutes(5));
  EXPECT_THAT(a.credentials, NotNull());
  EXPECT_EQ(a.scheme, "https");
}

TEST(GcsFileSystem, OptionsImpersonateServiceAccount) {
  auto base = GcsOptions::FromAccessToken(
      "invalid-access-token-test-only",
      std::chrono::system_clock::now() + std::chrono::minutes(5));
  auto a = GcsOptions::FromImpersonatedServiceAccount(
      *base.credentials, "invalid-sa-test-only@my-project.iam.gserviceaccount.com");
  EXPECT_THAT(a.credentials, NotNull());
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
  EXPECT_THAT(a.credentials, NotNull());
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
  auto a = internal::MakeGcsFileSystemForTest(a_options);
  EXPECT_THAT(a, NotNull());
  EXPECT_TRUE(a->Equals(*a));

  GcsOptions b_options;
  b_options.scheme = "http";
  b_options.endpoint_override = "localhost:1234";
  auto b = internal::MakeGcsFileSystemForTest(b_options);
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
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  arrow::fs::AssertFileInfo(fs.get(), PreexistingBucketPath(), FileType::Directory);
}

TEST_F(GcsIntegrationTest, GetFileInfoObject) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  auto object =
      GcsClient().GetObjectMetadata(PreexistingBucketName(), PreexistingObjectName());
  ASSERT_TRUE(object.ok()) << "status=" << object.status();
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::File,
                            object->time_created(), static_cast<int64_t>(object->size()));
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorRecursive) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));
  std::vector<arrow::fs::FileInfo> expected;
  std::copy_if(
      hierarchy.contents.begin(), hierarchy.contents.end(), std::back_inserter(expected),
      [&](const arrow::fs::FileInfo& info) { return hierarchy.base_dir != info.path(); });

  auto selector = FileSelector();
  selector.base_dir = hierarchy.base_dir;
  selector.allow_not_found = false;
  selector.recursive = true;
  ASSERT_OK_AND_ASSIGN(auto results, fs->GetFileInfo(selector));
  EXPECT_THAT(results, UnorderedElementsAreArray(expected.begin(), expected.end()));
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorNonRecursive) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));
  std::vector<arrow::fs::FileInfo> expected;
  std::copy_if(hierarchy.contents.begin(), hierarchy.contents.end(),
               std::back_inserter(expected), [&](const arrow::fs::FileInfo& info) {
                 if (info.path() == hierarchy.base_dir) return false;
                 return internal::EnsureTrailingSlash(
                            internal::GetAbstractPathParent(info.path()).first) ==
                        hierarchy.base_dir;
               });

  auto selector = FileSelector();
  selector.base_dir = hierarchy.base_dir;
  selector.allow_not_found = false;
  selector.recursive = false;
  ASSERT_OK_AND_ASSIGN(auto results, fs->GetFileInfo(selector));
  EXPECT_THAT(results, UnorderedElementsAreArray(expected.begin(), expected.end()));
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorNotFoundTrue) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  auto selector = FileSelector();
  selector.base_dir = NotFoundObjectPath() + "/";
  selector.allow_not_found = true;
  selector.recursive = true;
  ASSERT_OK_AND_ASSIGN(auto results, fs->GetFileInfo(selector));
  EXPECT_THAT(results, IsEmpty());
}

TEST_F(GcsIntegrationTest, GetFileInfoSelectorNotFoundFalse) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  auto selector = FileSelector();
  selector.base_dir = NotFoundObjectPath() + "/";
  selector.allow_not_found = false;
  selector.recursive = false;
  ASSERT_RAISES(IOError, fs->GetFileInfo(selector));
}

TEST_F(GcsIntegrationTest, CreateDirSuccessBucketOnly) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  auto bucket_name = RandomBucketName();
  ASSERT_OK(fs->CreateDir(bucket_name, false));
  arrow::fs::AssertFileInfo(fs.get(), bucket_name + "/", FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirSuccessBucketAndFolder) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto path = PreexistingBucketPath() + RandomFolderName();
  ASSERT_OK(fs->CreateDir(path, false));
  arrow::fs::AssertFileInfo(fs.get(), path, FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirFailureFolderWithMissingBucket) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto path = std::string("not-a-bucket/new-folder/");
  ASSERT_RAISES(IOError, fs->CreateDir(path, false));
}

TEST_F(GcsIntegrationTest, CreateDirRecursiveBucketOnly) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  auto bucket_name = RandomBucketName();
  ASSERT_OK(fs->CreateDir(bucket_name, true));
  arrow::fs::AssertFileInfo(fs.get(), bucket_name + "/", FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirRecursiveFolderOnly) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto parent = PreexistingBucketPath() + RandomFolderName();
  const auto path = parent + "new-sub/";
  ASSERT_OK(fs->CreateDir(path, true));
  arrow::fs::AssertFileInfo(fs.get(), path, FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), parent, FileType::Directory);
}

TEST_F(GcsIntegrationTest, CreateDirRecursiveBucketAndFolder) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  auto bucket_name = RandomBucketName();
  const auto parent = bucket_name + "/" + RandomFolderName();
  const auto path = parent + "new-sub/";
  ASSERT_OK(fs->CreateDir(path, true));
  arrow::fs::AssertFileInfo(fs.get(), path, FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), parent, FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), bucket_name + "/", FileType::Directory);
}

TEST_F(GcsIntegrationTest, DeleteDirSuccess) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));

  ASSERT_OK(fs->DeleteDir(hierarchy.base_dir));
  arrow::fs::AssertFileInfo(fs.get(), PreexistingBucketPath(), FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::File);
  for (auto const& info : hierarchy.contents) {
    arrow::fs::AssertFileInfo(fs.get(), info.path(), FileType::NotFound);
  }
}

TEST_F(GcsIntegrationTest, DeleteDirContentsSuccess) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_OK_AND_ASSIGN(auto hierarchy, CreateHierarchy(fs));

  ASSERT_OK(fs->DeleteDirContents(hierarchy.base_dir));
  arrow::fs::AssertFileInfo(fs.get(), hierarchy.base_dir, FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), PreexistingBucketPath(), FileType::Directory);
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::File);
  for (auto const& info : hierarchy.contents) {
    if (info.path() == hierarchy.base_dir) continue;
    arrow::fs::AssertFileInfo(fs.get(), info.path(), FileType::NotFound);
  }
}

TEST_F(GcsIntegrationTest, DeleteRootDirContents) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, HasSubstr("too dangerous"),
                                  fs->DeleteRootDirContents());
}

TEST_F(GcsIntegrationTest, DeleteFileSuccess) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_OK(fs->DeleteFile(PreexistingObjectPath()));
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::NotFound);
}

TEST_F(GcsIntegrationTest, DeleteFileFailure) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_RAISES(IOError, fs->DeleteFile(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, DeleteFileDirectoryFails) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto path = PreexistingBucketPath() + "DeleteFileDirectoryFails/";
  ASSERT_RAISES(IOError, fs->DeleteFile(path));
}

TEST_F(GcsIntegrationTest, MoveFileSuccess) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto destination_path = PreexistingBucketPath() + "move-destination";
  ASSERT_OK(fs->Move(PreexistingObjectPath(), destination_path));
  arrow::fs::AssertFileInfo(fs.get(), destination_path, FileType::File);
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::NotFound);
}

TEST_F(GcsIntegrationTest, MoveFileCannotRenameBuckets) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_RAISES(IOError, fs->Move(PreexistingBucketPath(), "another-bucket/"));
}

TEST_F(GcsIntegrationTest, MoveFileCannotRenameDirectories) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_RAISES(IOError, fs->Move(PreexistingBucketPath() + "folder/",
                                  PreexistingBucketPath() + "new-name"));
}

TEST_F(GcsIntegrationTest, MoveFileCannotRenameToDirectory) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_OK(fs->CreateDir(PreexistingBucketPath() + "destination/", false));
  ASSERT_RAISES(IOError, fs->Move(PreexistingObjectPath(),
                                  PreexistingBucketPath() + "destination/"));
}

TEST_F(GcsIntegrationTest, CopyFileSuccess) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto destination_path = PreexistingBucketPath() + "copy-destination";
  ASSERT_OK(fs->CopyFile(PreexistingObjectPath(), destination_path));
  arrow::fs::AssertFileInfo(fs.get(), destination_path, FileType::File);
}

TEST_F(GcsIntegrationTest, CopyFileNotFound) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto destination_path = PreexistingBucketPath() + "copy-destination";
  ASSERT_RAISES(IOError, fs->CopyFile(NotFoundObjectPath(), destination_path));
}

TEST_F(GcsIntegrationTest, ReadObjectString) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs->OpenInputStream(PreexistingObjectPath()));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));

  EXPECT_EQ(std::string(buffer.data(), size), kLoremIpsum);
}

TEST_F(GcsIntegrationTest, ReadObjectStringBuffers) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

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

TEST_F(GcsIntegrationTest, ReadObjectInfo) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingObjectPath()));

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs->OpenInputStream(info));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));

  EXPECT_EQ(std::string(buffer.data(), size), kLoremIpsum);
}

TEST_F(GcsIntegrationTest, ReadObjectNotFound) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  ASSERT_RAISES(IOError, fs->OpenInputStream(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, ReadObjectInfoInvalid) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingBucketPath()));
  ASSERT_RAISES(IOError, fs->OpenInputStream(info));

  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs->OpenInputStream(info));
}

TEST_F(GcsIntegrationTest, ReadObjectReadMetadata) {
  auto client = GcsClient();
  const auto custom_time = std::chrono::system_clock::now() + std::chrono::hours(1);
  const std::string object_name = "ReadObjectMetadataTest/simple.txt";
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

  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
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

TEST_F(GcsIntegrationTest, WriteObjectSmall) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

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

TEST_F(GcsIntegrationTest, WriteObjectLarge) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

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

TEST_F(GcsIntegrationTest, OpenInputFileMixedReadVsReadAt) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

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
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

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

TEST_F(GcsIntegrationTest, OpenInputFileInfo) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

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
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  ASSERT_RAISES(IOError, fs->OpenInputFile(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, OpenInputFileInfoInvalid) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingBucketPath()));
  ASSERT_RAISES(IOError, fs->OpenInputFile(info));

  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs->OpenInputFile(info));
}

}  // namespace
}  // namespace fs
}  // namespace arrow

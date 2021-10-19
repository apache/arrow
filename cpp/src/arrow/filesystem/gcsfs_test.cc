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

#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <google/cloud/credentials.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/options.h>
#include <gtest/gtest.h>

#include <boost/process.hpp>
#include <string>

#include "arrow/filesystem/gcsfs_internal.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace fs {
namespace {

namespace bp = boost::process;
namespace gc = google::cloud;
namespace gcs = google::cloud::storage;

using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::NotNull;

auto const* kPreexistingBucket = "test-bucket-name";

class GcsIntegrationTest : public ::testing::Test {
 public:
  ~GcsIntegrationTest() override {
    if (server_process_.valid()) {
      // Brutal shutdown
      server_process_.terminate();
      server_process_.wait();
    }
  }

 protected:
  void SetUp() override {
    port_ = std::to_string(GetListenPort());
    auto exe_path = bp::search_path("python3");
    ASSERT_THAT(exe_path, Not(IsEmpty()));

    server_process_ = bp::child(boost::this_process::environment(), exe_path, "-m",
                                "testbench", "--port", port_);

    // Create a bucket in the testbench. This makes it easier to bootstrap GcsFileSystem
    // and its tests.
    auto client = gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>("http://127.0.0.1:" + port_)
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials()));
    google::cloud::StatusOr<gcs::BucketMetadata> metadata = client.CreateBucketForProject(
        kPreexistingBucket, "ignored-by-testbench", gcs::BucketMetadata{});
    ASSERT_TRUE(metadata.ok()) << "Failed to create bucket <" << kPreexistingBucket
                               << ">, status=" << metadata.status();
  }

  GcsOptions TestGcsOptions() {
    GcsOptions options;
    options.endpoint_override = "127.0.0.1:" + port_;
    options.scheme = "http";
    return options;
  }

 private:
  std::string port_;
  bp::child server_process_;
};

TEST(GcsFileSystem, OptionsCompare) {
  GcsOptions a;
  GcsOptions b;
  b.endpoint_override = "localhost:1234";
  EXPECT_TRUE(a.Equals(a));
  EXPECT_TRUE(b.Equals(b));
  auto c = b;
  c.scheme = "http";
  EXPECT_FALSE(b.Equals(c));
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
      {google::cloud::StatusCode::kNotFound, StatusCode::UnknownError},
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

TEST_F(GcsIntegrationTest, MakeBucket) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  arrow::fs::AssertFileInfo(fs.get(), kPreexistingBucket, FileType::Directory);
}

}  // namespace
}  // namespace fs
}  // namespace arrow

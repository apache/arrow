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

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace fs {
namespace {

namespace bp = boost::process;
namespace gc = google::cloud;
namespace gcs = google::cloud::storage;

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

    // Create a bucket in the testbench so additional
    auto client = gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>("http://127.0.0.1:" + port_)
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials()));
    auto metadata = client.CreateBucketForProject(
        kPreexistingBucket, "ignored-by-testbench", gcs::BucketMetadata{});
    EXPECT_TRUE(metadata.ok()) << "Failed to create bucket <" << kPreexistingBucket
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

TEST(GcsFileSystem, FileSystemCompare) {
  auto a = internal::MakeGcsFileSystemForTest(GcsOptions{});
  EXPECT_THAT(a, NotNull());
  EXPECT_TRUE(a->Equals(*a));

  GcsOptions options;
  options.endpoint_override = "localhost:1234";
  auto b = internal::MakeGcsFileSystemForTest(options);
  EXPECT_THAT(b, NotNull());
  EXPECT_TRUE(b->Equals(*b));

  EXPECT_FALSE(a->Equals(*b));
}

TEST_F(GcsIntegrationTest, MakeBucket) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_OK(fs->GetFileInfo(kPreexistingBucket));
}

}  // namespace
}  // namespace fs
}  // namespace arrow

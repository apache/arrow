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

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3_test_util.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/filesystem/util_internal.h"
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

namespace arrow::fs {

auto* minio_env = ::testing::AddGlobalTestEnvironment(new MinioTestEnvironment);

MinioTestEnvironment* GetMinioEnv() {
  return ::arrow::internal::checked_cast<MinioTestEnvironment*>(minio_env);
}

class RegistrationTestEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    // Unregister the s3 filesystem factory so that we can be sure the module loading and
    // the factories from the module are actually working
    ASSERT_OK(internal::UnregisterFileSystemFactory("s3"));
    ASSERT_OK(LoadFileSystemFactories(ARROW_S3_LIBPATH));
  }
  void TearDown() override { EnsureFinalized(); }
};

auto* lib_env = ::testing::AddGlobalTestEnvironment(new RegistrationTestEnvironment);

TEST(S3Test, FromUri) {
  ASSERT_OK_AND_ASSIGN(auto minio, GetMinioEnv()->GetOneServer());

  std::string path;
  ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFromUri("s3://" + minio->access_key() + ":" +
                                                      minio->secret_key() +
                                                      "@bucket/somedir/subdir/subfile",
                                                  &path));

  EXPECT_EQ(fs->MakeUri("/" + path),
            "s3://minio:miniopass@bucket/somedir/subdir/subfile"
            "?region=us-east-1&scheme=https&endpoint_override="
            "&allow_bucket_creation=0&allow_bucket_deletion=0");
}

}  // namespace arrow::fs

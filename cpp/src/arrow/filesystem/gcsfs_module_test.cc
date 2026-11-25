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

#include <string>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::fs {

class RegistrationTestEnvironment : public ::testing::Environment {
 public:
  void SetUp() override { ASSERT_OK(LoadFileSystemFactories(ARROW_GCS_LIBPATH)); }
};

auto* lib_env = ::testing::AddGlobalTestEnvironment(new RegistrationTestEnvironment);

TEST(GcsModuleTest, FromUri) {
  std::string path;
  for (const auto* scheme : {"gs", "gcs"}) {
    ASSERT_OK_AND_ASSIGN(
        auto fs,
        FileSystemFromUri(std::string(scheme) + "://anonymous@mybucket/foo", &path));
    EXPECT_EQ(fs->type_name(), "gcs");
    EXPECT_EQ(path, "mybucket/foo");
  }
}

TEST(GcsModuleTest, FromUriAndOptionsNotImplemented) {
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented, ::testing::HasSubstr("Filesystem factory does not support"),
      FileSystemFromUriAndOptions("gs://anonymous@mybucket/foo",
                                  {{"foo", std::string{"bar"}}}));
}

}  // namespace arrow::fs

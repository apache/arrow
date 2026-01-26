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

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include <dbpa_interface.h>
#include "gtest/gtest.h"
#include "parquet/encryption/external/dbpa_library_wrapper.h"
#include "parquet/encryption/external/loadable_encryptor_utils.h"
#include "parquet/encryption/external/test_utils.h"

namespace parquet::encryption::external::test {

// Test fixture for LoadableEncryptorUtils tests
class LoadableEncryptorUtilsTest : public ::testing::Test {
 public:
  std::string library_path_;

 protected:
  void SetUp() override {
    // Get the path to the DBPATestAgent shared library
    // This assumes the library is built
    library_path_ = TestUtils::GetTestLibraryPath();
  }
};

// ============================================================================
// SUCCESS TESTS
// ============================================================================

TEST_F(LoadableEncryptorUtilsTest, LoadValidLibrary) {
  // Test loading the library
  std::unique_ptr<DataBatchProtectionAgentInterface> agent;

  try {
    agent = LoadableEncryptorUtils::LoadFromLibrary(library_path_);
    ASSERT_NE(agent, nullptr) << "Agent should be successfully loaded";
  } catch (const std::runtime_error& e) {
    // Library doesn't exist or failed to load - this is expected in some build
    // environments
    GTEST_SKIP() << "Library not available: " << e.what();
  }
}

TEST_F(LoadableEncryptorUtilsTest, MultipleLoads) {
  // Load multiple agents
  std::unique_ptr<DataBatchProtectionAgentInterface> agent1, agent2, agent3;

  try {
    agent1 = LoadableEncryptorUtils::LoadFromLibrary(library_path_);
    agent2 = LoadableEncryptorUtils::LoadFromLibrary(library_path_);
    agent3 = LoadableEncryptorUtils::LoadFromLibrary(library_path_);

    ASSERT_NE(agent1, nullptr) << "First agent should be successfully loaded";
    ASSERT_NE(agent2, nullptr) << "Second agent should be successfully loaded";
    ASSERT_NE(agent3, nullptr) << "Third agent should be successfully loaded";

    // Verify that all instances are different from each other
    ASSERT_NE(agent1.get(), agent2.get())
        << "First and second agents should be different instances";
    ASSERT_NE(agent1.get(), agent3.get())
        << "First and third agents should be different instances";
    ASSERT_NE(agent2.get(), agent3.get())
        << "Second and third agents should be different instances";
  } catch (const std::runtime_error& e) {
    // Library doesn't exist or failed to load - this is expected in some build
    // environments
    GTEST_SKIP() << "Library not available: " << e.what();
  }
}

TEST_F(LoadableEncryptorUtilsTest, ReturnsDBPALibraryWrapper) {
  // Test that LoadFromLibrary returns an instance of DBPALibraryWrapper
  std::unique_ptr<DataBatchProtectionAgentInterface> agent;

  try {
    agent = LoadableEncryptorUtils::LoadFromLibrary(library_path_);
    ASSERT_NE(agent, nullptr) << "Agent should be successfully loaded";

    // Verify that the returned instance is of type DBPALibraryWrapper
    DBPALibraryWrapper* wrapper = dynamic_cast<DBPALibraryWrapper*>(agent.get());
    EXPECT_NE(wrapper, nullptr)
        << "Returned instance should be of type DBPALibraryWrapper";
  } catch (const std::runtime_error& e) {
    // Library doesn't exist or failed to load - this is expected in some build
    // environments
    GTEST_SKIP() << "Library not available: " << e.what();
  }
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

TEST_F(LoadableEncryptorUtilsTest, EmptyLibraryPath) {
  EXPECT_THROW({ LoadableEncryptorUtils::LoadFromLibrary(""); }, std::invalid_argument);
}

TEST_F(LoadableEncryptorUtilsTest, NonexistentLibrary) {
  EXPECT_THROW({ LoadableEncryptorUtils::LoadFromLibrary("./nonexistent_library.so"); },
               std::runtime_error);
}

TEST_F(LoadableEncryptorUtilsTest, InvalidLibraryPath) {
  EXPECT_THROW(
      { LoadableEncryptorUtils::LoadFromLibrary("/invalid/path/to/library.so"); },
      std::runtime_error);
}

}  // namespace parquet::encryption::external::test

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

// Test that arrow_mimalloc_* functions can be interposed via LD_PRELOAD.
// This test spawns a subprocess with the interposition library preloaded
// and verifies that allocations are tracked.

#include <cstdlib>
#include <regex>
#include <sstream>
#include <string>

#ifndef _WIN32
#  include <sys/wait.h>
#  include <unistd.h>
#endif

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/config.h"
#include "arrow/util/io_util.h"

namespace arrow {

#if defined(ARROW_MIMALLOC) && !defined(_WIN32)

class TestMimallocInterpose : public ::testing::Test {
 protected:
  void SetUp() override {
    // Get paths from environment (set by CTest)
    auto helper_path_result = internal::GetEnvVar("ARROW_INTERPOSE_TEST_HELPER");
    auto lib_path_result = internal::GetEnvVar("ARROW_INTERPOSE_TEST_LIB");

    if (!helper_path_result.ok() || !lib_path_result.ok()) {
      GTEST_SKIP() << "ARROW_INTERPOSE_TEST_HELPER or ARROW_INTERPOSE_TEST_LIB not set";
    }

    helper_path_ = *helper_path_result;
    lib_path_ = *lib_path_result;
  }

  // Run the helper program with LD_PRELOAD and capture output
  struct ProcessResult {
    int exit_code;
    std::string stdout_str;
    std::string stderr_str;
  };

  ProcessResult RunWithPreload() {
    ProcessResult result;
    result.exit_code = -1;

    // Create pipes for stdout and stderr
    int stdout_pipe[2];
    int stderr_pipe[2];
    if (pipe(stdout_pipe) != 0 || pipe(stderr_pipe) != 0) {
      return result;
    }

    pid_t pid = fork();
    if (pid == -1) {
      return result;
    }

    if (pid == 0) {
      // Child process
      close(stdout_pipe[0]);
      close(stderr_pipe[0]);

      dup2(stdout_pipe[1], STDOUT_FILENO);
      dup2(stderr_pipe[1], STDERR_FILENO);

      close(stdout_pipe[1]);
      close(stderr_pipe[1]);

      // Set LD_PRELOAD
      setenv("LD_PRELOAD", lib_path_.c_str(), 1);

      // Execute helper
      execl(helper_path_.c_str(), helper_path_.c_str(), nullptr);
      _exit(127);  // exec failed
    }

    // Parent process
    close(stdout_pipe[1]);
    close(stderr_pipe[1]);

    // Read stdout
    char buffer[4096];
    ssize_t n;
    while ((n = read(stdout_pipe[0], buffer, sizeof(buffer) - 1)) > 0) {
      buffer[n] = '\0';
      result.stdout_str += buffer;
    }
    close(stdout_pipe[0]);

    // Read stderr
    while ((n = read(stderr_pipe[0], buffer, sizeof(buffer) - 1)) > 0) {
      buffer[n] = '\0';
      result.stderr_str += buffer;
    }
    close(stderr_pipe[0]);

    // Wait for child
    int status;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
      result.exit_code = WEXITSTATUS(status);
    }

    return result;
  }

  std::string helper_path_;
  std::string lib_path_;
};

TEST_F(TestMimallocInterpose, InterpositionWorks) {
  auto result = RunWithPreload();

  // Check program succeeded
  ASSERT_EQ(result.exit_code, 0) << "Helper exited with code " << result.exit_code
                                 << "\nstdout: " << result.stdout_str
                                 << "\nstderr: " << result.stderr_str;

  // Check that we got the expected output from helper
  ASSERT_NE(result.stdout_str.find("SUCCESS"), std::string::npos)
      << "Helper did not report success\nstdout: " << result.stdout_str;

  // Check that interposition library printed stats
  ASSERT_NE(result.stderr_str.find("ARROW_INTERPOSE_STATS:"), std::string::npos)
      << "Interposition stats not found\nstderr: " << result.stderr_str;

  // Parse the stats
  std::regex stats_regex(
      R"(ARROW_INTERPOSE_STATS: allocs=(\d+) frees=(\d+) reallocs=(\d+) total_bytes=(\d+))");
  std::smatch match;
  ASSERT_TRUE(std::regex_search(result.stderr_str, match, stats_regex))
      << "Could not parse interposition stats\nstderr: " << result.stderr_str;

  int allocs = std::stoi(match[1].str());
  int frees = std::stoi(match[2].str());
  int reallocs = std::stoi(match[3].str());
  int total_bytes = std::stoi(match[4].str());

  // The test helper does: 3 allocs, 1 realloc, 3 frees
  // The realloc might internally do an alloc, so we check >= expected
  EXPECT_GE(allocs, 3) << "Expected at least 3 allocations";
  EXPECT_GE(frees, 3) << "Expected at least 3 frees";
  EXPECT_GE(reallocs, 1) << "Expected at least 1 realloc";
  // Total allocated: 1024 + 2048 + 512 = 3584 (not counting realloc growth)
  EXPECT_GE(total_bytes, 3584) << "Expected at least 3584 bytes allocated";
}

#else  // !ARROW_MIMALLOC || _WIN32

TEST(TestMimallocInterpose, NotAvailable) {
  GTEST_SKIP() << "Mimalloc interposition test requires ARROW_MIMALLOC and non-Windows";
}

#endif

}  // namespace arrow

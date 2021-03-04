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

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <utility>

// We need BOOST_USE_WINDOWS_H definition with MinGW when we use
// boost/process.hpp. See ARROW_BOOST_PROCESS_COMPILE_DEFINITIONS in
// cpp/cmake_modules/BuildUtils.cmake for details.
#include <boost/process.hpp>

#include <gtest/gtest.h>

#include <aws/core/Aws.h>

#include "arrow/filesystem/s3fs.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace fs {

using ::arrow::internal::TemporaryDir;

namespace bp = boost::process;

// TODO: allocate an ephemeral port
static const char* kMinioExecutableName = "minio";
static const char* kMinioAccessKey = "minio";
static const char* kMinioSecretKey = "miniopass";

// Environment variables to configure another S3-compatible service
static const char* kEnvConnectString = "ARROW_TEST_S3_CONNECT_STRING";
static const char* kEnvAccessKey = "ARROW_TEST_S3_ACCESS_KEY";
static const char* kEnvSecretKey = "ARROW_TEST_S3_SECRET_KEY";

static std::string GenerateConnectString() {
  std::stringstream ss;
  ss << "127.0.0.1:" << GetListenPort();
  return ss.str();
}

// A minio test server, managed as a child process

class MinioTestServer {
 public:
  Status Start();

  Status Stop();

  std::string connect_string() const { return connect_string_; }

  std::string access_key() const { return access_key_; }

  std::string secret_key() const { return secret_key_; }

 private:
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string connect_string_;
  std::string access_key_ = kMinioAccessKey;
  std::string secret_key_ = kMinioSecretKey;
  std::shared_ptr<::boost::process::child> server_process_;
};

Status MinioTestServer::Start() {
  const char* connect_str = std::getenv(kEnvConnectString);
  const char* access_key = std::getenv(kEnvAccessKey);
  const char* secret_key = std::getenv(kEnvSecretKey);
  if (connect_str && access_key && secret_key) {
    // Use external instance
    connect_string_ = connect_str;
    access_key_ = access_key;
    secret_key_ = secret_key;
    return Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(temp_dir_, TemporaryDir::Make("s3fs-test-"));

  // Get a copy of the current environment.
  // (NOTE: using "auto" would return a native_environment that mutates
  //  the current environment)
  bp::environment env = boost::this_process::environment();
  env["MINIO_ACCESS_KEY"] = kMinioAccessKey;
  env["MINIO_SECRET_KEY"] = kMinioSecretKey;

  connect_string_ = GenerateConnectString();

  auto exe_path = bp::search_path(kMinioExecutableName);
  if (exe_path.empty()) {
    return Status::IOError("Failed to find minio executable ('", kMinioExecutableName,
                           "') in PATH");
  }

  try {
    // NOTE: --quiet makes startup faster by suppressing remote version check
    server_process_ = std::make_shared<bp::child>(
        env, exe_path, "server", "--quiet", "--compat", "--address", connect_string_,
        temp_dir_->path().ToString());
  } catch (const std::exception& e) {
    return Status::IOError("Failed to launch Minio server: ", e.what());
  }
  return Status::OK();
}

Status MinioTestServer::Stop() {
  if (server_process_ && server_process_->valid()) {
    // Brutal shutdown
    server_process_->terminate();
    server_process_->wait();
  }
  return Status::OK();
}

// A global test "environment", to ensure that the S3 API is initialized before
// running unit tests.

class S3Environment : public ::testing::Environment {
 public:
  void SetUp() override {
    // Change this to increase logging during tests
    S3GlobalOptions options;
    options.log_level = S3LogLevel::Fatal;
    ASSERT_OK(InitializeS3(options));
  }

  void TearDown() override { ASSERT_OK(FinalizeS3()); }

 protected:
  Aws::SDKOptions options_;
};

::testing::Environment* s3_env = ::testing::AddGlobalTestEnvironment(new S3Environment);

}  // namespace fs
}  // namespace arrow

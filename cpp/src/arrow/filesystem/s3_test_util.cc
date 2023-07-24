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

#ifndef _WIN32
#include <sys/wait.h>
#endif

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
#ifdef __MINGW32__
#include <boost/asio/io_context.hpp>
#endif
#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
// We need BOOST_USE_WINDOWS_H definition with MinGW when we use
// boost/process.hpp. See BOOST_USE_WINDOWS_H=1 in
// cpp/cmake_modules/ThirdpartyToolchain.cmake for details.
#include <boost/process.hpp>

#include "arrow/filesystem/s3_test_util.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/testing/util.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace fs {

using ::arrow::internal::TemporaryDir;

namespace bp = boost::process;

namespace {

const char* kMinioExecutableName = "minio";
const char* kMinioAccessKey = "minio";
const char* kMinioSecretKey = "miniopass";

// Environment variables to configure another S3-compatible service
const char* kEnvConnectString = "ARROW_TEST_S3_CONNECT_STRING";
const char* kEnvAccessKey = "ARROW_TEST_S3_ACCESS_KEY";
const char* kEnvSecretKey = "ARROW_TEST_S3_SECRET_KEY";

std::string GenerateConnectString() { return GetListenAddress(); }

}  // namespace

struct MinioTestServer::Impl {
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string connect_string_;
  std::string access_key_ = kMinioAccessKey;
  std::string secret_key_ = kMinioSecretKey;
  std::shared_ptr<::boost::process::child> server_process_;
};

MinioTestServer::MinioTestServer() : impl_(new Impl) {}

MinioTestServer::~MinioTestServer() {
  auto st = Stop();
  ARROW_UNUSED(st);
}

std::string MinioTestServer::connect_string() const { return impl_->connect_string_; }

std::string MinioTestServer::access_key() const { return impl_->access_key_; }

std::string MinioTestServer::secret_key() const { return impl_->secret_key_; }

Status MinioTestServer::Start() {
  const char* connect_str = std::getenv(kEnvConnectString);
  const char* access_key = std::getenv(kEnvAccessKey);
  const char* secret_key = std::getenv(kEnvSecretKey);
  if (connect_str && access_key && secret_key) {
    // Use external instance
    impl_->connect_string_ = connect_str;
    impl_->access_key_ = access_key;
    impl_->secret_key_ = secret_key;
    return Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(impl_->temp_dir_, TemporaryDir::Make("s3fs-test-"));

  // Get a copy of the current environment.
  // (NOTE: using "auto" would return a native_environment that mutates
  //  the current environment)
  bp::environment env = boost::this_process::environment();
  env["MINIO_ACCESS_KEY"] = kMinioAccessKey;
  env["MINIO_SECRET_KEY"] = kMinioSecretKey;
  // Disable the embedded console (one less listening address to care about)
  env["MINIO_BROWSER"] = "off";

  impl_->connect_string_ = GenerateConnectString();
  auto exe_path = bp::search_path(kMinioExecutableName);
  if (exe_path.empty()) {
    return Status::IOError("Failed to find minio executable ('", kMinioExecutableName,
                           "') in PATH");
  }

  try {
    // NOTE: --quiet makes startup faster by suppressing remote version check
    impl_->server_process_ = std::make_shared<bp::child>(
        env, exe_path, "server", "--quiet", "--compat", "--address",
        impl_->connect_string_, impl_->temp_dir_->path().ToString());
  } catch (const std::exception& e) {
    return Status::IOError("Failed to launch Minio server: ", e.what());
  }
  return Status::OK();
}

Status MinioTestServer::Stop() {
  if (impl_->server_process_ && impl_->server_process_->valid()) {
    // Brutal shutdown
    impl_->server_process_->terminate();
    impl_->server_process_->wait();
#ifndef _WIN32
    // Despite calling wait() above, boost::process fails to clear zombies
    // so do it ourselves.
    waitpid(impl_->server_process_->id(), nullptr, 0);
#endif
  }
  return Status::OK();
}

struct MinioTestEnvironment::Impl {
  std::function<Future<std::shared_ptr<MinioTestServer>>()> server_generator_;

  Result<std::shared_ptr<MinioTestServer>> LaunchOneServer() {
    auto server = std::make_shared<MinioTestServer>();
    RETURN_NOT_OK(server->Start());
    return server;
  }
};

MinioTestEnvironment::MinioTestEnvironment() : impl_(new Impl) {}

MinioTestEnvironment::~MinioTestEnvironment() = default;

void MinioTestEnvironment::SetUp() {
  auto pool = ::arrow::internal::GetCpuThreadPool();

  auto launch_one_server = []() -> Result<std::shared_ptr<MinioTestServer>> {
    auto server = std::make_shared<MinioTestServer>();
    RETURN_NOT_OK(server->Start());
    return server;
  };
  impl_->server_generator_ = [pool, launch_one_server]() {
    return DeferNotOk(pool->Submit(launch_one_server));
  };
  impl_->server_generator_ =
      MakeReadaheadGenerator(std::move(impl_->server_generator_), pool->GetCapacity());
}

Result<std::shared_ptr<MinioTestServer>> MinioTestEnvironment::GetOneServer() {
  return impl_->server_generator_().result();
}

}  // namespace fs
}  // namespace arrow

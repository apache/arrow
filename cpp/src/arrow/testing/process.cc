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

#include "arrow/testing/process.h"

// This boost/asio/io_context.hpp include is needless for no MinGW
// build.
//
// This is for including boost/asio/detail/socket_types.hpp before any
// "#include <windows.h>". boost/asio/detail/socket_types.hpp doesn't
// work if windows.h is already included.
#include <boost/asio/io_context.hpp>

#ifdef BOOST_PROCESS_NEED_SOURCE
// Workaround for https://github.com/boostorg/process/issues/312
#define BOOST_PROCESS_V2_SEPARATE_COMPILATION
#include <boost/process/v2.hpp>
#include <boost/process/v2/src.hpp>
#else
#include <boost/process/v2.hpp>
#endif

#ifdef __APPLE__
#include <limits.h>
#include <mach-o/dyld.h>
#endif

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

namespace asio = BOOST_PROCESS_V2_ASIO_NAMESPACE;
namespace process = BOOST_PROCESS_V2_NAMESPACE;

namespace arrow::util {

class Process::Impl {
 private:
  process::filesystem::path executable_;
  std::vector<std::string> args_;
  std::unordered_map<process::environment::key, process::environment::value> env_;
  std::string marker_;
  std::unique_ptr<process::process> process_;
  asio::io_context ctx_;

  Status ResolveCurrentExecutable(process::filesystem::path* out) {
    // See https://stackoverflow.com/a/1024937/10194 for various
    // platform-specific recipes.

    boost::system::error_code ec;

#if defined(__linux__)
    *out = process::filesystem::canonical("/proc/self/exe", ec);
#elif defined(__APPLE__)
    char buf[PATH_MAX + 1];
    uint32_t bufsize = sizeof(buf);
    if (_NSGetExecutablePath(buf, &bufsize) < 0) {
      return Status::Invalid("Can't resolve current exe: path too large");
    }
    *out = process::filesystem::canonical(buf, ec);
#elif defined(_WIN32)
    char buf[MAX_PATH + 1];
    if (!GetModuleFileNameA(NULL, buf, sizeof(buf))) {
      return Status::Invalid("Can't get executable file path");
    }
    *out = process::filesystem::canonical(buf, ec);
#else
    ARROW_UNUSED(ec);
    return Status::NotImplemented("Not available on this system");
#endif
    if (ec) {
      // XXX fold this into the Status class?
      return Status::IOError("Can't resolve current exe: ", ec.message());
    } else {
      return Status::OK();
    }
  }

 public:
  Impl() {
    // Get a copy of the current environment.
    for (const auto& kv : process::environment::current()) {
      env_[kv.key()] = kv.value().string();
    }
  }

  ~Impl() { process_ = nullptr; }

  Status SetExecutable(const std::string& name) {
    executable_ = process::environment::find_executable(name);
    if (executable_.empty()) {
      // Search the current executable directory as fallback.
      boost::filesystem::path current_exe;
      auto status = ResolveCurrentExecutable(&current_exe);
      if (status.ok()) {
        std::unordered_map<process::environment::key, process::environment::value> env = {
            {"PATH", current_exe.parent_path().string()},
        };
        executable_ = process::environment::find_executable(name, env);
      }
    }
    if (executable_.empty()) {
      return Status::IOError("Failed to find '", name, "' in PATH");
    }
    return Status::OK();
  }

  Status SetArgs(const std::vector<std::string>& args) {
    args_ = args;
    return Status::OK();
  }

  Status SetEnv(const std::string& name, const std::string& value) {
    // Workaround for https://github.com/boostorg/process/issues/365
    env_[name] = std::string(value);
    return Status::OK();
  }

  Status SetReadyErrorMessage(const std::string& marker) {
    marker_ = marker;
    return Status::OK();
  }

  Status Execute() {
    try {
      process::process_environment env(env_);
      if (marker_.empty()) {
        // We can't use std::make_unique<process::process>.
        process_ = std::unique_ptr<process::process>(
            new process::process(ctx_, executable_, args_, env));
        return Status::OK();
      }

      asio::readable_pipe stderr(ctx_);
      // We can't use std::make_unique<process::process>.
      process_ = std::unique_ptr<process::process>(new process::process(
          ctx_, executable_, args_, env, process::process_stdio{{}, {}, stderr}));
      std::stringstream buffered_output;
      std::array<char, 1024> buffer;
      std::string line;
      auto timeout = std::chrono::seconds(10);
      std::chrono::time_point<std::chrono::steady_clock> end =
          std::chrono::steady_clock::now() + timeout;
      while (process_->running() && std::chrono::steady_clock::now() < end) {
        auto read_bytes = stderr.read_some(asio::buffer(buffer.data(), buffer.size()));
        if (buffered_output.eof()) {
          buffered_output.clear();
          auto last = buffered_output.str().size();
          buffered_output.seekg(last);
          buffered_output.seekp(last);
        }
        buffered_output.write(buffer.data(), read_bytes);
        while (std::getline(buffered_output, line)) {
          std::cerr << line << std::endl;
          if (line.find(marker_) != std::string::npos) {
            return Status::OK();
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
      return Status::IOError("Failed to launch '", executable_, " in ", timeout.count(),
                             " seconds");
    } catch (const std::exception& e) {
      return Status::IOError("Failed to launch '", executable_, "': ", e.what());
    }
  }

  bool IsRunning() { return process_ && process_->running(); }

  uint64_t id() {
    if (!process_) {
      return 0;
    }
    return process_->id();
  }
};

Process::Process() : impl_(new Impl()) {}

Process::~Process() {}

Status Process::SetExecutable(const std::string& path) {
  return impl_->SetExecutable(path);
}

Status Process::SetArgs(const std::vector<std::string>& args) {
  return impl_->SetArgs(args);
}

Status Process::SetEnv(const std::string& key, const std::string& value) {
  return impl_->SetEnv(key, value);
}

Status Process::SetReadyErrorMessage(const std::string& marker) {
  return impl_->SetReadyErrorMessage(marker);
}

Status Process::Execute() { return impl_->Execute(); }

bool Process::IsRunning() { return impl_->IsRunning(); }

uint64_t Process::id() { return impl_->id(); }
}  // namespace arrow::util

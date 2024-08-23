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
#include "arrow/result.h"

// This boost/asio/io_context.hpp include is needless for no MinGW
// build.
//
// This is for including boost/asio/detail/socket_types.hpp before any
// "#include <windows.h>". boost/asio/detail/socket_types.hpp doesn't
// work if windows.h is already included.
#include <boost/asio/io_context.hpp>

#ifdef BOOST_PROCESS_HAVE_V2
#ifdef BOOST_PROCESS_NEED_SOURCE
// Workaround for https://github.com/boostorg/process/issues/312
#define BOOST_PROCESS_V2_SEPARATE_COMPILATION
#ifdef __APPLE__
#include <sys/sysctl.h>
#endif
#include <boost/process/v2.hpp>
#include <boost/process/v2/src.hpp>
#else
#include <boost/process/v2.hpp>
#endif
#include <unordered_map>
#else
// We need BOOST_USE_WINDOWS_H definition with MinGW when we use
// boost/process.hpp. See BOOST_USE_WINDOWS_H=1 in
// cpp/cmake_modules/ThirdpartyToolchain.cmake for details.
#include <boost/process.hpp>
#endif

#ifdef __APPLE__
#include <limits.h>
#include <mach-o/dyld.h>
#endif

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

#ifdef BOOST_PROCESS_HAVE_V2
namespace asio = BOOST_PROCESS_V2_ASIO_NAMESPACE;
namespace process = BOOST_PROCESS_V2_NAMESPACE;
namespace filesystem = process::filesystem;
#else
namespace process = boost::process;
namespace filesystem = boost::filesystem;
#endif

namespace arrow::util {

class Process::Impl {
 private:
  filesystem::path executable_;
  std::vector<std::string> args_;
  std::string ready_marker_;
  std::chrono::seconds ready_timeout_ = std::chrono::seconds(10);
#ifdef BOOST_PROCESS_HAVE_V2
  std::unordered_map<process::environment::key, process::environment::value> env_;
  std::unique_ptr<process::process> process_;
  asio::io_context ctx_;
  // boost/process/v2/ doesn't support process group yet:
  // https://github.com/boostorg/process/issues/259
#else
  process::environment env_;
  std::unique_ptr<process::child> process_;
  std::unique_ptr<process::group> process_group_;
#endif

  Result<filesystem::path> ResolveCurrentExecutable() {
    // See https://stackoverflow.com/a/1024937/10194 for various
    // platform-specific recipes.

    filesystem::path path;
    boost::system::error_code error_code;

#if defined(__linux__)
    path = filesystem::canonical("/proc/self/exe", error_code);
#elif defined(__APPLE__)
    char buf[PATH_MAX + 1];
    uint32_t bufsize = sizeof(buf);
    if (_NSGetExecutablePath(buf, &bufsize) < 0) {
      return Status::Invalid("Can't resolve current exe: path too large");
    }
    path = filesystem::canonical(buf, error_code);
#elif defined(_WIN32)
    char buf[MAX_PATH + 1];
    if (!GetModuleFileNameA(NULL, buf, sizeof(buf))) {
      return Status::Invalid("Can't get executable file path");
    }
    path = filesystem::canonical(buf, error_code);
#else
    ARROW_UNUSED(error_code);
    return Status::NotImplemented("Not available on this system");
#endif
    if (error_code) {
      // XXX fold this into the Status class?
      return Status::IOError("Can't resolve current exe: ", error_code.message());
    } else {
      return path;
    }
  }

#ifdef BOOST_PROCESS_HAVE_V2
  Status ExecuteV2() {
    process::process_environment env(env_);
    if (ready_marker_.empty()) {
      // We can't use std::make_unique<process::process>.
      process_ = std::unique_ptr<process::process>(
          new process::process(ctx_, executable_, args_, env));
      return Status::OK();
    }

    asio::readable_pipe error_output(ctx_);
    // We can't use std::make_unique<process::process>.
    process_ = std::unique_ptr<process::process>(new process::process(
        ctx_, executable_, args_, env, process::process_stdio{{}, {}, error_output}));
    std::stringstream buffered_output;
    std::array<char, 1024> buffer;
    std::string line;
    std::chrono::time_point<std::chrono::steady_clock> end =
        std::chrono::steady_clock::now() + ready_timeout_;
    while (process_->running() && std::chrono::steady_clock::now() < end) {
      auto read_bytes =
          error_output.read_some(asio::buffer(buffer.data(), buffer.size()));
      if (buffered_output.eof()) {
        // std::getline() in the previous loop may set the EOF
        // bit. If the EOF bit is set, all std::getline() calls are
        // failed. So we clear the EOF bit and set the position to
        // the last so that the next std::getline() can read
        // unconsumed line.
        buffered_output.clear();
        buffered_output.seekg(0, std::ios_base::end);
        buffered_output.seekp(0, std::ios_base::end);
      }
      buffered_output.write(buffer.data(), read_bytes);
      while (std::getline(buffered_output, line)) {
        std::cerr << line << std::endl;
        if (line.find(ready_marker_) != std::string::npos) {
          return Status::OK();
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return Status::IOError("Failed to launch '", executable_, " in ",
                           ready_timeout_.count(), " seconds");
  }
#else
  Status ExecuteV1() {
    process_group_ = std::make_unique<process::group>();
    if (ready_marker_.empty()) {
      process_ = std::make_unique<process::child>(executable_, process::args(args_), env_,
                                                  *process_group_);
      return Status::OK();
    }

    process::ipstream error_output;
    process_ = std::make_unique<process::child>(executable_, process::args(args_), env_,
                                                *process_group_,
                                                process::std_err > error_output);
    std::string line;
    std::chrono::time_point<std::chrono::steady_clock> end =
        std::chrono::steady_clock::now() + ready_timeout_;
    while (process_->valid() && process_->running() &&
           std::chrono::steady_clock::now() < end) {
      if (error_output.peek() && std::getline(error_output, line)) {
        std::cerr << line << std::endl;
        if (line.find(ready_marker_) != std::string::npos) {
          return Status::OK();
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return Status::IOError("Failed to launch '", executable_, " in ",
                           ready_timeout_.count(), " seconds");
  }
#endif

 public:
  Impl() {
    // Get a copy of the current environment.
#ifdef BOOST_PROCESS_HAVE_V2
    for (const auto& kv : process::environment::current()) {
      env_[kv.key()] = process::environment::value(kv.value());
    }
#else
    env_ = process::environment(boost::this_process::environment());
#endif
  }

  ~Impl() {
#ifdef BOOST_PROCESS_HAVE_V2
    // V2 doesn't provide process group support yet:
    // https://github.com/boostorg/process/issues/259
    //
    // So we try graceful shutdown (SIGTERM + waitpid()) before
    // immediate shutdown (SIGTERM). This assumes that the target
    // executable such as "python3 -m testbench" terminates all related
    // processes by graceful shutdown.
    boost::system::error_code error_code;
    if (process_ && process_->running(error_code)) {
      process_->interrupt(error_code);
      if (!error_code) {
        auto timeout = std::chrono::seconds(3);
        std::chrono::time_point<std::chrono::steady_clock> end =
            std::chrono::steady_clock::now() + timeout;
        int i = 0;
        while (process_->running(error_code) && std::chrono::steady_clock::now() < end) {
          std::cerr << "(waiting terminated: " << i++ << ")" << std::endl;
          std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        if (process_->running(error_code)) {
          std::cerr << "(failed graceful shutdown)" << std::endl;
          std::cerr << "(start terminate)" << std::endl;
          process_->terminate(error_code);
          std::cerr << "(finished terminate)" << std::endl;
        }
      }
    }
#else
    process_group_ = nullptr;
#endif
    std::cerr << "(start immediate shutdown)" << std::endl;
    process_ = nullptr;
    std::cerr << "(finished immediate shutdown)" << std::endl;
  }

  Status SetExecutable(const std::string& name) {
#ifdef BOOST_PROCESS_HAVE_V2
    executable_ = process::environment::find_executable(name);
#else
    executable_ = process::search_path(name);
#endif
    if (executable_.empty()) {
      // Search the current executable directory as fallback.
      ARROW_ASSIGN_OR_RAISE(auto current_exe, ResolveCurrentExecutable());
#ifdef BOOST_PROCESS_HAVE_V2
      std::unordered_map<process::environment::key, process::environment::value> env;
      for (const auto& kv : process::environment::current()) {
        env[kv.key()] = process::environment::value(kv.value());
      }
      env["PATH"] = process::environment::value(current_exe.parent_path());
      executable_ = process::environment::find_executable(name, env);
#else
      executable_ = process::search_path(name, {current_exe.parent_path()});
#endif
    }
    if (executable_.empty()) {
      return Status::IOError("Failed to find '", name, "' in PATH");
    }
    return Status::OK();
  }

  void SetArgs(const std::vector<std::string>& args) { args_ = args; }

  void SetEnv(const std::string& name, const std::string& value) {
#ifdef BOOST_PROCESS_HAVE_V2
    env_[name] = process::environment::value(value);
#else
    env_[name] = value;
#endif
  }

  void SetReadyErrorMessage(const std::string& marker) { ready_marker_ = marker; }

  Status Execute() {
    try {
#ifdef BOOST_PROCESS_HAVE_V2
      return ExecuteV2();
#else
      return ExecuteV1();
#endif
    } catch (const std::exception& e) {
      return Status::IOError("Failed to launch '", executable_, "': ", e.what());
    }
  }

  bool IsRunning() {
#ifdef BOOST_PROCESS_HAVE_V2
    boost::system::error_code error_code;
    return process_ && process_->running(error_code);
#else
    return process_ && process_->running();
#endif
  }

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

void Process::SetArgs(const std::vector<std::string>& args) { impl_->SetArgs(args); }

void Process::SetEnv(const std::string& key, const std::string& value) {
  impl_->SetEnv(key, value);
}

void Process::SetReadyErrorMessage(const std::string& marker) {
  impl_->SetReadyErrorMessage(marker);
}

Status Process::Execute() { return impl_->Execute(); }

bool Process::IsRunning() { return impl_->IsRunning(); }

uint64_t Process::id() { return impl_->id(); }
}  // namespace arrow::util

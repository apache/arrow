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
#include "arrow/util/config.h"

#ifdef ARROW_ENABLE_THREADING
#  define BOOST_PROCESS_AVAILABLE
#endif

#ifdef BOOST_PROCESS_AVAILABLE
// This boost/asio/io_context.hpp include is needless for no MinGW
// build.
//
// This is for including boost/asio/detail/socket_types.hpp before any
// "#include <windows.h>". boost/asio/detail/socket_types.hpp doesn't
// work if windows.h is already included.
#  include <boost/asio/io_context.hpp>

#  ifdef BOOST_PROCESS_USE_V2
#    ifdef BOOST_PROCESS_NEED_SOURCE
// Workaround for https://github.com/boostorg/process/issues/312
#      define BOOST_PROCESS_V2_SEPARATE_COMPILATION
#      ifdef __APPLE__
#        include <sys/sysctl.h>
#      endif
#      include <boost/process/v2/src.hpp>
#    endif
#    include <boost/process/v2/environment.hpp>
#    include <boost/process/v2/error.hpp>
#    include <boost/process/v2/execute.hpp>
#    include <boost/process/v2/exit_code.hpp>
#    include <boost/process/v2/pid.hpp>
#    include <boost/process/v2/popen.hpp>
#    include <boost/process/v2/process.hpp>
#    include <boost/process/v2/process_handle.hpp>
#    include <boost/process/v2/start_dir.hpp>
#    include <boost/process/v2/stdio.hpp>
#    include <unordered_map>
#  else
// We need BOOST_USE_WINDOWS_H definition with MinGW when we use
// boost/process.hpp. boost/process/detail/windows/handle_workaround.hpp
// doesn't work without BOOST_USE_WINDOWS_H with MinGW because MinGW
// doesn't provide __kernel_entry without winternl.h.
//
// See also:
// https://github.com/boostorg/process/blob/develop/include/boost/process/detail/windows/handle_workaround.hpp
#    ifdef __MINGW32__
#      define BOOST_USE_WINDOWS_H = 1
#    endif
#    ifdef BOOST_PROCESS_HAVE_V1
#      include <boost/process/v1/args.hpp>
#      include <boost/process/v1/async.hpp>
#      include <boost/process/v1/async_system.hpp>
#      include <boost/process/v1/child.hpp>
#      include <boost/process/v1/cmd.hpp>
#      include <boost/process/v1/env.hpp>
#      include <boost/process/v1/environment.hpp>
#      include <boost/process/v1/error.hpp>
#      include <boost/process/v1/exe.hpp>
#      include <boost/process/v1/group.hpp>
#      include <boost/process/v1/handles.hpp>
#      include <boost/process/v1/io.hpp>
#      include <boost/process/v1/pipe.hpp>
#      include <boost/process/v1/search_path.hpp>
#      include <boost/process/v1/shell.hpp>
#      include <boost/process/v1/spawn.hpp>
#      include <boost/process/v1/start_dir.hpp>
#      include <boost/process/v1/system.hpp>
#    else
#      include <boost/process.hpp>
#    endif
#  endif

#  ifdef __APPLE__
#    include <limits.h>
#    include <mach-o/dyld.h>
#  endif

#  include <chrono>
#  include <iostream>
#  include <sstream>
#  include <thread>

#  ifdef BOOST_PROCESS_USE_V2
namespace process = BOOST_PROCESS_V2_NAMESPACE;
namespace filesystem = process::filesystem;
// For Boost < 1.87.0
#    ifdef BOOST_PROCESS_V2_ASIO_NAMESPACE
namespace asio = BOOST_PROCESS_V2_ASIO_NAMESPACE;
#    else
namespace asio = process::net;
#    endif
#  elif defined(BOOST_PROCESS_HAVE_V1)
namespace process = boost::process::v1;
namespace filesystem = boost::process::v1::filesystem;
#  else
namespace process = boost::process;
namespace filesystem = boost::filesystem;
#  endif
#endif

namespace arrow::util {

class Process::Impl {
 public:
  Impl() {
    // Get a copy of the current environment.
#ifdef BOOST_PROCESS_AVAILABLE
#  ifdef BOOST_PROCESS_USE_V2
    for (const auto& kv : process::environment::current()) {
      env_[kv.key()] = process::environment::value(kv.value());
    }
#  else
    env_ = process::environment(boost::this_process::environment());
#  endif
#endif
  }

  ~Impl() {
#ifdef BOOST_PROCESS_AVAILABLE
#  ifdef BOOST_PROCESS_USE_V2
    // V2 doesn't provide process group support yet:
    // https://github.com/boostorg/process/issues/259
    //
    // So we try graceful shutdown (SIGTERM + waitpid()) before
    // immediate shutdown (SIGKILL). This assumes that the target
    // executable such as "python3 -m testbench" terminates all related
    // processes by graceful shutdown.
    boost::system::error_code error_code;
    if (process_ && process_->running(error_code)) {
      process_->request_exit(error_code);
      if (!error_code) {
        auto timeout = std::chrono::seconds(3);
        std::chrono::time_point<std::chrono::steady_clock> end =
            std::chrono::steady_clock::now() + timeout;
        while (process_->running(error_code) && std::chrono::steady_clock::now() < end) {
          std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
      }
    }
#  else
    process_group_ = nullptr;
#  endif
    process_ = nullptr;
#endif
  }

  Status SetExecutable(const std::string& name) {
#ifdef BOOST_PROCESS_AVAILABLE
#  ifdef BOOST_PROCESS_USE_V2
    executable_ = process::environment::find_executable(name);
#  else
    executable_ = process::search_path(name);
#  endif
    if (executable_.empty()) {
      // Search the current executable directory as fallback.
      ARROW_ASSIGN_OR_RAISE(auto current_exe, ResolveCurrentExecutable());
#  ifdef BOOST_PROCESS_USE_V2
      std::unordered_map<process::environment::key, process::environment::value> env;
      for (const auto& kv : process::environment::current()) {
        env[kv.key()] = process::environment::value(kv.value());
      }
      env["PATH"] = process::environment::value(current_exe.parent_path());
      executable_ = process::environment::find_executable(name, env);
#  else
      executable_ = process::search_path(name, {current_exe.parent_path()});
#  endif
    }
    if (executable_.empty()) {
      return Status::IOError("Failed to find '", name, "' in PATH");
    }
    return Status::OK();
#else
    return Status::NotImplemented("Boost.Process isn't available on this system");
#endif
  }

  void SetArgs(const std::vector<std::string>& args) {
#ifdef BOOST_PROCESS_AVAILABLE
    args_ = args;
#endif
  }

  void SetEnv(const std::string& name, const std::string& value) {
#ifdef BOOST_PROCESS_AVAILABLE
#  ifdef BOOST_PROCESS_USE_V2
    env_[name] = process::environment::value(value);
#  else
    env_[name] = value;
#  endif
#endif
  }

  void IgnoreStderr() {
#ifdef BOOST_PROCESS_AVAILABLE
    keep_stderr_ = false;
#endif
  }

  Status Execute() {
#ifdef BOOST_PROCESS_AVAILABLE
    try {
#  ifdef BOOST_PROCESS_USE_V2
      return ExecuteV2();
#  else
      return ExecuteV1();
#  endif
    } catch (const std::exception& e) {
      return Status::IOError("Failed to launch '", executable_, "': ", e.what());
    }
#else
    return Status::NotImplemented("Boost.Process isn't available on this system");
#endif
  }

  bool IsRunning() {
#ifdef BOOST_PROCESS_AVAILABLE
#  ifdef BOOST_PROCESS_USE_V2
    boost::system::error_code error_code;
    return process_ && process_->running(error_code);
#  else
    return process_ && process_->running();
#  endif
#else
    return false;
#endif
  }

  uint64_t pid() {
#ifdef BOOST_PROCESS_AVAILABLE
    if (!process_) {
      return 0;
    }
    return process_->id();
#else
    return 0;
#endif
  }

 private:
#ifdef BOOST_PROCESS_AVAILABLE
  filesystem::path executable_;
  std::vector<std::string> args_;
  bool keep_stderr_ = true;
#  ifdef BOOST_PROCESS_USE_V2
  std::unordered_map<process::environment::key, process::environment::value> env_;
  std::unique_ptr<process::process> process_;
  asio::io_context ctx_;
  // boost/process/v2/ doesn't support process group yet:
  // https://github.com/boostorg/process/issues/259
#  else
  process::environment env_;
  std::unique_ptr<process::child> process_;
  std::unique_ptr<process::group> process_group_;
#  endif
#endif

#ifdef BOOST_PROCESS_AVAILABLE
  Result<filesystem::path> ResolveCurrentExecutable() {
    // See https://stackoverflow.com/a/1024937/10194 for various
    // platform-specific recipes.

    filesystem::path path;
    boost::system::error_code error_code;

#  if defined(__linux__)
    path = filesystem::canonical("/proc/self/exe", error_code);
#  elif defined(__FreeBSD__)
    path = filesystem::canonical("/proc/curproc/file", error_code);
#  elif defined(__APPLE__)
    char buf[PATH_MAX + 1];
    uint32_t bufsize = sizeof(buf);
    if (_NSGetExecutablePath(buf, &bufsize) < 0) {
      return Status::Invalid("Can't resolve current exe: path too large");
    }
    path = filesystem::canonical(buf, error_code);
#  elif defined(_WIN32)
    char buf[MAX_PATH + 1];
    if (!GetModuleFileNameA(NULL, buf, sizeof(buf))) {
      return Status::Invalid("Can't get executable file path");
    }
    path = filesystem::canonical(buf, error_code);
#  else
    ARROW_UNUSED(error_code);
    return Status::NotImplemented("Not available on this system");
#  endif
    if (error_code) {
      // XXX fold this into the Status class?
      return Status::IOError("Can't resolve current exe: ", error_code.message());
    } else {
      return path;
    }
  }

#  ifdef BOOST_PROCESS_USE_V2
  Status ExecuteV2() {
    process::process_environment env(env_);
    // We can't use std::make_unique<process::process>.
    process_ = std::unique_ptr<process::process>(
        new process::process(ctx_, executable_, args_, env,
                             keep_stderr_ ? process::process_stdio{{}, {}, {}}
                                          : process::process_stdio{{}, {}, nullptr}));
    return Status::OK();
  }
#  else
  Status ExecuteV1() {
    process_group_ = std::make_unique<process::group>();
    if (keep_stderr_) {
      process_ = std::make_unique<process::child>(executable_, process::args(args_), env_,
                                                  *process_group_);
    } else {
      process_ = std::make_unique<process::child>(executable_, process::args(args_), env_,
                                                  *process_group_,
                                                  process::std_err > process::null);
    }
    return Status::OK();
  }
#  endif
#endif
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

void Process::IgnoreStderr() { impl_->IgnoreStderr(); }

Status Process::Execute() { return impl_->Execute(); }

bool Process::IsRunning() { return impl_->IsRunning(); }

uint64_t Process::pid() { return impl_->pid(); }
}  // namespace arrow::util
